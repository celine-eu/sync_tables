#!/usr/bin/env python3
"""Refuse what must not be in this public repository.

Usage:
    python scripts/check_public_tree.py [ROOT] [--names FILE] [--extra FILE ...]

Every text file under ROOT (default: the repository) is checked against structural
patterns: private keys, PEM certificate bodies, bearer tokens, IPv4 addresses outside the
loopback and documentation ranges, hostnames and e-mail addresses outside the reserved
example domains, environment-prefixed warehouse schemas, and personal identifiers. --names
adds a list of reserved names (one per line, `#` comments, matched case-insensitively),
kept outside the repository. --extra checks more files, e.g. a commit message.

A finding is reported as `path:line: label`. The matched text is never printed, so a
reserved name or a leaked host does not end up in a CI log. Exit status: 0 clean, 1 at
least one finding, 2 bad usage.
"""
from __future__ import annotations

import argparse
import ipaddress
import os
import pathlib
import re
import sys
from collections.abc import Iterable, Iterator
from dataclasses import dataclass

SKIP_DIRS = {".git", ".venv", "venv", "__pycache__", ".pytest_cache", ".ruff_cache", ".mypy_cache", "dist", "build"}

# Hosts a public repository may name: the reserved example domains (RFC 2606), the
# loopback name, and the services the build and the docs point at.
ALLOWED_HOSTS = (
    "example.org",
    "example.com",
    "example.net",
    "localhost",
    "github.com",
    "ghcr.io",
    "pypi.org",
    "files.pythonhosted.org",
    "apache.org",
    "astral.sh",
    "docker.com",
    "docker.io",
    "postgresql.org",
)

# Loopback, unspecified, the default Docker bridge, and the documentation ranges (RFC 5737).
ALLOWED_NETS = tuple(
    ipaddress.ip_network(n)
    for n in ("127.0.0.0/8", "0.0.0.0/32", "172.17.0.1/32", "192.0.2.0/24", "198.51.100.0/24", "203.0.113.0/24")
)

TLDS = "it|eu|com|org|net|io|cloud|dev|app|local|internal|lan|corp|de|fr|es|be|nl|ch|at|uk|us|info|biz"

PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("private key", re.compile(r"-----BEGIN [A-Z ]*PRIVATE KEY-----")),
    ("certificate", re.compile(r"-----BEGIN CERTIFICATE-----\s*$")),
    ("token", re.compile(r"\beyJ[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\.")),
    ("schema", re.compile(r"\b[a-z][a-z0-9]*_(?:dev|prod|staging|preprod|test)_(?:raw|bronze|silver|gold)\b")),
    ("pod", re.compile(r"\bIT\d{3}E\d{8}\b")),
    ("fiscal code", re.compile(r"\b[A-Z]{6}\d{2}[A-EHLMPR-T]\d{2}[A-Z]\d{3}[A-Z]\b")),
)

# The domain ends in a letter TLD, so a pinned tool (`ruff@0.16.5`) is not an address.
EMAIL = re.compile(r"\b[A-Za-z0-9._%+-]+@((?:[A-Za-z0-9-]+\.)+[A-Za-z]{2,})\b")
# A TLD followed by `.name` is an attribute chain, and `self.`, `cls.`, `a.`, `args.`, `cfg.` open one in code.
HOST = re.compile(rf"(?<![\w.@-])(?!(?:self|cls|a|args|cfg)\.)((?:[a-z0-9-]+\.)+(?:{TLDS}))(?![\w-]|\.\w)", re.IGNORECASE)
IPV4 = re.compile(r"(?<![\w.])(\d{1,3}(?:\.\d{1,3}){3})(?![\w.])")


@dataclass(frozen=True)
class Finding:
    path: str
    line: int
    label: str


def _host_allowed(host: str) -> bool:
    host = host.lower().rstrip(".")
    return any(host == a or host.endswith("." + a) for a in ALLOWED_HOSTS)


def _ip_allowed(text: str) -> bool:
    try:
        ip = ipaddress.ip_address(text)
    except ValueError:
        return True  # not an address (e.g. a four-part version with a part > 255)
    return any(ip in net for net in ALLOWED_NETS)


def _line_findings(line: str, names: list[str]) -> Iterator[str]:
    for label, rx in PATTERNS:
        if rx.search(line):
            yield label
    if any(not _ip_allowed(m.group(1)) for m in IPV4.finditer(line)):
        yield "address"
    emails = list(EMAIL.finditer(line))
    # user@address is a connection URL's userinfo, left to the address check
    emails = [m for m in emails if not IPV4.fullmatch(m.group(1))]
    if any(not _host_allowed(m.group(1)) for m in emails):
        yield "email"
    email_spans = [m.span(1) for m in emails]
    for m in HOST.finditer(line):
        if any(s <= m.start(1) < e for s, e in email_spans):
            continue
        if not _host_allowed(m.group(1)):
            yield "hostname"
            break
    low = line.lower()
    if any(n in low for n in names):
        yield "reserved name"


def _files(root: pathlib.Path) -> Iterator[pathlib.Path]:
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = sorted(d for d in dirnames if d not in SKIP_DIRS)
        for f in sorted(filenames):
            yield pathlib.Path(dirpath) / f


def _text(path: pathlib.Path) -> str | None:
    try:
        data = path.read_bytes()
    except OSError:
        return None
    if b"\x00" in data:
        return None
    try:
        return data.decode("utf-8")
    except UnicodeDecodeError:
        return None


def read_names(path: str | os.PathLike[str]) -> list[str]:
    """Reserved names from a file kept outside the repository, lowercased."""
    names = []
    for raw in pathlib.Path(path).read_text(encoding="utf-8").splitlines():
        s = raw.strip()
        if s and not s.startswith("#"):
            names.append(s.lower())
    return names


def scan(
    root: str | os.PathLike[str],
    names: Iterable[str] = (),
    extra: Iterable[str | os.PathLike[str]] = (),
) -> list[Finding]:
    """Every finding under root (paths relative to it), then in each extra file."""
    root = pathlib.Path(root).resolve()
    names = [n.lower() for n in names if n]
    findings: list[Finding] = []
    targets = [(p, p.relative_to(root).as_posix()) for p in _files(root)]
    targets += [(pathlib.Path(e), str(e)) for e in extra]
    for path, shown in targets:
        text = _text(path)
        if text is None:
            continue
        for i, line in enumerate(text.splitlines(), start=1):
            for label in dict.fromkeys(_line_findings(line, names)):
                findings.append(Finding(shown, i, label))
    return findings


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("root", nargs="?", default=str(pathlib.Path(__file__).resolve().parents[1]))
    p.add_argument("--names", help="file of reserved names, kept outside the repository")
    p.add_argument("--extra", action="append", default=[], help="another file to check (repeatable)")
    a = p.parse_args(argv)
    names = read_names(a.names) if a.names else []
    findings = scan(a.root, names=names, extra=a.extra)
    for f in findings:
        print(f"{f.path}:{f.line}: {f.label}")
    if findings:
        print(f"{len(findings)} finding(s)", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
