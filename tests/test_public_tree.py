"""The repository is public: scripts/check_public_tree.py refuses what must not be in it.

The check knows only structural patterns (keys, tokens, addresses, hostnames outside the
reserved example domains, environment-prefixed warehouse schemas, personal identifiers).
A list of reserved names can be passed with --names; it is kept outside the repository and
never printed. Every sample below is assembled at run time so that this file passes the
check itself.
"""
from __future__ import annotations

import importlib.util
import pathlib
import sys

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "check_public_tree.py"


def _load():
    spec = importlib.util.spec_from_file_location("check_public_tree", SCRIPT)
    assert spec and spec.loader, f"missing {SCRIPT}"
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod  # dataclasses look their module up there
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(scope="module")
def check():
    return _load()


def _tree(tmp_path: pathlib.Path, text: str, name: str = "notes.md") -> pathlib.Path:
    (tmp_path / name).write_text(text, encoding="utf-8")
    return tmp_path


DASHES = "-" * 5


def joined(sep: str, *parts: str) -> str:
    return sep.join(parts)


@pytest.mark.parametrize(
    "label, text",
    [
        ("private key", f"{DASHES}BEGIN RSA PRIVATE KEY{DASHES}\nMIIE\n"),
        ("private key", f"{DASHES}BEGIN PRIVATE KEY{DASHES}\nMIIE\n"),
        ("certificate", f"{DASHES}BEGIN CERTIFICATE{DASHES}\n" + "MIIB" + "A" * 40 + "\n"),
        ("token", "Authorization: Bearer " + "eyJ" + "a" * 20 + "." + "b" * 20 + ".c"),
        ("address", "host: " + joined(".", "10", "20", "30", "40")),
        ("hostname", "url: wss://sync." + "acme-energy" + ".it/sync/v1"),
        ("hostname", "see https://portal." + "acme" + ".cloud/x"),
        ("hostname", "ask " + "acme" + ".ch for access."),
        ("email", "contact: someone@" + "acme" + ".com"),
        ("address", "dsn: postgresql://u:p@" + joined(".", "10", "20", "30", "40") + "/db"),
        ("schema", "schema: " + joined("_", "wh", "dev", "gold")),
        ("schema", "FROM " + joined("_", "wh", "prod", "silver") + ".t"),
        ("pod", "meter " + "IT" + "001" + "E" + "12345678"),
        ("fiscal code", "cf " + "RSSMRA" + "80" + "A" + "01" + "H" + "501" + "U"),
    ],
)
def test_structural_patterns_are_refused(check, tmp_path, label, text):
    findings = check.scan(_tree(tmp_path, text))
    assert [f.label for f in findings] == [label]
    assert findings[0].path == "notes.md" and findings[0].line >= 1


@pytest.mark.parametrize(
    "text",
    [
        "url: wss://sink.example.org/sync/v1",
        "host: db.example.com and example-dev.example.net",
        "listen 0.0.0.0:8443 and 127.0.0.1 and 172.17.0.1",
        "doc address " + joined(".", "192", "0", "2", "10"),
        "https://github.com/astral-sh/uv and ghcr.io/celine-eu/sync-tables",
        "https://files.pythonhosted.org/x and https://pypi.org/simple",
        "http://www.apache.org/licenses/",
        "mail ops@example.org",
        "run: uvx ruff@0.16.5 check .",
        "postgresql+psycopg://postgres:test@127.0.0.1:55432/postgres",
        'if "' + DASHES + "BEGIN CERTIFICATE" + DASHES + '" in text:',
        "schema: silver, table: orders, version 1.2.3",
        "self.engine.dispose() and cfg.sources and a.b",
        "self.ch = ch; self.ch.close(); cls.it",
        "COMMENT ON DATABASE warehouse IS 'celine:env=dev'",
    ],
)
def test_generic_text_passes(check, tmp_path, text):
    assert check.scan(_tree(tmp_path, text)) == []


def test_reserved_names_are_matched_case_insensitively_and_never_printed(check, tmp_path, capsys):
    secret = "Zq" + "xwv" + "Corp"
    names = tmp_path.parent / (tmp_path.name + "-names.txt")
    names.write_text("# a comment\n\n" + secret.lower() + "\n", encoding="utf-8")
    root = _tree(tmp_path, "a line about " + secret.upper() + " here\n")

    findings = check.scan(root, names=check.read_names(names))
    assert [(f.path, f.line, f.label) for f in findings] == [("notes.md", 1, "reserved name")]

    rc = check.main([str(root), "--names", str(names)])
    out = capsys.readouterr()
    assert rc == 1
    assert secret.lower() not in (out.out + out.err).lower()
    assert "notes.md:1" in out.out + out.err


def test_matched_text_is_never_printed(check, tmp_path, capsys):
    host = "sync." + "acme-energy" + ".it"
    rc = check.main([str(_tree(tmp_path, "url: wss://" + host + "\n"))])
    out = capsys.readouterr()
    assert rc == 1
    assert host not in out.out + out.err
    assert "notes.md:1: hostname" in out.out + out.err


def test_extra_files_are_checked_too(check, tmp_path):
    """A commit message, kept outside the tree, goes through the same check."""
    msg = tmp_path.parent / (tmp_path.name + "-msg.txt")
    msg.write_text("feat: copy " + joined("_", "wh", "dev", "gold") + " tables\n", encoding="utf-8")
    empty = tmp_path / "tree"
    empty.mkdir()
    findings = check.scan(empty, extra=[msg])
    assert [f.label for f in findings] == ["schema"]


def test_build_and_vcs_directories_are_skipped(check, tmp_path):
    for d in (".git", ".venv", "__pycache__", ".pytest_cache", ".ruff_cache"):
        (tmp_path / d).mkdir()
        (tmp_path / d / "x.txt").write_text("someone@" + "acme" + ".com\n", encoding="utf-8")
    assert check.scan(tmp_path) == []


def test_binary_files_are_skipped(check, tmp_path):
    (tmp_path / "blob.bin").write_bytes(b"\x00\xff\xfe" + b"someone@" + b"acme" + b".com")
    assert check.scan(tmp_path) == []


def test_clean_tree_exits_zero(check, tmp_path, capsys):
    assert check.main([str(_tree(tmp_path, "nothing to see\n"))]) == 0


def test_this_repository_passes():
    """The gate CI runs: the whole tree, structural patterns only."""
    findings = _load().scan(ROOT)
    assert findings == [], "\n".join(f"{f.path}:{f.line}: {f.label}" for f in findings)
