"""`sync-tables certs`: the private mTLS material for the sync (plan W7, W8, W14).

One CA used only for this sync signs the server certificate (the listening host) and the
client certificates. Use one CA per purpose: `pull` (developers' machines dial a serving
source) and `push` (a source dials a sink in the target environment), each in its own
directory, so a certificate handed to a developer is never accepted by a push sink. The
purpose is written into the CA's name; keeping the two apart is the deployment's part.

Files are PEM, private keys mode 0600. Nothing secret is printed. An
existing file is never overwritten without --force, so a re-run is a no-op.
"""
from __future__ import annotations

import datetime as dt
import os
import secrets
from dataclasses import dataclass

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID

from sync_tables.log import SyncError, log

CA_NAME = "ca"
PURPOSES = ("pull", "push")
TOKEN_FILE = "token"


@dataclass(frozen=True)
class Paths:
    cert: str
    key: str


def _paths(out: str, name: str) -> Paths:
    return Paths(os.path.join(out, f"{name}.crt"), os.path.join(out, f"{name}.key"))


def _exists(p: Paths) -> bool:
    return os.path.exists(p.cert) or os.path.exists(p.key)


def _write(path: str, data: bytes, private: bool) -> None:
    flags = os.O_WRONLY | os.O_CREAT | os.O_TRUNC
    fd = os.open(path, flags, 0o600 if private else 0o644)
    try:
        os.write(fd, data)
    finally:
        os.close(fd)
    os.chmod(path, 0o600 if private else 0o644)


def _key() -> ec.EllipticCurvePrivateKey:
    return ec.generate_private_key(ec.SECP256R1())


def _save(p: Paths, cert: x509.Certificate, key: ec.EllipticCurvePrivateKey) -> None:
    _write(p.key, key.private_bytes(
        serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()
    ), private=True)
    _write(p.cert, cert.public_bytes(serialization.Encoding.PEM), private=False)


def _load_ca(out: str) -> tuple[x509.Certificate, ec.EllipticCurvePrivateKey]:
    p = _paths(out, CA_NAME)
    if not (os.path.exists(p.cert) and os.path.exists(p.key)):
        raise SyncError(f"no CA in {out}; run `sync-tables certs ca --out {out}` first")
    with open(p.cert, "rb") as f:
        cert = x509.load_pem_x509_certificate(f.read())
    with open(p.key, "rb") as f:
        key = serialization.load_pem_private_key(f.read(), password=None)
    if not isinstance(key, ec.EllipticCurvePrivateKey):
        raise SyncError("the CA key is not an EC key")
    return cert, key


def _now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def make_ca(out: str, days: int = 3650, force: bool = False, purpose: str | None = None) -> bool:
    if purpose is not None and purpose not in PURPOSES:
        raise SyncError(f"a CA's purpose is one of {', '.join(PURPOSES)}")
    cn = f"sync-tables {purpose} CA" if purpose else "sync-tables CA"
    os.makedirs(out, mode=0o700, exist_ok=True)
    p = _paths(out, CA_NAME)
    if _exists(p) and not force:
        log(f"certs: CA exists in {out}, kept (use --force to replace)")
        return False
    key = _key()
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, cn)])
    now = _now()
    cert = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - dt.timedelta(minutes=5))
        .not_valid_after(now + dt.timedelta(days=days))
        .add_extension(x509.BasicConstraints(ca=True, path_length=0), critical=True)
        .add_extension(
            x509.KeyUsage(
                digital_signature=False, content_commitment=False, key_encipherment=False,
                data_encipherment=False, key_agreement=False, key_cert_sign=True, crl_sign=True,
                encipher_only=False, decipher_only=False,
            ),
            critical=True,
        )
        .add_extension(x509.SubjectKeyIdentifier.from_public_key(key.public_key()), critical=False)
        .sign(key, hashes.SHA256())
    )
    _save(p, cert, key)
    log(f"certs: CA written to {out}")
    return True


def _leaf(
    out: str, name: str, cn: str, days: int, force: bool, server_dns: list[str] | None
) -> bool:
    p = _paths(out, name)
    if _exists(p) and not force:
        log(f"certs: {name} exists in {out}, kept (use --force to replace)")
        return False
    ca_cert, ca_key = _load_ca(out)
    key = _key()
    now = _now()
    builder = (
        x509.CertificateBuilder()
        .subject_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, cn)]))
        .issuer_name(ca_cert.subject)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - dt.timedelta(minutes=5))
        .not_valid_after(now + dt.timedelta(days=days))
        .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
        .add_extension(
            x509.KeyUsage(
                digital_signature=True, content_commitment=False, key_encipherment=False,
                data_encipherment=False, key_agreement=True, key_cert_sign=False, crl_sign=False,
                encipher_only=False, decipher_only=False,
            ),
            critical=True,
        )
        .add_extension(
            x509.AuthorityKeyIdentifier.from_issuer_public_key(ca_key.public_key()), critical=False
        )
    )
    if server_dns:
        builder = builder.add_extension(
            x509.SubjectAlternativeName([x509.DNSName(d) for d in server_dns]), critical=False
        ).add_extension(x509.ExtendedKeyUsage([ExtendedKeyUsageOID.SERVER_AUTH]), critical=False)
    else:
        builder = builder.add_extension(x509.ExtendedKeyUsage([ExtendedKeyUsageOID.CLIENT_AUTH]), critical=False)
    _save(p, builder.sign(ca_key, hashes.SHA256()), key)
    log(f"certs: {name} written to {out}")
    return True


def make_server(out: str, dns: list[str], days: int = 397, force: bool = False, name: str = "server") -> bool:
    if not dns:
        raise SyncError("a server certificate needs at least one --dns name")
    return _leaf(out, name, dns[0], days, force, server_dns=dns)


def make_client(out: str, client_name: str, days: int = 397, force: bool = False) -> bool:
    if not client_name or "/" in client_name or client_name.startswith("."):
        raise SyncError("a client needs a plain --name")
    return _leaf(out, f"client-{client_name}", client_name, days, force, server_dns=None)


def make_token(out: str, force: bool = False) -> bool:
    os.makedirs(out, mode=0o700, exist_ok=True)
    path = os.path.join(out, TOKEN_FILE)
    if os.path.exists(path) and not force:
        log(f"certs: token exists in {out}, kept (use --force to replace)")
        return False
    _write(path, (secrets.token_urlsafe(48) + "\n").encode(), private=True)
    log(f"certs: token written to {out}")
    return True


def main(argv: list[str]) -> int:
    import argparse  # noqa: PLC0415

    p = argparse.ArgumentParser(prog="sync-tables certs", description="Generate the sync's private mTLS material")
    sub = p.add_subparsers(dest="what", required=True)
    for what, helptext in (
        ("ca", "the sync CA"),
        ("server", "a server certificate for the listening host"),
        ("client", "a client certificate for a dialling party"),
        ("token", "a random bearer token"),
        ("all", "CA, server, one client and a token"),
    ):
        sp = sub.add_parser(what, help=helptext)
        sp.add_argument("--out", required=True, help="directory to write into (never the repository)")
        sp.add_argument("--force", action="store_true", help="replace existing files")
        if what in ("ca", "all"):
            sp.add_argument("--purpose", choices=PURPOSES, default=None,
                            help="what the CA is for: pull (developers) or push (a sink); one CA each")
        if what in ("ca", "server", "client", "all"):
            sp.add_argument("--days", type=int, default=None, help="validity in days")
        if what in ("server", "all"):
            sp.add_argument("--dns", action="append", default=[], help="DNS name of the listening host (repeatable)")
        if what in ("client", "all"):
            sp.add_argument("--name", required=True, help="the client's name (its certificate CN)")
    a = p.parse_args(argv)

    try:
        if a.what in ("ca", "all"):
            make_ca(a.out, days=a.days or 3650, force=a.force, purpose=a.purpose)
        if a.what in ("server", "all"):
            make_server(a.out, a.dns, days=a.days or 397, force=a.force)
        if a.what in ("client", "all"):
            make_client(a.out, a.name, days=a.days or 397, force=a.force)
        if a.what in ("token", "all"):
            make_token(a.out, force=a.force)
    except SyncError as e:
        log(f"certs: {e}")
        return 1
    return 0
