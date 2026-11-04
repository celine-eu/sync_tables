"""Who may talk to a listener: named bearer tokens, and proof that an ingress checked a
client certificate.

Tokens. A listener holds one token (a file) or several (a directory, one file per token,
named after its holder). Each holder is revoked on its own by removing its file; the
directory is re-read every few seconds, so a mounted Secret's update needs no restart.
The holder's name is a label the operator chose: it goes in the log of an accepted
connection, the token never does.

Upstream client certificate. Behind an ingress that ends TLS (`--tls-terminated-upstream`)
the listener never sees the handshake, so it cannot tell an ingress that verified a client
certificate from one that was misconfigured and verified nothing. With
`--require-upstream-client-cert HEADER`, the ingress must forward the client certificate
it verified (Traefik `passTLSClientCert`, ingress-nginx `auth-tls-pass-certificate-to-
upstream`), and the listener checks it again against the sync CA: issued directly by it,
within its validity, for client authentication. A request without one is refused, so an
ingress that stopped verifying makes every run fail instead of silently opening the route.
"""
from __future__ import annotations

import base64
import datetime as dt
import hmac
import os
import re
import threading
import time
from urllib.parse import unquote

from cryptography import x509
from cryptography.x509.oid import ExtendedKeyUsageOID

from sync_tables.log import SyncError, log

TOKEN_MIN = 32
NAME_RE = re.compile(r"[a-z0-9][a-z0-9_-]{0,63}")
RELOAD_SECONDS = 10


class TokenStore:
    """The tokens a listener accepts, each with its holder's name."""

    def __init__(self, path: str | None = None, tokens: dict[str, str] | None = None,
                 clock=time.monotonic) -> None:
        if (path is None) == (tokens is None):
            raise ValueError("a token store is a path or a mapping")
        self._path = path
        self._clock = clock
        self._lock = threading.Lock()
        self._loaded_at = clock()
        self._tokens = _checked(tokens) if tokens is not None else _load(path)  # type: ignore[arg-type]

    @classmethod
    def of(cls, value: "TokenStore | str") -> "TokenStore":
        """A store from a single token string (tests, a one-token listener built in code)."""
        return value if isinstance(value, TokenStore) else cls(tokens={"default": value})

    @property
    def names(self) -> list[str]:
        return sorted(self._tokens)

    def _current(self) -> dict[str, str]:
        if self._path is None:
            return self._tokens
        with self._lock:
            now = self._clock()
            if now - self._loaded_at >= RELOAD_SECONDS:
                self._loaded_at = now
                try:
                    self._tokens = _load(self._path)
                except SyncError as e:
                    log(f"tokens: reload failed, keeping the previous set: {e}")
            return self._tokens

    def match(self, authorization: str | None) -> str | None:
        """The holder of the bearer token in an Authorization header, or None.

        Every token is compared, in constant time, whether or not one already matched."""
        given = (authorization or "").encode()
        found = None
        for name, token in self._current().items():
            if hmac.compare_digest(given, f"Bearer {token}".encode()) and found is None:
                found = name
        return found


def _checked(tokens: dict[str, str]) -> dict[str, str]:
    if not tokens:
        raise SyncError("no token to accept")
    seen: set[str] = set()
    for name, token in tokens.items():
        if not NAME_RE.fullmatch(name):
            raise SyncError(f"token name {name!r} is not a plain lower-case name")
        if len(token) < TOKEN_MIN:
            raise SyncError(f"token {name!r} holds fewer than {TOKEN_MIN} characters")
        if token in seen:
            raise SyncError("two holders share one token")
        seen.add(token)
    return dict(tokens)


def _load(path: str) -> dict[str, str]:
    if os.path.isdir(path):
        tokens = {}
        for entry in sorted(os.listdir(path)):
            full = os.path.join(path, entry)
            # A mounted Secret also holds `..data` and timestamped dot-directories.
            if entry.startswith(".") or not os.path.isfile(full):
                continue
            with open(full, "r", encoding="utf-8") as f:
                tokens[entry] = f.read().strip()
        return _checked(tokens)
    with open(path, "r", encoding="utf-8") as f:
        return _checked({"default": f.read().strip()})


# ---------------------------------------------------------------------------
# The client certificate an ingress verified
# ---------------------------------------------------------------------------

class UpstreamCertCheck:
    """Check the client certificate an ingress forwards, against the sync CA."""

    def __init__(self, header: str, ca_file: str, clock=lambda: dt.datetime.now(dt.timezone.utc)) -> None:
        if not header:
            raise SyncError("name the header the ingress forwards the client certificate in")
        with open(ca_file, "rb") as f:
            self._ca = x509.load_pem_x509_certificate(f.read())
        self.header = header
        self._clock = clock

    def verify(self, value: str | None) -> bool:
        if not value:
            return False
        try:
            cert = _parse_forwarded(value)
            cert.verify_directly_issued_by(self._ca)
            now = self._clock()
            if not (cert.not_valid_before_utc <= now <= cert.not_valid_after_utc):
                return False
            bc = cert.extensions.get_extension_for_class(x509.BasicConstraints).value
            if bc.ca:
                return False
            eku = cert.extensions.get_extension_for_class(x509.ExtendedKeyUsage).value
            return ExtendedKeyUsageOID.CLIENT_AUTH in eku
        except Exception:
            return False


def _parse_forwarded(value: str) -> x509.Certificate:
    """ingress-nginx sends the URL-escaped PEM; Traefik the URL-escaped base64 body without
    its delimiters, several certificates separated by commas (the first is the client's)."""
    text = unquote(value).strip()
    if "-----BEGIN CERTIFICATE-----" in text:
        return x509.load_pem_x509_certificate(text.encode())
    first = text.split(",")[0]
    return x509.load_der_x509_certificate(base64.b64decode("".join(first.split()), validate=True))
