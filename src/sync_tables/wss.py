"""WSS transport for the push and pull edges (plan W7, W9, W10).

On a push the source dials a listening sink; on a pull the sink dials a listening (serving)
source. The dialling side trusts only the sync CA for the server's certificate. A push
uses mutual TLS: the sink (or the ingress in front of it) accepts only client certificates
from the push CA, and with `--require-upstream-client-cert` checks again what the ingress
forwarded (auth.UpstreamCertCheck). A pull may run on server TLS alone, where the network
the serving source is reachable from is the gate. A bearer token, one per holder
(auth.TokenStore), is checked before the WebSocket opens, and a client that keeps failing
it is refused for a while (429). One route, `/sync/v1`; anything else is 404. Health is
served on a separate plain-HTTP port, never exposed.
"""
from __future__ import annotations

import http.server
import logging
import ssl
import threading
import time
from collections import deque
from typing import Any, Callable

from websockets.exceptions import ConnectionClosed, InvalidHandshake, InvalidStatus
from websockets.sync.client import connect
from websockets.sync.server import ServerConnection, serve

from sync_tables.auth import TokenStore, UpstreamCertCheck
from sync_tables.log import Refused, SyncError, log
from sync_tables.protocol import MAX_FRAME, Channel, ConnectionLost, serve_sink

ROUTE = "/sync/v1"
PING_INTERVAL = 20
# The library logs peer addresses and handshake details; S8 keeps them out of our logs.
_QUIET = logging.getLogger("sync_tables.wss.quiet")
_QUIET.addHandler(logging.NullHandler())
_QUIET.propagate = False


class WsChannel:
    def __init__(self, ws: Any) -> None:
        self._ws = ws

    def send_raw(self, data: bytes) -> None:
        try:
            self._ws.send(data)
        except ConnectionClosed:
            raise ConnectionLost("connection closed") from None

    def recv_raw(self) -> bytes:
        try:
            data = self._ws.recv()
        except ConnectionClosed:
            raise ConnectionLost("connection closed") from None
        if isinstance(data, str):
            raise SyncError("text frame on a binary protocol")
        return data

    def close(self) -> None:
        try:
            self._ws.close()
        except Exception:
            pass


def _read(path: str) -> str:
    with open(path, "r", encoding="utf-8") as f:
        return f.read().strip()


# ---------------------------------------------------------------------------
# Dialling side (a push source, or a pull sink)
# ---------------------------------------------------------------------------

def client_tls(ca_file: str, cert_file: str | None = None, key_file: str | None = None) -> ssl.SSLContext:
    """Trust the sync CA alone; present a client certificate when one is given (a push)."""
    # With a cafile, the default context trusts that file only: no public CA.
    ctx = ssl.create_default_context(ssl.Purpose.SERVER_AUTH, cafile=ca_file)
    ctx.minimum_version = ssl.TLSVersion.TLSv1_2
    if cert_file or key_file:
        if not (cert_file and key_file):
            raise SyncError("a client certificate needs both --cert and --key")
        ctx.load_cert_chain(cert_file, key_file)
    return ctx


def channel_factory(
    url: str,
    tls: ssl.SSLContext,
    token: str,
    max_frame: int = MAX_FRAME,
    open_timeout: float = 30,
    peer: str = "sink",
) -> Callable[[], Channel]:
    if not url.startswith("wss://"):
        raise Refused("transport_mismatch", f"the {peer} is a wss:// URL")
    if not url.rstrip("/").endswith(ROUTE):
        url = url.rstrip("/") + ROUTE

    def open_channel() -> Channel:
        try:
            ws = connect(
                url,
                ssl=tls,
                additional_headers={"Authorization": f"Bearer {token}"},
                max_size=max_frame + 1024,
                ping_interval=PING_INTERVAL,
                ping_timeout=PING_INTERVAL,
                open_timeout=open_timeout,
                close_timeout=5,
                proxy=None,
                legacy=True,  # a long-lived connection held by the channel, not a `with` block
                logger=_QUIET,
            )
        except InvalidStatus as e:
            code = e.response.status_code
            if code == 401:
                raise Refused("auth_failed", f"the {peer} refused the token (HTTP 401)") from None
            if code == 403:
                # The listener answers 403 only when the ingress forwarded no verified client
                # certificate; otherwise it is the ingress's address allowlist.
                body = (e.response.body or b"").decode("utf-8", "replace")
                if "client certificate" in body:
                    raise Refused("client_cert_unverified", f"the {peer} saw no client certificate verified by its "
                                  "ingress (HTTP 403): the ingress is not checking client certificates") from None
                raise Refused("source_refused", f"the {peer}'s ingress refused this client's address (HTTP 403): "
                              "has the source's egress address changed?") from None
            if code == 429:
                wait = e.response.headers.get("Retry-After", "?")
                raise Refused("rate_limited", f"the {peer} refuses this client for {wait}s after repeated failed attempts") from None
            if code == 400:
                # An ingress that verifies client certificates answers 400 for a missing or
                # foreign one, before the request reaches the peer.
                raise Refused("client_cert_refused", f"the {peer}'s ingress refused the client certificate (HTTP 400)") from None
            raise SyncError(f"the {peer} answered HTTP {code}") from None
        except ssl.SSLError as e:
            raise Refused("tls_failed", f"TLS handshake failed ({e.reason or type(e).__name__})") from None
        except (ConnectionClosed, InvalidHandshake, EOFError):
            # TLS 1.3 settles the client certificate after the client's handshake: a
            # refused certificate shows up as the server closing the connection.
            raise Refused("handshake_failed", f"the {peer} closed the connection during the handshake "
                          "(client certificate not accepted?)") from None
        except OSError as e:
            # The message carries the host; keep the class only. Not a ConnectionLost:
            # an unreachable peer ends the run instead of failing table after table.
            raise SyncError(f"cannot reach the {peer} ({type(e).__name__})") from None
        return WsChannel(ws)

    return open_channel


# ---------------------------------------------------------------------------
# Listening side (a push sink, or a serving source)
# ---------------------------------------------------------------------------

def server_tls(cert_file: str, key_file: str, client_ca_file: str | None) -> ssl.SSLContext:
    """Server TLS; with a client CA, also require a client certificate from it (mutual)."""
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ctx.minimum_version = ssl.TLSVersion.TLSv1_2
    ctx.load_cert_chain(cert_file, key_file)
    # No TLS 1.3 session tickets: nothing resumes a session here, and on server-only TLS the
    # tickets written after the handshake intermittently left the request unread until the
    # opening timeout (about 1 connection in 10, on loopback).
    ctx.num_tickets = 0
    if client_ca_file:
        ctx.verify_mode = ssl.CERT_REQUIRED
        ctx.load_verify_locations(cafile=client_ca_file)
    return ctx


def make_server(
    sink: Any,
    host: str,
    port: int,
    token: TokenStore | str,
    tls: ssl.SSLContext | None,
    max_frame: int = MAX_FRAME,
    limiter: FailedAuthLimiter | None = None,
    upstream_cert: UpstreamCertCheck | None = None,
):
    """Build a sink's WebSocket server. `tls` None means TLS ends upstream (the ingress)."""
    return make_channel_server(lambda ch: serve_sink(ch, sink), host, port, token, tls, max_frame, limiter,
                               upstream_cert)


class FailedAuthLimiter:
    """Refuse a client that keeps presenting a wrong token, and everyone when many do.

    A client with `per_client` failures within `window` seconds is refused (429) for
    `lockout` seconds, before its token is looked at, so a right token does not end the
    lockout early. With `global_max` failures within `window` across all clients, every
    request is refused until the window drains: a spread-out guess cannot outrun it, at the
    price of locking out the right token too for that long. A client is its address: the
    socket's peer when TLS ends here, the ingress's X-Real-IP / X-Forwarded-For when it ends
    upstream (the ingress overwrites them; nothing else can reach the pod). Addresses are
    kept in memory only and never logged.
    """

    MAX_TRACKED = 10_000

    def __init__(
        self,
        per_client: int = 5,
        window: float = 60,
        lockout: float = 300,
        global_max: int = 50,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.per_client, self.window, self.lockout, self.global_max = per_client, window, lockout, global_max
        self._clock = clock
        self._lock = threading.Lock()
        self._failures: dict[str, deque[float]] = {}
        self._locked_until: dict[str, float] = {}
        self._all: deque[float] = deque()

    def _prune(self, now: float) -> None:
        while self._all and now - self._all[0] > self.window:
            self._all.popleft()
        for key in [k for k, until in self._locked_until.items() if until <= now]:
            del self._locked_until[key]
        for key in [k for k, q in self._failures.items() if not q or now - q[-1] > self.window]:
            del self._failures[key]

    def retry_after(self, client: str) -> int:
        """Seconds the client must wait, or 0 when it may try."""
        with self._lock:
            now = self._clock()
            self._prune(now)
            wait = self._locked_until.get(client, now) - now
            if len(self._all) >= self.global_max:
                wait = max(wait, self.window - (now - self._all[0]))
            return max(0, int(wait + 0.999))

    def failed(self, client: str) -> None:
        with self._lock:
            now = self._clock()
            self._prune(now)
            self._all.append(now)
            if client not in self._failures and len(self._failures) >= self.MAX_TRACKED:
                return  # still counted globally; the global cap bounds a flood of addresses
            q = self._failures.setdefault(client, deque())
            q.append(now)
            while q and now - q[0] > self.window:
                q.popleft()
            if len(q) >= self.per_client:
                self._locked_until[client] = now + self.lockout
                q.clear()


def _client_key(connection: ServerConnection, request: Any, upstream: bool) -> str:
    if upstream:
        real = request.headers.get("X-Real-IP")
        if real:
            return real.strip()
        fwd = request.headers.get("X-Forwarded-For")
        if fwd:
            return fwd.split(",")[0].strip()
    addr = connection.remote_address
    return str(addr[0]) if addr else "?"


def make_channel_server(
    serve_channel: Callable[[Channel], Any],
    host: str,
    port: int,
    token: TokenStore | str,
    tls: ssl.SSLContext | None,
    max_frame: int = MAX_FRAME,
    limiter: FailedAuthLimiter | None = None,
    upstream_cert: UpstreamCertCheck | None = None,
):
    """Serve `/sync/v1` behind the checks, one channel per connection."""
    tokens = TokenStore.of(token)
    limiter = limiter or FailedAuthLimiter()
    if upstream_cert is not None and tls is not None:
        raise SyncError("a forwarded client certificate is checked only when TLS ends upstream")

    def process_request(connection: ServerConnection, request: Any):
        if request.path != ROUTE:
            return connection.respond(404, "not found\n")
        client = _client_key(connection, request, upstream=tls is None)
        wait = limiter.retry_after(client)
        if wait:
            log("REFUSED [rate_limited]: too many failed attempts")
            response = connection.respond(429, "too many failed attempts\n")
            response.headers["Retry-After"] = str(wait)
            return response
        if upstream_cert is not None and not upstream_cert.verify(request.headers.get(upstream_cert.header)):
            limiter.failed(client)
            log("REFUSED [client_cert_unverified]: the ingress forwarded no client certificate from the sync CA")
            return connection.respond(403, "no verified client certificate\n")
        holder = tokens.match(request.headers.get("Authorization"))
        if holder is None:
            limiter.failed(client)
            log("REFUSED [auth_failed]: missing or wrong token")
            return connection.respond(401, "unauthorized\n")
        log(f"accepted: token holder {holder}")
        return None

    def handler(connection: ServerConnection) -> None:
        ch = WsChannel(connection)
        try:
            serve_channel(ch)
        finally:
            ch.close()

    return serve(
        handler,
        host,
        port,
        ssl=tls,
        process_request=process_request,
        max_size=max_frame + 1024,
        ping_interval=PING_INTERVAL,
        ping_timeout=PING_INTERVAL,
        server_header=None,
        logger=_QUIET,
    )


def start_health(host: str, port: int) -> http.server.ThreadingHTTPServer:
    class Health(http.server.BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            ok = self.path == "/healthz"
            self.send_response(200 if ok else 404)
            self.end_headers()
            self.wfile.write(b"ok\n" if ok else b"not found\n")

        def log_message(self, *args: Any) -> None:  # no client addresses in the logs
            pass

    server = http.server.ThreadingHTTPServer((host, port), Health)
    threading.Thread(target=server.serve_forever, name="health", daemon=True).start()
    return server


def read_token(path: str) -> str:
    token = _read(path)
    if len(token) < 32:
        raise SyncError("the token file holds fewer than 32 characters")
    return token
