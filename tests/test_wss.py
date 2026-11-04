"""WSS with private mTLS and a token, on localhost (plan W7–W10).

A real TLS socket and a real WebSocket, with SQLite behind the sink and fake identities.
No network beyond 127.0.0.1, no Postgres.
"""
from __future__ import annotations

import os
import socket
import stat
import tempfile
import threading
import time
import urllib.request
from datetime import datetime, timedelta

import pytest
from sqlalchemy import MetaData, Table, create_engine, text

from sync_tables import certs, wss
from sync_tables.config import TableSpec
from sync_tables.envs import Identity
from sync_tables.log import Refused, SyncError
from sync_tables.sink import Sink
from sync_tables.source import Source
from sync_tables.wss import channel_factory, client_tls, make_server, server_tls, start_health

DEV = Identity("dev", "1111")
PROD = Identity("prod", "3333")
SPEC = TableSpec("main", "readings", date_column="ts")


def engine():
    path = os.path.join(tempfile.mkdtemp(prefix="sync-tables-test-"), "db.sqlite")
    return create_engine(f"sqlite:///{path}", connect_args={"check_same_thread": False, "timeout": 10})


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture(scope="module")
def pki(tmp_path_factory):
    out = str(tmp_path_factory.mktemp("pki"))
    certs.make_ca(out)
    certs.make_server(out, ["localhost"])
    certs.make_client(out, "source")
    certs.make_token(out)
    other = str(tmp_path_factory.mktemp("other-pki"))
    certs.make_ca(other)
    certs.make_client(other, "intruder")
    return out, other


def token_of(out):
    with open(os.path.join(out, "token")) as f:
        return f.read().strip()


@pytest.fixture
def sink_server(pki):
    out, _ = pki
    tgt = engine()
    sink = Sink(tgt, ["main"], identify=lambda c: PROD, log_summaries=True)
    port = free_port()
    server = make_server(
        sink, "127.0.0.1", port, token_of(out),
        server_tls(f"{out}/server.crt", f"{out}/server.key", f"{out}/ca.crt"),
    )
    threading.Thread(target=server.serve_forever, daemon=True).start()
    yield f"wss://localhost:{port}", tgt
    server.shutdown()


def source_with_rows(n=10):
    src = engine()
    with src.begin() as c:
        c.execute(text("CREATE TABLE readings (id INTEGER PRIMARY KEY, ts DATETIME NOT NULL, value TEXT)"))
    t = Table("readings", MetaData(), autoload_with=src)
    with src.begin() as c:
        c.execute(t.insert(), [{"id": i, "ts": datetime(2026, 1, 1) + timedelta(hours=i), "value": f"v{i}"} for i in range(n)])
    return src


def rows(eng):
    with eng.connect() as c:
        return [tuple(r) for r in c.execute(text("SELECT id, datetime(ts), value FROM readings ORDER BY id"))]


def test_push_over_mtls_round_trip_and_rerun(pki, sink_server):
    out, _ = pki
    url, tgt = sink_server
    src = source_with_rows()
    tls = client_tls(f"{out}/ca.crt", f"{out}/client-source.crt", f"{out}/client-source.key")
    source = Source(src, ("dev", "prod"), identify=lambda c: DEV)
    for _ in range(2):
        r = source.run([SPEC], channel_factory(url, tls, token_of(out)))
        assert r.ok == ["main.readings"] and not r.failed
    assert rows(tgt) == rows(src)


class SlowSink(Sink):
    """Applies each frame slower than the keepalive budget allows a backlog to wait."""

    def __init__(self, *args, delay: float, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.delay = delay

    def begin_table(self, desc, plan):
        write = super().begin_table(desc, plan)
        apply = write.write

        def slow(rows):
            time.sleep(self.delay)
            apply(rows)

        write.write = slow
        return write


def test_a_push_to_a_sink_slower_than_the_source_completes(pki, monkeypatch):
    """REQ-0001: one row per frame, each applied in 0.1 s, against a 0.2 + 0.2 s keepalive.

    Without flow control the source outruns the sink, websockets stops reading the sink's
    socket once 16 frames wait, and the source's ping goes unanswered (F1)."""
    monkeypatch.setattr(wss, "PING_INTERVAL", 0.2)
    out, _ = pki
    tgt = engine()
    sink = SlowSink(tgt, ["main"], identify=lambda c: PROD, log_summaries=True, delay=0.1)
    port = free_port()
    server = make_server(
        sink, "127.0.0.1", port, token_of(out),
        server_tls(f"{out}/server.crt", f"{out}/server.key", f"{out}/ca.crt"),
    )
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        src = source_with_rows(40)
        tls = client_tls(f"{out}/ca.crt", f"{out}/client-source.crt", f"{out}/client-source.key")
        source = Source(src, ("dev", "prod"), identify=lambda c: DEV, batch_size=1)
        r = source.run([SPEC], channel_factory(f"wss://localhost:{port}", tls, token_of(out)))
        assert r.ok == ["main.readings"] and not r.failed
        assert rows(tgt) == rows(src)
    finally:
        server.shutdown()


def test_a_wrong_token_is_refused_before_the_websocket_opens(pki, sink_server):
    out, _ = pki
    url, tgt = sink_server
    tls = client_tls(f"{out}/ca.crt", f"{out}/client-source.crt", f"{out}/client-source.key")
    with pytest.raises(Refused) as e:
        channel_factory(url, tls, "w" * 64)()
    assert e.value.code == "auth_failed"


def test_a_client_certificate_from_another_ca_is_refused(pki, sink_server):
    out, other = pki
    url, _ = sink_server
    tls = client_tls(f"{out}/ca.crt", f"{other}/client-intruder.crt", f"{other}/client-intruder.key")
    with pytest.raises((Refused, SyncError)):
        channel_factory(url, tls, token_of(out))()


def test_no_client_certificate_is_refused(pki, sink_server):
    import ssl

    out, _ = pki
    url, _ = sink_server
    tls = ssl.create_default_context(cafile=f"{out}/ca.crt")
    with pytest.raises((Refused, SyncError)):
        channel_factory(url, tls, token_of(out))()


def test_the_client_trusts_only_the_sync_ca(pki, sink_server):
    """A server certificate from another CA (e.g. a public one) is not accepted."""
    out, other = pki
    url, _ = sink_server
    tls = client_tls(f"{other}/ca.crt", f"{out}/client-source.crt", f"{out}/client-source.key")
    with pytest.raises(Refused) as e:
        channel_factory(url, tls, token_of(out))()
    assert e.value.code == "tls_failed"


def test_other_paths_are_404(pki, sink_server):
    import ssl

    out, _ = pki
    url, _ = sink_server
    ctx = ssl.create_default_context(cafile=f"{out}/ca.crt")
    ctx.load_cert_chain(f"{out}/client-source.crt", f"{out}/client-source.key")
    https = url.replace("wss://", "https://")
    with pytest.raises(urllib.error.HTTPError) as e:
        urllib.request.urlopen(f"{https}/admin", context=ctx, timeout=5)
    assert e.value.code == 404
    e.value.close()


def test_health_is_plain_and_separate():
    port = free_port()
    server = start_health("127.0.0.1", port)
    try:
        assert urllib.request.urlopen(f"http://127.0.0.1:{port}/healthz", timeout=5).read() == b"ok\n"
    finally:
        server.shutdown()


def test_sink_refuses_the_wrong_edge_over_wss(pki, sink_server):
    """A sink marked prod refuses an offer for dev -> local, whoever dials."""
    out, _ = pki
    url, _ = sink_server
    tls = client_tls(f"{out}/ca.crt", f"{out}/client-source.crt", f"{out}/client-source.key")
    source = Source(source_with_rows(), ("dev", "local"), identify=lambda c: DEV)
    with pytest.raises(Refused) as e:
        source.run([SPEC], channel_factory(url, tls, token_of(out)))
    assert e.value.code == "marker_mismatch"


def test_no_log_line_carries_the_url_or_the_token(pki, sink_server, capsys):
    out, _ = pki
    url, _ = sink_server
    tls = client_tls(f"{out}/ca.crt", f"{out}/client-source.crt", f"{out}/client-source.key")
    Source(source_with_rows(), ("dev", "prod"), identify=lambda c: DEV).run([SPEC], channel_factory(url, tls, token_of(out)))
    with pytest.raises(Refused):
        channel_factory(url, tls, "w" * 64)()
    captured = capsys.readouterr()
    text_out = captured.out + captured.err
    assert token_of(out) not in text_out
    assert url.split("//")[1] not in text_out and "127.0.0.1" not in text_out
    assert "v3" not in text_out  # a row value


# ---------------------------------------------------------------------------
# `sync-tables certs`
# ---------------------------------------------------------------------------

def test_certs_are_idempotent_private_and_quiet(tmp_path, capsys):
    out = str(tmp_path / "pki")
    assert certs.main(["all", "--out", out, "--dns", "sink.example.org", "--name", "source"]) == 0
    files = sorted(os.listdir(out))
    assert files == ["ca.crt", "ca.key", "client-source.crt", "client-source.key", "server.crt", "server.key", "token"]
    for f in ("ca.key", "server.key", "client-source.key", "token"):
        assert stat.S_IMODE(os.stat(os.path.join(out, f)).st_mode) == 0o600
    before = {f: open(os.path.join(out, f), "rb").read() for f in files}

    assert certs.main(["all", "--out", out, "--dns", "sink.example.org", "--name", "source"]) == 0
    assert {f: open(os.path.join(out, f), "rb").read() for f in files} == before

    printed = capsys.readouterr().out
    assert "PRIVATE KEY" not in printed
    assert before["token"].decode().strip() not in printed


def test_a_leaf_needs_the_ca(tmp_path):
    assert certs.main(["client", "--out", str(tmp_path), "--name", "x"]) == 1


# ---------------------------------------------------------------------------
# Failed token attempts
# ---------------------------------------------------------------------------

class Clock:
    def __init__(self):
        self.t = 1000.0

    def __call__(self):
        return self.t


def test_a_client_is_locked_out_after_repeated_failures_and_released_later():
    from sync_tables.wss import FailedAuthLimiter

    clock = Clock()
    lim = FailedAuthLimiter(per_client=3, window=60, lockout=300, global_max=100, clock=clock)
    for _ in range(2):
        lim.failed("a")
    assert lim.retry_after("a") == 0
    lim.failed("a")
    assert lim.retry_after("a") == 300
    assert lim.retry_after("b") == 0  # another client is not affected
    clock.t += 301
    assert lim.retry_after("a") == 0


def test_failures_outside_the_window_do_not_add_up():
    from sync_tables.wss import FailedAuthLimiter

    clock = Clock()
    lim = FailedAuthLimiter(per_client=3, window=60, lockout=300, global_max=100, clock=clock)
    for _ in range(5):
        lim.failed("a")
        clock.t += 31
    assert lim.retry_after("a") == 0


def test_many_clients_failing_refuse_everyone_until_the_window_drains():
    from sync_tables.wss import FailedAuthLimiter

    clock = Clock()
    lim = FailedAuthLimiter(per_client=100, window=60, lockout=300, global_max=10, clock=clock)
    for i in range(10):
        lim.failed(f"c{i}")
    assert lim.retry_after("someone-else") == 60
    clock.t += 61
    assert lim.retry_after("someone-else") == 0


def test_repeated_wrong_tokens_get_429_and_then_even_the_right_token_waits(pki):
    from sync_tables.wss import FailedAuthLimiter

    out, _ = pki
    sink = Sink(engine(), ["main"], identify=lambda c: PROD, log_summaries=True)
    port = free_port()
    server = make_server(
        sink, "127.0.0.1", port, token_of(out),
        server_tls(f"{out}/server.crt", f"{out}/server.key", f"{out}/ca.crt"),
        limiter=FailedAuthLimiter(per_client=2, window=60, lockout=300),
    )
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        url = f"wss://localhost:{port}"
        tls = client_tls(f"{out}/ca.crt", f"{out}/client-source.crt", f"{out}/client-source.key")
        for _ in range(2):
            with pytest.raises(Refused) as e:
                channel_factory(url, tls, "w" * 64)()
            assert e.value.code == "auth_failed"
        with pytest.raises(Refused) as e:
            channel_factory(url, tls, token_of(out))()
        assert e.value.code == "rate_limited"
    finally:
        server.shutdown()


def test_a_ca_names_its_purpose(tmp_path):
    from cryptography import x509

    out = str(tmp_path / "pull")
    assert certs.main(["ca", "--out", out, "--purpose", "pull"]) == 0
    with open(os.path.join(out, "ca.crt"), "rb") as f:
        assert "sync-tables pull CA" in x509.load_pem_x509_certificate(f.read()).subject.rfc4514_string()
