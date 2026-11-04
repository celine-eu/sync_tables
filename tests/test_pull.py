"""The pull edge: the local sink dials a serving source (dev -> local without a port-forward).

SQLite on both sides with fake identities; a loopback for the conversation, and a real
TLS socket and WebSocket on 127.0.0.1 for the transport. No Postgres.
"""
from __future__ import annotations

import os
import socket
import tempfile
import threading
from datetime import datetime, timedelta

import pytest
from sqlalchemy import MetaData, Table, create_engine, text

from sync_tables import certs
from sync_tables.config import TableSpec
from sync_tables.envs import Identity
from sync_tables.log import Refused
from sync_tables.protocol import loopback_pair
from sync_tables.pull import SourceServer, pull
from sync_tables.sink import Sink
from sync_tables.wss import channel_factory, client_tls, make_channel_server, server_tls

DEV = Identity("dev", "1111")
LOCAL = Identity("local", "2222")
EDGE = ("dev", "local")
SPEC = TableSpec("main", "readings", date_column="ts")


def engine():
    path = os.path.join(tempfile.mkdtemp(prefix="sync-tables-test-"), "db.sqlite")
    return create_engine(f"sqlite:///{path}", connect_args={"check_same_thread": False, "timeout": 10})


def source_with_rows(n=10, start=0):
    src = engine()
    with src.begin() as c:
        c.execute(text("CREATE TABLE readings (id INTEGER PRIMARY KEY, ts DATETIME NOT NULL, value TEXT)"))
    add_rows(src, range(start, start + n))
    return src


def add_rows(src, ids):
    t = Table("readings", MetaData(), autoload_with=src)
    with src.begin() as c:
        c.execute(t.insert(), [{"id": i, "ts": datetime(2026, 1, 1) + timedelta(hours=i), "value": f"v{i}"} for i in ids])


def rows(eng):
    with eng.connect() as c:
        return [tuple(r) for r in c.execute(text("SELECT id, datetime(ts), value FROM readings ORDER BY id"))]


def served(server: SourceServer):
    """A channel factory whose every channel is answered by `server` on its own thread."""

    def open_channel():
        here, there = loopback_pair()

        def run():
            try:
                server.serve(there)
            finally:
                there.close()

        threading.Thread(target=run, daemon=True).start()
        return here

    return open_channel


def local_sink(tgt=None, identity=LOCAL):
    tgt = tgt or engine()
    return tgt, Sink(tgt, ["main"], identify=lambda c: identity, log_summaries=True)


def test_pull_round_trip_then_incremental():
    src = source_with_rows(10)
    tgt, sink = local_sink()
    server = SourceServer(src, ["main"], identify=lambda c: DEV)
    r = pull([SPEC], EDGE, served(server), sink)
    assert r.ok == ["main.readings"] and not r.failed
    assert rows(tgt) == rows(src)

    add_rows(src, range(10, 15))
    r = pull([SPEC], EDGE, served(server), sink)
    assert r.ok == ["main.readings"]
    assert rows(tgt) == rows(src) and len(rows(tgt)) == 15


def test_pull_with_parallelism_and_a_missing_table():
    src = source_with_rows(5)
    with src.begin() as c:
        c.execute(text("CREATE TABLE other (id INTEGER PRIMARY KEY, name TEXT)"))
        c.execute(text("INSERT INTO other VALUES (1, 'a'), (2, 'b')"))
    tgt, sink = local_sink()
    server = SourceServer(src, ["main"], identify=lambda c: DEV)
    specs = [SPEC, TableSpec("main", "other"), TableSpec("main", "absent")]
    r = pull(specs, EDGE, served(server), sink, parallelism=2)
    assert sorted(r.ok) == ["main.other", "main.readings"]
    assert r.skipped == ["main.absent"] and not r.failed


def test_a_view_is_recreated_after_its_tables():
    src = source_with_rows(3)
    with src.begin() as c:
        c.execute(text("CREATE VIEW recent AS SELECT id, value FROM readings WHERE id > 0"))
    tgt, sink = local_sink()
    server = SourceServer(src, ["main"], identify=lambda c: DEV)
    r = pull([TableSpec("main", "recent"), SPEC], EDGE, served(server), sink)
    assert r.ok == ["main.readings", "main.recent"]
    with tgt.connect() as c:
        assert c.execute(text("SELECT count(*) FROM recent")).scalar_one() == 2


def test_a_table_the_source_cannot_read_fails_without_looping():
    src = source_with_rows(3)
    tgt, sink = local_sink()
    server = SourceServer(src, ["main"], identify=lambda c: DEV)
    r = pull([TableSpec("main", "readings", date_column="nope")], EDGE, served(server), sink)
    assert r.failed == ["main.readings"] and not r.ok


def test_the_source_reads_only_its_allowlist():
    src = source_with_rows(3)
    _, sink = local_sink()
    server = SourceServer(src, ["somewhere_else"], identify=lambda c: DEV)
    with pytest.raises(Refused) as e:
        pull([SPEC], EDGE, served(server), sink)
    assert e.value.code == "schema_not_allowed"


@pytest.mark.parametrize("edge,code", [
    (("dev", "prod"), "edge_not_allowed"),     # prod is not in --serve-to (0.8.0: declared, not compiled)
    (("local", "dev"), "edge_not_allowed"),
])
def test_the_source_refuses_an_edge_that_does_not_pull(edge, code):
    src = source_with_rows(3)
    _, sink = local_sink()
    server = SourceServer(src, ["main"], identify=lambda c: DEV, serve_to={"local"})
    with pytest.raises(Refused) as e:
        pull([SPEC], edge, served(server), sink)
    assert e.value.code == code


def test_the_source_refuses_when_it_is_not_the_edge_start():
    src = source_with_rows(3)
    _, sink = local_sink()
    server = SourceServer(src, ["main"], identify=lambda c: Identity("prod", "9999"))
    with pytest.raises(Refused) as e:
        pull([SPEC], EDGE, served(server), sink)
    assert e.value.code == "marker_mismatch"


def test_the_sink_refuses_a_target_that_is_not_local():
    src = source_with_rows(3)
    tgt, sink = local_sink(identity=Identity("prod", "3333"))
    server = SourceServer(src, ["main"], identify=lambda c: DEV)
    with pytest.raises(Refused) as e:
        pull([SPEC], EDGE, served(server), sink)
    assert e.value.code == "marker_mismatch"
    with tgt.connect() as c:
        assert c.execute(text("SELECT count(*) FROM sqlite_master WHERE name = 'readings'")).scalar_one() == 0


def test_the_sink_refuses_the_same_cluster():
    src = source_with_rows(3)
    _, sink = local_sink(identity=Identity("local", DEV.system_identifier))
    server = SourceServer(src, ["main"], identify=lambda c: DEV)
    with pytest.raises(Refused) as e:
        pull([SPEC], EDGE, served(server), sink)
    assert e.value.code == "same_cluster"


def test_a_malformed_request_is_refused():
    src = source_with_rows(3)
    server = SourceServer(src, ["main"], identify=lambda c: DEV)
    from sync_tables.protocol import PROTOCOL_VERSION, recv, send

    ch = served(server)()
    send(ch, {"t": "request", "v": PROTOCOL_VERSION, "edge": list(EDGE), "phase": "run",
              "tables": [{"schema": "main", "table": "readings", "cutoff": "2d"}], "only": ["main.readings"]})
    with pytest.raises(Refused) as e:
        recv(ch, "offer")
    assert e.value.code == "bad_message"


# ---------------------------------------------------------------------------
# Over WSS with private mTLS
# ---------------------------------------------------------------------------

def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture(scope="module")
def pki(tmp_path_factory):
    out = str(tmp_path_factory.mktemp("pki"))
    certs.make_ca(out)
    certs.make_server(out, ["localhost"])
    certs.make_client(out, "dev-laptop")
    certs.make_token(out)
    return out


def token_of(out):
    with open(os.path.join(out, "token")) as f:
        return f.read().strip()


def test_pull_over_mtls(pki):
    src = source_with_rows(10)
    tgt, sink = local_sink()
    server_role = SourceServer(src, ["main"], identify=lambda c: DEV)
    port = free_port()
    server = make_channel_server(
        server_role.serve, "127.0.0.1", port, token_of(pki),
        server_tls(f"{pki}/server.crt", f"{pki}/server.key", f"{pki}/ca.crt"),
    )
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        tls = client_tls(f"{pki}/ca.crt", f"{pki}/client-dev-laptop.crt", f"{pki}/client-dev-laptop.key")
        open_channel = channel_factory(f"wss://localhost:{port}", tls, token_of(pki), peer="source")
        for _ in range(2):
            r = pull([SPEC], EDGE, open_channel, sink)
            assert r.ok == ["main.readings"] and not r.failed
        assert rows(tgt) == rows(src)

        bad = channel_factory(f"wss://localhost:{port}", tls, "x" * 40, peer="source")
        with pytest.raises(Refused) as e:
            pull([SPEC], EDGE, bad, sink)
        assert e.value.code == "auth_failed" and "source" in e.value.message
    finally:
        server.shutdown()


# ---------------------------------------------------------------------------
# The CLI picks the transport from the URL given
# ---------------------------------------------------------------------------

def test_a_source_url_on_a_push_edge_is_refused(capsys, monkeypatch, tmp_path):
    from sync_tables.__main__ import main
    from test_exchange import CONFIG_YAML

    monkeypatch.chdir(tmp_path)
    (tmp_path / "config.yaml").write_text(CONFIG_YAML)

    assert main(["--from", "dev", "--to", "prod", "--source-url", "wss://source.example.org"]) == 2
    assert main(["--from", "dev", "--to", "local", "--source-url", "wss://a.example.org",
                 "--sink-url", "wss://b.example.org"]) == 2
    assert capsys.readouterr().out.count("transport_mismatch") == 2


def test_a_pull_needs_its_client_material(monkeypatch, capsys, tmp_path):
    from sync_tables.__main__ import main

    monkeypatch.setattr("sync_tables.__main__.load_dotenv", lambda **k: None)  # not this checkout's .env
    for v in ("SYNC_TLS_CA", "SYNC_TLS_CERT", "SYNC_TLS_KEY", "SYNC_TOKEN_FILE"):
        monkeypatch.delenv(v, raising=False)
    monkeypatch.chdir(tmp_path)  # a config.yaml with the edges only: the built-in defaults otherwise
    (tmp_path / "config.yaml").write_text("edges:\n  - {from: dev, to: local, transports: [pull]}\n")
    (tmp_path / "profile.yaml").write_text("sources:\n  - {schema: main, table: readings}\n")
    assert main(["--from", "dev", "--to", "local", "--profile", "profile.yaml",
                 "--source-url", "wss://source.example.org"]) == 2
    assert "a pull needs" in capsys.readouterr().out
