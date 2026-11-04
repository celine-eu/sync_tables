"""The exchange layer on mocks: in-memory SQLite, an in-process loopback, fake identities.

No Postgres and no network (plan W15). Postgres-only guarantees are in test_pg.py.
"""
from __future__ import annotations

import itertools
import os
import tempfile
from datetime import datetime, timedelta

import pytest
from sqlalchemy import MetaData, Table, create_engine, text

from sync_tables.config import TableSpec
from sync_tables.envs import EdgePolicy, Identity, parse_marker
from sync_tables.catalog import check_type_sql
from sync_tables.log import Refused, SyncError
from sync_tables.codec import unpack
from sync_tables.protocol import (
    PROTOCOL_VERSION,
    WINDOW,
    ConnectionLost,
    loopback_pair,
    loopback_to,
    recv,
    send,
    serve_sink,
)
from sync_tables.sink import Sink
from sync_tables.source import Source

DEV = Identity("dev", "1111")
LOCAL = Identity("local", "2222")
PROD = Identity("prod", "3333")


def engine():
    # A file, not :memory: — an in-memory database exists once per connection, and the
    # sink works on its own threads and connections, as it does against Postgres.
    path = os.path.join(tempfile.mkdtemp(prefix="sync-tables-test-"), "db.sqlite")
    return create_engine(f"sqlite:///{path}", connect_args={"check_same_thread": False, "timeout": 10})


def make_pair(src_ident=DEV, tgt_ident=LOCAL, edge=("dev", "local"), allowed=("main",), **sink_kw):
    src, tgt = engine(), engine()
    source = Source(src, edge, identify=lambda c: src_ident, batch_size=3)
    sink = Sink(tgt, allowed, identify=lambda c: tgt_ident, **sink_kw)
    return src, tgt, source, sink


def rows(eng, sql):
    with eng.connect() as c:
        return [tuple(r) for r in c.execute(text(sql)).all()]


def seed(eng, n=7, start=datetime(2026, 1, 1)):
    with eng.begin() as c:
        c.execute(text("CREATE TABLE readings (id INTEGER PRIMARY KEY, ts DATETIME NOT NULL, value TEXT)"))
    insert_readings(eng, [(i, start + timedelta(hours=i), f"v{i}") for i in range(n)])


def insert_readings(eng, values):
    """Typed inserts: SQLite stores timestamps as text, in the format of whoever writes."""
    t = Table("readings", MetaData(), autoload_with=eng)
    with eng.begin() as c:
        c.execute(t.insert(), [{"id": i, "ts": ts, "value": v} for i, ts, v in values])


INCR = TableSpec(schema="main", table="readings", date_column="ts")
# SQLite keeps timestamps as text; compare them as timestamps.
ALL = "SELECT id, datetime(ts), value FROM readings ORDER BY id"


# ---------------------------------------------------------------------------
# S2: edges. Since 0.8.0 they are declared by configuration (plan decision 8): the 0.6.0
# edges are a config here, and the cases below check them through it. The compiled rule
# (only dev writes to prod) answers prod_only_from_dev; tests/test_edges.py covers the rest.
# ---------------------------------------------------------------------------

ENVIRONMENTS = ("local", "dev", "prod", "staging", "preprod")
EDGES = {
    ("dev", "local"): ("pull", "direct"),
    ("dev", "prod"): ("push",),
    ("dev", "staging"): ("push",),
    ("dev", "preprod"): ("push",),
}
POLICY = EdgePolicy.from_config([{"from": a, "to": b, "transports": list(t)} for (a, b), t in EDGES.items()])
CONFIG_YAML = "edges:\n" + "".join(
    f"  - {{from: {a}, to: {b}, transports: [{', '.join(t)}]}}\n" for (a, b), t in EDGES.items()
)


def check_edge(a, b):
    return POLICY.transports(a, b)


def refusal(a, b):
    return "prod_only_from_dev" if a != b and b == "prod" and a != "dev" else "edge_not_allowed"


@pytest.mark.parametrize("a,b", [p for p in itertools.product(ENVIRONMENTS, repeat=2) if p not in EDGES])
def test_every_other_edge_is_refused(a, b):
    with pytest.raises(Refused) as e:
        check_edge(a, b)
    assert e.value.code == refusal(a, b)


def test_the_reverse_of_each_allowed_edge_is_refused():
    for a, b in EDGES:
        with pytest.raises(Refused):
            check_edge(b, a)


def test_unknown_environment_is_refused():
    with pytest.raises(Refused) as e:
        check_edge("dev", "Qa")
    assert e.value.code == "unknown_environment"


def test_the_edges_are_exactly_these():
    """The warehouse (`dev`) feeds every environment; nothing else moves rows. staging and
    preprod are push sinks like prod: read-only copies that send nothing back."""
    assert POLICY.declared == EDGES


@pytest.mark.parametrize("a,b", [
    ("staging", "prod"), ("preprod", "prod"), ("staging", "preprod"), ("preprod", "staging"),
    ("prod", "staging"), ("prod", "preprod"), ("staging", "dev"), ("preprod", "dev"),
    ("staging", "local"), ("preprod", "local"),
])
def test_staging_and_preprod_send_nothing_on(a, b):
    with pytest.raises(Refused) as e:
        check_edge(a, b)
    assert e.value.code == refusal(a, b)


@pytest.mark.parametrize("env", ["staging", "preprod"])
def test_staging_and_preprod_markers_are_read(env):
    assert parse_marker(f"celine:env={env}", "target") == env


def test_a_refused_edge_opens_no_connection(monkeypatch, capsys, tmp_path):
    import sync_tables.db as db
    from sync_tables.__main__ import main

    monkeypatch.chdir(tmp_path)
    (tmp_path / "config.yaml").write_text(CONFIG_YAML)

    def boom(*a, **k):
        raise AssertionError("a connection was opened")

    monkeypatch.setattr(db, "make_source_engine", boom)
    monkeypatch.setattr(db, "make_engine", boom)
    assert main(["--from", "local", "--to", "dev"]) == 2
    assert main(["--from", "prod", "--to", "dev"]) == 2
    assert "edge_not_allowed" in capsys.readouterr().out


def test_transport_is_compiled_per_edge(capsys, monkeypatch, tmp_path):
    from sync_tables.__main__ import main

    monkeypatch.chdir(tmp_path)
    (tmp_path / "config.yaml").write_text(CONFIG_YAML)

    assert main(["--from", "dev", "--to", "local", "--sink-url", "wss://sink.example.org"]) == 2
    assert main(["--from", "dev", "--to", "prod"]) == 2
    out = capsys.readouterr().out
    assert out.count("transport_mismatch") == 2


def test_a_dsn_is_not_a_push_target():
    from sync_tables.wss import channel_factory

    with pytest.raises(Refused) as e:
        channel_factory("postgresql://u@h/db", tls=None, token="x" * 40)  # type: ignore[arg-type]
    assert e.value.code == "transport_mismatch"


# ---------------------------------------------------------------------------
# S3: markers
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("comment,code", [
    (None, "marker_missing"),
    ("", "marker_missing"),
    ("celine:env=-qa", "marker_malformed"),
    ("env=dev", "marker_malformed"),
    ("celine:env=dev extra", "marker_malformed"),
    ("celine:env=DEV", "marker_malformed"),
])
def test_missing_or_malformed_marker_is_refused(comment, code):
    with pytest.raises(Refused) as e:
        parse_marker(comment, "target")
    assert e.value.code == code


def test_a_missing_local_marker_names_the_task(capsys):
    src, tgt, source, sink = make_pair()
    seed(src)

    def unmarked(c):
        raise Refused("marker_missing", "the target database carries no environment marker")

    sink._identify = unmarked
    with pytest.raises(Refused) as e:
        source.run([INCR], loopback_to(sink))
    assert e.value.code == "marker_missing"
    assert "celine:env=local" in e.value.message


def test_sink_refuses_when_its_own_marker_does_not_match_the_edge():
    src, tgt, source, sink = make_pair(tgt_ident=PROD)  # edge dev -> local, sink says prod
    seed(src)
    with pytest.raises(Refused) as e:
        source.run([INCR], loopback_to(sink))
    assert e.value.code == "marker_mismatch"
    assert rows(tgt, "SELECT name FROM sqlite_master") == []


def test_source_refuses_when_its_own_marker_does_not_match_the_edge():
    src, tgt, source, sink = make_pair(src_ident=LOCAL)
    seed(src)
    with pytest.raises(Refused) as e:
        source.run([INCR], loopback_to(sink))
    assert e.value.code == "marker_mismatch"


def test_sink_refuses_whatever_the_source_claims_about_itself():
    """A forged offer: right edge, wrong claim about the source's environment."""
    _, tgt, _, sink = make_pair()
    here, there = loopback_pair()
    import threading

    threading.Thread(target=serve_sink, args=(there, sink), daemon=True).start()
    send(here, {"t": "offer", "v": PROTOCOL_VERSION, "edge": ["dev", "local"], "env": "prod",
                "sysid": "9", "tables": [], "views": []})
    reply = recv(here)
    assert reply["t"] == "refuse" and reply["code"] == "marker_mismatch"


def test_source_refuses_a_sink_reporting_another_environment():
    """A sink (or something pretending to be one) that says it is not the destination."""
    src = engine()
    seed(src)
    source = Source(src, ("dev", "local"), identify=lambda c: DEV)
    here, there = loopback_pair()
    import threading

    def fake_sink():
        recv(there, "offer")
        send(there, {"t": "accept", "env": "dev", "sysid": "2222", "watermarks": {}})

    threading.Thread(target=fake_sink, daemon=True).start()
    with pytest.raises(Refused) as e:
        source.run([INCR], lambda: here)
    assert e.value.code == "marker_mismatch"


# ---------------------------------------------------------------------------
# S4: same cluster
# ---------------------------------------------------------------------------

def test_same_cluster_is_refused():
    src, tgt, source, sink = make_pair(src_ident=Identity("dev", "same"), tgt_ident=Identity("local", "same"))
    seed(src)
    with pytest.raises(Refused) as e:
        source.run([INCR], loopback_to(sink))
    assert e.value.code == "same_cluster"


# ---------------------------------------------------------------------------
# S5: the sink writes only inside its allowlist
# ---------------------------------------------------------------------------

def test_offer_outside_the_allowlist_is_refused():
    src, tgt, source, sink = make_pair(allowed=("mirror",))
    seed(src)
    with pytest.raises(Refused) as e:
        source.run([INCR], loopback_to(sink))
    assert e.value.code == "schema_not_allowed"


def test_table_message_outside_the_allowlist_is_refused_after_accept():
    """Even a source that passed the offer cannot send a table for another schema."""
    _, tgt, _, sink = make_pair(allowed=("main",))
    here, there = loopback_pair()
    import threading

    threading.Thread(target=serve_sink, args=(there, sink), daemon=True).start()
    send(here, {"t": "offer", "v": PROTOCOL_VERSION, "edge": ["dev", "local"], "env": "dev",
                "sysid": "1111", "tables": [], "views": []})
    assert recv(here)["t"] == "accept"
    desc = {"schema": "other", "name": "x", "columns": [{"name": "id", "type_sql": "integer", "nullable": False}],
            "primary_key": ["id"], "indexes": []}
    send(here, {"t": "table", "desc": desc, "plan": {"mode": "full", "truncate": True}})
    send(here, {"t": "rows", "columns": [[1, 2]]})
    assert recv(here)["t"] == "ack"
    send(here, {"t": "end"})
    reply = recv(here)
    assert reply["t"] == "err" and reply["code"] == "schema_not_allowed"


def test_an_index_method_outside_the_list_is_refused():
    """REQ-0003: the method is a word from a closed list, never SQL."""
    from sync_tables.catalog import TableDesc

    desc = {"schema": "main", "name": "x", "columns": [{"name": "id", "type_sql": "integer", "nullable": False}],
            "primary_key": ["id"], "indexes": [{"name": "i", "columns": ["id"], "unique": False,
                                                "method": "gist (id); DROP TABLE x; --"}]}
    with pytest.raises(SyncError):
        TableDesc.from_wire(desc)
    desc["indexes"][0]["method"] = "gist"
    assert TableDesc.from_wire(desc).indexes[0].method == "gist"


def test_a_sqlite_target_keeps_btree_indexes_and_skips_the_others(capsys):
    from sync_tables.catalog import ColumnDesc, IndexDesc, TableDesc

    _, tgt, _, sink = make_pair()
    desc = TableDesc("main", "shapes", [ColumnDesc("id", "integer", False), ColumnDesc("geom", "text", True)], ["id"],
                     [IndexDesc("shapes_geom", ["geom"], False, "gist"), IndexDesc("shapes_id_geom", ["id", "geom"], False, "btree")])
    sink.begin_table(desc, {"mode": "full", "truncate": True}).commit()
    assert rows(tgt, "SELECT name FROM sqlite_master WHERE type = 'index' AND tbl_name = 'shapes' ORDER BY name") == [("shapes_id_geom",)]
    assert "shapes_geom" in capsys.readouterr().out


def test_a_type_that_carries_sql_is_refused():
    _, tgt, _, sink = make_pair()
    here, there = loopback_pair()
    import threading

    threading.Thread(target=serve_sink, args=(there, sink), daemon=True).start()
    send(here, {"t": "offer", "v": PROTOCOL_VERSION, "edge": ["dev", "local"], "env": "dev",
                "sysid": "1111", "tables": [], "views": []})
    recv(here)
    desc = {"schema": "main", "name": "x", "columns": [{"name": "id", "type_sql": "integer); DROP TABLE y; --", "nullable": False}],
            "primary_key": [], "indexes": []}
    send(here, {"t": "table", "desc": desc, "plan": {"mode": "full", "truncate": True}})
    send(here, {"t": "end"})
    assert recv(here)["t"] == "err"
    assert rows(tgt, "SELECT name FROM sqlite_master") == []


@pytest.mark.parametrize("type_sql", [
    "integer", "text", "varchar[]", "numeric(10, 2)", "timestamp with time zone",
    "timestamp(6) without time zone", "double precision", "character varying(20)",
    "geometry(POINT,4326)", "geometry(Point, 4326)", "jsonb", '"public"."my_enum"', "int4[][]",
])
def test_plain_types_pass(type_sql):
    assert check_type_sql(type_sql) == type_sql


@pytest.mark.parametrize("type_sql", [
    "integer); DROP TABLE y; --",
    "text DEFAULT pg_sleep(5)",
    "text default (pg_catalog.pg_read_file(concat(chr(47),chr(101))))",
    "int CHECK (pg_sleep(1) IS NULL)",
    "int GENERATED ALWAYS AS (1) STORED",
    "text, evil int",
    "text) INHERITS (public.users",
    "int REFERENCES public.users",
    "text COLLATE C",
    "numeric(pg_sleep(1))",
    "text NOT NULL",
    "",
])
def test_types_that_carry_an_expression_or_a_clause_are_refused(type_sql):
    with pytest.raises(SyncError):
        check_type_sql(type_sql)


def test_wrong_protocol_version_is_refused():
    _, _, _, sink = make_pair()
    here, there = loopback_pair()
    import threading

    threading.Thread(target=serve_sink, args=(there, sink), daemon=True).start()
    send(here, {"t": "offer", "v": 99, "edge": ["dev", "local"], "env": "dev", "sysid": "1", "tables": [], "views": []})
    reply = recv(here)
    assert reply["t"] == "refuse" and reply["code"] == "protocol_version"


def test_a_peer_without_flow_control_is_refused():
    """REQ-0004: a 0.4.0 source (protocol 1, no acks) never streams into a 0.5.0 sink."""
    _, _, _, sink = make_pair()
    here, there = loopback_pair()
    import threading

    threading.Thread(target=serve_sink, args=(there, sink), daemon=True).start()
    send(here, {"t": "offer", "v": 1, "edge": ["dev", "local"], "env": "dev", "sysid": "1111", "tables": [], "views": []})
    reply = recv(here)
    assert reply["t"] == "refuse" and reply["code"] == "protocol_version"


def test_an_aborted_table_leaves_the_channel_in_step_for_the_next():
    """REQ-0005: the source reads the acks it is owed after an abort, then copies on.

    Acks left unread would be taken for the next table's, letting up to twice WINDOW
    frames travel unacknowledged: what the window exists to prevent."""
    src, tgt, source, sink = make_pair()
    seed(src, n=20)
    with src.begin() as c:
        c.execute(text("CREATE TABLE other (id INTEGER PRIMARY KEY, value TEXT)"))
        for i in range(30):
            c.execute(text("INSERT INTO other VALUES (:i, :v)"), {"i": i, "v": f"o{i}"})
    plan = source._table_plan

    def failing_plan(conn, spec, src_table, raw_wm, full_refresh):
        p, rows_iter, note = plan(conn, spec, src_table, raw_wm, full_refresh)
        if spec.table != "readings":
            return p, rows_iter, note

        def broken():
            yield from itertools.islice(rows_iter, 5)  # 5 batches of 3 rows: acks owed
            raise RuntimeError("source read failed")

        return p, broken(), note

    source._table_plan = failing_plan

    in_flight = {"now": 0, "max": 0}
    open_sink = loopback_to(sink)

    class Counting:
        """Rows sent minus acks read, as the transport sees them."""

        def __init__(self, ch):
            self.ch = ch

        def send_raw(self, data):
            if unpack(data)["t"] == "rows":
                in_flight["now"] += 1
                in_flight["max"] = max(in_flight["max"], in_flight["now"])
            self.ch.send_raw(data)

        def recv_raw(self):
            data = self.ch.recv_raw()
            if unpack(data)["t"] == "ack":
                in_flight["now"] -= 1
            return data

        def close(self):
            self.ch.close()

    r = source.run([INCR, TableSpec(schema="main", table="other")], lambda: Counting(open_sink()))
    assert r.failed == ["main.readings"] and r.ok == ["main.other"]
    assert len(rows(tgt, "SELECT id FROM other")) == 30
    assert in_flight["max"] <= WINDOW


# ---------------------------------------------------------------------------
# Copying, idempotency (W14), the window re-copy (S7/D21)
# ---------------------------------------------------------------------------

def test_round_trip_and_rerun_changes_nothing():
    src, tgt, source, sink = make_pair()
    seed(src)
    r = source.run([INCR, TableSpec("main", "readings_full")], loopback_to(sink))
    assert r.ok == ["main.readings"] and r.skipped == ["main.readings_full"]
    first = rows(tgt, ALL)
    assert first == rows(src, ALL)

    source.run([INCR], loopback_to(sink))
    assert rows(tgt, ALL) == first


def test_full_mode_reloads_and_is_idempotent():
    src, tgt, source, sink = make_pair()
    seed(src)
    full = TableSpec("main", "readings")
    for _ in range(2):
        source.run([full], loopback_to(sink))
    assert rows(tgt, ALL) == rows(src, ALL)


def _recompute(eng, since: datetime, suffix: str):
    with eng.begin() as c:
        c.execute(text("UPDATE readings SET value = value || :s WHERE ts >= :since"), {"s": suffix, "since": since.isoformat(" ")})


def test_window_recopy_converges_after_a_recompute():
    src, tgt, source, sink = make_pair()
    seed(src, n=48)  # two days, hourly
    spec = TableSpec("main", "readings", date_column="ts", lookback="1d")
    source.run([spec], loopback_to(sink))

    _recompute(src, datetime(2026, 1, 2, 6), "-recomputed")  # inside the last day
    source.run([spec], loopback_to(sink))
    assert rows(tgt, ALL) == rows(src, ALL)


def test_without_lookback_a_recompute_stays_stale():
    """Why lookback exists: a plain watermark never revisits recomputed rows."""
    src, tgt, source, sink = make_pair()
    seed(src, n=48)
    source.run([INCR], loopback_to(sink))
    _recompute(src, datetime(2026, 1, 2, 6), "-recomputed")
    source.run([INCR], loopback_to(sink))
    assert rows(tgt, ALL) != rows(src, ALL)


def test_window_recopy_removes_rows_deleted_inside_the_window():
    src, tgt, source, sink = make_pair()
    seed(src, n=48)
    spec = TableSpec("main", "readings", date_column="ts", lookback="1d")
    source.run([spec], loopback_to(sink))
    with src.begin() as c:
        c.execute(text("DELETE FROM readings WHERE id = 40"))
    source.run([spec], loopback_to(sink))
    assert rows(tgt, "SELECT id FROM readings WHERE id = 40") == []


# ---------------------------------------------------------------------------
# W10: a dropped connection rolls the table back; a retry converges
# ---------------------------------------------------------------------------

class DropAfter:
    """A channel that drops after `n` frames sent (or right after `end`, before the ack)."""

    def __init__(self, inner, n=None, after_end=False):
        self.inner, self.n, self.after_end, self.sent = inner, n, after_end, 0
        self.dropped = False

    def send_raw(self, data):
        if self.dropped:
            raise ConnectionLost("dropped")
        self.inner.send_raw(data)
        self.sent += 1
        from sync_tables.codec import unpack

        kind = unpack(data)["t"]
        if (self.n is not None and self.sent >= self.n and kind == "rows") or (self.after_end and kind == "end"):
            self.dropped = True
            self.inner.close()

    def recv_raw(self):
        if self.dropped:
            raise ConnectionLost("dropped")
        return self.inner.recv_raw()

    def close(self):
        self.inner.close()


def test_a_dropped_connection_leaves_the_target_table_as_it_was():
    src, tgt, source, sink = make_pair()
    seed(src, n=4)
    source.run([INCR], loopback_to(sink))
    before = rows(tgt, ALL)
    insert_readings(src, [(i, datetime(2026, 2, 1) + timedelta(hours=i), "new") for i in range(4, 20)])

    opened = loopback_to(sink)
    r = source.run([INCR], lambda: DropAfter(opened(), n=3), retries=0)
    assert r.failed == ["main.readings"]
    import time

    time.sleep(0.2)  # the sink's thread rolls back on its own
    assert rows(tgt, ALL) == before


def test_a_retry_on_a_fresh_connection_converges():
    src, tgt, source, sink = make_pair()
    seed(src, n=20)
    opened = loopback_to(sink)
    channels = iter([DropAfter(opened(), n=2)])
    r = source.run([INCR], lambda: next(channels, None) or opened(), retries=1)
    assert r.ok == ["main.readings"]
    assert rows(tgt, ALL) == rows(src, ALL)


def test_a_commit_never_acknowledged_converges_on_the_retry():
    src, tgt, source, sink = make_pair()
    seed(src, n=10)
    opened = loopback_to(sink)
    channels = iter([DropAfter(opened(), after_end=True)])
    r = source.run([INCR], lambda: next(channels, None) or opened(), retries=1)
    assert r.ok == ["main.readings"]
    assert rows(tgt, ALL) == rows(src, ALL)


def test_batches_split_to_fit_the_frame_limit():
    src, tgt, _, sink = make_pair()
    with src.begin() as c:
        c.execute(text("CREATE TABLE blobs (id INTEGER PRIMARY KEY, payload TEXT)"))
        for i in range(6):
            c.execute(text("INSERT INTO blobs VALUES (:i, :p)"), {"i": i, "p": "x" * 3000})
    source = Source(src, ("dev", "local"), identify=lambda c: DEV, batch_size=100, max_frame=8000)
    source.run([TableSpec("main", "blobs")], loopback_to(sink))
    assert rows(tgt, "SELECT count(*) FROM blobs") == [(6,)]


# ---------------------------------------------------------------------------
# Views (S7/D20)
# ---------------------------------------------------------------------------

def test_a_view_is_recreated_after_its_base_table_and_resolves():
    src, tgt, source, sink = make_pair()
    seed(src)
    with src.begin() as c:
        c.execute(text("CREATE VIEW late AS SELECT id, value FROM readings WHERE id >= 5"))
        c.execute(text("CREATE VIEW later AS SELECT id FROM late WHERE id >= 6"))
    # Listed views-first on purpose: the order comes from the dependencies.
    r = source.run([TableSpec("main", "later"), TableSpec("main", "late"), INCR], loopback_to(sink))
    assert r.ok == ["main.readings", "main.late", "main.later"]
    assert rows(tgt, "SELECT type FROM sqlite_master WHERE name = 'late'") == [("view",)]
    assert rows(tgt, "SELECT id FROM later ORDER BY id") == [(6,)]


def test_a_view_whose_base_is_not_in_the_profile_refuses_before_any_data_moves():
    src, tgt, source, sink = make_pair()
    seed(src)
    with src.begin() as c:
        c.execute(text("CREATE TABLE other (id INTEGER PRIMARY KEY)"))
        c.execute(text("CREATE VIEW joined AS SELECT r.id FROM readings r JOIN other o ON o.id = r.id"))
    with pytest.raises(Refused) as e:
        source.run([INCR, TableSpec("main", "joined")], loopback_to(sink))
    assert e.value.code == "view_base_missing"
    assert "main.other" in e.value.message
    assert rows(tgt, "SELECT name FROM sqlite_master") == []


# ---------------------------------------------------------------------------
# S8/W11: no row value or connection detail in the output
# ---------------------------------------------------------------------------

def test_no_log_line_carries_a_row_value_or_the_dsn(tmp_path, capsys):
    secret_dir = tmp_path / "secret-host.example.org"
    secret_dir.mkdir()
    src = create_engine(f"sqlite:///{secret_dir}/src.db")
    tgt = create_engine(f"sqlite:///{secret_dir}/tgt.db", connect_args={"check_same_thread": False})
    with src.begin() as c:
        c.execute(text("CREATE TABLE people (id INTEGER PRIMARY KEY, ts DATETIME, name TEXT)"))
        c.execute(text("INSERT INTO people VALUES (1, '2026-01-01 00:00:00', 'ROW-SECRET-VALUE')"))
        c.execute(text("INSERT INTO people VALUES (2, '2026-01-02 00:00:00', NULL)"))
    # A target that rejects one row: the error must not echo the row.
    with tgt.begin() as c:
        c.execute(text("CREATE TABLE people (id INTEGER PRIMARY KEY, ts DATETIME, name TEXT NOT NULL)"))
    source = Source(src, ("dev", "local"), identify=lambda c: DEV)
    sink = Sink(tgt, ["main"], identify=lambda c: LOCAL)
    source.run([TableSpec("main", "people", date_column="ts")], loopback_to(sink))
    out = capsys.readouterr().out
    assert "ERROR main.people" in out
    assert "ROW-SECRET-VALUE" not in out
    assert "secret-host" not in out
    assert "sqlite:" not in out


def test_safe_error_drops_values_from_postgres_errors():
    from sync_tables.log import safe_error

    class Diag:
        message_primary = 'duplicate key value violates unique constraint "people_pkey"'
        message_detail = "Key (id)=(ROW-SECRET-VALUE) already exists."
        schema_name, table_name, column_name, constraint_name, datatype_name = "s", "people", None, "people_pkey", None

    class UniqueViolation(Exception):
        sqlstate = "23505"
        diag = Diag()

    msg = safe_error(UniqueViolation("Key (id)=(ROW-SECRET-VALUE) already exists"))
    assert "ROW-SECRET-VALUE" not in msg
    assert "23505" in msg and "people_pkey" in msg


def test_safe_error_drops_the_host_from_connection_errors():
    from sync_tables.log import safe_error

    class OperationalError(Exception):
        sqlstate = None

    msg = safe_error(OperationalError('connection to server at "db.secret.example.org" failed'))
    assert "secret" not in msg


def test_parallel_sessions_copy_every_table():
    src, tgt, source, sink = make_pair()
    with src.begin() as c:
        for name in ("a", "b", "c", "d"):
            c.execute(text(f"CREATE TABLE {name} (id INTEGER PRIMARY KEY, v TEXT)"))
            c.execute(text(f"INSERT INTO {name} VALUES (1, '{name}'), (2, '{name}')"))
    r = source.run([TableSpec("main", n) for n in ("a", "b", "c", "d")], loopback_to(sink), parallelism=3)
    assert sorted(r.ok) == ["main.a", "main.b", "main.c", "main.d"] and not r.failed
    for n in ("a", "b", "c", "d"):
        assert rows(tgt, f"SELECT * FROM {n} ORDER BY id") == [(1, n), (2, n)]
