"""Postgres-only guarantees, against throwaway local clusters (plan W15).

Needs two clusters, so source and target have different system identifiers:

    SYNC_TABLES_TEST_PG_URL=postgresql+psycopg://postgres:test@127.0.0.1:55432/postgres
    SYNC_TABLES_TEST_PG_URL_2=postgresql+psycopg://postgres:test@127.0.0.1:55433/postgres

Each must be a superuser URL to a throwaway cluster with PostGIS (e.g. postgis/postgis
containers). A database at either URL that carries a marker other than `local` is never
touched: the tests skip. Never point these at the dev or prod clusters.
"""
from __future__ import annotations

import os
import threading
import uuid
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

import psycopg
import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url

from sync_tables.config import TableSpec
from sync_tables.db import make_engine, make_source_engine
from sync_tables.envs import read_identity
from sync_tables.log import Refused
from sync_tables.protocol import loopback_to
from sync_tables.sink import Sink
from sync_tables.source import Source

URL_A = os.environ.get("SYNC_TABLES_TEST_PG_URL")
URL_B = os.environ.get("SYNC_TABLES_TEST_PG_URL_2")
PREFIX = "sync_tables_test_"

pytestmark = pytest.mark.skipif(
    not (URL_A and URL_B), reason="SYNC_TABLES_TEST_PG_URL and SYNC_TABLES_TEST_PG_URL_2 not set (two throwaway clusters)"
)


def _guard(url: str) -> None:
    eng = create_engine(url)
    with eng.connect() as c:
        comment = c.execute(text(
            "SELECT pg_catalog.shobj_description(d.oid, 'pg_database') FROM pg_catalog.pg_database d "
            "WHERE d.datname = pg_catalog.current_database()"
        )).scalar()
    eng.dispose()
    # Postgres's own default comment on `postgres` is not a marker; a celine one other
    # than local means this is not a throwaway cluster.
    if comment and comment.strip().startswith("celine:") and comment.strip() != "celine:env=local":
        pytest.skip("the test database carries a non-local marker; refusing to touch it")


@pytest.fixture(scope="module")
def clusters():
    _guard(URL_A)
    _guard(URL_B)
    return URL_A, URL_B


def fresh_db(admin_url: str, name: str, marker: str | None) -> str:
    """Create (or recreate) a database in the cluster, marked as asked; return its URL."""
    admin = create_engine(admin_url, isolation_level="AUTOCOMMIT")
    with admin.connect() as c:
        c.execute(text(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)'))
        c.execute(text(f'CREATE DATABASE "{name}"'))
        if marker is not None:
            c.execute(text(f"COMMENT ON DATABASE \"{name}\" IS '{marker}'"))
    admin.dispose()
    url = make_url(admin_url).set(database=name)
    eng = create_engine(url, isolation_level="AUTOCOMMIT")
    with eng.connect() as c:
        c.execute(text("CREATE EXTENSION IF NOT EXISTS postgis"))
    eng.dispose()
    return url.render_as_string(hide_password=False)


@pytest.fixture
def pair(clusters):
    a, b = clusters
    src_url = fresh_db(a, PREFIX + "src", "celine:env=dev")
    tgt_url = fresh_db(b, PREFIX + "tgt", "celine:env=local")
    return src_url, tgt_url


def seed(url: str) -> None:
    eng = create_engine(url)
    with eng.begin() as c:
        c.execute(text("CREATE SCHEMA mirror"))
        c.execute(text(
            "CREATE TABLE mirror.readings ("
            " id uuid PRIMARY KEY, ts timestamptz NOT NULL, day date, value numeric(12,3),"
            " tags text[], doc jsonb, geom geometry(Point, 4326))"
        ))
        start = datetime(2026, 1, 1, tzinfo=timezone.utc)
        for i in range(48):
            c.execute(
                text("INSERT INTO mirror.readings VALUES (:id, :ts, :day, :v, :tags, CAST(:doc AS jsonb), "
                     "ST_SetSRID(ST_MakePoint(:x, 46.0), 4326))"),
                {"id": uuid.UUID(int=i), "ts": start + timedelta(hours=i), "day": date(2026, 1, 1) + timedelta(days=i // 24),
                 "v": Decimal(i) / 7, "tags": ["a", f"t{i}"], "doc": '{"k": %d}' % i, "x": 11 + i / 100},
            )
        c.execute(text("CREATE INDEX readings_day ON mirror.readings (day)"))
        c.execute(text("CREATE VIEW mirror.daily AS SELECT day, count(*) AS n, ST_Collect(geom) AS g FROM mirror.readings GROUP BY day"))
        c.execute(text("CREATE VIEW mirror.busy AS SELECT day FROM mirror.daily WHERE n > 10"))
    eng.dispose()


def snapshot(url: str, sql: str):
    eng = create_engine(url)
    with eng.connect() as c:
        out = [tuple(r) for r in c.execute(text(sql)).all()]
    eng.dispose()
    return out


READINGS = "SELECT id, ts, day, value, tags, doc, ST_AsText(geom) FROM mirror.readings ORDER BY id"
SPECS = [
    TableSpec("mirror", "busy"),
    TableSpec("mirror", "daily"),
    TableSpec("mirror", "readings", date_column="ts", lookback="1d"),
]


def direct(src_url, tgt_url, specs=SPECS, allowed=("mirror",), edge=("dev", "local"), **kw):
    source = Source(make_source_engine(src_url), edge)
    sink = Sink(make_engine(tgt_url), allowed)
    return source.run(specs, loopback_to(sink), **kw)


# ---------------------------------------------------------------------------
# S3 markers, read from the catalog
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("marker,code", [(None, "marker_missing"), ("celine:env=-qa", "marker_malformed")])
def test_marker_missing_or_malformed(clusters, marker, code):
    url = fresh_db(clusters[0], PREFIX + "marker", marker)
    with create_engine(url).connect() as c, pytest.raises(Refused) as e:
        read_identity(c)
    assert e.value.code == code


def test_a_connection_option_cannot_change_what_is_read(clusters):
    url = fresh_db(clusters[0], PREFIX + "marker", "celine:env=dev")
    eng = create_engine(url)
    with eng.begin() as c:
        c.execute(text("CREATE SCHEMA evil"))
        c.execute(text(
            "CREATE FUNCTION evil.shobj_description(oid, name) RETURNS text "
            "LANGUAGE sql AS $$ SELECT 'celine:env=prod' $$"
        ))
    forged = create_engine(url, connect_args={"options": "-c search_path=evil,pg_catalog"})
    with forged.connect() as c:
        assert c.execute(text("SELECT shobj_description(1::oid, 'x')")).scalar() == "celine:env=prod"  # the shadow works…
        assert read_identity(c).env == "dev"  # …and is not what the tool reads


def test_unmarked_local_target_names_the_task(pair, capsys):
    src_url, _ = pair
    tgt_url = fresh_db(URL_B, PREFIX + "tgt", None)
    seed(src_url)
    with pytest.raises(Refused) as e:
        direct(src_url, tgt_url)
    assert e.value.code == "marker_missing" and "celine:env=local" in e.value.message


# ---------------------------------------------------------------------------
# S4 same cluster
# ---------------------------------------------------------------------------

def test_source_and_target_on_the_same_cluster_are_refused(clusters):
    src_url = fresh_db(clusters[0], PREFIX + "src", "celine:env=dev")
    tgt_url = fresh_db(clusters[0], PREFIX + "tgt_same", "celine:env=local")
    seed(src_url)
    with pytest.raises(Refused) as e:
        direct(src_url, tgt_url)
    assert e.value.code == "same_cluster"


# ---------------------------------------------------------------------------
# End to end: types, window re-copy, views, idempotency
# ---------------------------------------------------------------------------

def test_end_to_end_direct_converges_and_reruns_clean(pair):
    src_url, tgt_url = pair
    seed(src_url)
    r = direct(src_url, tgt_url)
    assert r.ok == ["mirror.readings", "mirror.daily", "mirror.busy"], r
    assert snapshot(tgt_url, READINGS) == snapshot(src_url, READINGS)

    # A producer recomputes the last day; the window re-copy picks it up.
    eng = create_engine(src_url)
    with eng.begin() as c:
        c.execute(text("UPDATE mirror.readings SET value = value + 1000 WHERE ts >= '2026-01-02 06:00+00'"))
        c.execute(text("DELETE FROM mirror.readings WHERE id = :id"), {"id": uuid.UUID(int=40)})
    eng.dispose()
    r = direct(src_url, tgt_url)
    assert not r.failed
    assert snapshot(tgt_url, READINGS) == snapshot(src_url, READINGS)

    # The recreated views resolve, in order, and read the copied rows.
    assert snapshot(tgt_url, "SELECT day, n FROM mirror.daily ORDER BY day") == snapshot(src_url, "SELECT day, n FROM mirror.daily ORDER BY day")
    assert snapshot(tgt_url, "SELECT relkind FROM pg_class WHERE relname IN ('daily', 'busy')") == [("v",), ("v",)]
    assert snapshot(tgt_url, "SELECT count(*) FROM mirror.busy") == snapshot(src_url, "SELECT count(*) FROM mirror.busy")
    # The index came along.
    assert snapshot(tgt_url, "SELECT indexname FROM pg_indexes WHERE indexname = 'readings_day'") == [("readings_day",)]

    before = snapshot(tgt_url, READINGS)
    direct(src_url, tgt_url)
    assert snapshot(tgt_url, READINGS) == before


def test_view_with_a_base_outside_the_profile_refuses_before_data_moves(pair):
    src_url, tgt_url = pair
    seed(src_url)
    with pytest.raises(Refused) as e:
        direct(src_url, tgt_url, specs=[TableSpec("mirror", "busy"), TableSpec("mirror", "readings")])
    assert e.value.code == "view_base_missing" and "mirror.daily" in e.value.message
    assert snapshot(tgt_url, "SELECT count(*) FROM pg_namespace WHERE nspname = 'mirror'") == [(0,)]


def test_the_source_transaction_is_read_only(pair):
    src_url, _ = pair
    seed(src_url)
    source = Source(make_source_engine(src_url), ("dev", "local"))
    with source.connect() as c, pytest.raises(Exception) as e:
        c.execute(text("DELETE FROM mirror.readings"))
    assert "read-only" in str(e.value)


# ---------------------------------------------------------------------------
# S5 confinement, by the tool and by the role (D20)
# ---------------------------------------------------------------------------

def _mirror_writer(tgt_url: str) -> str:
    """A role like prod's: it owns the mirror schema and nothing else."""
    eng = create_engine(tgt_url, isolation_level="AUTOCOMMIT")
    with eng.connect() as c:
        # Roles are cluster-wide and may own objects in other test databases: reuse it.
        c.execute(text(
            "DO $$ BEGIN IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'sync_tables_test_writer') "
            "THEN CREATE ROLE sync_tables_test_writer LOGIN PASSWORD 'writer-test'; END IF; END $$"
        ))
        c.execute(text("CREATE SCHEMA mirror AUTHORIZATION sync_tables_test_writer"))
        c.execute(text("CREATE SCHEMA other"))
        c.execute(text("CREATE TABLE other.keep (id int)"))
    eng.dispose()
    return make_url(tgt_url).set(username="sync_tables_test_writer", password="writer-test").render_as_string(hide_password=False)


def test_the_role_refuses_what_the_allowlist_lets_through(clusters):
    src_url = fresh_db(clusters[0], PREFIX + "src", "celine:env=dev")
    tgt_url = fresh_db(clusters[1], PREFIX + "tgt", "celine:env=prod")
    writer_url = _mirror_writer(tgt_url)
    eng = create_engine(src_url)
    with eng.begin() as c:
        c.execute(text("CREATE SCHEMA other"))
        c.execute(text("CREATE TABLE other.keep (id int PRIMARY KEY)"))
        c.execute(text("INSERT INTO other.keep VALUES (1)"))
    eng.dispose()
    # A sink whose allowlist is wrong: the role still stops the write.
    source = Source(make_source_engine(src_url), ("dev", "prod"))
    sink = Sink(make_engine(writer_url), ["mirror", "other"])
    r = source.run([TableSpec("other", "keep")], loopback_to(sink))
    assert r.failed == ["other.keep"]
    assert snapshot(tgt_url, "SELECT count(*) FROM other.keep") == [(0,)]


def test_the_mirror_role_can_read_its_identity(clusters):
    """The marker and the system identifier are readable by a non-superuser role."""
    tgt_url = fresh_db(clusters[1], PREFIX + "tgt", "celine:env=prod")
    writer_url = _mirror_writer(tgt_url)
    with create_engine(writer_url).connect() as c:
        me = read_identity(c)
    assert me.env == "prod" and me.system_identifier.isdigit()


def test_a_view_body_cannot_carry_a_second_statement(clusters):
    tgt_url = fresh_db(clusters[1], PREFIX + "tgt", "celine:env=local")
    eng = create_engine(tgt_url)
    with eng.begin() as c:
        c.execute(text("CREATE SCHEMA mirror"))
        c.execute(text("CREATE TABLE mirror.keep (id int)"))
    sink = Sink(make_engine(tgt_url), ["mirror"])
    with pytest.raises(psycopg.errors.SyntaxError, match="multiple commands"):
        sink.apply_view("mirror", "v", "SELECT 1 AS x; DROP TABLE mirror.keep")
    assert snapshot(tgt_url, "SELECT count(*) FROM pg_tables WHERE tablename = 'keep'") == [(1,)]


def test_a_view_name_taken_by_a_table_is_refused(clusters):
    tgt_url = fresh_db(clusters[1], PREFIX + "tgt", "celine:env=local")
    eng = create_engine(tgt_url)
    with eng.begin() as c:
        c.execute(text("CREATE SCHEMA mirror"))
        c.execute(text("CREATE TABLE mirror.v (id int)"))
    with pytest.raises(Refused) as e:
        Sink(make_engine(tgt_url), ["mirror"]).apply_view("mirror", "v", "SELECT 1 AS x")
    assert e.value.code == "view_name_taken"


# ---------------------------------------------------------------------------
# Push over WSS, end to end, as prod's role
# ---------------------------------------------------------------------------

def test_push_end_to_end_as_the_mirror_role(clusters, tmp_path, capsys):
    from sync_tables import certs
    from sync_tables.wss import channel_factory, client_tls, make_server, server_tls

    src_url = fresh_db(clusters[0], PREFIX + "src", "celine:env=dev")
    tgt_url = fresh_db(clusters[1], PREFIX + "tgt", "celine:env=prod")
    seed(src_url)
    writer_url = _mirror_writer(tgt_url)

    pki = str(tmp_path / "pki")
    certs.make_ca(pki)
    certs.make_server(pki, ["localhost"])
    certs.make_client(pki, "source")
    certs.make_token(pki)
    token = open(f"{pki}/token").read().strip()

    import socket

    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
    sink = Sink(make_engine(writer_url), ["mirror"], log_summaries=True)
    server = make_server(sink, "127.0.0.1", port, token, server_tls(f"{pki}/server.crt", f"{pki}/server.key", f"{pki}/ca.crt"))
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        tls = client_tls(f"{pki}/ca.crt", f"{pki}/client-source.crt", f"{pki}/client-source.key")
        source = Source(make_source_engine(src_url), ("dev", "prod"))
        for _ in range(2):
            r = source.run(SPECS, channel_factory(f"wss://localhost:{port}", tls, token))
            assert not r.failed, r
    finally:
        server.shutdown()
    assert snapshot(tgt_url, READINGS) == snapshot(src_url, READINGS)
    assert snapshot(tgt_url, "SELECT count(*) FROM mirror.busy") == snapshot(src_url, "SELECT count(*) FROM mirror.busy")

    out = capsys.readouterr()
    logged = out.out + out.err
    for secret in (token, "writer-test", "test@", str(port), "127.0.0.1", "0.142"):  # 0.142… is a row value (1/7)
        assert secret not in logged, secret


def test_every_type_postgres_reflects_passes_the_type_grammar(pair):
    """The sink's type grammar is strict; what a real catalog compiles to must still pass."""
    from sync_tables.catalog import check_type_sql, describe_table

    src_url, _ = pair
    eng = create_engine(src_url)
    with eng.begin() as c:
        c.execute(text("CREATE SCHEMA mirror"))
        c.execute(text("CREATE TYPE mirror.mood AS ENUM ('a', 'b')"))
        c.execute(text(
            "CREATE TABLE mirror.kinds ("
            " a smallint, b integer, c bigint, d numeric(10,2), e numeric, f real, g double precision,"
            " h varchar(20), i char(3), j text, k boolean, l date, m time, n timetz, o timestamp(3),"
            " p timestamptz, q interval, r uuid, s bytea, t json, u jsonb, v int[], w text[][],"
            " x geometry(Point, 4326), y geography(LineString, 4326), z geometry, aa mirror.mood,"
            " ab inet, ac bit varying(8), ad numeric(12,3)[])"
        ))
        desc = describe_table(c, "mirror", "kinds")
    eng.dispose()
    refused = []
    for col in desc.columns:
        try:
            check_type_sql(col.type_sql)
        except Exception:
            refused.append((col.name, col.type_sql))
    assert refused == []


def test_a_spatial_index_keeps_its_method_and_large_shapes_copy(pair):
    """REQ-0002: a GiST index arrives as GiST; as a btree it could not hold a large polygon (F3)."""
    src_url, tgt_url = pair
    eng = create_engine(src_url)
    with eng.begin() as c:
        c.execute(text("CREATE SCHEMA mirror"))
        c.execute(text("CREATE TABLE mirror.shapes (id integer PRIMARY KEY, name text, geom geometry(Polygon, 4326))"))
        # 8000 vertices: far beyond a btree row's 2704 bytes.
        c.execute(text("INSERT INTO mirror.shapes VALUES (1, 'big', ST_Buffer(ST_SetSRID(ST_MakePoint(11, 46), 4326), 1, 2000))"))
        c.execute(text("CREATE INDEX shapes_geom ON mirror.shapes USING gist (geom)"))
        c.execute(text("CREATE INDEX shapes_name ON mirror.shapes (name)"))
        c.execute(text("CREATE INDEX shapes_name_pattern ON mirror.shapes (name text_pattern_ops)"))
    eng.dispose()

    r = direct(src_url, tgt_url, specs=[TableSpec("mirror", "shapes")])
    assert r.ok == ["mirror.shapes"], r
    assert snapshot(tgt_url, "SELECT id, ST_NPoints(geom) FROM mirror.shapes") == [(1, 8001)]
    indexes = dict(snapshot(tgt_url, "SELECT indexname, indexdef FROM pg_indexes WHERE tablename = 'shapes'"))
    assert "USING gist (geom)" in indexes["shapes_geom"]
    assert "USING btree (name)" in indexes["shapes_name"]
    # A non-default operator class is not described on the wire: skipped, not built wrong.
    assert "shapes_name_pattern" not in indexes


def test_a_slow_source_read_does_not_leave_the_sink_idle_in_transaction(pair):
    """A source read slower than the target's idle_in_transaction_session_timeout copies:
    the sink's transaction opens only once the source has rows to send (prod, 2026-10-21)."""
    src_url, tgt_url = pair
    eng = create_engine(src_url, isolation_level="AUTOCOMMIT")
    with eng.connect() as c:
        c.execute(text(
            "DO $$ BEGIN IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'sync_tables_test_reader') "
            "THEN CREATE ROLE sync_tables_test_reader LOGIN PASSWORD 'reader-test'; END IF; END $$"
        ))
        c.execute(text("CREATE SCHEMA mirror"))
        c.execute(text("CREATE TABLE mirror.slow (id integer PRIMARY KEY, v text)"))
        c.execute(text("INSERT INTO mirror.slow SELECT i, 'row ' || i FROM generate_series(1, 300) i"))
        # Row security applies to a reader that does not own the table: every row costs 10 ms,
        # so the read takes ~3 s, against a 1 s timeout on the target.
        c.execute(text(
            "CREATE FUNCTION mirror.slowly(i integer) RETURNS boolean VOLATILE LANGUAGE plpgsql "
            "AS $$ BEGIN PERFORM pg_sleep(0.01); RETURN true; END $$"
        ))
        c.execute(text("ALTER TABLE mirror.slow ENABLE ROW LEVEL SECURITY"))
        c.execute(text("CREATE POLICY slowly ON mirror.slow FOR SELECT USING (mirror.slowly(id))"))
        c.execute(text("GRANT USAGE ON SCHEMA mirror TO sync_tables_test_reader"))
        c.execute(text("GRANT SELECT ON mirror.slow TO sync_tables_test_reader"))
    eng.dispose()
    reader_url = make_url(src_url).set(username="sync_tables_test_reader", password="reader-test").render_as_string(hide_password=False)
    tgt = create_engine(tgt_url, isolation_level="AUTOCOMMIT")
    with tgt.connect() as c:
        c.execute(text(f'ALTER DATABASE "{make_url(tgt_url).database}" SET idle_in_transaction_session_timeout = \'1s\''))
    tgt.dispose()

    r = direct(reader_url, tgt_url, specs=[TableSpec("mirror", "slow")])
    assert r.ok == ["mirror.slow"], r
    assert snapshot(tgt_url, "SELECT count(*) FROM mirror.slow") == [(300,)]
