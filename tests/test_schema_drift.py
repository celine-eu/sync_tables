"""Schema drift: a target that has drifted ahead of the source (e.g. a local migration)."""
from __future__ import annotations

import os

import pytest
from sqlalchemy import create_engine, text

from sync_tables.config import TableSpec
from sync_tables.sync import copy_one_table

# Optional PostgreSQL target, e.g. a throwaway container:
#   SYNC_TABLES_TEST_PG_URL=postgresql+psycopg://postgres:test@127.0.0.1:55432/postgres
PG_URL = os.environ.get("SYNC_TABLES_TEST_PG_URL")


def _sync(src_engine, tgt_engine, schema: str, table: str) -> None:
    with src_engine.connect() as sc, tgt_engine.connect() as tc:
        copy_one_table(sc, tc, TableSpec(schema=schema, table=table), 100, True)


def _source_without_extra(src_engine) -> None:
    with src_engine.begin() as c:
        c.execute(text("CREATE TABLE mirror (device_id TEXT PRIMARY KEY, name TEXT)"))
        c.execute(text("INSERT INTO mirror VALUES ('d1', 'a'), ('d2', 'b')"))


def _columns(tgt_engine, table: str) -> list[str]:
    with tgt_engine.connect() as c:
        return list(c.execute(text(f"SELECT * FROM {table}")).keys())


def test_extra_column_without_dependents_is_dropped(src_engine, tgt_engine, capsys):
    _source_without_extra(src_engine)
    with tgt_engine.begin() as c:
        c.execute(text("CREATE TABLE mirror (device_id TEXT PRIMARY KEY, name TEXT, group_id TEXT)"))

    _sync(src_engine, tgt_engine, "main", "mirror")

    assert _columns(tgt_engine, "mirror") == ["device_id", "name"]
    assert "dropped column 'group_id'" in capsys.readouterr().out


def test_extra_column_a_view_depends_on_is_kept_and_rows_reload(src_engine, tgt_engine, capsys):
    _source_without_extra(src_engine)
    with tgt_engine.begin() as c:
        c.execute(text("CREATE TABLE mirror (device_id TEXT PRIMARY KEY, name TEXT, group_id TEXT)"))
        c.execute(text("INSERT INTO mirror VALUES ('old', 'x', 'c0')"))
        c.execute(text("CREATE VIEW membership AS SELECT device_id, group_id FROM mirror"))

    _sync(src_engine, tgt_engine, "main", "mirror")

    out = capsys.readouterr().out
    assert "WARNING [mirror]: could not drop 'group_id', kept" in out
    assert "membership" in out  # the reason names the dependent view
    assert "full sync -> rows_written=2" in out
    with tgt_engine.connect() as c:
        rows = c.execute(text("SELECT device_id, group_id FROM membership ORDER BY device_id")).all()
    assert [tuple(r) for r in rows] == [("d1", None), ("d2", None)]


@pytest.mark.skipif(PG_URL is None, reason="SYNC_TABLES_TEST_PG_URL not set")
def test_pg_not_null_column_behind_a_view_chain_is_made_nullable(src_engine, capsys):
    """A local warehouse a migration took ahead of the source: group_id NOT NULL, views on it."""
    tgt_engine = create_engine(PG_URL)
    schema = "sync_tables_test_drift"
    with tgt_engine.begin() as c:
        c.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        c.execute(text(f'CREATE SCHEMA "{schema}"'))
        c.execute(text(
            f'CREATE TABLE "{schema}".mirror '
            "(device_id TEXT PRIMARY KEY, name TEXT, group_id TEXT NOT NULL)"
        ))
        c.execute(text(f"INSERT INTO \"{schema}\".mirror VALUES ('old', 'x', 'c0')"))
        c.execute(text(f'CREATE VIEW "{schema}".membership AS SELECT device_id, group_id FROM "{schema}".mirror'))
        c.execute(text(f'CREATE VIEW "{schema}".members AS SELECT * FROM "{schema}".membership'))
    # SQLite :memory: attachments are per connection; use one connection throughout
    with src_engine.connect() as sc:
        sc.execute(text(f"ATTACH DATABASE ':memory:' AS \"{schema}\""))
        sc.execute(text(f'CREATE TABLE "{schema}".mirror (device_id TEXT PRIMARY KEY, name TEXT)'))
        sc.execute(text(f"INSERT INTO \"{schema}\".mirror VALUES ('d1', 'a'), ('d2', 'b')"))
        sc.commit()
        try:
            for _ in range(2):  # the second run must be a quiet no-op on the schema
                with tgt_engine.connect() as tc:
                    copy_one_table(sc, tc, TableSpec(schema=schema, table="mirror"), 100, True)

            out = capsys.readouterr().out
            assert "ERROR" not in out
            assert out.count("could not drop 'group_id', kept") == 2
            assert out.count("kept column 'group_id' made nullable") == 1
            assert out.count("full sync -> rows_written=2") == 2
            with tgt_engine.connect() as c:
                rows = c.execute(text(f'SELECT device_id, group_id FROM "{schema}".members ORDER BY 1')).all()
            assert [tuple(r) for r in rows] == [("d1", None), ("d2", None)]
        finally:
            with tgt_engine.begin() as c:
                c.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
            tgt_engine.dispose()
