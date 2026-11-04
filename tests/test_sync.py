"""SQLite integration tests for sync logic."""
from __future__ import annotations

from datetime import date, datetime

import pytest
from sqlalchemy import Column, DateTime, Integer, String, Table, MetaData, text

from sync_tables.config import TableSpec
from sync_tables.sync import (
    get_target_watermark,
    normalize_watermark,
    stream_all_rows,
    stream_source_rows,
    upsert_batch,
)


# ---------------------------------------------------------------------------
# normalize_watermark
# ---------------------------------------------------------------------------

class _FakeCol:
    """Minimal column stub for normalize_watermark tests."""
    def __init__(self, col_type):
        self.type = col_type


def test_normalize_watermark_date_col_with_datetime():
    from sqlalchemy.sql.sqltypes import Date
    col = _FakeCol(Date())
    dt = datetime(2024, 3, 15, 10, 30)
    result = normalize_watermark(dt, col)
    assert result == date(2024, 3, 15)


def test_normalize_watermark_datetime_col_with_date():
    from sqlalchemy.sql.sqltypes import DateTime
    col = _FakeCol(DateTime())
    d = date(2024, 3, 15)
    result = normalize_watermark(d, col)
    assert result == datetime(2024, 3, 15, 0, 0, 0)


def test_normalize_watermark_none():
    from sqlalchemy.sql.sqltypes import DateTime
    col = _FakeCol(DateTime())
    assert normalize_watermark(None, col) is None


def test_normalize_watermark_datetime_passthrough():
    from sqlalchemy.sql.sqltypes import DateTime
    col = _FakeCol(DateTime())
    dt = datetime(2024, 1, 1, 12, 0)
    assert normalize_watermark(dt, col) is dt


# ---------------------------------------------------------------------------
# SQLite fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def src_conn(src_engine):
    with src_engine.connect() as conn:
        yield conn


@pytest.fixture
def tgt_conn(tgt_engine):
    with tgt_engine.connect() as conn:
        yield conn


def _make_table(engine, name: str) -> Table:
    meta = MetaData()
    t = Table(
        name, meta,
        Column("id", Integer, primary_key=True),
        Column("ts", DateTime),
        Column("value", String(64)),
    )
    meta.create_all(engine)
    return t


# ---------------------------------------------------------------------------
# get_target_watermark
# ---------------------------------------------------------------------------

def test_get_target_watermark_empty_table(tgt_engine, tgt_conn):
    tgt_table = _make_table(tgt_engine, "wm_test")
    result = get_target_watermark(tgt_conn, tgt_table, "ts")
    assert result is None


def test_get_target_watermark_returns_max(tgt_engine, tgt_conn):
    tgt_table = _make_table(tgt_engine, "wm_test2")
    tgt_conn.execute(tgt_table.insert(), [
        {"id": 1, "ts": datetime(2024, 1, 1), "value": "a"},
        {"id": 2, "ts": datetime(2024, 3, 15), "value": "b"},
        {"id": 3, "ts": datetime(2024, 2, 1), "value": "c"},
    ])
    tgt_conn.commit()
    result = get_target_watermark(tgt_conn, tgt_table, "ts")
    assert result == datetime(2024, 3, 15)


def test_get_target_watermark_missing_column_raises(tgt_engine, tgt_conn):
    tgt_table = _make_table(tgt_engine, "wm_test3")
    with pytest.raises(KeyError, match="no_col"):
        get_target_watermark(tgt_conn, tgt_table, "no_col")


# ---------------------------------------------------------------------------
# stream_all_rows
# ---------------------------------------------------------------------------

def test_stream_all_rows(src_engine, src_conn):
    src_table = _make_table(src_engine, "src_all")
    rows = [{"id": i, "ts": datetime(2024, 1, i), "value": f"v{i}"} for i in range(1, 6)]
    src_conn.execute(src_table.insert(), rows)
    src_conn.commit()

    batches = list(stream_all_rows(src_conn, src_table, batch_size=2))
    all_rows = [r for b in batches for r in b]
    assert len(all_rows) == 5
    # batches respect batch_size
    assert all(len(b) <= 2 for b in batches)


def test_stream_all_rows_empty(src_engine, src_conn):
    src_table = _make_table(src_engine, "src_empty")
    batches = list(stream_all_rows(src_conn, src_table, batch_size=100))
    assert batches == []


# ---------------------------------------------------------------------------
# stream_source_rows
# ---------------------------------------------------------------------------

def test_stream_source_rows_with_watermark(src_engine, src_conn):
    src_table = _make_table(src_engine, "src_wm")
    rows = [
        {"id": 1, "ts": datetime(2024, 1, 1), "value": "a"},
        {"id": 2, "ts": datetime(2024, 2, 1), "value": "b"},
        {"id": 3, "ts": datetime(2024, 3, 1), "value": "c"},
    ]
    src_conn.execute(src_table.insert(), rows)
    src_conn.commit()

    watermark = datetime(2024, 1, 1)  # strict >
    batches = list(stream_source_rows(src_conn, src_table, "ts", watermark, 100))
    ids = [r["id"] for b in batches for r in b]
    assert ids == [2, 3]  # ordered by ts ASC


def test_stream_source_rows_no_watermark(src_engine, src_conn):
    src_table = _make_table(src_engine, "src_nowm")
    rows = [
        {"id": 1, "ts": datetime(2024, 1, 1), "value": "a"},
        {"id": 2, "ts": datetime(2024, 2, 1), "value": "b"},
    ]
    src_conn.execute(src_table.insert(), rows)
    src_conn.commit()

    batches = list(stream_source_rows(src_conn, src_table, "ts", None, 100))
    ids = [r["id"] for b in batches for r in b]
    assert ids == [1, 2]


def test_stream_source_rows_missing_column_raises(src_engine, src_conn):
    src_table = _make_table(src_engine, "src_badcol")
    with pytest.raises(KeyError, match="nope"):
        list(stream_source_rows(src_conn, src_table, "nope", None, 100))


# ---------------------------------------------------------------------------
# upsert_batch
# ---------------------------------------------------------------------------

def test_upsert_batch_inserts_new_rows(tgt_engine, tgt_conn):
    tgt_table = _make_table(tgt_engine, "upsert_test")
    rows = [{"id": 1, "ts": datetime(2024, 1, 1), "value": "v1"}]
    count = upsert_batch(tgt_conn, tgt_table, rows)
    tgt_conn.commit()
    assert count >= 1
    result = tgt_conn.execute(tgt_table.select()).mappings().all()
    assert len(result) == 1
    assert result[0]["value"] == "v1"


def test_upsert_batch_updates_on_conflict(tgt_engine, tgt_conn):
    tgt_table = _make_table(tgt_engine, "upsert_conflict")
    # Insert initial row
    tgt_conn.execute(tgt_table.insert(), [{"id": 1, "ts": datetime(2024, 1, 1), "value": "old"}])
    tgt_conn.commit()

    # Upsert same id with updated value
    rows = [{"id": 1, "ts": datetime(2024, 1, 2), "value": "new"}]
    upsert_batch(tgt_conn, tgt_table, rows)
    tgt_conn.commit()

    result = tgt_conn.execute(tgt_table.select()).mappings().all()
    assert len(result) == 1
    assert result[0]["value"] == "new"


def test_upsert_batch_empty_noop(tgt_engine, tgt_conn):
    tgt_table = _make_table(tgt_engine, "upsert_empty")
    count = upsert_batch(tgt_conn, tgt_table, [])
    assert count == 0


# ---------------------------------------------------------------------------
# Incremental sync integration (streaming + upsert, no copy_one_table DDL)
# ---------------------------------------------------------------------------

def test_incremental_sync_only_new_rows(src_engine, tgt_engine):
    """Simulate the incremental path: only rows newer than watermark are synced."""
    src_table = _make_table(src_engine, "inc_src")
    tgt_table = _make_table(tgt_engine, "inc_tgt")

    # Populate source
    with src_engine.connect() as sc:
        sc.execute(src_table.insert(), [
            {"id": 1, "ts": datetime(2024, 1, 1), "value": "a"},
            {"id": 2, "ts": datetime(2024, 2, 1), "value": "b"},
            {"id": 3, "ts": datetime(2024, 3, 1), "value": "c"},
        ])
        sc.commit()

    # First sync: target empty, watermark = None → all rows
    with tgt_engine.connect() as tc:
        wm = get_target_watermark(tc, tgt_table, "ts")
        assert wm is None
        with src_engine.connect() as sc:
            for batch in stream_source_rows(sc, src_table, "ts", wm, 100):
                upsert_batch(tc, tgt_table, batch)
        tc.commit()
        count = tc.execute(tgt_table.select()).rowcount
        result = tc.execute(tgt_table.select()).mappings().all()
        assert len(result) == 3

    # Add one more row to source
    with src_engine.connect() as sc:
        sc.execute(src_table.insert(), [{"id": 4, "ts": datetime(2024, 4, 1), "value": "d"}])
        sc.commit()

    # Second sync: watermark = max(ts) in target → only new row
    with tgt_engine.connect() as tc:
        wm = get_target_watermark(tc, tgt_table, "ts")
        assert wm == datetime(2024, 3, 1)
        with src_engine.connect() as sc:
            batches = list(stream_source_rows(sc, src_table, "ts", wm, 100))
        synced_ids = [r["id"] for b in batches for r in b]
        assert synced_ids == [4]
        for batch in batches:
            upsert_batch(tc, tgt_table, batch)
        tc.commit()
        result = tc.execute(tgt_table.select()).mappings().all()
        assert len(result) == 4
