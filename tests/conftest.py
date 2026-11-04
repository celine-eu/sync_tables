"""Shared fixtures for sync_tables test suite."""
from __future__ import annotations

import pathlib
import textwrap

import pytest
from sqlalchemy import Column, DateTime, Integer, String, Table, MetaData, create_engine
from sqlalchemy.engine import Engine


@pytest.fixture
def src_engine() -> Engine:
    return create_engine("sqlite:///:memory:")


@pytest.fixture
def tgt_engine() -> Engine:
    return create_engine("sqlite:///:memory:")


def make_table(engine: Engine, name: str, with_ts: bool = True) -> Table:
    """Helper: create a simple test table in the given engine and return the Table object."""
    meta = MetaData()
    cols = [Column("id", Integer, primary_key=True)]
    if with_ts:
        cols.append(Column("ts", DateTime))
    cols.append(Column("value", String(64)))
    table = Table(name, meta, *cols)
    meta.create_all(engine)
    return table


@pytest.fixture
def pipeline_dir(tmp_path: pathlib.Path) -> pathlib.Path:
    """
    Create a minimal fake pipeline directory structure:
      tmp_path/
        my_pipeline/
          dbt/
            models/
              gold/
                orders.sql          ← incremental with ts
                summary.sql         ← NOT incremental → None
              silver/
                raw_orders.sql      ← incremental with created_at
    """
    base = tmp_path / "my_pipeline" / "dbt" / "models"

    gold = base / "gold"
    gold.mkdir(parents=True)

    (gold / "orders.sql").write_text(textwrap.dedent("""\
        {{ config(materialized='incremental', unique_key='id') }}
        select id, ts, amount from orders_raw
        {% if is_incremental() %}
        where ts >= (select coalesce(max(ts), '1900-01-01') from {{ this }})
        {% endif %}
    """), encoding="utf-8")

    (gold / "summary.sql").write_text(textwrap.dedent("""\
        select date, sum(amount) as total from orders group by date
    """), encoding="utf-8")

    silver = base / "silver"
    silver.mkdir()

    (silver / "raw_orders.sql").write_text(textwrap.dedent("""\
        {{ config(materialized='incremental') }}
        select * from source_table
        {% if is_incremental() %}
        where created_at >= (select max(created_at) from {{ this }})
        {% endif %}
    """), encoding="utf-8")

    return tmp_path
