"""Tests for config.py — pure parsing, no DB required."""
from __future__ import annotations

import os
import textwrap

import pytest

from sync_tables.config import AppConfig, TableSpec, expand_env_recursive, load_config, parse_cutoff


# ---------------------------------------------------------------------------
# parse_cutoff
# ---------------------------------------------------------------------------

def test_parse_cutoff_valid():
    from datetime import timedelta
    assert parse_cutoff("15d") == timedelta(days=15)
    assert parse_cutoff("1d") == timedelta(days=1)
    assert parse_cutoff("  7d  ") == timedelta(days=7)


def test_parse_cutoff_invalid():
    with pytest.raises(ValueError, match="Invalid cutoff"):
        parse_cutoff("1h")
    with pytest.raises(ValueError):
        parse_cutoff("abc")
    with pytest.raises(ValueError):
        parse_cutoff("")


# ---------------------------------------------------------------------------
# expand_env_recursive
# ---------------------------------------------------------------------------

def test_expand_env_recursive_resolves(monkeypatch):
    monkeypatch.setenv("MY_VAR", "hello")
    result = expand_env_recursive({"url": "${MY_VAR}"})
    assert result == {"url": "hello"}


def test_expand_env_recursive_strict_fails_on_unresolved():
    with pytest.raises(RuntimeError, match="Unresolved"):
        expand_env_recursive("${DEFINITELY_NOT_SET_XYZ_123}", strict=True)


def test_expand_env_recursive_non_strict_passes():
    result = expand_env_recursive("${DEFINITELY_NOT_SET_XYZ_123}", strict=False)
    assert "${DEFINITELY_NOT_SET_XYZ_123}" in result


# ---------------------------------------------------------------------------
# load_config — table sources
# ---------------------------------------------------------------------------

def _write_config(tmp_path, content: str) -> str:
    p = tmp_path / "config.yaml"
    p.write_text(textwrap.dedent(content), encoding="utf-8")
    return str(p)


def test_load_config_table_source(tmp_path, monkeypatch):
    monkeypatch.setenv("SRC_URL", "postgresql://src")
    monkeypatch.setenv("DST_URL", "postgresql://dst")
    path = _write_config(tmp_path, """\
        source_db:
          url: ${SRC_URL}
        destination_db:
          url: ${DST_URL}
        batch_size: 1000
        create_table_if_missing: false
        sources:
          - schema: raw
            table: events
            date_column: ts
            cutoff: 3d
          - schema: gold
            table: summary
    """)
    cfg = load_config(path)

    assert isinstance(cfg, AppConfig)
    assert cfg.source_db_url == "postgresql://src"
    assert cfg.destination_db_url == "postgresql://dst"
    assert cfg.batch_size == 1000
    assert cfg.create_table_if_missing is False
    assert len(cfg.sources) == 2

    s0 = cfg.sources[0]
    assert s0 == TableSpec(schema="raw", table="events", date_column="ts", cutoff="3d")

    s1 = cfg.sources[1]
    assert s1 == TableSpec(schema="gold", table="summary", date_column=None, cutoff=None)


def test_load_config_cutoff_requires_date_column(tmp_path, monkeypatch):
    monkeypatch.setenv("SRC_URL", "postgresql://src")
    monkeypatch.setenv("DST_URL", "postgresql://dst")
    path = _write_config(tmp_path, """\
        source_db:
          url: ${SRC_URL}
        destination_db:
          url: ${DST_URL}
        sources:
          - schema: raw
            table: events
            cutoff: 3d
    """)
    with pytest.raises(ValueError, match="date_column.*required"):
        load_config(path)


def test_load_config_invalid_cutoff_format(tmp_path, monkeypatch):
    monkeypatch.setenv("SRC_URL", "postgresql://src")
    monkeypatch.setenv("DST_URL", "postgresql://dst")
    path = _write_config(tmp_path, """\
        source_db:
          url: ${SRC_URL}
        destination_db:
          url: ${DST_URL}
        sources:
          - schema: raw
            table: events
            date_column: ts
            cutoff: 3h
    """)
    with pytest.raises(ValueError, match="Invalid cutoff"):
        load_config(path)


def test_load_config_unknown_type_raises(tmp_path, monkeypatch):
    monkeypatch.setenv("SRC_URL", "postgresql://src")
    monkeypatch.setenv("DST_URL", "postgresql://dst")
    path = _write_config(tmp_path, """\
        source_db:
          url: ${SRC_URL}
        destination_db:
          url: ${DST_URL}
        sources:
          - type: view
            schema: raw
            table: events
    """)
    with pytest.raises(ValueError, match="Unknown source type"):
        load_config(path)


def test_load_config_type_path_expands(tmp_path, monkeypatch, pipeline_dir):
    """type:path entries are expanded by path_sources.expand_path_source."""
    monkeypatch.setenv("SRC_URL", "postgresql://src")
    monkeypatch.setenv("DST_URL", "postgresql://dst")
    path = _write_config(tmp_path, f"""\
        source_db:
          url: ${{SRC_URL}}
        destination_db:
          url: ${{DST_URL}}
        pipelines_root: {pipeline_dir}
        sources:
          - type: path
            pipeline: my_pipeline
            schema_prefix: wh
            patterns:
              - gold/*
    """)
    cfg = load_config(path)
    # gold/ has orders.sql and summary.sql
    tables = {s.table for s in cfg.sources}
    assert "orders" in tables
    assert "summary" in tables
    # orders has is_incremental() → ts detected; summary does not → None
    orders = next(s for s in cfg.sources if s.table == "orders")
    assert orders.date_column == "ts"
    assert orders.schema == "wh_gold"
    summary = next(s for s in cfg.sources if s.table == "summary")
    assert summary.date_column is None


@pytest.mark.parametrize("given", [
    "postgres://u:p%40ss@db.example.org:5432/wh",
    "postgresql://u:p%40ss@db.example.org:5432/wh",
    "postgresql+asyncpg://u:p%40ss@db.example.org:5432/wh",
    "postgresql+psycopg2://u:p%40ss@db.example.org:5432/wh",
    "postgresql+psycopg://u:p%40ss@db.example.org:5432/wh",
])
def test_every_postgres_url_runs_on_psycopg(given):
    from sqlalchemy.engine import make_url

    from sync_tables.db import normalize_url

    u = make_url(normalize_url(given))
    assert u.drivername == "postgresql+psycopg"
    assert (u.username, u.password, u.host, u.port, u.database) == ("u", "p@ss", "db.example.org", 5432, "wh")


def test_a_non_postgres_url_is_left_alone():
    from sync_tables.db import normalize_url

    assert normalize_url("sqlite:///x.db") == "sqlite:///x.db"
