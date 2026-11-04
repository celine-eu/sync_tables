"""Tests for path_sources.py — pure logic, no DB required."""
from __future__ import annotations

import textwrap

import pytest

from sync_tables.path_sources import (
    _parse_pattern_entry,
    expand_path_source,
    extract_date_column_from_sql,
)


# ---------------------------------------------------------------------------
# extract_date_column_from_sql
# ---------------------------------------------------------------------------

def test_extract_simple_where_ts():
    sql = textwrap.dedent("""\
        select * from foo
        {% if is_incremental() %}
        where ts >= (select coalesce(max(ts), '1900-01-01') from {{ this }})
        {% endif %}
    """)
    assert extract_date_column_from_sql(sql) == "ts"


def test_extract_aliased_column():
    sql = textwrap.dedent("""\
        select v.ts, v.amount from foo v
        {% if is_incremental() %}
        where v.ts >= date_trunc('day', now() - interval '2 days')
        {% endif %}
    """)
    assert extract_date_column_from_sql(sql) == "ts"


def test_extract_and_clause_with_cast():
    sql = textwrap.dedent("""\
        select * from foo
        where period = 'plan'
        {% if is_incremental() %}
        and timestamp::timestamp::date >= current_date
        {% endif %}
    """)
    assert extract_date_column_from_sql(sql) == "timestamp"


def test_extract_no_incremental_block():
    sql = "select date, sum(amount) as total from orders group by date"
    assert extract_date_column_from_sql(sql) is None


def test_extract_incremental_block_no_where():
    # Block exists but has only a comment, no WHERE/AND
    sql = textwrap.dedent("""\
        select * from foo
        {% if is_incremental() %}
        -- full refresh only
        {% endif %}
    """)
    assert extract_date_column_from_sql(sql) is None


def test_extract_multiple_blocks_uses_first():
    sql = textwrap.dedent("""\
        select * from a
        {% if is_incremental() %}
        where created_at >= now() - interval '1 day'
        {% endif %}
        union all
        select * from b
        {% if is_incremental() %}
        where updated_at >= now() - interval '1 day'
        {% endif %}
    """)
    # First block wins
    assert extract_date_column_from_sql(sql) == "created_at"


def test_extract_with_jinja_whitespace_control():
    sql = textwrap.dedent("""\
        select * from foo
        {%- if is_incremental() -%}
        where ts >= (select max(ts) from {{ this }})
        {%- endif -%}
    """)
    assert extract_date_column_from_sql(sql) == "ts"


def test_extract_and_clause_with_a_cast():
    """An incremental block whose first filter is an AND on a cast column."""
    sql = textwrap.dedent("""\
        select * from windows where kind = 'w'
        {% if is_incremental() %}
        and timestamp::date >= (select max(timestamp)::date from {{ this }})
        {% endif %}
    """)
    assert extract_date_column_from_sql(sql) == "timestamp"


# ---------------------------------------------------------------------------
# _parse_pattern_entry
# ---------------------------------------------------------------------------

def test_parse_string_pattern():
    layer, glob, dc, cutoff = _parse_pattern_entry("gold/**/*")
    assert layer == "gold"
    assert glob == "**/*"
    assert dc is None
    assert cutoff is None


def test_parse_dict_pattern():
    layer, glob, dc, cutoff = _parse_pattern_entry({
        "pattern": "silver/readings_*",
        "date_column": "ts",
        "cutoff": "7d",
    })
    assert layer == "silver"
    assert glob == "readings_*"
    assert dc == "ts"
    assert cutoff == "7d"


def test_parse_dict_pattern_minimal():
    layer, glob, dc, cutoff = _parse_pattern_entry({"pattern": "gold/*"})
    assert layer == "gold"
    assert glob == "*"
    assert dc is None
    assert cutoff is None


def test_parse_pattern_no_slash_raises():
    with pytest.raises(ValueError, match="layer.*glob"):
        _parse_pattern_entry("gold_all")


def test_parse_empty_pattern_raises():
    with pytest.raises(ValueError):
        _parse_pattern_entry({"pattern": ""})


def test_parse_invalid_type_raises():
    with pytest.raises(ValueError, match="string or dict"):
        _parse_pattern_entry(42)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# expand_path_source
# ---------------------------------------------------------------------------

def test_expand_finds_sql_files(pipeline_dir):
    entry = {
        "pipeline": "my_pipeline",
        "schema_prefix": "wh",
        "patterns": ["gold/*"],
    }
    specs = expand_path_source(entry, str(pipeline_dir))
    tables = {s.table for s in specs}
    assert "orders" in tables
    assert "summary" in tables
    # schema derived from layer
    assert all(s.schema == "wh_gold" for s in specs)


def test_expand_date_column_detected(pipeline_dir):
    entry = {
        "pipeline": "my_pipeline",
        "schema_prefix": "wh",
        "patterns": ["gold/*"],
    }
    specs = expand_path_source(entry, str(pipeline_dir))
    orders = next(s for s in specs if s.table == "orders")
    assert orders.date_column == "ts"
    summary = next(s for s in specs if s.table == "summary")
    assert summary.date_column is None


def test_expand_explicit_date_column_overrides(pipeline_dir):
    entry = {
        "pipeline": "my_pipeline",
        "schema_prefix": "wh",
        "patterns": [{"pattern": "gold/orders", "date_column": "created_at"}],
    }
    specs = expand_path_source(entry, str(pipeline_dir))
    assert len(specs) == 1
    assert specs[0].date_column == "created_at"


def test_expand_requires_a_schema_prefix(pipeline_dir):
    entry = {"pipeline": "my_pipeline", "patterns": ["gold/*"]}
    with pytest.raises(ValueError, match="schema_prefix"):
        expand_path_source(entry, str(pipeline_dir))


def test_expand_zero_matches_raises(pipeline_dir):
    entry = {
        "pipeline": "my_pipeline",
        "schema_prefix": "wh",
        "patterns": ["gold/nonexistent_*"],
    }
    with pytest.raises(ValueError, match="matched no .sql files"):
        expand_path_source(entry, str(pipeline_dir))


def test_expand_missing_pipeline_raises(tmp_path):
    entry = {
        "pipeline": "no_such_pipeline",
        "schema_prefix": "wh",
        "patterns": ["gold/*"],
    }
    with pytest.raises(FileNotFoundError, match="dbt models directory not found"):
        expand_path_source(entry, str(tmp_path))


def test_expand_missing_layer_raises(pipeline_dir):
    entry = {
        "pipeline": "my_pipeline",
        "schema_prefix": "wh",
        "patterns": ["bronze/*"],  # bronze/ doesn't exist
    }
    with pytest.raises(FileNotFoundError, match="Layer directory not found"):
        expand_path_source(entry, str(pipeline_dir))


def test_expand_cutoff_requires_date_column(pipeline_dir):
    """summary.sql has no is_incremental() → date_column=None → cutoff should fail."""
    entry = {
        "pipeline": "my_pipeline",
        "schema_prefix": "wh",
        "patterns": [{"pattern": "gold/summary", "cutoff": "7d"}],
    }
    with pytest.raises(ValueError, match="cutoff.*requires.*date_column"):
        expand_path_source(entry, str(pipeline_dir))


def test_expand_silver_layer(pipeline_dir):
    entry = {
        "pipeline": "my_pipeline",
        "schema_prefix": "wh",
        "patterns": ["silver/*"],
    }
    specs = expand_path_source(entry, str(pipeline_dir))
    assert len(specs) == 1
    assert specs[0].table == "raw_orders"
    assert specs[0].schema == "wh_silver"
    assert specs[0].date_column == "created_at"
