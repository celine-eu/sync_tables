"""config.sample.yaml is the one config the repository ships: it must load as it is."""
from __future__ import annotations

import pathlib
import shutil

from sync_tables.config import TableSpec, load_config

ROOT = pathlib.Path(__file__).resolve().parents[1]
SAMPLE = ROOT / "config.sample.yaml"


def test_sample_loads_with_the_urls_from_the_environment(monkeypatch):
    monkeypatch.setenv("DB_DEV", "postgresql+psycopg://reader@example-dev.example.org/warehouse")
    monkeypatch.setenv("DB_LOCAL", "postgresql+psycopg://owner@127.0.0.1/warehouse")
    cfg = load_config(str(SAMPLE))
    assert cfg.source_db_url == "postgresql+psycopg://reader@example-dev.example.org/warehouse"
    assert cfg.destination_db_url == "postgresql+psycopg://owner@127.0.0.1/warehouse"
    assert cfg.sources, "the sample lists at least one table"
    assert all(isinstance(s, TableSpec) for s in cfg.sources)
    assert cfg.parallelism >= 1 and cfg.batch_size > 0


def test_sample_loads_without_the_urls(monkeypatch):
    """A push run has no local target: unset URLs resolve to None, not an error."""
    monkeypatch.delenv("DB_DEV", raising=False)
    monkeypatch.delenv("DB_LOCAL", raising=False)
    cfg = load_config(str(SAMPLE))
    assert cfg.source_db_url is None and cfg.destination_db_url is None


def test_sample_shows_each_kind_of_table_entry(monkeypatch):
    monkeypatch.delenv("DB_DEV", raising=False)
    monkeypatch.delenv("DB_LOCAL", raising=False)
    cfg = load_config(str(SAMPLE))
    assert any(s.date_column is None for s in cfg.sources), "a full copy"
    assert any(s.date_column and s.lookback for s in cfg.sources), "an incremental copy with lookback"
    assert any(s.date_column and s.cutoff for s in cfg.sources), "an incremental copy with cutoff"


def test_sample_works_as_config_yaml_with_a_named_config(tmp_path, monkeypatch):
    """Copied to config.yaml, the sample takes named configs from config/ beside it."""
    shutil.copy(SAMPLE, tmp_path / "config.yaml")
    (tmp_path / "config").mkdir()
    (tmp_path / "config" / "orders.yaml").write_text(
        "sources:\n  - schema: silver\n    table: orders\n    date_column: ts\n", encoding="utf-8"
    )
    monkeypatch.delenv("DB_DEV", raising=False)
    monkeypatch.delenv("DB_LOCAL", raising=False)
    cfg = load_config(str(tmp_path / "config.yaml"), config_name="orders")
    assert cfg.sources == [TableSpec(schema="silver", table="orders", date_column="ts")]


def test_sample_declares_the_developer_edge():
    """dev -> local is declared like every other edge (plan decision 8)."""
    from sync_tables.config import load_edges
    from sync_tables.envs import DIRECT, PULL

    assert load_edges(str(SAMPLE)).declared == {("dev", "local"): (PULL, DIRECT)}


def test_the_relay_sample_loads_for_each_of_its_sinks(monkeypatch):
    from sync_tables.relay import load_relay_config

    monkeypatch.setenv("DB_DEV", "postgresql+psycopg://reader@example-dev.example.org/warehouse")
    for sink in ("prod", "staging"):
        cfg = load_relay_config(str(ROOT / "relay.sample.yaml"), sink)
        assert (cfg.from_env, cfg.to_env) == ("dev", sink)
        assert cfg.sources and cfg.min_interval == cfg.max_interval == 1800
        assert cfg.url.startswith("wss://") and cfg.url.endswith(".example.org/sync/v1")
