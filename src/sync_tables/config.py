from __future__ import annotations

import os
import re
from dataclasses import dataclass
from datetime import timedelta
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

import yaml

if TYPE_CHECKING:
    from sync_tables.envs import EdgePolicy


def parse_cutoff(s: str) -> timedelta:
    """Parse a cutoff string like '15d' into a timedelta."""
    m = re.fullmatch(r"(\d+)d", s.strip())
    if not m:
        raise ValueError(f"Invalid cutoff format {s!r}; expected '<N>d' (e.g. '15d')")
    return timedelta(days=int(m.group(1)))


def parse_lookback(s: str) -> timedelta:
    """Parse a lookback like '2d' or '6h' into a timedelta (plan S7/D21)."""
    m = re.fullmatch(r"(\d+)([dh])", s.strip())
    if not m:
        raise ValueError(f"Invalid lookback format {s!r}; expected '<N>d' or '<N>h' (e.g. '2d')")
    n = int(m.group(1))
    return timedelta(days=n) if m.group(2) == "d" else timedelta(hours=n)


def expand_env_recursive(obj: Any, *, strict: bool = True) -> Any:
    if isinstance(obj, dict):
        return {k: expand_env_recursive(v, strict=strict) for k, v in obj.items()}
    if isinstance(obj, list):
        return [expand_env_recursive(v, strict=strict) for v in obj]
    if isinstance(obj, str):
        expanded = os.path.expandvars(obj)
        if strict and "$" in expanded:
            raise RuntimeError(f"Unresolved environment variable in: {obj}")
        return expanded
    return obj


@dataclass(frozen=True)
class TableSpec:
    schema: str
    table: str
    date_column: str | None = None
    cutoff: str | None = None
    lookback: str | None = None


@dataclass(frozen=True)
class AppConfig:
    source_db_url: str | None
    destination_db_url: str | None
    sources: list[TableSpec]
    batch_size: int = 5000
    create_table_if_missing: bool = True
    parallelism: int = 1


def _parse_sources(
    raw_sources: list[dict[str, Any]],
    get_pipelines_root: Callable[[], str],
) -> list[TableSpec]:
    sources: list[TableSpec] = []
    for s in raw_sources:
        source_type = s.get("type", "table")

        if source_type == "path":
            from sync_tables.path_sources import expand_path_source  # noqa: PLC0415
            sources.extend(expand_path_source(s, get_pipelines_root()))
            continue

        if source_type != "table":
            raise ValueError(f"Unknown source type {source_type!r}; expected 'table' or 'path'")

        raw_dc = s.get("date_column")
        raw_cutoff = s.get("cutoff")
        if raw_cutoff is not None:
            raw_cutoff = str(raw_cutoff)
            parse_cutoff(raw_cutoff)
            if raw_dc is None:
                raise ValueError(
                    f"Table {s.get('schema')}.{s.get('table')}: "
                    f"'date_column' is required when 'cutoff' is specified"
                )
        raw_lookback = s.get("lookback")
        if raw_lookback is not None:
            raw_lookback = str(raw_lookback)
            parse_lookback(raw_lookback)
            if raw_dc is None:
                raise ValueError(
                    f"Table {s.get('schema')}.{s.get('table')}: "
                    f"'date_column' is required when 'lookback' is specified"
                )
        sources.append(
            TableSpec(
                schema=str(s["schema"]),
                table=str(s["table"]),
                date_column=str(raw_dc) if raw_dc is not None else None,
                cutoff=raw_cutoff,
                lookback=raw_lookback,
            )
        )
    return sources


def _resolve_url(template: str | None) -> str | None:
    """Expand ${VAR}s in a URL; None when absent or unresolved (the caller decides)."""
    if template is None:
        return None
    try:
        return str(expand_env_recursive(str(template), strict=True))
    except RuntimeError:
        return None


def load_profile(path: str, pipelines_root: str | None = None) -> list[TableSpec]:
    """Load a table list from a profile file mounted at run time (plan S6)."""
    with open(path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f) or {}
    raw = expand_env_recursive(raw, strict=True)

    def get_pipelines_root() -> str:
        if pipelines_root is None:
            raise ValueError("a 'path' source needs a pipelines root; profiles mounted at run time list tables")
        return pipelines_root

    return _parse_sources(raw.get("sources", []), get_pipelines_root)


def load_edges(path: str = "./config.yaml") -> "EdgePolicy":
    """The `edges` a config file declares; none when the file is missing."""
    from sync_tables.envs import EdgePolicy  # noqa: PLC0415

    if not os.path.isfile(path):
        return EdgePolicy()
    with open(path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f) or {}
    if not isinstance(raw, dict):
        raise ValueError(f"{path}: not a mapping")
    return EdgePolicy.from_config(raw.get("edges"))


def list_configs(base_path: str = "./config") -> list[str]:
    config_dir = os.path.join(os.path.dirname(os.path.abspath(base_path)), "config")
    if not os.path.isdir(config_dir):
        return []
    return sorted(
        os.path.splitext(f)[0]
        for f in os.listdir(config_dir)
        if f.endswith((".yaml", ".yml"))
    )


# What a run uses when there is no config.yaml (the image ships none): the connection URLs
# from the environment, and the tuning a config.yaml would usually set.
DEFAULT_CONFIG: dict[str, Any] = {
    "source_db": {"url": "${DB_DEV}"},
    "destination_db": {"url": "${DB_LOCAL}"},
    "parallelism": 4,
    "batch_size": 5000,
    "create_table_if_missing": True,
}


def load_config(path: str = "./config.yaml", config_name: str | None = None) -> AppConfig:
    if os.path.isfile(path):
        with open(path, "r", encoding="utf-8") as f:
            raw = yaml.safe_load(f) or {}
    else:
        raw = {k: (dict(v) if isinstance(v, dict) else v) for k, v in DEFAULT_CONFIG.items()}

    # Connection URLs are resolved on their own: a push run has no local target, so an
    # unset ${DB_LOCAL} must not fail it. Everything else is expanded strictly.
    urls = {k: (raw.pop(k, None) or {}).get("url") for k in ("source_db", "destination_db")}
    raw = expand_env_recursive(raw, strict=True)

    _pipelines_root: str | None = None

    def get_pipelines_root() -> str:
        nonlocal _pipelines_root
        if _pipelines_root is None:
            raw_root = raw.get("pipelines_root")
            if not raw_root:
                raise ValueError(
                    "a 'path' source needs 'pipelines_root' in config.yaml "
                    "(the directory holding <pipeline>/dbt/models, relative to config.yaml)"
                )
            config_dir = os.path.dirname(os.path.abspath(path))
            _pipelines_root = os.path.normpath(os.path.join(config_dir, raw_root))
        return _pipelines_root

    if config_name is not None:
        config_dir = os.path.join(os.path.dirname(os.path.abspath(path)), "config")
        named_path = os.path.join(config_dir, f"{config_name}.yaml")
        if not os.path.isfile(named_path):
            named_path = os.path.join(config_dir, f"{config_name}.yml")
        if not os.path.isfile(named_path):
            available = list_configs(path)
            raise FileNotFoundError(
                f"Config '{config_name}' not found in {config_dir}/. "
                f"Available: {', '.join(available) or '(none)'}"
            )
        with open(named_path, "r", encoding="utf-8") as f:
            named_raw = yaml.safe_load(f) or {}
        named_raw = expand_env_recursive(named_raw, strict=True)
        sources = _parse_sources(named_raw.get("sources", []), get_pipelines_root)
    else:
        sources = _parse_sources(raw.get("sources", []), get_pipelines_root)

    return AppConfig(
        source_db_url=_resolve_url(urls["source_db"]),
        destination_db_url=_resolve_url(urls["destination_db"]),
        sources=sources,
        batch_size=int(raw.get("batch_size", 5000)),
        create_table_if_missing=bool(raw.get("create_table_if_missing", True)),
        parallelism=max(1, int(raw.get("parallelism", 1))),
    )
