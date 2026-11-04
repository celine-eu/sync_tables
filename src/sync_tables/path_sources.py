from __future__ import annotations

import pathlib
import re
from typing import Any

from sync_tables.config import TableSpec, parse_cutoff

# Matches the body of a dbt {% if is_incremental() %}...{% endif %} block
_INCREMENTAL_BLOCK_RE = re.compile(
    r"\{%-?\s*if\s+is_incremental\(\)\s*-?%\}(.*?)\{%-?\s*endif\s*-?%\}",
    re.DOTALL | re.IGNORECASE,
)

# Matches the first WHERE or AND clause column inside an incremental block.
# Captures the bare column name before any :: cast or comparison operator.
_WHERE_COL_RE = re.compile(
    r"(?:where|and)\s+(?:\w+\.)?(\w+)",
    re.IGNORECASE,
)

def extract_date_column_from_sql(sql_text: str) -> str | None:
    """
    Scan a dbt SQL file for {% if is_incremental() %} blocks and extract
    the filtered column from the first WHERE/AND clause inside each block.

    Examples:
      'where ts >= (select max(ts)...)'       → 'ts'
      'where v.ts >= date_trunc(...)'          → 'ts'
      'and timestamp::timestamp::date >= ...'  → 'timestamp'
      No is_incremental() block               → None (→ full sync)
    """
    for block_match in _INCREMENTAL_BLOCK_RE.finditer(sql_text):
        block_body = block_match.group(1)
        for line in block_body.splitlines():
            if line.strip().startswith("--"):
                continue  # skip SQL comments
            m = _WHERE_COL_RE.search(line)
            if m:
                return m.group(1)
    return None


def _parse_pattern_entry(p: str | dict[str, Any]) -> tuple[str, str, str | None, str | None]:
    """
    Parse a single pattern entry into (layer, glob_under_layer, date_column, cutoff).

    String form:  "gold/**/*"            → ("gold", "**/*", None, None)
    Dict form:    {pattern: "gold/**/*", date_column: "ts", cutoff: "7d"}
    """
    if isinstance(p, str):
        raw_pattern, explicit_dc, explicit_cutoff = p, None, None
    elif isinstance(p, dict):
        raw_pattern = str(p.get("pattern") or "")
        explicit_dc = p.get("date_column")
        explicit_cutoff = p.get("cutoff")
        if explicit_dc is not None:
            explicit_dc = str(explicit_dc)
        if explicit_cutoff is not None:
            explicit_cutoff = str(explicit_cutoff)
    else:
        raise ValueError(f"Pattern entry must be a string or dict, got {type(p).__name__}")

    if not raw_pattern:
        raise ValueError("Pattern entry has empty 'pattern' value")

    parts = raw_pattern.split("/", 1)
    if len(parts) != 2:
        raise ValueError(
            f"Pattern {raw_pattern!r} must be in '<layer>/<glob>' form, e.g. 'gold/**/*'"
        )
    layer, glob = parts
    return layer, glob, explicit_dc, explicit_cutoff


def expand_path_source(entry: dict[str, Any], pipelines_root: str) -> list[TableSpec]:
    """
    Expand a type:path config entry into a list of TableSpec objects by:
      1. Resolving <pipelines_root>/<pipeline>/dbt/models/
      2. Globbing for .sql files per pattern
      3. Deriving schema from layer (e.g. gold → {prefix}_gold)
      4. Detecting date_column from SQL or explicit override
    """
    pipeline = entry.get("pipeline")
    if not pipeline:
        raise ValueError("type:path source entry missing required field 'pipeline'")

    schema_prefix = entry.get("schema_prefix")
    if not schema_prefix:
        raise ValueError(
            f"type:path source for pipeline {pipeline!r} needs 'schema_prefix' "
            "(the target schema is <schema_prefix>_<layer>)"
        )
    patterns = entry.get("patterns")
    if not patterns:
        raise ValueError(
            f"type:path source for pipeline {pipeline!r} has empty 'patterns'"
        )

    models_root = pathlib.Path(pipelines_root) / pipeline / "dbt" / "models"
    if not models_root.exists():
        raise FileNotFoundError(
            f"dbt models directory not found: {models_root}\n"
            f"  (pipeline={pipeline!r}, pipelines_root={pipelines_root!r})"
        )

    specs: list[TableSpec] = []

    for p in patterns:
        layer, glob_pat, explicit_dc, explicit_cutoff = _parse_pattern_entry(p)

        layer_dir = models_root / layer
        if not layer_dir.is_dir():
            raise FileNotFoundError(
                f"Layer directory not found: {layer_dir}\n"
                f"  (pipeline={pipeline!r}, layer={layer!r})"
            )

        # Normalise glob: if it doesn't end with .sql, ensure we only match SQL files
        if glob_pat.endswith(".sql"):
            full_glob = glob_pat
        elif glob_pat.endswith("*"):
            full_glob = glob_pat + ".sql"
        else:
            full_glob = glob_pat + "*.sql"

        matched = sorted(f for f in layer_dir.glob(full_glob) if f.is_file())

        if not matched:
            raise ValueError(
                f"type:path pattern {p!r} matched no .sql files under {layer_dir}\n"
                f"  (pipeline={pipeline!r}, glob={full_glob!r})"
            )

        schema = f"{schema_prefix}_{layer}"

        for sql_file in matched:
            table = sql_file.stem

            if explicit_dc is not None:
                date_column: str | None = explicit_dc
            else:
                date_column = extract_date_column_from_sql(
                    sql_file.read_text(encoding="utf-8")
                )

            cutoff = explicit_cutoff
            if cutoff is not None:
                parse_cutoff(cutoff)  # validate early
                if date_column is None:
                    raise ValueError(
                        f"type:path: {schema}.{table}: "
                        f"'cutoff' requires a 'date_column', but none could be detected "
                        f"from {sql_file.name}. Add explicit 'date_column' to the pattern."
                    )

            specs.append(TableSpec(
                schema=schema,
                table=table,
                date_column=date_column,
                cutoff=cutoff,
            ))

    return specs
