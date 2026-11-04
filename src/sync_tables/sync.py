from __future__ import annotations

from datetime import date, datetime
from typing import Any, Iterable

from sqlalchemy import Table, and_, bindparam, func, select
from sqlalchemy.engine import Connection
from sqlalchemy.sql.sqltypes import Date, DateTime

from sync_tables.config import TableSpec
from sync_tables.db import reflect_table


def normalize_watermark(value: Any, column) -> Any:
    """Ensure watermark type matches the column type exactly."""
    if value is None:
        return None

    coltype = column.type

    if isinstance(coltype, Date):
        if isinstance(value, datetime):
            return value.date()
        if isinstance(value, date):
            return value
        raise TypeError(f"Unexpected watermark type for DATE: {type(value)}")

    if isinstance(coltype, DateTime):
        if isinstance(value, date) and not isinstance(value, datetime):
            return datetime.combine(value, datetime.min.time())
        if isinstance(value, datetime):
            return value
        raise TypeError(f"Unexpected watermark type for DATETIME: {type(value)}")

    return value


def get_target_watermark(
    target_conn: Connection, target_table: Table, date_column: str
) -> datetime | None:
    if date_column not in target_table.c:
        raise KeyError(
            f"date_column '{date_column}' not found in target table "
            f"{target_table.schema}.{target_table.name}"
        )
    stmt = select(func.max(target_table.c[date_column]))
    return target_conn.execute(stmt).scalar_one()


def stream_source_rows(
    source_conn: Connection,
    source_table: Table,
    date_column: str,
    watermark: datetime | None,
    batch_size: int,
    cutoff_lower: datetime | date | None = None,
) -> Iterable[list[dict[str, Any]]]:
    """
    Stream rows from source in ascending date order, filtered by watermark.
    If cutoff_lower is provided, only rows with date_column >= cutoff_lower are fetched.
    """
    if date_column not in source_table.c:
        raise KeyError(
            f"date_column '{date_column}' not found in source table "
            f"{source_table.schema}.{source_table.name}"
        )

    conds = []
    params: dict[str, Any] = {}
    if watermark is not None:
        conds.append(source_table.c[date_column] > bindparam("watermark"))
        params["watermark"] = watermark
    if cutoff_lower is not None:
        conds.append(source_table.c[date_column] >= bindparam("cutoff_lower"))
        params["cutoff_lower"] = cutoff_lower

    stmt = select(source_table)
    if conds:
        stmt = stmt.where(and_(*conds))
    stmt = stmt.order_by(source_table.c[date_column].asc())

    result = source_conn.execute(stmt, params)
    yield from _batch_rows(result, batch_size)


def stream_all_rows(
    source_conn: Connection,
    source_table: Table,
    batch_size: int,
) -> Iterable[list[dict[str, Any]]]:
    """Stream all rows from source without any filtering."""
    result = source_conn.execute(select(source_table))
    yield from _batch_rows(result, batch_size)


def _batch_rows(result, batch_size: int) -> Iterable[list[dict[str, Any]]]:
    batch: list[dict[str, Any]] = []
    for row in result.mappings():
        batch.append(dict(row))
        if len(batch) >= batch_size:
            yield batch
            batch = []
    if batch:
        yield batch


def upsert_batch(
    target_conn: Connection,
    target_table: Table,
    rows: list[dict[str, Any]],
) -> int:
    if not rows:
        return 0

    pk_cols = [c.name for c in target_table.primary_key.columns]

    dialect = target_conn.dialect.name
    if dialect == "postgresql":
        from sqlalchemy.dialects.postgresql import insert as _insert  # noqa: PLC0415
    else:
        from sqlalchemy.dialects.sqlite import insert as _insert  # noqa: PLC0415

    insert_stmt = _insert(target_table).values(rows)

    if pk_cols:
        update_cols = {
            c.name: insert_stmt.excluded[c.name]
            for c in target_table.columns
            if c.name not in pk_cols
        }
        stmt = insert_stmt.on_conflict_do_update(
            index_elements=[target_table.c[name] for name in pk_cols],
            set_=update_cols,
        )
    else:
        stmt = insert_stmt

    target_conn.execute(stmt)
    return len(rows)


def copy_one_table(
    source_conn: Connection,
    target_conn: Connection,
    spec: TableSpec,
    batch_size: int,
    create_table_if_missing: bool,
    full_refresh: bool = False,
) -> bool:
    """Copy one table in process, with no guards and no wire: the core that the source
    and the sink share, for tests and tools. Never raises; returns False on error."""
    from sync_tables.catalog import TABLE, describe_table, relation_kind  # noqa: PLC0415
    from sync_tables.log import log, safe_error  # noqa: PLC0415
    from sync_tables.sink import Sink  # noqa: PLC0415
    from sync_tables.source import Source  # noqa: PLC0415

    label = f"{spec.schema}.{spec.table}"
    if relation_kind(source_conn, spec.schema, spec.table) != TABLE:
        log(f"SKIP {label}: table not found in source (not yet created?)")
        return False
    sink = Sink(target_conn.engine, [spec.schema], identify=lambda c: None,  # type: ignore[arg-type,return-value]
                create_table_if_missing=create_table_if_missing)
    source = Source(source_conn.engine, ("dev", "local"), batch_size=batch_size)
    write = None
    try:
        desc = describe_table(source_conn, spec.schema, spec.table)
        src = reflect_table(source_conn, spec.schema, spec.table)
        with target_conn.engine.connect() as tc:
            raw_wm = sink._watermark(tc, spec.schema, spec.table, spec.date_column)
        plan, rows, note = source._table_plan(source_conn, spec, src, raw_wm, full_refresh)
        log(f"Processing table {label}")
        write = sink.begin_table(desc, plan)
        for batch in rows:
            write.write(batch)
        n = write.commit()
        write = None
        log(f"{label}: {note} -> rows_written={n}")
        return True
    except Exception as e:
        if write is not None:
            write.rollback()
        log(f"ERROR {label}: {safe_error(e)}")
        return False


def _fmt_exc(e: Exception) -> str:
    from sync_tables.log import safe_error  # noqa: PLC0415

    return safe_error(e)
