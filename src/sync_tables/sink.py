"""The sink: holds a database, says what it has, and applies what the source sends.

Plan W1, W3, W4, S5, S7, W14. The sink trusts nothing the source claims about the sink's
own database. It reads its own marker and cluster, writes only inside its allowlisted
schemas, and builds every statement from a table description and bound parameters. A
view body is the one piece of SQL it accepts, and only as the body of a CREATE VIEW in an
allowlisted schema.

Each table is applied in one transaction: a dropped connection or an error rolls it back
whole, and the next run converges from the sink's watermark.
"""
from __future__ import annotations

from typing import Any, Iterable

from sqlalchemy import MetaData, Table, func, select, text
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.exc import NoSuchTableError

from sync_tables.catalog import TableDesc
from sync_tables.db import _col_type_sql
from sync_tables.envs import (
    Identity,
    IdentityReader,
    check_distinct,
    check_listener,
    check_marker_for,
    read_identity,
)
from sync_tables.log import Refused, SyncError, log, safe_error
from sync_tables.publish import Publisher, publish_items
from sync_tables.sync import normalize_watermark, upsert_batch

PG_MAX_PARAMS = 65535


class Sink:
    def __init__(
        self,
        engine: Engine,
        allowed_schemas: Iterable[str],
        identify: IdentityReader | None = None,
        create_table_if_missing: bool = True,
        log_summaries: bool = False,
        publisher: "Publisher | None" = None,
        accept_from: Iterable[str] | None = None,
    ) -> None:
        """`accept_from`: the sources this sink accepts (`receive --accept-from`). None leaves
        only the compiled rule; the CLI always passes a list."""
        self.engine = engine
        self.allowed_schemas = frozenset(allowed_schemas)
        if not self.allowed_schemas:
            raise SyncError("the sink needs at least one allowed schema")
        self._identify = identify or (lambda c: read_identity(c, side="target"))
        self.create_table_if_missing = create_table_if_missing
        self.log_summaries = log_summaries
        self.publisher = publisher
        self.accept_from = None if accept_from is None else frozenset(accept_from)

    @property
    def caps(self) -> list[str]:
        """What this sink advertises in `accept`: `events` when it can publish them."""
        return ["events"] if self.publisher is not None else []

    def publish_events(self, items: Any) -> int:
        """Publish the run events of a push (`publish.publish_items`); never raises.

        A broker failure counts as nothing published: the tables are committed already,
        and the push stays `ok`.
        """
        if self.publisher is None:
            raise Refused("events_unsupported", "this sink has no publisher")
        return publish_items(self.publisher, items)

    # -- guards ---------------------------------------------------------------

    def check_schema(self, schema: str) -> None:
        if schema not in self.allowed_schemas:
            raise Refused("schema_not_allowed", f"schema {schema!r} is not in the sink's allowlist")

    def identity_for(self, expected_env: str) -> Identity:
        def read() -> Identity:
            with self.engine.connect() as conn:
                return self._identify(conn)

        return check_marker_for(read, expected_env, "target")

    def accept(self, offer: dict[str, Any]) -> dict[str, Any]:
        """Check an offer against what the sink reads for itself; return its state."""
        try:
            from_env, to_env = (str(x) for x in offer["edge"])
            source = Identity(env=str(offer["env"]), system_identifier=str(offer["sysid"]))
            tables = [(str(t["schema"]), str(t["table"]), t.get("date_column")) for t in offer["tables"]]
            views = [(str(v["schema"]), str(v["name"])) for v in offer["views"]]
        except (KeyError, TypeError, ValueError):
            raise Refused("bad_message", "malformed offer") from None

        check_listener(from_env, to_env, self.accept_from, end="from")
        if source.env != from_env:
            raise Refused("marker_mismatch", f"the source says it is {source.env!r}, the edge starts at {from_env!r}")
        mine = self.identity_for(to_env)
        check_distinct(source, mine)
        for schema, *_ in tables + views:
            self.check_schema(schema)

        watermarks: dict[str, Any] = {}
        with self.engine.connect() as conn:
            for schema, table, date_column in tables:
                watermarks[f"{schema}.{table}"] = self._watermark(conn, schema, table, date_column)
        return {"env": mine.env, "sysid": mine.system_identifier, "watermarks": watermarks}

    def _watermark(self, conn: Connection, schema: str, table: str, date_column: str | None) -> Any:
        if date_column is None:
            return None
        try:
            tgt = _reflect(conn, schema, table)
        except NoSuchTableError:
            return None
        if date_column not in tgt.c:
            return None
        # Typed through the reflected column, so a timestamp comes back as one.
        return conn.execute(select(func.max(tgt.c[date_column]))).scalar_one()

    # -- applying a table -----------------------------------------------------

    def begin_table(self, desc: TableDesc, plan: dict[str, Any]) -> "TableWrite":
        self.check_schema(desc.schema)
        return TableWrite(self, desc, plan)

    def apply_view(self, schema: str, name: str, body: str) -> None:
        self.check_schema(schema)
        if not isinstance(body, str) or not body.strip():
            raise SyncError("empty view definition")
        with self.engine.connect() as conn:
            with conn.begin():
                _ensure_schema(conn, schema)
                qualified = _qualified(conn, schema, name)
                if conn.dialect.name == "postgresql":
                    kind = conn.execute(
                        text(
                            "SELECT c.relkind FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n "
                            "ON n.oid = c.relnamespace WHERE n.nspname = :s AND c.relname = :n"
                        ),
                        {"s": schema, "n": name},
                    ).scalar_one_or_none()
                    if kind is not None and kind != "v":
                        raise Refused(
                            "view_name_taken",
                            f"{schema}.{name} exists on the target as a non-view relation; "
                            "drop it by hand once, then re-run",
                        )
                    try:
                        with conn.begin_nested():
                            _exec_single(conn, f"CREATE OR REPLACE VIEW {qualified} AS {body}")
                    except Exception:
                        # Columns changed in a way CREATE OR REPLACE cannot follow. Without
                        # CASCADE: a view something else depends on stays, and this fails.
                        _exec_single(conn, f"DROP VIEW IF EXISTS {qualified}")
                        _exec_single(conn, f"CREATE VIEW {qualified} AS {body}")
                else:
                    kind = conn.execute(
                        text(f"SELECT type FROM {_sqlite_master(schema)} WHERE name = :n"), {"n": name}
                    ).scalar_one_or_none()
                    if kind is not None and kind != "view":
                        raise Refused("view_name_taken", f"{schema}.{name} exists on the target as a table")
                    conn.exec_driver_sql(f"DROP VIEW IF EXISTS {qualified}")
                    conn.exec_driver_sql(f"CREATE VIEW {qualified} AS {body}")
        if self.log_summaries:
            log(f"{schema}.{name}: view recreated")

    @staticmethod
    def _q(conn: Connection):
        return conn.dialect.identifier_preparer.quote


class TableWrite:
    """One table, one connection, one transaction."""

    def __init__(self, sink: Sink, desc: TableDesc, plan: dict[str, Any]) -> None:
        self.sink = sink
        self.desc = desc
        self.plan = plan
        self.rows = 0
        self.conn = sink.engine.connect()
        self.tx = self.conn.begin()
        try:
            self.table = self._prepare()
        except BaseException:
            self.rollback()
            raise
        n_cols = max(len(self.table.columns), 1)
        self.chunk = max(1, PG_MAX_PARAMS // n_cols)

    def _prepare(self) -> Table:
        conn, desc = self.conn, self.desc
        _ensure_schema(conn, desc.schema)
        try:
            tgt = _reflect(conn, desc.schema, desc.name)
        except NoSuchTableError:
            if not self.sink.create_table_if_missing:
                raise SyncError(f"{desc.label} does not exist on the target") from None
            _create_table(conn, desc)
            tgt = _reflect(conn, desc.schema, desc.name)
        _align_columns(conn, desc, tgt)
        _align_indexes(conn, desc)
        tgt = _reflect(conn, desc.schema, desc.name)

        if self.plan.get("truncate"):
            _truncate(conn, desc.schema, desc.name)
        delete_from = self.plan.get("delete_from")
        if delete_from is not None:
            dc = self.plan.get("date_column")
            if dc is None or dc not in tgt.c:
                raise SyncError(f"{desc.label}: a window delete needs its date column")
            delete_from = normalize_watermark(delete_from, tgt.c[dc])
            # Typed through the reflected column, as the rows that land are.
            conn.execute(tgt.delete().where(tgt.c[dc] >= delete_from))
        return tgt

    def write(self, rows: list[dict[str, Any]]) -> None:
        if self.plan.get("mode") == "full":
            for i in range(0, len(rows), self.chunk):
                self.conn.execute(self.table.insert(), rows[i : i + self.chunk])
        else:
            for i in range(0, len(rows), self.chunk):
                upsert_batch(self.conn, self.table, rows[i : i + self.chunk])
        self.rows += len(rows)

    def commit(self) -> int:
        try:
            self.tx.commit()
        finally:
            self.conn.close()
        return self.rows

    def rollback(self) -> None:
        try:
            if self.tx.is_active:
                self.tx.rollback()
        finally:
            self.conn.close()


# ---------------------------------------------------------------------------
# DDL built from a description
# ---------------------------------------------------------------------------

def _qualified(conn: Connection, schema: str, name: str) -> str:
    q = conn.dialect.identifier_preparer.quote
    return f"{q(schema)}.{q(name)}"


def _sqlite_master(schema: str) -> str:
    return "sqlite_master" if schema == "main" else f'"{schema}".sqlite_master'


def _reflect(conn: Connection, schema: str, name: str) -> Table:
    return Table(name, MetaData(schema=schema), autoload_with=conn, schema=schema)


def _ensure_schema(conn: Connection, schema: str) -> None:
    if conn.dialect.name != "postgresql":
        return
    # Check first: CREATE SCHEMA IF NOT EXISTS checks the database privilege before it
    # checks existence, and the prod role (D20) has no CREATE on the database.
    exists = conn.execute(
        text("SELECT 1 FROM pg_catalog.pg_namespace WHERE nspname = :s"), {"s": schema}
    ).scalar_one_or_none()
    if exists is None:
        conn.exec_driver_sql(f"CREATE SCHEMA {conn.dialect.identifier_preparer.quote(schema)}")


def _create_table(conn: Connection, desc: TableDesc) -> None:
    q = conn.dialect.identifier_preparer.quote
    cols = [f"{q(c.name)} {c.type_sql}{'' if c.nullable else ' NOT NULL'}" for c in desc.columns]
    if desc.primary_key:
        cols.append(f"PRIMARY KEY ({', '.join(q(c) for c in desc.primary_key)})")
    conn.exec_driver_sql(f"CREATE TABLE {_qualified(conn, desc.schema, desc.name)} ({', '.join(cols)})")
    log(f"  {desc.label}: created on the target")


def _tolerant(conn: Connection, sql: str) -> None:
    """A statement whose failure is reported and survived, without losing the transaction."""
    if conn.dialect.name == "postgresql":
        with conn.begin_nested():
            conn.exec_driver_sql(sql)
    else:
        conn.exec_driver_sql(sql)  # SQLite: a failed statement leaves the transaction usable


def _align_columns(conn: Connection, desc: TableDesc, tgt: Table) -> None:
    """Make the target's columns match the source's (each change survived on its own)."""
    name = desc.name
    qualified = _qualified(conn, desc.schema, name)
    q = conn.dialect.identifier_preparer.quote
    src_cols = {c.name: c for c in desc.columns}
    tgt_cols = {c.name: c for c in tgt.columns}

    for col_name, src_col in src_cols.items():
        if col_name not in tgt_cols:
            conn.exec_driver_sql(f"ALTER TABLE {qualified} ADD COLUMN {q(col_name)} {src_col.type_sql}")
            log(f"  schema drift [{name}]: added column {col_name!r} ({src_col.type_sql})")

    # A column a view depends on cannot be dropped: the target has drifted ahead of the
    # source (e.g. a local migration). It is kept, and made nullable so rows that carry
    # no value for it can still land.
    for col_name, tgt_col in tgt_cols.items():
        if col_name in src_cols:
            continue
        try:
            _tolerant(conn, f"ALTER TABLE {qualified} DROP COLUMN {q(col_name)}")
            log(f"  schema drift [{name}]: dropped column {col_name!r}")
            continue
        except Exception as e:
            log(
                f"  WARNING [{name}]: could not drop {col_name!r}, kept "
                f"(drop the dependent views and re-run for an exact copy): {safe_error(e)}"
            )
        if tgt_col.nullable:
            continue
        try:
            _tolerant(conn, f"ALTER TABLE {qualified} ALTER COLUMN {q(col_name)} DROP NOT NULL")
            log(f"  schema drift [{name}]: kept column {col_name!r} made nullable")
        except Exception as e:
            log(f"  WARNING [{name}]: could not make {col_name!r} nullable: {safe_error(e)}")

    for col_name, src_col in src_cols.items():
        if col_name not in tgt_cols:
            continue
        tgt_type = _col_type_sql(tgt_cols[col_name], conn.dialect)
        if src_col.type_sql.lower() == tgt_type:
            continue
        try:
            _tolerant(
                conn,
                f"ALTER TABLE {qualified} ALTER COLUMN {q(col_name)} TYPE {src_col.type_sql} "
                f"USING {q(col_name)}::{src_col.type_sql}",
            )
            log(f"  schema drift [{name}]: {col_name!r} type {tgt_type!r} -> {src_col.type_sql!r}")
        except Exception as e:
            log(f"  WARNING [{name}]: could not change {col_name!r} type ({tgt_type} -> {src_col.type_sql}): {safe_error(e)}")


def _align_indexes(conn: Connection, desc: TableDesc) -> None:
    from sqlalchemy import inspect as sa_inspect  # noqa: PLC0415

    existing = {i["name"] for i in sa_inspect(conn).get_indexes(desc.name, schema=desc.schema)}
    q = conn.dialect.identifier_preparer.quote
    for idx in desc.indexes:
        if idx.name in existing:
            continue
        unique = "UNIQUE " if idx.unique else ""
        cols = ", ".join(q(c) for c in idx.columns)
        if conn.dialect.name != "postgresql" and idx.method != "btree":
            log(f"  index [{desc.name}]: skipping {idx.method} index {idx.name!r} (not on this target)")
            continue
        try:
            if conn.dialect.name == "postgresql":
                # Index names are per schema in Postgres, so the bare name is right. The
                # method is one of catalog.INDEX_METHODS (checked on the wire).
                sql = (f"CREATE {unique}INDEX IF NOT EXISTS {q(idx.name)} ON {_qualified(conn, desc.schema, desc.name)} "
                       f"USING {idx.method} ({cols})")
            else:
                sql = f"CREATE {unique}INDEX IF NOT EXISTS {q(desc.schema)}.{q(idx.name)} ON {q(desc.name)} ({cols})"
            _tolerant(conn, sql)
            log(f"  index [{desc.name}]: created {unique}{idx.method} index {idx.name!r} on ({cols})")
        except Exception as e:
            log(f"  WARNING [{desc.name}]: could not create index {idx.name!r}: {safe_error(e)}")


def _truncate(conn: Connection, schema: str, name: str) -> None:
    if conn.dialect.name == "postgresql":
        conn.exec_driver_sql(f"TRUNCATE TABLE {_qualified(conn, schema, name)}")
    else:
        conn.exec_driver_sql(f"DELETE FROM {_qualified(conn, schema, name)}")


def _exec_single(conn: Connection, sql: str) -> None:
    """Run exactly one statement. psycopg sends a statement without parameters by the
    simple query protocol, which runs several `;`-separated statements; a prepared
    statement (extended protocol) refuses more than one."""
    raw = conn.connection.driver_connection
    with raw.cursor() as cur:
        cur.execute(sql, prepare=True)

