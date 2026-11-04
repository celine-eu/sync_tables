"""What the source says about a relation: its shape, whether it is a view, and what a view
reads. Plain data, so it can cross the wire (plan W5) instead of SQL.
"""
from __future__ import annotations

import re
from dataclasses import asdict, dataclass, field
from typing import Any

from sqlalchemy import inspect as sa_inspect, text
from sqlalchemy.engine import Connection

from sync_tables.db import _col_type_sql, reflect_table
from sync_tables.log import SyncError, log

# A column type as a dialect compiles it, and nothing more: one type name (optionally
# schema-qualified or quoted), its modifiers, an optional time zone clause and array
# brackets. `timestamp(6) with time zone`, `numeric(10, 2)`, `geometry(POINT,4326)`,
# `varchar[]`, `"public"."my_enum"`. The type lands in CREATE TABLE and ALTER TABLE, so a
# looser pattern would let a type carry a DEFAULT, a CHECK, a GENERATED expression or an
# extra column (each of which runs a function on the target): the grammar leaves no room
# for a second word, a nested call or a comma outside the modifiers.
_IDENT = r'(?:[a-z_][a-z0-9_$]*|"[^"\x00]{1,63}")'
_TYPE_GRAMMAR = (
    r"(?:double precision|character varying|bit varying|"
    rf"{_IDENT}(?:\.{_IDENT})?)"
    r"(?:\s*\(\s*(?:\d+|[a-z][a-z0-9_]*)\s*(?:,\s*-?\d+\s*)*\))?"
    r"(?:\s+with(?:out)?\s+time\s+zone)?"
    r"(?:\[\d*\])*"
)
TYPE_RE = re.compile(_TYPE_GRAMMAR, re.IGNORECASE)
TYPE_MAX = 200


def check_type_sql(type_sql: str) -> str:
    if len(type_sql) > TYPE_MAX or not TYPE_RE.fullmatch(type_sql):
        raise SyncError("column type is not a plain type name")
    return type_sql


# An index's access method lands in CREATE INDEX ... USING, so it is a word from Postgres's
# built-in methods, never free text. A GiST index rebuilt as a btree cannot hold a large
# geometry (ProgramLimitExceeded), so the method travels with the columns.
INDEX_METHODS = frozenset({"btree", "hash", "gist", "spgist", "gin", "brin"})


@dataclass
class ColumnDesc:
    name: str
    type_sql: str
    nullable: bool


@dataclass
class IndexDesc:
    name: str
    columns: list[str]
    unique: bool
    method: str = "btree"


@dataclass
class TableDesc:
    schema: str
    name: str
    columns: list[ColumnDesc]
    primary_key: list[str]
    indexes: list[IndexDesc] = field(default_factory=list)

    @property
    def label(self) -> str:
        return f"{self.schema}.{self.name}"

    def to_wire(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_wire(cls, d: dict[str, Any]) -> "TableDesc":
        try:
            return cls(
                schema=str(d["schema"]),
                name=str(d["name"]),
                columns=[
                    ColumnDesc(str(c["name"]), check_type_sql(str(c["type_sql"])), bool(c["nullable"]))
                    for c in d["columns"]
                ],
                primary_key=[str(c) for c in d["primary_key"]],
                indexes=[
                    IndexDesc(str(i["name"]), [str(c) for c in i["columns"]], bool(i["unique"]), _index_method(i))
                    for i in d["indexes"]
                ],
            )
        except (KeyError, TypeError) as e:
            raise SyncError(f"malformed table description ({type(e).__name__})") from None


def _index_method(wire: dict[str, Any]) -> str:
    method = str(wire.get("method", "btree"))
    if method not in INDEX_METHODS:
        raise SyncError("malformed table description (index method)")
    return method


def describe_table(conn: Connection, schema: str, name: str) -> TableDesc:
    table = reflect_table(conn, schema, name)
    dialect = conn.dialect
    insp = sa_inspect(conn)
    pk = insp.get_pk_constraint(name, schema=schema) or {}
    indexes = []
    for idx in insp.get_indexes(name, schema=schema):
        if idx["name"] == pk.get("name"):
            continue
        cols = idx.get("column_names") or []
        if not cols or any(c is None for c in cols):
            log(f"  index [{name}]: skipping expression index {idx['name']!r}")
            continue
        options = idx.get("dialect_options") or {}
        method = str(options.get("postgresql_using") or "btree")
        if method not in INDEX_METHODS:
            log(f"  index [{name}]: skipping index {idx['name']!r} (access method {method!r})")
            continue
        if options.get("postgresql_ops"):
            log(f"  index [{name}]: skipping index {idx['name']!r} (non-default operator class)")
            continue
        indexes.append(IndexDesc(idx["name"], list(cols), bool(idx.get("unique")), method))
    return TableDesc(
        schema=schema,
        name=name,
        columns=[ColumnDesc(c.name, _col_type_sql(c, dialect), bool(c.nullable)) for c in table.columns],
        primary_key=[c.name for c in table.primary_key.columns],
        indexes=indexes,
    )


# ---------------------------------------------------------------------------
# Relation kinds and views (plan S7, D20)
# ---------------------------------------------------------------------------

TABLE = "table"
VIEW = "view"


def relation_kind(conn: Connection, schema: str, name: str) -> str | None:
    """'table' (tables, partitioned and foreign tables, materialized views), 'view', or None."""
    if conn.dialect.name == "postgresql":
        kind = conn.execute(
            text(
                "SELECT c.relkind FROM pg_catalog.pg_class c "
                "JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace "
                "WHERE n.nspname = :s AND c.relname = :n"
            ),
            {"s": schema, "n": name},
        ).scalar_one_or_none()
        if kind is None:
            return None
        return VIEW if kind == "v" else TABLE
    if conn.dialect.name == "sqlite":
        kind = conn.execute(
            text(f"SELECT type FROM {_sqlite_master(schema)} WHERE name = :n AND type IN ('table', 'view')"),
            {"n": name},
        ).scalar_one_or_none()
        return None if kind is None else (VIEW if kind == "view" else TABLE)
    raise SyncError(f"unsupported source dialect {conn.dialect.name}")


def view_definition(conn: Connection, schema: str, name: str) -> str:
    """The view's SELECT, with every name schema-qualified so it resolves the same anywhere."""
    if conn.dialect.name == "postgresql":
        # An empty search_path makes pg_get_viewdef qualify every relation and function.
        conn.execute(text("SELECT pg_catalog.set_config('search_path', '', false)"))
        try:
            body = conn.execute(
                text(
                    "SELECT pg_catalog.pg_get_viewdef(c.oid, true) FROM pg_catalog.pg_class c "
                    "JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace "
                    "WHERE n.nspname = :s AND c.relname = :n"
                ),
                {"s": schema, "n": name},
            ).scalar_one()
        finally:
            conn.execute(text("RESET search_path"))
        return body.strip().rstrip(";").strip()
    if conn.dialect.name == "sqlite":
        sql = conn.execute(
            text(f"SELECT sql FROM {_sqlite_master(schema)} WHERE type = 'view' AND name = :n"), {"n": name}
        ).scalar_one()
        m = re.search(r"\bAS\b(.*)$", sql, re.IGNORECASE | re.DOTALL)
        if m is None:
            raise SyncError(f"cannot read the definition of view {schema}.{name}")
        return m.group(1).strip().rstrip(";").strip()
    raise SyncError(f"unsupported source dialect {conn.dialect.name}")


def view_dependencies(conn: Connection, schema: str, name: str) -> set[tuple[str, str]]:
    """The relations a view reads directly, outside the system catalogs."""
    if conn.dialect.name == "postgresql":
        rows = conn.execute(
            text(
                "SELECT DISTINCT rn.nspname, rc.relname "
                "FROM pg_catalog.pg_class v "
                "JOIN pg_catalog.pg_namespace vn ON vn.oid = v.relnamespace "
                "JOIN pg_catalog.pg_rewrite r ON r.ev_class = v.oid "
                "JOIN pg_catalog.pg_depend d ON d.objid = r.oid "
                " AND d.classid = 'pg_catalog.pg_rewrite'::regclass "
                " AND d.refclassid = 'pg_catalog.pg_class'::regclass "
                "JOIN pg_catalog.pg_class rc ON rc.oid = d.refobjid "
                "JOIN pg_catalog.pg_namespace rn ON rn.oid = rc.relnamespace "
                "WHERE vn.nspname = :s AND v.relname = :n AND rc.oid <> v.oid "
                " AND rn.nspname NOT IN ('pg_catalog', 'information_schema')"
            ),
            {"s": schema, "n": name},
        ).all()
        return {(r[0], r[1]) for r in rows}
    if conn.dialect.name == "sqlite":
        # SQLite keeps no dependency catalog. Test databases only: any relation of the
        # same schema whose name appears as a word in the definition.
        body = view_definition(conn, schema, name)
        names = conn.execute(
            text(f"SELECT name FROM {_sqlite_master(schema)} WHERE type IN ('table', 'view') AND name <> :n"),
            {"n": name},
        ).scalars()
        return {(schema, n) for n in names if re.search(rf"\b{re.escape(n)}\b", body)}
    raise SyncError(f"unsupported source dialect {conn.dialect.name}")


def order_views(views: dict[tuple[str, str], set[tuple[str, str]]]) -> list[tuple[str, str]]:
    """Views after the views they read (tables all come first anyway)."""
    ordered: list[tuple[str, str]] = []
    done: set[tuple[str, str]] = set()
    visiting: set[tuple[str, str]] = set()

    def visit(v: tuple[str, str]) -> None:
        if v in done:
            return
        if v in visiting:
            raise SyncError(f"views depend on each other in a cycle at {v[0]}.{v[1]}")
        visiting.add(v)
        for dep in sorted(views[v]):
            if dep in views:
                visit(dep)
        visiting.discard(v)
        done.add(v)
        ordered.append(v)

    for v in sorted(views):
        visit(v)
    return ordered


def _sqlite_master(schema: str) -> str:
    return "sqlite_master" if schema == "main" else f'"{schema}".sqlite_master'
