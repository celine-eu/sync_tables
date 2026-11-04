from __future__ import annotations

try:
    import geoalchemy2  # noqa: F401 — side-effect: registers PostGIS types with SQLAlchemy
except ImportError:
    pass

from sqlalchemy import MetaData, Table, create_engine
from sqlalchemy.engine import Connection, Engine, make_url
from sqlalchemy.exc import NoSuchTableError


def normalize_url(url: str) -> str:
    """Run every Postgres URL on psycopg 3, the one driver this tool ships.

    Connection strings are often written for another client: `postgres://`,
    `postgresql://` (SQLAlchemy reads it as psycopg2) or `postgresql+asyncpg://`.
    The host, credentials and database are kept as they are.
    """
    if url.startswith("postgres://"):
        url = "postgresql://" + url[len("postgres://"):]
    if not url.startswith("postgresql"):
        return url
    u = make_url(url)
    if u.drivername == "postgresql+psycopg":
        return url
    return u.set(drivername="postgresql+psycopg").render_as_string(hide_password=False)


def make_source_engine(url: str, pool_size: int = 5) -> Engine:
    """
    Create a read-only source engine.
    Write protection: 'default_transaction_read_only=on' at session level — PostgreSQL
    rejects any DML/DDL with "cannot execute in a read-only transaction".
    AUTOCOMMIT prevents idle-in-transaction timeouts: each SELECT runs outside a
    transaction block, so the connection is never "idle in transaction" between queries.
    """
    url = normalize_url(url)
    connect_args: dict = {}
    if url.startswith("postgresql"):
        connect_args = {"options": "-c default_transaction_read_only=on"}
    return create_engine(
        url,
        pool_pre_ping=True,
        pool_size=pool_size,
        max_overflow=0,
        connect_args=connect_args,
        isolation_level="AUTOCOMMIT",
    )


def make_engine(url: str, pool_size: int = 5) -> Engine:
    """Create a standard (read-write) engine for the target database.
    pool_size should match the parallelism setting so each worker gets its own connection.
    """
    return create_engine(normalize_url(url), pool_pre_ping=True, pool_size=pool_size, max_overflow=0)


def set_source_read_only(source_conn: Connection) -> None:
    """
    Belt-and-suspenders read-only guard at the session level.
    Called once after opening the source connection, in addition to the
    engine-level connect_args set in make_source_engine().
    """
    source_conn.exec_driver_sql("SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY")


def ensure_postgis(target_conn: Connection) -> None:
    """Create PostGIS on the target when it is missing and the role may create it.
    A role without the privilege (the prod mirror role, D20) relies on it being there."""
    if target_conn.dialect.name != "postgresql":
        return
    present = target_conn.exec_driver_sql("SELECT 1 FROM pg_catalog.pg_extension WHERE extname = 'postgis'").scalar()
    if present is None:
        target_conn.exec_driver_sql("CREATE EXTENSION IF NOT EXISTS postgis")
    target_conn.commit()


def reflect_table(conn: Connection, schema: str, table: str) -> Table:
    md = MetaData(schema=schema)
    try:
        return Table(table, md, autoload_with=conn, schema=schema)
    except NoSuchTableError as e:
        raise NoSuchTableError(f"Table not found: {schema}.{table}") from e


def _col_type_sql(col, dialect) -> str:
    """Compile a column's SQLAlchemy type to a SQL type string."""
    try:
        return col.type.compile(dialect=dialect).lower()
    except Exception:
        return str(col.type).lower()
