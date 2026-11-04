"""Environments, the edges between them, and how each database says which it is.

Plan S1–S4 and W2. The allowed edges and their transports are compiled in: nothing in a
config file, a variable or a profile can add one. The copy runs one way only.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Callable

from sqlalchemy import text
from sqlalchemy.engine import Connection

from sync_tables.log import Refused

ENVIRONMENTS = ("local", "dev", "prod", "staging", "preprod")

DIRECT = "direct"  # in process: the tool reads the source and writes the sink's database itself
PUSH = "push"      # the source dials a sink over WSS
PULL = "pull"      # the sink dials a serving source over WSS (a developer's machine, behind NAT)

# (from, to) -> the transports it may use. Every other pair, including the reverse of each,
# is refused. Whichever side dials, the rows flow from the source to the sink.
EDGES: dict[tuple[str, str], tuple[str, ...]] = {
    ("dev", "local"): (PULL, DIRECT),
    ("dev", "prod"): (PUSH,),
    # staging and preprod: read-only copies of the warehouse's output, like prod; they send
    # nothing on, to prod or anywhere else.
    ("dev", "staging"): (PUSH,),
    ("dev", "preprod"): (PUSH,),
}

MARKER_RE = re.compile(r"celine:env=(local|dev|prod|staging|preprod)")


def check_edge(from_env: str, to_env: str) -> tuple[str, ...]:
    """Return the edge's transports, or refuse. Runs before any connection is opened."""
    for env in (from_env, to_env):
        if env not in ENVIRONMENTS:
            raise Refused("unknown_environment", f"{env!r} is not one of {', '.join(ENVIRONMENTS)}")
    transport = EDGES.get((from_env, to_env))
    if transport is None:
        allowed = ", ".join(f"{a} -> {b}" for a, b in EDGES)
        raise Refused("edge_not_allowed", f"{from_env} -> {to_env} is not an allowed edge (allowed: {allowed})")
    return transport


def pick_transport(from_env: str, to_env: str, wanted: str) -> str:
    """Refuse a transport the edge does not use; `wanted` follows from the URLs given."""
    transports = check_edge(from_env, to_env)
    if wanted not in transports:
        raise Refused(
            "transport_mismatch",
            f"{from_env} -> {to_env} does not {wanted} (it uses: {', '.join(transports)})",
        )
    return wanted


def destinations_from(from_env: str) -> set[str]:
    return {b for (a, b) in EDGES if a == from_env}


@dataclass(frozen=True)
class Identity:
    """What a database says about itself: its environment marker and its cluster."""
    env: str
    system_identifier: str


IdentityReader = Callable[[Connection], Identity]

# Fully qualified, so neither search_path nor any `options=-c …` on the connection can
# redirect what is read. Database-level settings were rejected for the same reason (S3).
_MARKER_SQL = text(
    "SELECT pg_catalog.shobj_description(d.oid, 'pg_database') "
    "FROM pg_catalog.pg_database d WHERE d.datname = pg_catalog.current_database()"
)
_SYSID_SQL = text("SELECT system_identifier::text FROM pg_catalog.pg_control_system()")

LOCAL_MARKER_HINT = "mark it once, as its owner: COMMENT ON DATABASE <db> IS 'celine:env=local'"


def parse_marker(comment: str | None, side: str) -> str:
    if comment is None or comment.strip() == "":
        raise Refused("marker_missing", f"the {side} database carries no environment marker")
    m = MARKER_RE.fullmatch(comment.strip())
    if m is None:
        raise Refused("marker_malformed", f"the {side} database's comment is not a celine:env=<env> marker")
    return m.group(1)


def read_identity(conn: Connection, side: str = "database") -> Identity:
    """Read a Postgres database's marker and system identifier."""
    if conn.dialect.name != "postgresql":
        raise Refused("marker_unsupported", f"the {side} database is not Postgres; it cannot carry a marker")
    comment = conn.execute(_MARKER_SQL).scalar_one_or_none()
    env = parse_marker(comment, side)
    sysid = conn.execute(_SYSID_SQL).scalar_one()
    return Identity(env=env, system_identifier=str(sysid))


def expect_env(identity: Identity, expected: str, side: str) -> None:
    if identity.env != expected:
        hint = f"; {LOCAL_MARKER_HINT}" if expected == "local" else ""
        raise Refused(
            "marker_mismatch",
            f"the {side} database says it is {identity.env!r}, the edge needs {expected!r}{hint}",
        )


def check_marker_for(conn_reader: Callable[[], Identity], expected: str, side: str) -> Identity:
    """Read and check a marker; a missing local marker names the task that sets it."""
    try:
        identity = conn_reader()
    except Refused as r:
        if expected == "local" and r.code == "marker_missing":
            raise Refused(r.code, f"{r.message}; {LOCAL_MARKER_HINT}") from None
        raise
    expect_env(identity, expected, side)
    return identity


def check_distinct(source: Identity, target: Identity) -> None:
    if source.system_identifier == target.system_identifier:
        raise Refused("same_cluster", "source and target are the same Postgres cluster")
