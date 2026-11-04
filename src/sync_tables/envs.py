"""Environments, the edges between them, and how each database says which it is.

Plan decision 8 and design item 6. Environment names are free: one lowercase word
(`[a-z][a-z0-9-]*`). One rule is compiled in and no configuration can change it: a sink
marked `prod` accepts only a source marked `dev`. Every other edge comes from
configuration, declared on both sides: the side that dials lists it in the `edges` of its
config file (`EdgePolicy`), and the side that listens names the other end
(`receive --accept-from`, `serve --serve-to`; `check_listener`). A copy needs both, so no
single file opens an edge, and none opens one into prod. The copy runs one way only.
"""
from __future__ import annotations

import re
from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import Any, Callable

from sqlalchemy import text
from sqlalchemy.engine import Connection

from sync_tables.log import Refused, SyncError

DIRECT = "direct"  # in process: the tool reads the source and writes the sink's database itself
PUSH = "push"      # the source dials a sink over WSS
PULL = "pull"      # the sink dials a serving source over WSS (a developer's machine, behind NAT)
TRANSPORTS = (DIRECT, PUSH, PULL)

# The one compiled rule: only this source may write to this target.
PROD, DEV = "prod", "dev"

ENV_NAME = r"[a-z][a-z0-9-]*"
ENV_NAME_RE = re.compile(ENV_NAME)
MARKER_RE = re.compile(rf"celine:env=({ENV_NAME})")


def check_name(env: Any) -> str:
    """Refuse anything that is not an environment name."""
    if not isinstance(env, str) or ENV_NAME_RE.fullmatch(env) is None:
        raise Refused("unknown_environment", f"{env!r} is not an environment name ([a-z][a-z0-9-]*)")
    return env


def check_rule(from_env: Any, to_env: Any) -> None:
    """The compiled rule, which every edge passes before any configuration is read."""
    check_name(from_env)
    check_name(to_env)
    if from_env == to_env:
        raise Refused("edge_not_allowed", f"an edge joins two environments, not {from_env!r} to itself")
    if to_env == PROD and from_env != DEV:
        raise Refused("prod_only_from_dev", f"only {DEV!r} writes to {PROD!r}, not {from_env!r}")


@dataclass(frozen=True)
class EdgePolicy:
    """The edges the dialling side declares, each with its transports."""

    declared: dict[tuple[str, str], tuple[str, ...]] = field(default_factory=dict)

    @classmethod
    def from_config(cls, raw: Any) -> "EdgePolicy":
        """Parse an `edges` list: [{from, to, transports: [push|pull|direct, ...]}, ...]."""
        if raw is None:
            return cls()
        if not isinstance(raw, list):
            raise SyncError("config: `edges` is a list of {from, to, transports}")
        declared: dict[tuple[str, str], tuple[str, ...]] = {}
        for entry in raw:
            if not isinstance(entry, dict) or not {"from", "to", "transports"} <= set(entry):
                raise SyncError("config: each edge has `from`, `to` and `transports`")
            transports = entry["transports"]
            if (not isinstance(transports, list) or not transports
                    or any(t not in TRANSPORTS for t in transports)):
                raise SyncError(f"config: an edge's transports are a list of {', '.join(TRANSPORTS)}")
            edge = (entry["from"], entry["to"])
            check_rule(*edge)
            if edge in declared:
                raise SyncError(f"config: the edge {edge[0]} -> {edge[1]} is declared twice")
            declared[edge] = tuple(dict.fromkeys(transports))
        return cls(declared)

    def transports(self, from_env: str, to_env: str) -> tuple[str, ...]:
        """The edge's transports, or refuse. Runs before any connection is opened."""
        check_rule(from_env, to_env)
        found = self.declared.get((from_env, to_env))
        if found is None:
            known = ", ".join(f"{a} -> {b}" for a, b in self.declared) or "none"
            raise Refused(
                "edge_not_allowed",
                f"{from_env} -> {to_env} is not declared in the `edges` of config.yaml (declared: {known})",
            )
        return found

    def pick(self, from_env: str, to_env: str, wanted: str) -> str:
        """Refuse a transport the edge does not declare; `wanted` follows from the URLs given."""
        transports = self.transports(from_env, to_env)
        if wanted not in transports:
            raise Refused(
                "transport_mismatch",
                f"{from_env} -> {to_env} does not {wanted} (it uses: {', '.join(transports)})",
            )
        return wanted


def check_listener(from_env: Any, to_env: Any, accepted: Iterable[str] | None, end: str) -> None:
    """The listening side's half: the compiled rule, then its own list of the other end.

    `end` is "from" for a sink (the sources it accepts) and "to" for a serving source (the
    targets it serves). `accepted=None` leaves only the rule: the CLI always passes a list.
    """
    check_rule(from_env, to_env)
    if accepted is None:
        return
    other = from_env if end == "from" else to_env
    if other not in set(accepted):
        flag = "--accept-from" if end == "from" else "--serve-to"
        raise Refused("edge_not_allowed", f"{from_env} -> {to_env}: {other!r} is not in this listener's {flag}")


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
