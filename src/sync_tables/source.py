"""The source: reads its database and sends the sink what the sink is missing.

Plan W1, W4, S7, W10. It only ever reads, in a read-only transaction. It checks its own
marker, and that the sink reports the edge's destination and a different cluster. Tables
go first, then views in dependency order. A view whose base relations are not all in the
profile refuses the run before any data moves.
"""
from __future__ import annotations

import itertools
import threading
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from typing import Any, Callable

from sqlalchemy.engine import Connection, Engine
from sqlalchemy.sql.sqltypes import Date, DateTime

from sync_tables.catalog import VIEW, describe_table, order_views, relation_kind, view_definition, view_dependencies
from sync_tables.codec import pack, to_columns
from sync_tables.config import TableSpec, parse_cutoff, parse_lookback
from sync_tables.db import reflect_table, set_source_read_only
from sync_tables.envs import Identity, IdentityReader, check_distinct, check_marker_for, expect_env, read_identity
from sync_tables.log import Refused, SyncError, log, safe_error
from sync_tables.protocol import MAX_FRAME, PROTOCOL_VERSION, WINDOW, Channel, ConnectionLost, recv, send
from sync_tables.sync import normalize_watermark, stream_all_rows, stream_source_rows

PG_MAX_PARAMS = 65535


@dataclass
class RunResult:
    ok: list[str] = field(default_factory=list)
    failed: list[str] = field(default_factory=list)
    skipped: list[str] = field(default_factory=list)

    def merge(self, other: "RunResult") -> None:
        self.ok += other.ok
        self.failed += other.failed
        self.skipped += other.skipped


@dataclass
class Plan:
    tables: list[TableSpec]
    views: list[tuple[str, str]]
    skipped: list[str]


class Source:
    def __init__(
        self,
        engine: Engine,
        edge: tuple[str, str],
        identify: IdentityReader | None = None,
        batch_size: int = 5000,
        max_frame: int = MAX_FRAME,
    ) -> None:
        self.engine = engine
        self.edge = edge
        self._identify = identify or (lambda c: read_identity(c, side="source"))
        self.batch_size = batch_size
        self.max_frame = max_frame
        self.identity: Identity | None = None

    def connect(self) -> Connection:
        conn = self.engine.connect()
        if conn.dialect.name == "postgresql":
            set_source_read_only(conn)
        return conn

    # -- before any data moves --------------------------------------------------

    def check_identity(self) -> Identity:
        def read() -> Identity:
            with self.connect() as conn:
                return self._identify(conn)

        self.identity = check_marker_for(read, self.edge[0], "source")
        return self.identity

    def plan(self, specs: list[TableSpec]) -> Plan:
        tables: list[TableSpec] = []
        views: dict[tuple[str, str], set[tuple[str, str]]] = {}
        skipped: list[str] = []
        with self.connect() as conn:
            for spec in specs:
                kind = relation_kind(conn, spec.schema, spec.table)
                if kind is None:
                    log(f"SKIP {spec.schema}.{spec.table}: not found in source (not yet created?)")
                    skipped.append(f"{spec.schema}.{spec.table}")
                elif kind == VIEW:
                    views[(spec.schema, spec.table)] = view_dependencies(conn, spec.schema, spec.table)
                else:
                    tables.append(spec)
        in_profile = {(t.schema, t.table) for t in tables} | set(views)
        for v, deps in sorted(views.items()):
            missing = sorted(deps - in_profile)
            if missing:
                names = ", ".join(f"{s}.{n}" for s, n in missing)
                raise Refused(
                    "view_base_missing",
                    f"view {v[0]}.{v[1]} reads {names}, which the profile does not mirror; add them or drop the view",
                )
        return Plan(tables=tables, views=order_views(views), skipped=skipped)

    # -- a whole run ------------------------------------------------------------

    def run(
        self,
        specs: list[TableSpec],
        open_channel: Callable[[], Channel],
        parallelism: int = 1,
        full_refresh: bool = False,
        retries: int = 1,
    ) -> RunResult:
        self.check_identity()
        plan = self.plan(specs)
        result = RunResult(skipped=list(plan.skipped))

        groups = [plan.tables[i::parallelism] for i in range(max(1, parallelism))]
        groups = [g for g in groups if g]
        errors: list[BaseException] = []
        lock = threading.Lock()

        def work(group: list[TableSpec]) -> None:
            try:
                r = self._run_group(group, [], open_channel, full_refresh, retries)
                with lock:
                    result.merge(r)
            except BaseException as e:  # noqa: BLE001 — re-raised below, on the main thread
                with lock:
                    errors.append(e)

        if len(groups) <= 1:
            for g in groups:
                work(g)
        else:
            log(f"Running with parallelism={len(groups)}")
            threads = [threading.Thread(target=work, args=(g,), name=f"source-{i}") for i, g in enumerate(groups)]
            for t in threads:
                t.start()
            for t in threads:
                t.join()
        if errors:
            raise errors[0]

        if plan.views:
            result.merge(self._run_group([], plan.views, open_channel, full_refresh, retries))
        return result

    def _run_group(
        self,
        tables: list[TableSpec],
        views: list[tuple[str, str]],
        open_channel: Callable[[], Channel],
        full_refresh: bool,
        retries: int,
    ) -> RunResult:
        result = RunResult()
        pending_t, pending_v = list(tables), list(views)
        attempts: dict[str, int] = {}
        while pending_t or pending_v:
            ch = open_channel()
            try:
                self._session(ch, pending_t, pending_v, full_refresh, result)
            except ConnectionLost:
                current = (
                    f"{pending_t[0].schema}.{pending_t[0].table}" if pending_t else f"{pending_v[0][0]}.{pending_v[0][1]}"
                )
                attempts[current] = attempts.get(current, 0) + 1
                if attempts[current] > retries:
                    log(f"ERROR {current}: connection lost, giving up after {retries} retry")
                    result.failed.append(current)
                    if pending_t:
                        pending_t.pop(0)
                    else:
                        pending_v.pop(0)
                else:
                    log(f"  {current}: connection lost, retrying on a fresh connection")
            finally:
                ch.close()
        return result

    # -- one connection ---------------------------------------------------------

    def _session(
        self,
        ch: Channel,
        tables: list[TableSpec],
        views: list[tuple[str, str]],
        full_refresh: bool,
        result: RunResult,
    ) -> None:
        """Offer, then send tables and views, popping each from its list once settled."""
        assert self.identity is not None
        send(ch, {
            "t": "offer",
            "v": PROTOCOL_VERSION,
            "edge": list(self.edge),
            "env": self.identity.env,
            "sysid": self.identity.system_identifier,
            "tables": [{"schema": t.schema, "table": t.table, "date_column": t.date_column} for t in tables],
            "views": [{"schema": s, "name": n} for s, n in views],
        })
        accept = recv(ch, "accept")
        sink_identity = Identity(env=str(accept["env"]), system_identifier=str(accept["sysid"]))
        expect_env(sink_identity, self.edge[1], "target")
        check_distinct(self.identity, sink_identity)
        watermarks: dict[str, Any] = accept.get("watermarks") or {}

        with self.connect() as conn:
            while tables:
                spec = tables[0]
                label = f"{spec.schema}.{spec.table}"
                if self._send_table(ch, conn, spec, watermarks.get(label), full_refresh):
                    result.ok.append(label)
                else:
                    result.failed.append(label)
                tables.pop(0)
            while views:
                schema, name = views[0]
                label = f"{schema}.{name}"
                body = view_definition(conn, schema, name)
                send(ch, {"t": "view", "schema": schema, "name": name, "body": body})
                reply = recv(ch, "ok", "err")
                if reply["t"] == "ok":
                    log(f"{label}: view recreated on the target")
                    result.ok.append(label)
                else:
                    log(f"ERROR {label}: view not recreated [{reply.get('code')}]: {reply.get('message')}")
                    result.failed.append(label)
                views.pop(0)
        send(ch, {"t": "bye"})

    def _send_table(self, ch: Channel, conn: Connection, spec: TableSpec, raw_wm: Any, full_refresh: bool) -> bool:
        label = f"{spec.schema}.{spec.table}"
        started = time.monotonic()
        try:
            desc = describe_table(conn, spec.schema, spec.table)
            src = reflect_table(conn, spec.schema, spec.table)
            plan, rows_iter, note = self._table_plan(conn, spec, src, raw_wm, full_refresh)
            # Read before the sink opens its transaction: the source fetches the whole result
            # on the first batch, and a sink idle meanwhile is killed by the target's
            # idle_in_transaction_session_timeout.
            first = next(rows_iter, None)
            if first is not None:
                rows_iter = itertools.chain([first], rows_iter)
        except ConnectionLost:
            raise
        except Exception as e:
            log(f"ERROR {label}: {safe_error(e)}")
            return False

        log(f"Processing table {label}")
        send(ch, {"t": "table", "desc": desc.to_wire(), "plan": plan})
        names = [c.name for c in desc.columns]
        sent = 0
        unacked = 0
        try:
            for batch in rows_iter:
                for frame in self._frames(names, batch):
                    if unacked >= WINDOW:
                        recv(ch, "ack")
                        unacked -= 1
                    ch.send_raw(frame)
                    unacked += 1
                sent += len(batch)
        except ConnectionLost:
            raise
        except Exception as e:
            log(f"ERROR {label}: reading the source failed: {safe_error(e)}")
            send(ch, {"t": "abort", "code": "source_failed"})
            for _ in range(unacked):
                recv(ch, "ack")
            return False
        send(ch, {"t": "end"})
        reply = recv(ch, "ack", "ok", "err")
        while reply["t"] == "ack":
            reply = recv(ch, "ack", "ok", "err")
        took = time.monotonic() - started
        if reply["t"] == "err":
            log(f"ERROR {label} [{reply.get('code')}]: {reply.get('message')}")
            return False
        log(f"{label}: {note} -> rows_written={reply.get('rows')} ({took:.1f}s)")
        return True

    def _table_plan(self, conn: Connection, spec: TableSpec, src, raw_wm: Any, full_refresh: bool):
        safe_batch = min(self.batch_size, PG_MAX_PARAMS // max(len(src.columns), 1))
        if spec.date_column is None:
            return (
                {"mode": "full", "date_column": None, "truncate": True, "delete_from": None},
                stream_all_rows(conn, src, safe_batch),
                "full sync",
            )
        if spec.date_column not in src.c:
            raise SyncError(f"date_column {spec.date_column!r} not found in source table {spec.schema}.{spec.table}")
        col = src.c[spec.date_column]
        if not isinstance(col.type, (Date, DateTime)):
            raise SyncError(f"{spec.schema}.{spec.table}.{spec.date_column} must be DATE or TIMESTAMP, got {col.type}")

        watermark = None if full_refresh else normalize_watermark(raw_wm, col)

        cutoff_lower: datetime | date | None = None
        if spec.cutoff is not None:
            now = datetime.now(tz=timezone.utc)
            if not getattr(col.type, "timezone", False):
                now = now.replace(tzinfo=None)
            cutoff_lower = normalize_watermark(now - parse_cutoff(spec.cutoff), col)

        delete_from = None
        lower = cutoff_lower
        stream_wm = watermark
        if spec.lookback is not None and watermark is not None:
            # Re-copy the trailing window: rows a producer recomputed inside it (D21).
            delete_from = normalize_watermark(watermark - parse_lookback(spec.lookback), col)
            if cutoff_lower is not None and cutoff_lower > delete_from:
                delete_from = cutoff_lower  # never delete what the cutoff will not re-copy
            lower, stream_wm = delete_from, None

        wm_str = watermark.isoformat() if watermark is not None else "NULL"
        note = f"watermark={wm_str}"
        if spec.cutoff:
            note += f", cutoff={spec.cutoff}"
        if delete_from is not None:
            note += f", lookback={spec.lookback} (re-copy from {delete_from.isoformat()})"
        if full_refresh:
            note += " (full-refresh)"
        plan = {"mode": "incremental", "date_column": spec.date_column, "truncate": full_refresh, "delete_from": delete_from}
        rows = stream_source_rows(conn, src, spec.date_column, stream_wm, safe_batch, cutoff_lower=lower)
        return plan, rows, note

    def _frames(self, names: list[str], batch: list[dict[str, Any]]):
        frame = pack({"t": "rows", "columns": to_columns(batch, names)})
        if len(frame) <= self.max_frame:
            yield frame
            return
        if len(batch) == 1:
            raise SyncError("a single row exceeds the frame size limit")
        mid = len(batch) // 2
        yield from self._frames(names, batch[:mid])
        yield from self._frames(names, batch[mid:])
