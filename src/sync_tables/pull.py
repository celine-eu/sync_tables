"""The pull edge: the sink dials, the source serves (dev -> local without a port-forward).

A developer's machine cannot be reached, so on `dev -> local` the local sink dials a source
serving beside dev's Postgres. The rows still flow one way only: the dialling side asks for
tables, and the serving side runs the same offer/accept/table conversation as a push.

The serving source trusts nothing the request claims. It reads its own marker, refuses an
edge that does not start at it or does not pull, reads only inside its own schema allowlist
and only in a read-only transaction. The sink checks its own marker as on every edge.
"""
from __future__ import annotations

import threading
from typing import Any, Callable, Iterable

from sqlalchemy.engine import Engine

from sync_tables.config import TableSpec, parse_cutoff, parse_lookback
from sync_tables.envs import IdentityReader, check_listener
from sync_tables.log import Refused, SyncError, log, safe_error
from sync_tables.protocol import PROTOCOL_VERSION, Channel, ConnectionLost, recv, send, serve_sink
from sync_tables.source import RunResult, Source

MAX_REQUEST_TABLES = 1000


def _label(spec: TableSpec) -> str:
    return f"{spec.schema}.{spec.table}"


def specs_to_wire(specs: Iterable[TableSpec]) -> list[dict[str, Any]]:
    return [
        {"schema": s.schema, "table": s.table, "date_column": s.date_column, "cutoff": s.cutoff, "lookback": s.lookback}
        for s in specs
    ]


def specs_from_wire(raw: Any) -> list[TableSpec]:
    if not isinstance(raw, list) or not raw or len(raw) > MAX_REQUEST_TABLES:
        raise Refused("bad_message", f"a request names 1 to {MAX_REQUEST_TABLES} tables")
    specs: list[TableSpec] = []
    for t in raw:
        try:
            spec = TableSpec(
                schema=str(t["schema"]),
                table=str(t["table"]),
                date_column=None if t.get("date_column") is None else str(t["date_column"]),
                cutoff=None if t.get("cutoff") is None else str(t["cutoff"]),
                lookback=None if t.get("lookback") is None else str(t["lookback"]),
            )
            if spec.cutoff is not None:
                parse_cutoff(spec.cutoff)
            if spec.lookback is not None:
                parse_lookback(spec.lookback)
        except (KeyError, TypeError, ValueError, AttributeError):
            raise Refused("bad_message", "malformed table in the request") from None
        if (spec.cutoff or spec.lookback) and spec.date_column is None:
            raise Refused("bad_message", f"{_label(spec)}: cutoff and lookback need a date_column")
        specs.append(spec)
    return specs


# ---------------------------------------------------------------------------
# The serving source (beside dev's Postgres)
# ---------------------------------------------------------------------------

class SourceServer:
    def __init__(
        self,
        engine: Engine,
        allowed_schemas: Iterable[str],
        identify: IdentityReader | None = None,
        batch_size: int = 5000,
        serve_to: Iterable[str] | None = None,
    ) -> None:
        """`serve_to`: the targets this source serves (`serve --serve-to`). None leaves only
        the compiled rule; the CLI always passes a list."""
        self.engine = engine
        self.serve_to = None if serve_to is None else frozenset(serve_to)
        self.allowed_schemas = frozenset(allowed_schemas)
        if not self.allowed_schemas:
            raise SyncError("the serving source needs at least one allowed schema")
        self._identify = identify
        self.batch_size = batch_size

    def serve(self, ch: Channel) -> None:
        """Serve one connection: a plan, or one run of the tables it names."""
        try:
            req = recv(ch, "request")
            source, specs, only, phase, full_refresh = self._check(req)
            plan = source.plan(specs)
        except Refused as r:
            log(str(r))
            _try_send(ch, {"t": "refuse", "code": r.code, "message": r.message})
            return
        except ConnectionLost:
            return
        except Exception as e:  # a bad frame, or the database: refuse without detail
            log(f"REFUSED [bad_request]: {safe_error(e)}")
            _try_send(ch, {"t": "refuse", "code": "bad_request", "message": safe_error(e)})
            return

        if phase == "plan":
            send(ch, {
                "t": "plan",
                "tables": [_label(t) for t in plan.tables],
                "views": [f"{s}.{n}" for s, n in plan.views],
                "skipped": plan.skipped,
            })
            return

        tables = [t for t in plan.tables if _label(t) in only]
        views = [v for v in plan.views if f"{v[0]}.{v[1]}" in only]
        result = RunResult()
        try:
            source._session(ch, tables, views, full_refresh, result)
        except ConnectionLost:
            log("pull: connection lost; the sink rolls back the table in flight")
        except Refused as r:
            log(str(r))
        except Exception as e:
            log(f"ERROR pull: {safe_error(e)}")
        if result.ok or result.failed:
            log(f"pull: served {len(result.ok)} ok, {len(result.failed)} failed")

    def _check(self, req: dict[str, Any]):
        if req.get("v") != PROTOCOL_VERSION:
            raise Refused("protocol_version", f"protocol version {req.get('v')!r} is not {PROTOCOL_VERSION}")
        try:
            from_env, to_env = (str(x) for x in req["edge"])
        except (KeyError, TypeError, ValueError):
            raise Refused("bad_message", "malformed edge") from None
        check_listener(from_env, to_env, self.serve_to, end="to")
        phase = req.get("phase")
        if phase not in ("plan", "run"):
            raise Refused("bad_message", "a request's phase is plan or run")
        specs = specs_from_wire(req.get("tables"))
        for spec in specs:
            if spec.schema not in self.allowed_schemas:
                raise Refused("schema_not_allowed", f"schema {spec.schema!r} is not in the source's allowlist")
        only = req.get("only") or []
        if phase == "run" and (not isinstance(only, list) or not only):
            raise Refused("bad_message", "a run names the tables it wants in `only`")
        source = Source(self.engine, (from_env, to_env), identify=self._identify, batch_size=self.batch_size)
        source.check_identity()  # this database must be the edge's start, every time
        return source, specs, {str(x) for x in only}, phase, bool(req.get("full_refresh"))


def _try_send(ch: Channel, msg: dict[str, Any]) -> None:
    try:
        send(ch, msg)
    except Exception:
        pass


# ---------------------------------------------------------------------------
# The dialling sink (a developer's machine)
# ---------------------------------------------------------------------------

def pull(
    specs: list[TableSpec],
    edge: tuple[str, str],
    open_channel: Callable[[], Channel],
    sink: Any,
    parallelism: int = 1,
    full_refresh: bool = False,
    retries: int = 1,
) -> RunResult:
    """Ask the serving source for `specs` and apply what it sends: tables, then views."""
    by_label = {_label(s): s for s in specs}
    wire = specs_to_wire(specs)

    def request(phase: str, only: list[str] | None = None) -> dict[str, Any]:
        msg = {"t": "request", "v": PROTOCOL_VERSION, "edge": list(edge), "phase": phase,
               "tables": wire, "full_refresh": full_refresh}
        if only is not None:
            msg["only"] = only
        return msg

    ch = open_channel()
    try:
        send(ch, request("plan"))
        plan = recv(ch, "plan")
    finally:
        ch.close()
    tables = [str(x) for x in plan.get("tables") or [] if str(x) in by_label]
    views = [str(x) for x in plan.get("views") or [] if str(x) in by_label]
    result = RunResult(skipped=[str(x) for x in plan.get("skipped") or []])
    for label in result.skipped:
        log(f"SKIP {label}: not found in source (not yet created?)")

    def run_group(labels: list[str]) -> RunResult:
        r = RunResult()
        pending = list(labels)
        attempts: dict[str, int] = {}
        while pending:
            ch = open_channel()
            settled: list[tuple[str, bool]] = []
            try:
                send(ch, request("run", pending))
                offer = recv(ch, "offer")  # a refusal of the request raises here
                clean = serve_sink(ch, sink, offer=offer, settled=settled, reraise=True)
            except ConnectionLost:
                clean = False
            finally:
                ch.close()
            for label, applied in settled:
                (r.ok if applied else r.failed).append(label)
            done = {label for label, _ in settled}
            pending = [x for x in pending if x not in done]
            if clean:
                for label in pending:  # the source could not read it; its own log says why
                    log(f"ERROR {label}: the source did not send it")
                    r.failed.append(label)
                break
            if pending:
                current = pending[0]
                attempts[current] = attempts.get(current, 0) + 1
                if attempts[current] > retries:
                    log(f"ERROR {current}: connection lost, giving up after {retries} retry")
                    r.failed.append(current)
                    pending.pop(0)
                else:
                    log(f"  {current}: connection lost, retrying on a fresh connection")
        return r

    groups = [g for g in (tables[i::max(1, parallelism)] for i in range(max(1, parallelism))) if g]
    errors: list[BaseException] = []
    lock = threading.Lock()

    def work(group: list[str]) -> None:
        try:
            r = run_group(group)
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
        threads = [threading.Thread(target=work, args=(g,), name=f"pull-{i}") for i, g in enumerate(groups)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
    if errors:
        raise errors[0]
    if views:
        result.merge(run_group(views))
    return result
