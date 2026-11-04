"""The relay: takes the flows' run events over HTTP and pushes them with the data.

Plan design 2, decisions 2, 4, 5, 6, 9, 13 and 15. One relay per environment, each with
its own config entry and one sink. The flows POST a `PipelineRunEvent` to `/events`; the
relay checks no token (its boundary is the network), validates the body, keeps
`completed` events and answers `202`. One push loop pushes the tables (and, after them,
the pending events) when events are pending and `min_interval` has passed since the last
push started, or when `max_interval` has passed with no push. One push at a time; only
events received before a push started go with it; a failed push keeps them for the next.

`local` mode, for a development stack where flows and services share one database:
no data push, each pending event is published directly on the broker.
"""
from __future__ import annotations

import json
import os
import re
import threading
import time
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

import yaml

from sync_tables.config import TableSpec, _parse_sources, _resolve_url, expand_env_recursive
from sync_tables.envs import PUSH, EdgePolicy, check_name
from sync_tables.log import SyncError, log, safe_error
from sync_tables.publish import Publisher, publish_items
from sync_tables.source import EVENTS_DELIVERED, EVENTS_UNDELIVERED, Source

COMPLETED = "completed"
MAX_BODY = 64 * 1024          # a run event is a few hundred bytes
DEFAULT_PENDING_MAX = 1000    # events kept while a sink is unreachable; the oldest go first
TICK = 1.0                    # seconds between two looks at the timers

Push = Callable[[list[dict[str, Any]]], str]

_REQUIRED_STR = ("namespace", "status", "run_id", "timestamp")
_OPTIONAL_STR = ("flow", "error", "created", "correlation_id")


def validate_event(body: Any) -> dict[str, Any]:
    """Return `body` unchanged when it is a `PipelineRunEvent`; raise ValueError otherwise.

    Fields it does not know are kept, as celine-sdk's model ignores them.
    """
    if not isinstance(body, dict):
        raise ValueError("a run event is a JSON object")
    for name in _REQUIRED_STR:
        if not isinstance(body.get(name), str) or not body[name]:
            raise ValueError(f"`{name}` is a non-empty string")
    for name in _OPTIONAL_STR:
        if body.get(name) is not None and not isinstance(body[name], str):
            raise ValueError(f"`{name}` is a string or null")
    duration = body.get("duration_ms")
    if duration is not None and (isinstance(duration, bool) or not isinstance(duration, int)):
        raise ValueError("`duration_ms` is an integer or null")
    return body


def parse_interval(value: Any) -> int:
    """Seconds from `45s`, `30m`, `1h` or a plain number of seconds."""
    if isinstance(value, bool):
        raise ValueError(f"invalid interval {value!r}")
    if isinstance(value, int):
        if value < 0:
            raise ValueError(f"invalid interval {value!r}")
        return value
    m = re.fullmatch(r"(\d+)([smh])", str(value).strip())
    if not m:
        raise ValueError(f"invalid interval {value!r}; expected e.g. 45s, 30m, 1h")
    return int(m.group(1)) * {"s": 1, "m": 60, "h": 3600}[m.group(2)]


class Relay:
    """The pending events and the one push loop of one relay."""

    def __init__(
        self,
        push: Push,
        min_interval: float,
        max_interval: float,
        pending_max: int = DEFAULT_PENDING_MAX,
        clock: Callable[[], float] = time.monotonic,
        tick: float = TICK,
    ) -> None:
        if min_interval < 0 or max_interval < min_interval:
            raise SyncError("the relay needs 0 <= min_interval <= max_interval")
        if pending_max < 1:
            raise SyncError("the relay keeps at least one pending event")
        self._push = push
        self.min_interval, self.max_interval = min_interval, max_interval
        self.pending_max = pending_max
        self._clock, self._tick = clock, tick
        self._pending: deque[dict[str, Any]] = deque()
        self._lock = threading.Lock()        # the pending events
        self._push_lock = threading.Lock()   # one push at a time (decision 5)
        self._last_start: float | None = None

    # -- events in --------------------------------------------------------------

    def offer(self, event: dict[str, Any]) -> bool:
        """Keep a `completed` event for the next push; True when it was kept."""
        if event.get("status") != COMPLETED:
            return False
        with self._lock:
            self._pending.append(event)
            self._trim()
        return True

    def pending(self) -> list[dict[str, Any]]:
        with self._lock:
            return list(self._pending)

    def _trim(self) -> None:
        dropped = 0
        while len(self._pending) > self.pending_max:
            self._pending.popleft()
            dropped += 1
        if dropped:
            log(f"WARNING relay: {dropped} pending event(s) dropped, the oldest first (pending_max={self.pending_max})")

    # -- the push loop ----------------------------------------------------------

    def _due(self, now: float) -> bool:
        if self._last_start is None:
            return True  # the first cycle pushes at once
        since = now - self._last_start
        with self._lock:
            has_events = bool(self._pending)
        return (has_events and since >= self.min_interval) or since >= self.max_interval

    def step(self) -> bool:
        """Push once if a timer says so; True when a push ran."""
        with self._push_lock:
            now = self._clock()
            if not self._due(now):
                return False
            self._last_start = now
            with self._lock:
                batch = list(self._pending)
                self._pending.clear()
            try:
                state = self._push(batch)
            except Exception as e:  # a refusal or a dead sink: keep the events, keep looping
                log(f"ERROR relay: push failed: {safe_error(e) if not hasattr(e, 'code') else e}")
                state = EVENTS_UNDELIVERED
            if state == EVENTS_UNDELIVERED and batch:
                with self._lock:
                    self._pending.extendleft(reversed(batch))
                    self._trim()
            return True

    def run(self, stop: threading.Event) -> None:
        """Loop until `stop` is set."""
        while not stop.is_set():
            self.step()
            stop.wait(self._tick)


def make_push(source: Source, specs: list[TableSpec], open_channel: Callable[[], Any], parallelism: int) -> Push:
    """A push of `specs` from `source` that carries the pending events after the tables."""

    def push(events: list[dict[str, Any]]) -> str:
        started = time.monotonic()
        result = source.run(specs, open_channel, parallelism=parallelism, events=events)
        log(
            f"relay: {source.edge[0]} -> {source.edge[1]} in {time.monotonic() - started:.1f}s: "
            f"{len(result.ok)} ok, {len(result.failed)} failed, {len(result.skipped)} skipped; "
            f"{len(events)} event(s) {result.events_state}"
        )
        return result.events_state if events else EVENTS_DELIVERED

    return push


def make_local_push(publisher: Publisher) -> Push:
    """`local` mode: no data push, the events go straight to the broker."""

    def push(events: list[dict[str, Any]]) -> str:
        if events:
            publish_items(publisher, events)
        return EVENTS_DELIVERED

    return push


# ---------------------------------------------------------------------------
# HTTP: POST /events, GET /healthz
# ---------------------------------------------------------------------------

def start_http(relay: Relay, host: str, port: int) -> ThreadingHTTPServer:
    """Serve the relay's endpoint on a thread of its own; return the server."""

    class Handler(BaseHTTPRequestHandler):
        server_version = "sync-tables-relay"
        sys_version = ""

        def log_message(self, format: str, *args: Any) -> None:  # noqa: A002 — no client addresses in logs
            return

        def _answer(self, status: int, body: dict[str, Any]) -> None:
            data = json.dumps(body).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def do_GET(self) -> None:  # noqa: N802
            if self.path == "/healthz":
                self._answer(200, {"status": "ok"})
            elif self.path == "/events":
                self._answer(405, {"error": "POST a run event"})
            else:
                self._answer(404, {"error": "not found"})

        def do_POST(self) -> None:  # noqa: N802
            if self.path != "/events":
                self._answer(404, {"error": "not found"})
                return
            try:
                length = int(self.headers.get("Content-Length") or 0)
            except ValueError:
                length = -1
            if length > MAX_BODY:
                self.close_connection = True
                self._answer(413, {"error": f"a run event is at most {MAX_BODY} bytes"})
                return
            if length <= 0:
                self._answer(400, {"error": "empty body"})
                return
            try:
                event = validate_event(json.loads(self.rfile.read(length)))
            except (ValueError, UnicodeDecodeError) as e:
                self._answer(400, {"error": str(e)[:200]})
                return
            self._answer(202, {"kept": relay.offer(event)})

    server = ThreadingHTTPServer((host, port), Handler)
    server.daemon_threads = True
    threading.Thread(target=server.serve_forever, name="relay-http", daemon=True).start()
    return server


# ---------------------------------------------------------------------------
# Config: a source, the edges, defaults, and one entry per sink
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class RelayConfig:
    sink: str
    from_env: str
    to_env: str
    source_db_url: str | None
    url: str
    ca: str
    cert: str
    key: str
    token_file: str
    sources: list[TableSpec]
    min_interval: int
    max_interval: int
    pending_max: int
    parallelism: int
    batch_size: int


_TUNING = ("min_interval", "max_interval", "pending_max", "parallelism", "batch_size", "sources", "allow_schemas")
_MATERIAL = ("url", "ca", "cert", "key", "token_file")


def load_relay_config(path: str, sink: str) -> RelayConfig:
    """Read the relay config and select one sink: its entry over `defaults`.

    Refuses, before any connection, a sink the file does not name, an edge its `edges`
    do not declare for push, and an edge the compiled rule refuses.
    """
    if not os.path.isfile(path):
        raise SyncError(f"no relay config at {path}")
    with open(path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f) or {}
    if not isinstance(raw, dict):
        raise SyncError("relay config: not a mapping")
    source = raw.get("source") or {}
    sinks = raw.get("sinks") or {}
    defaults = raw.get("defaults") or {}
    if not isinstance(source, dict) or not isinstance(sinks, dict) or not isinstance(defaults, dict):
        raise SyncError("relay config: `source`, `defaults` and `sinks` are mappings")
    entry = sinks.get(sink)
    if not isinstance(entry, dict):
        raise SyncError(f"relay config: no sink {sink!r} (sinks: {', '.join(sorted(sinks)) or 'none'})")

    from_env = check_name(source.get("env"))
    to_env = check_name(entry.get("env", sink))
    EdgePolicy.from_config(raw.get("edges")).pick(from_env, to_env, PUSH)

    merged = {k: defaults[k] for k in _TUNING if k in defaults}
    merged.update({k: entry[k] for k in _TUNING if k in entry})
    material = expand_env_recursive({k: entry.get(k) for k in _MATERIAL}, strict=True)
    missing = [k for k in _MATERIAL if not material.get(k)]
    if missing:
        raise SyncError(f"relay config: sink {sink!r} needs {', '.join(missing)}")
    if not str(material["url"]).startswith("wss://"):
        raise SyncError(f"relay config: sink {sink!r} url is a wss:// URL")

    def no_path_sources() -> str:
        raise SyncError("relay config: list tables; `type: path` entries need a pipelines root")

    specs = _parse_sources(expand_env_recursive(merged.get("sources") or [], strict=True), no_path_sources)
    allow = merged.get("allow_schemas")
    if allow is not None:
        specs = [s for s in specs if s.schema in set(allow)]
    if not specs:
        raise SyncError(f"relay config: sink {sink!r} has no tables")

    min_interval = parse_interval(merged.get("min_interval", "30m"))
    max_interval = parse_interval(merged.get("max_interval", "30m"))
    if min_interval > max_interval:
        raise SyncError("relay config: min_interval is above max_interval")
    pending_max = int(merged.get("pending_max", DEFAULT_PENDING_MAX))
    if pending_max < 1:
        raise SyncError("relay config: pending_max is at least 1")

    db = source.get("db") or {}
    return RelayConfig(
        sink=sink, from_env=from_env, to_env=to_env,
        source_db_url=_resolve_url(db.get("url") if isinstance(db, dict) else None),
        url=str(material["url"]), ca=str(material["ca"]), cert=str(material["cert"]),
        key=str(material["key"]), token_file=str(material["token_file"]),
        sources=specs, min_interval=min_interval, max_interval=max_interval, pending_max=pending_max,
        parallelism=max(1, int(merged.get("parallelism", 1))),
        batch_size=int(merged.get("batch_size", 5000)),
    )
