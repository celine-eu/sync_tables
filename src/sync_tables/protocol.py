"""The exchange: a closed set of typed messages between a source and a sink (plan W5).

The conversation is the same whichever side dials:

    source -> sink   offer    {v, edge, env, sysid, tables: [{schema, table, date_column}], views}
    sink -> source   accept   {env, sysid, watermarks}            or  refuse {code, message}
    then, per table:
    source -> sink   table    {desc, plan: {mode, date_column, truncate, delete_from}}
    source -> sink   rows     {columns: [[...], ...]}             (zero or more)
    sink -> source   ack      {}                                  (one per rows, once applied)
    source -> sink   end      {}                                  or  abort {code}
    sink -> source   ok       {rows}                              or  err {code, message}
    then, per view (after every table):
    source -> sink   view     {schema, name, body}
    sink -> source   ok / err
    then, once, only when the sink's accept had caps ["events"] and nothing failed:
    source -> sink   events   {items: [PipelineRunEvent, ...]}
    sink -> source   ok {published}                               or  err {code, message}
    source -> sink   bye

On a pull the sink dials, so it opens each connection with a request, and the source
answers with a plan or goes on with the offer above:

    sink -> source   request  {v, edge, phase: plan|run, tables: [{schema, table,
                               date_column, cutoff, lookback}], only, full_refresh}
    source -> sink   plan     {tables, views, skipped}            (phase plan)
                     offer …                                      (phase run)
                     or refuse {code, message}

No message carries SQL except a view body. No message flows from the sink into the
source's database, so the copy runs one way whoever opens the socket.

Flow control (version 2): the source keeps at most WINDOW `rows` unacknowledged, and reads
the acks still owed before `ok`/`err` and after an `abort`. A sink slower than its source
then never holds more than WINDOW frames, so the transport keeps reading the socket and
answering keepalive pings however long a write takes.
"""
from __future__ import annotations

import queue
import threading
from typing import Any, Callable, Protocol

from sync_tables.catalog import TableDesc
from sync_tables.codec import from_columns, pack, unpack
from sync_tables.log import Refused, SyncError, log, safe_error

PROTOCOL_VERSION = 2
MAX_FRAME = 16 * 1024 * 1024
# Unacknowledged `rows` frames in flight. Below websockets' receive high-water mark (16
# frames), where its reader stops reading the socket, pings included.
WINDOW = 4


class ConnectionLost(SyncError):
    """The peer went away. The table in flight was rolled back on the sink."""


class Channel(Protocol):
    def send_raw(self, data: bytes) -> None: ...
    def recv_raw(self) -> bytes: ...
    def close(self) -> None: ...


def send(ch: Channel, msg: dict[str, Any]) -> None:
    ch.send_raw(pack(msg))


def recv(ch: Channel, *expected: str) -> dict[str, Any]:
    msg = unpack(ch.recv_raw())
    if expected and msg["t"] not in expected:
        if msg["t"] == "refuse":
            raise Refused(str(msg.get("code")), str(msg.get("message")))
        raise SyncError(f"unexpected message {msg['t']!r} (expected {', '.join(expected)})")
    return msg


# ---------------------------------------------------------------------------
# In-process loopback: the same bytes, no socket
# ---------------------------------------------------------------------------

class _LoopbackEnd:
    _CLOSED = object()

    def __init__(self, inbox: queue.Queue, outbox: queue.Queue, max_frame: int) -> None:
        self._in, self._out, self._max = inbox, outbox, max_frame
        self._closed = False

    def send_raw(self, data: bytes) -> None:
        if self._closed:
            raise ConnectionLost("connection closed")
        if len(data) > self._max:
            raise SyncError("frame exceeds the size limit")
        self._out.put(data)

    def recv_raw(self) -> bytes:
        if self._closed:
            raise ConnectionLost("connection closed")
        item = self._in.get()
        if item is self._CLOSED:
            self._closed = True
            raise ConnectionLost("peer closed the connection")
        return item

    def close(self) -> None:
        if not self._closed:
            self._closed = True
            self._out.put(self._CLOSED)


def loopback_pair(max_frame: int = MAX_FRAME) -> tuple[_LoopbackEnd, _LoopbackEnd]:
    a, b = queue.Queue(), queue.Queue()
    return _LoopbackEnd(a, b, max_frame), _LoopbackEnd(b, a, max_frame)


def loopback_to(sink: "Any") -> Callable[[], Channel]:
    """A channel factory whose every channel is served by `sink` on its own thread."""

    def open_channel() -> Channel:
        here, there = loopback_pair()

        def serve() -> None:
            try:
                serve_sink(there, sink)
            finally:
                there.close()

        threading.Thread(target=serve, name="sink", daemon=True).start()
        return here

    return open_channel


# ---------------------------------------------------------------------------
# The sink's side of the conversation
# ---------------------------------------------------------------------------

def serve_sink(
    ch: Channel,
    sink: Any,
    offer: dict[str, Any] | None = None,
    settled: list[tuple[str, bool]] | None = None,
    reraise: bool = False,
) -> bool:
    """Serve one connection. True when the source said bye, False when it dropped.

    `offer` is one the caller already read (a pull reads it to see a refusal first).
    `settled` collects (label, applied) for every table and view the source finished.
    A refusal is sent to the source and, with `reraise`, raised here too.
    """
    settled = [] if settled is None else settled
    try:
        if offer is None:
            offer = recv(ch, "offer")
        if offer.get("v") != PROTOCOL_VERSION:
            raise Refused("protocol_version", f"protocol version {offer.get('v')!r} is not {PROTOCOL_VERSION}")
        state = sink.accept(offer)
    except Refused as r:
        _sink_log(sink, str(r))
        _try_send(ch, {"t": "refuse", "code": r.code, "message": r.message})
        if reraise:
            raise
        return False
    except ConnectionLost:
        return False
    except SyncError as e:
        _sink_log(sink, f"REFUSED [bad_message]: {e}")
        _try_send(ch, {"t": "refuse", "code": "bad_message", "message": str(e)})
        if reraise:
            raise Refused("bad_message", str(e)) from None
        return False
    caps = list(getattr(sink, "caps", []))
    send(ch, {"t": "accept", **state, **({"caps": caps} if caps else {})})

    while True:
        try:
            msg = recv(ch, "table", "view", "events", "bye")
        except ConnectionLost:
            return False
        if msg["t"] == "bye":
            return True
        if msg["t"] == "events":
            _serve_events(ch, sink, msg)
            continue
        if msg["t"] == "view":
            _serve_view(ch, sink, msg, settled)
            continue
        if not _serve_table(ch, sink, msg, settled):
            return False


def _serve_view(ch: Channel, sink: Any, msg: dict[str, Any], settled: list[tuple[str, bool]]) -> None:
    label = f"{msg.get('schema')}.{msg.get('name')}"
    try:
        sink.apply_view(str(msg["schema"]), str(msg["name"]), msg["body"])
        settled.append((label, True))
        send(ch, {"t": "ok", "rows": 0})
    except Exception as e:
        code = e.code if isinstance(e, Refused) else "view_failed"
        _sink_log(sink, f"ERROR {label}: view not recreated: {safe_error(e)}")
        settled.append((label, False))
        send(ch, {"t": "err", "code": code, "message": safe_error(e)})


def _serve_events(ch: Channel, sink: Any, msg: dict[str, Any]) -> None:
    """Publish the run events of a push whose tables and views all settled ok."""
    if "events" not in getattr(sink, "caps", []):
        send(ch, {"t": "err", "code": "events_unsupported", "message": "this sink does not publish events"})
        return
    published = sink.publish_events(msg.get("items"))
    send(ch, {"t": "ok", "published": published})


def _serve_table(ch: Channel, sink: Any, msg: dict[str, Any], settled: list[tuple[str, bool]]) -> bool:
    """Apply one table. Returns False when the connection dropped."""
    write = None
    error: BaseException | None = None
    names: list[str] = []
    label = "?"
    try:
        desc = TableDesc.from_wire(msg["desc"])
        label = desc.label
        names = [c.name for c in desc.columns]
        write = sink.begin_table(desc, dict(msg["plan"]))
    except Exception as e:
        error = e

    while True:
        try:
            m = recv(ch, "rows", "end", "abort")
        except ConnectionLost:
            if write is not None:
                write.rollback()
            _sink_log(sink, f"ERROR {label}: connection lost mid-table, rolled back")
            return False
        except BaseException as e:
            if write is not None:
                write.rollback()
            raise e
        if m["t"] == "rows":
            if error is None and write is not None:
                try:
                    write.write(from_columns(names, m["columns"]))
                except Exception as e:
                    error = e
                    write.rollback()
                    write = None
            try:
                send(ch, {"t": "ack"})
            except ConnectionLost:
                if write is not None:
                    write.rollback()
                _sink_log(sink, f"ERROR {label}: connection lost mid-table, rolled back")
                return False
            continue
        if m["t"] == "abort":
            if write is not None:
                write.rollback()
            _sink_log(sink, f"ERROR {label}: the source aborted the table ({m.get('code')}), rolled back")
            settled.append((label, False))
            return True
        break  # end

    if error is None and write is not None:
        try:
            rows = write.commit()
            _sink_log(sink, f"{label}: applied rows={rows}")
            settled.append((label, True))
            send(ch, {"t": "ok", "rows": rows})
            return True
        except Exception as e:
            error = e
    elif write is not None:
        write.rollback()
    code = error.code if isinstance(error, Refused) else "table_failed"
    _sink_log(sink, f"ERROR {label}: {safe_error(error)}")
    settled.append((label, False))
    send(ch, {"t": "err", "code": code, "message": safe_error(error)})
    return True


def _sink_log(sink: Any, line: str) -> None:
    """Outcomes the source reports anyway; a listening sink logs them too (its own logs)."""
    if sink.log_summaries:
        log(line)


def _try_send(ch: Channel, msg: dict[str, Any]) -> None:
    try:
        send(ch, msg)
    except Exception:
        pass
