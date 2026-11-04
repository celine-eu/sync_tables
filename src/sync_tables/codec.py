"""Wire encoding: msgpack, with rows sent column by column (plan W6).

Values msgpack has no type for travel as extension types with a fixed binary layout,
so a batch of timestamps costs 8 bytes a value, not an ISO string.
"""
from __future__ import annotations

import struct
import uuid
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from typing import Any

import msgpack

from sync_tables.log import SyncError

_EXT_NAIVE_DT = 1   # >q  microseconds since 1970-01-01, no zone
_EXT_AWARE_DT = 2   # >qi microseconds since the epoch (UTC), offset in seconds
_EXT_DATE = 3       # >i  proleptic ordinal
_EXT_TIME = 4       # >q  microseconds since midnight (zone dropped, as Postgres `time`)
_EXT_DECIMAL = 5    # ascii text of the decimal
_EXT_UUID = 6       # 16 bytes
_EXT_TIMEDELTA = 7  # >q  microseconds
_EXT_GEOMETRY = 8   # >i?  srid, extended flag, then WKB bytes

_EPOCH_NAIVE = datetime(1970, 1, 1)
_EPOCH_AWARE = datetime(1970, 1, 1, tzinfo=timezone.utc)
_US = timedelta(microseconds=1)

try:
    from geoalchemy2.elements import WKBElement
except ImportError:  # pragma: no cover
    WKBElement = None  # type: ignore[assignment,misc]


def _us(td: timedelta) -> int:
    return td // _US


def _default(obj: Any) -> msgpack.ExtType:
    if isinstance(obj, datetime):
        if obj.tzinfo is None:
            return msgpack.ExtType(_EXT_NAIVE_DT, struct.pack(">q", _us(obj - _EPOCH_NAIVE)))
        offset = obj.utcoffset() or timedelta(0)
        return msgpack.ExtType(
            _EXT_AWARE_DT,
            struct.pack(">qi", _us(obj - _EPOCH_AWARE), int(offset.total_seconds())),
        )
    if isinstance(obj, date):
        return msgpack.ExtType(_EXT_DATE, struct.pack(">i", obj.toordinal()))
    if isinstance(obj, time):
        us = ((obj.hour * 60 + obj.minute) * 60 + obj.second) * 1_000_000 + obj.microsecond
        return msgpack.ExtType(_EXT_TIME, struct.pack(">q", us))
    if isinstance(obj, Decimal):
        return msgpack.ExtType(_EXT_DECIMAL, str(obj).encode("ascii"))
    if isinstance(obj, uuid.UUID):
        return msgpack.ExtType(_EXT_UUID, obj.bytes)
    if isinstance(obj, timedelta):
        return msgpack.ExtType(_EXT_TIMEDELTA, struct.pack(">q", _us(obj)))
    if WKBElement is not None and isinstance(obj, WKBElement):
        srid = obj.srid if obj.srid is not None else -1
        return msgpack.ExtType(_EXT_GEOMETRY, struct.pack(">i?", srid, bool(obj.extended)) + bytes(obj.data))
    if isinstance(obj, memoryview):
        return obj.tobytes()  # type: ignore[return-value]
    # The type's name only: never the value.
    raise SyncError(f"cannot encode a value of type {type(obj).__name__}")


def _ext_hook(code: int, data: bytes) -> Any:
    if code == _EXT_NAIVE_DT:
        return _EPOCH_NAIVE + timedelta(microseconds=struct.unpack(">q", data)[0])
    if code == _EXT_AWARE_DT:
        us, off = struct.unpack(">qi", data)
        return (_EPOCH_AWARE + timedelta(microseconds=us)).astimezone(timezone(timedelta(seconds=off)))
    if code == _EXT_DATE:
        return date.fromordinal(struct.unpack(">i", data)[0])
    if code == _EXT_TIME:
        us = struct.unpack(">q", data)[0]
        s, us = divmod(us, 1_000_000)
        m, s = divmod(s, 60)
        h, m = divmod(m, 60)
        return time(h, m, s, us)
    if code == _EXT_DECIMAL:
        return Decimal(data.decode("ascii"))
    if code == _EXT_UUID:
        return uuid.UUID(bytes=data)
    if code == _EXT_TIMEDELTA:
        return timedelta(microseconds=struct.unpack(">q", data)[0])
    if code == _EXT_GEOMETRY:
        if WKBElement is None:  # pragma: no cover
            raise SyncError("geometry received but geoalchemy2 is not installed")
        srid, extended = struct.unpack(">i?", data[:5])
        return WKBElement(data[5:], srid=srid, extended=extended)
    raise SyncError(f"unknown extension type {code}")


def pack(message: dict[str, Any]) -> bytes:
    return msgpack.packb(message, default=_default, use_bin_type=True, datetime=False)


def unpack(data: bytes) -> dict[str, Any]:
    try:
        msg = msgpack.unpackb(data, ext_hook=_ext_hook, raw=False, strict_map_key=False, timestamp=0)
    except SyncError:
        raise
    except Exception as e:
        raise SyncError(f"undecodable message ({type(e).__name__})") from None
    if not isinstance(msg, dict) or not isinstance(msg.get("t"), str):
        raise SyncError("malformed message")
    return msg


def to_columns(rows: list[dict[str, Any]], names: list[str]) -> list[list[Any]]:
    return [[r[n] for r in rows] for n in names]


def from_columns(names: list[str], columns: list[list[Any]]) -> list[dict[str, Any]]:
    if len(names) != len(columns):
        raise SyncError("batch column count does not match the table")
    n = len(columns[0]) if columns else 0
    if any(len(c) != n for c in columns):
        raise SyncError("batch columns have different lengths")
    return [dict(zip(names, values)) for values in zip(*columns)]
