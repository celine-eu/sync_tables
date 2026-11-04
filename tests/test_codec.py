"""The wire codec: every column type in use survives a round trip (plan W6)."""
from __future__ import annotations

import uuid
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal

import pytest

from sync_tables.codec import from_columns, pack, to_columns, unpack
from sync_tables.log import SyncError


@pytest.mark.parametrize("value", [
    None,
    True,
    42,
    -(2**62),
    3.25,
    "text, with ünïcode",
    b"\x00\x01binary",
    datetime(2026, 10, 15, 12, 30, 45, 123456),
    datetime(1969, 12, 31, 23, 59, 59, 999999),
    datetime(2026, 10, 15, 12, 30, 45, 123456, tzinfo=timezone.utc),
    datetime(2026, 3, 29, 2, 30, tzinfo=timezone(timedelta(hours=2))),
    date(2026, 10, 15),
    date(1, 1, 1),
    time(23, 59, 59, 999999),
    Decimal("12345678901234567890.123456789"),
    Decimal("-0.000001"),
    uuid.UUID("12345678-1234-5678-1234-567812345678"),
    timedelta(days=-1, microseconds=5),
    {"json": [1, 2, {"nested": None}], "k": "v"},
    [1.5, 2.5],
])
def test_round_trip(value):
    out = unpack(pack({"t": "rows", "columns": [[value]]}))["columns"][0][0]
    assert out == value
    assert type(out) is type(value)
    if isinstance(value, datetime) and value.tzinfo is not None:
        assert out.utcoffset() == value.utcoffset()


def test_geometry_round_trip():
    from geoalchemy2.elements import WKBElement
    from geoalchemy2.shape import from_shape
    from shapely.geometry import Point

    geom = from_shape(Point(11.12, 46.07), srid=4326, extended=True)
    out = unpack(pack({"t": "rows", "columns": [[geom]]}))["columns"][0][0]
    assert isinstance(out, WKBElement)
    assert bytes(out.data) == bytes(geom.data) and out.srid == 4326 and out.extended


def test_columns_round_trip():
    rows = [{"a": 1, "b": "x"}, {"a": 2, "b": None}]
    cols = to_columns(rows, ["a", "b"])
    assert cols == [[1, 2], ["x", None]]
    assert from_columns(["a", "b"], cols) == rows


def test_ragged_columns_are_refused():
    with pytest.raises(SyncError):
        from_columns(["a", "b"], [[1, 2], [1]])


def test_an_unknown_type_names_the_type_not_the_value():
    class Secret:
        def __repr__(self):
            return "ROW-SECRET-VALUE"

    with pytest.raises(SyncError) as e:
        pack({"t": "rows", "columns": [[Secret()]]})
    assert "Secret" in str(e.value) and "ROW-SECRET-VALUE" not in str(e.value)


def test_garbage_is_refused():
    with pytest.raises(SyncError):
        unpack(b"\xc1\xc1\xc1")
    with pytest.raises(SyncError):
        unpack(pack({"no": "type"}))  # type: ignore[arg-type]


def test_columnar_timestamps_are_compact():
    ts = [datetime(2026, 1, 1) + timedelta(minutes=15 * i) for i in range(1000)]
    size = len(pack({"t": "rows", "columns": [ts]}))
    assert size < 1000 * 12  # ~10 bytes a value, against ~28 for ISO text
