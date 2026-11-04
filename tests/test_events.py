"""Run events ride the push: the `events` capability, on mocks (SQLite, loopback, fakes).

The sink advertises `caps: ["events"]` in `accept` when it has a publisher. The source
sends `events {items}` once, after every table and view, only when the sink has the
capability and nothing failed. The sink publishes each item unchanged on
`celine/pipelines/runs/<namespace>` and answers `ok {published}`.
"""
from __future__ import annotations

import threading

import pytest
from sqlalchemy import text

from sync_tables.codec import pack, unpack
from sync_tables.config import TableSpec
from sync_tables.log import Refused
from sync_tables.protocol import PROTOCOL_VERSION, loopback_pair, loopback_to, recv, send, serve_sink
from sync_tables.publish import TOPIC_PREFIX, topic_for
from test_exchange import DEV, INCR, PROD, make_pair, rows, seed

EVENTS = [
    {"namespace": "shop", "flow": "orders-flow", "status": "completed", "run_id": "r1",
     "timestamp": "2026-11-04T10:00:00+00:00", "duration_ms": 12, "error": None},
    {"namespace": "shop.sales", "flow": None, "status": "completed", "run_id": "r2",
     "timestamp": "2026-11-04T10:01:00+00:00", "duration_ms": 0, "error": None},
]


class FakePublisher:
    """Records what the sink publishes; `fail` raises like a broker that is down."""

    def __init__(self, fail: bool = False, on_publish=None) -> None:
        self.sent: list[tuple[str, dict]] = []
        self.fail = fail
        self.on_publish = on_publish

    def publish(self, messages: list[tuple[str, dict]]) -> int:
        if self.on_publish:
            self.on_publish()
        if self.fail:
            raise ConnectionError("broker unreachable")
        self.sent.extend(messages)
        return len(messages)


def pair_with(publisher=None, **kw):
    src, tgt, source, sink = make_pair(publisher=publisher, **kw)
    seed(src)
    return src, tgt, source, sink


def test_events_are_published_unchanged_after_the_tables():
    pub = FakePublisher()
    src, tgt, source, sink = pair_with(pub)
    result = source.run([INCR], loopback_to(sink), events=EVENTS)

    assert result.failed == []
    assert result.events_state == "delivered"
    assert result.events_published == 2
    assert pub.sent == [(f"{TOPIC_PREFIX}shop", EVENTS[0]), (f"{TOPIC_PREFIX}shop.sales", EVENTS[1])]
    assert rows(tgt, "SELECT count(*) FROM readings") == [(7,)]


def test_the_topic_is_the_namespace_under_the_runs_prefix():
    assert topic_for({"namespace": "shop"}) == "celine/pipelines/runs/shop"


def test_events_go_after_the_views():
    seen_view: list[bool] = []
    holder: dict = {}

    def check():
        with holder["tgt"].connect() as c:
            seen_view.append(bool(c.execute(text(
                "SELECT count(*) FROM sqlite_master WHERE type='view' AND name='recent'")).scalar_one()))

    pub = FakePublisher(on_publish=check)
    src, tgt, source, sink = pair_with(pub)
    holder["tgt"] = tgt
    with src.begin() as c:
        c.execute(text("CREATE VIEW recent AS SELECT id FROM readings WHERE id > 3"))
    result = source.run([INCR, TableSpec("main", "recent")], loopback_to(sink), events=EVENTS)
    assert result.failed == [] and result.events_state == "delivered"
    assert seen_view == [True]


def test_a_failed_table_keeps_the_events_back():
    pub = FakePublisher()
    src, tgt, source, sink = pair_with(pub)
    missing_col = TableSpec("main", "readings", date_column="no_such_column")
    result = source.run([missing_col], loopback_to(sink), events=EVENTS)

    assert result.failed == ["main.readings"]
    assert result.events_state == "undelivered"
    assert pub.sent == []


def test_a_sink_without_a_publisher_gets_no_events_and_the_push_is_ok():
    src, tgt, source, sink = pair_with(None)
    result = source.run([INCR], loopback_to(sink), events=EVENTS)
    assert result.failed == []
    assert result.events_state == "unsupported"
    assert rows(tgt, "SELECT count(*) FROM readings") == [(7,)]


def test_without_events_there_is_no_events_exchange():
    pub = FakePublisher()
    src, tgt, source, sink = pair_with(pub)
    for events in (None, []):
        result = source.run([INCR], loopback_to(sink), events=events)
        assert result.events_state == "none"
    assert pub.sent == []


def test_a_publish_failure_is_logged_and_the_push_stays_ok(capsys):
    pub = FakePublisher(fail=True)
    src, tgt, source, sink = pair_with(pub)
    result = source.run([INCR], loopback_to(sink), events=EVENTS)
    assert result.failed == []
    assert result.events_state == "delivered"
    assert result.events_published == 0
    assert "publish" in capsys.readouterr().out.lower()


@pytest.mark.parametrize("namespace", ["a/b", "a+", "#", "", None, 3, "a b"])
def test_an_item_whose_namespace_is_not_a_topic_level_is_not_published(namespace):
    pub = FakePublisher()
    src, tgt, source, sink = pair_with(pub)
    bad = dict(EVENTS[0], namespace=namespace)
    result = source.run([INCR], loopback_to(sink), events=[bad, EVENTS[1]])
    assert result.events_published == 1
    assert pub.sent == [(f"{TOPIC_PREFIX}shop.sales", EVENTS[1])]


def test_a_refused_push_sends_nothing():
    pub = FakePublisher()
    src, tgt, source, sink = pair_with(pub, tgt_ident=PROD)  # the edge says local
    with pytest.raises(Refused):
        source.run([INCR], loopback_to(sink), events=EVENTS)
    assert pub.sent == []


def test_the_sink_advertises_the_capability_only_with_a_publisher():
    for publisher, caps in ((FakePublisher(), ["events"]), (None, [])):
        _, _, source, sink = pair_with(publisher)
        here, there = loopback_pair()
        t = threading.Thread(target=serve_sink, args=(there, sink), daemon=True)
        t.start()
        send(here, {"t": "offer", "v": PROTOCOL_VERSION, "edge": ["dev", "local"], "env": "dev",
                    "sysid": DEV.system_identifier, "tables": [], "views": []})
        accept = recv(here, "accept")
        assert accept.get("caps", []) == caps
        send(here, {"t": "bye"})
        t.join(5)


def test_events_sent_to_a_sink_without_the_capability_are_refused_with_an_error():
    _, _, _, sink = pair_with(None)
    here, there = loopback_pair()
    t = threading.Thread(target=serve_sink, args=(there, sink), daemon=True)
    t.start()
    send(here, {"t": "offer", "v": PROTOCOL_VERSION, "edge": ["dev", "local"], "env": "dev",
                "sysid": DEV.system_identifier, "tables": [], "views": []})
    recv(here, "accept")
    send(here, {"t": "events", "items": EVENTS})
    reply = recv(here, "ok", "err")
    assert reply["t"] == "err" and reply["code"] == "events_unsupported"
    send(here, {"t": "bye"})
    t.join(5)


def test_an_unknown_message_is_still_refused():
    _, _, _, sink = pair_with(FakePublisher())
    here, there = loopback_pair()
    raised: list[BaseException] = []

    def serve() -> None:
        try:
            serve_sink(there, sink)
        except Exception as e:  # the sink stops on it; it never takes the message
            raised.append(e)

    t = threading.Thread(target=serve, daemon=True)
    t.start()
    send(here, {"t": "offer", "v": PROTOCOL_VERSION, "edge": ["dev", "local"], "env": "dev",
                "sysid": DEV.system_identifier, "tables": [], "views": []})
    recv(here, "accept")
    here.send_raw(pack({"t": "bogus"}))
    t.join(5)
    assert len(raised) == 1 and "unexpected message 'bogus'" in str(raised[0])


def test_the_protocol_version_is_still_2():
    assert PROTOCOL_VERSION == 2


def test_the_wire_messages_round_trip():
    msg = {"t": "events", "items": EVENTS}
    assert unpack(pack(msg)) == msg
