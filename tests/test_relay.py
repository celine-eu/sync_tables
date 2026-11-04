"""The relay: POST /events, one push loop per environment, its config (plan design 2).

The relay takes no token (its boundary is the network, decision 6), keeps only `completed`
events (decision 13), and pushes when events are pending and `min_interval` has passed
since the last push started, or when `max_interval` has passed with no push. One push at
a time; only events received before a push started go with it; a failed push keeps them.
"""
from __future__ import annotations

import http.client
import json
import threading
import time

import pytest

from sync_tables.log import Refused, SyncError
from sync_tables.protocol import loopback_to
from sync_tables.relay import (
    Relay,
    load_relay_config,
    make_local_push,
    make_push,
    parse_interval,
    start_http,
    validate_event,
)
from sync_tables.source import EVENTS_DELIVERED, EVENTS_UNDELIVERED, EVENTS_UNSUPPORTED
from test_events import EVENTS, FakePublisher
from test_exchange import INCR, make_pair, seed
from test_wss import pki  # noqa: F401 — the module-scoped PKI fixture


def event(run_id: str, status: str = "completed") -> dict:
    return dict(EVENTS[0], run_id=run_id, status=status)


class Clock:
    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now


class Pushes:
    """A fake push: records each batch and answers with the next state queued."""

    def __init__(self, *states: str) -> None:
        self.batches: list[list[dict]] = []
        self.states = list(states)
        self.during = None

    def __call__(self, events: list[dict]) -> str:
        self.batches.append([e["run_id"] for e in events])
        if self.during:
            self.during()
        return self.states.pop(0) if self.states else EVENTS_DELIVERED


def relay(push, clock, min_interval=600, max_interval=1800, pending_max=1000):
    return Relay(push, min_interval=min_interval, max_interval=max_interval, pending_max=pending_max, clock=clock)


# ---------------------------------------------------------------------------
# Events in
# ---------------------------------------------------------------------------

def test_only_completed_events_are_kept():
    r = relay(Pushes(), Clock())
    assert r.offer(event("a")) is True
    assert r.offer(event("b", "started")) is False
    assert r.offer(event("c", "failed")) is False
    assert [e["run_id"] for e in r.pending()] == ["a"]


def test_the_pending_set_is_bounded_and_drops_the_oldest(capsys):
    r = relay(Pushes(), Clock(), pending_max=2)
    for i in range(3):
        r.offer(event(f"e{i}"))
    assert [e["run_id"] for e in r.pending()] == ["e1", "e2"]
    assert "dropped" in capsys.readouterr().out


@pytest.mark.parametrize("body", [
    {"namespace": "shop", "status": "completed", "run_id": "r1"},          # no timestamp
    dict(EVENTS[0], namespace=3),
    dict(EVENTS[0], duration_ms="12"),
    dict(EVENTS[0], flow=["x"]),
    dict(EVENTS[0], run_id=""),
    [EVENTS[0]],
    "completed",
    None,
])
def test_a_body_that_is_not_a_run_event_is_refused(body):
    with pytest.raises(ValueError):
        validate_event(body)


def test_a_run_event_is_kept_unchanged_with_fields_it_does_not_know():
    body = dict(EVENTS[0], created=None, correlation_id=None, extra_field="x")
    assert validate_event(body) == body


# ---------------------------------------------------------------------------
# The push loop
# ---------------------------------------------------------------------------

def test_the_first_cycle_pushes_at_once():
    clock, pushes = Clock(), Pushes()
    r = relay(pushes, clock)
    assert r.step() is True
    assert pushes.batches == [[]]


def test_min_interval_is_respected():
    clock, pushes = Clock(), Pushes()
    r = relay(pushes, clock)
    r.step()
    clock.now += 1
    r.offer(event("a"))
    assert r.step() is False
    clock.now += 599
    assert r.step() is True
    assert pushes.batches == [[], ["a"]]


def test_max_interval_fires_with_no_events():
    clock, pushes = Clock(), Pushes()
    r = relay(pushes, clock)
    r.step()
    clock.now += 1799
    assert r.step() is False
    clock.now += 1
    assert r.step() is True
    assert pushes.batches == [[], []]


def test_only_events_received_before_a_push_started_go_with_it():
    clock, pushes = Clock(), Pushes()
    r = relay(pushes, clock)
    r.offer(event("before"))
    pushes.during = lambda: r.offer(event("during"))
    r.step()
    assert pushes.batches == [["before"]]
    assert [e["run_id"] for e in r.pending()] == ["during"]


def test_a_failed_push_keeps_its_events_ahead_of_newer_ones():
    clock, pushes = Clock(), Pushes(EVENTS_UNDELIVERED)
    r = relay(pushes, clock)
    r.offer(event("a"))
    pushes.during = lambda: r.offer(event("b"))
    r.step()
    assert [e["run_id"] for e in r.pending()] == ["a", "b"]


def test_a_sink_without_events_drops_them():
    clock, pushes = Clock(), Pushes(EVENTS_UNSUPPORTED)
    r = relay(pushes, clock)
    r.offer(event("a"))
    r.step()
    assert r.pending() == []


def test_a_push_that_raises_keeps_the_events_and_the_loop_goes_on(capsys):
    clock = Clock()

    def boom(events):
        raise Refused("edge_not_allowed", "no")

    r = relay(boom, clock)
    r.offer(event("a"))
    assert r.step() is True
    assert [e["run_id"] for e in r.pending()] == ["a"]
    assert "edge_not_allowed" in capsys.readouterr().out


def test_never_two_pushes_at_once():
    running, peak = [0], [0]
    lock = threading.Lock()

    def slow(events):
        with lock:
            running[0] += 1
            peak[0] = max(peak[0], running[0])
        time.sleep(0.05)
        with lock:
            running[0] -= 1
        return EVENTS_DELIVERED

    r = Relay(slow, min_interval=0, max_interval=0, pending_max=10)
    threads = [threading.Thread(target=r.step) for _ in range(5)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert peak[0] == 1


def test_the_loop_runs_until_stopped():
    pushes = Pushes()
    r = Relay(pushes, min_interval=0, max_interval=0, pending_max=10, tick=0.01)
    stop = threading.Event()
    t = threading.Thread(target=r.run, args=(stop,))
    t.start()
    time.sleep(0.1)
    stop.set()
    t.join(2)
    assert not t.is_alive() and len(pushes.batches) >= 2


# ---------------------------------------------------------------------------
# HTTP
# ---------------------------------------------------------------------------

@pytest.fixture
def http_relay():
    r = relay(Pushes(), Clock())
    server = start_http(r, "127.0.0.1", 0)
    yield r, server.server_address[1]
    server.shutdown()


def call(port, method, path, body=None, raw=None, headers=None):
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
    data = raw if raw is not None else (json.dumps(body).encode() if body is not None else None)
    conn.request(method, path, body=data, headers={"Content-Type": "application/json", **(headers or {})})
    resp = conn.getresponse()
    out = resp.status, resp.read()
    conn.close()
    return out


def test_a_valid_event_is_accepted_without_a_token(http_relay):
    r, port = http_relay
    status, _ = call(port, "POST", "/events", EVENTS[0])
    assert status == 202
    assert r.pending() == [EVENTS[0]]


def test_a_started_event_is_accepted_and_not_kept(http_relay):
    r, port = http_relay
    assert call(port, "POST", "/events", event("s", "started"))[0] == 202
    assert r.pending() == []


@pytest.mark.parametrize("raw", [b"not json", b"[]", b'{"namespace": "shop"}', b""])
def test_a_malformed_body_is_a_400(http_relay, raw):
    r, port = http_relay
    assert call(port, "POST", "/events", raw=raw)[0] == 400
    assert r.pending() == []


def test_a_body_over_the_limit_is_a_413(http_relay):
    _, port = http_relay
    big = dict(EVENTS[0], error="x" * (70 * 1024))
    assert call(port, "POST", "/events", big)[0] == 413


def test_healthz_and_unknown_paths(http_relay):
    _, port = http_relay
    assert call(port, "GET", "/healthz")[0] == 200
    assert call(port, "GET", "/events")[0] == 405
    assert call(port, "POST", "/elsewhere", EVENTS[0])[0] == 404


# ---------------------------------------------------------------------------
# The push itself: a Source run carrying the events, and local mode
# ---------------------------------------------------------------------------

def test_make_push_copies_the_tables_and_hands_over_the_events():
    pub = FakePublisher()
    src, tgt, source, sink = make_pair(publisher=pub)
    seed(src)
    push = make_push(source, [INCR], loopback_to(sink), parallelism=1)
    assert push(EVENTS) == EVENTS_DELIVERED
    assert [p for _, p in pub.sent] == EVENTS


def test_local_mode_publishes_directly():
    pub = FakePublisher()
    push = make_local_push(pub)
    assert push(EVENTS) == EVENTS_DELIVERED
    assert [t for t, _ in pub.sent] == ["celine/pipelines/runs/shop", "celine/pipelines/runs/shop.sales"]
    assert push([]) == EVENTS_DELIVERED


# ---------------------------------------------------------------------------
# Config: defaults + one entry per sink, selected by --sink
# ---------------------------------------------------------------------------

RELAY_YAML = """
source:
  env: dev
  db: {url: "${DB_DEV}"}
edges:
  - {from: dev, to: prod, transports: [push]}
  - {from: dev, to: staging, transports: [push]}
defaults:
  min_interval: 30m
  max_interval: 30m
  pending_max: 500
  parallelism: 2
  sources:
    - {schema: silver, table: orders, date_column: ts}
    - {schema: gold, table: totals}
sinks:
  prod:
    url: wss://sink.example.org/sync/v1
    ca: /etc/sync/prod/ca.crt
    cert: /etc/sync/prod/client.crt
    key: /etc/sync/prod/client.key
    token_file: /etc/sync/prod/token
  staging:
    url: wss://staging-sink.example.org/sync/v1
    ca: /etc/sync/staging/ca.crt
    cert: /etc/sync/staging/client.crt
    key: /etc/sync/staging/client.key
    token_file: /etc/sync/staging/token
    min_interval: 10m
    allow_schemas: [gold]
"""


def write(tmp_path, text):
    p = tmp_path / "relay.yaml"
    p.write_text(text, encoding="utf-8")
    return str(p)


def test_a_sink_entry_overrides_the_defaults(tmp_path, monkeypatch):
    monkeypatch.setenv("DB_DEV", "postgresql+psycopg://reader@example-dev.example.org/warehouse")
    cfg = load_relay_config(write(tmp_path, RELAY_YAML), "staging")
    assert (cfg.from_env, cfg.to_env) == ("dev", "staging")
    assert (cfg.min_interval, cfg.max_interval, cfg.pending_max, cfg.parallelism) == (600, 1800, 500, 2)
    assert [(s.schema, s.table) for s in cfg.sources] == [("gold", "totals")]  # allow_schemas
    assert cfg.url == "wss://staging-sink.example.org/sync/v1"


def test_the_defaults_apply_where_the_entry_says_nothing(tmp_path):
    cfg = load_relay_config(write(tmp_path, RELAY_YAML), "prod")
    assert (cfg.min_interval, cfg.max_interval) == (1800, 1800)
    assert len(cfg.sources) == 2
    assert cfg.source_db_url is None  # ${DB_DEV} unset: resolved at start, not at load


def test_a_sink_the_config_does_not_name_is_refused(tmp_path):
    with pytest.raises(SyncError) as e:
        load_relay_config(write(tmp_path, RELAY_YAML), "preprod")
    assert "preprod" in str(e.value)


def test_an_edge_the_config_does_not_declare_is_refused(tmp_path):
    text = RELAY_YAML.replace("  - {from: dev, to: staging, transports: [push]}\n", "")
    with pytest.raises(Refused) as e:
        load_relay_config(write(tmp_path, text), "staging")
    assert e.value.code == "edge_not_allowed"


def test_a_relay_into_prod_from_anything_but_dev_is_refused(tmp_path):
    text = RELAY_YAML.replace("env: dev", "env: staging").replace("from: dev, to: prod", "from: staging, to: prod")
    with pytest.raises(Refused) as e:
        load_relay_config(write(tmp_path, text), "prod")
    assert e.value.code == "prod_only_from_dev"


@pytest.mark.parametrize("change", [
    ("  min_interval: 30m\n  max_interval: 30m", "  min_interval: 40m\n  max_interval: 30m"),
    ("    url: wss://sink.example.org/sync/v1\n", ""),
    ("  min_interval: 30m", "  min_interval: soon"),
    ("  pending_max: 500", "  pending_max: 0"),
])
def test_a_bad_entry_is_refused_at_load(tmp_path, change):
    with pytest.raises((SyncError, ValueError)):
        load_relay_config(write(tmp_path, RELAY_YAML.replace(*change)), "prod")


@pytest.mark.parametrize("text,seconds", [("30m", 1800), ("10m", 600), ("1h", 3600), ("45s", 45), (900, 900)])
def test_intervals(text, seconds):
    assert parse_interval(text) == seconds


def test_check_config_opens_no_connection(tmp_path, monkeypatch, capsys):
    import sync_tables.db as db  # noqa: PLC0415
    from sync_tables.__main__ import main  # noqa: PLC0415

    def boom(*a, **k):
        raise AssertionError("a connection was opened")

    monkeypatch.setattr(db, "make_source_engine", boom)
    path = write(tmp_path, RELAY_YAML)
    assert main(["relay", "--config", path, "--sink", "staging", "--check-config"]) == 0
    assert "dev -> staging" in capsys.readouterr().out
    assert main(["relay", "--config", path, "--sink", "preprod", "--check-config"]) == 2


# ---------------------------------------------------------------------------
# End to end over WSS: a relay pushes to a listening sink that publishes
# ---------------------------------------------------------------------------

def test_a_relay_pushes_over_wss_and_the_sink_publishes(pki):  # noqa: F811 — pytest fixture
    from sync_tables.sink import Sink  # noqa: PLC0415
    from sync_tables.source import Source  # noqa: PLC0415
    from sync_tables.wss import channel_factory, client_tls, make_server, server_tls  # noqa: PLC0415
    from test_wss import DEV, PROD, SPEC, engine, free_port, rows, source_with_rows, token_of  # noqa: PLC0415

    out, _ = pki
    pub = FakePublisher()
    tgt = engine()
    sink = Sink(tgt, ["main"], identify=lambda c: PROD, accept_from={"dev"}, publisher=pub)
    port = free_port()
    server = make_server(sink, "127.0.0.1", port, token_of(out),
                         server_tls(f"{out}/server.crt", f"{out}/server.key", f"{out}/ca.crt"))
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        src = source_with_rows()
        tls = client_tls(f"{out}/ca.crt", f"{out}/client-source.crt", f"{out}/client-source.key")
        source = Source(src, ("dev", "prod"), identify=lambda c: DEV)
        push = make_push(source, [SPEC], channel_factory(f"wss://localhost:{port}", tls, token_of(out)), 1)
        r = Relay(push, min_interval=0, max_interval=60)
        r.offer(EVENTS[0])
        assert r.step() is True
        assert r.pending() == []
        assert pub.sent == [("celine/pipelines/runs/shop", EVENTS[0])]
        assert rows(tgt) == rows(src)
    finally:
        server.shutdown()
