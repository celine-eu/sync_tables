"""The MQTT publisher (extra `events`, celine-sdk), against a fake broker.

It opens one connection per push, publishes each message unchanged with QoS 1 and no
retain, closes the connection whatever happens, and reports how many the broker took.
Skipped without the extra.
"""
from __future__ import annotations

import asyncio
import time

import pytest

pytest.importorskip("celine.sdk.broker")

from celine.sdk.broker import QoS  # noqa: E402

from sync_tables import publish_mqtt  # noqa: E402
from sync_tables.publish_mqtt import MqttPublisher  # noqa: E402

MESSAGES = [
    ("celine/pipelines/runs/shop", {"namespace": "shop", "status": "completed", "run_id": "r1"}),
    ("celine/pipelines/runs/shop.sales", {"namespace": "shop.sales", "status": "completed", "run_id": "r2"}),
]


class Result:
    def __init__(self, success: bool) -> None:
        self.success = success
        self.error = None if success else "refused"


class FakeBroker:
    instances: list["FakeBroker"] = []
    fail_connect = False
    hang_connect = False
    hang_publish = False
    refuse_topics: set[str] = set()

    def __init__(self, config=None, token_provider=None) -> None:
        self.config, self.token_provider = config, token_provider
        self.published: list = []
        self.connected = self.disconnected = False
        FakeBroker.instances.append(self)

    async def connect(self) -> None:
        if FakeBroker.fail_connect:
            raise ConnectionError("broker unreachable")
        if FakeBroker.hang_connect:
            await asyncio.sleep(60)
        self.connected = True

    async def publish(self, message) -> Result:
        if FakeBroker.hang_publish:
            await asyncio.sleep(60)
        self.published.append(message)
        return Result(message.topic not in FakeBroker.refuse_topics)

    async def disconnect(self) -> None:
        self.disconnected = True


@pytest.fixture(autouse=True)
def fake_broker(monkeypatch):
    FakeBroker.instances = []
    FakeBroker.fail_connect = False
    FakeBroker.hang_connect = False
    FakeBroker.hang_publish = False
    FakeBroker.refuse_topics = set()
    monkeypatch.setattr(publish_mqtt, "MqttBroker", FakeBroker)
    return FakeBroker


def publisher() -> MqttPublisher:
    return MqttPublisher(publish_mqtt.MqttConfig(host="broker.example.org"), token_provider=None)


def test_each_message_is_published_unchanged_with_qos_1_and_no_retain():
    assert publisher().publish(MESSAGES) == 2
    (broker,) = FakeBroker.instances
    assert [(m.topic, m.payload) for m in broker.published] == MESSAGES
    assert all(m.qos == QoS.AT_LEAST_ONCE and m.retain is False for m in broker.published)


def test_one_connection_per_push_closed_afterwards():
    publisher().publish(MESSAGES)
    publisher().publish(MESSAGES[:1])
    assert len(FakeBroker.instances) == 2
    assert all(b.connected and b.disconnected for b in FakeBroker.instances)


def test_a_refused_message_is_not_counted():
    FakeBroker.refuse_topics = {MESSAGES[0][0]}
    assert publisher().publish(MESSAGES) == 1


def test_an_unreachable_broker_raises_and_the_connection_is_still_closed():
    FakeBroker.fail_connect = True
    with pytest.raises(ConnectionError):
        publisher().publish(MESSAGES)
    assert FakeBroker.instances[0].disconnected


@pytest.mark.parametrize("hang", ["hang_connect", "hang_publish"])
def test_a_broker_that_does_not_answer_raises_within_the_bound(hang):
    """A push waits for the publisher, so a silent broker must not hold it: the whole
    publication, connect included, is bounded and the connection still closed.
    """
    setattr(FakeBroker, hang, True)
    pub = MqttPublisher(publish_mqtt.MqttConfig(host="broker.example.org"), token_provider=None, timeout=0.1)
    started = time.monotonic()
    with pytest.raises(TimeoutError, match="0.1"):
        pub.publish(MESSAGES)
    assert time.monotonic() - started < 5
    assert FakeBroker.instances[0].disconnected


def test_the_bound_defaults_to_30_seconds():
    assert publisher().timeout == 30.0


def test_a_broker_refusing_the_token_fails_the_publish_instead_of_hanging(monkeypatch):
    """The 2026-11-06 staging hang, against celine-sdk's real broker: a refused connect
    with the publisher's bounded retries ends in an error, not a wait (celine-sdk >= 2.2.3).
    """
    from celine.sdk.broker import MqttBroker  # noqa: PLC0415

    class Refusing:
        def __init__(self, **kwargs) -> None:
            pass

        async def __aenter__(self):
            raise RuntimeError("Not authorized")

        async def __aexit__(self, *exc) -> None:
            pass

    monkeypatch.setattr(publish_mqtt, "MqttBroker", MqttBroker)
    monkeypatch.setattr("celine.sdk.broker.mqtt.aiomqtt.Client", Refusing)
    config = publish_mqtt.MqttConfig(host="broker.example.org", reconnect_interval=0.01, max_reconnect_attempts=2)
    pub = MqttPublisher(config, token_provider=None, timeout=5)
    started = time.monotonic()
    with pytest.raises(ConnectionError):
        pub.publish(MESSAGES)
    assert time.monotonic() - started < 2


def test_a_silent_broker_leaves_the_push_ok_with_nothing_published():
    from test_events import EVENTS, INCR, make_pair, seed  # noqa: PLC0415

    from sync_tables.protocol import loopback_to  # noqa: PLC0415

    FakeBroker.hang_publish = True
    pub = MqttPublisher(publish_mqtt.MqttConfig(host="broker.example.org"), token_provider=None, timeout=0.1)
    src, tgt, source, sink = make_pair(publisher=pub)
    seed(src)
    result = source.run([INCR], loopback_to(sink), events=EVENTS)
    assert result.failed == [] and result.ok
    assert result.events_state == "delivered" and result.events_published == 0


def test_from_env_reads_the_celine_settings(monkeypatch):
    for k, v in {
        "CELINE_MQTT_HOST": "broker.example.org", "CELINE_MQTT_PORT": "8883", "CELINE_MQTT_USE_TLS": "true",
        "CELINE_OIDC_BASE_URL": "https://id.example.org/realms/example",
        "CELINE_OIDC_CLIENT_ID": "svc-sync-tables-sink", "CELINE_OIDC_CLIENT_SECRET": "s" * 40,
    }.items():
        monkeypatch.setenv(k, v)
    pub = MqttPublisher.from_env()
    assert (pub.config.host, pub.config.port, pub.config.use_tls) == ("broker.example.org", 8883, True)
    assert pub.token_provider is not None


def test_from_env_without_client_credentials_has_no_token_provider(monkeypatch):
    monkeypatch.setenv("CELINE_MQTT_HOST", "broker.example.org")
    for k in ("CELINE_OIDC_CLIENT_ID", "CELINE_OIDC_CLIENT_SECRET"):
        monkeypatch.delenv(k, raising=False)
    assert MqttPublisher.from_env().token_provider is None


def test_the_sink_publishes_through_it(capsys):
    from test_events import EVENTS, INCR, make_pair, seed  # noqa: PLC0415

    from sync_tables.protocol import loopback_to  # noqa: PLC0415

    src, tgt, source, sink = make_pair(publisher=publisher())
    seed(src)
    result = source.run([INCR], loopback_to(sink), events=EVENTS)
    assert result.events_state == "delivered" and result.events_published == 2
    assert [m.payload for m in FakeBroker.instances[0].published] == EVENTS


def test_receive_builds_a_publisher_only_when_asked(monkeypatch):
    from sync_tables.__main__ import _events_publisher  # noqa: PLC0415

    monkeypatch.setenv("CELINE_MQTT_HOST", "broker.example.org")
    assert _events_publisher(False) is None
    assert isinstance(_events_publisher(True), MqttPublisher)


def test_receive_events_without_the_extra_is_refused_at_start(monkeypatch):
    import sys  # noqa: PLC0415

    from sync_tables.__main__ import _events_publisher  # noqa: PLC0415
    from sync_tables.log import SyncError  # noqa: PLC0415

    monkeypatch.setitem(sys.modules, "sync_tables.publish_mqtt", None)  # as if celine-sdk were absent
    with pytest.raises(SyncError) as e:
        _events_publisher(True)
    assert "events" in str(e.value)
