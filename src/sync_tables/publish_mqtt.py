"""The MQTT publisher of a sink's run events (extra `events`: celine-sdk).

One connection per push: connect, publish each message with QoS 1 and no retain,
disconnect, all of it within `timeout` seconds (30 by default) so a silent broker cannot hold
the push. The broker and the client credentials come from celine-sdk's settings, read
from the environment (`CELINE_MQTT_*`, `CELINE_OIDC_*`), the same variables every CELINE
service reads; the token is requested for the broker (celine-sdk's `mqtt` scope).
"""
from __future__ import annotations

import asyncio
from typing import Any

from celine.sdk.auth import OidcClientCredentialsProvider
from celine.sdk.broker import BrokerMessage, MqttBroker, MqttConfig, QoS
from celine.sdk.settings import SdkSettings

from sync_tables.log import log


class MqttPublisher:
    """Publishes (topic, payload) pairs on one broker; see `sync_tables.publish.Publisher`."""

    def __init__(
        self,
        config: MqttConfig,
        token_provider: OidcClientCredentialsProvider | None,
        timeout: float = 30.0,
    ) -> None:
        self.config = config
        self.token_provider = token_provider
        self.timeout = timeout

    @classmethod
    def from_env(cls) -> "MqttPublisher":
        """Build from celine-sdk's settings; a token provider only with client credentials."""
        sdk = SdkSettings()
        mqtt, oidc = sdk.mqtt, sdk.oidc
        provider = None
        if oidc.base_url and oidc.client_id and oidc.client_secret:
            provider = OidcClientCredentialsProvider(
                base_url=oidc.base_url,
                client_id=oidc.client_id,
                client_secret=oidc.client_secret,
                scope=oidc.scope,
                timeout=oidc.timeout,
                verify_ssl=oidc.verify_ssl,
            )
        config = MqttConfig(
            host=mqtt.host,
            port=mqtt.port,
            client_id=mqtt.client_id,
            username=mqtt.username,
            password=mqtt.password,
            use_tls=mqtt.use_tls,
            ca_certs=mqtt.ca_certs,
            certfile=mqtt.certfile,
            keyfile=mqtt.keyfile,
            keepalive=mqtt.keepalive,
            clean_session=mqtt.clean_session,
            reconnect_interval=1.0,
            max_reconnect_attempts=2,  # fail fast: the push is waiting for the answer
            topic_prefix="",
            token_refresh_margin=mqtt.token_refresh_margin,
        )
        return cls(config, provider)

    def publish(self, messages: list[tuple[str, dict[str, Any]]]) -> int:
        """Publish every message; return how many the broker took.

        Raises when it cannot connect, and `TimeoutError` when connecting, publishing and
        closing take longer than `timeout` seconds; the sink then counts nothing published.
        """
        return asyncio.run(self._bounded(messages))

    async def _bounded(self, messages: list[tuple[str, dict[str, Any]]]) -> int:
        try:
            return await asyncio.wait_for(self._publish(messages), timeout=self.timeout)
        except asyncio.TimeoutError:
            raise TimeoutError(f"events: the broker did not answer within {self.timeout}s") from None

    async def _publish(self, messages: list[tuple[str, dict[str, Any]]]) -> int:
        broker = MqttBroker(config=self.config, token_provider=self.token_provider)
        published = 0
        try:
            await broker.connect()
            for topic, payload in messages:
                result = await broker.publish(
                    BrokerMessage(topic=topic, payload=payload, qos=QoS.AT_LEAST_ONCE, retain=False)
                )
                if result.success:
                    published += 1
                else:
                    log(f"WARNING events: the broker refused one event on {topic}")
        finally:
            try:
                await broker.disconnect()
            except Exception:  # noqa: BLE001 — the connection is going away anyway
                pass
        return published
