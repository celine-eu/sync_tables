"""Where a sink publishes the run events a push carried.

The sink publishes each item unchanged on `celine/pipelines/runs/<namespace>`, the topic
the flows published on before (plan decision 1). The publisher is an interface: the MQTT
implementation needs the optional `events` extra (celine-sdk) and lives in
`sync_tables.publish_mqtt`, so a pull-only install carries no broker client.
"""
from __future__ import annotations

import re
from typing import Any, Protocol

TOPIC_PREFIX = "celine/pipelines/runs/"

# One MQTT topic level: no separator, no wildcard, no whitespace.
NAMESPACE_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]*")


class Publisher(Protocol):
    """Publishes (topic, payload) pairs; returns how many the broker took.

    It may raise when the broker cannot be reached at all; the sink logs that and the push
    stays `ok` (plan design 4).
    """

    def publish(self, messages: list[tuple[str, dict[str, Any]]]) -> int: ...


def valid_namespace(value: Any) -> bool:
    """True when `value` can be one topic level (no `/`, `+`, `#` or whitespace)."""
    return isinstance(value, str) and NAMESPACE_RE.fullmatch(value) is not None


def topic_for(item: dict[str, Any]) -> str:
    """The topic of one run event: the runs prefix and its namespace."""
    return f"{TOPIC_PREFIX}{item['namespace']}"
