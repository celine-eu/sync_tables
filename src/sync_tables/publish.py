"""Where a sink publishes the run events a push carried.

The sink publishes each item unchanged on `celine/pipelines/runs/<namespace>`, the topic
the flows published on before (plan decision 1). The publisher is an interface: the MQTT
implementation needs the optional `events` extra (celine-sdk) and lives in
`sync_tables.publish_mqtt`, so a pull-only install carries no broker client.
"""
from __future__ import annotations

import re
from typing import Any, Protocol

from sync_tables.log import log, safe_error

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


def publish_items(publisher: Publisher, items: Any) -> int:
    """Publish each run event unchanged on its namespace's topic; never raises.

    An item that is not a map with a namespace usable as one topic level is skipped and
    logged. A broker failure is logged and counts as nothing published.
    """
    messages: list[tuple[str, dict[str, Any]]] = []
    for item in items if isinstance(items, list) else []:
        if isinstance(item, dict) and valid_namespace(item.get("namespace")):
            messages.append((topic_for(item), item))
        else:
            log("WARNING events: an item without a usable namespace was not published")
    if not messages:
        return 0
    try:
        published = int(publisher.publish(messages))
    except Exception as e:
        log(f"ERROR events: publish failed, {len(messages)} event(s) not published: {safe_error(e)}")
        return 0
    log(f"events: published {published} of {len(messages)}")
    return published
