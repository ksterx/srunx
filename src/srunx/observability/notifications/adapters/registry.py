"""Delivery adapter registry.

Module-level singleton keyed on ``endpoints.kind``. The
:class:`~srunx.pollers.delivery_poller.DeliveryPoller` dispatches each
claimed delivery through this registry to the matching adapter.
"""

from __future__ import annotations

from srunx.observability.notifications.adapters.base import DeliveryAdapter
from srunx.observability.notifications.adapters.slack_webhook import SlackWebhookAdapter

# Stateless adapters — safe to share a single instance process-wide.
ADAPTERS: dict[str, DeliveryAdapter] = {
    "slack_webhook": SlackWebhookAdapter(),
}


def get_adapter(kind: str) -> DeliveryAdapter:
    """Return the adapter registered for ``kind``.

    Args:
        kind: Endpoint kind string (e.g. ``'slack_webhook'``).

    Returns:
        The registered adapter instance.

    Raises:
        KeyError: When no adapter is registered for ``kind``.
    """
    try:
        return ADAPTERS[kind]
    except KeyError as exc:
        raise KeyError(f"No delivery adapter registered for kind={kind!r}") from exc
