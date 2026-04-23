"""Backward-compat shim. Canonical: :mod:`srunx.observability.notifications.formatting`."""

from __future__ import annotations

from srunx.observability.notifications.formatting import (  # noqa: F401
    SlackNotificationFormatter,
    SlackTableFormatter,
)

__all__ = ["SlackNotificationFormatter", "SlackTableFormatter"]
