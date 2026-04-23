"""Abstract contract for delivery adapters.

A delivery adapter takes an :class:`~srunx.observability.storage.models.Event` plus the
owning endpoint's ``config`` JSON and performs the external side-effect
(e.g. POST to Slack). Adapters are stateless; the concrete ``send``
implementation raises :class:`DeliveryError` on any non-success
outcome that should cause the delivery poller to retry.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from srunx.observability.storage.models import Event


class DeliveryError(Exception):
    """Raised by a delivery adapter when the outbound call fails.

    The delivery poller catches this, records the error message on the
    :class:`~srunx.observability.storage.models.Delivery` row, and applies its retry /
    abandon policy.
    """


@runtime_checkable
class DeliveryAdapter(Protocol):
    """Protocol for delivery channel implementations.

    Attributes:
        kind: Short identifier matching ``endpoints.kind``
            (e.g. ``'slack_webhook'``). Used by the registry to
            dispatch events to the correct adapter.
    """

    kind: str

    def send(self, event: Event, endpoint_config: dict) -> None:
        """Deliver ``event`` to the external sink described by ``endpoint_config``.

        Args:
            event: The event whose payload should be rendered and sent.
            endpoint_config: Kind-specific configuration JSON
                (e.g. ``{"webhook_url": "https://..."}``).

        Raises:
            DeliveryError: When the delivery fails in a way that should
                cause a retry / abandon.
        """
        ...
