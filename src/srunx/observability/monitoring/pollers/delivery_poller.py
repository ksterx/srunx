"""Delivery outbox consumer (PR 2 / C.3).

See :mod:`srunx.observability.monitoring.pollers.supervisor` for the supervision contract.
Each :meth:`DeliveryPoller.run_cycle` call:

1. Reclaims any lease whose ``leased_until`` has elapsed (crash recovery).
2. Drains up to :attr:`MAX_CLAIMS_PER_CYCLE` claimable rows, dispatching each
   to the :class:`~srunx.observability.notifications.adapters.base.DeliveryAdapter`
   registered for the target endpoint's ``kind``.
3. Applies the retry / abandon policy based on ``attempt_count`` and the
   configured ``max_retries``.

All sqlite3 calls and outbound adapter sends run on a worker thread via
``anyio.to_thread.run_sync`` so the poller never blocks the event loop.
Structured counts are emitted once per cycle for observability.
"""

from __future__ import annotations

import os
import time
from collections.abc import Callable
from pathlib import Path

import anyio

from srunx.common.logging import get_logger
from srunx.observability.notifications.adapters.base import DeliveryError
from srunx.observability.notifications.adapters.registry import get_adapter
from srunx.observability.storage.connection import open_connection
from srunx.observability.storage.models import Delivery, Endpoint, Event
from srunx.observability.storage.repositories.deliveries import DeliveryRepository
from srunx.observability.storage.repositories.endpoints import EndpointRepository
from srunx.observability.storage.repositories.events import EventRepository

logger = get_logger(__name__)


# Cap on claims per ``run_cycle`` to prevent a single cycle from monopolising
# the worker when the backlog is large.
MAX_CLAIMS_PER_CYCLE: int = 100


class DeliveryPoller:
    """Consume the ``deliveries`` outbox and dispatch to adapters.

    Implements the :class:`~srunx.observability.monitoring.pollers.supervisor.Poller` protocol.
    """

    name: str = "delivery_poller"
    interval_seconds: float = 10.0

    def __init__(
        self,
        *,
        db_path: Path | None = None,
        worker_id: str | None = None,
        interval_seconds: float = 10.0,
        max_retries: int = 5,
        lease_duration_secs: int = 300,
    ) -> None:
        """Initialize the delivery poller.

        Args:
            db_path: Override for the sqlite DB path. ``None`` resolves the
                default XDG path at connection time.
            worker_id: Unique identifier written to ``deliveries.worker_id``
                on claim. Defaults to ``delivery-<pid>``.
            interval_seconds: Sleep between cycles when the outbox is empty.
            max_retries: Total attempts before moving a row to ``abandoned``.
                A fresh row has ``attempt_count=0``; after ``max_retries``
                failures the row is abandoned.
            lease_duration_secs: Lease TTL applied on claim. A lease that
                expires without completion is reclaimed by the next cycle.
        """
        self._db_path = db_path
        self.worker_id = worker_id or f"delivery-{os.getpid()}"
        self.interval_seconds = interval_seconds
        self.max_retries = max_retries
        self.lease_duration_secs = lease_duration_secs

    async def run_cycle(self) -> None:
        """Run one pass: reclaim, then claim-and-dispatch up to the batch cap.

        Opens a single short-lived sqlite connection for the whole cycle.
        Exceptions bubble to the supervisor, which applies backoff.
        """
        start_ns = time.monotonic_ns()
        conn = await anyio.to_thread.run_sync(open_connection, self._db_path)
        try:
            delivery_repo = DeliveryRepository(conn)
            event_repo = EventRepository(conn)
            endpoint_repo = EndpointRepository(conn)

            reclaimed = await anyio.to_thread.run_sync(
                delivery_repo.reclaim_expired_leases
            )

            counts = {
                "claimed": 0,
                "delivered": 0,
                "retried": 0,
                "abandoned": 0,
            }

            for _ in range(MAX_CLAIMS_PER_CYCLE):
                # Cooperative cancellation checkpoint: if the supervisor
                # cancelled the task group (lifespan shutdown), this lets
                # us bail out BEFORE claiming another delivery. The
                # already-sent + marked row stays consistent; the next
                # outstanding lease will be reclaimed on restart via
                # ``reclaim_expired_leases``.
                await anyio.sleep(0)

                delivery = await anyio.to_thread.run_sync(
                    delivery_repo.claim_one,
                    self.worker_id,
                    self.lease_duration_secs,
                )
                if delivery is None:
                    break
                counts["claimed"] += 1

                outcome = await self._process_delivery(
                    delivery=delivery,
                    delivery_repo=delivery_repo,
                    event_repo=event_repo,
                    endpoint_repo=endpoint_repo,
                )
                counts[outcome] += 1
        finally:
            await anyio.to_thread.run_sync(conn.close)

        elapsed_ms = (time.monotonic_ns() - start_ns) // 1_000_000
        logger.bind(
            poller=self.name,
            worker_id=self.worker_id,
            reclaimed=reclaimed,
            claimed=counts["claimed"],
            delivered=counts["delivered"],
            retried=counts["retried"],
            abandoned=counts["abandoned"],
            elapsed_ms=elapsed_ms,
        ).info("delivery poller cycle complete")

    async def _process_delivery(
        self,
        *,
        delivery: Delivery,
        delivery_repo: DeliveryRepository,
        event_repo: EventRepository,
        endpoint_repo: EndpointRepository,
    ) -> str:
        """Dispatch a single claimed delivery. Returns the outcome bucket.

        Possible return values match keys in the ``counts`` dict inside
        :meth:`run_cycle`: ``'delivered'``, ``'retried'``, or ``'abandoned'``.
        """
        delivery_id = delivery.id
        assert delivery_id is not None, "claimed delivery must have an id"

        # -- Preflight: ensure referenced rows still exist ----------------
        event = await anyio.to_thread.run_sync(event_repo.get, delivery.event_id)
        endpoint = await anyio.to_thread.run_sync(
            endpoint_repo.get, delivery.endpoint_id
        )
        if event is None or endpoint is None:
            await anyio.to_thread.run_sync(
                delivery_repo.mark_abandoned,
                delivery_id,
                "referenced event or endpoint vanished",
            )
            return "abandoned"

        if endpoint.disabled_at is not None:
            await anyio.to_thread.run_sync(
                delivery_repo.mark_abandoned,
                delivery_id,
                "endpoint disabled",
            )
            return "abandoned"

        # -- Resolve adapter ---------------------------------------------
        try:
            adapter = get_adapter(endpoint.kind)
        except KeyError:
            await anyio.to_thread.run_sync(
                delivery_repo.mark_abandoned,
                delivery_id,
                f"unknown adapter kind {endpoint.kind}",
            )
            return "abandoned"

        # -- Send ---------------------------------------------------------
        return await self._send_and_record(
            delivery=delivery,
            event=event,
            endpoint=endpoint,
            adapter_send=adapter.send,
            delivery_repo=delivery_repo,
        )

    async def _send_and_record(
        self,
        *,
        delivery: Delivery,
        event: Event,
        endpoint: Endpoint,
        adapter_send: Callable[[Event, dict], None],
        delivery_repo: DeliveryRepository,
    ) -> str:
        """Perform the send and transition the delivery row. Returns the bucket."""
        delivery_id = delivery.id
        assert delivery_id is not None

        try:
            # adapter.send is a blocking call (HTTP POST etc.) — always run
            # it on a worker thread so the event loop stays responsive.
            await anyio.to_thread.run_sync(
                adapter_send,
                event,
                endpoint.config,
            )
        except DeliveryError as exc:
            return await self._handle_failure(
                delivery=delivery,
                error=str(exc),
                delivery_repo=delivery_repo,
            )
        except Exception as exc:
            # Any unexpected exception is treated as a transient failure
            # subject to the same retry / abandon policy. The supervisor's
            # backoff only triggers on exceptions that escape run_cycle,
            # which we do NOT want per-delivery: one bad row should not
            # stall the rest of the queue.
            return await self._handle_failure(
                delivery=delivery,
                error=f"{type(exc).__name__}: {exc}",
                delivery_repo=delivery_repo,
            )

        await anyio.to_thread.run_sync(delivery_repo.mark_delivered, delivery_id)
        return "delivered"

    async def _handle_failure(
        self,
        *,
        delivery: Delivery,
        error: str,
        delivery_repo: DeliveryRepository,
    ) -> str:
        """Apply retry/abandon policy for a failed send. Returns the bucket."""
        delivery_id = delivery.id
        assert delivery_id is not None

        # attempt_count on the row reflects *completed* attempts. The
        # current send is attempt number attempt_count+1. If that reaches
        # max_retries, abandon.
        next_attempt_count = delivery.attempt_count + 1
        if next_attempt_count >= self.max_retries:
            await anyio.to_thread.run_sync(
                delivery_repo.mark_abandoned,
                delivery_id,
                error,
            )
            return "abandoned"

        backoff_secs = DeliveryRepository._backoff_secs(delivery.attempt_count)
        await anyio.to_thread.run_sync(
            delivery_repo.mark_retry,
            delivery_id,
            error,
            backoff_secs,
        )
        return "retried"
