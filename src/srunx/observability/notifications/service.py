"""NotificationService: event → delivery fan-out.

Given an :class:`~srunx.observability.storage.models.Event`, look up every open
:class:`~srunx.observability.storage.models.Watch` whose ``target_ref`` matches, walk each
watch's subscriptions, apply the preset filter, skip disabled
endpoints, and insert one :class:`~srunx.observability.storage.models.Delivery` per
match. The caller owns the transaction — this service never issues
``BEGIN`` / ``COMMIT`` internally.
"""

from __future__ import annotations

import sqlite3

from srunx.observability.notifications.presets import should_deliver
from srunx.observability.storage.models import Event, Watch
from srunx.observability.storage.repositories.deliveries import DeliveryRepository
from srunx.observability.storage.repositories.endpoints import EndpointRepository
from srunx.observability.storage.repositories.events import EventRepository
from srunx.observability.storage.repositories.subscriptions import (
    SubscriptionRepository,
)
from srunx.observability.storage.repositories.watches import WatchRepository


class NotificationService:
    """Orchestrate event → delivery fan-out across watches and subscriptions."""

    def __init__(
        self,
        watch_repo: WatchRepository,
        subscription_repo: SubscriptionRepository,
        event_repo: EventRepository,
        delivery_repo: DeliveryRepository,
        endpoint_repo: EndpointRepository,
    ) -> None:
        self.watch_repo = watch_repo
        self.subscription_repo = subscription_repo
        self.event_repo = event_repo
        self.delivery_repo = delivery_repo
        self.endpoint_repo = endpoint_repo

    # -- fan-out -----------------------------------------------------------

    def fan_out(self, event: Event, conn: sqlite3.Connection) -> list[int]:
        """Fan ``event`` out to the deliveries table.

        The caller is expected to have already inserted ``event`` via
        :class:`EventRepository` and to own the surrounding
        transaction. ``conn`` is accepted so future versions can route
        writes through a caller-supplied connection if the repos are
        ever configured with a different one, but is not otherwise
        required by the body today.

        Args:
            event: The event being fanned out. ``event.id`` must be
                set (i.e. the event must already be persisted).
            conn: The active connection owning the enclosing
                transaction. Present for contract symmetry; not used
                directly because all writes flow through the already-
                bound repositories.

        Returns:
            IDs of freshly-inserted ``deliveries`` rows. Rows absorbed
            by the ``(endpoint_id, idempotency_key)`` UNIQUE index are
            omitted.
        """
        if event.id is None:
            raise ValueError(
                "NotificationService.fan_out requires an event with a persisted id"
            )

        to_status = self._extract_to_status(event)

        # Find matching open watches. We match on exact source_ref
        # equality — this is the contract documented in design.md.
        # Use list_by_target() to scope the query and avoid scanning
        # every open watch in the system.
        watches = self._open_watches_for_source_ref(event.source_ref)

        created_delivery_ids: list[int] = []
        for watch in watches:
            if watch.id is None:
                continue
            for subscription in self.subscription_repo.list_by_watch(watch.id):
                if subscription.id is None:
                    continue

                endpoint = self.endpoint_repo.get(subscription.endpoint_id)
                if endpoint is None or endpoint.disabled_at is not None:
                    continue

                if not should_deliver(subscription.preset, event.kind, to_status):
                    continue

                idempotency_key = self._idempotency_key(event)
                delivery_id = self.delivery_repo.insert(
                    event_id=event.id,
                    subscription_id=subscription.id,
                    endpoint_id=endpoint.id or subscription.endpoint_id,
                    idempotency_key=idempotency_key,
                )
                if delivery_id is not None:
                    created_delivery_ids.append(delivery_id)

        return created_delivery_ids

    # -- convenience helpers for routers -----------------------------------

    def create_watch_for_job(
        self,
        job_id: int,
        endpoint_id: int | None,
        preset: str,
        *,
        scheduler_key: str = "local",
    ) -> int:
        """Create a job-kind watch and (optionally) its subscription.

        Args:
            job_id: SLURM job id to watch.
            endpoint_id: When not ``None``, also create a subscription
                from the new watch to this endpoint with ``preset``.
            preset: Preset string for the subscription. Ignored when
                ``endpoint_id`` is ``None``.
            scheduler_key: ``"local"`` (default) for local SLURM, or
                ``"ssh:<profile>"`` for an SSH transport. Encoded into
                the V5 3-segment ``target_ref`` grammar
                ``job:<scheduler_key>:<id>`` so the poller can route
                each watch back to the right transport.

        Returns:
            The newly-created ``watches.id``.
        """
        target_ref = f"job:{scheduler_key}:{job_id}"
        watch_id = self.watch_repo.create(kind="job", target_ref=target_ref)
        if endpoint_id is not None:
            self.subscription_repo.create(
                watch_id=watch_id,
                endpoint_id=endpoint_id,
                preset=preset,
            )
        return watch_id

    def create_watch_for_workflow_run(
        self,
        run_id: int,
        endpoint_id: int | None,
        preset: str | None,
    ) -> int:
        """Create a workflow_run-kind watch and (optionally) its subscription.

        The watch is always created (auto-watch policy, see design.md
        § "Workflow router"). A subscription is only created when both
        ``endpoint_id`` and ``preset`` are provided.

        Args:
            run_id: Workflow run id to watch.
            endpoint_id: Endpoint to subscribe, or ``None`` for a
                watch-only (no delivery) record.
            preset: Preset to bind to the subscription; see
                ``endpoint_id``.

        Returns:
            The newly-created ``watches.id``.
        """
        watch_id = self.watch_repo.create(
            kind="workflow_run",
            target_ref=f"workflow_run:{run_id}",
        )
        if endpoint_id is not None and preset is not None:
            self.subscription_repo.create(
                watch_id=watch_id,
                endpoint_id=endpoint_id,
                preset=preset,
            )
        return watch_id

    def create_watch_for_sweep_run(
        self,
        sweep_run_id: int,
        endpoint_id: int | None = None,
        preset: str = "terminal",
    ) -> int:
        """Create a sweep_run-kind watch and optional subscription for aggregated sweep-level notifications.

        Mirrors :meth:`create_watch_for_workflow_run` but scopes the watch
        to the parent sweep (``target_ref='sweep_run:<id>'``). When
        ``endpoint_id`` is provided the subscription wires sweep-level
        ``sweep_run.status_changed`` events to the endpoint via the
        delivery pipeline. When ``endpoint_id`` is ``None`` the watch is
        created without a subscription.
        """
        watch_id = self.watch_repo.create(
            kind="sweep_run",
            target_ref=f"sweep_run:{sweep_run_id}",
        )
        if endpoint_id is not None:
            self.subscription_repo.create(
                watch_id=watch_id,
                endpoint_id=endpoint_id,
                preset=preset,
            )
        return watch_id

    # -- internals ---------------------------------------------------------

    def _open_watches_for_source_ref(self, source_ref: str) -> list[Watch]:
        """Return open watches whose ``target_ref`` equals ``source_ref``.

        ``target_ref`` uses the same ``"<kind>:<id>"`` format as
        ``events.source_ref``, so the kind prefix alone is enough to
        derive the watch kind. Keeping this as a private helper lets
        us swap the query strategy (index-scoped vs table-scan) later
        without touching ``fan_out``.
        """
        kind_prefix = source_ref.split(":", 1)[0]
        if kind_prefix in {"job", "workflow_run", "sweep_run"}:
            return self.watch_repo.list_by_target(
                kind=kind_prefix,
                target_ref=source_ref,
                only_open=True,
            )
        # Fallback: linear scan of open watches. Unused by the current
        # event kinds but keeps future kinds like ``resource:...`` /
        # ``schedule:...`` working without requiring extra indexes.
        return [w for w in self.watch_repo.list_open() if w.target_ref == source_ref]

    @staticmethod
    def _extract_to_status(event: Event) -> str | None:
        """Return ``payload['to_status']`` when present, else ``None``."""
        payload = event.payload or {}
        value = payload.get("to_status")
        if value is None:
            return None
        return str(value)

    @staticmethod
    def _idempotency_key(event: Event) -> str:
        """Deterministic key for the ``(endpoint_id, idempotency_key)`` UNIQUE.

        Reuses :meth:`EventRepository._compute_payload_hash` so the
        delivery-side dedup scheme stays in lock-step with the event-
        side ``payload_hash``. Same logical transition → same key.
        """
        return EventRepository._compute_payload_hash(
            event.kind, event.source_ref, event.payload or {}
        )
