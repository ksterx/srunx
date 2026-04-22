"""Active-watch producer poller.

The :class:`ActiveWatchPoller` walks every open row in ``watches``, asks
the SLURM backend for the current state of the referenced job(s), and
translates observed transitions into:

1. an append-only row in ``job_state_transitions`` (SSOT for status
   history, R6.1),
2. an ``UPDATE`` on ``jobs`` with the lifecycle timestamps,
3. a new row in ``events`` (``INSERT OR IGNORE`` on
   ``(kind, source_ref, payload_hash)`` — producer-side dedup),
4. zero-or-more rows in ``deliveries`` via
   :meth:`srunx.notifications.service.NotificationService.fan_out`,
5. an automatic ``watches.close()`` once the job reaches a terminal
   state (so subsequent cycles do not re-query SLURM for jobs that
   have already finished).

Workflow-run watches are aggregated from their child jobs per R2
(any CANCELLED → ``cancelled``; any FAILED/TIMEOUT/NODE_FAIL →
``failed``; all COMPLETED → ``completed``; any RUNNING → ``running``;
otherwise ``pending``).

Connection ownership
--------------------
The poller owns a short-lived sqlite3 connection per cycle. It opens
one at the top of :meth:`run_cycle`, routes all reads and writes
through it, and closes it in ``finally`` — this isolates the
producer from FastAPI request connections and keeps WAL fsyncs off
the hot web path.

See ``.claude/specs/notification-and-state-persistence/design.md``
§ ActiveWatchPoller.
"""

from __future__ import annotations

import sqlite3
import time
from collections.abc import Callable
from pathlib import Path
from typing import TYPE_CHECKING

import anyio

from srunx.client_protocol import JobStatusInfo, SlurmClientProtocol
from srunx.db.connection import open_connection, transaction
from srunx.db.models import WorkflowRunJob
from srunx.db.repositories.base import now_iso
from srunx.db.repositories.deliveries import DeliveryRepository
from srunx.db.repositories.endpoints import EndpointRepository
from srunx.db.repositories.events import EventRepository
from srunx.db.repositories.job_state_transitions import (
    JobStateTransitionRepository,
)
from srunx.db.repositories.jobs import JobRepository
from srunx.db.repositories.subscriptions import SubscriptionRepository
from srunx.db.repositories.watches import WatchRepository
from srunx.db.repositories.workflow_run_jobs import WorkflowRunJobRepository
from srunx.db.repositories.workflow_runs import WorkflowRunRepository
from srunx.logging import get_logger
from srunx.notifications.service import NotificationService
from srunx.slurm.states import SLURM_TERMINAL_JOB_STATES
from srunx.sweep.state_service import WorkflowRunStateService

if TYPE_CHECKING:
    from srunx.transport.registry import TransportRegistry

logger = get_logger(__name__)


# Terminal workflow_run statuses per our own domain model.
_TERMINAL_WORKFLOW_RUN_STATUSES: frozenset[str] = frozenset(
    {"completed", "failed", "cancelled"}
)


NotificationServiceFactory = Callable[
    [
        WatchRepository,
        SubscriptionRepository,
        EventRepository,
        DeliveryRepository,
        EndpointRepository,
    ],
    NotificationService,
]


def _default_notification_service_factory(
    watch_repo: WatchRepository,
    subscription_repo: SubscriptionRepository,
    event_repo: EventRepository,
    delivery_repo: DeliveryRepository,
    endpoint_repo: EndpointRepository,
) -> NotificationService:
    return NotificationService(
        watch_repo=watch_repo,
        subscription_repo=subscription_repo,
        event_repo=event_repo,
        delivery_repo=delivery_repo,
        endpoint_repo=endpoint_repo,
    )


def _dt_to_iso(value: object) -> str | None:
    """Return an ISO string for a ``datetime`` (or ``None``).

    ``JobStatusInfo`` fields may be ``None`` or ``datetime`` — both
    the repository writes and the event payload need ``str | None``.
    """
    if value is None:
        return None
    # datetime has .isoformat(); any other type is coerced via str().
    isoformat = getattr(value, "isoformat", None)
    if callable(isoformat):
        return str(isoformat())
    return str(value)


def _parse_target_ref(
    target_ref: str, expected_kind: str = "job"
) -> tuple[str, int] | None:
    """Parse a V5+ ``target_ref`` into ``(scheduler_key, job_id)``.

    Grammar accepted (REQ-8 / AC-8.2 / AC-8.3):

    - ``job:local:<N>``          → ``("local", N)``
    - ``job:ssh:<profile>:<N>``  → ``(f"ssh:{profile}", N)``

    Returns ``None`` for:

    - legacy 2-segment refs (``job:<N>``) — V5 migration backfills every
      existing row to 3+ segments, so this should not appear in practice;
      returning ``None`` here keeps the parser strict (AC-8.4).
    - malformed refs (non-int tail, unknown kind prefix, empty segments).

    Phase 6 callers use the returned ``scheduler_key`` to group watches
    by transport and to write ``source_ref`` back with the correct
    transport segment.
    """
    parts = target_ref.split(":")
    if not parts or parts[0] != expected_kind:
        return None
    if len(parts) < 3:
        # Legacy 2-segment — after V5 migration these should not exist.
        return None
    try:
        job_id = int(parts[-1])
    except ValueError:
        return None
    middle = parts[1:-1]
    if middle == ["local"]:
        return ("local", job_id)
    if len(middle) == 2 and middle[0] == "ssh" and middle[1]:
        return (f"ssh:{middle[1]}", job_id)
    return None


def _parse_target_id(target_ref: str, expected_prefix: str) -> int | None:
    """Return the integer id encoded in a watch ``target_ref``.

    Thin wrapper around :func:`_parse_target_ref` for callers that only
    need the numeric id (``workflow_run:<N>`` / ``sweep_run:<N>`` etc.).
    For the ``job`` kind it drops the scheduler_key segment; new code
    that needs transport awareness should call :func:`_parse_target_ref`
    directly.

    Retains backward compatibility for the 2-segment ``workflow_run:<N>``
    and ``sweep_run:<N>`` forms (those kinds never got the V5 scheduler
    segment) while deferring to the strict 3+ segment parser for ``job``.
    """
    if expected_prefix == "job":
        parsed = _parse_target_ref(target_ref, "job")
        return parsed[1] if parsed else None

    prefix, _, remainder = target_ref.partition(":")
    if prefix != expected_prefix or not remainder:
        return None
    tail = remainder.rsplit(":", 1)[-1]
    try:
        return int(tail)
    except ValueError:
        return None


class ActiveWatchPoller:
    """Producer side of the notification Outbox.

    One instance is registered with :class:`~srunx.pollers.supervisor.PollerSupervisor`
    at web-server start-up. Each ``run_cycle`` iteration:

    1. Opens a dedicated sqlite3 connection (caller-owned for the
       lifetime of the cycle).
    2. Loads every open watch.
    3. Batches a single ``queue_by_ids`` call against the SLURM backend.
    4. For each observed transition, writes the transition row,
       updates ``jobs``, inserts the event, fans out to ``deliveries``,
       and closes the watch on terminal states — all inside a single
       ``BEGIN IMMEDIATE`` transaction.
    5. Closes the connection.
    """

    name: str = "active_watch_poller"

    def __init__(
        self,
        slurm_client: SlurmClientProtocol | None = None,
        *,
        registry: TransportRegistry | None = None,
        db_path: Path | None = None,
        notification_service_factory: NotificationServiceFactory | None = None,
        interval_seconds: float = 15.0,
    ) -> None:
        """Initialise the poller.

        Args:
            slurm_client: Legacy single-transport client kept for
                backward compatibility with existing tests and the Web
                app's single-profile lifespan. When provided without a
                ``registry`` the poller runs in single-transport mode
                and treats every job watch as belonging to that client
                (same semantics as pre-Phase 6).
            registry: V5 :class:`TransportRegistry`. When provided, the
                poller groups watches by ``scheduler_key`` and routes
                each group to the matching
                :class:`SlurmClientProtocol` implementation (REQ-8).
                Takes precedence over ``slurm_client``.
            db_path: Absolute path to the srunx state DB. ``None``
                resolves to :func:`srunx.db.connection.get_db_path` at
                connection time.
            notification_service_factory: Override the constructor
                used to build the per-cycle :class:`NotificationService`.
                Useful for tests that need to spy on fan-out; in
                production the default factory is sufficient.
            interval_seconds: Sleep between cycles. Default 15 s per
                R10.1 (``DeliveryPoller`` uses 10 s so the producer
                stays slower than the consumer to avoid hammering
                ``squeue``).

        Raises:
            ValueError: When neither ``registry`` nor ``slurm_client``
                is supplied.
        """
        if registry is None and slurm_client is None:
            raise ValueError(
                "ActiveWatchPoller requires either 'registry' (preferred) or "
                "'slurm_client' (backcompat)."
            )
        self._registry = registry
        self._slurm_client = slurm_client
        self._db_path = db_path
        self._notification_service_factory = (
            notification_service_factory or _default_notification_service_factory
        )
        self.interval_seconds = interval_seconds

    # ------------------------------------------------------------------
    # Poller protocol entry point
    # ------------------------------------------------------------------

    async def run_cycle(self) -> None:
        """Execute one cycle end-to-end.

        The method re-raises any exception it encounters so that
        :class:`~srunx.pollers.supervisor.PollerSupervisor` can apply
        exponential backoff. Every handled path writes a structured
        log entry summarising the cycle.
        """
        started_at = time.monotonic()
        open_watches = 0
        transitions_detected = 0
        events_emitted = 0

        conn = open_connection(self._db_path)
        try:
            watch_repo = WatchRepository(conn)
            subscription_repo = SubscriptionRepository(conn)
            event_repo = EventRepository(conn)
            delivery_repo = DeliveryRepository(conn)
            endpoint_repo = EndpointRepository(conn)
            job_repo = JobRepository(conn)
            transition_repo = JobStateTransitionRepository(conn)
            workflow_run_repo = WorkflowRunRepository(conn)
            workflow_run_job_repo = WorkflowRunJobRepository(conn)

            notification_service = self._notification_service_factory(
                watch_repo,
                subscription_repo,
                event_repo,
                delivery_repo,
                endpoint_repo,
            )

            watches = watch_repo.list_open()
            open_watches = len(watches)

            # Group job watches by scheduler_key so each transport group
            # can be routed to its own queue_client. Each entry is
            # ``(watch_id, job_id)``; the outer dict key is the
            # ``scheduler_key`` parsed from ``target_ref`` (REQ-8).
            job_watches_by_scheduler: dict[str, list[tuple[int, int]]] = {}
            workflow_watches: list[tuple[int, int]] = []  # (watch_id, run_id)

            for watch in watches:
                if watch.id is None:
                    continue
                if watch.kind == "job":
                    parsed = _parse_target_ref(watch.target_ref, "job")
                    if parsed is None:
                        continue
                    scheduler_key, job_id = parsed
                    job_watches_by_scheduler.setdefault(scheduler_key, []).append(
                        (watch.id, job_id)
                    )
                elif watch.kind == "workflow_run":
                    run_id = _parse_target_id(watch.target_ref, "workflow_run")
                    if run_id is not None:
                        workflow_watches.append((watch.id, run_id))
                # Other kinds (resource_threshold, scheduled_report) are
                # TBD — Phase 1 ignores them here and lets the owning
                # pollers handle them once they land.

            # -- Job transitions (per scheduler_key) ---------------------------
            # Each scheduler_key gets its own ``queue_by_ids`` round-trip
            # against the matching transport, then produces events whose
            # ``source_ref`` carries the scheduler_key so downstream
            # notifications / dedup stay consistent across transports.
            for scheduler_key, pairs in job_watches_by_scheduler.items():
                queue_client = self._resolve_queue_client(scheduler_key)
                if queue_client is None:
                    logger.warning(
                        "Unknown scheduler_key %r in %d watch(es); skipping "
                        "this cycle (profile may have been removed). "
                        "Watches will be retried next cycle.",
                        scheduler_key,
                        len(pairs),
                    )
                    continue

                job_ids = [jid for _, jid in pairs]
                try:
                    partial_result: dict[
                        int, JobStatusInfo
                    ] = await anyio.to_thread.run_sync(
                        queue_client.queue_by_ids, job_ids
                    )
                except Exception as exc:  # noqa: BLE001 — transport failure
                    logger.warning(
                        "queue_by_ids failed for scheduler_key %r: %s; "
                        "skipping this group for this cycle.",
                        scheduler_key,
                        exc,
                    )
                    continue

                group_transitions, group_events = self._process_job_watches(
                    conn=conn,
                    job_watches=pairs,
                    queue_result=partial_result,
                    scheduler_key=scheduler_key,
                    job_repo=job_repo,
                    transition_repo=transition_repo,
                    event_repo=event_repo,
                    watch_repo=watch_repo,
                    notification_service=notification_service,
                )
                transitions_detected += group_transitions
                events_emitted += group_events

            # -- Workflow-run aggregation --------------------------------------
            wf_transitions, wf_events = self._process_workflow_watches(
                conn=conn,
                workflow_watches=workflow_watches,
                workflow_run_repo=workflow_run_repo,
                workflow_run_job_repo=workflow_run_job_repo,
                job_repo=job_repo,
                watch_repo=watch_repo,
            )
            transitions_detected += wf_transitions
            events_emitted += wf_events
        finally:
            conn.close()

        elapsed_ms = int((time.monotonic() - started_at) * 1000)
        logger.bind(
            poller=self.name,
            open_watches=open_watches,
            transitions_detected=transitions_detected,
            events_emitted=events_emitted,
            elapsed_ms=elapsed_ms,
        ).info(f"active_watch_poller cycle complete: {open_watches} open watches")

    # ------------------------------------------------------------------
    # Job branch
    # ------------------------------------------------------------------

    def _resolve_queue_client(self, scheduler_key: str) -> SlurmClientProtocol | None:
        """Resolve ``scheduler_key`` to a ``SlurmClientProtocol``.

        Preference order:

        1. If a :class:`TransportRegistry` was injected, delegate to
           :meth:`TransportRegistry.resolve` so SSH transports pick up
           their profile-specific adapter (REQ-8).
        2. Otherwise, fall back to the legacy single-transport
           ``slurm_client`` for ``scheduler_key == "local"`` — this
           keeps existing tests and single-profile Web lifespans
           working without changes.

        Returns ``None`` for unresolvable keys so the caller can log a
        warning and skip the affected group (AC-8.5) without aborting
        the entire cycle.
        """
        if self._registry is not None:
            handle = self._registry.resolve(scheduler_key)
            return handle.queue_client if handle is not None else None
        # Backcompat: single-transport mode. Only local watches are
        # served by the supplied client; SSH watches are skipped so the
        # poller does not mis-route remote jobs to a local SLURM.
        if scheduler_key == "local":
            return self._slurm_client
        return None

    def _process_job_watches(
        self,
        *,
        conn: sqlite3.Connection,
        job_watches: list[tuple[int, int]],
        queue_result: dict[int, JobStatusInfo],
        scheduler_key: str,
        job_repo: JobRepository,
        transition_repo: JobStateTransitionRepository,
        event_repo: EventRepository,
        watch_repo: WatchRepository,
        notification_service: NotificationService,
    ) -> tuple[int, int]:
        """Process every job-kind watch. Returns (transitions, events).

        ``scheduler_key`` is the transport axis for every watch in the
        ``job_watches`` slice; it's used to write V5-grammar
        ``source_ref`` (``job:{scheduler_key}:{job_id}``) so
        notifications / dedup / Slack templating all see the same
        transport segment the watch was created with (REQ-8).
        """
        transitions_count = 0
        events_count = 0

        # De-duplicate job_ids — multiple watches can target the same
        # job, we only want to emit one transition/event per job per
        # cycle, then close every matching watch when terminal.
        seen_job_ids: set[int] = set()

        for _watch_id, job_id in job_watches:
            if job_id in seen_job_ids:
                continue
            seen_job_ids.add(job_id)

            status_info = queue_result.get(job_id)
            if status_info is None:
                # Job not visible in queue/sacct — skip. Either it has
                # not registered with SLURM yet, or it is older than
                # sacct's retention window.
                continue

            current_status = status_info.status
            latest = transition_repo.latest_for_job(job_id)

            if latest is None:
                # First observation of this job. Skip per design:
                # submission-time transitions are captured by the
                # submit router, not the poller, so the poller would
                # only emit noise for jobs that started before watch
                # creation.
                continue

            from_status = latest.to_status
            if from_status == current_status:
                continue

            # Transition detected — commit everything atomically.
            started_iso = _dt_to_iso(status_info.started_at)
            completed_iso = _dt_to_iso(status_info.completed_at)

            # Look up the job's human-friendly name so downstream adapters
            # (Slack, email, generic webhooks) can render informative
            # messages without having to re-query the DB or parse
            # source_ref themselves.
            existing_job = job_repo.get(job_id)
            job_name = existing_job.name if existing_job is not None else None

            with transaction(conn, "IMMEDIATE"):
                transition_repo.insert(
                    job_id=job_id,
                    from_status=from_status,
                    to_status=current_status,
                    source="poller",
                )
                job_repo.update_status(
                    job_id,
                    current_status,
                    started_at=started_iso,
                    completed_at=completed_iso,
                    duration_secs=status_info.duration_secs,
                    nodelist=status_info.nodelist,
                )
                transitions_count += 1

                payload: dict[str, object] = {
                    "job_id": job_id,
                    "job_name": job_name,
                    "from_status": from_status,
                    "to_status": current_status,
                    "started_at": started_iso,
                    "completed_at": completed_iso,
                }
                # V5 grammar: ``job:<scheduler_key>:<id>``. In Phase 6
                # the scheduler_key is threaded in from the watch row
                # so SSH-backed watches produce ``job:ssh:<profile>:<id>``
                # and local watches produce ``job:local:<id>``.
                source_ref = f"job:{scheduler_key}:{job_id}"
                event_id = event_repo.insert(
                    kind="job.status_changed",
                    source_ref=source_ref,
                    payload=payload,
                )
                if event_id is not None:
                    events_count += 1
                    event = event_repo.get(event_id)
                    if event is not None:
                        notification_service.fan_out(event, conn)

                # Close every matching watch once the job hits a
                # terminal state — a batch of N watches on the same
                # job collapses to a single transition + N closes.
                if current_status in SLURM_TERMINAL_JOB_STATES:
                    for closing_watch in watch_repo.list_by_target(
                        kind="job",
                        target_ref=source_ref,
                        only_open=True,
                    ):
                        if closing_watch.id is not None:
                            watch_repo.close(closing_watch.id)

        return transitions_count, events_count

    # ------------------------------------------------------------------
    # Workflow-run branch
    # ------------------------------------------------------------------

    def _process_workflow_watches(
        self,
        *,
        conn: sqlite3.Connection,
        workflow_watches: list[tuple[int, int]],
        workflow_run_repo: WorkflowRunRepository,
        workflow_run_job_repo: WorkflowRunJobRepository,
        job_repo: JobRepository,
        watch_repo: WatchRepository,
    ) -> tuple[int, int]:
        """Process every workflow_run-kind watch. Returns (transitions, events)."""
        transitions_count = 0
        events_count = 0
        seen_run_ids: set[int] = set()

        for _watch_id, run_id in workflow_watches:
            if run_id in seen_run_ids:
                continue
            seen_run_ids.add(run_id)

            run = workflow_run_repo.get(run_id)
            if run is None:
                continue

            memberships = workflow_run_job_repo.list_by_run(run_id)
            new_status = self._aggregate_workflow_status(
                memberships=memberships,
                job_repo=job_repo,
            )

            if new_status == run.status:
                continue

            source_ref = f"workflow_run:{run_id}"
            is_terminal = new_status in _TERMINAL_WORKFLOW_RUN_STATUSES
            completed_iso: str | None = now_iso() if is_terminal else None

            # Route through WorkflowRunStateService so status UPDATE,
            # workflow_run.status_changed event insert, delivery fan-out,
            # and (for sweep-backed runs) sweep aggregation all land in
            # one TX with consistent dedup semantics.
            with transaction(conn, "IMMEDIATE"):
                updated = WorkflowRunStateService.update(
                    conn=conn,
                    workflow_run_id=run_id,
                    from_status=run.status,
                    to_status=new_status,
                    completed_at=completed_iso,
                )
                if updated:
                    transitions_count += 1
                    events_count += 1

                    if is_terminal:
                        for closing_watch in watch_repo.list_by_target(
                            kind="workflow_run",
                            target_ref=source_ref,
                            only_open=True,
                        ):
                            if closing_watch.id is not None:
                                watch_repo.close(closing_watch.id)

        return transitions_count, events_count

    # ------------------------------------------------------------------
    # Pure helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _aggregate_workflow_status(
        *,
        memberships: list[WorkflowRunJob],
        job_repo: JobRepository,
    ) -> str:
        """Aggregate child-job statuses into a workflow-run status.

        R2 rules, evaluated in order:

        1. Any child CANCELLED → ``cancelled``.
        2. Any child in ``{FAILED, TIMEOUT, NODE_FAIL}`` → ``failed``.
        3. All children observed and COMPLETED → ``completed``.
        4. Any child RUNNING → ``running``.
        5. Otherwise → ``pending``.

        Memberships whose SLURM ``job_id`` is still ``NULL`` (jobs
        that haven't been submitted yet) count towards "not all
        completed" but don't force ``pending`` on their own.
        """
        if not memberships:
            return "pending"

        child_statuses: list[str | None] = []
        for membership in memberships:
            job_id = getattr(membership, "job_id", None)
            if job_id is None:
                child_statuses.append(None)
                continue
            job = job_repo.get(job_id)
            child_statuses.append(job.status if job is not None else None)

        # Rule 1 — any CANCELLED wins.
        if any(s == "CANCELLED" for s in child_statuses):
            return "cancelled"

        # Rule 2 — any hard failure (FAILED / TIMEOUT / NODE_FAIL /
        # PREEMPTED / OUT_OF_MEMORY). The design.md wording is
        # "FAILED/TIMEOUT" but the other SLURM terminal failures are
        # semantically identical and already in
        # ``SLURM_TERMINAL_JOB_STATES`` minus COMPLETED/CANCELLED.
        if any(
            s in {"FAILED", "TIMEOUT", "NODE_FAIL", "PREEMPTED", "OUT_OF_MEMORY"}
            for s in child_statuses
        ):
            return "failed"

        # Rule 3 — all COMPLETED (every membership resolved and every
        # child is COMPLETED).
        if all(s == "COMPLETED" for s in child_statuses):
            return "completed"

        # Rule 4 — any RUNNING child.
        if any(s == "RUNNING" for s in child_statuses):
            return "running"

        return "pending"
