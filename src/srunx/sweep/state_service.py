"""Centralized entry point for workflow_run status transitions.

Every writer (runner, poller, orchestrator) routes status updates through
this service so that: (1) the ``workflow_runs`` row is updated under an
optimistic lock, (2) a ``workflow_run.status_changed`` event is always
emitted (the UNIQUE ``(kind, source_ref, payload_hash)`` index dedups
parallel observers), and (3) when the run belongs to a sweep, the sweep
aggregator is invoked to roll forward sweep-level status + events.
"""

from __future__ import annotations

import sqlite3

from srunx.db.repositories.deliveries import DeliveryRepository
from srunx.db.repositories.endpoints import EndpointRepository
from srunx.db.repositories.events import EventRepository
from srunx.db.repositories.subscriptions import SubscriptionRepository
from srunx.db.repositories.sweep_runs import SweepRunRepository
from srunx.db.repositories.watches import WatchRepository
from srunx.notifications.service import NotificationService

# Terminal workflow_run statuses. Once a run reaches one of these, the
# state service refuses to transition it back to a non-terminal status
# (e.g. ``completed → pending``). This guards against stale poller
# aggregations observing an empty ``workflow_run_jobs`` set (as happens
# for sweep cells) and incorrectly pulling the cell back to pending.
_TERMINAL_WORKFLOW_RUN_STATUSES: frozenset[str] = frozenset(
    {"completed", "failed", "cancelled"}
)


class WorkflowRunStateService:
    """Single entry point for workflow_run status transitions."""

    @classmethod
    def update(
        cls,
        *,
        conn: sqlite3.Connection,
        workflow_run_id: int,
        from_status: str,
        to_status: str,
        error: str | None = None,
        completed_at: str | None = None,
    ) -> bool:
        """Transition a workflow_run and fan out related events.

        ``conn`` must own an active ``BEGIN IMMEDIATE`` transaction; this
        method never opens one. Returns True iff the optimistic UPDATE
        affected a row (i.e. this caller won the race).

        Order of operations (all under caller TX):

        1. ``SweepRunRepository.transition_cell(...)`` — optimistic
           ``UPDATE workflow_runs`` + optional sweep counter sync.
        2. If the transition did not happen, return False.
        3. Insert a ``workflow_run.status_changed`` event
           (``EventRepository.insert`` is idempotent thanks to the
           ``(kind, source_ref, payload_hash)`` UNIQUE index).
        4. Fan the event out to deliveries.
        5. If the run is sweep-backed, invoke
           :func:`srunx.sweep.aggregator.evaluate_and_fire_sweep_status_event`.
        """
        # Circular import guard: aggregator imports this module's siblings.
        from srunx.sweep.aggregator import evaluate_and_fire_sweep_status_event

        # Reject terminal → non-terminal regressions. A poller observing
        # a stale view (e.g. sweep cells lack workflow_run_jobs rows and
        # aggregate to 'pending') would otherwise revive a finalized run.
        if (
            from_status in _TERMINAL_WORKFLOW_RUN_STATUSES
            and to_status not in _TERMINAL_WORKFLOW_RUN_STATUSES
        ):
            return False

        sweep_repo = SweepRunRepository(conn)
        cell_updated = sweep_repo.transition_cell(
            conn=conn,
            workflow_run_id=workflow_run_id,
            from_status=from_status,
            to_status=to_status,
            error=error,
            completed_at=completed_at,
        )
        if not cell_updated:
            return False

        event_repo = EventRepository(conn)
        payload: dict[str, object] = {
            "workflow_run_id": workflow_run_id,
            "from_status": from_status,
            "to_status": to_status,
        }
        workflow_name = _fetch_workflow_name(conn, workflow_run_id)
        if workflow_name is not None:
            payload["workflow_name"] = workflow_name
        if error is not None:
            payload["error"] = error

        event_id = event_repo.insert(
            kind="workflow_run.status_changed",
            source_ref=f"workflow_run:{workflow_run_id}",
            payload=payload,
        )
        if event_id is not None:
            event = event_repo.get(event_id)
            if event is not None:
                notification_service = NotificationService(
                    watch_repo=WatchRepository(conn),
                    subscription_repo=SubscriptionRepository(conn),
                    event_repo=event_repo,
                    delivery_repo=DeliveryRepository(conn),
                    endpoint_repo=EndpointRepository(conn),
                )
                notification_service.fan_out(event, conn)

        sweep_run_id = _fetch_sweep_run_id(conn, workflow_run_id)
        if sweep_run_id is not None:
            evaluate_and_fire_sweep_status_event(conn=conn, sweep_run_id=sweep_run_id)

        return True


def _fetch_sweep_run_id(conn: sqlite3.Connection, workflow_run_id: int) -> int | None:
    row = conn.execute(
        "SELECT sweep_run_id FROM workflow_runs WHERE id = ?",
        (workflow_run_id,),
    ).fetchone()
    if row is None:
        return None
    value = row["sweep_run_id"] if isinstance(row, sqlite3.Row) else row[0]
    return int(value) if value is not None else None


def _fetch_workflow_name(conn: sqlite3.Connection, workflow_run_id: int) -> str | None:
    """Look up ``workflow_runs.workflow_name`` for event payload enrichment."""
    row = conn.execute(
        "SELECT workflow_name FROM workflow_runs WHERE id = ?",
        (workflow_run_id,),
    ).fetchone()
    if row is None:
        return None
    value = row["workflow_name"] if isinstance(row, sqlite3.Row) else row[0]
    return str(value) if value is not None else None


__all__ = ["WorkflowRunStateService"]
