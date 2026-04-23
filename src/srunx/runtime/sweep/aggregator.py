"""Sweep-level status aggregation + event fan-out.

See design.md § "evaluate_and_fire_sweep_status_event" for the full
algorithm. All writes happen under the caller's BEGIN IMMEDIATE TX; the
function is idempotent via three defences: (1) target==current guard,
(2) optimistic-locked UPDATE, (3) event UNIQUE ``(kind, source_ref,
payload_hash)`` index.
"""

from __future__ import annotations

import sqlite3

from srunx.observability.notifications.service import NotificationService
from srunx.observability.storage.repositories.base import now_iso
from srunx.observability.storage.repositories.deliveries import DeliveryRepository
from srunx.observability.storage.repositories.endpoints import EndpointRepository
from srunx.observability.storage.repositories.events import EventRepository
from srunx.observability.storage.repositories.subscriptions import (
    SubscriptionRepository,
)
from srunx.observability.storage.repositories.watches import WatchRepository

_TERMINAL_SWEEP_STATUSES: frozenset[str] = frozenset(
    {"completed", "failed", "cancelled"}
)


def evaluate_and_fire_sweep_status_event(
    *,
    conn: sqlite3.Connection,
    sweep_run_id: int,
) -> None:
    """Evaluate sweep status transitions and fire a sweep_run.status_changed event.

    Runs under caller TX. No-op if the sweep has already reached its
    target state.
    """
    row = conn.execute(
        """
        SELECT status, cell_count, cells_pending, cells_running,
               cells_completed, cells_failed, cells_cancelled,
               cancel_requested_at, name
        FROM sweep_runs WHERE id = ?
        """,
        (sweep_run_id,),
    ).fetchone()
    if row is None:
        return

    current_status: str = row["status"]
    cell_count: int = row["cell_count"]
    cells_pending: int = row["cells_pending"]
    cells_running: int = row["cells_running"]
    cells_completed: int = row["cells_completed"]
    cells_failed: int = row["cells_failed"]
    cells_cancelled: int = row["cells_cancelled"]
    cancel_requested_at = row["cancel_requested_at"]
    name: str = row["name"]

    target_status = _compute_target_status(
        current_status=current_status,
        cell_count=cell_count,
        cells_pending=cells_pending,
        cells_running=cells_running,
        cells_completed=cells_completed,
        cells_failed=cells_failed,
        cells_cancelled=cells_cancelled,
        cancel_requested=cancel_requested_at is not None,
    )
    if target_status is None or target_status == current_status:
        return

    is_terminal = target_status in _TERMINAL_SWEEP_STATUSES
    completed_iso = now_iso() if is_terminal else None

    if completed_iso is not None:
        cur = conn.execute(
            "UPDATE sweep_runs SET status = ?, completed_at = ? "
            "WHERE id = ? AND status = ?",
            (target_status, completed_iso, sweep_run_id, current_status),
        )
    else:
        cur = conn.execute(
            "UPDATE sweep_runs SET status = ? WHERE id = ? AND status = ?",
            (target_status, sweep_run_id, current_status),
        )
    if cur.rowcount == 0:
        return

    representative_error = _fetch_representative_error(conn, sweep_run_id)

    payload: dict[str, object] = {
        "sweep_run_id": sweep_run_id,
        "name": name,
        "from_status": current_status,
        "to_status": target_status,
        "cell_count": cell_count,
        "cells_completed": cells_completed,
        "cells_failed": cells_failed,
        "cells_cancelled": cells_cancelled,
        "cells_running": cells_running,
        "cells_pending": cells_pending,
        "representative_error": representative_error,
    }

    event_repo = EventRepository(conn)
    event_id = event_repo.insert(
        kind="sweep_run.status_changed",
        source_ref=f"sweep_run:{sweep_run_id}",
        payload=payload,
    )
    if event_id is None:
        return

    event = event_repo.get(event_id)
    if event is None:
        return

    notification_service = NotificationService(
        watch_repo=WatchRepository(conn),
        subscription_repo=SubscriptionRepository(conn),
        event_repo=event_repo,
        delivery_repo=DeliveryRepository(conn),
        endpoint_repo=EndpointRepository(conn),
    )
    notification_service.fan_out(event, conn)


def _compute_target_status(
    *,
    current_status: str,
    cell_count: int,
    cells_pending: int,
    cells_running: int,
    cells_completed: int,
    cells_failed: int,
    cells_cancelled: int,
    cancel_requested: bool,
) -> str | None:
    """Return the next sweep status per R4.6, or ``None`` for no-op.

    Evaluation order matches R4.6:
      1. cancel_requested AND all in-flight terminal => 'cancelled'
      2. all in-flight terminal AND any failed       => 'failed'
      3. all in-flight terminal AND any cancelled    => 'cancelled'
      4. all in-flight terminal AND all completed    => 'completed'
      5. cells_running > 0 AND current == 'pending'  => 'running'
      6. otherwise                                    => no-op
    """
    all_in_flight_done = (cells_pending + cells_running) == 0

    if cancel_requested and all_in_flight_done:
        return "cancelled"
    if all_in_flight_done and cells_failed > 0:
        return "failed"
    if all_in_flight_done and cells_cancelled > 0:
        return "cancelled"
    if all_in_flight_done and cells_completed == cell_count:
        return "completed"
    if cells_running > 0 and current_status == "pending":
        return "running"
    return None


def _fetch_representative_error(
    conn: sqlite3.Connection, sweep_run_id: int
) -> str | None:
    """Pick the earliest-completing failed child's ``error`` field.

    Tie-break on ``id`` ascending per R6.4 so the choice is deterministic
    under concurrent observers.
    """
    row = conn.execute(
        """
        SELECT error FROM workflow_runs
        WHERE sweep_run_id = ? AND status = 'failed'
        ORDER BY completed_at ASC, id ASC
        LIMIT 1
        """,
        (sweep_run_id,),
    ).fetchone()
    if row is None:
        return None
    value = row["error"] if isinstance(row, sqlite3.Row) else row[0]
    return str(value) if value is not None else None


__all__ = ["evaluate_and_fire_sweep_status_event"]
