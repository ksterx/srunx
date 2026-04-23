"""``workflow_runs`` status-transition helper.

Pulled unchanged out of :mod:`srunx.runner` as part of Phase 7 (#163). The
workflow runner calls :func:`_transition_workflow_run` at ``pending ->
running`` and at terminal transitions so DB-backed dashboards (sweep
aggregation, Web UI, ``srunx report``) stay in sync with the in-memory DAG
state machine.

Fails closed: DB outages are logged at debug level and swallowed so they
never take down the primary workflow flow.
"""

from srunx.logging import get_logger

logger = get_logger(__name__)


def _transition_workflow_run(
    workflow_run_id: int,
    from_status: str,
    to_status: str,
    *,
    error: str | None = None,
) -> None:
    """Best-effort ``workflow_runs`` status transition via the state service.

    Opens a short ``BEGIN IMMEDIATE`` TX on a fresh connection so that
    :class:`WorkflowRunStateService` (which refuses to open its own TX)
    can emit the ``workflow_run.status_changed`` event and — when the
    run belongs to a sweep — fan out sweep aggregation atomically.

    Fails closed: any exception is logged at debug and swallowed, so a
    DB outage never takes down the primary workflow flow.
    """
    try:
        from srunx.db.connection import initialized_connection, transaction
        from srunx.db.repositories.base import now_iso
        from srunx.sweep.state_service import WorkflowRunStateService

        completed_at = (
            now_iso() if to_status in {"completed", "failed", "cancelled"} else None
        )
        with initialized_connection() as conn:
            with transaction(conn, "IMMEDIATE"):
                WorkflowRunStateService.update(
                    conn=conn,
                    workflow_run_id=workflow_run_id,
                    from_status=from_status,
                    to_status=to_status,
                    error=error,
                    completed_at=completed_at,
                )
    except Exception as exc:  # noqa: BLE001 — best-effort
        logger.debug(f"_transition_workflow_run failed: {exc}")
