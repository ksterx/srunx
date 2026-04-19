"""Preset → event filter logic.

Pure function with no external dependencies. Used by
:class:`srunx.notifications.service.NotificationService` when deciding
whether a given subscription should generate a delivery for an event.
"""

from __future__ import annotations

# Terminal job statuses, per SLURM conventions.
_TERMINAL_JOB_STATUSES: frozenset[str] = frozenset(
    {
        "COMPLETED",
        "FAILED",
        "CANCELLED",
        "TIMEOUT",
        "NODE_FAIL",
        "PREEMPTED",
        "OUT_OF_MEMORY",
    }
)

# Terminal workflow_run statuses, per our own domain model.
_TERMINAL_WORKFLOW_RUN_STATUSES: frozenset[str] = frozenset(
    {"completed", "failed", "cancelled"}
)


def should_deliver(
    preset: str,
    event_kind: str,
    to_status: str | None,
) -> bool:
    """Return ``True`` when ``preset`` allows delivery for the given event.

    Args:
        preset: One of ``'terminal' | 'running_and_terminal' | 'all' | 'digest'``.
        event_kind: The kind of event (e.g. ``'job.status_changed'``).
        to_status: The ``to_status`` payload field for status-change events
            (``None`` for events without a status).

    Returns:
        ``True`` if the subscription with ``preset`` should produce a
        delivery for this event, ``False`` otherwise.

    Notes:
        - ``'digest'`` batching is not yet implemented. This function
          always returns ``False`` for it. The ``/api/subscriptions``
          router refuses to create *new* rows with this preset (see
          ``_ACCEPTED_PRESETS_FOR_CREATE`` in
          ``srunx.web.routers.subscriptions``), so the only way to
          reach this branch today is on rows that pre-date the
          enforcement — they are read-through but produce zero
          deliveries. (P1-3)
        - ``job.submitted``, ``resource.threshold_crossed`` and
          ``scheduled_report.due`` only deliver under preset ``'all'``.
    """
    if preset == "digest":
        return False

    if preset == "all":
        return True

    # From here on: preset is 'terminal' or 'running_and_terminal'.
    if event_kind == "job.status_changed":
        if to_status in _TERMINAL_JOB_STATUSES:
            return True
        if preset == "running_and_terminal" and to_status == "RUNNING":
            return True
        return False

    if event_kind == "workflow_run.status_changed":
        if to_status in _TERMINAL_WORKFLOW_RUN_STATUSES:
            return True
        if preset == "running_and_terminal" and to_status == "running":
            return True
        return False

    # job.submitted, resource.threshold_crossed, scheduled_report.due
    # only fire under preset='all' (handled above).
    return False
