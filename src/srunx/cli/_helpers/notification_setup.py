"""CLI helper: attach a notification watch + subscription to a submitted job.

Thin wrapper around :func:`srunx.observability.notifications.attach.attach_job_notification`.
The shared module holds the endpoint-lookup + dedup + transition-seed
logic so the Web router (``POST /api/watches``) and the CLI submit path
both call the same core.

This wrapper adds CLI-specific semantics:

1. Looks the endpoint up by ``(kind, name)`` — users type a name, not an
   id, and the DB's uniqueness guarantee is ``UNIQUE(kind, name)``.
2. Catches every exception and logs a warning: a watch that can't be
   created should never break ``srunx submit``.
"""

from __future__ import annotations

from srunx.common.logging import get_logger

logger = get_logger(__name__)

# Default kind — Phase 1 of the notification stack only wires up
# slack_webhook. When other kinds (email / generic_webhook / slack_bot)
# ship we'll expose a ``--endpoint-kind`` CLI flag; until then we keep
# the API narrow so ``(kind, name)`` can always be resolved without
# ambiguity.
DEFAULT_ENDPOINT_KIND = "slack_webhook"


def attach_notification_watch(
    *,
    job_id: int,
    endpoint_name: str,
    preset: str = "terminal",
    endpoint_kind: str = DEFAULT_ENDPOINT_KIND,
    scheduler_key: str = "local",
) -> int | None:
    """Attach an endpoint-backed notification watch to a SLURM job.

    Args:
        job_id: SLURM job id. Must already be recorded in the new state
            DB (``jobs`` table) —
            :func:`srunx.observability.storage.cli_helpers.record_submission_from_job`
            handles that for CLI submits.
        endpoint_name: Name of the endpoint to notify. Must exist, not
            be disabled, and be of ``endpoint_kind``.
        preset: Subscription preset — ``terminal`` (default),
            ``running_and_terminal``, or ``all``.
        endpoint_kind: Endpoint kind to disambiguate name lookups.
            Defaults to ``slack_webhook`` (the only enabled Phase 1 kind).
        scheduler_key: ``"local"`` (default) for local SLURM, or
            ``"ssh:<profile>"`` for an SSH transport.

    Returns:
        The ``subscriptions.id`` on success (newly created or
        deduplicated existing), ``None`` on any failure (endpoint
        missing / disabled / preset unimplemented / DB error). All
        failure paths log a warning so ``srunx submit`` never aborts.
    """
    try:
        from srunx.observability.notifications.attach import (
            AttachWatchError,
            attach_job_notification,
        )
        from srunx.observability.storage.connection import init_db, open_connection
        from srunx.observability.storage.repositories.endpoints import (
            EndpointRepository,
        )

        init_db(delete_legacy=False)
        conn = open_connection()
        try:
            endpoint_repo = EndpointRepository(conn)
            # R12: scope the lookup to (kind, name) since the DB's UNIQUE
            # constraint is (kind, name); matching on name alone could
            # return the wrong row once other kinds are enabled.
            endpoint = endpoint_repo.get_by_name(endpoint_kind, endpoint_name)
            if endpoint is None:
                logger.warning(
                    "Endpoint %s:%s not found; skipping watch creation. "
                    "Create one via `Settings → Notifications` in the Web UI "
                    "or the /api/endpoints API.",
                    endpoint_kind,
                    endpoint_name,
                )
                return None
            if endpoint.id is None:
                return None

            # BEGIN IMMEDIATE so endpoint validation + watch/subscription
            # insert + transition seed land as one transaction. Without
            # this, two concurrent submits for the same (job, endpoint,
            # preset) triple could both pass dedup and both insert —
            # zombie rows the poller would fan out twice. The Web router
            # uses the same envelope (routers/watches.py).
            conn.execute("BEGIN IMMEDIATE")
            try:
                result = attach_job_notification(
                    conn=conn,
                    job_id=job_id,
                    endpoint_id=endpoint.id,
                    preset=preset,
                    scheduler_key=scheduler_key,
                )
            except AttachWatchError as exc:
                conn.rollback()
                logger.warning(
                    "Skipping watch creation for job %s on %s:%s (preset=%s): %s",
                    job_id,
                    endpoint_kind,
                    endpoint_name,
                    preset,
                    exc,
                )
                return None
            except BaseException:
                conn.rollback()
                raise
            else:
                conn.commit()

            if not result.created:
                logger.debug(
                    "Existing watch+subscription for job %s on %s:%s "
                    "(preset=%s); reusing.",
                    job_id,
                    endpoint_kind,
                    endpoint_name,
                    preset,
                )
            return result.subscription_id
        finally:
            conn.close()
    except Exception as exc:
        logger.warning(
            "Failed to attach notification watch for job %s: %s",
            job_id,
            exc,
        )
        return None
