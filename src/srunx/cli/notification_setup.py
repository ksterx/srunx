"""CLI helper: attach a notification watch + subscription to a submitted job.

Bridges the CLI submit path with the endpoint/watch/subscription domain.
Call sites (:mod:`srunx.cli.main` / :mod:`srunx.cli.workflow`) invoke
:func:`attach_notification_watch` after :meth:`srunx.client.Slurm.submit`
returns so the job lands in the notification pipeline:

1. Resolves the endpoint by ``(kind, name)`` — the DB's uniqueness
   guarantee is ``UNIQUE(kind, name)``, so name alone is ambiguous
   once more than one endpoint kind is enabled.
2. Creates a ``kind='job'`` watch targeting the submitted job.
3. Creates the subscription with the chosen preset.
4. Seeds a PENDING ``job_state_transitions`` row so the active-watch
   poller's first observation produces a real transition.

Failures are logged but non-fatal: a watch that can't be created should
never break the submit.
"""

from __future__ import annotations

from srunx.logging import get_logger

logger = get_logger(__name__)

# Default kind — Phase 1 of the notification stack only wires up
# slack_webhook. When other kinds (email / generic_webhook / slack_bot)
# ship we'll expose a ``--endpoint-kind`` CLI flag; until then we keep
# the API narrow so ``(kind, name)`` can always be resolved without
# ambiguity.
DEFAULT_ENDPOINT_KIND = "slack_webhook"

# Presets that actually produce deliveries today. ``digest`` is a
# documented schema value but no delivery-time aggregator exists yet —
# letting it through here reproduces the footgun the POST subscriptions
# router closes (P1-3 #D): a subscription with ``preset='digest'`` is
# created silently and nothing ever ships.
_IMPLEMENTED_PRESETS = ("terminal", "running_and_terminal", "all")


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
            :func:`srunx.db.cli_helpers.record_submission_from_job`
            handles that for CLI submits.
        endpoint_name: Name of the endpoint to notify. Must exist, not
            be disabled, and be of ``endpoint_kind``.
        preset: Subscription preset — ``terminal`` (default),
            ``running_and_terminal``, ``all``, or ``digest``.
        endpoint_kind: Endpoint kind to disambiguate name lookups.
            Defaults to ``slack_webhook`` (the only enabled Phase 1 kind).

    Returns:
        The new ``subscriptions.id`` on success, ``None`` on any
        failure (endpoint missing / disabled / DB error). All failure
        paths log a warning so ``srunx submit`` never aborts.
    """
    # Reject presets that don't have a delivery implementation today.
    # Silently accepting ``digest`` here would write a subscription the
    # delivery poller never fans out — the same footgun P1-3 closes at
    # the POST ``/api/subscriptions`` endpoint. Keep the guard narrow:
    # we still accept anything else in case a test config or future
    # preset lands ahead of the lookup constant.
    if preset not in _IMPLEMENTED_PRESETS:
        logger.warning(
            "Preset %r is not implemented for delivery; skipping watch "
            "creation for job %s. Use one of: %s.",
            preset,
            job_id,
            ", ".join(_IMPLEMENTED_PRESETS),
        )
        return None

    try:
        from srunx.db.connection import init_db, open_connection
        from srunx.db.repositories.endpoints import EndpointRepository
        from srunx.db.repositories.job_state_transitions import (
            JobStateTransitionRepository,
        )
        from srunx.db.repositories.subscriptions import SubscriptionRepository
        from srunx.db.repositories.watches import WatchRepository

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
            if endpoint.disabled_at is not None:
                logger.warning(
                    "Endpoint %s:%s is disabled; skipping watch creation.",
                    endpoint_kind,
                    endpoint_name,
                )
                return None
            if endpoint.id is None:
                return None

            watch_repo = WatchRepository(conn)
            sub_repo = SubscriptionRepository(conn)
            transition_repo = JobStateTransitionRepository(conn)

            # Dedup: ``WatchRepository`` has no ``(kind, target_ref)``
            # uniqueness constraint, so a second ``srunx monitor jobs
            # 123 --endpoint foo`` would otherwise spawn a duplicate
            # watch + subscription for the same (job, endpoint, preset)
            # triple. Deliveries stay idempotent via their own
            # ``idempotency_key`` uniqueness, but the DB would still
            # accumulate zombie rows. Reuse an existing open watch +
            # subscription when one already exists.
            #
            # V5 grammar: ``target_ref`` is ``job:<scheduler_key>:<id>``
            # for local, or ``job:ssh:<profile>:<id>`` for SSH. The
            # ``scheduler_key`` kwarg encodes "local" / "ssh:<profile>"
            # already, so a single format-string covers both.
            target_ref = f"job:{scheduler_key}:{job_id}"
            existing = watch_repo.list_by_target(
                kind="job",
                target_ref=target_ref,
                only_open=True,
            )
            for w in existing:
                if w.id is None:
                    continue
                for sub in sub_repo.list_by_watch(w.id):
                    if sub.endpoint_id == endpoint.id and sub.preset == preset:
                        logger.debug(
                            "Existing watch+subscription for job %s on "
                            "%s:%s (preset=%s); skipping.",
                            job_id,
                            endpoint_kind,
                            endpoint_name,
                            preset,
                        )
                        return sub.id

            watch_id = watch_repo.create(kind="job", target_ref=target_ref)
            subscription_id = sub_repo.create(
                watch_id=watch_id,
                endpoint_id=endpoint.id,
                preset=preset,
            )
            # Only insert if no transition exists yet (dual-write may
            # have already seeded one on record_job).
            if (
                transition_repo.latest_for_job(job_id, scheduler_key=scheduler_key)
                is None
            ):
                transition_repo.insert(
                    job_id=job_id,
                    from_status=None,
                    to_status="PENDING",
                    source="webhook",
                    scheduler_key=scheduler_key,
                )
            return subscription_id
        finally:
            conn.close()
    except Exception as exc:
        logger.warning(
            "Failed to attach notification watch for job %s: %s",
            job_id,
            exc,
        )
        return None
