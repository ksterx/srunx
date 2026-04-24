"""Shared logic for attaching a notification watch to an existing job.

Both the CLI (``srunx watch jobs --endpoint ...`` / submit-time
``--endpoint``) and the Web UI (``POST /api/watches`` for kind=job) need
the same three-step operation against an already-recorded job:

1. Resolve the endpoint by id; reject missing / disabled / unimplemented-preset
2. Idempotently create (or reuse) an open ``kind='job'`` watch + its
   subscription for the (job, endpoint, preset) triple
3. Seed a PENDING ``job_state_transitions`` baseline when absent, so the
   active-watch poller's first observation produces a real transition

The CLI wraps this with warn-log-and-return-None semantics (submit is
never aborted by a notify failure); the Web router wraps it with
``HTTPException`` translation. Both sit on the same core.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass

from srunx.observability.storage.repositories.endpoints import EndpointRepository
from srunx.observability.storage.repositories.job_state_transitions import (
    JobStateTransitionRepository,
)
from srunx.observability.storage.repositories.subscriptions import (
    SubscriptionRepository,
)
from srunx.observability.storage.repositories.watches import WatchRepository

# Presets the DB schema CHECK allows. Kept in lock-step with
# ``subscriptions.py`` ``_VALID_PRESETS`` so error messages stay aligned
# across the two POST routes.
VALID_PRESETS = ("terminal", "running_and_terminal", "all", "digest")

# Presets that actually produce deliveries today. ``digest`` is schema-
# valid but has no aggregator — silently accepting it would write a
# subscription the poller never fans out.
ACCEPTED_PRESETS = ("terminal", "running_and_terminal", "all")


class AttachWatchError(ValueError):
    """Base for attach-time validation errors."""


class EndpointNotFoundError(AttachWatchError):
    """The referenced endpoint does not exist."""


class EndpointDisabledError(AttachWatchError):
    """The referenced endpoint exists but is disabled."""


class InvalidPresetError(AttachWatchError):
    """The preset value is not in the schema allowlist at all."""


class UnsupportedPresetError(AttachWatchError):
    """The preset is schema-valid but has no delivery implementation yet (e.g. ``digest``)."""


@dataclass(frozen=True)
class AttachResult:
    """Outcome of :func:`attach_job_notification`.

    ``created`` is ``False`` when the call was a no-op reuse of an
    existing (watch, subscription) pair — callers can surface a
    "Already enabled" hint instead of "Created" when that happens.
    """

    watch_id: int
    subscription_id: int
    created: bool


def attach_job_notification(
    *,
    conn: sqlite3.Connection,
    job_id: int,
    endpoint_id: int,
    preset: str = "terminal",
    scheduler_key: str = "local",
) -> AttachResult:
    """Attach a watch + subscription to an existing SLURM job.

    Caller owns the transaction boundary — wrap the call in
    ``BEGIN IMMEDIATE`` / ``COMMIT`` on the same ``conn`` when atomicity
    across endpoint validation + watch/subscription insert + transition
    seed matters (the Web router does; the CLI path runs in sqlite
    autocommit mode like the old helper).

    Raises:
        InvalidPresetError: ``preset`` is not in the schema allowlist.
        UnsupportedPresetError: ``preset`` is schema-valid but has no
            delivery path today (e.g. ``digest``).
        EndpointNotFoundError: ``endpoint_id`` does not exist.
        EndpointDisabledError: the endpoint exists but ``disabled_at``
            is set.
    """
    # Two-tier validation mirroring ``subscriptions.py`` POST: reject
    # schema-invalid values with a distinct error so API consumers get
    # a clearer message than "not implemented yet" for, say, a typo.
    if preset not in VALID_PRESETS:
        raise InvalidPresetError(f"Invalid preset {preset!r}. Allowed: {VALID_PRESETS}")
    if preset not in ACCEPTED_PRESETS:
        raise UnsupportedPresetError(
            f"Preset {preset!r} is not implemented yet. Accepted: {ACCEPTED_PRESETS}"
        )

    endpoint_repo = EndpointRepository(conn)
    endpoint = endpoint_repo.get(endpoint_id)
    if endpoint is None:
        raise EndpointNotFoundError(f"Endpoint id={endpoint_id} not found")
    if endpoint.disabled_at is not None:
        raise EndpointDisabledError(f"Endpoint id={endpoint_id} is disabled")

    watch_repo = WatchRepository(conn)
    sub_repo = SubscriptionRepository(conn)
    transition_repo = JobStateTransitionRepository(conn)

    # V5 grammar: ``target_ref`` is ``job:<scheduler_key>:<id>``. The
    # ``scheduler_key`` kwarg encodes "local" / "ssh:<profile>" already.
    target_ref = f"job:{scheduler_key}:{job_id}"

    # Dedup: ``(kind, target_ref)`` is not UNIQUE, so a second attach
    # with the same (job, endpoint, preset) triple must reuse the
    # existing open watch + subscription rather than creating zombies.
    for existing_watch in watch_repo.list_by_target(
        kind="job", target_ref=target_ref, only_open=True
    ):
        if existing_watch.id is None:
            continue
        for existing_sub in sub_repo.list_by_watch(existing_watch.id):
            if (
                existing_sub.endpoint_id == endpoint_id
                and existing_sub.preset == preset
                and existing_sub.id is not None
            ):
                return AttachResult(
                    watch_id=existing_watch.id,
                    subscription_id=existing_sub.id,
                    created=False,
                )

    watch_id = watch_repo.create(kind="job", target_ref=target_ref)
    subscription_id = sub_repo.create(
        watch_id=watch_id,
        endpoint_id=endpoint_id,
        preset=preset,
    )

    # Baseline transition: only insert when no history exists yet so we
    # don't clobber observations from the poller or the workflow runner.
    if transition_repo.latest_for_job(job_id, scheduler_key=scheduler_key) is None:
        transition_repo.insert(
            job_id=job_id,
            from_status=None,
            to_status="PENDING",
            source="webhook",
            scheduler_key=scheduler_key,
        )

    return AttachResult(
        watch_id=watch_id,
        subscription_id=subscription_id,
        created=True,
    )
