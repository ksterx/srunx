"""Tests for :mod:`srunx.observability.notifications.attach`.

Covers the shared core that the CLI bridge
(``cli._helpers.notification_setup``) and the Web router
(``POST /api/watches`` for kind=job) both call. The CLI wrapper
catches every exception and warn-logs it — these tests exercise the
function directly so the error surface (``EndpointNotFoundError`` /
``EndpointDisabledError`` / ``UnsupportedPresetError``) is observable.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from srunx.observability.notifications.attach import (
    AttachResult,
    EndpointDisabledError,
    EndpointNotFoundError,
    InvalidPresetError,
    UnsupportedPresetError,
    attach_job_notification,
)


@pytest.fixture
def isolated_db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path))
    from srunx.observability.storage.connection import init_db

    return init_db(delete_legacy=False)


def _seed(*, endpoint_name: str = "primary", disabled: bool = False) -> tuple[int, int]:
    """Seed an endpoint + a job. Returns (endpoint_id, job_id)."""
    from srunx.observability.storage.connection import open_connection
    from srunx.observability.storage.repositories.endpoints import EndpointRepository
    from srunx.observability.storage.repositories.jobs import JobRepository

    conn = open_connection()
    try:
        endpoint_id = EndpointRepository(conn).create(
            kind="slack_webhook",
            name=endpoint_name,
            config={"webhook_url": "https://hooks.slack.com/services/A/B/C"},
        )
        if disabled:
            EndpointRepository(conn).disable(endpoint_id)

        job_id = 12345
        JobRepository(conn).record_submission(
            job_id=job_id,
            name=f"job_{job_id}",
            status="PENDING",
            submission_source="cli",
        )
        return endpoint_id, job_id
    finally:
        conn.close()


def test_happy_path_creates_watch_subscription_and_baseline(
    isolated_db: Path,
) -> None:
    endpoint_id, job_id = _seed()

    from srunx.observability.storage.connection import open_connection

    conn = open_connection()
    try:
        result = attach_job_notification(
            conn=conn,
            job_id=job_id,
            endpoint_id=endpoint_id,
            preset="terminal",
        )
    finally:
        conn.close()

    assert isinstance(result, AttachResult)
    assert result.created is True
    assert result.watch_id > 0
    assert result.subscription_id > 0

    # Verify side-effects: 1 watch, 1 subscription, 1 baseline transition.
    from srunx.observability.storage.repositories.job_state_transitions import (
        JobStateTransitionRepository,
    )
    from srunx.observability.storage.repositories.subscriptions import (
        SubscriptionRepository,
    )
    from srunx.observability.storage.repositories.watches import WatchRepository

    conn = open_connection()
    try:
        watches = WatchRepository(conn).list_open()
        assert len(watches) == 1
        assert watches[0].target_ref == f"job:local:{job_id}"

        subs = SubscriptionRepository(conn).list_by_watch(result.watch_id)
        assert len(subs) == 1
        assert subs[0].endpoint_id == endpoint_id
        assert subs[0].preset == "terminal"

        latest = JobStateTransitionRepository(conn).latest_for_job(
            job_id, scheduler_key="local"
        )
        assert latest is not None
        assert latest.to_status == "PENDING"
    finally:
        conn.close()


def test_dedup_returns_existing_watch_subscription(isolated_db: Path) -> None:
    endpoint_id, job_id = _seed()

    from srunx.observability.storage.connection import open_connection

    conn = open_connection()
    try:
        first = attach_job_notification(
            conn=conn, job_id=job_id, endpoint_id=endpoint_id, preset="terminal"
        )
        second = attach_job_notification(
            conn=conn, job_id=job_id, endpoint_id=endpoint_id, preset="terminal"
        )
    finally:
        conn.close()

    assert first.created is True
    assert second.created is False
    assert second.watch_id == first.watch_id
    assert second.subscription_id == first.subscription_id

    from srunx.observability.storage.repositories.subscriptions import (
        SubscriptionRepository,
    )
    from srunx.observability.storage.repositories.watches import WatchRepository

    conn = open_connection()
    try:
        assert len(WatchRepository(conn).list_open()) == 1
        assert len(SubscriptionRepository(conn).list_by_watch(first.watch_id)) == 1
    finally:
        conn.close()


def test_dedup_scoped_by_preset(isolated_db: Path) -> None:
    endpoint_id, job_id = _seed()

    from srunx.observability.storage.connection import open_connection

    conn = open_connection()
    try:
        terminal = attach_job_notification(
            conn=conn, job_id=job_id, endpoint_id=endpoint_id, preset="terminal"
        )
        all_preset = attach_job_notification(
            conn=conn, job_id=job_id, endpoint_id=endpoint_id, preset="all"
        )
    finally:
        conn.close()

    # Matches the pre-existing CLI helper behavior: a distinct watch per
    # (job, endpoint, preset) triple. Consolidating onto one watch with
    # multiple subscriptions would be a nice-to-have refactor but is out
    # of scope here.
    assert terminal.subscription_id != all_preset.subscription_id
    assert terminal.watch_id != all_preset.watch_id


def test_missing_endpoint_raises(isolated_db: Path) -> None:
    from srunx.observability.storage.connection import open_connection

    conn = open_connection()
    try:
        with pytest.raises(EndpointNotFoundError):
            attach_job_notification(
                conn=conn, job_id=1, endpoint_id=999, preset="terminal"
            )
    finally:
        conn.close()


def test_disabled_endpoint_raises(isolated_db: Path) -> None:
    endpoint_id, job_id = _seed(disabled=True)

    from srunx.observability.storage.connection import open_connection

    conn = open_connection()
    try:
        with pytest.raises(EndpointDisabledError):
            attach_job_notification(
                conn=conn,
                job_id=job_id,
                endpoint_id=endpoint_id,
                preset="terminal",
            )
    finally:
        conn.close()


def test_bogus_preset_raises_invalid(isolated_db: Path) -> None:
    """A preset not in the schema allowlist gets ``InvalidPresetError``."""
    endpoint_id, job_id = _seed()

    from srunx.observability.storage.connection import open_connection

    conn = open_connection()
    try:
        with pytest.raises(InvalidPresetError):
            attach_job_notification(
                conn=conn,
                job_id=job_id,
                endpoint_id=endpoint_id,
                preset="bogus",
            )
    finally:
        conn.close()


def test_digest_preset_rejected(isolated_db: Path) -> None:
    endpoint_id, job_id = _seed()

    from srunx.observability.storage.connection import open_connection

    conn = open_connection()
    try:
        with pytest.raises(UnsupportedPresetError):
            attach_job_notification(
                conn=conn,
                job_id=job_id,
                endpoint_id=endpoint_id,
                preset="digest",
            )
    finally:
        conn.close()


def test_preserves_existing_transition(isolated_db: Path) -> None:
    """If a transition already exists we must not overwrite its source/status."""
    endpoint_id, job_id = _seed()

    from srunx.observability.storage.connection import open_connection
    from srunx.observability.storage.repositories.job_state_transitions import (
        JobStateTransitionRepository,
    )

    # Seed a RUNNING observation first — the poller / workflow runner
    # would do this on an already-running job.
    conn = open_connection()
    try:
        JobStateTransitionRepository(conn).insert(
            job_id=job_id,
            from_status=None,
            to_status="RUNNING",
            source="poller",
            scheduler_key="local",
        )
    finally:
        conn.close()

    conn = open_connection()
    try:
        attach_job_notification(
            conn=conn, job_id=job_id, endpoint_id=endpoint_id, preset="terminal"
        )
    finally:
        conn.close()

    conn = open_connection()
    try:
        latest = JobStateTransitionRepository(conn).latest_for_job(
            job_id, scheduler_key="local"
        )
        assert latest is not None
        assert latest.to_status == "RUNNING"
        assert latest.source == "poller"
    finally:
        conn.close()


def test_scheduler_key_ssh_writes_three_segment_target_ref(
    isolated_db: Path,
) -> None:
    endpoint_id, _ = _seed()

    # Create a job with scheduler_key="ssh:staging" so latest_for_job
    # lookup resolves the same way the poller would.
    from srunx.observability.storage.connection import open_connection
    from srunx.observability.storage.repositories.jobs import JobRepository
    from srunx.observability.storage.repositories.watches import WatchRepository

    conn = open_connection()
    try:
        JobRepository(conn).record_submission(
            job_id=55,
            name="job_ssh",
            status="PENDING",
            submission_source="cli",
            transport_type="ssh",
            profile_name="staging",
            scheduler_key="ssh:staging",
        )
    finally:
        conn.close()

    conn = open_connection()
    try:
        result = attach_job_notification(
            conn=conn,
            job_id=55,
            endpoint_id=endpoint_id,
            preset="terminal",
            scheduler_key="ssh:staging",
        )
    finally:
        conn.close()

    conn = open_connection()
    try:
        watch = WatchRepository(conn).get(result.watch_id)
        assert watch is not None
        assert watch.target_ref == "job:ssh:staging:55"
    finally:
        conn.close()
