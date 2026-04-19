"""Tests for :mod:`srunx.cli.notification_setup`.

Covers the CLI notification bridge: after ``srunx submit`` creates a
SLURM job + records it to the state DB, this helper wires up the
endpoint / watch / subscription triplet so the active-watch poller
picks the job up.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from srunx.cli.notification_setup import (
    attach_notification_watch,
    resolve_endpoint_name,
)


@pytest.fixture
def isolated_db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Isolate XDG_CONFIG_HOME so the helper writes into a tmp DB."""
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path))
    from srunx.db.connection import init_db

    return init_db(delete_legacy=False)


def _seed_endpoint_and_job(
    *,
    endpoint_name: str,
    disabled: bool = False,
) -> tuple[int, int]:
    """Create an endpoint row + a corresponding job row. Returns (endpoint_id, job_id)."""
    from srunx.db.connection import open_connection
    from srunx.db.repositories.endpoints import EndpointRepository
    from srunx.db.repositories.jobs import JobRepository

    conn = open_connection()
    try:
        endpoint_id = EndpointRepository(conn).create(
            kind="slack_webhook",
            name=endpoint_name,
            config={"webhook_url": "https://hooks.slack.com/services/A/B/C"},
        )
        if disabled:
            EndpointRepository(conn).disable(endpoint_id)

        job_id = 9001
        JobRepository(conn).record_submission(
            job_id=job_id,
            name=f"job_{job_id}",
            status="PENDING",
            submission_source="cli",
        )
        return endpoint_id, job_id
    finally:
        conn.close()


def test_resolve_endpoint_name_explicit_wins() -> None:
    assert resolve_endpoint_name("cli", "config") == "cli"
    assert resolve_endpoint_name(None, "config") == "config"
    assert resolve_endpoint_name(None, None) is None


def test_attach_watch_happy_path(isolated_db: Path) -> None:
    endpoint_id, job_id = _seed_endpoint_and_job(endpoint_name="primary")

    subscription_id = attach_notification_watch(
        job_id=job_id, endpoint_name="primary", preset="terminal"
    )

    assert subscription_id is not None

    from srunx.db.connection import open_connection
    from srunx.db.repositories.job_state_transitions import (
        JobStateTransitionRepository,
    )
    from srunx.db.repositories.subscriptions import SubscriptionRepository
    from srunx.db.repositories.watches import WatchRepository

    conn = open_connection()
    try:
        subs = SubscriptionRepository(conn).list_by_watch(
            WatchRepository(conn).list_open()[0].id  # type: ignore[arg-type]
        )
        assert len(subs) == 1
        assert subs[0].endpoint_id == endpoint_id
        assert subs[0].preset == "terminal"

        # A PENDING transition should be present so the poller's first
        # observation produces a real state change.
        latest = JobStateTransitionRepository(conn).latest_for_job(job_id)
        assert latest is not None
        assert latest.to_status == "PENDING"
    finally:
        conn.close()


def test_attach_watch_missing_endpoint_is_noop(isolated_db: Path) -> None:
    """Unknown endpoint name logs a warning and returns None."""
    # Seed only the job — no endpoint with the requested name.
    from srunx.db.connection import open_connection
    from srunx.db.repositories.jobs import JobRepository

    conn = open_connection()
    try:
        JobRepository(conn).record_submission(
            job_id=42,
            name="job_42",
            status="PENDING",
            submission_source="cli",
        )
    finally:
        conn.close()

    result = attach_notification_watch(
        job_id=42, endpoint_name="nope", preset="terminal"
    )
    assert result is None


def test_attach_watch_disabled_endpoint_skipped(isolated_db: Path) -> None:
    """Disabled endpoints are refused with a warning, not an exception."""
    _seed_endpoint_and_job(endpoint_name="off", disabled=True)

    result = attach_notification_watch(
        job_id=9001, endpoint_name="off", preset="terminal"
    )
    assert result is None
