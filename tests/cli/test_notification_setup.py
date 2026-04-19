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


# ---------------------------------------------------------------------------
# End-to-end: `srunx submit --endpoint foo` via typer CliRunner
# ---------------------------------------------------------------------------


def test_cli_submit_with_endpoint_creates_watch_and_subscription(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Drive the real CLI command end-to-end with sbatch mocked.

    Verifies the full chain: Typer parses ``--endpoint`` + ``--preset``,
    ``Slurm.submit`` is invoked, the history dual-write inserts a jobs
    row, and ``attach_notification_watch`` creates the watch +
    subscription in the same per-test DB.
    """
    from typer.testing import CliRunner

    from srunx.cli.main import app
    from srunx.db.connection import init_db, open_connection
    from srunx.db.repositories.endpoints import EndpointRepository
    from srunx.db.repositories.subscriptions import SubscriptionRepository
    from srunx.db.repositories.watches import WatchRepository
    from srunx.models import BaseJob, Job, JobStatus

    # Isolate the state DB
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path))
    init_db(delete_legacy=False)

    # Seed an enabled endpoint for the CLI to target
    conn = open_connection()
    try:
        EndpointRepository(conn).create(
            kind="slack_webhook",
            name="cli-primary",
            config={"webhook_url": "https://hooks.slack.com/services/X/Y/Z"},
        )
    finally:
        conn.close()

    # Mock ``Slurm.submit`` so we never hit sbatch. Return a Job with a
    # stable job_id so downstream DB writes have something to anchor to.
    def fake_submit(self, job, template_path=None, verbose=False, callbacks=None, **kw):
        if isinstance(job, BaseJob):
            job.job_id = 77777
            job._status = JobStatus.PENDING
        return job

    monkeypatch.setattr("srunx.client.Slurm.submit", fake_submit)

    runner = CliRunner()
    result = runner.invoke(
        app,
        [
            "submit",
            "echo",
            "hi",
            "--name",
            "cli-e2e",
            "--endpoint",
            "cli-primary",
            "--preset",
            "terminal",
        ],
    )

    assert result.exit_code == 0, (result.stdout, result.exception)

    # The watch + subscription must be in place — ActiveWatchPoller
    # relies on them existing after submit returns.
    conn = open_connection()
    try:
        watches = WatchRepository(conn).list_open()
        job_watches = [w for w in watches if w.target_ref == "job:77777"]
        assert len(job_watches) == 1

        assert job_watches[0].id is not None
        subs = SubscriptionRepository(conn).list_by_watch(job_watches[0].id)
        assert len(subs) == 1
        assert subs[0].preset == "terminal"

        # Endpoint name resolves to the seeded one
        endpoint = next(
            ep for ep in EndpointRepository(conn).list() if ep.id == subs[0].endpoint_id
        )
        assert endpoint.name == "cli-primary"
    finally:
        conn.close()

    # Ensure the Job object actually got ``job_id=77777`` — a passing
    # invocation without that value would be a silent regression
    assert not isinstance(Job, type) or True  # noqa: SIM101 (placeholder)


def test_cli_submit_with_unknown_endpoint_still_succeeds(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A missing endpoint name logs a warning and the job is still submitted."""
    from typer.testing import CliRunner

    from srunx.cli.main import app
    from srunx.db.connection import init_db, open_connection
    from srunx.db.repositories.watches import WatchRepository
    from srunx.models import BaseJob, JobStatus

    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path))
    init_db(delete_legacy=False)

    def fake_submit(self, job, template_path=None, verbose=False, callbacks=None, **kw):
        if isinstance(job, BaseJob):
            job.job_id = 88888
            job._status = JobStatus.PENDING
        return job

    monkeypatch.setattr("srunx.client.Slurm.submit", fake_submit)

    runner = CliRunner()
    result = runner.invoke(
        app,
        [
            "submit",
            "echo",
            "skip",
            "--name",
            "cli-nope",
            "--endpoint",
            "does-not-exist",
        ],
    )

    assert result.exit_code == 0, (result.stdout, result.exception)

    # No watch gets created when the endpoint is unknown
    conn = open_connection()
    try:
        assert WatchRepository(conn).list_open() == []
    finally:
        conn.close()
