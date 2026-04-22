"""Tests for :class:`~srunx.monitor.job_monitor.JobMonitor` SSOT writes.

Covers task D/47-48 of the notification-and-state-persistence plan: the
CLI monitor, when given a ``JobStateTransitionRepository``, must also
persist observed state transitions to the ``job_state_transitions``
table with ``source='cli_monitor'``, while keeping its existing callback
behavior unchanged.

Backward-compat invariant: when ``transition_repo`` is ``None`` (the
default), no DB writes happen and no DB-related imports are required on
the call path.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from unittest.mock import MagicMock

import pytest

from srunx.db.repositories.job_state_transitions import (
    JobStateTransitionRepository,
)
from srunx.models import Job, JobStatus
from srunx.monitor.job_monitor import JobMonitor

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def db_conn(tmp_srunx_db: tuple[sqlite3.Connection, object]) -> sqlite3.Connection:
    """Unpack the shared ``tmp_srunx_db`` fixture to just the connection."""
    conn, _path = tmp_srunx_db
    return conn


@pytest.fixture
def repo(db_conn: sqlite3.Connection) -> JobStateTransitionRepository:
    return JobStateTransitionRepository(db_conn)


def _seed_job(conn: sqlite3.Connection, job_id: int) -> None:
    """Insert a minimal ``jobs`` row so the FK on transitions holds.

    The ``job_state_transitions.job_id`` column has a foreign key
    reference to ``jobs.job_id``; without this row, ``repo.insert``
    would fail with an FK constraint violation.
    """
    conn.execute(
        "INSERT INTO jobs (job_id, name, status, submitted_at, submission_source) "
        "VALUES (?, ?, ?, ?, ?)",
        (job_id, f"job_{job_id}", "PENDING", "2026-04-18T00:00:00Z", "cli"),
    )
    conn.commit()


def _make_job(job_id: int, status: JobStatus) -> Job:
    job = Job(name=f"job_{job_id}", job_id=job_id, command=["test"])
    job._status = status
    return job


def _drive_states(
    monitor: JobMonitor, client: MagicMock, states: Iterator[JobStatus]
) -> None:
    """Drive ``monitor._notify_callbacks`` through each status in order.

    Simulates what ``watch_continuous`` does in its inner loop: fetch
    current state, compare with previous, notify on change.  Each
    iteration clears the per-cycle cache so the mock returns the next
    status.
    """
    for status in states:
        monitor._cached_jobs = None
        client.retrieve = MagicMock(
            side_effect=lambda _jid, _s=status: _make_job(_jid, _s)
        )
        monitor._notify_callbacks("state_changed")


# ---------------------------------------------------------------------------
# With repo: transitions written with source='cli_monitor'
# ---------------------------------------------------------------------------


def test_transitions_recorded_with_cli_monitor_source(
    db_conn: sqlite3.Connection, repo: JobStateTransitionRepository
) -> None:
    """PENDING -> RUNNING -> COMPLETED produces 2 transition rows."""
    job_id = 4242
    _seed_job(db_conn, job_id)

    client = MagicMock()
    monitor = JobMonitor(job_ids=[job_id], client=client, transition_repo=repo)

    _drive_states(
        monitor,
        client,
        iter([JobStatus.PENDING, JobStatus.RUNNING, JobStatus.COMPLETED]),
    )

    history = repo.history_for_job(job_id, scheduler_key="local")
    # First observation (previous=None) is NOT recorded; only the real
    # transitions are persisted.
    assert [(t.from_status, t.to_status) for t in history] == [
        ("PENDING", "RUNNING"),
        ("RUNNING", "COMPLETED"),
    ]
    assert all(t.source == "cli_monitor" for t in history)


def test_first_observation_is_not_recorded(
    db_conn: sqlite3.Connection, repo: JobStateTransitionRepository
) -> None:
    """Observing a single state produces zero DB rows."""
    job_id = 4243
    _seed_job(db_conn, job_id)

    client = MagicMock()
    monitor = JobMonitor(job_ids=[job_id], client=client, transition_repo=repo)

    _drive_states(monitor, client, iter([JobStatus.PENDING]))

    assert repo.history_for_job(job_id, scheduler_key="local") == []


def test_duplicate_status_does_not_produce_extra_rows(
    db_conn: sqlite3.Connection, repo: JobStateTransitionRepository
) -> None:
    """Seeing the same status twice in a row inserts only the real transitions."""
    job_id = 4244
    _seed_job(db_conn, job_id)

    client = MagicMock()
    monitor = JobMonitor(job_ids=[job_id], client=client, transition_repo=repo)

    _drive_states(
        monitor,
        client,
        iter(
            [
                JobStatus.PENDING,
                JobStatus.PENDING,  # unchanged — no write
                JobStatus.RUNNING,
                JobStatus.RUNNING,  # unchanged — no write
                JobStatus.COMPLETED,
            ]
        ),
    )

    history = repo.history_for_job(job_id, scheduler_key="local")
    assert [(t.from_status, t.to_status) for t in history] == [
        ("PENDING", "RUNNING"),
        ("RUNNING", "COMPLETED"),
    ]


def test_multiple_jobs_tracked_independently(
    db_conn: sqlite3.Connection, repo: JobStateTransitionRepository
) -> None:
    """Each job's transitions are scoped to its own job_id."""
    _seed_job(db_conn, 100)
    _seed_job(db_conn, 200)

    client = MagicMock()
    monitor = JobMonitor(job_ids=[100, 200], client=client, transition_repo=repo)

    # Per-cycle statuses: job 100 goes PENDING->RUNNING->COMPLETED,
    # job 200 stays PENDING then jumps straight to FAILED.
    cycle_states: list[dict[int, JobStatus]] = [
        {100: JobStatus.PENDING, 200: JobStatus.PENDING},
        {100: JobStatus.RUNNING, 200: JobStatus.PENDING},
        {100: JobStatus.COMPLETED, 200: JobStatus.FAILED},
    ]

    for cycle in cycle_states:
        monitor._cached_jobs = None
        client.retrieve = MagicMock(
            side_effect=lambda jid, _c=cycle: _make_job(jid, _c[jid])
        )
        monitor._notify_callbacks("state_changed")

    hist_100 = repo.history_for_job(100, scheduler_key="local")
    hist_200 = repo.history_for_job(200, scheduler_key="local")

    assert [(t.from_status, t.to_status) for t in hist_100] == [
        ("PENDING", "RUNNING"),
        ("RUNNING", "COMPLETED"),
    ]
    assert [(t.from_status, t.to_status) for t in hist_200] == [
        ("PENDING", "FAILED"),
    ]
    assert all(t.source == "cli_monitor" for t in hist_100 + hist_200)


# ---------------------------------------------------------------------------
# Without repo: backward compatibility
# ---------------------------------------------------------------------------


def test_no_repo_injected_no_writes_no_errors(
    db_conn: sqlite3.Connection, repo: JobStateTransitionRepository
) -> None:
    """Default ``transition_repo=None`` is safe; only the terminal transition is mirrored.

    Without explicit ``transition_repo`` injection, intermediate
    PENDING / RUNNING transitions are not recorded. The terminal
    state, however, is mirrored into the SSOT via
    :class:`srunx.history.JobHistory.update_job_completion` — see the
    dual-write migration in :mod:`srunx.history`.
    """
    job_id = 5555
    _seed_job(db_conn, job_id)

    client = MagicMock()
    # Deliberately omit ``transition_repo`` — default is None.
    monitor = JobMonitor(job_ids=[job_id], client=client)

    # Should not raise even with callbacks absent and real transitions.
    _drive_states(
        monitor,
        client,
        iter([JobStatus.PENDING, JobStatus.RUNNING, JobStatus.COMPLETED]),
    )

    # Only the terminal transition is mirrored by the history dual-write.
    history = repo.history_for_job(job_id, scheduler_key="local")
    assert [(t.from_status, t.to_status) for t in history] == [
        (None, "COMPLETED"),
    ]
    assert all(t.source == "cli_monitor" for t in history)


# ---------------------------------------------------------------------------
# DB failures are swallowed
# ---------------------------------------------------------------------------


def test_repo_insert_failure_is_logged_and_monitor_continues() -> None:
    """When ``repo.insert`` raises, the monitor logs a warning and keeps going.

    The callback path must still fire for every real transition, proving
    that DB failures do not break the existing CLI behavior.
    """
    failing_repo = MagicMock(spec=JobStateTransitionRepository)
    failing_repo.insert.side_effect = RuntimeError("simulated DB outage")

    callback = MagicMock()
    client = MagicMock()
    monitor = JobMonitor(
        job_ids=[777],
        client=client,
        callbacks=[callback],
        transition_repo=failing_repo,
    )

    # PENDING -> RUNNING -> COMPLETED: two real transitions.
    _drive_states(
        monitor,
        client,
        iter([JobStatus.PENDING, JobStatus.RUNNING, JobStatus.COMPLETED]),
    )

    # Both inserts were attempted (failure did not short-circuit the loop).
    assert failing_repo.insert.call_count == 2
    # Callbacks still fired for each real transition.
    assert callback.on_job_running.call_count == 1
    assert callback.on_job_completed.call_count == 1


def test_repo_insert_called_with_expected_arguments() -> None:
    """Verify the exact signature used for ``repo.insert``."""
    spy_repo = MagicMock(spec=JobStateTransitionRepository)
    spy_repo.insert.return_value = 1

    client = MagicMock()
    monitor = JobMonitor(job_ids=[321], client=client, transition_repo=spy_repo)

    _drive_states(monitor, client, iter([JobStatus.PENDING, JobStatus.RUNNING]))

    spy_repo.insert.assert_called_once_with(
        job_id=321,
        from_status="PENDING",
        to_status="RUNNING",
        source="cli_monitor",
        scheduler_key="local",
    )
