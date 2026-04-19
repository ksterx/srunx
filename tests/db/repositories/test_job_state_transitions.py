"""Tests for :class:`srunx.db.repositories.job_state_transitions.JobStateTransitionRepository`."""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from datetime import datetime
from pathlib import Path

import pytest

from srunx.db.connection import open_connection
from srunx.db.migrations import apply_migrations
from srunx.db.repositories.job_state_transitions import (
    JobStateTransitionRepository,
)


@pytest.fixture
def conn(tmp_path: Path) -> Iterator[sqlite3.Connection]:
    db = tmp_path / "srunx.db"
    connection = open_connection(db)
    apply_migrations(connection)
    try:
        yield connection
    finally:
        connection.close()


@pytest.fixture
def repo(conn: sqlite3.Connection) -> JobStateTransitionRepository:
    return JobStateTransitionRepository(conn)


def _seed_job(conn: sqlite3.Connection, job_id: int) -> None:
    """Insert a minimal jobs row so the FK on ``job_state_transitions.job_id`` holds."""
    conn.execute(
        "INSERT INTO jobs (job_id, name, status, submitted_at, submission_source) "
        "VALUES (?, ?, ?, ?, ?)",
        (job_id, f"job_{job_id}", "PENDING", "2026-04-18T00:00:00Z", "cli"),
    )
    conn.commit()


def test_insert_returns_positive_id(
    conn: sqlite3.Connection, repo: JobStateTransitionRepository
) -> None:
    _seed_job(conn, 1)
    row_id = repo.insert(1, None, "PENDING", "poller")
    assert row_id > 0


def test_insert_default_observed_at_is_parseable(
    conn: sqlite3.Connection, repo: JobStateTransitionRepository
) -> None:
    _seed_job(conn, 1)
    repo.insert(1, None, "PENDING", "poller")
    latest = repo.latest_for_job(1)
    assert latest is not None
    assert isinstance(latest.observed_at, datetime)


def test_insert_with_explicit_observed_at(
    conn: sqlite3.Connection, repo: JobStateTransitionRepository
) -> None:
    _seed_job(conn, 1)
    repo.insert(
        1, "PENDING", "RUNNING", "cli_monitor", observed_at="2026-04-18T01:02:03.000Z"
    )
    latest = repo.latest_for_job(1)
    assert latest is not None
    assert latest.from_status == "PENDING"
    assert latest.to_status == "RUNNING"
    assert latest.source == "cli_monitor"
    assert isinstance(latest.observed_at, datetime)
    assert latest.observed_at.year == 2026
    assert latest.observed_at.hour == 1


def test_latest_for_job_missing_returns_none(
    repo: JobStateTransitionRepository,
) -> None:
    assert repo.latest_for_job(9999) is None


def test_latest_for_job_picks_most_recent(
    conn: sqlite3.Connection, repo: JobStateTransitionRepository
) -> None:
    _seed_job(conn, 1)
    repo.insert(1, None, "PENDING", "poller", observed_at="2026-04-18T00:00:00Z")
    repo.insert(1, "PENDING", "RUNNING", "poller", observed_at="2026-04-18T00:05:00Z")
    repo.insert(1, "RUNNING", "COMPLETED", "poller", observed_at="2026-04-18T00:10:00Z")
    latest = repo.latest_for_job(1)
    assert latest is not None
    assert latest.to_status == "COMPLETED"


def test_history_for_job_empty(repo: JobStateTransitionRepository) -> None:
    assert repo.history_for_job(9999) == []


def test_history_for_job_is_chronological(
    conn: sqlite3.Connection, repo: JobStateTransitionRepository
) -> None:
    _seed_job(conn, 1)
    # Insert out of chronological order — repo must still return ASC by observed_at.
    repo.insert(1, "RUNNING", "COMPLETED", "poller", observed_at="2026-04-18T00:10:00Z")
    repo.insert(1, None, "PENDING", "poller", observed_at="2026-04-18T00:00:00Z")
    repo.insert(1, "PENDING", "RUNNING", "poller", observed_at="2026-04-18T00:05:00Z")

    history = repo.history_for_job(1)
    assert [t.to_status for t in history] == ["PENDING", "RUNNING", "COMPLETED"]
    # Ensure datetime conversion happened for every row.
    assert all(isinstance(t.observed_at, datetime) for t in history)


def test_history_scoped_by_job(
    conn: sqlite3.Connection, repo: JobStateTransitionRepository
) -> None:
    _seed_job(conn, 1)
    _seed_job(conn, 2)
    repo.insert(1, None, "PENDING", "poller")
    repo.insert(2, None, "RUNNING", "webhook")
    assert [t.to_status for t in repo.history_for_job(1)] == ["PENDING"]
    assert [t.to_status for t in repo.history_for_job(2)] == ["RUNNING"]


def test_job_delete_sets_job_id_null(
    conn: sqlite3.Connection, repo: JobStateTransitionRepository
) -> None:
    _seed_job(conn, 1)
    repo.insert(1, None, "PENDING", "poller")
    # FK is ON DELETE SET NULL; transition row survives but job_id goes NULL.
    conn.execute("DELETE FROM jobs WHERE job_id = 1")
    conn.commit()

    assert repo.latest_for_job(1) is None
    orphan = conn.execute(
        "SELECT job_id, to_status FROM job_state_transitions"
    ).fetchone()
    assert orphan["job_id"] is None
    assert orphan["to_status"] == "PENDING"
