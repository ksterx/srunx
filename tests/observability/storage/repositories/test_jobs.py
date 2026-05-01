"""Tests for :class:`srunx.observability.storage.repositories.jobs.JobRepository`."""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from pathlib import Path

import pytest

from srunx.observability.storage.connection import open_connection
from srunx.observability.storage.migrations import apply_migrations
from srunx.observability.storage.repositories.jobs import JobRepository


@pytest.fixture
def conn(tmp_path: Path) -> Iterator[sqlite3.Connection]:
    connection = open_connection(tmp_path / "t.db")
    apply_migrations(connection)
    try:
        yield connection
    finally:
        connection.close()


@pytest.fixture
def repo(conn: sqlite3.Connection) -> JobRepository:
    return JobRepository(conn)


def test_record_submission_roundtrip(repo: JobRepository) -> None:
    row_id = repo.record_submission(
        job_id=101,
        name="train",
        status="PENDING",
        submission_source="cli",
        command=["python", "train.py"],
        nodes=2,
        gpus_per_node=4,
        memory_per_node="64GB",
        time_limit="2:00:00",
        partition="gpu",
        conda="ml",
        env_vars={"CUDA_VISIBLE_DEVICES": "0,1"},
        metadata={"run_id": "abc"},
    )
    assert row_id > 0

    job = repo.get(101, scheduler_key="local")
    assert job is not None
    assert job.job_id == 101
    assert job.name == "train"
    assert job.status == "PENDING"
    assert job.submission_source == "cli"
    assert job.command == ["python", "train.py"]
    assert job.nodes == 2
    assert job.gpus_per_node == 4
    assert job.env_vars == {"CUDA_VISIBLE_DEVICES": "0,1"}
    assert job.metadata == {"run_id": "abc"}


def test_record_submission_duplicate_job_id_is_noop(
    repo: JobRepository,
) -> None:
    """P1-2: duplicate record_submission must NOT overwrite.

    ``INSERT OR REPLACE`` would ``DELETE`` the existing row (cascading
    ``ON DELETE SET NULL`` to ``workflow_run_jobs.job_id`` and
    ``job_state_transitions.job_id``) before inserting the new one —
    silently orphaning every prior transition and membership. With
    ``INSERT OR IGNORE`` the first call wins, subsequent callers get
    ``lastrowid=0`` and can decide explicitly what to do.
    """
    first_row_id = repo.record_submission(
        job_id=202, name="first", status="PENDING", submission_source="web"
    )
    assert first_row_id > 0

    second_row_id = repo.record_submission(
        job_id=202,
        name="second",
        status="RUNNING",
        submission_source="workflow",
    )
    assert second_row_id == 0  # INSERT OR IGNORE → no row inserted

    job = repo.get(202, scheduler_key="local")
    assert job is not None
    # Original fields preserved — not clobbered to "second" / RUNNING.
    assert job.name == "first"
    assert job.status == "PENDING"
    assert job.submission_source == "web"


def test_record_submission_ignore_preserves_fk_references(
    repo: JobRepository, conn: sqlite3.Connection
) -> None:
    """P1-2: ensure the FK references survive a duplicate call.

    ``INSERT OR REPLACE`` would have cascaded a ``SET NULL`` through
    the ``job_state_transitions.jobs_row_id`` FK; ``INSERT OR IGNORE``
    preserves.
    """
    repo.record_submission(
        job_id=210, name="fk-test", status="PENDING", submission_source="web"
    )
    # Resolve the jobs.id (AUTOINCREMENT PK) so the transition points
    # at the correct V5 FK target.
    jobs_row_id = conn.execute(
        "SELECT id FROM jobs WHERE scheduler_key = 'local' AND job_id = ?",
        (210,),
    ).fetchone()[0]
    # Simulate a prior transition observed by the poller.
    conn.execute(
        "INSERT INTO job_state_transitions "
        "(jobs_row_id, to_status, observed_at, source) "
        "VALUES (?, 'RUNNING', '2026-04-19T00:00:00Z', 'poller')",
        (jobs_row_id,),
    )
    conn.commit()

    # A second (spurious) record_submission for the same job_id.
    repo.record_submission(
        job_id=210,
        name="fk-test",
        status="PENDING",
        submission_source="web",
    )

    # The prior transition still references the jobs row — not SET NULL.
    rows = conn.execute(
        "SELECT jobs_row_id FROM job_state_transitions WHERE to_status='RUNNING'"
    ).fetchall()
    assert [r[0] for r in rows] == [jobs_row_id]


def test_update_status_fills_optional_fields(repo: JobRepository) -> None:
    repo.record_submission(
        job_id=303, name="j", status="PENDING", submission_source="web"
    )
    updated = repo.update_status(
        303,
        "RUNNING",
        scheduler_key="local",
        started_at="2026-04-19T01:00:00.000Z",
        nodelist="node01",
    )
    assert updated is True
    job = repo.get(303, scheduler_key="local")
    assert job is not None
    assert job.status == "RUNNING"
    assert job.nodelist == "node01"
    assert job.started_at is not None


def test_update_status_returns_false_for_missing(repo: JobRepository) -> None:
    assert repo.update_status(99999, "RUNNING", scheduler_key="local") is False


def test_update_completion_computes_duration(repo: JobRepository) -> None:
    repo.record_submission(
        job_id=404,
        name="t",
        status="PENDING",
        submission_source="cli",
        submitted_at="2026-04-19T10:00:00.000Z",
    )
    ok = repo.update_completion(
        404,
        "COMPLETED",
        completed_at="2026-04-19T11:00:00.000Z",
        scheduler_key="local",
    )
    assert ok is True
    job = repo.get(404, scheduler_key="local")
    assert job is not None
    assert job.status == "COMPLETED"
    assert job.duration_secs == 3600


def test_update_completion_defaults_completed_at_to_now(
    repo: JobRepository,
) -> None:
    repo.record_submission(
        job_id=405, name="t", status="PENDING", submission_source="cli"
    )
    assert repo.update_completion(405, "COMPLETED", scheduler_key="local") is True
    job = repo.get(405, scheduler_key="local")
    assert job is not None and job.completed_at is not None


def test_list_all_orders_and_filters(repo: JobRepository) -> None:
    # Seed a workflow_run so the FK on jobs.workflow_run_id can point to it.
    repo.conn.execute(
        "INSERT INTO workflow_runs (id, workflow_name, status, started_at, triggered_by) "
        "VALUES (42, 'wf', 'running', '2026-04-19T00:00:00.000Z', 'cli')"
    )
    for jid in (501, 502, 503):
        repo.record_submission(
            job_id=jid,
            name=f"j{jid}",
            status="PENDING",
            submission_source="cli",
            submitted_at=f"2026-04-19T0{jid - 500}:00:00.000Z",
        )
    repo.record_submission(
        job_id=504,
        name="w",
        status="PENDING",
        submission_source="workflow",
        workflow_run_id=42,
        submitted_at="2026-04-19T04:00:00.000Z",
    )

    all_rows = repo.list_all(limit=10)
    ids = [j.job_id for j in all_rows]
    assert ids == [504, 503, 502, 501]

    wf_rows = repo.list_all(workflow_run_id=42)
    assert [j.job_id for j in wf_rows] == [504]


def test_list_all_respects_limit_and_offset(repo: JobRepository) -> None:
    for jid in range(601, 606):
        repo.record_submission(
            job_id=jid,
            name=f"j{jid}",
            status="PENDING",
            submission_source="cli",
            submitted_at=f"2026-04-19T{jid - 601:02d}:00:00.000Z",
        )
    page1 = repo.list_all(limit=2)
    page2 = repo.list_all(limit=2, offset=2)
    assert {j.job_id for j in page1} != {j.job_id for j in page2}
    assert len(page1) == 2 and len(page2) == 2


def test_count_by_status_in_range(repo: JobRepository) -> None:
    for jid, status in [(701, "COMPLETED"), (702, "COMPLETED"), (703, "FAILED")]:
        repo.record_submission(
            job_id=jid,
            name=f"j{jid}",
            status=status,
            submission_source="cli",
            submitted_at="2026-04-19T12:00:00.000Z",
        )
    repo.record_submission(
        job_id=704,
        name="j704",
        status="COMPLETED",
        submission_source="cli",
        submitted_at="2025-12-31T12:00:00.000Z",
    )

    counts = repo.count_by_status_in_range(
        "2026-04-19T00:00:00Z", "2026-04-20T00:00:00Z"
    )
    assert counts == {"COMPLETED": 2, "FAILED": 1}


def test_count_by_status_in_range_filters_statuses(repo: JobRepository) -> None:
    repo.record_submission(
        job_id=801,
        name="a",
        status="COMPLETED",
        submission_source="cli",
        submitted_at="2026-04-19T00:00:00Z",
    )
    repo.record_submission(
        job_id=802,
        name="b",
        status="FAILED",
        submission_source="cli",
        submitted_at="2026-04-19T00:00:00Z",
    )
    counts = repo.count_by_status_in_range(
        "2026-04-19T00:00:00Z",
        "2026-04-20T00:00:00Z",
        statuses=["COMPLETED"],
    )
    assert counts == {"COMPLETED": 1}


def test_count_by_status_in_range_uses_timestamp_field(repo: JobRepository) -> None:
    """``timestamp_field='completed_at'`` counts by terminal ts, not submit.

    Exercises the P2-6 follow-up: the scheduler windows by when jobs
    *finished*, so a row submitted before the window but completed inside
    it must count, and vice versa.
    """
    # Submitted long ago, completed inside the window → counted under
    # completed_at, missed under submitted_at.
    repo.record_submission(
        job_id=901,
        name="long_running",
        status="COMPLETED",
        submission_source="cli",
        submitted_at="2026-01-01T00:00:00Z",
    )
    repo.update_status(
        901,
        "COMPLETED",
        scheduler_key="local",
        completed_at="2026-04-19T12:00:00Z",
        duration_secs=1,
    )
    # Submitted inside the window, not yet completed → excluded under
    # completed_at because ``completed_at >= ?`` is false when it's NULL.
    repo.record_submission(
        job_id=902,
        name="still_running",
        status="RUNNING",
        submission_source="cli",
        submitted_at="2026-04-19T11:00:00Z",
    )

    by_completed = repo.count_by_status_in_range(
        "2026-04-19T00:00:00Z",
        "2026-04-20T00:00:00Z",
        statuses=["COMPLETED"],
        timestamp_field="completed_at",
    )
    assert by_completed == {"COMPLETED": 1}

    by_submitted = repo.count_by_status_in_range(
        "2026-04-19T00:00:00Z",
        "2026-04-20T00:00:00Z",
        statuses=["RUNNING"],
        timestamp_field="submitted_at",
    )
    assert by_submitted == {"RUNNING": 1}


def test_count_by_status_in_range_rejects_bad_field(repo: JobRepository) -> None:
    """Column names are interpolated, so only whitelisted values are allowed."""
    import pytest

    with pytest.raises(ValueError, match="timestamp_field"):
        repo.count_by_status_in_range(
            "2026-04-19T00:00:00Z",
            "2026-04-20T00:00:00Z",
            timestamp_field="status; DROP TABLE jobs;--",  # noqa: S608 — negative test
        )


def test_delete_existing_returns_true(repo: JobRepository) -> None:
    repo.record_submission(
        job_id=901, name="t", status="PENDING", submission_source="cli"
    )
    assert repo.delete(901, scheduler_key="local") is True
    assert repo.get(901, scheduler_key="local") is None


def test_delete_missing_returns_false(repo: JobRepository) -> None:
    assert repo.delete(99999, scheduler_key="local") is False
