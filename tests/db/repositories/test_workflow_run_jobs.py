"""Tests for :class:`srunx.db.repositories.workflow_run_jobs.WorkflowRunJobRepository`."""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from pathlib import Path

import pytest
from srunx.db.connection import open_connection
from srunx.db.migrations import apply_migrations
from srunx.db.repositories.workflow_run_jobs import WorkflowRunJobRepository
from srunx.db.repositories.workflow_runs import WorkflowRunRepository


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
def run_id(conn: sqlite3.Connection) -> int:
    return WorkflowRunRepository(conn).create("wf", None, None, "cli")


@pytest.fixture
def repo(conn: sqlite3.Connection) -> WorkflowRunJobRepository:
    return WorkflowRunJobRepository(conn)


def test_create_minimal_returns_positive_id(
    repo: WorkflowRunJobRepository, run_id: int
) -> None:
    row_id = repo.create(run_id, "train")
    assert row_id > 0


def test_create_persists_fields(repo: WorkflowRunJobRepository, run_id: int) -> None:
    row_id = repo.create(run_id, "train", depends_on=["preprocess"], job_id=None)
    rows = repo.list_by_run(run_id)
    assert len(rows) == 1
    row = rows[0]
    assert row.id == row_id
    assert row.workflow_run_id == run_id
    assert row.job_name == "train"
    assert row.depends_on == ["preprocess"]
    assert row.job_id is None


def test_update_job_id_associates_slurm_id(
    conn: sqlite3.Connection,
    repo: WorkflowRunJobRepository,
    run_id: int,
) -> None:
    # FK: workflow_run_jobs.job_id references jobs(job_id). Insert a matching row.
    conn.execute(
        "INSERT INTO jobs (job_id, name, status, submitted_at, submission_source) "
        "VALUES (?, ?, ?, ?, ?)",
        (101, "train", "PENDING", "2026-04-18T00:00:00Z", "workflow"),
    )
    conn.commit()

    row_id = repo.create(run_id, "train")
    assert repo.update_job_id(row_id, 101) is True

    rows = repo.list_by_run(run_id)
    assert rows[0].job_id == 101


def test_update_job_id_returns_false_on_missing(
    repo: WorkflowRunJobRepository,
) -> None:
    assert repo.update_job_id(9999, 1) is False


def test_list_by_run_empty(repo: WorkflowRunJobRepository, run_id: int) -> None:
    assert repo.list_by_run(run_id) == []


def test_list_by_run_orders_by_insertion(
    repo: WorkflowRunJobRepository, run_id: int
) -> None:
    a = repo.create(run_id, "a")
    b = repo.create(run_id, "b", depends_on=["a"])
    c = repo.create(run_id, "c", depends_on=["b"])

    rows = repo.list_by_run(run_id)
    assert [r.id for r in rows] == [a, b, c]
    assert [r.job_name for r in rows] == ["a", "b", "c"]


def test_list_by_run_is_run_scoped(
    conn: sqlite3.Connection, repo: WorkflowRunJobRepository, run_id: int
) -> None:
    other_run = WorkflowRunRepository(conn).create("other", None, None, "cli")
    repo.create(run_id, "job_a")
    repo.create(other_run, "job_b")
    assert [r.job_name for r in repo.list_by_run(run_id)] == ["job_a"]
    assert [r.job_name for r in repo.list_by_run(other_run)] == ["job_b"]


def test_cascade_delete_on_workflow_run(
    conn: sqlite3.Connection, repo: WorkflowRunJobRepository, run_id: int
) -> None:
    repo.create(run_id, "a")
    repo.create(run_id, "b")
    assert len(repo.list_by_run(run_id)) == 2

    conn.execute("DELETE FROM workflow_runs WHERE id = ?", (run_id,))
    conn.commit()

    assert repo.list_by_run(run_id) == []
