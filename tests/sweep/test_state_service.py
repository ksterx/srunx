"""Tests for ``srunx.sweep.state_service.WorkflowRunStateService``."""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from pathlib import Path

import pytest

from srunx.db.connection import open_connection
from srunx.db.migrations import apply_migrations
from srunx.db.repositories.base import now_iso
from srunx.sweep.state_service import WorkflowRunStateService


@pytest.fixture
def conn(tmp_path: Path) -> Iterator[sqlite3.Connection]:
    db = tmp_path / "srunx.db"
    c = open_connection(db)
    apply_migrations(c)
    try:
        yield c
    finally:
        c.close()


def _insert_workflow_run(
    conn: sqlite3.Connection,
    *,
    status: str = "pending",
    sweep_run_id: int | None = None,
) -> int:
    cur = conn.execute(
        """
        INSERT INTO workflow_runs (workflow_name, status, started_at,
                                    triggered_by, sweep_run_id)
        VALUES (?, ?, ?, ?, ?)
        """,
        ("wf", status, now_iso(), "cli", sweep_run_id),
    )
    return int(cur.lastrowid or 0)


def _insert_sweep(conn: sqlite3.Connection, *, cell_count: int) -> int:
    cur = conn.execute(
        """
        INSERT INTO sweep_runs (name, status, matrix, args, fail_fast,
                                max_parallel, cell_count, cells_pending,
                                submission_source, started_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "sweep",
            "pending",
            "{}",
            "{}",
            0,
            2,
            cell_count,
            cell_count,
            "cli",
            now_iso(),
        ),
    )
    return int(cur.lastrowid or 0)


def _count_events(conn: sqlite3.Connection, source_ref: str, kind: str) -> int:
    row = conn.execute(
        "SELECT COUNT(*) FROM events WHERE kind = ? AND source_ref = ?",
        (kind, source_ref),
    ).fetchone()
    return int(row[0])


def test_non_sweep_transition_fires_event(conn: sqlite3.Connection) -> None:
    run_id = _insert_workflow_run(conn, status="pending")

    ok = WorkflowRunStateService.update(
        conn=conn,
        workflow_run_id=run_id,
        from_status="pending",
        to_status="running",
    )
    assert ok is True

    row = conn.execute(
        "SELECT status FROM workflow_runs WHERE id = ?", (run_id,)
    ).fetchone()
    assert row["status"] == "running"
    assert (
        _count_events(conn, f"workflow_run:{run_id}", "workflow_run.status_changed")
        == 1
    )


def test_stale_from_status_returns_false(conn: sqlite3.Connection) -> None:
    run_id = _insert_workflow_run(conn, status="running")
    # Someone else already moved it forward; our from=pending won't match.
    ok = WorkflowRunStateService.update(
        conn=conn,
        workflow_run_id=run_id,
        from_status="pending",
        to_status="running",
    )
    assert ok is False
    # No event emitted when transition did not happen.
    assert (
        _count_events(conn, f"workflow_run:{run_id}", "workflow_run.status_changed")
        == 0
    )


def test_sweep_cell_transition_updates_counters(
    conn: sqlite3.Connection,
) -> None:
    sweep_id = _insert_sweep(conn, cell_count=2)
    run_id = _insert_workflow_run(conn, status="pending", sweep_run_id=sweep_id)

    ok = WorkflowRunStateService.update(
        conn=conn,
        workflow_run_id=run_id,
        from_status="pending",
        to_status="running",
    )
    assert ok is True

    sweep_row = conn.execute(
        "SELECT status, cells_pending, cells_running FROM sweep_runs WHERE id = ?",
        (sweep_id,),
    ).fetchone()
    assert sweep_row["cells_pending"] == 1
    assert sweep_row["cells_running"] == 1
    # First cell entering running → aggregator flips sweep to running.
    assert sweep_row["status"] == "running"
    assert _count_events(conn, f"sweep_run:{sweep_id}", "sweep_run.status_changed") == 1


def test_second_call_with_same_from_is_idempotent(
    conn: sqlite3.Connection,
) -> None:
    run_id = _insert_workflow_run(conn, status="pending")

    assert (
        WorkflowRunStateService.update(
            conn=conn,
            workflow_run_id=run_id,
            from_status="pending",
            to_status="running",
        )
        is True
    )
    assert (
        WorkflowRunStateService.update(
            conn=conn,
            workflow_run_id=run_id,
            from_status="pending",
            to_status="running",
        )
        is False
    )
    # Only one event in DB.
    assert (
        _count_events(conn, f"workflow_run:{run_id}", "workflow_run.status_changed")
        == 1
    )
