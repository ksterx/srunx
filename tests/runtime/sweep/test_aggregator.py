"""Tests for ``srunx.runtime.sweep.aggregator.evaluate_and_fire_sweep_status_event``."""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from pathlib import Path

import pytest

from srunx.observability.storage.connection import open_connection
from srunx.observability.storage.migrations import apply_migrations
from srunx.observability.storage.repositories.base import now_iso
from srunx.observability.storage.repositories.events import EventRepository
from srunx.runtime.sweep.aggregator import evaluate_and_fire_sweep_status_event


@pytest.fixture
def conn(tmp_path: Path) -> Iterator[sqlite3.Connection]:
    db = tmp_path / "srunx.observability.storage"
    c = open_connection(db)
    apply_migrations(c)
    try:
        yield c
    finally:
        c.close()


def _create_sweep(
    conn: sqlite3.Connection,
    *,
    cell_count: int,
    status: str = "pending",
    cells_pending: int | None = None,
    cells_running: int = 0,
    cells_completed: int = 0,
    cells_failed: int = 0,
    cells_cancelled: int = 0,
    cancel_requested: bool = False,
) -> int:
    if cells_pending is None:
        cells_pending = cell_count
    cur = conn.execute(
        """
        INSERT INTO sweep_runs (
            name, status, matrix, args,
            fail_fast, max_parallel, cell_count,
            cells_pending, cells_running, cells_completed,
            cells_failed, cells_cancelled,
            submission_source, started_at, cancel_requested_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "sweep_agg_test",
            status,
            '{"lr":[1,2]}',
            "{}",
            0,
            2,
            cell_count,
            cells_pending,
            cells_running,
            cells_completed,
            cells_failed,
            cells_cancelled,
            "cli",
            now_iso(),
            now_iso() if cancel_requested else None,
        ),
    )
    return int(cur.lastrowid or 0)


def _insert_failed_cell(
    conn: sqlite3.Connection,
    sweep_run_id: int,
    *,
    error: str,
    completed_at: str | None = None,
) -> int:
    cur = conn.execute(
        """
        INSERT INTO workflow_runs (
            workflow_name, status, started_at, completed_at, args, error,
            triggered_by, sweep_run_id
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "wf",
            "failed",
            now_iso(),
            completed_at or now_iso(),
            None,
            error,
            "cli",
            sweep_run_id,
        ),
    )
    return int(cur.lastrowid or 0)


def _count_events(conn: sqlite3.Connection, kind: str, source_ref: str) -> int:
    row = conn.execute(
        "SELECT COUNT(*) FROM events WHERE kind = ? AND source_ref = ?",
        (kind, source_ref),
    ).fetchone()
    return int(row[0])


def test_pending_to_running_fires_event(conn: sqlite3.Connection) -> None:
    sweep_id = _create_sweep(
        conn,
        cell_count=3,
        status="pending",
        cells_pending=2,
        cells_running=1,
    )
    evaluate_and_fire_sweep_status_event(conn=conn, sweep_run_id=sweep_id)

    row = conn.execute(
        "SELECT status, completed_at FROM sweep_runs WHERE id = ?",
        (sweep_id,),
    ).fetchone()
    assert row["status"] == "running"
    # Non-terminal → no completed_at stamp.
    assert row["completed_at"] is None

    assert (
        _count_events(
            conn,
            "sweep_run.status_changed",
            f"sweep_run:{sweep_id}",
        )
        == 1
    )


def test_all_cells_completed_fires_completed(conn: sqlite3.Connection) -> None:
    sweep_id = _create_sweep(
        conn,
        cell_count=3,
        status="running",
        cells_pending=0,
        cells_running=0,
        cells_completed=3,
    )
    evaluate_and_fire_sweep_status_event(conn=conn, sweep_run_id=sweep_id)

    row = conn.execute(
        "SELECT status, completed_at FROM sweep_runs WHERE id = ?",
        (sweep_id,),
    ).fetchone()
    assert row["status"] == "completed"
    assert row["completed_at"] is not None

    # Verify payload has the right to_status.
    events = EventRepository(conn).list_recent(limit=10)
    sweep_events = [e for e in events if e.source_ref == f"sweep_run:{sweep_id}"]
    assert len(sweep_events) == 1
    assert sweep_events[0].payload["to_status"] == "completed"
    assert sweep_events[0].payload["from_status"] == "running"


def test_failed_with_representative_error(conn: sqlite3.Connection) -> None:
    sweep_id = _create_sweep(
        conn,
        cell_count=3,
        status="running",
        cells_pending=0,
        cells_running=0,
        cells_completed=2,
        cells_failed=1,
    )
    # Two failed cells — earlier completed_at wins.
    _insert_failed_cell(
        conn, sweep_id, error="first-error", completed_at="2026-01-01T00:00:00.000Z"
    )
    _insert_failed_cell(
        conn, sweep_id, error="later-error", completed_at="2026-01-02T00:00:00.000Z"
    )

    evaluate_and_fire_sweep_status_event(conn=conn, sweep_run_id=sweep_id)

    row = conn.execute(
        "SELECT status FROM sweep_runs WHERE id = ?", (sweep_id,)
    ).fetchone()
    assert row["status"] == "failed"

    events = EventRepository(conn).list_recent(limit=10)
    sweep_events = [e for e in events if e.source_ref == f"sweep_run:{sweep_id}"]
    assert len(sweep_events) == 1
    assert sweep_events[0].payload["to_status"] == "failed"
    assert sweep_events[0].payload["representative_error"] == "first-error"


def test_cancel_precedes_failed(conn: sqlite3.Connection) -> None:
    """cancel_requested + terminal counters with failed still → cancelled."""
    sweep_id = _create_sweep(
        conn,
        cell_count=3,
        status="draining",
        cells_pending=0,
        cells_running=0,
        cells_completed=1,
        cells_failed=1,
        cells_cancelled=1,
        cancel_requested=True,
    )
    evaluate_and_fire_sweep_status_event(conn=conn, sweep_run_id=sweep_id)

    row = conn.execute(
        "SELECT status FROM sweep_runs WHERE id = ?", (sweep_id,)
    ).fetchone()
    assert row["status"] == "cancelled"

    events = EventRepository(conn).list_recent(limit=10)
    sweep_events = [e for e in events if e.source_ref == f"sweep_run:{sweep_id}"]
    assert len(sweep_events) == 1
    assert sweep_events[0].payload["to_status"] == "cancelled"


def test_draining_with_inflight_no_event(conn: sqlite3.Connection) -> None:
    """While cells are still running, draining sweeps do not emit events."""
    sweep_id = _create_sweep(
        conn,
        cell_count=3,
        status="draining",
        cells_pending=0,
        cells_running=1,
        cells_completed=1,
        cells_cancelled=1,
    )
    evaluate_and_fire_sweep_status_event(conn=conn, sweep_run_id=sweep_id)

    row = conn.execute(
        "SELECT status FROM sweep_runs WHERE id = ?", (sweep_id,)
    ).fetchone()
    # No transition — still draining.
    assert row["status"] == "draining"
    assert _count_events(conn, "sweep_run.status_changed", f"sweep_run:{sweep_id}") == 0


def test_idempotent_no_double_event(conn: sqlite3.Connection) -> None:
    sweep_id = _create_sweep(
        conn,
        cell_count=3,
        status="running",
        cells_pending=0,
        cells_running=0,
        cells_completed=3,
    )
    evaluate_and_fire_sweep_status_event(conn=conn, sweep_run_id=sweep_id)
    # Second call is a no-op (target == current after first call).
    evaluate_and_fire_sweep_status_event(conn=conn, sweep_run_id=sweep_id)

    assert _count_events(conn, "sweep_run.status_changed", f"sweep_run:{sweep_id}") == 1


def test_missing_sweep_is_noop(conn: sqlite3.Connection) -> None:
    # Must not raise on unknown id.
    evaluate_and_fire_sweep_status_event(conn=conn, sweep_run_id=99999)


def test_all_cancelled_without_failures(conn: sqlite3.Connection) -> None:
    sweep_id = _create_sweep(
        conn,
        cell_count=3,
        status="draining",
        cells_pending=0,
        cells_running=0,
        cells_completed=1,
        cells_cancelled=2,
    )
    evaluate_and_fire_sweep_status_event(conn=conn, sweep_run_id=sweep_id)

    row = conn.execute(
        "SELECT status FROM sweep_runs WHERE id = ?", (sweep_id,)
    ).fetchone()
    assert row["status"] == "cancelled"


def test_pending_with_no_cells_running_is_noop(
    conn: sqlite3.Connection,
) -> None:
    sweep_id = _create_sweep(
        conn,
        cell_count=3,
        status="pending",
        cells_pending=3,
    )
    evaluate_and_fire_sweep_status_event(conn=conn, sweep_run_id=sweep_id)

    row = conn.execute(
        "SELECT status FROM sweep_runs WHERE id = ?", (sweep_id,)
    ).fetchone()
    assert row["status"] == "pending"
    assert _count_events(conn, "sweep_run.status_changed", f"sweep_run:{sweep_id}") == 0
