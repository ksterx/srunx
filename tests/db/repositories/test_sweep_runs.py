"""Tests for :class:`srunx.db.repositories.sweep_runs.SweepRunRepository`."""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from datetime import datetime
from pathlib import Path

import pytest
from srunx.db.connection import open_connection
from srunx.db.migrations import apply_migrations
from srunx.db.repositories.base import now_iso
from srunx.db.repositories.sweep_runs import SweepRunRepository
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
def sweep_repo(conn: sqlite3.Connection) -> SweepRunRepository:
    return SweepRunRepository(conn)


@pytest.fixture
def wf_repo(conn: sqlite3.Connection) -> WorkflowRunRepository:
    return WorkflowRunRepository(conn)


def _create_sweep(
    sweep_repo: SweepRunRepository,
    *,
    name: str = "sweep_test",
    cell_count: int = 3,
    matrix: dict | None = None,
    fail_fast: bool = False,
    max_parallel: int = 2,
) -> int:
    return sweep_repo.create(
        name=name,
        matrix=matrix or {"lr": [0.001, 0.01, 0.1]},
        args={"dataset": "cifar10"},
        fail_fast=fail_fast,
        max_parallel=max_parallel,
        cell_count=cell_count,
        submission_source="cli",
    )


def _insert_cell_workflow_run(
    conn: sqlite3.Connection,
    *,
    sweep_run_id: int | None,
    status: str = "pending",
) -> int:
    cur = conn.execute(
        "INSERT INTO workflow_runs "
        "(workflow_name, status, started_at, triggered_by, sweep_run_id) "
        "VALUES (?, ?, ?, ?, ?)",
        ("wf", status, now_iso(), "cli", sweep_run_id),
    )
    return int(cur.lastrowid or 0)


def test_create_and_get_round_trip(sweep_repo: SweepRunRepository) -> None:
    sid = _create_sweep(sweep_repo, cell_count=4)
    row = sweep_repo.get(sid)
    assert row is not None
    assert row.id == sid
    assert row.name == "sweep_test"
    assert row.status == "pending"
    assert row.matrix == {"lr": [0.001, 0.01, 0.1]}
    assert row.args == {"dataset": "cifar10"}
    assert row.fail_fast is False
    assert row.max_parallel == 2
    assert row.cell_count == 4
    assert row.cells_pending == 4
    assert row.cells_running == 0
    assert row.cells_completed == 0
    assert row.cells_failed == 0
    assert row.cells_cancelled == 0
    assert row.submission_source == "cli"
    assert isinstance(row.started_at, datetime)
    assert row.completed_at is None
    assert row.cancel_requested_at is None
    assert row.error is None


def test_get_missing_returns_none(sweep_repo: SweepRunRepository) -> None:
    assert sweep_repo.get(9999) is None


def test_list_all_orders_newest_first(sweep_repo: SweepRunRepository) -> None:
    a = _create_sweep(sweep_repo, name="a")
    b = _create_sweep(sweep_repo, name="b")
    c = _create_sweep(sweep_repo, name="c")

    ids = [r.id for r in sweep_repo.list_all()]
    assert ids == [c, b, a]


def test_list_all_respects_limit(sweep_repo: SweepRunRepository) -> None:
    for _ in range(5):
        _create_sweep(sweep_repo)
    runs = sweep_repo.list_all(limit=3)
    assert len(runs) == 3


def test_list_incomplete_filters_by_status(
    sweep_repo: SweepRunRepository,
) -> None:
    a = _create_sweep(sweep_repo, name="a")  # pending
    b = _create_sweep(sweep_repo, name="b")
    c = _create_sweep(sweep_repo, name="c")
    d = _create_sweep(sweep_repo, name="d")
    e = _create_sweep(sweep_repo, name="e")

    sweep_repo.update_status(b, "running")
    sweep_repo.update_status(c, "draining")
    sweep_repo.update_status(d, "completed", completed_at=now_iso())
    sweep_repo.update_status(e, "failed", error="boom", completed_at=now_iso())

    ids = {r.id for r in sweep_repo.list_incomplete()}
    assert ids == {a, b, c}


def test_update_status_sets_error_and_completed_at(
    sweep_repo: SweepRunRepository,
) -> None:
    sid = _create_sweep(sweep_repo)
    ts = "2026-04-20T12:00:00.000Z"
    assert sweep_repo.update_status(sid, "failed", error="kaboom", completed_at=ts)

    row = sweep_repo.get(sid)
    assert row is not None
    assert row.status == "failed"
    assert row.error == "kaboom"
    assert row.completed_at is not None
    assert row.completed_at.year == 2026


def test_update_status_returns_false_on_missing(
    sweep_repo: SweepRunRepository,
) -> None:
    assert sweep_repo.update_status(9999, "running") is False


def test_request_cancel_stamps_timestamp(sweep_repo: SweepRunRepository) -> None:
    sid = _create_sweep(sweep_repo)
    assert sweep_repo.request_cancel(sid) is True
    row = sweep_repo.get(sid)
    assert row is not None
    assert row.cancel_requested_at is not None
    # status should be unchanged.
    assert row.status == "pending"


def test_request_cancel_is_idempotent(sweep_repo: SweepRunRepository) -> None:
    sid = _create_sweep(sweep_repo)
    assert sweep_repo.request_cancel(sid) is True
    # Second call sees cancel_requested_at already set; returns False.
    assert sweep_repo.request_cancel(sid) is False


def test_request_cancel_returns_false_on_missing(
    sweep_repo: SweepRunRepository,
) -> None:
    assert sweep_repo.request_cancel(9999) is False


def test_transition_cell_counters_update_on_success(
    conn: sqlite3.Connection,
    sweep_repo: SweepRunRepository,
) -> None:
    sid = _create_sweep(sweep_repo, cell_count=3)
    wr_id = _insert_cell_workflow_run(conn, sweep_run_id=sid, status="running")
    # Bootstrap the running counter by hand (orchestrator normally does
    # this via the pending -> running transition).
    conn.execute(
        "UPDATE sweep_runs SET cells_pending = cells_pending - 1, "
        "cells_running = cells_running + 1 WHERE id = ?",
        (sid,),
    )

    changed = sweep_repo.transition_cell(
        conn=conn,
        workflow_run_id=wr_id,
        from_status="running",
        to_status="completed",
        completed_at=now_iso(),
    )
    assert changed is True

    row = sweep_repo.get(sid)
    assert row is not None
    assert row.cells_pending == 2
    assert row.cells_running == 0
    assert row.cells_completed == 1


def test_transition_cell_is_idempotent_on_second_call(
    conn: sqlite3.Connection,
    sweep_repo: SweepRunRepository,
) -> None:
    sid = _create_sweep(sweep_repo, cell_count=2)
    wr_id = _insert_cell_workflow_run(conn, sweep_run_id=sid, status="running")
    conn.execute(
        "UPDATE sweep_runs SET cells_pending = cells_pending - 1, "
        "cells_running = cells_running + 1 WHERE id = ?",
        (sid,),
    )

    first = sweep_repo.transition_cell(
        conn=conn,
        workflow_run_id=wr_id,
        from_status="running",
        to_status="completed",
        completed_at=now_iso(),
    )
    second = sweep_repo.transition_cell(
        conn=conn,
        workflow_run_id=wr_id,
        from_status="running",
        to_status="completed",
        completed_at=now_iso(),
    )
    assert first is True
    assert second is False

    row = sweep_repo.get(sid)
    assert row is not None
    # Counter moved exactly once.
    assert row.cells_completed == 1
    assert row.cells_running == 0


def test_transition_cell_without_sweep_returns_true_and_no_counter_update(
    conn: sqlite3.Connection,
    sweep_repo: SweepRunRepository,
) -> None:
    # Detached workflow_run (no parent sweep).
    wr_id = _insert_cell_workflow_run(conn, sweep_run_id=None, status="pending")
    changed = sweep_repo.transition_cell(
        conn=conn,
        workflow_run_id=wr_id,
        from_status="pending",
        to_status="running",
    )
    assert changed is True
    # workflow_run status did move.
    row = conn.execute(
        "SELECT status FROM workflow_runs WHERE id = ?", (wr_id,)
    ).fetchone()
    assert row["status"] == "running"


def test_transition_cell_rejects_unknown_status(
    conn: sqlite3.Connection,
    sweep_repo: SweepRunRepository,
) -> None:
    sid = _create_sweep(sweep_repo, cell_count=1)
    wr_id = _insert_cell_workflow_run(conn, sweep_run_id=sid, status="pending")
    with pytest.raises(ValueError):
        sweep_repo.transition_cell(
            conn=conn,
            workflow_run_id=wr_id,
            from_status="pending",
            to_status="bogus",
        )
    with pytest.raises(ValueError):
        sweep_repo.transition_cell(
            conn=conn,
            workflow_run_id=wr_id,
            from_status="bogus",
            to_status="running",
        )


def test_transition_cell_pending_to_cancelled_adjusts_counters(
    conn: sqlite3.Connection,
    sweep_repo: SweepRunRepository,
) -> None:
    sid = _create_sweep(sweep_repo, cell_count=2)
    wr_id = _insert_cell_workflow_run(conn, sweep_run_id=sid, status="pending")

    changed = sweep_repo.transition_cell(
        conn=conn,
        workflow_run_id=wr_id,
        from_status="pending",
        to_status="cancelled",
        completed_at=now_iso(),
    )
    assert changed is True

    row = sweep_repo.get(sid)
    assert row is not None
    assert row.cells_pending == 1
    assert row.cells_cancelled == 1
