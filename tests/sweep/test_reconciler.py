"""Unit tests for :class:`srunx.sweep.reconciler.SweepReconciler`.

Uses the ``isolated_db`` fixture to redirect ``XDG_CONFIG_HOME`` to a
tmp dir so the reconciler's fresh connections see an empty DB.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import yaml  # type: ignore

from srunx.db.connection import open_connection, transaction
from srunx.db.repositories.base import now_iso
from srunx.db.repositories.workflow_runs import WorkflowRunRepository
from srunx.sweep import CellSpec
from srunx.sweep.orchestrator import SweepOrchestrator
from srunx.sweep.reconciler import SweepReconciler

# ---------------------------------------------------------------------------
# Test helpers
# ---------------------------------------------------------------------------


def _write_yaml(tmp_path: Path) -> Path:
    path = tmp_path / "wf.yaml"
    path.write_text(
        yaml.dump(
            {
                "name": "resume_test",
                "args": {"lr": 0.1},
                "jobs": [
                    {
                        "name": "train",
                        "command": ["train.py"],
                        "environment": {"conda": "env"},
                    }
                ],
            }
        )
    )
    return path


def _seed_sweep(
    *,
    yaml_path: str,
    status: str,
    cell_count: int,
    cells_pending: int,
    cells_running: int,
    cells_completed: int = 0,
    cells_failed: int = 0,
    cells_cancelled: int = 0,
    max_parallel: int = 2,
    fail_fast: bool = False,
) -> int:
    conn = open_connection()
    try:
        cur = conn.execute(
            """
            INSERT INTO sweep_runs (
                name, workflow_yaml_path, status, matrix, args,
                fail_fast, max_parallel, cell_count,
                cells_pending, cells_running, cells_completed,
                cells_failed, cells_cancelled,
                submission_source, started_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "resume_test",
                yaml_path,
                status,
                '{"lr":[0.1,0.01]}',
                "{}",
                1 if fail_fast else 0,
                max_parallel,
                cell_count,
                cells_pending,
                cells_running,
                cells_completed,
                cells_failed,
                cells_cancelled,
                "cli",
                now_iso(),
            ),
        )
        sweep_id = int(cur.lastrowid or 0)
    finally:
        conn.close()
    return sweep_id


def _seed_cells(
    sweep_id: int,
    statuses: list[str],
) -> list[int]:
    """Create ``len(statuses)`` cells with the given statuses, return ids."""
    conn = open_connection()
    try:
        ids: list[int] = []
        repo = WorkflowRunRepository(conn)
        for idx, status in enumerate(statuses):
            wf_id = repo.create(
                workflow_name="resume_test",
                yaml_path=None,
                args={"idx": idx},
                triggered_by="cli",
                sweep_run_id=sweep_id,
            )
            # Override status directly (the repo always starts at 'pending').
            if status != "pending":
                conn.execute(
                    "UPDATE workflow_runs SET status = ? WHERE id = ?",
                    (status, wf_id),
                )
            ids.append(wf_id)
    finally:
        conn.close()
    return ids


def _read_sweep_status(sweep_id: int) -> str:
    conn = open_connection()
    try:
        row = conn.execute(
            "SELECT status FROM sweep_runs WHERE id = ?", (sweep_id,)
        ).fetchone()
    finally:
        conn.close()
    assert row is not None
    return str(row["status"])


# ---------------------------------------------------------------------------
# Reconciler tests
# ---------------------------------------------------------------------------


class TestReconciler:
    def test_draining_sweep_is_skipped(
        self,
        isolated_db: Path,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A ``draining`` sweep is left alone — reconciler never spawns resumes."""
        yaml_path = _write_yaml(tmp_path)
        sweep_id = _seed_sweep(
            yaml_path=str(yaml_path),
            status="draining",
            cell_count=3,
            cells_pending=2,
            cells_running=1,
        )
        _seed_cells(sweep_id, ["pending", "pending", "running"])

        spawn_calls: list[int] = []

        def fake_spawn(
            cls: type,
            sweep_run_id: int,
            sweep_row: Any,
            pending_cells: list[CellSpec],
        ) -> None:
            spawn_calls.append(sweep_run_id)

        monkeypatch.setattr(
            SweepReconciler,
            "_spawn_resume",
            classmethod(fake_spawn),
        )

        SweepReconciler.scan_and_resume()

        assert spawn_calls == []
        assert _read_sweep_status(sweep_id) == "draining"

    def test_all_cells_terminal_forces_evaluate(
        self,
        isolated_db: Path,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Sweep stuck with every cell terminal → aggregator finalizes it."""
        yaml_path = _write_yaml(tmp_path)
        sweep_id = _seed_sweep(
            yaml_path=str(yaml_path),
            status="running",
            cell_count=2,
            cells_pending=0,
            cells_running=0,
            cells_completed=2,
        )
        _seed_cells(sweep_id, ["completed", "completed"])

        # Drive each cell through running→completed so counters stay
        # consistent with the seeded aggregate counts.
        # (The seed already put the aggregate counts where they should be.)

        spawn_calls: list[int] = []

        def fake_spawn(
            cls: type,
            sweep_run_id: int,
            sweep_row: Any,
            pending_cells: list[CellSpec],
        ) -> None:
            spawn_calls.append(sweep_run_id)

        monkeypatch.setattr(
            SweepReconciler,
            "_spawn_resume",
            classmethod(fake_spawn),
        )

        SweepReconciler.scan_and_resume()

        # No resume needed; everything already done.
        assert spawn_calls == []
        assert _read_sweep_status(sweep_id) == "completed"

    def test_running_sweep_with_pending_cells_triggers_resume(
        self,
        isolated_db: Path,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """status=running + pending cells + headroom → orchestrator resume is spawned."""
        yaml_path = _write_yaml(tmp_path)
        sweep_id = _seed_sweep(
            yaml_path=str(yaml_path),
            status="running",
            cell_count=3,
            cells_pending=2,
            cells_running=0,
            cells_completed=1,
            max_parallel=2,
        )
        cell_ids = _seed_cells(sweep_id, ["completed", "pending", "pending"])

        spawn_calls: list[tuple[int, list[int]]] = []

        def fake_spawn(
            cls: type,
            sweep_run_id: int,
            sweep_row: Any,
            pending_cells: list[CellSpec],
        ) -> None:
            spawn_calls.append(
                (sweep_run_id, [c.workflow_run_id for c in pending_cells])
            )

        monkeypatch.setattr(
            SweepReconciler,
            "_spawn_resume",
            classmethod(fake_spawn),
        )

        SweepReconciler.scan_and_resume()

        assert len(spawn_calls) == 1
        resumed_sweep_id, resumed_cell_ids = spawn_calls[0]
        assert resumed_sweep_id == sweep_id
        # The two pending cells were passed through.
        assert sorted(resumed_cell_ids) == sorted(cell_ids[1:])

    def test_no_headroom_skips_resume(
        self,
        isolated_db: Path,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """max_parallel=1 with 1 running cell → headroom=0, reconciler waits."""
        yaml_path = _write_yaml(tmp_path)
        sweep_id = _seed_sweep(
            yaml_path=str(yaml_path),
            status="running",
            cell_count=3,
            cells_pending=2,
            cells_running=1,
            max_parallel=1,
        )
        _seed_cells(sweep_id, ["running", "pending", "pending"])

        spawn_calls: list[int] = []

        def fake_spawn(
            cls: type,
            sweep_run_id: int,
            sweep_row: Any,
            pending_cells: list[CellSpec],
        ) -> None:
            spawn_calls.append(sweep_run_id)

        monkeypatch.setattr(
            SweepReconciler,
            "_spawn_resume",
            classmethod(fake_spawn),
        )

        SweepReconciler.scan_and_resume()

        assert spawn_calls == []
        # Sweep remains in running — pollers will drive completion.
        assert _read_sweep_status(sweep_id) == "running"

    def test_resume_drives_pending_cells_to_completion(
        self,
        isolated_db: Path,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """End-to-end resume: orchestrator.resume_from_db actually finishes the cells."""
        yaml_path = _write_yaml(tmp_path)
        sweep_id = _seed_sweep(
            yaml_path=str(yaml_path),
            status="running",
            cell_count=3,
            cells_pending=2,
            cells_running=0,
            cells_completed=1,
            max_parallel=2,
        )
        cell_ids = _seed_cells(sweep_id, ["completed", "pending", "pending"])

        # Fake the cell execution path by simulating the DB transitions.
        from srunx.sweep.state_service import WorkflowRunStateService

        def _simulate(cell: CellSpec) -> None:
            conn = open_connection()
            try:
                with transaction(conn, "IMMEDIATE"):
                    WorkflowRunStateService.update(
                        conn=conn,
                        workflow_run_id=cell.workflow_run_id,
                        from_status="pending",
                        to_status="running",
                    )
                with transaction(conn, "IMMEDIATE"):
                    WorkflowRunStateService.update(
                        conn=conn,
                        workflow_run_id=cell.workflow_run_id,
                        from_status="running",
                        to_status="completed",
                        completed_at=now_iso(),
                    )
            finally:
                conn.close()

        monkeypatch.setattr(
            SweepOrchestrator,
            "_run_cell_sync",
            lambda self, cell: _simulate(cell),
        )

        SweepReconciler.scan_and_resume()

        # All cells now completed, sweep aggregated to completed.
        conn = open_connection()
        try:
            row = conn.execute(
                "SELECT cells_completed, cells_pending, status "
                "FROM sweep_runs WHERE id = ?",
                (sweep_id,),
            ).fetchone()
            cell_rows = conn.execute(
                "SELECT status FROM workflow_runs WHERE sweep_run_id = ?",
                (sweep_id,),
            ).fetchall()
        finally:
            conn.close()

        assert row["cells_completed"] == 3
        assert row["cells_pending"] == 0
        assert row["status"] == "completed"
        assert {r["status"] for r in cell_rows} == {"completed"}
        # The previously-completed cell stayed completed.
        conn = open_connection()
        try:
            pre_completed = conn.execute(
                "SELECT status FROM workflow_runs WHERE id = ?",
                (cell_ids[0],),
            ).fetchone()
        finally:
            conn.close()
        assert pre_completed["status"] == "completed"
