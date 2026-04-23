"""Unit tests for :class:`srunx.sweep.orchestrator.SweepOrchestrator`.

Uses the ``isolated_db`` fixture (see ``tests/sweep/conftest.py``) which
redirects ``XDG_CONFIG_HOME`` to a tmp dir so every orchestrator call
opens fresh connections against a clean DB.

Cell execution is simulated by patching ``_run_cell_sync`` so no real
``WorkflowRunner`` / SLURM traffic happens. The simulator drives the
``workflow_runs`` row through ``pending → running → <final>`` via
:class:`WorkflowRunStateService` — exactly the path the real runner
takes — so sweep counters and aggregation events are exercised for
real.
"""

from __future__ import annotations

import sqlite3
import threading
from pathlib import Path
from typing import Any

import pytest
import yaml  # type: ignore
from srunx.db.connection import open_connection, transaction
from srunx.db.repositories.sweep_runs import SweepRunRepository

from srunx.exceptions import SweepExecutionError
from srunx.sweep import CellSpec, SweepSpec
from srunx.sweep.orchestrator import SweepOrchestrator
from srunx.sweep.state_service import WorkflowRunStateService

# ---------------------------------------------------------------------------
# Test helpers
# ---------------------------------------------------------------------------


def _write_yaml(path: Path, data: dict[str, Any]) -> Path:
    path.write_text(yaml.dump(data))
    return path


def _simulate_cell(
    workflow_run_id: int,
    *,
    final_status: str = "completed",
    error: str | None = None,
) -> None:
    """Drive one cell's ``workflow_runs`` row to a terminal status.

    Mirrors :class:`WorkflowRunner`'s own sequence
    (``pending → running → <final>``) so sweep counters + aggregation
    events fire for real. Every transition opens its own short
    ``BEGIN IMMEDIATE`` TX so we match the production shape.
    """
    conn = open_connection()
    try:
        with transaction(conn, "IMMEDIATE"):
            WorkflowRunStateService.update(
                conn=conn,
                workflow_run_id=workflow_run_id,
                from_status="pending",
                to_status="running",
            )
        with transaction(conn, "IMMEDIATE"):
            WorkflowRunStateService.update(
                conn=conn,
                workflow_run_id=workflow_run_id,
                from_status="running",
                to_status=final_status,
                error=error,
                completed_at=_now(),
            )
    finally:
        conn.close()


def _now() -> str:
    from srunx.db.repositories.base import now_iso

    return now_iso()


def _read_sweep(sweep_run_id: int) -> dict[str, Any]:
    conn = open_connection()
    try:
        row = conn.execute(
            "SELECT status, cell_count, cells_pending, cells_running, "
            "       cells_completed, cells_failed, cells_cancelled, error "
            "FROM sweep_runs WHERE id = ?",
            (sweep_run_id,),
        ).fetchone()
    finally:
        conn.close()
    assert row is not None
    return dict(row)


def _read_cells(sweep_run_id: int) -> list[sqlite3.Row]:
    conn = open_connection()
    try:
        return conn.execute(
            "SELECT id, status FROM workflow_runs "
            "WHERE sweep_run_id = ? ORDER BY id ASC",
            (sweep_run_id,),
        ).fetchall()
    finally:
        conn.close()


def _build_orchestrator(
    tmp_path: Path,
    *,
    matrix: dict[str, list[Any]],
    fail_fast: bool = False,
    max_parallel: int = 2,
    endpoint_id: int | None = None,
) -> SweepOrchestrator:
    yaml_path = _write_yaml(
        tmp_path / "wf.yaml",
        {
            "name": "sweep_under_test",
            "args": {"lr": 0.1},
            "jobs": [
                {
                    "name": "train",
                    "command": ["train.py"],
                    "environment": {"conda": "env"},
                }
            ],
        },
    )
    return SweepOrchestrator(
        workflow_yaml_path=yaml_path,
        workflow_data={"name": "sweep_under_test", "args": {"lr": 0.1}},
        args_override=None,
        sweep_spec=SweepSpec(
            matrix=matrix,
            fail_fast=fail_fast,
            max_parallel=max_parallel,
        ),
        submission_source="cli",
        endpoint_id=endpoint_id,
    )


# ---------------------------------------------------------------------------
# Happy-path tests
# ---------------------------------------------------------------------------


class TestHappyPath:
    """Orchestrator drives a 4-cell sweep to completed."""

    def test_four_cells_all_complete(
        self,
        isolated_db: Path,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        orch = _build_orchestrator(
            tmp_path,
            matrix={"lr": [0.1, 0.01], "seed": [1, 2]},
            max_parallel=4,
        )

        def fake_run_cell_sync(cell: CellSpec) -> None:
            _simulate_cell(cell.workflow_run_id, final_status="completed")

        monkeypatch.setattr(
            SweepOrchestrator,
            "_run_cell_sync",
            lambda self, cell: fake_run_cell_sync(cell),
        )

        sweep = orch.run()

        assert sweep.id is not None
        row = _read_sweep(sweep.id)
        assert row["cell_count"] == 4
        assert row["cells_pending"] == 0
        assert row["cells_running"] == 0
        assert row["cells_completed"] == 4
        assert row["cells_failed"] == 0
        assert row["cells_cancelled"] == 0
        assert row["status"] == "completed"

    def test_max_parallel_exceeds_cell_count_is_clamped(
        self,
        isolated_db: Path,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """``max_parallel=10`` with 2 cells must not error."""
        orch = _build_orchestrator(
            tmp_path,
            matrix={"lr": [0.1, 0.01]},
            max_parallel=10,
        )

        monkeypatch.setattr(
            SweepOrchestrator,
            "_run_cell_sync",
            lambda self, cell: _simulate_cell(cell.workflow_run_id),
        )

        sweep = orch.run()
        assert sweep.id is not None
        row = _read_sweep(sweep.id)
        assert row["cells_completed"] == 2
        assert row["status"] == "completed"


# ---------------------------------------------------------------------------
# Failure fan-out
# ---------------------------------------------------------------------------


class TestCellFailure:
    def test_one_cell_fails_fail_fast_false(
        self,
        isolated_db: Path,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """With ``fail_fast=false``, surviving cells all complete and the sweep ends as failed."""
        orch = _build_orchestrator(
            tmp_path,
            matrix={"lr": [0.1, 0.01, 0.001, 0.0001]},
            fail_fast=False,
            max_parallel=4,
        )

        def fake_run(cell: CellSpec) -> None:
            if cell.cell_index == 1:
                raise RuntimeError("simulated cell failure")
            _simulate_cell(cell.workflow_run_id, final_status="completed")

        # For the failing cell, _run_cell catches the exception and then
        # calls _on_cell_done with final_status='failed'. But the DB row
        # still needs to transition running→failed; the real runner would
        # do that, so we emulate it via a side effect on the failing path.
        def _run_cell_sync(self: SweepOrchestrator, cell: CellSpec) -> None:
            try:
                fake_run(cell)
            except Exception:
                _simulate_cell(cell.workflow_run_id, final_status="failed", error="sim")
                raise

        monkeypatch.setattr(SweepOrchestrator, "_run_cell_sync", _run_cell_sync)

        sweep = orch.run()
        assert sweep.id is not None
        row = _read_sweep(sweep.id)
        assert row["cell_count"] == 4
        assert row["cells_completed"] == 3
        assert row["cells_failed"] == 1
        assert row["cells_pending"] == 0
        assert row["cells_running"] == 0
        assert row["status"] == "failed"

    def test_fail_fast_blocks_semaphore_queued_cells_from_starting(
        self,
        isolated_db: Path,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Regression for C1: fail_fast must set ``_cancelled`` before draining.

        Under ``max_parallel=1`` with 5 cells, cells 1..4 are queued on
        the semaphore while cell 0 runs. When cell 0 fails, the
        fail-fast branch of ``_on_cell_done`` must flip ``_cancelled``
        so any cell that *subsequently* acquires the semaphore observes
        the flag and returns early instead of executing. Without the
        ``_cancelled = True`` line, queued cells would proceed to run
        even though ``_drain`` already marked their rows ``cancelled``.
        """
        orch = _build_orchestrator(
            tmp_path,
            matrix={"lr": [0.1, 0.01, 0.001, 0.0001, 0.00001]},
            fail_fast=True,
            max_parallel=1,
        )

        started: list[int] = []

        def _run_cell_sync(self: SweepOrchestrator, cell: CellSpec) -> None:
            started.append(cell.cell_index)
            if cell.cell_index == 0:
                _simulate_cell(
                    cell.workflow_run_id, final_status="failed", error="boom"
                )
                raise RuntimeError("boom")
            # Any cell that starts after the failure is a C1 regression —
            # the semaphore-queued cell should have bailed out when it
            # saw ``self._cancelled`` right after ``sem.acquire()``.
            pytest.fail(
                f"cell {cell.cell_index} started after fail_fast drain "
                "(C1 regression: _cancelled not set before _drain)"
            )

        monkeypatch.setattr(SweepOrchestrator, "_run_cell_sync", _run_cell_sync)

        sweep = orch.run()
        assert sweep.id is not None
        # Exactly one cell actually executed (the failing one).
        assert started == [0]
        row = _read_sweep(sweep.id)
        assert row["cells_failed"] == 1
        assert row["cells_cancelled"] == 4
        assert row["status"] == "failed"

    def test_one_cell_fails_fail_fast_true(
        self,
        isolated_db: Path,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """With ``fail_fast=true`` the first failure drains pending cells.

        To deterministically keep some cells pending when the drain fires,
        we serialize cells (``max_parallel=1``) and schedule the failure
        first. Subsequent cells should be drained to ``cancelled``.
        """
        orch = _build_orchestrator(
            tmp_path,
            matrix={"lr": [0.1, 0.01, 0.001, 0.0001]},
            fail_fast=True,
            max_parallel=1,
        )

        started: list[int] = []

        def _run_cell_sync(self: SweepOrchestrator, cell: CellSpec) -> None:
            started.append(cell.cell_index)
            # Skip cells that have already been cancelled by drain —
            # the real runner would observe the row is no longer pending
            # and WorkflowRunStateService.update would no-op.
            if cell.cell_index == 0:
                _simulate_cell(
                    cell.workflow_run_id, final_status="failed", error="boom"
                )
                raise RuntimeError("boom")
            # If we were drained, the row is no longer pending; nothing
            # to do. Note: under max_parallel=1 the subsequent cells
            # should not run at all because _cancelled is set before the
            # task group schedules them. This branch is defensive.

        monkeypatch.setattr(SweepOrchestrator, "_run_cell_sync", _run_cell_sync)

        sweep = orch.run()
        assert sweep.id is not None
        row = _read_sweep(sweep.id)
        # 1 failed + 3 cancelled by drain
        assert row["cells_failed"] == 1
        assert row["cells_cancelled"] == 3
        assert row["cells_completed"] == 0
        assert row["cells_pending"] == 0
        assert row["cells_running"] == 0
        assert row["status"] == "failed"


# ---------------------------------------------------------------------------
# C1 atomic-claim regression: drain must beat late sem wake to SLURM submit
# ---------------------------------------------------------------------------


class TestAtomicCellClaim:
    """Regression: drained cell must not call ``_run_cell_sync`` even if a
    racing task already passed the ``if self._cancelled`` check.

    Simulates the narrow TOCTOU window where a queued cell wakes from the
    semaphore before the fail-fast drain flips ``_cancelled``. The
    ``_claim_cell_running`` DB-level optimistic UPDATE must fail (because
    drain already set the row to ``cancelled``) so ``_run_cell_sync`` is
    never called.
    """

    def test_race_drain_vs_acquire_skips_drained_cell(
        self,
        isolated_db: Path,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        orch = _build_orchestrator(
            tmp_path,
            matrix={"lr": [0.1, 0.01]},
            fail_fast=False,  # we'll simulate the drain manually
            max_parallel=2,
        )

        # Materialize up-front so we know both workflow_run_ids.
        cells = orch._expand_cells()
        sweep_run_id = orch._materialize(cells)
        assert len(orch._cells) == 2

        # Simulate a concurrent drain: flip cell 1's row to cancelled
        # BEFORE the orchestrator attempts its atomic claim. This mirrors
        # the fail-fast race where ``_drain`` wins between
        # ``_cancelled`` being observed False and the claim firing.
        from srunx.sweep.orchestrator import drain_sweep_pending_cells

        # Drain everything that's still pending. Since no cell has moved
        # off ``pending`` yet, both cells will be marked ``cancelled``.
        drain_sweep_pending_cells(sweep_run_id)

        call_counts: dict[int, int] = {}

        def _run_cell_sync(self: SweepOrchestrator, cell: CellSpec) -> None:
            call_counts[cell.cell_index] = call_counts.get(cell.cell_index, 0) + 1
            _simulate_cell(cell.workflow_run_id, final_status="completed")

        monkeypatch.setattr(SweepOrchestrator, "_run_cell_sync", _run_cell_sync)

        # Resume with the already-materialized (now drained) cells. Every
        # cell's atomic claim must return False because the rows are
        # ``cancelled``, so ``_run_cell_sync`` is never invoked.
        import anyio

        anyio.run(orch.arun_from_materialized, sweep_run_id)

        assert call_counts == {}, (
            "atomic claim should have skipped drained cells; "
            f"_run_cell_sync was invoked for indices: {sorted(call_counts)}"
        )

        # Every cell remains cancelled; nothing transitioned to running.
        cells_rows = _read_cells(sweep_run_id)
        assert [r["status"] for r in cells_rows] == ["cancelled", "cancelled"]

    def test_claim_succeeds_for_normal_cells(
        self,
        isolated_db: Path,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Happy path: every cell's atomic claim returns True exactly once."""
        orch = _build_orchestrator(
            tmp_path,
            matrix={"lr": [0.1, 0.01, 0.001]},
            fail_fast=False,
            max_parallel=3,
        )

        claim_results: list[bool] = []
        original_claim = SweepOrchestrator._claim_cell_running

        def _recording_claim(self: SweepOrchestrator, workflow_run_id: int) -> bool:
            result = original_claim(self, workflow_run_id)
            claim_results.append(result)
            return result

        monkeypatch.setattr(SweepOrchestrator, "_claim_cell_running", _recording_claim)
        monkeypatch.setattr(
            SweepOrchestrator,
            "_run_cell_sync",
            lambda self, cell: _simulate_cell(cell.workflow_run_id),
        )

        sweep = orch.run()
        assert sweep.id is not None

        # Every cell claimed exactly once and each claim won.
        assert len(claim_results) == 3
        assert all(claim_results)

        row = _read_sweep(sweep.id)
        assert row["cells_completed"] == 3
        assert row["status"] == "completed"


# ---------------------------------------------------------------------------
# C2 regression: from_yaml-style failure must transition cell to failed
# ---------------------------------------------------------------------------


class TestRunnerRaisesBeforeTransition:
    def test_exception_in_run_cell_sync_records_failed_transition(
        self,
        isolated_db: Path,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Regression for C2: ``_run_cell_sync`` raising pre-transition still finalizes.

        Simulates ``from_yaml`` blowing up inside ``_run_cell_sync`` —
        the runner never gets to flip ``workflow_runs.status`` from
        ``pending``. The orchestrator must observe the failure and
        drive the cell to ``failed`` itself via
        :meth:`SweepOrchestrator._record_cell_failure`.
        """
        orch = _build_orchestrator(
            tmp_path,
            matrix={"lr": [0.1, 0.01]},
            fail_fast=False,
            max_parallel=2,
        )

        def _run_cell_sync(self: SweepOrchestrator, cell: CellSpec) -> None:
            if cell.cell_index == 0:
                raise RuntimeError("simulated from_yaml failure")
            _simulate_cell(cell.workflow_run_id, final_status="completed")

        monkeypatch.setattr(SweepOrchestrator, "_run_cell_sync", _run_cell_sync)

        sweep = orch.run()
        assert sweep.id is not None

        row = _read_sweep(sweep.id)
        assert row["cells_failed"] == 1
        assert row["cells_completed"] == 1
        assert row["cells_pending"] == 0
        assert row["cells_running"] == 0

        cells = _read_cells(sweep.id)
        statuses = sorted(r["status"] for r in cells)
        assert statuses == ["completed", "failed"]


# ---------------------------------------------------------------------------
# User cancel
# ---------------------------------------------------------------------------


class TestUserCancel:
    def test_cancel_precedence_over_late_failure(
        self,
        isolated_db: Path,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Cancelled takes precedence even if a later cell fails.

        Sequence (``max_parallel=1``):
          1. cell 0 runs and completes
          2. before cell 1 starts, user requests cancel → cells 1..3 cancelled
          3. the loop observes ``_cancelled`` and does not spawn cell 1

        Expected: status=cancelled per R4.6 (cancel precedence).
        """
        orch = _build_orchestrator(
            tmp_path,
            matrix={"lr": [0.1, 0.01, 0.001, 0.0001]},
            fail_fast=False,
            max_parallel=1,
        )

        call_count = {"n": 0}

        def _run_cell_sync(self: SweepOrchestrator, cell: CellSpec) -> None:
            call_count["n"] += 1
            _simulate_cell(cell.workflow_run_id, final_status="completed")
            # After the first cell finishes, request cancel. The outer
            # arun() loop will observe ``_cancelled`` before starting
            # cell 2 (we only have 1 sem slot, so nothing else is
            # running yet).
            if cell.cell_index == 0:
                orch.request_cancel()

        monkeypatch.setattr(SweepOrchestrator, "_run_cell_sync", _run_cell_sync)

        sweep = orch.run()
        assert sweep.id is not None
        row = _read_sweep(sweep.id)
        # Exactly 1 cell actually ran (the rest were cancelled before start).
        assert call_count["n"] == 1
        assert row["cells_completed"] == 1
        assert row["cells_cancelled"] == 3
        assert row["status"] == "cancelled"


# ---------------------------------------------------------------------------
# Materialize error path (task 18b)
# ---------------------------------------------------------------------------


class TestMaterializeError:
    def test_materialize_failure_records_failed_audit_row(
        self,
        isolated_db: Path,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Injecting a DB error during cell INSERT leaves a single failed sweep row."""
        orch = _build_orchestrator(
            tmp_path,
            matrix={"lr": [0.1, 0.01]},
            max_parallel=2,
        )

        def exploding_create(self: Any, *args: Any, **kwargs: Any) -> int:
            raise sqlite3.OperationalError("simulated disk full")

        # Patch WorkflowRunRepository.create — SweepRunRepository.create
        # runs first inside the happy-path TX, then cell INSERTs trip
        # on this patched method and roll back.
        from srunx.db.repositories.workflow_runs import WorkflowRunRepository

        monkeypatch.setattr(WorkflowRunRepository, "create", exploding_create)

        with pytest.raises(SweepExecutionError):
            orch.run()

        # Audit row present, happy-path rows NOT present.
        conn = open_connection()
        try:
            rows = conn.execute(
                "SELECT status, cell_count, error FROM sweep_runs"
            ).fetchall()
        finally:
            conn.close()

        assert len(rows) == 1
        audit = rows[0]
        assert audit["status"] == "failed"
        assert audit["cell_count"] == 0
        assert audit["error"] is not None
        assert "simulated disk full" in audit["error"]

        # No workflow_runs were committed.
        conn = open_connection()
        try:
            wf_rows = conn.execute("SELECT COUNT(*) FROM workflow_runs").fetchone()
        finally:
            conn.close()
        assert wf_rows[0] == 0


# ---------------------------------------------------------------------------
# Concurrency sanity
# ---------------------------------------------------------------------------


class TestConcurrencyBehavior:
    def test_cells_execute_under_parallel_bound(
        self,
        isolated_db: Path,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Under ``max_parallel=2`` at most 2 cells are in-flight at once."""
        orch = _build_orchestrator(
            tmp_path,
            matrix={"lr": [0.1, 0.01, 0.001, 0.0001, 0.00001, 0.000001]},
            max_parallel=2,
        )

        in_flight = {"count": 0, "max": 0}
        lock = threading.Lock()

        def _run_cell_sync(self: SweepOrchestrator, cell: CellSpec) -> None:
            with lock:
                in_flight["count"] += 1
                in_flight["max"] = max(in_flight["max"], in_flight["count"])
            try:
                # Simulate a little work via DB transitions.
                _simulate_cell(cell.workflow_run_id, final_status="completed")
            finally:
                with lock:
                    in_flight["count"] -= 1

        monkeypatch.setattr(SweepOrchestrator, "_run_cell_sync", _run_cell_sync)

        sweep = orch.run()
        assert sweep.id is not None
        assert in_flight["max"] <= 2
        row = _read_sweep(sweep.id)
        assert row["cells_completed"] == 6
        assert row["status"] == "completed"


# ---------------------------------------------------------------------------
# Expand + materialize units
# ---------------------------------------------------------------------------


class TestExpandAndMaterialize:
    def test_expand_produces_cross_product_merged_with_base_args(
        self,
        isolated_db: Path,
        tmp_path: Path,
    ) -> None:
        orch = _build_orchestrator(
            tmp_path,
            matrix={"lr": [0.1, 0.01], "seed": [1, 2]},
            max_parallel=2,
        )
        cells = orch._expand_cells()
        # 2 * 2 = 4 cells, base_args merged.
        assert len(cells) == 4
        for cell in cells:
            # lr comes from matrix (overrides base_args), seed from matrix.
            assert "lr" in cell
            assert "seed" in cell

    def test_materialize_creates_rows_without_per_cell_watches(
        self,
        isolated_db: Path,
        tmp_path: Path,
    ) -> None:
        """Sweep cells must NOT get per-cell workflow_run watches.

        Regression guard for C3: sweep cells don't populate
        ``workflow_run_jobs``, so a workflow_run watch on each cell would
        cause the active-watch poller to aggregate over the empty child
        set and pull a terminal cell back to 'pending'.
        """
        orch = _build_orchestrator(
            tmp_path,
            matrix={"lr": [0.1, 0.01]},
            max_parallel=1,
        )
        cells = orch._expand_cells()
        sweep_run_id = orch._materialize(cells)

        assert orch._sweep_run_id == sweep_run_id
        conn = open_connection()
        try:
            sweep_repo = SweepRunRepository(conn)
            sweep = sweep_repo.get(sweep_run_id)
            assert sweep is not None
            assert sweep.cell_count == 2
            assert sweep.cells_pending == 2

            wf_rows = conn.execute(
                "SELECT id, status, sweep_run_id FROM workflow_runs "
                "WHERE sweep_run_id = ?",
                (sweep_run_id,),
            ).fetchall()
            assert len(wf_rows) == 2
            assert all(r["status"] == "pending" for r in wf_rows)
            assert all(r["sweep_run_id"] == sweep_run_id for r in wf_rows)

            # No per-cell workflow_run watches created when endpoint_id is None.
            watch_rows = conn.execute(
                "SELECT kind, target_ref FROM watches WHERE kind = 'workflow_run'"
            ).fetchall()
            assert len(watch_rows) == 0
            # No sweep_run watch either (endpoint_id was None).
            sweep_watch_rows = conn.execute(
                "SELECT kind, target_ref FROM watches WHERE kind = 'sweep_run'"
            ).fetchall()
            assert len(sweep_watch_rows) == 0
        finally:
            conn.close()
