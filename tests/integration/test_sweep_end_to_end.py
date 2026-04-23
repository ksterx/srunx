"""9-cell end-to-end smoke test for the sweep pipeline.

Phase-J Task 53. Exercises the Phase-G SweepOrchestrator's stable public
API (``run()`` + ``SweepRunRepository.get()``) without booting any real
SLURM plumbing. Cell execution is simulated by patching
:meth:`SweepOrchestrator._run_cell_sync` — the same pattern the
``tests/sweep/test_orchestrator.py`` unit tests use — but here we crank
the matrix up to 3x3 so this acts as a regression smoke for the full
expand → materialize → drive → aggregate → notify loop.

Scenarios covered:

- 3x3 matrix, one cell deliberately fails mid-run. ``fail_fast=false``
  means the other 8 cells still complete, and the sweep's final status
  is ``failed`` with the right counters.
- Exactly two ``sweep_run.status_changed`` events fire (pending → running
  at first-cell-start, running → failed at final terminal).
- Without an endpoint subscription, **zero** deliveries are queued for
  either the parent sweep or any individual cell.
- When a sweep-level subscription is attached (``endpoint_id=...``),
  exactly one delivery is queued on the terminal transition — no
  per-cell deliveries are produced.

The tests only touch the ``isolated_db`` fixture pattern already used by
``tests/sweep/``; they avoid calling into ``WorkflowRunner`` or real
SLURM so they remain robust to the in-flight backend refactor on the
state service.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import yaml  # type: ignore

from srunx.db.connection import open_connection, transaction
from srunx.db.repositories.deliveries import DeliveryRepository
from srunx.db.repositories.endpoints import EndpointRepository
from srunx.db.repositories.sweep_runs import SweepRunRepository
from srunx.sweep import CellSpec, SweepSpec
from srunx.sweep.orchestrator import SweepOrchestrator
from srunx.sweep.state_service import WorkflowRunStateService

# ---------------------------------------------------------------------------
# Fixture — reuse the tests/sweep/ pattern but define locally so tests/
# integration/ doesn't need to import sibling conftests.
# ---------------------------------------------------------------------------


@pytest.fixture
def isolated_db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Per-test srunx SQLite DB under an isolated XDG_CONFIG_HOME."""
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path))
    from srunx.db.connection import init_db

    return init_db(delete_legacy=False)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _write_yaml(path: Path, data: dict[str, Any]) -> Path:
    path.write_text(yaml.dump(data))
    return path


def _now() -> str:
    from srunx.db.repositories.base import now_iso

    return now_iso()


def _simulate_cell(
    workflow_run_id: int,
    *,
    final_status: str = "completed",
    error: str | None = None,
) -> None:
    """Drive one cell's ``workflow_runs`` row pending → running → terminal.

    Mirrors the real ``WorkflowRunner`` sequence so sweep counters and
    aggregation events fire on the real state-service path.
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


def _count_sweep_events(sweep_run_id: int) -> int:
    conn = open_connection()
    try:
        row = conn.execute(
            "SELECT COUNT(*) AS c FROM events WHERE kind = ? AND source_ref = ?",
            ("sweep_run.status_changed", f"sweep_run:{sweep_run_id}"),
        ).fetchone()
    finally:
        conn.close()
    return int(row["c"])


def _count_cell_deliveries(sweep_run_id: int) -> int:
    """Deliveries queued against any of this sweep's *cell* workflow_runs."""
    conn = open_connection()
    try:
        row = conn.execute(
            """
            SELECT COUNT(*) AS c
            FROM deliveries d
            JOIN events e ON e.id = d.event_id
            WHERE e.kind = 'workflow_run.status_changed'
              AND e.source_ref IN (
                  SELECT 'workflow_run:' || id
                  FROM workflow_runs
                  WHERE sweep_run_id = ?
              )
            """,
            (sweep_run_id,),
        ).fetchone()
    finally:
        conn.close()
    return int(row["c"])


def _build_orchestrator(
    tmp_path: Path,
    *,
    matrix: dict[str, list[Any]],
    fail_fast: bool = False,
    max_parallel: int = 4,
    endpoint_id: int | None = None,
) -> SweepOrchestrator:
    yaml_path = _write_yaml(
        tmp_path / "wf.yaml",
        {
            "name": "sweep_e2e",
            "args": {"lr": 0.1, "seed": 0},
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
        workflow_data={"name": "sweep_e2e", "args": {"lr": 0.1, "seed": 0}},
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
# Tests
# ---------------------------------------------------------------------------


class TestNineCellSweep:
    """3x3 matrix (9 cells) end-to-end smoke."""

    def test_nine_cells_one_failure_fail_fast_false(
        self,
        isolated_db: Path,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """8 cells complete, 1 fails; sweep terminal = failed."""
        orch = _build_orchestrator(
            tmp_path,
            matrix={
                "lr": [0.001, 0.01, 0.1],
                "seed": [1, 2, 3],
            },
            fail_fast=False,
            max_parallel=3,
        )

        # Force exactly one cell (index 4 — middle of the 9) to fail.
        FAILING_INDEX = 4

        def _run_cell_sync(self: SweepOrchestrator, cell: CellSpec) -> None:
            if cell.cell_index == FAILING_INDEX:
                _simulate_cell(
                    cell.workflow_run_id,
                    final_status="failed",
                    error="simulated cell failure",
                )
                raise RuntimeError("simulated cell failure")
            _simulate_cell(cell.workflow_run_id, final_status="completed")

        monkeypatch.setattr(SweepOrchestrator, "_run_cell_sync", _run_cell_sync)

        sweep = orch.run()

        assert sweep.id is not None
        assert sweep.cell_count == 9
        assert sweep.cells_completed == 8
        assert sweep.cells_failed == 1
        assert sweep.cells_cancelled == 0
        assert sweep.cells_pending == 0
        assert sweep.cells_running == 0
        assert sweep.status == "failed"

        # Confirm via repository too — parity check with the returned model.
        conn = open_connection()
        try:
            repo = SweepRunRepository(conn)
            row = repo.get(sweep.id)
        finally:
            conn.close()
        assert row is not None
        assert row.status == "failed"
        assert row.cells_completed == 8
        assert row.cells_failed == 1

    def test_exactly_two_sweep_level_events_fire(
        self,
        isolated_db: Path,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """pending → running (on first cell start) + running → failed (terminal) = 2 events."""
        orch = _build_orchestrator(
            tmp_path,
            matrix={
                "lr": [0.001, 0.01, 0.1],
                "seed": [1, 2, 3],
            },
            fail_fast=False,
            max_parallel=3,
        )

        def _run_cell_sync(self: SweepOrchestrator, cell: CellSpec) -> None:
            if cell.cell_index == 0:
                _simulate_cell(
                    cell.workflow_run_id,
                    final_status="failed",
                    error="simulated",
                )
                raise RuntimeError("simulated")
            _simulate_cell(cell.workflow_run_id, final_status="completed")

        monkeypatch.setattr(SweepOrchestrator, "_run_cell_sync", _run_cell_sync)

        sweep = orch.run()
        assert sweep.id is not None

        # Expect pending→running (first cell start) and running→failed (final).
        assert _count_sweep_events(sweep.id) == 2

    def test_without_endpoint_no_cell_deliveries_queued(
        self,
        isolated_db: Path,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """No sweep-level subscription ⇒ zero deliveries (cells or parent)."""
        orch = _build_orchestrator(
            tmp_path,
            matrix={"lr": [0.001, 0.01, 0.1], "seed": [1, 2, 3]},
            fail_fast=False,
            max_parallel=3,
            endpoint_id=None,
        )

        monkeypatch.setattr(
            SweepOrchestrator,
            "_run_cell_sync",
            lambda self, cell: _simulate_cell(cell.workflow_run_id),
        )

        sweep = orch.run()
        assert sweep.id is not None

        # Any cell delivery would signal a regression — cell-level
        # subscriptions are explicitly not created for sweeps (see
        # orchestrator._materialize_happy_path).
        assert _count_cell_deliveries(sweep.id) == 0

        conn = open_connection()
        try:
            total = conn.execute("SELECT COUNT(*) AS c FROM deliveries").fetchone()
        finally:
            conn.close()
        assert int(total["c"]) == 0

    def test_with_endpoint_one_sweep_delivery_zero_cell_deliveries(
        self,
        isolated_db: Path,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """``endpoint_id`` set ⇒ single delivery on sweep terminal, no cell deliveries."""
        conn = open_connection()
        try:
            endpoint_id = EndpointRepository(conn).create(
                kind="slack_webhook",
                name="sweep_e2e_ep",
                config={
                    "webhook_url": "https://hooks.slack.com/services/X/Y/Z",
                },
            )
        finally:
            conn.close()

        orch = _build_orchestrator(
            tmp_path,
            matrix={"lr": [0.001, 0.01, 0.1], "seed": [1, 2, 3]},
            fail_fast=False,
            max_parallel=3,
            endpoint_id=endpoint_id,
        )

        monkeypatch.setattr(
            SweepOrchestrator,
            "_run_cell_sync",
            lambda self, cell: _simulate_cell(cell.workflow_run_id),
        )

        sweep = orch.run()
        assert sweep.id is not None

        # No per-cell deliveries.
        assert _count_cell_deliveries(sweep.id) == 0

        # Exactly one sweep-level delivery: terminal-preset means only
        # the running→completed transition queues a send. The initial
        # pending→running sweep event is observed but not delivered for
        # preset='terminal'. A 9-cell happy path ends as ``completed``.
        conn2 = open_connection()
        try:
            # Locate the sweep subscription — the orchestrator created a
            # single watch + subscription for this sweep.
            sub_row = conn2.execute(
                """
                SELECT s.id
                FROM subscriptions s
                JOIN watches w ON w.id = s.watch_id
                WHERE w.kind = 'sweep_run' AND w.target_ref = ?
                """,
                (f"sweep_run:{sweep.id}",),
            ).fetchone()
            assert sub_row is not None, "sweep watch+subscription not created"
            sub_id = int(sub_row["id"])

            deliveries = DeliveryRepository(conn2).list_by_subscription(
                subscription_id=sub_id
            )
        finally:
            conn2.close()

        assert len(deliveries) == 1
        assert deliveries[0].endpoint_id == endpoint_id
