"""Crash-recovery reconciler for sweeps.

Scans the DB at Web lifespan startup (before the active-watch poller)
for sweeps stuck in ``pending`` / ``running`` with pending cells and
either (1) finalizes sweeps whose cells are all already terminal, or
(2) re-spawns :class:`SweepOrchestrator` against the pending cells.

See ``.claude/specs/workflow-parameter-sweep/design.md`` § Crash
Recovery and tasks 21-22.
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

import anyio

from srunx.db.connection import init_db, open_connection, transaction
from srunx.db.models import SweepRun
from srunx.db.repositories.sweep_runs import SweepRunRepository
from srunx.logging import get_logger
from srunx.sweep import CellSpec, SweepSpec
from srunx.sweep.aggregator import evaluate_and_fire_sweep_status_event
from srunx.sweep.orchestrator import SweepOrchestrator

logger = get_logger(__name__)


class SweepReconciler:
    """Best-effort resume / finalize pass for incomplete sweeps.

    Designed to run once at lifespan startup **before** the active-watch
    poller is scheduled so that the poller doesn't race the orchestrator
    on observation of running cells.
    """

    @classmethod
    def scan_and_resume(cls) -> None:
        """Walk incomplete sweeps and either finalize or resume them.

        Synchronous entry point used by the CLI startup path. Uses
        ``anyio.run`` internally to drive orchestrator resume tasks so
        the caller blocks until every resume task group has scheduled
        its pending cells.
        """
        sweeps = cls._load_incomplete_sweeps()

        for sweep in sweeps:
            if sweep.id is None:
                continue
            if sweep.status == "draining":
                # Cancel/drain already in flight. Let running cells finish
                # and let the aggregator finalize; never re-spawn.
                logger.info(
                    f"reconciler: sweep {sweep.id} is draining, skipping resume"
                )
                continue

            cls._reconcile_one(sweep.id)

    @classmethod
    async def scan_and_resume_async(cls) -> None:
        """Async twin of :meth:`scan_and_resume`.

        Used by the FastAPI lifespan so we don't spin up a nested
        ``anyio.run`` inside the already-running event loop. The DB
        bookkeeping steps still run synchronously (they're tiny, purely
        local sqlite3 reads) — only the orchestrator resume is awaited
        directly instead of going through ``anyio.run``.
        """
        sweeps = cls._load_incomplete_sweeps()

        for sweep in sweeps:
            if sweep.id is None:
                continue
            if sweep.status == "draining":
                logger.info(
                    f"reconciler: sweep {sweep.id} is draining, skipping resume"
                )
                continue

            await cls._reconcile_one_async(sweep.id)

    @classmethod
    def _load_incomplete_sweeps(cls) -> list[SweepRun]:
        """Load every sweep still in an incomplete status (pending/running/draining)."""
        init_db(delete_legacy=True)
        conn = open_connection()
        try:
            return SweepRunRepository(conn).list_incomplete()
        finally:
            conn.close()

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    @classmethod
    def _reconcile_one(cls, sweep_run_id: int) -> None:
        plan = cls._prepare_reconcile_plan(sweep_run_id)
        if plan is None:
            return
        sweep_row, pending = plan
        cls._spawn_resume(sweep_run_id, sweep_row, pending)

    @classmethod
    async def _reconcile_one_async(cls, sweep_run_id: int) -> None:
        plan = cls._prepare_reconcile_plan(sweep_run_id)
        if plan is None:
            return
        sweep_row, pending = plan
        await cls._spawn_resume_async(sweep_run_id, sweep_row, pending)

    @classmethod
    def _prepare_reconcile_plan(
        cls, sweep_run_id: int
    ) -> tuple[sqlite3.Row, list[CellSpec]] | None:
        """Return ``(sweep_row, pending_cells)`` when resume is required.

        Returns ``None`` when the reconciler either has nothing to do
        (already finalized) or decided to wait (no headroom). Side-effect
        cases (aggregator evaluate, info logs) are handled inline so the
        caller only has to act on the happy path.
        """
        init_db(delete_legacy=True)
        conn = open_connection()
        try:
            running = _count_cells(conn, sweep_run_id, "running")
            pending = _list_pending_cells(conn, sweep_run_id)
            sweep_row = conn.execute(
                "SELECT max_parallel, fail_fast, name, workflow_yaml_path "
                "FROM sweep_runs WHERE id = ?",
                (sweep_run_id,),
            ).fetchone()
            if sweep_row is None:
                return None

            if not pending and running == 0:
                with transaction(conn, "IMMEDIATE"):
                    evaluate_and_fire_sweep_status_event(
                        conn=conn, sweep_run_id=sweep_run_id
                    )
                logger.info(
                    f"reconciler: sweep {sweep_run_id} all cells terminal, "
                    "forced aggregator evaluate"
                )
                return None

            headroom = max(0, int(sweep_row["max_parallel"]) - running)
            if headroom <= 0 or not pending:
                logger.info(
                    f"reconciler: sweep {sweep_run_id} has "
                    f"{len(pending)} pending / {running} running, no headroom "
                    "to resume"
                )
                return None

            logger.info(
                f"reconciler: sweep {sweep_run_id} resuming with "
                f"{len(pending)} pending cells, headroom={headroom}"
            )
        finally:
            conn.close()

        return sweep_row, pending

    @classmethod
    def _spawn_resume(
        cls,
        sweep_run_id: int,
        sweep_row: sqlite3.Row,
        pending_cells: list[CellSpec],
    ) -> None:
        """Drive the resume through a fresh :class:`SweepOrchestrator`.

        Runs the orchestrator's ``resume_from_db`` via ``anyio.run`` so the
        lifespan-startup caller (synchronous) blocks until the resume
        task group has scheduled every pending cell.
        """
        orch = cls._build_orchestrator(sweep_run_id, sweep_row)
        if orch is None:
            return

        async def _resume() -> None:
            await orch.resume_from_db(sweep_run_id, pending_cells)

        try:
            anyio.run(_resume)
        except Exception as exc:  # noqa: BLE001
            logger.warning(f"reconciler: sweep {sweep_run_id} resume raised: {exc!r}")

    @classmethod
    async def _spawn_resume_async(
        cls,
        sweep_run_id: int,
        sweep_row: sqlite3.Row,
        pending_cells: list[CellSpec],
    ) -> None:
        """Async twin of :meth:`_spawn_resume`.

        Awaits the orchestrator's ``resume_from_db`` directly inside the
        caller's event loop (FastAPI lifespan). Cell execution still hops
        to a worker thread per :meth:`SweepOrchestrator._run_cell`, so
        the lifespan task group stays responsive while cells run.
        """
        orch = cls._build_orchestrator(sweep_run_id, sweep_row)
        if orch is None:
            return

        try:
            await orch.resume_from_db(sweep_run_id, pending_cells)
        except Exception as exc:  # noqa: BLE001
            logger.warning(f"reconciler: sweep {sweep_run_id} resume raised: {exc!r}")

    @classmethod
    def _build_orchestrator(
        cls,
        sweep_run_id: int,
        sweep_row: sqlite3.Row,
    ) -> SweepOrchestrator | None:
        """Rebuild the minimal SweepOrchestrator needed for a resume."""
        yaml_path = sweep_row["workflow_yaml_path"]
        if yaml_path is None:
            logger.warning(
                f"reconciler: sweep {sweep_run_id} has no workflow_yaml_path; "
                "cannot resume"
            )
            return None

        # Rebuild a minimal SweepSpec (matrix is unused on the resume
        # path; the orchestrator only needs fail_fast + max_parallel).
        sweep_spec = SweepSpec(
            matrix={"_axis": [0]},
            fail_fast=bool(sweep_row["fail_fast"]),
            max_parallel=int(sweep_row["max_parallel"]),
        )

        return SweepOrchestrator(
            workflow_yaml_path=Path(yaml_path),
            workflow_data={"name": sweep_row["name"]},
            args_override=None,
            sweep_spec=sweep_spec,
            submission_source="cli",
        )


def _count_cells(conn: sqlite3.Connection, sweep_run_id: int, status: str) -> int:
    row = conn.execute(
        "SELECT COUNT(*) FROM workflow_runs WHERE sweep_run_id = ? AND status = ?",
        (sweep_run_id, status),
    ).fetchone()
    return int(row[0] if row is not None else 0)


def _list_pending_cells(conn: sqlite3.Connection, sweep_run_id: int) -> list[CellSpec]:
    rows = conn.execute(
        "SELECT id, args FROM workflow_runs "
        "WHERE sweep_run_id = ? AND status = 'pending' "
        "ORDER BY id ASC",
        (sweep_run_id,),
    ).fetchall()
    cells: list[CellSpec] = []
    for index, row in enumerate(rows):
        args_payload: dict[str, Any] = {}
        raw = row["args"]
        if raw:
            try:
                parsed = json.loads(raw)
                if isinstance(parsed, dict):
                    args_payload = parsed
            except (TypeError, ValueError):
                args_payload = {}
        cells.append(
            CellSpec(
                workflow_run_id=int(row["id"]),
                effective_args=args_payload,
                cell_index=index,
            )
        )
    return cells


__all__ = ["SweepReconciler"]
