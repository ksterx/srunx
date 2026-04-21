"""SweepOrchestrator: materialize matrix cells and drive them under a semaphore.

See ``.claude/specs/workflow-parameter-sweep/design.md`` § SweepOrchestrator
and tasks 17-20.
"""

from __future__ import annotations

import sqlite3
import threading
from collections.abc import Sequence
from functools import partial
from pathlib import Path
from typing import Any, Literal

import anyio

from srunx.callbacks import Callback
from srunx.db.connection import init_db, open_connection, transaction
from srunx.db.models import SweepRun, SweepSubmissionSource, WorkflowRunTriggeredBy
from srunx.db.repositories.base import now_iso
from srunx.db.repositories.sweep_runs import SweepRunRepository
from srunx.db.repositories.workflow_runs import WorkflowRunRepository
from srunx.exceptions import SweepExecutionError
from srunx.logging import get_logger
from srunx.notifications.service import NotificationService
from srunx.sweep import CellSpec, SweepSpec

# In-process registry of live orchestrators keyed by ``sweep_run_id``.
# The cancel endpoint (``POST /api/sweep_runs/{id}/cancel``) looks up
# the running orchestrator here so :meth:`request_cancel` can drain the
# task group immediately. Crash-recovery path (no in-proc entry) falls
# back to a DB-only ``cancel_requested_at`` stamp — the reconciler +
# aggregator observe it on the next cycle.
_ACTIVE_ORCHESTRATORS: dict[int, SweepOrchestrator] = {}
_ACTIVE_LOCK = threading.Lock()


def get_active_orchestrator(sweep_run_id: int) -> SweepOrchestrator | None:
    """Return the live orchestrator for ``sweep_run_id`` or ``None``."""
    with _ACTIVE_LOCK:
        return _ACTIVE_ORCHESTRATORS.get(sweep_run_id)


logger = get_logger(__name__)

# Map the orchestrator-level submission_source to the child workflow_runs
# triggered_by CHECK-constraint value. MCP is recorded as 'web' in the
# child (see design.md § submission_source/triggered_by 対応表) because
# the v1 CHECK allowlist is ('cli','web','schedule').
_TRIGGERED_BY_BY_SOURCE: dict[Literal["cli", "web", "mcp"], WorkflowRunTriggeredBy] = {
    "cli": "cli",
    "web": "web",
    "mcp": "web",
}


class SweepOrchestrator:
    """Drive sweep execution: materialize cells, run them, aggregate status."""

    def __init__(
        self,
        *,
        workflow_yaml_path: Path | None,
        workflow_data: dict[str, Any],
        args_override: dict[str, Any] | None,
        sweep_spec: SweepSpec,
        submission_source: Literal["cli", "web", "mcp"],
        callbacks: Sequence[Callback] | None = None,
        endpoint_id: int | None = None,
        preset: str = "terminal",
    ) -> None:
        self.workflow_yaml_path = workflow_yaml_path
        self.workflow_data = workflow_data
        self.args_override = args_override
        self.sweep_spec = sweep_spec
        self.submission_source: SweepSubmissionSource = submission_source
        self.callbacks = list(callbacks) if callbacks is not None else []
        self.endpoint_id = endpoint_id
        self.preset = preset

        self._cancelled: bool = False
        self._sweep_run_id: int | None = None
        # Cells we've materialized; populated inside _materialize.
        self._cells: list[CellSpec] = []

    # ------------------------------------------------------------------
    # Expansion + materialization
    # ------------------------------------------------------------------

    def _expand_cells(self) -> list[dict[str, Any]]:
        """Cross-product matrix axes over base_args, return list of effective args."""
        from srunx.sweep.expand import expand_matrix

        base_args: dict[str, Any] = {
            **(self.workflow_data.get("args") or {}),
            **(self.args_override or {}),
        }
        return expand_matrix(self.sweep_spec.matrix, base_args)

    def _materialize(self, cells: list[dict[str, Any]]) -> int:
        """Atomically create sweep_runs, N workflow_runs, and watches.

        On any DB failure the happy-path TX is rolled back and a
        separate audit row (``status='failed'``, ``cell_count=0``) is
        inserted so the sweep remains visible in the UI (R4.7). Raises
        :class:`SweepExecutionError` after the audit row is written.
        """
        init_db(delete_legacy=True)
        conn = open_connection()
        try:
            try:
                sweep_run_id = self._materialize_happy_path(conn, cells)
            except Exception as exc:
                # The enclosing BEGIN IMMEDIATE rolled back; now record a
                # failed audit row in a fresh TX so the sweep shows up
                # in listings even though none of its cells exist.
                self._record_materialize_failure(conn, exc)
                raise SweepExecutionError(f"sweep materialize failed: {exc!r}") from exc
        finally:
            conn.close()

        self._sweep_run_id = sweep_run_id
        return sweep_run_id

    def _materialize_happy_path(
        self,
        conn: sqlite3.Connection,
        cells: list[dict[str, Any]],
    ) -> int:
        """Insert sweep + cells + watches inside one BEGIN IMMEDIATE TX."""
        matrix_keys = set(self.sweep_spec.matrix.keys())
        base_args: dict[str, Any] = {
            **(self.workflow_data.get("args") or {}),
            **(self.args_override or {}),
        }
        # Strip matrix-axis keys so sweep_runs.args reflects only the
        # non-matrix inputs; per-cell effective args live on workflow_runs.
        sweep_args: dict[str, Any] = {
            k: v for k, v in base_args.items() if k not in matrix_keys
        }

        name = str(self.workflow_data.get("name") or "unnamed")
        yaml_path_str = (
            str(self.workflow_yaml_path)
            if self.workflow_yaml_path is not None
            else None
        )
        triggered_by = _TRIGGERED_BY_BY_SOURCE[self.submission_source]

        sweep_repo = SweepRunRepository(conn)
        wr_repo = WorkflowRunRepository(conn)
        notifier = _build_notification_service(conn)

        cell_specs: list[CellSpec] = []

        with transaction(conn, "IMMEDIATE"):
            sweep_run_id = sweep_repo.create(
                name=name,
                workflow_yaml_path=yaml_path_str,
                matrix=self.sweep_spec.matrix,
                args=sweep_args or None,
                fail_fast=self.sweep_spec.fail_fast,
                max_parallel=self.sweep_spec.max_parallel,
                cell_count=len(cells),
                submission_source=self.submission_source,
            )

            for index, effective in enumerate(cells):
                workflow_run_id = wr_repo.create(
                    workflow_name=name,
                    yaml_path=yaml_path_str,
                    args=effective,
                    triggered_by=triggered_by,
                    sweep_run_id=sweep_run_id,
                )
                notifier.create_watch_for_workflow_run(
                    run_id=workflow_run_id,
                    endpoint_id=None,
                    preset=None,
                )
                cell_specs.append(
                    CellSpec(
                        workflow_run_id=workflow_run_id,
                        effective_args=effective,
                        cell_index=index,
                    )
                )

            if self.endpoint_id is not None:
                notifier.create_watch_for_sweep_run(
                    sweep_run_id=sweep_run_id,
                    endpoint_id=self.endpoint_id,
                    preset=self.preset,
                )

        self._cells = cell_specs
        return sweep_run_id

    def _record_materialize_failure(
        self, conn: sqlite3.Connection, exc: BaseException
    ) -> None:
        """Insert a ``sweep_runs`` row with ``status='failed'`` after rollback.

        The original TX was rolled back by ``transaction(...)``'s
        ``__exit__`` so the happy-path rows are gone. A fresh IMMEDIATE
        TX writes a single audit row. Any failure here is swallowed to
        avoid masking the primary exception.
        """
        try:
            repo = SweepRunRepository(conn)
            name = str(self.workflow_data.get("name") or "unnamed")
            yaml_path_str = (
                str(self.workflow_yaml_path)
                if self.workflow_yaml_path is not None
                else None
            )
            with transaction(conn, "IMMEDIATE"):
                repo.create(
                    name=name,
                    workflow_yaml_path=yaml_path_str,
                    matrix=self.sweep_spec.matrix,
                    args=None,
                    fail_fast=self.sweep_spec.fail_fast,
                    max_parallel=self.sweep_spec.max_parallel,
                    cell_count=0,
                    submission_source=self.submission_source,
                    status="failed",
                    cells_pending=0,
                    error=repr(exc),
                )
        except Exception as audit_exc:  # noqa: BLE001
            logger.warning(
                f"failed to record sweep materialize failure audit row: {audit_exc!r}"
            )

    # ------------------------------------------------------------------
    # Execution
    # ------------------------------------------------------------------

    def run(self) -> SweepRun:
        """Execute the sweep synchronously and return the final SweepRun."""
        return anyio.run(self.arun)

    async def arun(self) -> SweepRun:
        """Execute the sweep with bounded concurrency.

        Steps: expand → materialize → spawn N cells behind an
        ``anyio.Semaphore(min(max_parallel, cell_count))`` → return
        the final SweepRun row.
        """
        cells = self._expand_cells()
        sweep_run_id = self._materialize(cells)

        if not self._cells:
            # No cells (shouldn't happen since expand_matrix rejects
            # empty matrices, but guard defensively).
            return self._load_sweep(sweep_run_id)

        effective_parallel = min(self.sweep_spec.max_parallel, len(self._cells))
        if effective_parallel < self.sweep_spec.max_parallel:
            logger.warning(
                f"sweep max_parallel={self.sweep_spec.max_parallel} clamped to "
                f"effective_parallel={effective_parallel} (cell_count={len(self._cells)})"
            )

        self._register_active(sweep_run_id)
        try:
            sem = anyio.Semaphore(effective_parallel)
            async with anyio.create_task_group() as tg:
                for cell in self._cells:
                    if self._cancelled:
                        break
                    tg.start_soon(self._run_cell, sem, cell, sweep_run_id)
        finally:
            self._unregister_active(sweep_run_id)

        return self._load_sweep(sweep_run_id)

    async def resume_from_db(
        self,
        sweep_run_id: int,
        pending_cells: list[CellSpec],
    ) -> SweepRun:
        """Resume a sweep whose cells were already materialized.

        Used by :class:`srunx.sweep.reconciler.SweepReconciler` to
        spawn orchestrator tasks after a crash. Skips expand +
        materialize; the caller provides the already-materialized
        pending cells.
        """
        self._sweep_run_id = sweep_run_id
        self._cells = list(pending_cells)

        if not self._cells:
            return self._load_sweep(sweep_run_id)

        effective_parallel = min(self.sweep_spec.max_parallel, len(self._cells))
        self._register_active(sweep_run_id)
        try:
            sem = anyio.Semaphore(effective_parallel)
            async with anyio.create_task_group() as tg:
                for cell in self._cells:
                    if self._cancelled:
                        break
                    tg.start_soon(self._run_cell, sem, cell, sweep_run_id)
        finally:
            self._unregister_active(sweep_run_id)

        return self._load_sweep(sweep_run_id)

    def _register_active(self, sweep_run_id: int) -> None:
        with _ACTIVE_LOCK:
            _ACTIVE_ORCHESTRATORS[sweep_run_id] = self

    def _unregister_active(self, sweep_run_id: int) -> None:
        with _ACTIVE_LOCK:
            _ACTIVE_ORCHESTRATORS.pop(sweep_run_id, None)

    async def _run_cell(
        self,
        sem: anyio.Semaphore,
        cell: CellSpec,
        sweep_run_id: int,
    ) -> None:
        async with sem:
            if self._cancelled:
                return
            final_status: str = "failed"
            error: str | None = None
            try:
                await anyio.to_thread.run_sync(partial(self._run_cell_sync, cell))
                final_status = "completed"
            except Exception as exc:  # noqa: BLE001 — cell failures must not propagate
                final_status = "failed"
                error = repr(exc)
                logger.warning(
                    f"sweep cell {cell.cell_index} "
                    f"(workflow_run_id={cell.workflow_run_id}) failed: {exc!r}"
                )
            self._on_cell_done(cell, sweep_run_id, final_status, error)

    def _run_cell_sync(self, cell: CellSpec) -> None:
        """Drive one cell through :class:`WorkflowRunner`.

        The runner's ``from_yaml(..., args_override=cell.effective_args)``
        produces a per-cell :class:`WorkflowRunner`, and ``run(workflow_run_id=...)``
        attaches the execution to the pre-materialized ``workflow_runs`` row.
        Runner-internal state transitions (pending→running→completed/failed)
        flow through :class:`WorkflowRunStateService` and fire sweep
        aggregation events automatically.
        """
        from srunx.runner import WorkflowRunner

        if self.workflow_yaml_path is None:
            raise SweepExecutionError(
                "SweepOrchestrator._run_cell_sync requires workflow_yaml_path "
                "(in-memory workflow execution not supported in Phase 1)"
            )

        runner = WorkflowRunner.from_yaml(
            self.workflow_yaml_path,
            callbacks=self.callbacks,
            args_override=cell.effective_args,
        )
        runner.run(workflow_run_id=cell.workflow_run_id)

    def _on_cell_done(
        self,
        cell: CellSpec,
        sweep_run_id: int,
        final_status: str,
        error: str | None,
    ) -> None:
        """Fail-fast drain hook.

        Runner-internal status transitions are already routed through
        :class:`WorkflowRunStateService`, so we never need to re-record
        the cell's terminal status here. The only job of this method is
        to decide whether a failure should trigger a fail-fast drain.
        """
        if self.sweep_spec.fail_fast and final_status in {"failed", "timeout"}:
            self._drain(sweep_run_id)

    # ------------------------------------------------------------------
    # Cancellation / drain
    # ------------------------------------------------------------------

    def request_cancel(self) -> None:
        """Mark the sweep as cancel-requested and drain pending cells.

        Idempotent: a second call is a no-op because ``_cancelled`` is
        set and ``SweepRunRepository.request_cancel`` guards on
        ``cancel_requested_at IS NULL``.
        """
        self._cancelled = True
        sweep_run_id = self._sweep_run_id
        if sweep_run_id is None:
            # Cancel before materialize: nothing to drain, but flip the
            # flag so an in-progress arun() doesn't start new cells.
            return

        init_db(delete_legacy=True)
        conn = open_connection()
        try:
            with transaction(conn, "IMMEDIATE"):
                SweepRunRepository(conn).request_cancel(sweep_run_id)
            self._drain(sweep_run_id)
        finally:
            conn.close()

    def _drain(self, sweep_run_id: int) -> None:
        """Atomically cancel every still-pending cell and adjust counters.

        Runs in a single IMMEDIATE TX on a fresh connection so the
        caller (either the fail-fast hook from ``_on_cell_done`` or
        ``request_cancel``) doesn't have to thread a connection through.
        After the drain we trigger the aggregator to roll the sweep
        toward its target terminal status if all in-flight cells are
        already done.
        """
        init_db(delete_legacy=True)
        conn = open_connection()
        try:
            with transaction(conn, "IMMEDIATE"):
                cur = conn.execute(
                    "UPDATE workflow_runs "
                    "SET status = 'cancelled', completed_at = ? "
                    "WHERE sweep_run_id = ? AND status = 'pending'",
                    (now_iso(), sweep_run_id),
                )
                k = cur.rowcount or 0
                if k > 0:
                    conn.execute(
                        "UPDATE sweep_runs "
                        "SET cells_pending = cells_pending - ?, "
                        "    cells_cancelled = cells_cancelled + ?, "
                        "    status = 'draining' "
                        "WHERE id = ? AND status IN ('pending','running')",
                        (k, k, sweep_run_id),
                    )
                # Let the aggregator evaluate terminal status: if every
                # in-flight cell is already done, the sweep transitions
                # to its final status in the same TX.
                from srunx.sweep.aggregator import (
                    evaluate_and_fire_sweep_status_event,
                )

                evaluate_and_fire_sweep_status_event(
                    conn=conn, sweep_run_id=sweep_run_id
                )
        finally:
            conn.close()

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _load_sweep(self, sweep_run_id: int) -> SweepRun:
        """Return the current ``SweepRun`` row, raising if it vanished."""
        init_db(delete_legacy=True)
        conn = open_connection()
        try:
            sweep = SweepRunRepository(conn).get(sweep_run_id)
        finally:
            conn.close()
        if sweep is None:
            raise SweepExecutionError(
                f"sweep_run_id={sweep_run_id} disappeared from DB after materialize"
            )
        return sweep


def _build_notification_service(conn: sqlite3.Connection) -> NotificationService:
    """Construct a :class:`NotificationService` bound to ``conn``."""
    from srunx.db.repositories.deliveries import DeliveryRepository
    from srunx.db.repositories.endpoints import EndpointRepository
    from srunx.db.repositories.events import EventRepository
    from srunx.db.repositories.subscriptions import SubscriptionRepository
    from srunx.db.repositories.watches import WatchRepository

    return NotificationService(
        watch_repo=WatchRepository(conn),
        subscription_repo=SubscriptionRepository(conn),
        event_repo=EventRepository(conn),
        delivery_repo=DeliveryRepository(conn),
        endpoint_repo=EndpointRepository(conn),
    )


__all__ = ["SweepOrchestrator", "get_active_orchestrator"]
