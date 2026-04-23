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
from srunx.client_protocol import WorkflowJobExecutorFactory
from srunx.db.connection import initialized_connection, transaction
from srunx.db.models import SweepRun, SweepSubmissionSource, WorkflowRunTriggeredBy
from srunx.db.repositories.base import now_iso
from srunx.db.repositories.sweep_runs import SweepRunRepository
from srunx.db.repositories.workflow_runs import WorkflowRunRepository
from srunx.exceptions import SweepExecutionError
from srunx.logging import get_logger
from srunx.notifications.service import NotificationService
from srunx.rendering import SubmissionRenderContext
from srunx.runtime.sweep import CellSpec, SweepSpec
from srunx.runtime.sweep.state_service import WorkflowRunStateService

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
        executor_factory: WorkflowJobExecutorFactory | None = None,
        submission_context: SubmissionRenderContext | None = None,
    ) -> None:
        self.workflow_yaml_path = workflow_yaml_path
        self.workflow_data = workflow_data
        self.args_override = args_override
        self.sweep_spec = sweep_spec
        self.submission_source: SweepSubmissionSource = submission_source
        self.callbacks = list(callbacks) if callbacks is not None else []
        self.endpoint_id = endpoint_id
        self.preset = preset
        # Optional executor factory forwarded verbatim to every per-cell
        # :class:`WorkflowRunner`. ``None`` (default) keeps the legacy CLI
        # behaviour of running cells against a local :class:`Slurm`
        # singleton; the Web dispatcher injects
        # :meth:`SlurmSSHExecutorPool.lease` here to route sweep cells
        # through the configured SSH adapter.
        self.executor_factory = executor_factory
        # Optional submission context forwarded to each cell's runner so
        # SSH-backed executors can translate local mount paths before
        # rendering. ``None`` (default) preserves local CLI semantics —
        # no mount translation, job ``work_dir`` / ``log_dir`` used
        # verbatim.
        self.submission_context = submission_context

        self._cancelled: bool = False
        self._sweep_run_id: int | None = None
        # Cells we've materialized; populated inside _materialize.
        self._cells: list[CellSpec] = []

    # ------------------------------------------------------------------
    # Expansion + materialization
    # ------------------------------------------------------------------

    def _expand_cells(self) -> list[dict[str, Any]]:
        """Cross-product matrix axes over base_args, return list of effective args."""
        from srunx.runtime.sweep.expand import expand_matrix

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
        # init+open via context manager: callers don't need to know about
        # migrations; apply_migrations is idempotent on the hot path.
        with initialized_connection() as conn:
            try:
                sweep_run_id = self._materialize_happy_path(conn, cells)
            except Exception as exc:
                # The enclosing BEGIN IMMEDIATE rolled back; now record a
                # failed audit row in a fresh TX so the sweep shows up
                # in listings even though none of its cells exist.
                self._record_materialize_failure(conn, exc)
                raise SweepExecutionError(f"sweep materialize failed: {exc!r}") from exc

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
        # submission_source ('cli'|'web'|'mcp') is a strict subset of
        # workflow_runs.triggered_by after the V4 CHECK widening, so the
        # value flows through unchanged — every origin records its true
        # identity on the child rows.
        triggered_by: WorkflowRunTriggeredBy = self.submission_source

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
                # Deliberately no per-cell workflow_run watch: sweep
                # cells don't populate ``workflow_run_jobs``, so the
                # active-watch poller would aggregate child-job status
                # over an empty set and pull a terminal cell back to
                # 'pending'. Orchestrator + WorkflowRunStateService
                # already drive cell status + sweep counters end-to-end;
                # sweep-level notifications use the sweep_run watch
                # created below.
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

    def materialize(self) -> int:
        """Expand + materialize cells synchronously; return ``sweep_run_id``.

        Separated from :meth:`arun` so HTTP callers can materialize
        inside the request (to obtain ``sweep_run_id``) and then spawn
        :meth:`arun_from_materialized` as a background task.
        """
        cells = self._expand_cells()
        return self._materialize(cells)

    async def arun_from_materialized(self, sweep_run_id: int) -> SweepRun:
        """Run the execution loop for an already-materialized sweep.

        Assumes :meth:`materialize` (or equivalent) populated
        ``self._cells`` and ``self._sweep_run_id``. Used by both
        :meth:`arun` (materialize + run in the same call) and the Web
        dispatcher (materialize synchronously, then spawn this as a
        background task).
        """
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

    async def arun(self) -> SweepRun:
        """Execute the sweep with bounded concurrency.

        Steps: expand → materialize → spawn N cells behind an
        ``anyio.Semaphore(min(max_parallel, cell_count))`` → return
        the final SweepRun row.
        """
        sweep_run_id = self.materialize()
        return await self.arun_from_materialized(sweep_run_id)

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
            # Close the drain/fail_fast TOCTOU race via a DB-level optimistic
            # lock: a concurrent ``_drain`` flips this cell's row to
            # ``cancelled`` atomically, so the ``pending → running`` UPDATE
            # either wins (we own the cell and may submit) or loses (drain
            # got there first — bail out instead of submitting to SLURM).
            # Runner's own pending→running flip downstream is already
            # idempotent (``_transition_workflow_run`` swallows ``False``).
            claimed = await anyio.to_thread.run_sync(
                partial(self._claim_cell_running, cell.workflow_run_id)
            )
            if not claimed:
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
                # If the runner raised before it could record a terminal
                # transition (e.g. ``from_yaml`` blew up during load), the
                # workflow_runs row is still pending/running. Record the
                # failure here so the cell doesn't stall and sweep
                # counters can converge.
                self._record_cell_failure(cell, error)
            self._on_cell_done(cell, sweep_run_id, final_status, error)

    def _claim_cell_running(self, workflow_run_id: int) -> bool:
        """Atomically flip a cell from ``pending`` to ``running``.

        Returns True iff the caller won the optimistic UPDATE (i.e. the
        row was still ``pending`` at claim time). A False result means a
        concurrent ``_drain`` already cancelled the cell — the caller
        must skip ``_run_cell_sync`` to avoid a wasted SLURM submission.
        """
        with initialized_connection() as conn:
            with transaction(conn, "IMMEDIATE"):
                return WorkflowRunStateService.update(
                    conn=conn,
                    workflow_run_id=workflow_run_id,
                    from_status="pending",
                    to_status="running",
                )

    def _record_cell_failure(self, cell: CellSpec, error: str) -> None:
        """Best-effort failed transition for a cell the runner never finalized.

        Tries ``pending → failed`` first (the typical case when
        ``from_yaml`` raises before ``run()`` starts). Falls back to
        ``running → failed`` when the runner had already flipped the row
        before raising. Swallows all exceptions so a DB hiccup here
        cannot mask the original cell failure.
        """
        try:
            with initialized_connection() as conn:
                for from_status in ("pending", "running"):
                    with transaction(conn, "IMMEDIATE"):
                        transitioned = WorkflowRunStateService.update(
                            conn=conn,
                            workflow_run_id=cell.workflow_run_id,
                            from_status=from_status,
                            to_status="failed",
                            error=error,
                            completed_at=now_iso(),
                        )
                    if transitioned:
                        return
        except Exception as exc:  # noqa: BLE001 — never mask the original failure
            logger.warning(
                f"sweep cell {cell.cell_index} "
                f"(workflow_run_id={cell.workflow_run_id}): failed to record "
                f"terminal failure transition: {exc!r}"
            )

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
            executor_factory=self.executor_factory,
            submission_context=self.submission_context,
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
            # Flip ``_cancelled`` BEFORE draining so any cell currently
            # blocked in the semaphore's ``acquire()`` sees the flag on
            # wake and bails out instead of starting new work.
            self._cancelled = True
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

        with initialized_connection() as conn:
            with transaction(conn, "IMMEDIATE"):
                SweepRunRepository(conn).request_cancel(sweep_run_id)
            self._drain(sweep_run_id)

    def _drain(self, sweep_run_id: int) -> None:
        """Atomically cancel every still-pending cell and adjust counters.

        Thin instance-method wrapper around :func:`drain_sweep_pending_cells`
        so fail-fast hooks and ``request_cancel`` can share the drain
        logic with out-of-process callers (e.g. the cancel endpoint).
        """
        drain_sweep_pending_cells(sweep_run_id)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _load_sweep(self, sweep_run_id: int) -> SweepRun:
        """Return the current ``SweepRun`` row, raising if it vanished."""
        with initialized_connection() as conn:
            sweep = SweepRunRepository(conn).get(sweep_run_id)
        if sweep is None:
            raise SweepExecutionError(
                f"sweep_run_id={sweep_run_id} disappeared from DB after materialize"
            )
        return sweep


def drain_sweep_pending_cells(sweep_run_id: int) -> int:
    """Cancel every still-pending cell for ``sweep_run_id`` and sync counters.

    Runs in a single IMMEDIATE TX on a fresh connection. After the drain
    it triggers the aggregator so the sweep can transition to its final
    status if every in-flight cell is already done. Returns the number
    of cells moved from ``pending`` to ``cancelled`` (0 when nothing was
    pending).

    This is the out-of-process drain used by the cancel endpoint when
    no in-process orchestrator is registered (crash-recovery path) and
    by :class:`SweepOrchestrator` itself via :meth:`SweepOrchestrator._drain`.
    """
    with initialized_connection() as conn:
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
            # Aggregator evaluate: if every in-flight cell is already
            # done, the sweep transitions to its final status in the
            # same TX.
            from srunx.runtime.sweep.aggregator import (
                evaluate_and_fire_sweep_status_event,
            )

            evaluate_and_fire_sweep_status_event(conn=conn, sweep_run_id=sweep_run_id)
    return k


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


__all__ = [
    "SweepOrchestrator",
    "drain_sweep_pending_cells",
    "get_active_orchestrator",
]
