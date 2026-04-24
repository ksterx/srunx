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
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import anyio

from srunx.common.logging import get_logger
from srunx.observability.storage.connection import initialized_connection, transaction
from srunx.observability.storage.models import SweepRun
from srunx.observability.storage.repositories.sweep_runs import SweepRunRepository
from srunx.runtime.rendering import SubmissionRenderContext
from srunx.runtime.sweep import CellSpec, SweepSpec
from srunx.runtime.sweep.aggregator import evaluate_and_fire_sweep_status_event
from srunx.runtime.sweep.orchestrator import SweepOrchestrator
from srunx.slurm.protocols import WorkflowJobExecutorFactory

logger = get_logger(__name__)


@dataclass(frozen=True)
class ExecutorFactoryBundle:
    """Per-sweep executor wiring returned by a reconciler provider.

    The bundle keeps the three cross-cutting injections the orchestrator
    needs to behave identically to the originally-submitting process
    (before the crash / restart):

    - ``factory``: passed to each cell's ``WorkflowRunner`` so cells
      submit through the correct executor (e.g. an SSH pool lease for
      Web-originated sweeps instead of the local ``Slurm`` singleton).
    - ``submission_context``: mount-aware render context for path
      translation (the Web dispatcher builds this from the active
      profile's selected mount).
    - ``cleanup``: optional finalizer invoked after the sweep resume
      returns (success or crash). Used by Web to close a per-sweep
      SSH pool; ``None`` means "nothing to clean up".
    """

    factory: WorkflowJobExecutorFactory
    submission_context: SubmissionRenderContext | None = None
    cleanup: Callable[[], None] | None = None


# Provider signature: given a :class:`SweepRun`, return a bundle to wire
# into the resumed orchestrator, or ``None`` to keep the legacy local
# ``Slurm`` behaviour (CLI-originated sweeps).
ExecutorFactoryProvider = Callable[[SweepRun], ExecutorFactoryBundle | None]


class SweepReconciler:
    """Best-effort resume / finalize pass for incomplete sweeps.

    Designed to run once at lifespan startup **before** the active-watch
    poller is scheduled so that the poller doesn't race the orchestrator
    on observation of running cells.
    """

    @classmethod
    def scan_and_resume(
        cls,
        *,
        executor_factory_provider: ExecutorFactoryProvider | None = None,
    ) -> None:
        """Walk incomplete sweeps and either finalize or resume them.

        Synchronous entry point used by the CLI startup path. Uses
        ``anyio.run`` internally to drive orchestrator resume tasks so
        the caller blocks until every resume task group has scheduled
        its pending cells.

        ``executor_factory_provider`` is invoked once per resumed sweep
        with the :class:`SweepRun` row; returning a bundle replaces the
        orchestrator's default local-``Slurm`` execution with the
        provided factory + context (and arranges for cleanup after the
        resume completes). ``None`` (default) preserves the CLI-path
        behaviour and is the expected value for the synchronous entry
        point.
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

            cls._reconcile_one(
                sweep,
                executor_factory_provider=executor_factory_provider,
            )

    @classmethod
    async def scan_and_resume_async(
        cls,
        *,
        executor_factory_provider: ExecutorFactoryProvider | None = None,
    ) -> None:
        """Async twin of :meth:`scan_and_resume`.

        Used by the FastAPI lifespan so we don't spin up a nested
        ``anyio.run`` inside the already-running event loop. The DB
        bookkeeping steps still run synchronously (they're tiny, purely
        local sqlite3 reads) — only the orchestrator resume is awaited
        directly instead of going through ``anyio.run``.

        ``executor_factory_provider`` is supplied by the Web lifespan so
        sweeps originally submitted via the Web API (``submission_source
        ∈ {'web','mcp'}``) resume through the configured SSH adapter
        instead of the local ``Slurm`` singleton. See
        :class:`ExecutorFactoryBundle` for the wiring contract.
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

            await cls._reconcile_one_async(
                sweep,
                executor_factory_provider=executor_factory_provider,
            )

    @classmethod
    def _load_incomplete_sweeps(cls) -> list[SweepRun]:
        """Load every sweep still in an incomplete status (pending/running/draining)."""
        with initialized_connection() as conn:
            return SweepRunRepository(conn).list_incomplete()

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    @classmethod
    def _reconcile_one(
        cls,
        sweep: SweepRun,
        *,
        executor_factory_provider: ExecutorFactoryProvider | None = None,
    ) -> None:
        assert sweep.id is not None  # caller filtered ``sweep.id is None``
        plan = cls._prepare_reconcile_plan(sweep.id)
        if plan is None:
            return
        sweep_row, pending = plan
        cls._spawn_resume(
            sweep.id,
            sweep_row,
            pending,
            sweep=sweep,
            executor_factory_provider=executor_factory_provider,
        )

    @classmethod
    async def _reconcile_one_async(
        cls,
        sweep: SweepRun,
        *,
        executor_factory_provider: ExecutorFactoryProvider | None = None,
    ) -> None:
        assert sweep.id is not None  # caller filtered ``sweep.id is None``
        plan = cls._prepare_reconcile_plan(sweep.id)
        if plan is None:
            return
        sweep_row, pending = plan
        await cls._spawn_resume_async(
            sweep.id,
            sweep_row,
            pending,
            sweep=sweep,
            executor_factory_provider=executor_factory_provider,
        )

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
        with initialized_connection() as conn:
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

        return sweep_row, pending

    @classmethod
    def _spawn_resume(
        cls,
        sweep_run_id: int,
        sweep_row: sqlite3.Row,
        pending_cells: list[CellSpec],
        *,
        sweep: SweepRun,
        executor_factory_provider: ExecutorFactoryProvider | None = None,
    ) -> None:
        """Drive the resume through a fresh :class:`SweepOrchestrator`.

        Runs the orchestrator's ``resume_from_db`` via ``anyio.run`` so the
        lifespan-startup caller (synchronous) blocks until the resume
        task group has scheduled every pending cell.
        """
        bundle = cls._resolve_bundle(sweep, executor_factory_provider)
        try:
            try:
                orch = cls._build_orchestrator(sweep_run_id, sweep_row, sweep, bundle)
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    f"reconciler: sweep {sweep_run_id} orchestrator "
                    f"construction failed: {exc!r}"
                )
                return
            if orch is None:
                return

            async def _resume() -> None:
                await orch.resume_from_db(sweep_run_id, pending_cells)

            try:
                anyio.run(_resume)
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    f"reconciler: sweep {sweep_run_id} resume raised: {exc!r}"
                )
        finally:
            cls._run_bundle_cleanup(sweep_run_id, bundle)

    @classmethod
    async def _spawn_resume_async(
        cls,
        sweep_run_id: int,
        sweep_row: sqlite3.Row,
        pending_cells: list[CellSpec],
        *,
        sweep: SweepRun,
        executor_factory_provider: ExecutorFactoryProvider | None = None,
    ) -> None:
        """Async twin of :meth:`_spawn_resume`.

        Awaits the orchestrator's ``resume_from_db`` directly inside the
        caller's event loop (FastAPI lifespan). Cell execution still hops
        to a worker thread per :meth:`SweepOrchestrator._run_cell`, so
        the lifespan task group stays responsive while cells run.
        """
        bundle = cls._resolve_bundle(sweep, executor_factory_provider)
        try:
            try:
                orch = cls._build_orchestrator(sweep_run_id, sweep_row, sweep, bundle)
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    f"reconciler: sweep {sweep_run_id} orchestrator "
                    f"construction failed: {exc!r}"
                )
                return
            if orch is None:
                return

            try:
                await orch.resume_from_db(sweep_run_id, pending_cells)
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    f"reconciler: sweep {sweep_run_id} resume raised: {exc!r}"
                )
        finally:
            await cls._run_bundle_cleanup_async(sweep_run_id, bundle)

    @classmethod
    def _resolve_bundle(
        cls,
        sweep: SweepRun,
        executor_factory_provider: ExecutorFactoryProvider | None,
    ) -> ExecutorFactoryBundle | None:
        """Invoke the provider (if any) and swallow provider exceptions.

        A provider that raises must not take the whole startup pass down
        — fall back to the CLI-style ``None`` bundle so resume still
        happens through the local ``Slurm`` client.
        """
        if executor_factory_provider is None:
            return None
        try:
            return executor_factory_provider(sweep)
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                f"reconciler: executor_factory_provider raised for sweep "
                f"{sweep.id} ({sweep.submission_source!r}); falling back to "
                f"local Slurm: {exc!r}"
            )
            return None

    @classmethod
    def _run_bundle_cleanup(
        cls, sweep_run_id: int, bundle: ExecutorFactoryBundle | None
    ) -> None:
        if bundle is None or bundle.cleanup is None:
            return
        try:
            bundle.cleanup()
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                f"reconciler: sweep {sweep_run_id} bundle cleanup raised: {exc!r}"
            )

    @classmethod
    async def _run_bundle_cleanup_async(
        cls, sweep_run_id: int, bundle: ExecutorFactoryBundle | None
    ) -> None:
        if bundle is None or bundle.cleanup is None:
            return
        try:
            await anyio.to_thread.run_sync(bundle.cleanup)
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                f"reconciler: sweep {sweep_run_id} bundle cleanup raised: {exc!r}"
            )

    @classmethod
    def _build_orchestrator(
        cls,
        sweep_run_id: int,
        sweep_row: sqlite3.Row,
        sweep: SweepRun,
        bundle: ExecutorFactoryBundle | None,
    ) -> SweepOrchestrator | None:
        """Rebuild the minimal SweepOrchestrator needed for a resume.

        ``submission_source`` is copied from the DB row (no more
        hard-coded ``"cli"``) so resumed sweeps keep their audit trail
        intact — Web-originated sweeps stay ``"web"``, MCP ``"mcp"``,
        and CLI ``"cli"``. ``bundle`` optionally plugs the Web's SSH
        pool lease + mount-aware submission context into each cell's
        ``WorkflowRunner``; passing ``None`` falls back to the local
        ``Slurm`` executor (the CLI / no-provider case).
        """
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

        executor_factory = bundle.factory if bundle is not None else None
        submission_context = bundle.submission_context if bundle is not None else None

        return SweepOrchestrator(
            workflow_yaml_path=Path(yaml_path),
            workflow_data={"name": sweep_row["name"]},
            args_override=None,
            sweep_spec=sweep_spec,
            submission_source=sweep.submission_source,
            executor_factory=executor_factory,
            submission_context=submission_context,
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


__all__ = ["ExecutorFactoryBundle", "ExecutorFactoryProvider", "SweepReconciler"]
