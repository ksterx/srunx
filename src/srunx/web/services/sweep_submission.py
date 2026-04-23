"""Sweep workflow dispatch.

Drives the ``POST /api/workflows/{name}/run`` sweep branch — materializes
all matrix cells synchronously, then spawns the orchestrator as a
background task so the HTTP 202 returns immediately. The execution loop
lives in :func:`run_sweep_background`.

``SweepSpec`` and ``SweepOrchestrator`` are imported at the router-module
level (not here) because tests patch them via
``srunx.web.routers.workflows.SweepSpec`` / ``.SweepOrchestrator``;
:meth:`SweepSubmissionService.dispatch` accepts them as constructor
arguments so those patches flow through.
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any

import anyio
import yaml
from fastapi import HTTPException, Request

from srunx.common.exceptions import SweepExecutionError, WorkflowValidationError
from srunx.common.logging import get_logger
from srunx.runtime.workflow.runner import WorkflowRunner
from srunx.slurm.ssh import SlurmSSHAdapter
from srunx.slurm.ssh_executor import SlurmSSHExecutorPool

from ..schemas.workflows import WorkflowRunRequest
from ._submission_common import (
    build_submission_context,
    enforce_shell_script_roots,
    hold_workflow_mounts_web,
)

logger = get_logger(__name__)


async def run_sweep_background(
    orchestrator: Any,
    sweep_run_id: int,
    pool: SlurmSSHExecutorPool | None = None,
) -> None:
    """Background task body: drive already-materialized cells to completion.

    Exceptions are logged and swallowed — the sweep's status columns in
    the DB are authoritative, and the aggregator will converge the sweep
    to a terminal state even if this task crashes mid-flight.

    When a ``pool`` is supplied, every pooled SSH adapter is torn down
    after the orchestrator returns (success or crash) so a completed
    sweep never leaks SSH sessions against the cluster.
    """
    try:
        await orchestrator.arun_from_materialized(sweep_run_id)
    except Exception:  # noqa: BLE001
        logger.warning(
            "Background sweep task for sweep_run_id=%s raised",
            sweep_run_id,
            exc_info=True,
        )
    finally:
        if pool is not None:
            try:
                await anyio.to_thread.run_sync(pool.close)
            except Exception:  # noqa: BLE001 — pool cleanup is best-effort
                logger.warning(
                    "Failed to close SSH executor pool for sweep_run_id=%s",
                    sweep_run_id,
                    exc_info=True,
                )


class SweepSubmissionService:
    """Materialize + dispatch a sweep request.

    :param sweep_spec_cls: The :class:`~srunx.runtime.sweep.SweepSpec` class as
        named in the router module. Passed in so tests patching
        ``srunx.web.routers.workflows.SweepSpec`` affect materialization.
    :param orchestrator_cls: The :class:`~srunx.runtime.sweep.orchestrator.SweepOrchestrator`
        class. Same patchability rationale.
    :param profile_resolver: Zero-arg callable returning the active
        :class:`ServerProfile` (or ``None``).
    """

    def __init__(
        self,
        *,
        sweep_spec_cls: Any,
        orchestrator_cls: Any,
        profile_resolver: Callable[[], Any],
        workflow_runner_cls: Any = WorkflowRunner,
        executor_pool_cls: Any = SlurmSSHExecutorPool,
    ) -> None:
        self._sweep_spec_cls = sweep_spec_cls
        self._orchestrator_cls = orchestrator_cls
        self._profile_resolver = profile_resolver
        self._runner_cls = workflow_runner_cls
        self._executor_pool_cls = executor_pool_cls

    async def dispatch(
        self,
        *,
        yaml_path: Path,
        name: str,
        body: WorkflowRunRequest,
        request: Request,
        adapter: SlurmSSHAdapter,
        mount: str | None = None,
    ) -> dict[str, Any]:
        """Materialize synchronously + spawn orchestrator as a background task.

        Returns 202 as soon as the cells exist in the DB so HTTP
        clients don't block on the full sweep.
        """
        assert body.sweep is not None  # narrowed by caller

        # Read raw YAML once so the orchestrator can see base ``args``
        # and the workflow name.
        def _load_raw() -> dict[str, Any]:
            raw = yaml.safe_load(yaml_path.read_text(encoding="utf-8"))
            return raw if isinstance(raw, dict) else {}

        workflow_data = await anyio.to_thread.run_sync(_load_raw)

        try:
            sweep_spec = self._sweep_spec_cls(
                matrix=body.sweep.matrix,
                fail_fast=body.sweep.fail_fast,
                max_parallel=body.sweep.max_parallel,
            )
        except Exception as exc:  # noqa: BLE001 — Pydantic / value errors
            raise HTTPException(status_code=422, detail=str(exc)) from exc

        # C3 (security): apply the same ShellJob script-root guard the
        # non-sweep path runs, before any DB materialize side effect.
        profile = await anyio.to_thread.run_sync(self._profile_resolver)
        base_runner: WorkflowRunner | None = None
        if mount is not None:
            runner_cls = self._runner_cls
            try:
                base_runner = await anyio.to_thread.run_sync(
                    lambda: runner_cls.from_yaml(
                        yaml_path,
                        args_override=body.args_override or None,
                    )
                )
            except WorkflowValidationError as exc:
                raise HTTPException(status_code=422, detail=str(exc)) from exc
            except Exception as exc:  # noqa: BLE001 — YAML / Jinja errors
                raise HTTPException(status_code=422, detail=str(exc)) from exc
            assert base_runner is not None
            runner_for_guard = base_runner
            await anyio.to_thread.run_sync(
                lambda: enforce_shell_script_roots(
                    runner_for_guard.workflow,
                    mount,
                    profile,
                    profile_resolver=self._profile_resolver,
                )
            )

            # Workflow Phase 2 (#135 web parity): rsync each touched
            # mount **once** at dispatch time, even when the sweep
            # expands into many cells targeting the same mount.
            async with hold_workflow_mounts_web(
                runner_for_guard.workflow, runner_for_guard, sync_required=True
            ):
                pass

        endpoint_id: int | None = None
        if body.notify and body.endpoint_id is not None:
            endpoint_id = body.endpoint_id
        elif body.notify and body.endpoint_id is None:
            logger.warning("sweep run: notify=true with no endpoint_id; skipping")

        # Build a per-sweep SSH executor pool so each cell's runner
        # submits through the configured cluster adapter instead of
        # the local :class:`Slurm` client. ``self._executor_pool_cls``
        # lets tests patch
        # ``srunx.web.routers.workflows.SlurmSSHExecutorPool`` and have
        # that replacement reach this constructor call.
        pool_size = max(1, min(sweep_spec.max_parallel, 8))
        pool = self._executor_pool_cls(
            adapter.connection_spec,
            callbacks=[],
            size=pool_size,
        )

        submission_context = build_submission_context(mount, profile)

        orchestrator = self._orchestrator_cls(
            workflow_yaml_path=yaml_path,
            workflow_data={"name": name, **workflow_data},
            args_override=body.args_override or None,
            sweep_spec=sweep_spec,
            submission_source="web",
            endpoint_id=endpoint_id,
            preset=body.preset,
            executor_factory=pool.lease,
            submission_context=submission_context,
        )

        try:
            sweep_run_id = await anyio.to_thread.run_sync(orchestrator.materialize)
        except WorkflowValidationError as exc:
            pool.close()
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        except SweepExecutionError as exc:
            pool.close()
            raise HTTPException(status_code=500, detail=str(exc)) from exc
        except BaseException:
            pool.close()
            raise

        # Spawn the execution loop on the app's lifespan task group so
        # the HTTP request can return 202 immediately.
        task_group = getattr(request.app.state, "task_group", None)
        if task_group is not None:
            task_group.start_soon(
                run_sweep_background, orchestrator, sweep_run_id, pool
            )
        else:
            # Fallback for test harnesses that don't run lifespan.
            import asyncio

            pending = getattr(request.app.state, "background_tasks", None)
            if pending is None:
                pending = set()
                request.app.state.background_tasks = pending
            task = asyncio.create_task(
                run_sweep_background(orchestrator, sweep_run_id, pool)
            )
            pending.add(task)
            task.add_done_callback(pending.discard)

        # Read the freshly-materialized row so counters + status
        # reflect the DB state, not the orchestrator's pre-run view.
        from srunx.observability.storage.connection import open_connection as _open
        from srunx.observability.storage.repositories.sweep_runs import (
            SweepRunRepository,
        )

        def _load_sweep() -> Any:
            db_conn = _open()
            try:
                return SweepRunRepository(db_conn).get(sweep_run_id)
            finally:
                db_conn.close()

        sweep_row = await anyio.to_thread.run_sync(_load_sweep)
        return {
            "sweep_run_id": sweep_run_id,
            "status": sweep_row.status if sweep_row is not None else "pending",
            "cell_count": sweep_row.cell_count if sweep_row is not None else 0,
        }
