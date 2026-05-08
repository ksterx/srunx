"""Workflow management endpoints: /api/workflows/*

Workflow runs are persisted in the ``workflow_runs`` + ``workflow_run_jobs``
tables. Status transitions are driven by
:class:`~srunx.observability.monitoring.pollers.active_watch_poller.ActiveWatchPoller`, which
aggregates child job statuses into the workflow run via an internal
``kind='workflow_run'`` watch created when the run starts.
"""

from __future__ import annotations

import re
import sqlite3
from pathlib import Path
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Request

from srunx.common.logging import get_logger
from srunx.runtime.sweep import SweepSpec
from srunx.runtime.sweep.orchestrator import SweepOrchestrator
from srunx.runtime.workflow.runner import WorkflowRunner
from srunx.slurm.clients.ssh import SlurmSSHClient
from srunx.slurm.ssh_executor import SlurmSSHExecutorPool

from ..deps import get_adapter, get_db_conn
from ..schemas.workflows import (
    WorkflowCreateRequest,
    WorkflowRunRequest,
)
from ..services import _submission_common
from ..services.sweep_submission import SweepSubmissionService
from ..services.workflow_run_cancellation import WorkflowRunCancellationService
from ..services.workflow_run_query import WorkflowRunQueryService
from ..services.workflow_storage import WorkflowStorageService
from ..services.workflow_submission import WorkflowSubmissionService
from ..services.workflow_validation import WorkflowValidationService

__all__ = ["router"]

logger = get_logger(__name__)

_WORKFLOW_TERMINAL_STATUSES: frozenset[str] = frozenset(
    {"completed", "failed", "cancelled"}
)

router = APIRouter(prefix="/api/workflows", tags=["workflows"])

_SAFE_NAME = re.compile(r"^[\w\-]+$")
_RESERVED_NAMES = frozenset({"new"})


def _find_yaml(name: str, mount_name: str) -> Path:
    return _submission_common.find_yaml(
        name, mount_name, _submission_common.get_current_profile
    )


def _reject_python_prefix_web(payload: Any, *, source: str) -> None:
    _submission_common.reject_python_prefix_web(payload, source=source)


def _storage() -> WorkflowStorageService:
    return WorkflowStorageService(
        profile_resolver=_submission_common.get_current_profile
    )


@router.get("")
async def list_workflows(mount: str) -> list[dict[str, Any]]:
    return await _storage().list_workflows(mount)


@router.get("/runs")
async def list_runs(
    name: str | None = None,
    conn: sqlite3.Connection = Depends(get_db_conn),
) -> list[dict[str, Any]]:
    return await WorkflowRunQueryService().list_runs(conn, name)


@router.get("/runs/{run_id}")
async def get_run(
    run_id: str,
    conn: sqlite3.Connection = Depends(get_db_conn),
) -> dict[str, Any]:
    """Get the status and details of a single workflow run."""
    return await WorkflowRunQueryService().get_run(conn, run_id)


@router.post("/runs/{run_id}/cancel")
async def cancel_run(
    run_id: str,
    adapter: SlurmSSHClient = Depends(get_adapter),
    conn: sqlite3.Connection = Depends(get_db_conn),
) -> dict[str, Any]:
    """Cancel all jobs in a running workflow."""
    return await WorkflowRunCancellationService(_WORKFLOW_TERMINAL_STATUSES).cancel(
        run_id=run_id,
        conn=conn,
        adapter=adapter,
    )


@router.post("/validate")
async def validate_workflow(body: dict[str, str]) -> dict[str, Any]:
    return await WorkflowValidationService().validate_yaml(body.get("yaml", ""))


@router.post("/upload")
async def upload_workflow(body: dict[str, str]) -> dict[str, Any]:
    return await _storage().upload(
        yaml_content=body.get("yaml", ""),
        filename=body.get("filename", ""),
        mount_name=body.get("mount", ""),
        safe_name_re=_SAFE_NAME,
    )


@router.post("/create")
async def create_workflow(body: WorkflowCreateRequest) -> dict[str, Any]:
    """Create a new workflow from a structured JSON payload.

    Validates all jobs via Pydantic model construction, checks for
    dependency cycles, serializes to YAML, and persists to disk.
    """
    return await _storage().create(body, reserved_names=_RESERVED_NAMES)


_WORKFLOW_RUN_PRESETS = ("terminal", "running_and_terminal", "all")


# ── Sweep shims — preserved as module-level entry points so
# ``tests/sweep/test_sweep_ssh_integration.py`` can call
# ``wf_mod._dispatch_sweep(...)`` / ``wf_mod._run_sweep_background(...)``
# directly. Real logic lives in ``SweepSubmissionService``.


async def _dispatch_sweep(
    *,
    yaml_path: Path,
    name: str,
    body: WorkflowRunRequest,
    request: Request,
    adapter: SlurmSSHClient,
    mount: str | None = None,
) -> dict[str, Any]:
    """Dispatch shim — see :meth:`SweepSubmissionService.dispatch`."""
    sweep_service = SweepSubmissionService(
        sweep_spec_cls=SweepSpec,
        orchestrator_cls=SweepOrchestrator,
        profile_resolver=_submission_common.get_current_profile,
        workflow_runner_cls=WorkflowRunner,
        executor_pool_cls=SlurmSSHExecutorPool,
    )
    return await sweep_service.dispatch(
        yaml_path=yaml_path,
        name=name,
        body=body,
        request=request,
        adapter=adapter,
        mount=mount,
    )


async def _run_sweep_background(
    orchestrator: Any,
    sweep_run_id: int,
    pool: SlurmSSHExecutorPool | None = None,
) -> None:
    """Background-task shim — see :func:`run_sweep_background`."""
    from ..services.sweep_submission import run_sweep_background

    await run_sweep_background(orchestrator, sweep_run_id, pool)


@router.post("/{name}/run", status_code=202)
async def run_workflow(
    name: str,
    request: Request,
    mount: str | None = None,
    body: WorkflowRunRequest | None = None,
    adapter: SlurmSSHClient = Depends(get_adapter),
    conn: sqlite3.Connection = Depends(get_db_conn),
) -> dict[str, Any]:
    """Run a workflow: sync mounts, submit jobs with SLURM dependencies.

    On success, creates a kind='workflow_run' watch that
    :class:`~srunx.observability.monitoring.pollers.active_watch_poller.ActiveWatchPoller`
    consumes to drive aggregate status transitions after the request
    returns.
    """
    if not _SAFE_NAME.match(name):
        raise HTTPException(status_code=422, detail="Invalid workflow name")
    if not mount:
        raise HTTPException(status_code=422, detail="mount query parameter is required")

    run_opts = body or WorkflowRunRequest()

    # Sweep-axis python:-prefix guard must run before dispatching into
    # the sweep service (the matrix is consumed there and wouldn't
    # re-check). The args_override guard runs inside
    # WorkflowSubmissionService.run for the non-sweep branch.
    if run_opts.sweep is not None:
        _reject_python_prefix_web(run_opts.sweep.matrix, source="sweep.matrix")
        if run_opts.args_override:
            _reject_python_prefix_web(run_opts.args_override, source="args_override")

    yaml_path = _find_yaml(name, mount)

    # Sweep branch: materialize synchronously so the 202 response
    # carries a real sweep_run_id, then spawn the execution loop on the
    # app's lifespan task group.
    #
    # SweepSpec / SweepOrchestrator are passed by reference so that
    # unittest.mock.patch('srunx.web.routers.workflows.SweepSpec',
    # ...) continues to affect the materialize call.
    if run_opts.sweep is not None:
        return await _dispatch_sweep(
            yaml_path=yaml_path,
            name=name,
            body=run_opts,
            request=request,
            adapter=adapter,
            mount=mount,
        )

    submission_service = WorkflowSubmissionService(
        profile_resolver=_submission_common.get_current_profile,
        terminal_statuses=_WORKFLOW_TERMINAL_STATUSES,
        allowed_presets=_WORKFLOW_RUN_PRESETS,
        # Pass the router's ``WorkflowRunner`` attribute so tests that
        # patch ``srunx.web.routers.workflows.WorkflowRunner`` reach
        # the ``from_yaml`` call.
        workflow_runner_cls=WorkflowRunner,
    )
    return await submission_service.run(
        name=name,
        mount=mount,
        yaml_path=yaml_path,
        run_opts=run_opts,
        request=request,
        adapter=adapter,
        conn=conn,
    )


@router.delete("/{name}")
async def delete_workflow(name: str, mount: str) -> dict[str, str]:
    """Delete a workflow YAML file."""
    if not _SAFE_NAME.match(name):
        raise HTTPException(status_code=422, detail="Invalid workflow name")
    return await _storage().delete(name, mount)


@router.get("/{name}")
async def get_workflow(name: str, mount: str) -> dict[str, Any]:
    if not _SAFE_NAME.match(name):
        raise HTTPException(status_code=422, detail="Invalid workflow name")
    return await _storage().get(name, mount)
