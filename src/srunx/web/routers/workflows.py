"""Workflow management endpoints: /api/workflows/*

Workflow runs are persisted in the ``workflow_runs`` + ``workflow_run_jobs``
tables. Status transitions are driven by
:class:`~srunx.pollers.active_watch_poller.ActiveWatchPoller`, which
aggregates child job statuses into the workflow run via an internal
``kind='workflow_run'`` watch created when the run starts.
"""

from __future__ import annotations

import contextlib
import functools
import re
import sqlite3
import tempfile
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

import anyio
import yaml
from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, Field, model_validator

from srunx.db.connection import transaction
from srunx.db.models import WorkflowRun as DBWorkflowRun
from srunx.db.models import WorkflowRunJob
from srunx.db.repositories.base import now_iso
from srunx.db.repositories.job_state_transitions import (
    JobStateTransitionRepository,
)
from srunx.db.repositories.jobs import JobRepository
from srunx.db.repositories.watches import WatchRepository
from srunx.db.repositories.workflow_run_jobs import WorkflowRunJobRepository
from srunx.db.repositories.workflow_runs import WorkflowRunRepository
from srunx.exceptions import SweepExecutionError, WorkflowValidationError
from srunx.logging import get_logger
from srunx.models import (
    Job,
    JobEnvironment,
    JobResource,
    ShellJob,
    Workflow,
)
from srunx.rendering import (
    RenderedWorkflow,
    SubmissionRenderContext,
    render_workflow_for_submission,
)
from srunx.runner import WorkflowRunner
from srunx.security import find_python_prefix
from srunx.sweep import SweepSpec
from srunx.sweep.orchestrator import SweepOrchestrator
from srunx.sweep.state_service import WorkflowRunStateService

from ..deps import get_adapter, get_db_conn
from ..ssh_adapter import SlurmSSHAdapter
from ..ssh_executor import SlurmSSHExecutorPool

logger = get_logger(__name__)

_WORKFLOW_TERMINAL_STATUSES: frozenset[str] = frozenset(
    {"completed", "failed", "cancelled"}
)

router = APIRouter(prefix="/api/workflows", tags=["workflows"])

_SAFE_NAME = re.compile(r"^[\w\-]+$")
_RESERVED_NAMES = frozenset({"new"})


# ── Request models ───────────────────────────────────


class WorkflowJobInput(BaseModel):
    name: str
    command: list[str]
    depends_on: list[str] = []
    template: str | None = None
    exports: dict[str, str] = Field(default_factory=dict)
    resources: dict[str, Any] | None = None
    environment: dict[str, Any] | None = None
    work_dir: str | None = None
    log_dir: str | None = None
    retry: int | None = None
    retry_delay: int | None = None
    srun_args: str | None = None
    launch_prefix: str | None = None

    model_config = {"extra": "forbid"}

    @model_validator(mode="before")
    @classmethod
    def reject_legacy_outputs(cls, data: Any) -> Any:
        if isinstance(data, dict) and "outputs" in data:
            raise ValueError(
                "The 'outputs' field was renamed to 'exports' (see CHANGELOG). "
                "Dependent jobs now reference values as '{{ deps.<job_name>.<key> }}'."
            )
        return data


class WorkflowCreateRequest(BaseModel):
    name: str = Field(..., pattern=r"^[\w\-]+$")
    args: dict[str, Any] = Field(default_factory=dict)
    jobs: list[WorkflowJobInput]
    default_project: str | None = None
    overwrite: bool = False


# ── Shared helpers ───────────────────────────────────


def _validate_and_build_workflow(data: dict[str, Any]) -> Workflow:
    """Construct and validate a Workflow from a plain dict.

    Builds Job instances with JobResource / JobEnvironment, then runs
    cycle-detection via ``Workflow.validate()``.  Raises on any
    Pydantic or workflow-level validation failure.
    """
    name: str = data["name"]
    jobs_data: list[dict[str, Any]] = data.get("jobs", [])

    jobs: list[Job | ShellJob] = []
    for jd in jobs_data:
        resource = JobResource.model_validate(jd.get("resources") or {})
        environment = JobEnvironment.model_validate(jd.get("environment") or {})
        job_kwargs: dict[str, Any] = {
            "name": jd["name"],
            "command": jd["command"],
            "depends_on": jd.get("depends_on", []),
            "exports": jd.get("exports", {}),
            "resources": resource,
            "environment": environment,
        }
        # Always pass work_dir and log_dir explicitly to prevent Job's
        # default_factory from calling os.getcwd() (wrong for the web server)
        # or defaulting to "logs" (meaningless on a remote SLURM host).
        # Empty strings are falsy and skipped by _workflow_to_yaml and
        # the SLURM template (#SBATCH --chdir is only emitted when truthy).
        job_kwargs["work_dir"] = jd.get("work_dir") or ""
        job_kwargs["log_dir"] = jd.get("log_dir") or ""
        if jd.get("retry") is not None:
            job_kwargs["retry"] = jd["retry"]
        if jd.get("retry_delay") is not None:
            job_kwargs["retry_delay"] = jd["retry_delay"]
        job = Job(**job_kwargs)
        jobs.append(job)

    workflow = Workflow(name=name, jobs=jobs)
    workflow.validate()
    return workflow


def _workflow_to_yaml(
    name: str,
    jobs_data: list[dict[str, Any]],
    default_project: str | None = None,
    args: dict[str, Any] | None = None,
) -> str:
    """Serialize a workflow to YAML compatible with ``WorkflowRunner.from_yaml``.

    Only includes non-default / non-None resource and environment fields so
    the resulting file stays clean.
    """
    serialized_jobs: list[dict[str, Any]] = []
    for jd in jobs_data:
        entry: dict[str, Any] = {
            "name": jd["name"],
            "command": jd["command"],
        }

        depends = jd.get("depends_on", [])
        if depends:
            entry["depends_on"] = depends

        exports = jd.get("exports", {})
        if exports:
            entry["exports"] = exports

        # Resources — only include non-None values
        raw_res = jd.get("resources") or {}
        resources = {k: v for k, v in raw_res.items() if v is not None}
        if resources:
            entry["resources"] = resources

        # Environment — only include non-None values
        raw_env = jd.get("environment") or {}
        environment = {k: v for k, v in raw_env.items() if v is not None}
        if environment:
            entry["environment"] = environment

        # Job-level optional fields
        if jd.get("template"):
            entry["template"] = jd["template"]
        if jd.get("work_dir"):
            entry["work_dir"] = jd["work_dir"]
        if jd.get("log_dir"):
            entry["log_dir"] = jd["log_dir"]
        if jd.get("retry") is not None:
            entry["retry"] = jd["retry"]
        if jd.get("retry_delay") is not None:
            entry["retry_delay"] = jd["retry_delay"]
        if jd.get("srun_args"):
            entry["srun_args"] = jd["srun_args"]
        if jd.get("launch_prefix"):
            entry["launch_prefix"] = jd["launch_prefix"]

        serialized_jobs.append(entry)

    doc: dict[str, Any] = {"name": name}
    if default_project:
        doc["default_project"] = default_project
    if args:
        doc["args"] = args
    doc["jobs"] = serialized_jobs
    return yaml.dump(doc, default_flow_style=False, sort_keys=False)


def _get_current_profile():
    """Get the current SSH profile from web config or ConfigManager."""
    from ..sync_utils import get_current_profile

    return get_current_profile()


def _find_mount(profile, mount_name: str):
    """Find a mount by name. Raises HTTPException 404 if not found."""
    from ..sync_utils import find_mount

    try:
        return find_mount(profile, mount_name)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e


def _workflow_dir(mount_name: str) -> Path:
    """Resolve workflow directory for a given mount.

    Returns ``<mount.local>/.srunx/workflows/``.
    """
    profile = _get_current_profile()
    if profile is None:
        raise HTTPException(status_code=503, detail="No SSH profile configured")
    mount = _find_mount(profile, mount_name)
    return Path(mount.local) / ".srunx" / "workflows"


def _ensure_workflow_dir(mount_name: str) -> Path:
    """Like ``_workflow_dir`` but creates the directory (and ``.srunx/.gitignore``) if needed."""
    d = _workflow_dir(mount_name)
    d.mkdir(parents=True, exist_ok=True)
    gitignore = d.parent / ".gitignore"
    if not gitignore.exists():
        gitignore.write_text("*\n!workflows/\n!workflows/**\n!.gitignore\n")
    return d


def _find_yaml(name: str, mount_name: str) -> Path:
    d = _workflow_dir(mount_name)
    for ext in (".yaml", ".yml"):
        p = d / f"{name}{ext}"
        if p.exists():
            return p
    raise HTTPException(status_code=404, detail=f"Workflow '{name}' not found")


def _reject_python_prefix_web(payload: Any, *, source: str) -> None:
    """Reject ``python:``-prefixed strings in Web API payloads.

    Centralizes the guard applied to both YAML args (pre-parsed by the
    caller) and JSON ``args_override`` / ``sweep.matrix`` payloads.
    Raises ``HTTPException(422)`` on the first violation.
    """
    violation = find_python_prefix(payload, source=source)
    if violation is not None:
        raise HTTPException(
            status_code=422,
            detail=(
                f"{violation.source} at '{violation.path}' contains 'python:' "
                "prefix which is not allowed via web for security reasons"
            ),
        )


def _reject_python_prefix_in_yaml_args(yaml_content: str) -> None:
    """Parse YAML text and apply the ``python:`` guard to its ``args`` section.

    Uses ``yaml.safe_load`` so legitimate uses of ``python:`` in commands
    or comments are not blocked. Malformed YAML is left to downstream
    validation to report.
    """
    try:
        data = yaml.safe_load(yaml_content)
    except Exception:
        return

    if not isinstance(data, dict):
        return

    args = data.get("args")
    if not isinstance(args, dict):
        return

    _reject_python_prefix_web(args, source="args")


def _serialize_workflow(
    runner: WorkflowRunner,
    raw_yaml_jobs: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    wf = runner.workflow
    # Build lookup for extra YAML fields not stored in Job model
    raw_by_name: dict[str, dict[str, Any]] = {}
    if raw_yaml_jobs:
        for rj in raw_yaml_jobs:
            raw_by_name[rj.get("name", "")] = rj

    jobs: list[dict[str, Any]] = []
    for job in wf.jobs:
        d: dict[str, Any] = {
            "name": job.name,
            "job_id": job.job_id,
            "status": job._status.value,
            "depends_on": job.depends_on,
            "exports": job.exports,
        }
        raw_job = raw_by_name.get(job.name, {})
        if raw_job.get("template"):
            d["template"] = raw_job["template"]
        if hasattr(job, "command"):
            cmd = job.command  # type: ignore[union-attr]
            d["command"] = [cmd] if isinstance(cmd, str) else cmd
            d["resources"] = {
                "nodes": job.resources.nodes,  # type: ignore[union-attr]
                "gpus_per_node": job.resources.gpus_per_node,  # type: ignore[union-attr]
                "partition": job.resources.partition,  # type: ignore[union-attr]
                "time_limit": job.resources.time_limit,  # type: ignore[union-attr]
            }
        elif hasattr(job, "script_path"):
            d["script_path"] = job.script_path  # type: ignore[union-attr]
            d["command"] = []
            d["resources"] = {}
        else:
            d["command"] = []
            d["resources"] = {}
        jobs.append(d)
    result: dict[str, Any] = {"name": wf.name, "jobs": jobs}
    if runner.args:
        result["args"] = runner.args
    if runner.default_project:
        result["default_project"] = runner.default_project
    return result


@router.get("")
async def list_workflows(mount: str) -> list[dict[str, Any]]:
    d = _workflow_dir(mount)
    if not d.exists():
        return []

    results: list[dict[str, Any]] = []
    for p in sorted(d.glob("*.y*ml")):
        try:

            def _load(_p=p):
                import yaml as _yaml

                runner = WorkflowRunner.from_yaml(_p)
                raw = _yaml.safe_load(_p.read_text(encoding="utf-8"))
                return runner, raw.get("jobs", [])

            runner, raw_jobs = await anyio.to_thread.run_sync(_load)
            results.append(_serialize_workflow(runner, raw_yaml_jobs=raw_jobs))
        except Exception:
            continue
    return results


def _parse_run_id(run_id: str) -> int:
    """Parse a run_id string → int, raising 404 for non-integer ids.

    The API accepts ``run_id`` as a string for historical compatibility
    (the old UUID-keyed registry); internally we always store an int.
    """
    try:
        return int(run_id)
    except (TypeError, ValueError) as exc:
        raise HTTPException(
            status_code=404, detail=f"Run '{run_id}' not found"
        ) from exc


def _serialize_run(
    run: DBWorkflowRun,
    memberships: list[WorkflowRunJob],
    jobs_by_id: dict[int, str],
) -> dict[str, Any]:
    """Build the API response for a workflow run.

    ``jobs_by_id`` maps SLURM job_id → observed status. Memberships with
    no job_id (not yet submitted) are omitted from both ``job_ids`` and
    ``job_statuses``.
    """
    job_ids: dict[str, str] = {}
    job_statuses: dict[str, str] = {}
    for wrj in memberships:
        if wrj.job_id is None:
            continue
        job_ids[wrj.job_name] = str(wrj.job_id)
        status = jobs_by_id.get(wrj.job_id)
        if status is not None:
            job_statuses[wrj.job_name] = status

    return {
        "id": str(run.id),
        "workflow_name": run.workflow_name,
        "started_at": run.started_at.isoformat() if run.started_at else None,
        "completed_at": run.completed_at.isoformat() if run.completed_at else None,
        "status": run.status,
        "job_ids": job_ids,
        "job_statuses": job_statuses,
        "error": run.error,
        "sweep_run_id": run.sweep_run_id,
    }


def _build_run_response(conn: sqlite3.Connection, run: DBWorkflowRun) -> dict[str, Any]:
    """Load memberships + child job statuses and serialize."""
    if run.id is None:
        return _serialize_run(run, [], {})
    memberships = WorkflowRunJobRepository(conn).list_by_run(run.id)
    job_repo = JobRepository(conn)
    jobs_by_id: dict[int, str] = {}
    for m in memberships:
        # V5+: ``jobs_row_id`` is the authoritative FK to ``jobs.id``.
        # Looking up via ``get_by_row_id`` avoids the pre-V5
        # ``scheduler_key='local'`` default, which would drop SSH
        # workflow children and miss their statuses in the API response.
        if m.jobs_row_id is None or m.job_id is None:
            continue
        job = job_repo.get_by_row_id(m.jobs_row_id)
        if job is not None:
            jobs_by_id[m.job_id] = job.status
    return _serialize_run(run, memberships, jobs_by_id)


@router.get("/runs")
async def list_runs(
    name: str | None = None,
    conn: sqlite3.Connection = Depends(get_db_conn),
) -> list[dict[str, Any]]:
    def _load() -> list[dict[str, Any]]:
        runs = WorkflowRunRepository(conn).list_all()
        if name is not None:
            runs = [r for r in runs if r.workflow_name == name]
        return [_build_run_response(conn, r) for r in runs]

    return await anyio.to_thread.run_sync(_load)


@router.get("/runs/{run_id}")
async def get_run(
    run_id: str,
    conn: sqlite3.Connection = Depends(get_db_conn),
) -> dict[str, Any]:
    """Get the status and details of a single workflow run."""
    rid = _parse_run_id(run_id)

    def _load() -> dict[str, Any]:
        run = WorkflowRunRepository(conn).get(rid)
        if run is None:
            raise HTTPException(status_code=404, detail=f"Run '{run_id}' not found")
        return _build_run_response(conn, run)

    return await anyio.to_thread.run_sync(_load)


@router.post("/runs/{run_id}/cancel")
async def cancel_run(
    run_id: str,
    adapter: SlurmSSHAdapter = Depends(get_adapter),
    conn: sqlite3.Connection = Depends(get_db_conn),
) -> dict[str, Any]:
    """Cancel all jobs in a running workflow."""
    rid = _parse_run_id(run_id)
    run_repo = WorkflowRunRepository(conn)
    wrj_repo = WorkflowRunJobRepository(conn)
    watch_repo = WatchRepository(conn)

    run = await anyio.to_thread.run_sync(lambda: run_repo.get(rid))
    if run is None:
        raise HTTPException(404, f"Run '{run_id}' not found")
    if run.status in _WORKFLOW_TERMINAL_STATUSES:
        raise HTTPException(422, f"Run is already {run.status}")

    memberships = await anyio.to_thread.run_sync(lambda: wrj_repo.list_by_run(rid))
    errors: list[str] = []
    for m in memberships:
        if m.job_id is None:
            continue
        try:
            await anyio.to_thread.run_sync(
                lambda jid=m.job_id: adapter.cancel_job(int(jid))  # type: ignore[misc]
            )
        except Exception as e:
            errors.append(f"{m.job_name}: {e}")

    # Route through WorkflowRunStateService so a
    # ``workflow_run.status_changed`` event is emitted and subscribers
    # receive a delivery. ``run.status`` was captured above and is
    # guaranteed non-terminal by the 422 guard.
    current_status = run.status

    def _finalize() -> None:
        with transaction(conn, "IMMEDIATE"):
            transitioned = WorkflowRunStateService.update(
                conn=conn,
                workflow_run_id=rid,
                from_status=current_status,
                to_status="cancelled",
                completed_at=now_iso(),
            )
            if not transitioned:
                # Race with the poller: re-read the latest status and
                # retry once. Skip if the row has reached a terminal
                # state in the meantime.
                latest = run_repo.get(rid)
                if (
                    latest is not None
                    and latest.status not in _WORKFLOW_TERMINAL_STATUSES
                ):
                    WorkflowRunStateService.update(
                        conn=conn,
                        workflow_run_id=rid,
                        from_status=latest.status,
                        to_status="cancelled",
                        completed_at=now_iso(),
                    )
            for w in watch_repo.list_by_target(
                kind="workflow_run",
                target_ref=f"workflow_run:{rid}",
                only_open=True,
            ):
                if w.id is not None:
                    watch_repo.close(w.id)

    await anyio.to_thread.run_sync(_finalize)

    result: dict[str, Any] = {"status": "cancelled", "run_id": str(rid)}
    if errors:
        result["warnings"] = errors
    return result


@router.post("/validate")
async def validate_workflow(body: dict[str, str]) -> dict[str, Any]:
    yaml_content = body.get("yaml", "")
    if not yaml_content:
        return {"valid": False, "errors": ["Empty YAML content"]}

    _reject_python_prefix_in_yaml_args(yaml_content)

    with tempfile.NamedTemporaryFile(suffix=".yaml", mode="w", delete=False) as f:
        f.write(yaml_content)
        tmp_path = f.name

    try:
        runner = await anyio.to_thread.run_sync(
            lambda: WorkflowRunner.from_yaml(tmp_path)
        )
        await anyio.to_thread.run_sync(runner.workflow.validate)
        return {"valid": True}
    except WorkflowValidationError as e:
        return {"valid": False, "errors": [str(e)]}
    except Exception as e:
        return {"valid": False, "errors": [str(e)]}
    finally:
        Path(tmp_path).unlink(missing_ok=True)


@router.post("/upload")
async def upload_workflow(body: dict[str, str]) -> dict[str, Any]:
    yaml_content = body.get("yaml", "")
    filename = body.get("filename", "")
    mount_name = body.get("mount", "")

    if not yaml_content or not filename or not mount_name:
        raise HTTPException(
            status_code=422, detail="'yaml', 'filename', and 'mount' are required"
        )

    _reject_python_prefix_in_yaml_args(yaml_content)

    if len(yaml_content) > 1_000_000:
        raise HTTPException(status_code=413, detail="YAML content exceeds 1MB limit")

    safe_filename = Path(filename).name
    name = Path(safe_filename).stem
    if not _SAFE_NAME.match(name):
        raise HTTPException(
            status_code=422,
            detail="Filename must be alphanumeric with hyphens/underscores only",
        )

    d = _ensure_workflow_dir(mount_name)
    dest = d / safe_filename
    dest.write_text(yaml_content)

    try:
        runner = await anyio.to_thread.run_sync(lambda: WorkflowRunner.from_yaml(dest))
        await anyio.to_thread.run_sync(runner.workflow.validate)
        return _serialize_workflow(runner)
    except Exception as e:
        dest.unlink(missing_ok=True)
        raise HTTPException(status_code=422, detail=str(e)) from e


@router.post("/create")
async def create_workflow(body: WorkflowCreateRequest) -> dict[str, Any]:
    """Create a new workflow from a structured JSON payload.

    Validates all jobs via Pydantic model construction, checks for
    dependency cycles, serializes to YAML, and persists to disk.
    """
    name = body.name
    mount_name = body.default_project
    if not mount_name:
        raise HTTPException(
            status_code=422,
            detail="A mount (default_project) is required to save a workflow",
        )

    # Reserved name guard
    if name in _RESERVED_NAMES:
        raise HTTPException(
            status_code=422,
            detail=f"Workflow name '{name}' is reserved",
        )

    # Check for existing workflow with the same name
    d = _ensure_workflow_dir(mount_name)
    if not body.overwrite:
        for ext in (".yaml", ".yml"):
            if (d / f"{name}{ext}").exists():
                raise HTTPException(
                    status_code=409,
                    detail=f"Workflow '{name}' already exists",
                )

    # Reject python: args from web for security (shared guard).
    _reject_python_prefix_web(body.args, source="args")

    # Build the raw dict list from the request for validation + serialization
    jobs_raw: list[dict[str, Any]] = [
        j.model_dump(exclude_none=True) for j in body.jobs
    ]

    data: dict[str, Any] = {"name": name, "jobs": jobs_raw}

    # Validate by constructing domain models (synchronous — CPU-only, no I/O)
    try:
        _validate_and_build_workflow(data)
    except WorkflowValidationError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except Exception as exc:
        from pydantic import ValidationError as _VE

        if isinstance(exc, _VE):
            errors = [
                {"loc": list(e["loc"]), "msg": e["msg"], "type": e["type"]}
                for e in exc.errors()
            ]
            raise HTTPException(status_code=422, detail=errors) from exc
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    # Serialize and write to disk
    yaml_content = _workflow_to_yaml(
        name, jobs_raw, default_project=body.default_project, args=body.args or None
    )
    dest = d / f"{name}.yaml"
    await anyio.to_thread.run_sync(lambda: dest.write_text(yaml_content))

    # Re-load via WorkflowRunner to return the canonical serialized form
    runner = await anyio.to_thread.run_sync(lambda: WorkflowRunner.from_yaml(dest))
    return _serialize_workflow(runner)


class SweepSpecRequest(BaseModel):
    """Sweep payload accepted by ``POST /api/workflows/{name}/run``.

    Mirrors :class:`srunx.sweep.SweepSpec` but with a server-side default
    for ``max_parallel`` (R7.9) so the client can omit it for small
    sweeps.
    """

    model_config = {"extra": "forbid"}

    matrix: dict[str, list[Any]] = Field(default_factory=dict)
    fail_fast: bool = False
    max_parallel: int = 4


class WorkflowRunRequest(BaseModel):
    from_job: str | None = None
    to_job: str | None = None
    single_job: str | None = None
    dry_run: bool = False
    # Notification subscription wiring. When ``notify`` is true and
    # ``endpoint_id`` resolves to an enabled endpoint row, the run's
    # auto-created ``kind='workflow_run'`` watch is paired with a
    # subscription so the delivery poller fans status-transition
    # events out to that endpoint. Matches the shape accepted by
    # ``/api/jobs`` submit (R6 in design.md §Request models).
    notify: bool = False
    endpoint_id: int | None = Field(default=None, gt=0)
    preset: str = "terminal"
    # Sweep wiring (Phase G). ``args_override`` expands workflow-level
    # ``args`` before Jinja rendering; ``sweep`` switches the request
    # onto the :class:`SweepOrchestrator` path.
    args_override: dict[str, Any] = Field(default_factory=dict)
    sweep: SweepSpecRequest | None = None


_WORKFLOW_RUN_PRESETS = ("terminal", "running_and_terminal", "all")


def _filter_workflow_jobs(
    workflow: Workflow,
    from_job: str | None,
    to_job: str | None,
    single_job: str | None,
) -> list[Job | ShellJob]:
    """Filter workflow jobs based on execution control parameters."""
    all_jobs = {job.name: job for job in workflow.jobs}

    if single_job:
        if single_job not in all_jobs:
            raise HTTPException(422, f"Job '{single_job}' not found in workflow")
        job = all_jobs[single_job]
        # Create a copy-like job with no dependencies for standalone execution
        return [job]

    names = [job.name for job in workflow.jobs]

    start_idx = 0
    end_idx = len(names)

    if from_job:
        if from_job not in all_jobs:
            raise HTTPException(422, f"Job '{from_job}' not found in workflow")
        start_idx = names.index(from_job)

    if to_job:
        if to_job not in all_jobs:
            raise HTTPException(422, f"Job '{to_job}' not found in workflow")
        end_idx = names.index(to_job) + 1

    if from_job and to_job and start_idx >= end_idx:
        raise HTTPException(
            422,
            f"from_job '{from_job}' must appear before to_job '{to_job}' in the workflow",
        )

    selected_names = set(names[start_idx:end_idx])
    return [job for job in workflow.jobs if job.name in selected_names]


@contextlib.asynccontextmanager
async def _hold_workflow_mounts_web(
    workflow: Workflow,
    runner: WorkflowRunner,
    *,
    sync_required: bool = True,
) -> AsyncIterator[Any]:
    """Hold the per-mount sync lock across the whole workflow submission.

    Workflow Phase 2 (#135) — web parity with the CLI's
    :func:`srunx.cli.workflow._hold_workflow_mounts`. Each unique
    mount touched by the workflow's :class:`ShellJob` ``script_path``
    values is rsynced **once** under
    :func:`~srunx.sync.service.mount_sync_session`, and the
    per-(profile, mount) lock is held for the entire ``async with``
    block so a concurrent ``srunx flow run`` / ``/api/workflows/run``
    can't rsync different bytes between our sync and our submission.

    Sort order matches the profile's ``mounts`` list so two web
    requests touching overlapping mount sets always acquire locks
    in the same global order — eliminates lock-inversion deadlock,
    same fix as Codex follow-up #2 on PR #141.

    Yields the resolved :class:`ServerProfile` so the caller can use
    it for path translation / submission-context construction. Yields
    ``None`` when no SSH profile is configured (legacy local path).

    Lock acquisition + rsync errors surface as
    :class:`HTTPException(502)`. Exceptions raised from the body
    (sbatch failures, render errors) propagate **unchanged** so the
    caller can route them through the existing per-phase ``_fail``
    bookkeeping. Mirrors the CLI exception-scoping rationale (Codex
    blocker #1 on PR #141).

    ``sync_required=False`` skips the rsync but still acquires the
    lock — preserves the race-free submission invariant for callers
    that opted out of the transfer.
    """
    from srunx.cli.submission_plan import collect_touched_mounts
    from srunx.config import get_config
    from srunx.ssh.core.config import ConfigManager
    from srunx.sync.lock import SyncLockTimeoutError
    from srunx.sync.service import SyncAbortedError, mount_sync_session

    from ..config import get_web_config
    from ..sync_utils import get_current_profile

    def _resolve_profile_with_name() -> tuple[Any, str | None]:
        # Mirrors ``sync_utils.get_current_profile`` but returns the
        # name too — :func:`acquire_sync_lock` keys the lock file on
        # ``(profile_name, mount_name)`` so we need both halves.
        web_cfg = get_web_config()
        cm = ConfigManager()
        name = web_cfg.ssh_profile or cm.get_current_profile_name()
        if not name:
            return None, None
        return cm.get_profile(name), name

    profile, profile_name = await anyio.to_thread.run_sync(_resolve_profile_with_name)
    if profile is None:
        # Fall back to the patched-in ``get_current_profile`` so tests
        # that mock the helper directly (without seeding the
        # ``ConfigManager`` registry) still see a profile here.
        profile = await anyio.to_thread.run_sync(get_current_profile)
        if profile is None:
            yield None
            return
        profile_name = profile_name or runner.default_project or ""

    mounts = collect_touched_mounts(workflow, profile)
    if not mounts:
        yield profile
        return

    mount_order = {m.name: i for i, m in enumerate(profile.mounts)}
    mounts.sort(key=lambda m: mount_order.get(m.name, len(mount_order)))

    config = get_config()
    # Lock-file key — falls back to the workflow's default_project so
    # we never hand an empty string to acquire_sync_lock.
    effective_profile_name = profile_name or runner.default_project or "default"

    def _enter_all_mounts() -> contextlib.ExitStack:
        # ExitStack lifetime spans the ``async with`` body; constructed
        # off-thread because mount_sync_session is a synchronous CM
        # (file lock + subprocess rsync). The stack is closed back in
        # ``_close_stack`` after the body exits.
        stack = contextlib.ExitStack()
        try:
            for mount in mounts:
                outcome = stack.enter_context(
                    mount_sync_session(
                        profile_name=effective_profile_name,
                        profile=profile,
                        mount=mount,
                        config=config.sync,
                        sync_required=sync_required,
                    )
                )
                if outcome.performed:
                    logger.info("Synced mount '%s'", mount.name)
        except BaseException:
            stack.close()
            raise
        return stack

    try:
        stack = await anyio.to_thread.run_sync(_enter_all_mounts)
    except SyncAbortedError as exc:
        raise HTTPException(
            status_code=502, detail=f"Mount sync failed: {exc}"
        ) from exc
    except SyncLockTimeoutError as exc:
        raise HTTPException(
            status_code=502, detail=f"Mount sync failed: {exc}"
        ) from exc
    except RuntimeError as exc:
        raise HTTPException(
            status_code=502, detail=f"Mount sync failed: {exc}"
        ) from exc

    try:
        # Body exceptions MUST propagate as-is (sbatch failures vs sync
        # failures must stay distinguishable to the caller's per-phase
        # _fail bookkeeping). Codex blocker #1 on PR #141.
        yield profile
    finally:
        await anyio.to_thread.run_sync(stack.close)


def _build_submission_context(
    mount_name: str | None,
    profile: Any,
) -> SubmissionRenderContext:
    """Construct a :class:`SubmissionRenderContext` from the Web profile + mount.

    - ``mount_name`` is the selected ``?mount=<name>`` query parameter.
      When ``None``, no mount translation is performed.
    - ``profile`` is the configured :class:`ServerProfile` (or ``None``
      when no SSH profile is set up). Its ``mounts`` list is frozen into
      a tuple so the context stays hashable / immutable.
    - ``default_work_dir`` is the selected mount's remote path (so jobs
      whose ``work_dir`` is empty inherit the mount root).
    """
    mounts: tuple[Any, ...] = tuple(profile.mounts) if profile is not None else ()
    default_work_dir: str | None = None
    if mount_name is not None and profile is not None:
        for m in profile.mounts:
            if m.name == mount_name:
                default_work_dir = m.remote
                break
    return SubmissionRenderContext(
        mount_name=mount_name,
        mounts=mounts,
        default_work_dir=default_work_dir,
    )


def _render_workflow(
    yaml_path: Path,
    *,
    submission_context: SubmissionRenderContext,
    args_override: dict[str, Any] | None = None,
    single_job: str | None = None,
) -> RenderedWorkflow:
    """Thin wrapper around :func:`render_workflow_for_submission`.

    The previous ``_prepare_render_context`` / ``_render_scripts`` pair
    re-implemented a mount-aware render local to the Web router; this
    delegates to the canonical helper so Web non-sweep, Web sweep, and
    MCP all share identical semantics.
    """
    return render_workflow_for_submission(
        yaml_path,
        args_override=args_override,
        context=submission_context,
        single_job=single_job,
    )


def _enforce_shell_script_roots(
    workflow: Workflow,
    mount: str,
    profile: Any,
) -> None:
    """Guard that every :class:`ShellJob`'s script_path stays under allowed roots.

    The canonical render helper reads :class:`ShellJob` scripts verbatim
    (``render_shell_job_script`` uses ``script_path`` as the template); we
    still need the directory-traversal check that the old ``_render_scripts``
    performed before the file was opened. Called before render so bogus
    paths surface as 403 with no partial render side effects.
    """
    from srunx.security import find_shell_script_violation

    allowed_roots = [_workflow_dir(mount).resolve()]
    if profile:
        allowed_roots.extend(Path(m.local).resolve() for m in profile.mounts)
    violation = find_shell_script_violation(workflow, allowed_roots)
    if violation is not None:
        raise HTTPException(
            403,
            f"Script path '{violation.script_path}' is outside allowed directories",
        )


def _resolve_in_place_target(
    job: Job | ShellJob,
    rendered_text: str,
    profile: Any,
) -> tuple[str, str] | None:
    """Decide whether *job* qualifies for the in-place sbatch path.

    Workflow Phase 2 (#135): only :class:`ShellJob` instances whose
    ``script_path`` resolves under one of the SSH profile's mount
    ``local`` roots, and whose Jinja-rendered bytes still equal the
    on-disk source bytes, can run the user's file verbatim on the
    cluster. Anything else (``Job`` with ``command``, ShellJob outside
    every mount, or rendered output that diverged from source) must
    fall back to the legacy temp-upload path so the rendered artifact
    actually reaches the cluster.

    Returns ``(remote_path, submit_cwd)`` when the in-place path is
    safe, otherwise ``None``. ``submit_cwd`` is the script's parent
    directory on the remote so relative paths inside the user's
    ``#SBATCH`` directives resolve as they would on a head-node
    ``sbatch ./script.sh``. Same ``parent_remote or remote_script``
    fallback the single-job /api/jobs path uses.

    Path security: the workflow's ``_enforce_shell_script_roots``
    guard already rejected scripts outside every allowed root before
    render, so reaching here means the path is safe to translate.
    The mount lookup is a longest-prefix match via
    :func:`resolve_mount_for_path` so nested mounts pick the deepest
    owner.
    """
    from srunx.cli.submission_plan import (
        render_text_matches_source,
        resolve_mount_for_path,
        translate_local_to_remote,
    )

    if profile is None or not isinstance(job, ShellJob):
        return None

    script_attr = getattr(job, "script_path", None)
    if not script_attr:
        return None

    try:
        source_path = Path(script_attr)
    except (TypeError, ValueError):
        return None

    mount = resolve_mount_for_path(source_path, profile)
    if mount is None:
        return None

    if not render_text_matches_source(rendered_text, source_path):
        return None

    remote_path = translate_local_to_remote(source_path, mount)
    parent_remote, _, _ = remote_path.rpartition("/")
    submit_cwd = parent_remote or remote_path
    return remote_path, submit_cwd


async def _submit_jobs_bfs(
    workflow: Workflow,
    scripts: dict[str, str],
    run_opts: WorkflowRunRequest,
    adapter: SlurmSSHAdapter,
    *,
    conn: sqlite3.Connection,
    run_id: int,
    profile: Any = None,
) -> dict[str, str]:
    """Submit jobs in topological order via BFS, returning {name: slurm_id}.

    Each successful submit is persisted atomically:

    1. ``jobs`` row via :meth:`JobRepository.record_submission` (with
       ``submission_source='workflow'`` + ``workflow_run_id``).
    2. Seed ``job_state_transitions`` with ``PENDING`` so the active
       watch poller's first observation produces a real transition.
    3. Link to the workflow via :meth:`WorkflowRunJobRepository.create`.

    On sbatch failure we record a membership row with ``job_id=None`` so
    the response can still reflect the attempted job set, then raise.

    Per-job dispatch (workflow Phase 2 / #135 web parity): when
    *profile* is supplied AND a job is a :class:`ShellJob` whose
    rendered bytes match the on-disk source AND that source lives
    under one of the profile's mounts, sbatch runs against the
    already-on-remote path via :meth:`SlurmSSHAdapter.submit_remote_sbatch`
    (no tmp upload, the user's own ``#SBATCH`` directives win).
    Everything else stays on the legacy temp-upload path —
    :meth:`SlurmSSHAdapter.submit_job` ships the rendered bytes to
    ``$SRUNX_TEMP_DIR`` so the cluster runs what srunx generated.
    """
    from collections import deque

    job_repo = JobRepository(conn)
    wrj_repo = WorkflowRunJobRepository(conn)
    transition_repo = JobStateTransitionRepository(conn)

    filtered_names = {job.name for job in workflow.jobs}
    job_map: dict[str, Job | ShellJob] = {job.name: job for job in workflow.jobs}
    dependents: dict[str, list[str]] = {job.name: [] for job in workflow.jobs}
    in_degree: dict[str, int] = {
        job.name: len(
            [d for d in job.parsed_dependencies if d.job_name in filtered_names]
        )
        for job in workflow.jobs
    }

    for job in workflow.jobs:
        for dep in job.parsed_dependencies:
            if dep.job_name in filtered_names:
                dependents[dep.job_name].append(job.name)

    queue: deque[str] = deque(
        job.name for job in workflow.jobs if in_degree[job.name] == 0
    )
    submitted: dict[str, str] = {}

    while queue:
        current_name = queue.popleft()
        current_job = job_map[current_name]

        dep_parts: list[str] = []
        if not run_opts.single_job:
            for dep in current_job.parsed_dependencies:
                if dep.job_name in submitted:
                    parent_id = submitted[dep.job_name]
                    dep_parts.append(f"{dep.dep_type}:{parent_id}")
        dependency = ",".join(dep_parts) if dep_parts else None

        depends_on = [
            d.job_name
            for d in current_job.parsed_dependencies
            if d.job_name in filtered_names
        ]

        in_place = _resolve_in_place_target(current_job, scripts[current_name], profile)

        try:
            if in_place is not None:
                remote_path, submit_cwd = in_place

                def _in_place_submit(
                    rp: str = remote_path,
                    cwd: str = submit_cwd,
                    n: str = current_name,
                    d: str | None = dependency,
                ) -> int:
                    submitted_obj = adapter.submit_remote_sbatch(
                        rp,
                        submit_cwd=cwd,
                        job_name=n,
                        dependency=d,
                    )
                    if submitted_obj is None or submitted_obj.job_id is None:
                        raise RuntimeError("remote sbatch returned no job_id")
                    return int(submitted_obj.job_id)

                slurm_id = await anyio.to_thread.run_sync(_in_place_submit)
            else:
                result = await anyio.to_thread.run_sync(
                    lambda s=scripts[current_name],  # type: ignore[misc]
                    n=current_name,
                    d=dependency: adapter.submit_job(s, job_name=n, dependency=d)
                )
                slurm_id = int(result["job_id"])
            submitted[current_name] = str(slurm_id)
        except Exception as exc:
            # R3: record a membership row with ``job_id=None`` so the
            # GET /runs/{id} response still shows the failed node.
            # Best-effort — a write failure must not mask the original
            # sbatch exception.
            try:

                def _record_failed(
                    jname: str = current_name,
                    deps: list[str] = depends_on,
                ) -> None:
                    wrj_repo.create(
                        workflow_run_id=run_id,
                        job_name=jname,
                        depends_on=deps or None,
                        job_id=None,
                    )

                await anyio.to_thread.run_sync(_record_failed)
            except Exception:
                logger.debug(
                    "Failed to record membership for the failed job",
                    exc_info=True,
                )
            raise HTTPException(
                status_code=502,
                detail=f"sbatch failed for '{current_name}': {exc}",
            ) from exc

        # R1: persist the three related rows atomically. On autocommit
        # connections (isolation_level=None) each ``execute`` would
        # otherwise commit on its own — a mid-sequence failure would
        # leave e.g. the jobs row inserted with no transition or
        # membership to match it, breaking poller dedup downstream.
        #
        # Transport axis: pick up the adapter's scheduler_key so SSH-
        # submitted workflow jobs land on the correct (ssh, profile,
        # scheduler_key) triple — otherwise the poller would query
        # local SLURM for remote job ids.
        wf_scheduler_key = adapter.scheduler_key
        if wf_scheduler_key.startswith("ssh:"):
            wf_transport_type = "ssh"
            wf_profile_name: str | None = wf_scheduler_key[len("ssh:") :]
        else:
            wf_transport_type = "local"
            wf_profile_name = None

        def _persist(
            jid: int = slurm_id,
            jname: str = current_name,
            job_obj: Job | ShellJob = current_job,
            deps: list[str] = depends_on,
            tt: str = wf_transport_type,
            pn: str | None = wf_profile_name,
            sk: str = wf_scheduler_key,
        ) -> None:
            resources = getattr(job_obj, "resources", None)
            environment = getattr(job_obj, "environment", None)
            command_val = getattr(job_obj, "command", None)
            with transaction(conn, "IMMEDIATE"):
                job_repo.record_submission(
                    job_id=jid,
                    name=jname,
                    status="PENDING",
                    submission_source="workflow",
                    transport_type=tt,  # type: ignore[arg-type]
                    profile_name=pn,
                    scheduler_key=sk,
                    workflow_run_id=run_id,
                    command=command_val if isinstance(command_val, list) else None,
                    nodes=getattr(resources, "nodes", None) if resources else None,
                    gpus_per_node=(
                        getattr(resources, "gpus_per_node", None) if resources else None
                    ),
                    memory_per_node=(
                        getattr(resources, "memory_per_node", None)
                        if resources
                        else None
                    ),
                    time_limit=(
                        getattr(resources, "time_limit", None) if resources else None
                    ),
                    partition=(
                        getattr(resources, "partition", None) if resources else None
                    ),
                    nodelist=(
                        getattr(resources, "nodelist", None) if resources else None
                    ),
                    conda=getattr(environment, "conda", None) if environment else None,
                    venv=getattr(environment, "venv", None) if environment else None,
                    env_vars=(
                        getattr(environment, "env_vars", None) if environment else None
                    ),
                )
                transition_repo.insert(
                    job_id=jid,
                    from_status=None,
                    to_status="PENDING",
                    source="webhook",
                    scheduler_key=sk,
                )
                wrj_repo.create(
                    workflow_run_id=run_id,
                    job_name=jname,
                    depends_on=deps or None,
                    job_id=jid,
                    scheduler_key=sk,
                )

        await anyio.to_thread.run_sync(_persist)

        for dep_name in dependents[current_name]:
            in_degree[dep_name] -= 1
            if in_degree[dep_name] == 0:
                queue.append(dep_name)

    return submitted


async def _run_sweep_background(
    orchestrator: SweepOrchestrator,
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


async def _dispatch_sweep(
    *,
    yaml_path: Path,
    name: str,
    body: WorkflowRunRequest,
    request: Request,
    adapter: SlurmSSHAdapter,
    mount: str | None = None,
) -> dict[str, Any]:
    """Materialize synchronously + spawn the orchestrator as a background task.

    Returns 202 as soon as the cells exist in the DB so HTTP clients
    don't block on the full sweep. Matrix validation (non-scalar
    values, reserved axis names, oversize matrices) is routed through
    :class:`WorkflowValidationError` by ``expand_matrix`` and surfaced
    as HTTP 422.

    Sweep cells run through a per-sweep :class:`SlurmSSHExecutorPool`
    bounded by ``min(max_parallel, 8)`` so concurrent cells share a small
    set of pooled SSH sessions against the cluster. The pool is closed in
    :func:`_run_sweep_background` once the orchestrator returns.
    """
    assert body.sweep is not None  # narrowed by caller
    # ``SweepSpec`` / ``SweepOrchestrator`` are module-imported so
    # tests can patch them via ``srunx.web.routers.workflows.*``.

    # Read raw YAML once so the orchestrator can see base ``args`` and
    # the workflow name. ``from_yaml`` would redundantly parse it.
    def _load_raw() -> dict[str, Any]:
        raw = yaml.safe_load(yaml_path.read_text(encoding="utf-8"))
        return raw if isinstance(raw, dict) else {}

    workflow_data = await anyio.to_thread.run_sync(_load_raw)

    try:
        sweep_spec = SweepSpec(
            matrix=body.sweep.matrix,
            fail_fast=body.sweep.fail_fast,
            max_parallel=body.sweep.max_parallel,
        )
    except Exception as exc:  # noqa: BLE001 — Pydantic / value errors
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    # C3 (security): apply the same ShellJob script-root guard the
    # non-sweep path runs, before any DB materialize side effect. The
    # matrix axes in ``sweep.matrix`` are scalars (validated by
    # ``expand_matrix``) so a per-cell check would only be redundant;
    # the base workflow's ShellJob ``script_path`` is the entire attack
    # surface. ``mount`` is required by the caller (``run_workflow``)
    # so it is non-None in practice; keep the guard defensive for
    # direct callers.
    profile = await anyio.to_thread.run_sync(_get_current_profile)
    base_runner: WorkflowRunner | None = None
    if mount is not None:
        try:
            base_runner = await anyio.to_thread.run_sync(
                lambda: WorkflowRunner.from_yaml(
                    yaml_path,
                    args_override=body.args_override or None,
                )
            )
        except WorkflowValidationError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        except Exception as exc:  # noqa: BLE001 — YAML load / Jinja errors
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        assert base_runner is not None  # narrowed by the from_yaml call above
        runner_for_guard = base_runner
        await anyio.to_thread.run_sync(
            lambda: _enforce_shell_script_roots(
                runner_for_guard.workflow, mount, profile
            )
        )

        # Workflow Phase 2 (#135 web parity): rsync each touched mount
        # **once** at dispatch time, even when the sweep expands into
        # many cells targeting the same mount. The lock is released as
        # soon as the rsync completes — sweep cells then take the
        # legacy temp-upload path (the CLI keeps ``allow_in_place=False``
        # for sweeps because cell-specific Jinja args can move
        # ``script_path`` to a mount the base render didn't touch).
        async with _hold_workflow_mounts_web(
            runner_for_guard.workflow, runner_for_guard, sync_required=True
        ):
            pass

    endpoint_id: int | None = None
    if body.notify and body.endpoint_id is not None:
        endpoint_id = body.endpoint_id
    elif body.notify and body.endpoint_id is None:
        # Non-fatal — matches the non-sweep path's contract: the sweep
        # still runs, but no external deliveries are wired.
        logger.warning("sweep run: notify=true with no endpoint_id; skipping")

    # Build a per-sweep SSH executor pool so each cell's runner submits
    # through the configured cluster adapter instead of the local
    # :class:`Slurm` client. Size is capped at 8 to avoid opening more
    # SSH sessions than most clusters comfortably accept from a single
    # web host. ``pool.lease`` matches ``WorkflowJobExecutorFactory`` so
    # it can be handed to the orchestrator without further wrapping.
    pool_size = max(1, min(sweep_spec.max_parallel, 8))
    pool = SlurmSSHExecutorPool(
        adapter.connection_spec,
        callbacks=[],
        size=pool_size,
    )

    # Hand the mount-aware render context through to the orchestrator so
    # every sweep cell's ``WorkflowRunner`` sees the same ``work_dir`` /
    # ``log_dir`` translation as the non-sweep path. ``profile`` was
    # already resolved above for the ShellJob script-root guard.
    submission_context = _build_submission_context(mount, profile)

    orchestrator = SweepOrchestrator(
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

    # Spawn the execution loop on the app's lifespan task group so the
    # HTTP request can return 202 immediately. ``task_group`` is set up
    # in :func:`srunx.web.app.lifespan`; fall back to a stand-alone
    # ``asyncio.create_task`` if the app state hasn't been wired (e.g.
    # a bare ``TestClient`` without lifespan). Either way the pool is
    # closed inside :func:`_run_sweep_background`.
    task_group = getattr(request.app.state, "task_group", None)
    if task_group is not None:
        task_group.start_soon(_run_sweep_background, orchestrator, sweep_run_id, pool)
    else:
        # Fallback for test harnesses that don't run lifespan: keep a
        # weak-ish reference to the spawned task so it isn't garbage-
        # collected mid-flight.
        import asyncio

        pending = getattr(request.app.state, "background_tasks", None)
        if pending is None:
            pending = set()
            request.app.state.background_tasks = pending
        task = asyncio.create_task(
            _run_sweep_background(orchestrator, sweep_run_id, pool)
        )
        pending.add(task)
        task.add_done_callback(pending.discard)

    # Read the freshly-materialized row so counters + status reflect the
    # DB state, not the orchestrator's pre-run view.
    from srunx.db.connection import open_connection as _open
    from srunx.db.repositories.sweep_runs import SweepRunRepository

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


@router.post("/{name}/run", status_code=202)
async def run_workflow(
    name: str,
    request: Request,
    mount: str | None = None,
    body: WorkflowRunRequest | None = None,
    adapter: SlurmSSHAdapter = Depends(get_adapter),
    conn: sqlite3.Connection = Depends(get_db_conn),
) -> dict[str, Any]:
    """Run a workflow: sync mounts, submit jobs with SLURM dependencies.

    On success, creates a ``kind='workflow_run'`` watch that
    :class:`~srunx.pollers.active_watch_poller.ActiveWatchPoller`
    consumes to drive aggregate status transitions after the request
    returns.
    """
    # ``request`` is forwarded to ``_dispatch_sweep`` so the sweep
    # branch can spawn the execution loop on the app's lifespan task
    # group (avoiding a blocked HTTP response). Non-sweep branches
    # don't need it.
    if not _SAFE_NAME.match(name):
        raise HTTPException(status_code=422, detail="Invalid workflow name")
    if not mount:
        raise HTTPException(status_code=422, detail="mount query parameter is required")

    run_opts = body or WorkflowRunRequest()

    # Validate preset up-front — before mounting, rendering, and
    # ``sbatch``ing. Deferring this check until Phase 5 (post-submit)
    # means a bogus preset returns 422 with jobs already queued on
    # the cluster, leaving orphans behind. The implementation set
    # matches ``SubscriptionRepository`` + the subscriptions router
    # guard (P1-3) — ``digest`` has no delivery pipeline yet.
    if run_opts.notify and run_opts.preset not in _WORKFLOW_RUN_PRESETS:
        raise HTTPException(
            status_code=422,
            detail=(
                f"Invalid preset '{run_opts.preset}'. Allowed: {_WORKFLOW_RUN_PRESETS}"
            ),
        )

    # R7: sanitize structured sweep / args_override payloads. YAML-level
    # guard still runs on upload; this catches requests that bypass it.
    if run_opts.args_override:
        _reject_python_prefix_web(run_opts.args_override, source="args_override")
    if run_opts.sweep is not None:
        _reject_python_prefix_web(run_opts.sweep.matrix, source="sweep.matrix")

    yaml_path = _find_yaml(name, mount)

    # Sweep branch: materialize synchronously so the 202 response carries
    # a real ``sweep_run_id``, then spawn the execution loop on the app's
    # lifespan task group. Per-cell workflow_run rows are created inside
    # the orchestrator's happy-path TX; the client polls
    # ``/api/sweep_runs/{id}``.
    if run_opts.sweep is not None:
        return await _dispatch_sweep(
            yaml_path=yaml_path,
            name=name,
            body=run_opts,
            request=request,
            adapter=adapter,
            mount=mount,
        )

    # Load and optionally filter workflow
    runner = await anyio.to_thread.run_sync(
        lambda: WorkflowRunner.from_yaml(
            yaml_path,
            args_override=run_opts.args_override or None,
        )
    )
    workflow = runner.workflow
    if run_opts.from_job or run_opts.to_job or run_opts.single_job:
        filtered_jobs = _filter_workflow_jobs(
            workflow, run_opts.from_job, run_opts.to_job, run_opts.single_job
        )
        workflow = Workflow(name=workflow.name, jobs=filtered_jobs)

    run_repo = WorkflowRunRepository(conn)
    watch_repo = WatchRepository(conn)

    # Create run record (skip for dry runs)
    run_id: int | None = None
    if not run_opts.dry_run:
        run_id = await anyio.to_thread.run_sync(
            lambda: run_repo.create(
                workflow_name=name,
                yaml_path=str(yaml_path),
                args=runner.args or None,
                triggered_by="web",
            )
        )

    def _fail(reason: str) -> None:
        if run_id is None:
            return
        # Route through WorkflowRunStateService so a status_changed event
        # is emitted (subscribers of the auto-created workflow_run watch
        # then receive a Slack-etc. delivery). Read the current status
        # fresh — the poller may have already advanced the row before
        # the failure fires.
        with transaction(conn, "IMMEDIATE"):
            latest = run_repo.get(run_id)
            if latest is None or latest.status in _WORKFLOW_TERMINAL_STATUSES:
                return
            WorkflowRunStateService.update(
                conn=conn,
                workflow_run_id=run_id,
                from_status=latest.status,
                to_status="failed",
                error=reason,
                completed_at=now_iso(),
            )

    # Resolve the SSH profile up-front so render + the shell-script
    # guard see the same mount registry the lock-and-submit path will
    # use. ``_hold_workflow_mounts_web`` re-resolves it internally
    # (matches the CLI structure) — the per-request profile read is
    # cheap and keeps the render path independent of the lock context.
    profile = await anyio.to_thread.run_sync(_get_current_profile)

    # Phase 2: Render scripts via the canonical helper. Mount translation
    # and template resolution live in :mod:`srunx.rendering` so Web
    # non-sweep, Web sweep, and MCP share identical semantics.
    #
    # The shell script-root check runs against the set the helper will
    # actually read. ``single_job`` restricts the helper to that one
    # target; ``from_job`` / ``to_job`` are post-render filters so the
    # helper reads every job in the YAML. Checking the full
    # ``runner.workflow`` in those cases matches the helper's read set.
    submission_context = _build_submission_context(mount, profile)
    shell_check_workflow = (
        Workflow(
            name=runner.workflow.name,
            jobs=[j for j in runner.workflow.jobs if j.name == run_opts.single_job],
        )
        if run_opts.single_job
        else runner.workflow
    )
    try:
        await anyio.to_thread.run_sync(
            lambda: _enforce_shell_script_roots(shell_check_workflow, mount, profile)
        )
        rendered = await anyio.to_thread.run_sync(
            lambda: _render_workflow(
                yaml_path,
                submission_context=submission_context,
                args_override=run_opts.args_override or None,
                single_job=run_opts.single_job,
            )
        )
    except HTTPException:
        # 403 shell-script-root violation: propagate without _fail side
        # effects (no jobs queued, no cluster state to roll back).
        raise
    except Exception as exc:
        reason = f"Script rendering failed: {exc}"
        await anyio.to_thread.run_sync(functools.partial(_fail, reason))
        raise HTTPException(status_code=500, detail=reason) from exc

    # When ``from_job`` / ``to_job`` are set the canonical helper doesn't
    # prune; apply the existing filter over the rendered result. The
    # ``single_job`` case is already handled inside the helper.
    if run_opts.from_job or run_opts.to_job:
        filtered_names = {
            j.name
            for j in _filter_workflow_jobs(
                rendered.workflow,
                run_opts.from_job,
                run_opts.to_job,
                None,
            )
        }
        rendered_jobs = tuple(
            rj for rj in rendered.jobs if rj.job.name in filtered_names
        )
    else:
        rendered_jobs = rendered.jobs

    # The rendered workflow drives everything downstream so ``work_dir`` /
    # ``log_dir`` translations stay visible to the submit + dry-run paths.
    submission_workflow = Workflow(
        name=rendered.workflow.name,
        jobs=[rj.job for rj in rendered_jobs],
    )
    scripts: dict[str, str] = {rj.job.name: rj.script_text for rj in rendered_jobs}

    # Phase 3: Dry run early return
    if run_opts.dry_run:
        job_names_in_wf = {job.name for job in submission_workflow.jobs}
        return {
            "dry_run": True,
            "jobs": [
                {
                    "name": job.name,
                    "script": scripts.get(job.name, ""),
                    "depends_on": [
                        d.job_name
                        for d in job.parsed_dependencies
                        if d.job_name in job_names_in_wf
                    ],
                    "resources": job.resources.model_dump()
                    if isinstance(job, Job)
                    else {},
                }
                for job in submission_workflow.jobs
            ],
            "execution_order": [job.name for job in submission_workflow.jobs],
        }

    # Phase 4: Submit each job + persist + link to workflow_run + seed transition.
    #
    # Workflow Phase 2 (#135 web parity): hold the per-(profile, mount)
    # sync lock across the entire BFS so a concurrent /api/jobs or
    # ``srunx flow run`` can't rsync different bytes between our sync
    # and our submissions. Lock acquisition / rsync errors raise
    # HTTPException(502) tagged "Mount sync failed: …"; sbatch
    # failures from inside the body keep their existing
    # "sbatch failed for X" detail so the two failure classes stay
    # distinguishable in the API response.
    assert run_id is not None
    try:
        async with _hold_workflow_mounts_web(
            submission_workflow, runner, sync_required=True
        ) as locked_profile:
            try:
                await _submit_jobs_bfs(
                    submission_workflow,
                    scripts,
                    run_opts,
                    adapter,
                    conn=conn,
                    run_id=run_id,
                    profile=locked_profile,
                )
            except HTTPException as exc:
                reason = (
                    f"Submission failed: {exc.detail}"
                    if isinstance(exc.detail, str)
                    else "Submission failed"
                )

                # R2: cancel any jobs that were already accepted by sbatch before
                # the failure. Without this the workflow_run is marked failed
                # but cluster resources keep running the orphan jobs (and any
                # independent successors the DAG may have scheduled).
                def _load_orphan_ids() -> list[int]:
                    memberships = WorkflowRunJobRepository(conn).list_by_run(run_id)
                    return [m.job_id for m in memberships if m.job_id is not None]

                orphan_ids = await anyio.to_thread.run_sync(_load_orphan_ids)
                for jid in orphan_ids:
                    try:
                        await anyio.to_thread.run_sync(
                            lambda x=jid: adapter.cancel_job(int(x))  # type: ignore[misc]
                        )
                    except Exception:
                        logger.warning(
                            "Failed to cancel orphan SLURM job %s during workflow-run rollback",
                            jid,
                            exc_info=True,
                        )

                await anyio.to_thread.run_sync(functools.partial(_fail, reason))
                raise
    except HTTPException as exc:
        # Lock-acquisition failures land here (raised from
        # ``_hold_workflow_mounts_web`` before the body runs). Mark
        # the run as failed with the sync-failure reason. Body-raised
        # exceptions were already handled inside the inner try/except,
        # which re-raises after recording — we only need to mark and
        # rethrow here when the outer raise originates from the lock
        # acquisition itself.
        if isinstance(exc.detail, str) and exc.detail.startswith("Mount sync failed"):
            await anyio.to_thread.run_sync(functools.partial(_fail, exc.detail))
        raise

    # Phase 5: open the workflow_run watch so the poller can drive
    # status transitions going forward. ``workflow_runs.status='running'``
    # is deliberately NOT written here — the run record stays ``pending``
    # (as created above) until ``ActiveWatchPoller`` observes a child
    # job in RUNNING state (P1-1). Writing ``running`` eagerly would
    # race the poller's "otherwise→pending" rule and emit a spurious
    # ``running → pending`` transition (+ a
    # ``workflow_run.status_changed`` event to every subscriber) on the
    # very first cycle, while all children are still PENDING in SLURM.
    #
    # When ``notify`` is requested, also pair the watch with a
    # subscription for the chosen endpoint; the delivery poller then
    # fans ``workflow_run.status_changed`` events out to Slack/etc.
    def _open_watch() -> int | None:
        new_watch_id = watch_repo.create(
            kind="workflow_run",
            target_ref=f"workflow_run:{run_id}",
        )
        if run_opts.notify and run_opts.endpoint_id is not None:
            from srunx.db.repositories.endpoints import EndpointRepository
            from srunx.db.repositories.subscriptions import SubscriptionRepository

            endpoint = EndpointRepository(conn).get(run_opts.endpoint_id)
            if endpoint is None or endpoint.disabled_at is not None:
                # Non-fatal: the watch still exists, the run is open,
                # the user just won't get external notifications. Jobs
                # are already queued on the cluster — 4xx'ing here
                # would be misleading.
                logger.warning(
                    "workflow_run %s: requested endpoint_id=%s not usable "
                    "(missing or disabled); skipping subscription",
                    run_id,
                    run_opts.endpoint_id,
                )
                return new_watch_id
            SubscriptionRepository(conn).create(
                watch_id=new_watch_id,
                endpoint_id=run_opts.endpoint_id,
                preset=run_opts.preset,
            )
        return new_watch_id

    await anyio.to_thread.run_sync(_open_watch)

    def _load_final() -> dict[str, Any]:
        final_run = run_repo.get(run_id)  # type: ignore[arg-type]
        if final_run is None:
            return {"id": str(run_id), "status": "pending"}
        return _build_run_response(conn, final_run)

    return await anyio.to_thread.run_sync(_load_final)


@router.delete("/{name}")
async def delete_workflow(name: str, mount: str) -> dict[str, str]:
    """Delete a workflow YAML file."""
    if not _SAFE_NAME.match(name):
        raise HTTPException(status_code=422, detail="Invalid workflow name")
    yaml_path = _find_yaml(name, mount)  # raises 404 if not found
    await anyio.to_thread.run_sync(lambda: yaml_path.unlink())
    return {"status": "deleted", "name": name}


@router.get("/{name}")
async def get_workflow(name: str, mount: str) -> dict[str, Any]:
    if not _SAFE_NAME.match(name):
        raise HTTPException(status_code=422, detail="Invalid workflow name")

    yaml_path = _find_yaml(name, mount)
    try:

        def _load():
            import yaml as _yaml

            runner = WorkflowRunner.from_yaml(yaml_path)
            raw = _yaml.safe_load(yaml_path.read_text(encoding="utf-8"))
            return runner, raw.get("jobs", [])

        runner, raw_jobs = await anyio.to_thread.run_sync(_load)
        return _serialize_workflow(runner, raw_yaml_jobs=raw_jobs)
    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e)) from e
