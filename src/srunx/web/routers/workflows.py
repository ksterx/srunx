"""Workflow management endpoints: /api/workflows/*

Workflow runs are persisted in the ``workflow_runs`` + ``workflow_run_jobs``
tables. Status transitions are driven by
:class:`~srunx.pollers.active_watch_poller.ActiveWatchPoller`, which
aggregates child job statuses into the workflow run via an internal
``kind='workflow_run'`` watch created when the run starts.
"""

from __future__ import annotations

import functools
import re
import sqlite3
import tempfile
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
from srunx.runner import WorkflowRunner
from srunx.sweep import SweepSpec
from srunx.sweep.orchestrator import SweepOrchestrator

from ..deps import get_adapter, get_db_conn
from ..ssh_adapter import SlurmSSHAdapter

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


def _reject_python_args(yaml_content: str) -> None:
    """Reject YAML whose args section contains python: values (case-insensitive).

    Parses the YAML first so legitimate uses of "python:" in commands or
    comments are not blocked.
    """
    try:
        data = yaml.safe_load(yaml_content)
    except Exception:
        # Let downstream validation handle malformed YAML
        return

    if not isinstance(data, dict):
        return

    args = data.get("args")
    if not isinstance(args, dict):
        return

    for key, val in args.items():
        if isinstance(val, str) and "python:" in val.lower():
            raise HTTPException(
                status_code=422,
                detail=f"Arg '{key}' contains 'python:' prefix which is not allowed via web for security reasons",
            )


def _reject_python_in_mapping(mapping: dict[str, Any], *, source: str) -> None:
    """Reject ``python:`` values in structured request payloads.

    Used by the Web API sweep path where ``args_override`` and
    ``sweep.matrix`` come in as JSON (not YAML text). The YAML path
    uses :func:`_reject_python_args`.
    """
    for key, val in mapping.items():
        if isinstance(val, str) and "python:" in val.lower():
            raise HTTPException(
                status_code=422,
                detail=(
                    f"{source} key '{key}' contains 'python:' which is not "
                    "allowed via web for security reasons"
                ),
            )
        if isinstance(val, list):
            for i, element in enumerate(val):
                if isinstance(element, str) and "python:" in element.lower():
                    raise HTTPException(
                        status_code=422,
                        detail=(
                            f"{source} key '{key}' contains 'python:' at index "
                            f"{i} which is not allowed via web for security reasons"
                        ),
                    )


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
        if m.job_id is None:
            continue
        job = job_repo.get(m.job_id)
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

    def _finalize() -> None:
        run_repo.update_status(rid, "cancelled", completed_at=now_iso())
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

    _reject_python_args(yaml_content)

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

    _reject_python_args(yaml_content)

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

    # Reject python: args from web for security (case-insensitive)
    for val in body.args.values():
        if isinstance(val, str) and "python:" in val.lower():
            raise HTTPException(
                status_code=422,
                detail="Args with 'python:' values are not allowed via web for security reasons",
            )

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


async def _sync_mounts(
    workflow: Workflow,
    runner: WorkflowRunner,
    *,
    skip_sync: bool = False,
) -> Any:
    """Sync SSH mounts for the workflow. Returns the SSH profile or None.

    Raises :class:`HTTPException` on failure — the caller is responsible
    for marking the owning workflow run as failed.
    """
    from ..sync_utils import (
        get_current_profile,
        resolve_mounts_for_workflow,
        sync_mount_by_name,
    )

    profile = await anyio.to_thread.run_sync(get_current_profile)
    if profile is None or skip_sync:
        return profile

    jobs_raw: list[dict[str, Any]] = []
    for job in workflow.jobs:
        jd: dict[str, Any] = {"name": job.name}
        if hasattr(job, "work_dir"):
            jd["work_dir"] = getattr(job, "work_dir", "")
        jobs_raw.append(jd)

    mount_names = resolve_mounts_for_workflow(
        profile, jobs_raw, default_project=runner.default_project
    )
    for mount_name in mount_names:
        try:
            await anyio.to_thread.run_sync(
                lambda mn=mount_name: sync_mount_by_name(profile, mn)  # type: ignore[misc]
            )
        except Exception as exc:
            raise HTTPException(
                status_code=502,
                detail=f"Mount sync failed for '{mount_name}': {exc}",
            ) from exc
    return profile


def _prepare_render_context(
    yaml_path: Path,
    workflow: Workflow,
    name: str,
) -> tuple[dict[str, str], dict[str, dict[str, str]]]:
    """Parse raw YAML for template/extra args.

    Returns (job_template_map, job_extra_args).
    """
    import yaml as _yaml

    raw_data = _yaml.safe_load(yaml_path.read_text(encoding="utf-8"))
    raw_jobs = raw_data.get("jobs", [])

    job_template_map: dict[str, str] = {}
    job_extra_args: dict[str, dict[str, str]] = {}
    for rj in raw_jobs:
        rj_name = rj.get("name", "")
        if rj.get("template"):
            job_template_map[rj_name] = rj["template"]
        extras: dict[str, str] = {}
        if rj.get("srun_args"):
            extras["srun_args"] = rj["srun_args"]
        if rj.get("launch_prefix"):
            extras["launch_prefix"] = rj["launch_prefix"]
        if extras:
            job_extra_args[rj_name] = extras

    return job_template_map, job_extra_args


def _render_scripts(
    workflow: Workflow,
    mount: str,
    profile: Any,
    job_template_map: dict[str, str],
    job_extra_args: dict[str, dict[str, str]],
) -> dict[str, str]:
    """Render SLURM scripts for all jobs in the workflow."""
    from srunx.models import render_job_script
    from srunx.template import TEMPLATES

    templates_dir = Path(__file__).resolve().parent.parent.parent / "templates"
    scripts: dict[str, str] = {}

    with tempfile.TemporaryDirectory() as tmpdir:
        for job in workflow.jobs:
            if isinstance(job, Job):
                tpl_name = job_template_map.get(job.name, "base")
                tpl_info = TEMPLATES.get(tpl_name)
                if tpl_info:
                    template_path = templates_dir / tpl_info["path"]
                else:
                    template_path = templates_dir / "base.slurm.jinja"

                extras = job_extra_args.get(job.name, {})
                rendered_path = render_job_script(
                    template_path,
                    job,
                    output_dir=tmpdir,
                    extra_srun_args=extras.get("srun_args"),
                    extra_launch_prefix=extras.get("launch_prefix"),
                )
                scripts[job.name] = Path(rendered_path).read_text()
            else:
                script_path = Path(job.script_path).resolve()  # type: ignore[union-attr]
                allowed_roots = [_workflow_dir(mount).resolve()]
                if profile:
                    allowed_roots.extend(
                        Path(m.local).resolve() for m in profile.mounts
                    )
                if not any(script_path.is_relative_to(root) for root in allowed_roots):
                    raise HTTPException(
                        403,
                        f"Script path '{job.script_path}' is outside allowed directories",  # type: ignore[union-attr]
                    )
                scripts[job.name] = script_path.read_text()
    return scripts


async def _submit_jobs_bfs(
    workflow: Workflow,
    scripts: dict[str, str],
    run_opts: WorkflowRunRequest,
    adapter: SlurmSSHAdapter,
    *,
    conn: sqlite3.Connection,
    run_id: int,
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

        try:
            result = await anyio.to_thread.run_sync(
                lambda s=scripts[current_name], n=current_name, d=dependency: (  # type: ignore[misc]
                    adapter.submit_job(s, job_name=n, dependency=d)
                )
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
        def _persist(
            jid: int = slurm_id,
            jname: str = current_name,
            job_obj: Job | ShellJob = current_job,
            deps: list[str] = depends_on,
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
                )
                wrj_repo.create(
                    workflow_run_id=run_id,
                    job_name=jname,
                    depends_on=deps or None,
                    job_id=jid,
                )

        await anyio.to_thread.run_sync(_persist)

        for dep_name in dependents[current_name]:
            in_degree[dep_name] -= 1
            if in_degree[dep_name] == 0:
                queue.append(dep_name)

    return submitted


async def _dispatch_sweep(
    *,
    yaml_path: Path,
    name: str,
    body: WorkflowRunRequest,
) -> dict[str, Any]:
    """Materialize + spawn a :class:`SweepOrchestrator` for the request.

    Matrix validation (non-scalar values, reserved axis names, oversize
    matrices) is routed through :class:`WorkflowValidationError` by
    ``expand_matrix`` and surfaced as HTTP 422.
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

    endpoint_id: int | None = None
    if body.notify and body.endpoint_id is not None:
        endpoint_id = body.endpoint_id
    elif body.notify and body.endpoint_id is None:
        # Non-fatal — matches the non-sweep path's contract: the sweep
        # still runs, but no external deliveries are wired.
        logger.warning("sweep run: notify=true with no endpoint_id; skipping")

    orchestrator = SweepOrchestrator(
        workflow_yaml_path=yaml_path,
        workflow_data={"name": name, **workflow_data},
        args_override=body.args_override or None,
        sweep_spec=sweep_spec,
        submission_source="web",
        endpoint_id=endpoint_id,
        preset=body.preset,
    )

    try:
        sweep_run = await orchestrator.arun()
    except WorkflowValidationError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except SweepExecutionError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    return {
        "sweep_run_id": sweep_run.id,
        "status": sweep_run.status,
        "cell_count": sweep_run.cell_count,
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
    _ = request  # referenced for route-signature compatibility only
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
        _reject_python_in_mapping(run_opts.args_override, source="args_override")
    if run_opts.sweep is not None:
        _reject_python_in_mapping(run_opts.sweep.matrix, source="sweep.matrix")

    yaml_path = _find_yaml(name, mount)

    # Sweep branch: materialize + execute N cells through the orchestrator.
    # The per-cell workflow_run rows are created inside the orchestrator's
    # happy-path TX; we return the ``sweep_run_id`` + 202 so the client
    # can poll ``/api/sweep_runs/{id}``.
    if run_opts.sweep is not None:
        return await _dispatch_sweep(
            yaml_path=yaml_path,
            name=name,
            body=run_opts,
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
        if run_id is not None:
            run_repo.update_status(
                run_id, "failed", error=reason, completed_at=now_iso()
            )

    # Phase 1: Sync mounts
    try:
        profile = await _sync_mounts(workflow, runner, skip_sync=run_id is None)
    except HTTPException as exc:
        reason = f"Mount sync failed: {exc.detail}"
        await anyio.to_thread.run_sync(functools.partial(_fail, reason))
        raise

    # Phase 2: Prepare render context and render scripts
    job_template_map, job_extra_args = await anyio.to_thread.run_sync(
        lambda: _prepare_render_context(yaml_path, workflow, name)
    )

    try:
        scripts = await anyio.to_thread.run_sync(
            lambda: _render_scripts(
                workflow,
                mount,
                profile,
                job_template_map,
                job_extra_args,
            )
        )
    except Exception as exc:
        reason = f"Script rendering failed: {exc}"
        await anyio.to_thread.run_sync(functools.partial(_fail, reason))
        raise HTTPException(status_code=500, detail=reason) from exc

    # Phase 3: Dry run early return
    if run_opts.dry_run:
        job_names_in_wf = {job.name for job in workflow.jobs}
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
                for job in workflow.jobs
            ],
            "execution_order": [job.name for job in workflow.jobs],
        }

    # Phase 4: Submit each job + persist + link to workflow_run + seed transition
    assert run_id is not None
    try:
        await _submit_jobs_bfs(
            workflow, scripts, run_opts, adapter, conn=conn, run_id=run_id
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
