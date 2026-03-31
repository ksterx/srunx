"""Workflow management endpoints: /api/workflows/*"""

from __future__ import annotations

import logging
import re
import tempfile
from pathlib import Path
from typing import Any

import anyio
import yaml
from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, Field

from srunx.exceptions import WorkflowValidationError
from srunx.models import (
    Job,
    JobEnvironment,
    JobResource,
    ShellJob,
    Workflow,
)
from srunx.runner import WorkflowRunner

from ..config import get_web_config
from ..deps import get_adapter
from ..ssh_adapter import SlurmSSHAdapter
from ..state import run_registry

_logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/workflows", tags=["workflows"])

_SAFE_NAME = re.compile(r"^[\w\-]+$")
_RESERVED_NAMES = frozenset({"new"})


# ── Request models ───────────────────────────────────


class WorkflowJobInput(BaseModel):
    name: str
    command: list[str]
    depends_on: list[str] = []
    resources: dict[str, Any] | None = None
    environment: dict[str, Any] | None = None
    work_dir: str | None = None
    log_dir: str | None = None
    retry: int | None = None
    retry_delay: int | None = None


class WorkflowCreateRequest(BaseModel):
    name: str = Field(..., pattern=r"^[\w\-]+$")
    jobs: list[WorkflowJobInput]
    default_project: str | None = None


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
        if jd.get("work_dir"):
            entry["work_dir"] = jd["work_dir"]
        if jd.get("log_dir"):
            entry["log_dir"] = jd["log_dir"]
        if jd.get("retry") is not None:
            entry["retry"] = jd["retry"]
        if jd.get("retry_delay") is not None:
            entry["retry_delay"] = jd["retry_delay"]

        serialized_jobs.append(entry)

    doc: dict[str, Any] = {"name": name}
    if default_project:
        doc["default_project"] = default_project
    doc["jobs"] = serialized_jobs
    return yaml.dump(doc, default_flow_style=False, sort_keys=False)


def _workflow_dir() -> Path:
    return get_web_config().workflow_dir


def _find_yaml(name: str) -> Path:
    d = _workflow_dir()
    for ext in (".yaml", ".yml"):
        p = d / f"{name}{ext}"
        if p.exists():
            return p
    raise HTTPException(status_code=404, detail=f"Workflow '{name}' not found")


def _reject_python_args(yaml_content: str) -> None:
    if "python:" in yaml_content:
        raise HTTPException(
            status_code=422,
            detail="Workflow YAML contains 'python:' args which are not allowed via web for security reasons",
        )


def _serialize_workflow(runner: WorkflowRunner) -> dict[str, Any]:
    wf = runner.workflow
    jobs: list[dict[str, Any]] = []
    for job in wf.jobs:
        d: dict[str, Any] = {
            "name": job.name,
            "job_id": job.job_id,
            "status": job._status.value,
            "depends_on": job.depends_on,
        }
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
    if runner.default_project:
        result["default_project"] = runner.default_project
    return result


@router.get("")
async def list_workflows() -> list[dict[str, Any]]:
    d = _workflow_dir()
    if not d.exists():
        return []

    results: list[dict[str, Any]] = []
    for p in sorted(d.glob("*.y*ml")):
        try:
            runner = await anyio.to_thread.run_sync(
                lambda _p=p: WorkflowRunner.from_yaml(_p)  # type: ignore[misc]
            )
            results.append(_serialize_workflow(runner))
        except Exception:
            continue
    return results


@router.get("/runs")
async def list_runs(name: str | None = None) -> list[dict[str, Any]]:
    runs = run_registry.list_runs(name)
    return [r.model_dump() for r in runs]


@router.get("/runs/{run_id}")
async def get_run(run_id: str) -> dict[str, Any]:
    """Get the status and details of a single workflow run."""
    run = run_registry.get(run_id)
    if run is None:
        raise HTTPException(status_code=404, detail=f"Run '{run_id}' not found")
    return run.model_dump()


@router.post("/runs/{run_id}/cancel")
async def cancel_run(
    run_id: str,
    adapter: SlurmSSHAdapter = Depends(get_adapter),
) -> dict[str, Any]:
    """Cancel all jobs in a running workflow."""
    run = run_registry.get(run_id)
    if run is None:
        raise HTTPException(404, f"Run '{run_id}' not found")
    if run.status in ("completed", "failed", "cancelled"):
        raise HTTPException(422, f"Run is already {run.status}")
    errors: list[str] = []
    for job_name, job_id in run.job_ids.items():
        try:
            await anyio.to_thread.run_sync(
                lambda jid=job_id: adapter.cancel_job(int(jid))  # type: ignore[misc]
            )
        except Exception as e:
            errors.append(f"{job_name}: {e}")

    run_registry.complete_run(run_id, "cancelled")

    result: dict[str, Any] = {"status": "cancelled", "run_id": run_id}
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

    if not yaml_content or not filename:
        raise HTTPException(
            status_code=422, detail="Both 'yaml' and 'filename' required"
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

    d = _workflow_dir()
    d.mkdir(parents=True, exist_ok=True)
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

    # Reserved name guard
    if name in _RESERVED_NAMES:
        raise HTTPException(
            status_code=422,
            detail=f"Workflow name '{name}' is reserved",
        )

    # Check for existing workflow with the same name
    d = _workflow_dir()
    d.mkdir(parents=True, exist_ok=True)
    for ext in (".yaml", ".yml"):
        if (d / f"{name}{ext}").exists():
            raise HTTPException(
                status_code=409,
                detail=f"Workflow '{name}' already exists",
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
        name, jobs_raw, default_project=body.default_project
    )
    dest = d / f"{name}.yaml"
    await anyio.to_thread.run_sync(lambda: dest.write_text(yaml_content))

    # Re-load via WorkflowRunner to return the canonical serialized form
    runner = await anyio.to_thread.run_sync(lambda: WorkflowRunner.from_yaml(dest))
    return _serialize_workflow(runner)


async def _monitor_run(
    run_id: str, job_ids: dict[str, str], adapter: SlurmSSHAdapter
) -> None:
    """Background task: poll SLURM job statuses and update run registry."""
    terminal = {"COMPLETED", "FAILED", "CANCELLED", "TIMEOUT"}
    consecutive_errors = 0
    MAX_ERRORS = 30  # ~5 minutes at 10s interval

    while True:
        all_terminal = True
        for job_name, job_id in job_ids.items():
            try:
                status = await anyio.to_thread.run_sync(
                    lambda jid=job_id: adapter.get_job_status(int(jid))  # type: ignore[misc]
                )
                run_registry.update_job_status(run_id, job_name, status)
                if status not in terminal:
                    all_terminal = False
                consecutive_errors = 0  # reset on any success
            except Exception:
                consecutive_errors += 1
                all_terminal = False  # keep polling on transient errors

        if consecutive_errors >= MAX_ERRORS:
            run_registry.fail_run(
                run_id,
                "Lost connection to SLURM cluster after repeated failures",
            )
            break

        if all_terminal:
            run = run_registry.get(run_id)
            if run is not None:
                statuses = set(run.job_statuses.values())
                if statuses <= {"COMPLETED"}:
                    run_registry.complete_run(run_id, "completed")
                else:
                    run_registry.complete_run(run_id, "failed")
            break

        await anyio.sleep(10)


@router.post("/{name}/run", status_code=202)
async def run_workflow(
    name: str,
    request: Request,
    adapter: SlurmSSHAdapter = Depends(get_adapter),
) -> dict[str, Any]:
    """Run a workflow: sync mounts, submit jobs with SLURM dependencies."""
    from collections import deque

    from srunx.models import render_job_script

    from ..sync_utils import resolve_mounts_for_workflow, sync_mount_by_name

    if not _SAFE_NAME.match(name):
        raise HTTPException(status_code=422, detail="Invalid workflow name")

    yaml_path = _find_yaml(name)

    # Load workflow
    runner = await anyio.to_thread.run_sync(lambda: WorkflowRunner.from_yaml(yaml_path))
    workflow = runner.workflow

    # Create run record
    run = run_registry.create(name)
    run_id = run.id
    run_registry.update_status(run_id, "syncing")

    # ── Sync mounts ─────────────────────────────────────────────────
    def _get_current_profile():
        from srunx.ssh.core.config import ConfigManager

        config = get_web_config()
        cm = ConfigManager()
        profile_name = config.ssh_profile
        if not profile_name:
            profile_name = cm.get_current_profile_name()
        if not profile_name:
            return None
        return cm.get_profile(profile_name)

    profile = await anyio.to_thread.run_sync(_get_current_profile)

    if profile is not None:
        # Build raw jobs data from the loaded workflow for mount resolution
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
                run_registry.fail_run(
                    run_id, f"Sync failed for mount '{mount_name}': {exc}"
                )
                raise HTTPException(
                    status_code=502,
                    detail=f"Mount sync failed for '{mount_name}': {exc}",
                ) from exc

    # ── Render SLURM scripts ────────────────────────────────────────
    run_registry.update_status(run_id, "submitting")

    template_path = (
        Path(__file__).resolve().parent.parent.parent
        / "templates"
        / "advanced.slurm.jinja"
    )

    # Only render Job instances (not ShellJob)
    job_map: dict[str, Job | ShellJob] = {job.name: job for job in workflow.jobs}

    def _render_all_scripts() -> dict[str, str]:
        scripts: dict[str, str] = {}
        with tempfile.TemporaryDirectory() as tmpdir:
            for job in workflow.jobs:
                if isinstance(job, Job):
                    rendered_path = render_job_script(
                        template_path, job, output_dir=tmpdir
                    )
                    scripts[job.name] = Path(rendered_path).read_text()
                else:
                    # ShellJob: read the script directly
                    # Validate script_path is within allowed boundaries
                    script_path = Path(job.script_path).resolve()  # type: ignore[union-attr]
                    allowed_roots = [_workflow_dir().resolve()]
                    if profile:
                        allowed_roots.extend(
                            Path(m.local).resolve() for m in profile.mounts
                        )
                    if not any(
                        script_path.is_relative_to(root) for root in allowed_roots
                    ):
                        raise HTTPException(
                            403,
                            f"Script path '{job.script_path}' is outside allowed directories",  # type: ignore[union-attr]
                        )
                    scripts[job.name] = script_path.read_text()
        return scripts

    try:
        scripts = await anyio.to_thread.run_sync(_render_all_scripts)
    except Exception as exc:
        run_registry.fail_run(run_id, f"Script rendering failed: {exc}")
        raise HTTPException(
            status_code=500, detail=f"Script rendering failed: {exc}"
        ) from exc

    # ── Topological submit (BFS) ────────────────────────────────────
    dependents: dict[str, list[str]] = {job.name: [] for job in workflow.jobs}
    in_degree: dict[str, int] = {
        job.name: len(job.parsed_dependencies) for job in workflow.jobs
    }

    for job in workflow.jobs:
        for dep in job.parsed_dependencies:
            dependents[dep.job_name].append(job.name)

    queue: deque[str] = deque(
        job.name for job in workflow.jobs if in_degree[job.name] == 0
    )
    submitted: dict[str, str] = {}  # job_name -> slurm_job_id

    while queue:
        current_name = queue.popleft()
        current_job = job_map[current_name]

        # Build dependency flag from parent SLURM IDs
        dep_parts: list[str] = []
        for dep in current_job.parsed_dependencies:
            parent_id = submitted[dep.job_name]
            dep_parts.append(f"{dep.dep_type}:{parent_id}")
        dependency = ",".join(dep_parts) if dep_parts else None

        try:
            result = await anyio.to_thread.run_sync(
                lambda s=scripts[current_name], n=current_name, d=dependency: (  # type: ignore[misc]
                    adapter.submit_job(s, job_name=n, dependency=d)
                )
            )
            job_id_str = str(result["job_id"])
            submitted[current_name] = job_id_str
        except Exception as exc:
            run_registry.set_job_ids(run_id, submitted)
            run_registry.fail_run(
                run_id,
                f"Submission failed for job '{current_name}': {exc}",
            )
            raise HTTPException(
                status_code=502,
                detail=f"sbatch failed for '{current_name}': {exc}",
            ) from exc

        # Enqueue dependents whose in-degree reaches 0
        for dep_name in dependents[current_name]:
            in_degree[dep_name] -= 1
            if in_degree[dep_name] == 0:
                queue.append(dep_name)

    # ── Finalize and start monitor ──────────────────────────────────
    run_registry.set_job_ids(run_id, submitted)
    run_registry.update_status(run_id, "running")

    # Initialize job statuses to PENDING
    for jname in submitted:
        run_registry.update_job_status(run_id, jname, "PENDING")

    # Start background monitor (task_group may not exist in test environments)
    tg = getattr(request.app.state, "task_group", None)
    if tg is not None:
        tg.start_soon(_monitor_run, run_id, submitted, adapter)

    # Re-fetch the run to return the latest state
    final_run = run_registry.get(run_id)
    return final_run.model_dump() if final_run else {"id": run_id, "status": "running"}


@router.delete("/{name}")
async def delete_workflow(name: str) -> dict[str, str]:
    """Delete a workflow YAML file."""
    if not _SAFE_NAME.match(name):
        raise HTTPException(status_code=422, detail="Invalid workflow name")
    yaml_path = _find_yaml(name)  # raises 404 if not found
    await anyio.to_thread.run_sync(lambda: yaml_path.unlink())
    return {"status": "deleted", "name": name}


@router.get("/{name}")
async def get_workflow(name: str) -> dict[str, Any]:
    if not _SAFE_NAME.match(name):
        raise HTTPException(status_code=422, detail="Invalid workflow name")

    yaml_path = _find_yaml(name)
    try:
        runner = await anyio.to_thread.run_sync(
            lambda: WorkflowRunner.from_yaml(yaml_path)
        )
        return _serialize_workflow(runner)
    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e)) from e
