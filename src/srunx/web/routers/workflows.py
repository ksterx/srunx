"""Workflow management endpoints: /api/workflows/*"""

from __future__ import annotations

import re
import tempfile
from pathlib import Path
from typing import Any

import anyio
import yaml
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from srunx.exceptions import WorkflowValidationError
from srunx.models import Job, JobEnvironment, JobResource, ShellJob, Workflow
from srunx.runner import WorkflowRunner

from ..config import get_web_config
from ..state import run_registry

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


def _workflow_to_yaml(name: str, jobs_data: list[dict[str, Any]]) -> str:
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

    doc: dict[str, Any] = {"name": name, "jobs": serialized_jobs}
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
    return {"name": wf.name, "jobs": jobs}


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
    yaml_content = _workflow_to_yaml(name, jobs_raw)
    dest = d / f"{name}.yaml"
    await anyio.to_thread.run_sync(lambda: dest.write_text(yaml_content))

    # Re-load via WorkflowRunner to return the canonical serialized form
    runner = await anyio.to_thread.run_sync(lambda: WorkflowRunner.from_yaml(dest))
    return _serialize_workflow(runner)


@router.post("/{name}/run")
async def run_workflow(name: str) -> dict[str, Any]:
    """Start a workflow run.

    NOTE: Full workflow execution via SSH is not yet supported.
    This creates a run record for tracking purposes.
    Actual execution should be done via `srunx flow run` on the remote.
    """
    if not _SAFE_NAME.match(name):
        raise HTTPException(status_code=422, detail="Invalid workflow name")
    _find_yaml(name)  # validate exists

    run = run_registry.create(name)
    return run.model_dump()


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
