"""Workflow management endpoints: /api/workflows/*"""

from __future__ import annotations

import re
import tempfile
from pathlib import Path
from typing import Any

import anyio
from fastapi import APIRouter, HTTPException

from srunx.exceptions import WorkflowValidationError
from srunx.runner import WorkflowRunner

from ..config import get_web_config
from ..state import run_registry

router = APIRouter(prefix="/api/workflows", tags=["workflows"])

_SAFE_NAME = re.compile(r"^[\w\-]+$")


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
