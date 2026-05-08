"""Template management endpoints: /api/templates"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from srunx.runtime.templates import (
    get_template_info,
    get_template_path,
    list_templates,
)
from srunx.slurm.clients.ssh import SlurmSSHClient

from ..deps import get_adapter

router = APIRouter(prefix="/api/templates", tags=["templates"])


class TemplateListItem(BaseModel):
    name: str
    description: str
    use_case: str
    user_defined: bool = False


class TemplateDetail(BaseModel):
    name: str
    description: str
    use_case: str
    content: str


@router.get("")
async def list_all_templates() -> list[TemplateListItem]:
    """List all available job templates."""
    return [
        TemplateListItem(
            name=t["name"],
            description=t["description"],
            use_case=t["use_case"],
            user_defined=t.get("user_defined") == "true",
        )
        for t in list_templates()
    ]


@router.get("/{name}")
async def get_template(name: str) -> TemplateDetail:
    """Get template info and raw Jinja content."""
    try:
        info = get_template_info(name)
        template_file = Path(get_template_path(name))
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e

    content = template_file.read_text(encoding="utf-8")
    return TemplateDetail(
        name=info["name"],
        description=info["description"],
        use_case=info["use_case"],
        content=content,
    )


class TemplateApplyRequest(BaseModel):
    command: list[str]
    job_name: str = "job"
    resources: dict[str, Any] | None = None
    environment: dict[str, Any] | None = None
    work_dir: str | None = None
    log_dir: str | None = None
    mount_name: str | None = None
    preview_only: bool = False


@router.post("/{name}/apply")
async def apply_template(
    name: str,
    req: TemplateApplyRequest,
    adapter: SlurmSSHClient = Depends(get_adapter),
) -> dict[str, Any]:
    """Submit a job using a template, or preview the rendered script."""
    import tempfile

    import anyio

    from srunx.domain import Job, JobEnvironment, JobResource
    from srunx.runtime.rendering import render_job_script

    try:
        template_path = get_template_path(name)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e

    resources = JobResource(**(req.resources or {}))
    environment = JobEnvironment(**(req.environment or {}))
    job = Job(
        name=req.job_name,
        command=req.command,
        resources=resources,
        environment=environment,
        work_dir=req.work_dir or ".",
        log_dir=req.log_dir or ".",
    )

    with tempfile.TemporaryDirectory() as tmpdir:
        script_path = await anyio.to_thread.run_sync(
            lambda: render_job_script(template_path, job, output_dir=tmpdir)
        )
        script_content = Path(script_path).read_text()

    if req.preview_only:
        return {"script": script_content, "template_used": name}

    # Sync mount before submission if requested
    if req.mount_name:
        from ..sync_utils import get_current_profile, sync_mount_by_name

        profile = await anyio.to_thread.run_sync(get_current_profile)
        if profile:
            mount_name = req.mount_name
            try:
                await anyio.to_thread.run_sync(
                    lambda: sync_mount_by_name(profile, mount_name, delete=True)
                )
            except ValueError as exc:
                raise HTTPException(
                    status_code=404, detail=f"Mount '{mount_name}' not found"
                ) from exc
            except RuntimeError as exc:
                raise HTTPException(
                    status_code=502, detail=f"Mount sync failed: {exc}"
                ) from exc

    try:
        result = await anyio.to_thread.run_sync(
            lambda: adapter.submit_job(script_content, job_name=req.job_name)
        )
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"sbatch failed: {e}") from e

    return result


# ── User template CRUD ───────────────────────────


class TemplateCreateRequest(BaseModel):
    name: str
    description: str
    use_case: str
    content: str


class TemplateUpdateRequest(BaseModel):
    description: str | None = None
    use_case: str | None = None
    content: str | None = None


@router.post("", status_code=201)
async def create_template(req: TemplateCreateRequest) -> TemplateListItem:
    """Create a new user-defined template."""
    from srunx.runtime.templates import create_user_template

    try:
        info = create_user_template(
            name=req.name,
            description=req.description,
            use_case=req.use_case,
            content=req.content,
        )
    except ValueError as e:
        raise HTTPException(status_code=409, detail=str(e)) from e

    return TemplateListItem(
        name=info["name"],
        description=info["description"],
        use_case=info["use_case"],
    )


@router.put("/{name}")
async def update_template(name: str, req: TemplateUpdateRequest) -> TemplateDetail:
    """Update a user-defined template."""
    from srunx.runtime.templates import update_user_template

    try:
        info = update_user_template(
            name=name,
            description=req.description,
            use_case=req.use_case,
            content=req.content,
        )
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e

    template_file = Path(get_template_path(name))
    content = template_file.read_text(encoding="utf-8")
    return TemplateDetail(
        name=info["name"],
        description=info["description"],
        use_case=info["use_case"],
        content=content,
    )


@router.delete("/{name}", status_code=204)
async def delete_template(name: str) -> None:
    """Delete a user-defined template."""
    from srunx.runtime.templates import delete_user_template

    try:
        delete_user_template(name)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
