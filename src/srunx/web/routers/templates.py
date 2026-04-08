"""Template management endpoints: /api/templates"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from srunx.template import get_template_info, get_template_path, list_templates

from ..deps import get_adapter
from ..ssh_adapter import SlurmSSHAdapter  # noqa: F811

router = APIRouter(prefix="/api/templates", tags=["templates"])


class TemplateListItem(BaseModel):
    name: str
    description: str
    use_case: str


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
    adapter: SlurmSSHAdapter = Depends(get_adapter),
) -> dict[str, Any]:
    """Submit a job using a template, or preview the rendered script."""
    import tempfile

    import anyio

    from srunx.models import Job, JobEnvironment, JobResource, render_job_script

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
                    lambda: sync_mount_by_name(profile, mount_name)
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
