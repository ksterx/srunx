"""Job management endpoints: /api/jobs/*"""

from __future__ import annotations

import logging
from typing import Any

import anyio
from fastapi import APIRouter, Depends, HTTPException, Response
from pydantic import BaseModel

from ..deps import get_adapter
from ..ssh_adapter import SlurmSSHAdapter

router = APIRouter(prefix="/api/jobs", tags=["jobs"])
_logger = logging.getLogger(__name__)


class ScriptPreviewRequest(BaseModel):
    name: str = "preview"
    command: list[str]
    resources: dict[str, str | int | None] | None = None
    environment: dict[str, str | dict | None] | None = None
    work_dir: str | None = None
    log_dir: str | None = None
    template_name: str | None = None


class ScriptPreviewResponse(BaseModel):
    script: str
    template_used: str


@router.post("/preview")
async def preview_script(req: ScriptPreviewRequest) -> ScriptPreviewResponse:
    """Render a SLURM script from job config without submitting."""
    import tempfile

    from srunx.models import Job, JobEnvironment, JobResource
    from srunx.template import get_template_path

    template_name = req.template_name or "advanced"
    try:
        template_path = get_template_path(template_name)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e

    resources = JobResource(**(req.resources or {}))
    environment = JobEnvironment(**(req.environment or {}))
    job = Job(
        name=req.name,
        command=req.command,
        resources=resources,
        environment=environment,
        work_dir=req.work_dir or ".",
        log_dir=req.log_dir or ".",
    )

    from srunx.models import render_job_script

    with tempfile.TemporaryDirectory() as tmpdir:
        script_path = await anyio.to_thread.run_sync(
            lambda: render_job_script(template_path, job, output_dir=tmpdir)
        )
        from pathlib import Path

        script_content = Path(script_path).read_text()

    return ScriptPreviewResponse(script=script_content, template_used=template_name)


class JobSubmitRequest(BaseModel):
    name: str
    script_content: str
    job_name: str | None = None
    mount_name: str | None = None
    notify_slack: bool = False


@router.post("", status_code=201)
async def submit_job(
    req: JobSubmitRequest,
    adapter: SlurmSSHAdapter = Depends(get_adapter),
) -> dict[str, Any]:
    """Submit a new job to SLURM via SSH.

    If *mount_name* is provided, the corresponding mount is synced
    via rsync before submission.
    """
    # Sync mount before submission if requested
    if req.mount_name:
        from ..sync_utils import get_current_profile, sync_mount_by_name

        mount_name = req.mount_name
        profile = await anyio.to_thread.run_sync(get_current_profile)
        if profile is None:
            raise HTTPException(
                status_code=503,
                detail="No SSH profile configured; cannot sync mount",
            )
        try:
            _logger.info("Syncing mount '%s' before job submission", mount_name)
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
            lambda: adapter.submit_job(
                req.script_content, job_name=req.job_name or req.name
            )
        )
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"sbatch failed: {e}") from e

    # Send Slack notification if requested
    if req.notify_slack and result.get("job_id"):
        try:
            from srunx.callbacks import SlackCallback
            from srunx.config import get_config
            from srunx.models import Job

            cfg = get_config()
            webhook_url = cfg.notifications.slack_webhook_url
            if webhook_url:
                job = Job(
                    name=req.job_name or req.name,
                    job_id=result["job_id"],
                    command=[],
                )
                slack = SlackCallback(webhook_url=webhook_url)
                await anyio.to_thread.run_sync(lambda: slack.on_job_submitted(job))
        except Exception:
            _logger.warning("Failed to send Slack notification", exc_info=True)

    return result


@router.get("")
async def list_jobs(
    adapter: SlurmSSHAdapter = Depends(get_adapter),
) -> list[dict[str, Any]]:
    """List all user jobs from SLURM queue via SSH."""
    try:
        return await anyio.to_thread.run_sync(adapter.list_jobs)
    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e)) from e


@router.get("/{job_id}")
async def get_job(
    job_id: int,
    adapter: SlurmSSHAdapter = Depends(get_adapter),
) -> dict[str, Any]:
    """Get a single job's status via SSH."""
    if job_id <= 0:
        raise HTTPException(status_code=400, detail="Invalid job ID")
    try:
        return await anyio.to_thread.run_sync(lambda: adapter.get_job(job_id))
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e
    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e)) from e


@router.delete("/{job_id}", status_code=204)
async def cancel_job(
    job_id: int,
    adapter: SlurmSSHAdapter = Depends(get_adapter),
) -> Response:
    """Cancel a SLURM job via SSH."""
    if job_id <= 0:
        raise HTTPException(status_code=400, detail="Invalid job ID")
    try:
        await anyio.to_thread.run_sync(lambda: adapter.cancel_job(job_id))
    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e)) from e
    return Response(status_code=204)


@router.get("/{job_id}/logs")
async def get_job_logs(
    job_id: int,
    stdout_offset: int = 0,
    stderr_offset: int = 0,
    adapter: SlurmSSHAdapter = Depends(get_adapter),
) -> dict[str, str | int]:
    """Get stdout/stderr log contents for a job via SSH.

    Pass ``stdout_offset`` / ``stderr_offset`` (byte positions) to
    receive only the **new** content since the last read.
    """
    if job_id <= 0:
        raise HTTPException(status_code=400, detail="Invalid job ID")
    try:
        (
            stdout,
            stderr,
            new_stdout_offset,
            new_stderr_offset,
        ) = await anyio.to_thread.run_sync(
            lambda: adapter.get_job_output(
                job_id,
                stdout_offset=stdout_offset,
                stderr_offset=stderr_offset,
            )
        )
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e
    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e)) from e
    return {
        "stdout": stdout,
        "stderr": stderr,
        "stdout_offset": new_stdout_offset,
        "stderr_offset": new_stderr_offset,
    }
