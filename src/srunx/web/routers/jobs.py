"""Job management endpoints: /api/jobs/*"""

from __future__ import annotations

from typing import Any

import anyio
from fastapi import APIRouter, Depends, HTTPException, Response
from pydantic import BaseModel

from ..deps import get_adapter
from ..ssh_adapter import SlurmSSHAdapter

router = APIRouter(prefix="/api/jobs", tags=["jobs"])


class JobSubmitRequest(BaseModel):
    name: str
    script_content: str
    job_name: str | None = None


@router.post("", status_code=201)
async def submit_job(
    req: JobSubmitRequest,
    adapter: SlurmSSHAdapter = Depends(get_adapter),
) -> dict[str, Any]:
    """Submit a new job to SLURM via SSH."""
    try:
        return await anyio.to_thread.run_sync(
            lambda: adapter.submit_job(
                req.script_content, job_name=req.job_name or req.name
            )
        )
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"sbatch failed: {e}") from e


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
    adapter: SlurmSSHAdapter = Depends(get_adapter),
) -> dict[str, str]:
    """Get stdout/stderr log contents for a job via SSH."""
    if job_id <= 0:
        raise HTTPException(status_code=400, detail="Invalid job ID")
    try:
        stdout, stderr = await anyio.to_thread.run_sync(
            lambda: adapter.get_job_output(job_id)
        )
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e
    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e)) from e
    return {"stdout": stdout, "stderr": stderr}
