"""Job management endpoints: /api/jobs/*"""

from __future__ import annotations

import sqlite3
from typing import Any

import anyio
from fastapi import APIRouter, Depends, HTTPException, Response
from pydantic import BaseModel, Field, model_validator

from srunx.db.repositories.deliveries import DeliveryRepository
from srunx.db.repositories.endpoints import EndpointRepository
from srunx.db.repositories.events import EventRepository
from srunx.db.repositories.job_state_transitions import (
    JobStateTransitionRepository,
)
from srunx.db.repositories.jobs import JobRepository
from srunx.db.repositories.subscriptions import SubscriptionRepository
from srunx.db.repositories.watches import WatchRepository
from srunx.logging import get_logger
from srunx.notifications.service import NotificationService

from ..deps import get_adapter, get_db_conn
from ..ssh_adapter import SlurmSSHAdapter

router = APIRouter(prefix="/api/jobs", tags=["jobs"])
logger = get_logger(__name__)


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

    template_name = req.template_name or "base"
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
    """Request body for submitting a job via the Web UI.

    ``notify_slack`` (DEPRECATED) is retained for backward compatibility with
    older frontend builds but has no effect beyond logging a warning. Prefer
    the ``notify`` + ``endpoint_id`` + ``preset`` triplet.
    """

    name: str
    script_content: str
    job_name: str | None = None
    mount_name: str | None = None

    # DEPRECATED: keep for API backward-compat but behaviour is a no-op.
    notify_slack: bool = False

    # New notification-watch fields (optional; all three must be present
    # together for a watch to be created on submit).
    notify: bool = False
    endpoint_id: int | None = Field(default=None, gt=0)
    preset: str = "terminal"

    @model_validator(mode="after")
    def _check_notify(self) -> JobSubmitRequest:
        if self.notify and self.endpoint_id is None:
            raise ValueError("notify=true requires endpoint_id")
        return self


def _validate_notify_endpoint(conn: sqlite3.Connection, req: JobSubmitRequest) -> None:
    """Validate the notification endpoint BEFORE SLURM submission.

    Called on the request path so that a bad ``endpoint_id`` fails the
    request without leaking a SLURM job.
    """
    if not req.notify or req.endpoint_id is None:
        return
    endpoint = EndpointRepository(conn).get(req.endpoint_id)
    if endpoint is None:
        raise HTTPException(
            status_code=404, detail=f"endpoint {req.endpoint_id} not found"
        )
    if endpoint.disabled_at is not None:
        raise HTTPException(
            status_code=422, detail=f"endpoint {req.endpoint_id} is disabled"
        )


def _record_and_watch(
    conn: sqlite3.Connection,
    *,
    job_id: int,
    job_name: str,
    req: JobSubmitRequest,
    adapter: SlurmSSHAdapter,
) -> None:
    """Persist the submission record and (optionally) create a notification watch.

    Executes inside a single IMMEDIATE transaction so the ``jobs`` insert,
    watch/subscription rows, initial ``job_state_transitions`` seed, and
    ``job.submitted`` event (plus fan-out) appear atomically.

    Endpoint validation is done BEFORE this function is called (in the
    request path, pre-sbatch) so that a 4xx never strands a SLURM job.

    The transport axis (local vs ``ssh:<profile>``) is taken from the
    adapter so the watch / event rows are keyed to the same transport
    the poller will query — without this, an SSH-submitted job's watch
    would be written with ``scheduler_key='local'`` and the poller
    would look it up on the wrong cluster.
    """
    scheduler_key = adapter.scheduler_key
    if scheduler_key.startswith("ssh:"):
        transport_type = "ssh"
        profile_name: str | None = scheduler_key[len("ssh:") :]
    else:
        transport_type = "local"
        profile_name = None

    job_repo = JobRepository(conn)
    watch_repo = WatchRepository(conn)
    sub_repo = SubscriptionRepository(conn)
    event_repo = EventRepository(conn)
    transition_repo = JobStateTransitionRepository(conn)

    conn.execute("BEGIN IMMEDIATE")
    try:
        # R5.1 bug fix: web-submitted jobs now hit the history.
        job_repo.record_submission(
            job_id=job_id,
            name=job_name,
            status="PENDING",
            submission_source="web",
            transport_type=transport_type,  # type: ignore[arg-type]
            profile_name=profile_name,
            scheduler_key=scheduler_key,
        )
        # Seed baseline transition so ActiveWatchPoller's first observation
        # produces a real transition (from_status='PENDING') rather than
        # being skipped as a "first observation". (Codex-flagged critical.)
        transition_repo.insert(
            job_id=job_id,
            from_status=None,
            to_status="PENDING",
            source="webhook",
            scheduler_key=scheduler_key,
        )
        if req.notify and req.endpoint_id is not None:
            # V5 grammar: ``job:<scheduler_key>:<id>``. The adapter-owned
            # ``scheduler_key`` already encodes ``local`` / ``ssh:<profile>``
            # so a single format-string covers both transports.
            target_ref = f"job:{scheduler_key}:{job_id}"
            watch_id = watch_repo.create(kind="job", target_ref=target_ref)
            sub_repo.create(watch_id, req.endpoint_id, req.preset)
            event_id = event_repo.insert(
                kind="job.submitted",
                source_ref=target_ref,
                payload={"job_id": job_id, "job_name": job_name},
            )
            # Fan out the job.submitted event so ``preset='all'`` subscribers
            # receive the notification.
            if event_id is not None:
                event = event_repo.get(event_id)
                if event is not None:
                    NotificationService(
                        watch_repo=watch_repo,
                        subscription_repo=sub_repo,
                        event_repo=event_repo,
                        delivery_repo=DeliveryRepository(conn),
                        endpoint_repo=EndpointRepository(conn),
                    ).fan_out(event, conn)
        conn.commit()
    except BaseException:
        conn.rollback()
        raise


@router.post("", status_code=201)
async def submit_job(
    req: JobSubmitRequest,
    adapter: SlurmSSHAdapter = Depends(get_adapter),
    conn: sqlite3.Connection = Depends(get_db_conn),
) -> dict[str, Any]:
    """Submit a new job to SLURM via SSH.

    If *mount_name* is provided, the corresponding mount is synced
    via rsync before submission.
    """
    # Legacy-flag diagnostic: surface a soft warning if the frontend still
    # ships ``notify_slack`` without the new trio.
    if req.notify_slack and not req.notify:
        logger.warning(
            "Request sent deprecated `notify_slack=True`; field has no effect. "
            "Use notify/endpoint_id/preset instead."
        )

    # Validate the notification endpoint BEFORE sbatch so a bad ID never
    # results in a leaked SLURM job. (Codex-flagged.)
    _validate_notify_endpoint(conn, req)

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
            logger.info("Syncing mount '%s' before job submission", mount_name)
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
            lambda: adapter.submit_job(
                req.script_content, job_name=req.job_name or req.name
            )
        )
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"sbatch failed: {e}") from e

    # Persist the submission + (optionally) create a notification watch.
    job_id = result.get("job_id")
    if job_id:
        try:
            await anyio.to_thread.run_sync(
                lambda: _record_and_watch(
                    conn,
                    job_id=int(job_id),
                    job_name=req.job_name or req.name,
                    req=req,
                    adapter=adapter,
                )
            )
        except HTTPException:
            raise
        except Exception:
            logger.warning(
                "Failed to record submission / create watch for job %s",
                job_id,
                exc_info=True,
            )

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
