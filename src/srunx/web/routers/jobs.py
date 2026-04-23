"""Job management endpoints: /api/jobs/*"""

from __future__ import annotations

import sqlite3
from typing import Any

import anyio
from fastapi import APIRouter, Depends, HTTPException, Response
from pydantic import BaseModel, Field, model_validator

from srunx.common.logging import get_logger
from srunx.observability.notifications.service import NotificationService
from srunx.observability.storage.repositories.deliveries import DeliveryRepository
from srunx.observability.storage.repositories.endpoints import EndpointRepository
from srunx.observability.storage.repositories.events import EventRepository
from srunx.observability.storage.repositories.job_state_transitions import (
    JobStateTransitionRepository,
)
from srunx.observability.storage.repositories.jobs import JobRepository
from srunx.observability.storage.repositories.subscriptions import (
    SubscriptionRepository,
)
from srunx.observability.storage.repositories.watches import WatchRepository
from srunx.slurm.ssh import SlurmSSHAdapter

from ..deps import get_adapter, get_db_conn

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

    from srunx.domain import Job, JobEnvironment, JobResource
    from srunx.runtime.templates import get_template_path

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

    from srunx.runtime.rendering import render_job_script

    with tempfile.TemporaryDirectory() as tmpdir:
        script_path = await anyio.to_thread.run_sync(
            lambda: render_job_script(template_path, job, output_dir=tmpdir)
        )
        from pathlib import Path

        script_content = Path(script_path).read_text()

    return ScriptPreviewResponse(script=script_content, template_used=template_name)


class JobSubmitRequest(BaseModel):
    """Request body for submitting a job via the Web UI.

    Two submission modes are supported, mutually exclusive:

    * ``script_content`` — bytes uploaded to ``/tmp/srunx/`` on the
      remote (legacy default; same behaviour as before #136).
    * ``script_path`` — path to a script that already lives under
      ``mount_name``'s ``mount.local`` root. The Web rsyncs that mount
      and dispatches sbatch directly against the remote-mount path,
      mirroring the CLI's in-place execution. Requires ``mount_name``.

    ``notify_slack`` (DEPRECATED) is retained for backward compatibility with
    older frontend builds but has no effect beyond logging a warning. Prefer
    the ``notify`` + ``endpoint_id`` + ``preset`` triplet.
    """

    name: str
    # Both content and path are optional individually; the
    # ``_check_script_source`` validator enforces "exactly one set".
    # Old clients that always send ``script_content`` keep working.
    script_content: str | None = None
    script_path: str | None = None
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

    @model_validator(mode="after")
    def _check_script_source(self) -> JobSubmitRequest:
        # Exactly one of (script_content, script_path) must be set.
        # Both → ambiguous; neither → nothing to submit. Pre-#136
        # clients always set ``script_content`` so the legacy contract
        # is preserved; ``script_path`` is the opt-in addition.
        if self.script_content is None and self.script_path is None:
            raise ValueError("exactly one of script_content / script_path must be set")
        if self.script_content is not None and self.script_path is not None:
            raise ValueError("script_content and script_path are mutually exclusive")
        # ``script_path`` requires ``mount_name`` so the path can be
        # validated against a known mount root (security) and
        # translated to the remote filesystem.
        if self.script_path is not None and not self.mount_name:
            raise ValueError(
                "script_path requires mount_name (so the path can be "
                "validated and translated to its remote equivalent)"
            )
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


def _resolve_in_place_plan(
    *,
    profile: Any,
    mount_name: str,
    script_path: str,
) -> tuple[Any, str, str]:
    """Validate ``script_path`` and translate it to its remote equivalent.

    Pure (no I/O) — validates that ``mount_name`` exists on the
    profile and that ``script_path`` resolves under that mount's
    ``local`` root, then returns the matched ``MountConfig`` together
    with the translated remote path and a sensible ``submit_cwd``.

    Raises :class:`HTTPException` (404 for unknown mount, 403 for path
    escapes) so the caller doesn't have to translate exception types.
    """
    from pathlib import Path

    from srunx.runtime.submission_plan import translate_local_to_remote

    matching = [m for m in profile.mounts if m.name == mount_name]
    if not matching:
        raise HTTPException(
            status_code=404,
            detail=f"Mount '{mount_name}' not configured on the active profile",
        )
    mount = matching[0]

    mount_root = Path(mount.local).expanduser().resolve()
    resolved = Path(script_path).expanduser().resolve()
    if not resolved.is_relative_to(mount_root):
        # Mirrors the workflow router's directory-traversal guard
        # (``_enforce_shell_script_roots``) — same 403 contract so
        # the Web UI surfaces a single error shape for both routes.
        raise HTTPException(
            status_code=403,
            detail=(
                f"script_path '{script_path}' is outside mount "
                f"'{mount_name}' (root: {mount_root})"
            ),
        )

    remote_script = translate_local_to_remote(resolved, mount)
    # Run sbatch from the script's parent dir on the remote so any
    # relative paths inside the user's own ``#SBATCH --output=`` etc.
    # resolve where they would on a head-node ``sbatch ./script.sh``.
    parent_remote, _, _ = remote_script.rpartition("/")
    submit_cwd = parent_remote or remote_script
    return mount, remote_script, submit_cwd


def _submit_in_place(
    req: JobSubmitRequest,
    adapter: SlurmSSHAdapter,
) -> dict[str, Any]:
    """Run script_path mode end-to-end on a worker thread.

    Wrapped behind one ``anyio.to_thread.run_sync`` so the held mount
    lock + rsync + sbatch all execute under the same OS thread. If we
    instead awaited each phase separately, the lock could be released
    between sync and submit by an event-loop context switch — defeating
    the whole purpose of holding the lock across submission (Codex
    blocker #3 on PR #134, same shape).
    """
    from srunx.common.config import get_config
    from srunx.sync.lock import SyncLockTimeoutError
    from srunx.sync.service import SyncAbortedError, mount_sync_session

    from ..sync_utils import get_current_profile

    assert req.script_path is not None  # validator guarantees this
    assert req.mount_name is not None  # validator guarantees this

    profile = get_current_profile()
    if profile is None:
        raise HTTPException(
            status_code=503,
            detail="No SSH profile configured; cannot resolve script_path",
        )
    mount, remote_script, submit_cwd = _resolve_in_place_plan(
        profile=profile,
        mount_name=req.mount_name,
        script_path=req.script_path,
    )

    sync_cfg = get_config().sync
    try:
        # Lock held across rsync + sbatch — a concurrent /api/jobs or
        # CLI invocation can't rsync different bytes between our sync
        # and our submission.
        with mount_sync_session(
            profile_name=adapter.scheduler_key.removeprefix("ssh:"),
            profile=profile,
            mount=mount,
            config=sync_cfg,
            sync_required=True,
        ):
            submitted = adapter.submit_remote_sbatch(
                remote_script,
                submit_cwd=submit_cwd,
                job_name=req.job_name or req.name,
            )
    except SyncAbortedError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except SyncLockTimeoutError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(
            status_code=502,
            detail=f"Mount sync failed: {exc}",
        ) from exc

    # ``submit_remote_sbatch`` returns the in-memory job; reshape into
    # the dict ``submit_job`` produces so the response payload stays
    # stable for legacy frontend builds. The route does ``record_and
    # _watch`` itself so we don't double-record here — the adapter
    # already wrote a ``jobs`` row, and ``_record_and_watch`` uses
    # ``INSERT OR IGNORE`` so the Web router's call is a no-op for
    # the row but still creates the notification watch.
    return {
        "name": getattr(submitted, "name", None) or req.job_name or req.name,
        "job_id": int(submitted.job_id) if submitted.job_id else None,
        "status": "PENDING",
        "depends_on": [],
        "command": [],
        "resources": {},
    }


def _submit_via_tmp_upload(
    req: JobSubmitRequest,
    adapter: SlurmSSHAdapter,
) -> dict[str, Any]:
    """Legacy script_content path — uploads bytes to ``/tmp/srunx/``.

    Preserves the pre-#136 behaviour bit-for-bit (including
    ``delete=True`` on the optional mount sync) so existing clients
    that always send ``script_content`` see no contract change.
    """
    from ..sync_utils import get_current_profile, sync_mount_by_name

    if req.mount_name:
        profile = get_current_profile()
        if profile is None:
            raise HTTPException(
                status_code=503,
                detail="No SSH profile configured; cannot sync mount",
            )
        try:
            logger.info("Syncing mount '%s' before job submission", req.mount_name)
            sync_mount_by_name(profile, req.mount_name, delete=True)
        except ValueError as exc:
            raise HTTPException(
                status_code=404,
                detail=f"Mount '{req.mount_name}' not found",
            ) from exc
        except RuntimeError as exc:
            raise HTTPException(
                status_code=502, detail=f"Mount sync failed: {exc}"
            ) from exc

    assert req.script_content is not None  # validator guarantees this
    return adapter.submit_job(req.script_content, job_name=req.job_name or req.name)


@router.post("", status_code=201)
async def submit_job(
    req: JobSubmitRequest,
    adapter: SlurmSSHAdapter = Depends(get_adapter),
    conn: sqlite3.Connection = Depends(get_db_conn),
) -> dict[str, Any]:
    """Submit a new job to SLURM via SSH.

    Two modes (mutually exclusive, enforced by the request validator):

    * ``script_content`` (legacy): bytes uploaded to ``/tmp/srunx/``,
      sbatch runs against the tmp copy. Optional ``mount_name``
      triggers a pre-submit rsync.
    * ``script_path`` (#136): script must live under
      ``mount_name``'s local root. The mount is rsynced under the
      per-mount lock (held across submission), and sbatch runs
      directly against the remote-mount path — no tmp copy, the
      user's own ``#SBATCH`` directives win. Mirrors the CLI's
      in-place execution on top of PR #141.
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

    try:
        if req.script_path is not None:
            result = await anyio.to_thread.run_sync(
                lambda: _submit_in_place(req, adapter)
            )
        else:
            result = await anyio.to_thread.run_sync(
                lambda: _submit_via_tmp_upload(req, adapter)
            )
    except HTTPException:
        raise
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
