"""Watches observability + post-hoc notification attach: ``/api/watches/*``.

Historically read-only. Now also exposes:

- ``POST ""`` — create a watch + subscription for an existing ``kind='job'``
  target, so the Web UI can enable notifications on a job that was
  already submitted. Mirrors the CLI's
  ``srunx watch jobs <id> --endpoint ...`` path via the shared
  :mod:`srunx.observability.notifications.attach` module.
- ``POST "/{id}/close"`` — close a watch (sets ``closed_at``). Used by the
  UI's "disable notifications for this job" control.
"""

from __future__ import annotations

import sqlite3
from typing import Any, Literal

import anyio
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field

from srunx.observability.notifications.attach import (
    AttachResult,
    EndpointDisabledError,
    EndpointNotFoundError,
    InvalidPresetError,
    UnsupportedPresetError,
    attach_job_notification,
)
from srunx.slurm.clients.ssh import SlurmSSHClient

from ..deps import get_adapter, get_db_conn, get_watch_repo

router = APIRouter(prefix="/api/watches", tags=["watches"])


def _serialize(watch: Any) -> dict[str, Any]:
    d = watch.model_dump()
    for field in ("created_at", "closed_at"):
        val = d.get(field)
        if val is not None and hasattr(val, "isoformat"):
            d[field] = val.isoformat().replace("+00:00", "Z")
    return d


class CreateJobWatchRequest(BaseModel):
    """Payload for ``POST /api/watches`` targeting an existing SLURM job.

    Only ``kind='job'`` is supported here — workflow_run / sweep_run
    watches are created server-side as part of the submit flow and don't
    need a post-hoc attach endpoint yet.
    """

    kind: Literal["job"] = "job"
    job_id: int = Field(..., gt=0)
    endpoint_id: int = Field(..., gt=0)
    preset: str = Field(default="terminal")


@router.get("")
async def list_watches(
    open_only: bool = Query(default=True, alias="open"),
    kind: str | None = Query(default=None),
    target_ref: str | None = Query(default=None),
    conn: sqlite3.Connection = Depends(get_db_conn),
) -> list[dict[str, Any]]:
    repo = get_watch_repo(conn)

    if kind is not None and target_ref is not None:
        rows = await anyio.to_thread.run_sync(
            lambda: repo.list_by_target(kind, target_ref, only_open=open_only)
        )
    elif open_only:
        rows = await anyio.to_thread.run_sync(lambda: repo.list_open())
    else:
        raise HTTPException(
            status_code=400,
            detail="Unfiltered listing disabled. Provide kind+target_ref or open=true.",
        )
    return [_serialize(r) for r in rows]


@router.get("/{watch_id}")
async def get_watch(
    watch_id: int,
    conn: sqlite3.Connection = Depends(get_db_conn),
) -> dict[str, Any]:
    repo = get_watch_repo(conn)
    w = await anyio.to_thread.run_sync(lambda: repo.get(watch_id))
    if w is None:
        raise HTTPException(status_code=404, detail="watch not found")
    return _serialize(w)


@router.post("", status_code=201)
async def create_job_watch(
    body: CreateJobWatchRequest,
    conn: sqlite3.Connection = Depends(get_db_conn),
    adapter: SlurmSSHClient = Depends(get_adapter),
) -> dict[str, Any]:
    """Attach a notification watch + subscription to an existing job.

    Idempotent: repeated calls for the same (job, endpoint, preset)
    triple return the same ``watch_id`` / ``subscription_id`` with
    ``created=false``. The 201 status matches the sibling
    ``POST /api/subscriptions`` convention; the ``created`` flag in the
    body tells the caller whether the write was new or a reuse.

    The ``scheduler_key`` is taken from the active adapter so the watch
    is keyed to the transport the poller will query — explicitly
    accepting it from the client would be a footgun (sending ``local``
    while the server talks to an SSH cluster would silently strand the
    watch on the wrong scheduler).
    """
    scheduler_key = adapter.scheduler_key

    def _do() -> AttachResult:
        # BEGIN IMMEDIATE so endpoint validation + watch/subscription
        # insert + transition seed land as one transaction. The jobs
        # submit path uses the same envelope (routers/jobs.py L204).
        conn.execute("BEGIN IMMEDIATE")
        try:
            result = attach_job_notification(
                conn=conn,
                job_id=body.job_id,
                endpoint_id=body.endpoint_id,
                preset=body.preset,
                scheduler_key=scheduler_key,
            )
        except BaseException:
            conn.rollback()
            raise
        else:
            conn.commit()
            return result

    try:
        result = await anyio.to_thread.run_sync(_do)
    except EndpointNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except EndpointDisabledError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except InvalidPresetError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except UnsupportedPresetError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    return {
        "watch_id": result.watch_id,
        "subscription_id": result.subscription_id,
        "created": result.created,
    }


@router.post("/{watch_id}/close", status_code=200)
async def close_watch(
    watch_id: int,
    conn: sqlite3.Connection = Depends(get_db_conn),
) -> dict[str, Any]:
    """Close an open watch. Idempotent on already-closed watches (returns 404 only for unknown ids)."""
    repo = get_watch_repo(conn)

    def _do() -> Any:
        existing = repo.get(watch_id)
        if existing is None:
            return None
        conn.execute("BEGIN IMMEDIATE")
        try:
            repo.close(watch_id)
        except BaseException:
            conn.rollback()
            raise
        conn.commit()
        return repo.get(watch_id)

    updated = await anyio.to_thread.run_sync(_do)
    if updated is None:
        raise HTTPException(status_code=404, detail="watch not found")
    return _serialize(updated)
