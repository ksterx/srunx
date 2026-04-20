"""Job history endpoints: /api/history/*

Reads from the unified ``~/.config/srunx/srunx.db`` via
:class:`~srunx.db.repositories.jobs.JobRepository`. Prior to this
cutover (P2-4 #A) the router consumed the legacy
``~/.srunx/history.db``; the dual-write introduced in C2 ensured both
DBs stayed in sync, which lets the read path flip without any data
migration step.
"""

from __future__ import annotations

import sqlite3
from typing import Any

import anyio
from fastapi import APIRouter, Depends, HTTPException, Query

from srunx.db.repositories.jobs import JobRepository

from ..deps import get_db_conn
from ..serializers import serialize_history_entry, serialize_job_stats

router = APIRouter(prefix="/api/history", tags=["history"])


@router.get("/stats")
async def get_stats(
    from_date: str | None = Query(None, alias="from"),
    to_date: str | None = Query(None, alias="to"),
    conn: sqlite3.Connection = Depends(get_db_conn),
) -> dict[str, Any]:
    """Get job statistics for a date range.

    Frontend sends ``?from=...&to=...`` but 'from' is a Python keyword,
    so we use ``Query(alias=...)`` to map the parameter names.
    """
    repo = JobRepository(conn)
    try:
        raw = await anyio.to_thread.run_sync(
            lambda: repo.compute_stats(from_date, to_date)
        )
    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e)) from e
    return serialize_job_stats(raw)


@router.get("")
async def get_recent(
    limit: int = 50,
    conn: sqlite3.Connection = Depends(get_db_conn),
) -> list[dict[str, Any]]:
    """Get recent job history entries."""
    repo = JobRepository(conn)
    try:
        rows = await anyio.to_thread.run_sync(lambda: repo.list_recent_as_dict(limit))
    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e)) from e
    return [serialize_history_entry(r) for r in rows]
