"""Job history endpoints: /api/history/*"""

from __future__ import annotations

from typing import Any

import anyio
from fastapi import APIRouter, Depends, HTTPException, Query

from srunx.history import JobHistory

from ..deps import get_history_db
from ..serializers import serialize_history_entry, serialize_job_stats

router = APIRouter(prefix="/api/history", tags=["history"])


@router.get("/stats")
async def get_stats(
    from_date: str | None = Query(None, alias="from"),
    to_date: str | None = Query(None, alias="to"),
    history: JobHistory = Depends(get_history_db),
) -> dict[str, Any]:
    """Get job statistics for a date range.

    Frontend sends ?from=...&to=... but 'from' is a Python keyword,
    so we use Query(alias=...) to map the parameter names.
    """
    try:
        raw = await anyio.to_thread.run_sync(
            lambda: history.get_job_stats(from_date, to_date)
        )
    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e)) from e
    return serialize_job_stats(raw)


@router.get("")
async def get_recent(
    limit: int = 50,
    history: JobHistory = Depends(get_history_db),
) -> list[dict[str, Any]]:
    """Get recent job history entries."""
    try:
        rows = await anyio.to_thread.run_sync(lambda: history.get_recent_jobs(limit))
    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e)) from e
    return [serialize_history_entry(r) for r in rows]
