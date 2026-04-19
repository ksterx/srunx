"""Watches (read-only observability): ``/api/watches/*``."""

from __future__ import annotations

import sqlite3
from typing import Any

import anyio
from fastapi import APIRouter, Depends, HTTPException, Query

from ..deps import get_db_conn, get_watch_repo

router = APIRouter(prefix="/api/watches", tags=["watches"])


def _serialize(watch: Any) -> dict[str, Any]:
    d = watch.model_dump()
    for field in ("created_at", "closed_at"):
        val = d.get(field)
        if val is not None and hasattr(val, "isoformat"):
            d[field] = val.isoformat().replace("+00:00", "Z")
    return d


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
