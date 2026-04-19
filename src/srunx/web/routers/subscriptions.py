"""Notification subscriptions CRUD: ``/api/subscriptions/*``."""

from __future__ import annotations

import sqlite3
from typing import Any

import anyio
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field

from ..deps import get_db_conn, get_subscription_repo

router = APIRouter(prefix="/api/subscriptions", tags=["subscriptions"])

_VALID_PRESETS = ("terminal", "running_and_terminal", "all", "digest")


class SubscriptionCreate(BaseModel):
    watch_id: int = Field(..., gt=0)
    endpoint_id: int = Field(..., gt=0)
    preset: str = Field(default="terminal")


def _serialize(sub: Any) -> dict[str, Any]:
    d = sub.model_dump()
    for field in ("created_at",):
        val = d.get(field)
        if val is not None and hasattr(val, "isoformat"):
            d[field] = val.isoformat().replace("+00:00", "Z")
    return d


@router.get("")
async def list_subscriptions(
    watch_id: int | None = Query(default=None),
    endpoint_id: int | None = Query(default=None),
    conn: sqlite3.Connection = Depends(get_db_conn),
) -> list[dict[str, Any]]:
    repo = get_subscription_repo(conn)
    if watch_id is not None:
        rows = await anyio.to_thread.run_sync(lambda: repo.list_by_watch(watch_id))
    elif endpoint_id is not None:
        rows = await anyio.to_thread.run_sync(
            lambda: repo.list_by_endpoint(endpoint_id)
        )
    else:
        raise HTTPException(
            status_code=400,
            detail="watch_id or endpoint_id query parameter required",
        )
    return [_serialize(r) for r in rows]


@router.post("", status_code=201)
async def create_subscription(
    body: SubscriptionCreate,
    conn: sqlite3.Connection = Depends(get_db_conn),
) -> dict[str, Any]:
    if body.preset not in _VALID_PRESETS:
        raise HTTPException(
            status_code=422,
            detail=f"Invalid preset '{body.preset}'. Allowed: {_VALID_PRESETS}",
        )
    repo = get_subscription_repo(conn)
    try:
        new_id = await anyio.to_thread.run_sync(
            lambda: repo.create(body.watch_id, body.endpoint_id, body.preset)
        )
    except sqlite3.IntegrityError as exc:
        raise HTTPException(
            status_code=409,
            detail="subscription for this (watch, endpoint) already exists",
        ) from exc
    created = await anyio.to_thread.run_sync(lambda: repo.get(new_id))
    if created is None:
        raise HTTPException(
            status_code=500, detail="subscription vanished after create"
        )
    return _serialize(created)


@router.delete("/{subscription_id}", status_code=204)
async def delete_subscription(
    subscription_id: int,
    conn: sqlite3.Connection = Depends(get_db_conn),
) -> None:
    repo = get_subscription_repo(conn)
    ok = await anyio.to_thread.run_sync(lambda: repo.delete(subscription_id))
    if not ok:
        raise HTTPException(status_code=404, detail="subscription not found")
