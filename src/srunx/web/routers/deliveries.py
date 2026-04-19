"""Deliveries observability: ``/api/deliveries/*``.

Read-only routes for surfacing outbox state. No direct delivery-send
endpoint here — delivery is the ``DeliveryPoller``'s job.
"""

from __future__ import annotations

import sqlite3
from typing import Any

import anyio
from fastapi import APIRouter, Depends, HTTPException, Query

from ..deps import get_db_conn, get_delivery_repo

router = APIRouter(prefix="/api/deliveries", tags=["deliveries"])


def _serialize(delivery: Any) -> dict[str, Any]:
    d = delivery.model_dump()
    for field in ("created_at", "delivered_at", "leased_until", "next_attempt_at"):
        val = d.get(field)
        if val is not None and hasattr(val, "isoformat"):
            d[field] = val.isoformat().replace("+00:00", "Z")
    return d


@router.get("")
async def list_deliveries(
    subscription_id: int | None = Query(default=None),
    status: str | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=500),
    conn: sqlite3.Connection = Depends(get_db_conn),
) -> list[dict[str, Any]]:
    """List deliveries — scoped to a subscription, or recent across all.

    - ``subscription_id`` + optional ``status`` → per-subscription view.
    - ``subscription_id`` omitted → most recent deliveries across every
      subscription (bounded by ``limit``, max 500). Used by the
      NotificationsCenter dashboard.
    """
    repo = get_delivery_repo(conn)
    if subscription_id is None:
        rows = await anyio.to_thread.run_sync(
            lambda: repo.list_recent(status=status, limit=limit)
        )
    else:
        rows = await anyio.to_thread.run_sync(
            lambda: repo.list_by_subscription(subscription_id, status=status)
        )
    return [_serialize(r) for r in rows]


@router.get("/stuck")
async def count_stuck(
    older_than_sec: int = Query(default=300, ge=0),
    conn: sqlite3.Connection = Depends(get_db_conn),
) -> dict[str, int]:
    """Return the count of deliveries ``pending`` past ``older_than_sec``."""
    repo = get_delivery_repo(conn)
    count = await anyio.to_thread.run_sync(
        lambda: repo.count_stuck_pending(older_than_sec=older_than_sec)
    )
    return {"count": int(count), "older_than_sec": older_than_sec}


@router.get("/{delivery_id}")
async def get_delivery(
    delivery_id: int,
    conn: sqlite3.Connection = Depends(get_db_conn),
) -> dict[str, Any]:
    repo = get_delivery_repo(conn)
    d = await anyio.to_thread.run_sync(lambda: repo.get(delivery_id))
    if d is None:
        raise HTTPException(status_code=404, detail="delivery not found")
    return _serialize(d)
