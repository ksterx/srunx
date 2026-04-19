"""Notification endpoints CRUD: ``/api/endpoints/*``."""

from __future__ import annotations

import re
import sqlite3
from typing import Any

import anyio
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from ..deps import get_db_conn, get_endpoint_repo

router = APIRouter(prefix="/api/endpoints", tags=["endpoints"])

_SLACK_WEBHOOK_RE = re.compile(
    r"^https://hooks\.slack\.com/services/[A-Za-z0-9_-]+/[A-Za-z0-9_-]+/[A-Za-z0-9_-]+$"
)
_ALLOWED_KINDS = ("slack_webhook",)  # Phase 1: Slack only


class EndpointCreate(BaseModel):
    kind: str = Field(
        ..., description="Endpoint kind; Phase 1 allows 'slack_webhook' only."
    )
    name: str = Field(..., min_length=1, max_length=80)
    config: dict[str, Any] = Field(
        ...,
        description=(
            "Kind-specific config. For slack_webhook: {'webhook_url': "
            "'https://hooks.slack.com/services/.../.../..'}"
        ),
    )


class EndpointUpdate(BaseModel):
    name: str | None = None
    config: dict[str, Any] | None = None


def _validate(kind: str, config: dict[str, Any]) -> None:
    if kind not in _ALLOWED_KINDS:
        raise HTTPException(
            status_code=422,
            detail=(
                f"Endpoint kind '{kind}' is not enabled in Phase 1. "
                f"Allowed: {_ALLOWED_KINDS}"
            ),
        )
    if kind == "slack_webhook":
        url = config.get("webhook_url")
        if not isinstance(url, str) or not _SLACK_WEBHOOK_RE.match(url):
            raise HTTPException(
                status_code=422,
                detail=(
                    "slack_webhook endpoint requires 'webhook_url' matching "
                    "https://hooks.slack.com/services/WORKSPACE/CHANNEL/TOKEN"
                ),
            )


def _serialize(endpoint: Any) -> dict[str, Any]:
    d = endpoint.model_dump()
    # Emit ISO strings for timestamps so frontend/json round-trip is stable.
    for field in ("created_at", "disabled_at"):
        val = d.get(field)
        if val is not None:
            d[field] = (
                val.isoformat().replace("+00:00", "Z")
                if hasattr(val, "isoformat")
                else str(val)
            )
    return d


@router.get("")
async def list_endpoints(
    conn: sqlite3.Connection = Depends(get_db_conn),
    include_disabled: bool = True,
) -> list[dict[str, Any]]:
    repo = get_endpoint_repo(conn)
    rows = await anyio.to_thread.run_sync(
        lambda: repo.list(include_disabled=include_disabled)
    )
    return [_serialize(r) for r in rows]


@router.post("", status_code=201)
async def create_endpoint(
    body: EndpointCreate,
    conn: sqlite3.Connection = Depends(get_db_conn),
) -> dict[str, Any]:
    _validate(body.kind, body.config)
    repo = get_endpoint_repo(conn)
    try:
        new_id = await anyio.to_thread.run_sync(
            lambda: repo.create(body.kind, body.name, body.config)
        )
    except sqlite3.IntegrityError as exc:
        raise HTTPException(
            status_code=409,
            detail=f"Endpoint (kind={body.kind}, name={body.name}) already exists",
        ) from exc
    created = await anyio.to_thread.run_sync(lambda: repo.get(new_id))
    if created is None:
        raise HTTPException(status_code=500, detail="endpoint vanished after create")
    return _serialize(created)


@router.patch("/{endpoint_id}")
async def update_endpoint(
    endpoint_id: int,
    body: EndpointUpdate,
    conn: sqlite3.Connection = Depends(get_db_conn),
) -> dict[str, Any]:
    repo = get_endpoint_repo(conn)
    current = await anyio.to_thread.run_sync(lambda: repo.get(endpoint_id))
    if current is None:
        raise HTTPException(status_code=404, detail="endpoint not found")
    # If config is changing, re-validate with the (possibly new) kind.
    if body.config is not None:
        _validate(current.kind, body.config)
    changed = await anyio.to_thread.run_sync(
        lambda: repo.update(endpoint_id, name=body.name, config=body.config)
    )
    if not changed:
        # Nothing to update OR not found — return current state unchanged.
        pass
    updated = await anyio.to_thread.run_sync(lambda: repo.get(endpoint_id))
    if updated is None:
        raise HTTPException(status_code=404, detail="endpoint not found")
    return _serialize(updated)


@router.post("/{endpoint_id}/disable")
async def disable_endpoint(
    endpoint_id: int,
    conn: sqlite3.Connection = Depends(get_db_conn),
) -> dict[str, Any]:
    repo = get_endpoint_repo(conn)
    ok = await anyio.to_thread.run_sync(lambda: repo.disable(endpoint_id))
    if not ok:
        raise HTTPException(status_code=404, detail="endpoint not found")
    updated = await anyio.to_thread.run_sync(lambda: repo.get(endpoint_id))
    return _serialize(updated) if updated else {}


@router.post("/{endpoint_id}/enable")
async def enable_endpoint(
    endpoint_id: int,
    conn: sqlite3.Connection = Depends(get_db_conn),
) -> dict[str, Any]:
    repo = get_endpoint_repo(conn)
    ok = await anyio.to_thread.run_sync(lambda: repo.enable(endpoint_id))
    if not ok:
        raise HTTPException(status_code=404, detail="endpoint not found")
    updated = await anyio.to_thread.run_sync(lambda: repo.get(endpoint_id))
    return _serialize(updated) if updated else {}


@router.delete("/{endpoint_id}", status_code=204)
async def delete_endpoint(
    endpoint_id: int,
    conn: sqlite3.Connection = Depends(get_db_conn),
) -> None:
    repo = get_endpoint_repo(conn)
    ok = await anyio.to_thread.run_sync(lambda: repo.delete(endpoint_id))
    if not ok:
        raise HTTPException(status_code=404, detail="endpoint not found")
