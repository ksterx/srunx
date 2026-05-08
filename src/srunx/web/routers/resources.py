"""Resource monitoring endpoints: /api/resources"""

from __future__ import annotations

from typing import Any

import anyio
from fastapi import APIRouter, Depends, HTTPException

from srunx.slurm.clients.ssh import SlurmSSHClient

from ..deps import get_adapter

router = APIRouter(prefix="/api/resources", tags=["resources"])


@router.get("")
async def get_resources(
    partition: str | None = None,
    adapter: SlurmSSHClient = Depends(get_adapter),
) -> list[dict[str, Any]]:
    """Get current resource availability via SSH."""
    try:
        return await anyio.to_thread.run_sync(lambda: adapter.get_resources(partition))
    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e)) from e
