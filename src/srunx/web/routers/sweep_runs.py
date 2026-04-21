"""Sweep runs read + cancel endpoints: ``/api/sweep_runs/*``.

See ``.claude/specs/workflow-parameter-sweep/design.md`` § Web API.
Phase G exposes just enough surface for the Web UI Phase I work:
list, detail, cell list, and cancel.
"""

from __future__ import annotations

import sqlite3
from typing import Any

import anyio
from fastapi import APIRouter, Depends, HTTPException

from srunx.db.repositories.sweep_runs import SweepRunRepository
from srunx.logging import get_logger

from ..deps import get_db_conn

router = APIRouter(prefix="/api/sweep_runs", tags=["sweep_runs"])
logger = get_logger(__name__)


def _serialize_sweep(row: Any) -> dict[str, Any]:
    """Convert a :class:`~srunx.db.models.SweepRun` into a JSON-friendly dict."""
    return {
        "id": row.id,
        "name": row.name,
        "workflow_yaml_path": row.workflow_yaml_path,
        "status": row.status,
        "matrix": row.matrix,
        "args": row.args,
        "fail_fast": row.fail_fast,
        "max_parallel": row.max_parallel,
        "cell_count": row.cell_count,
        "cells_pending": row.cells_pending,
        "cells_running": row.cells_running,
        "cells_completed": row.cells_completed,
        "cells_failed": row.cells_failed,
        "cells_cancelled": row.cells_cancelled,
        "submission_source": row.submission_source,
        "started_at": row.started_at.isoformat() if row.started_at else None,
        "completed_at": (row.completed_at.isoformat() if row.completed_at else None),
        "cancel_requested_at": (
            row.cancel_requested_at.isoformat() if row.cancel_requested_at else None
        ),
        "error": row.error,
    }


@router.get("")
async def list_sweep_runs(
    conn: sqlite3.Connection = Depends(get_db_conn),
) -> list[dict[str, Any]]:
    def _load() -> list[dict[str, Any]]:
        rows = SweepRunRepository(conn).list_all(limit=200)
        return [_serialize_sweep(r) for r in rows]

    return await anyio.to_thread.run_sync(_load)


@router.get("/{sweep_run_id}")
async def get_sweep_run(
    sweep_run_id: int,
    conn: sqlite3.Connection = Depends(get_db_conn),
) -> dict[str, Any]:
    def _load() -> dict[str, Any]:
        row = SweepRunRepository(conn).get(sweep_run_id)
        if row is None:
            raise HTTPException(
                status_code=404, detail=f"Sweep run {sweep_run_id} not found"
            )
        return _serialize_sweep(row)

    return await anyio.to_thread.run_sync(_load)


@router.get("/{sweep_run_id}/cells")
async def list_sweep_cells(
    sweep_run_id: int,
    conn: sqlite3.Connection = Depends(get_db_conn),
) -> list[dict[str, Any]]:
    """Return all workflow_runs rows linked to the sweep.

    Order: (status, started_at ASC) so callers see a stable progression
    through the sweep's cells.
    """

    def _load() -> list[dict[str, Any]]:
        if SweepRunRepository(conn).get(sweep_run_id) is None:
            raise HTTPException(
                status_code=404, detail=f"Sweep run {sweep_run_id} not found"
            )
        rows = conn.execute(
            """
            SELECT id, workflow_name, status, started_at, completed_at,
                   args, error, triggered_by
            FROM workflow_runs
            WHERE sweep_run_id = ?
            ORDER BY status, started_at ASC, id ASC
            """,
            (sweep_run_id,),
        ).fetchall()
        result: list[dict[str, Any]] = []
        for r in rows:
            result.append(
                {
                    "id": r["id"],
                    "workflow_name": r["workflow_name"],
                    "status": r["status"],
                    "started_at": r["started_at"],
                    "completed_at": r["completed_at"],
                    "args": _loads_json(r["args"]),
                    "error": r["error"],
                    "triggered_by": r["triggered_by"],
                }
            )
        return result

    return await anyio.to_thread.run_sync(_load)


def _loads_json(raw: Any) -> Any:
    if raw is None:
        return None
    if isinstance(raw, (dict, list)):
        return raw
    try:
        import json as _json

        return _json.loads(raw)
    except (TypeError, ValueError):
        return None


@router.post("/{sweep_run_id}/cancel", status_code=202)
async def cancel_sweep_run(
    sweep_run_id: int,
    conn: sqlite3.Connection = Depends(get_db_conn),
) -> dict[str, Any]:
    """Stamp ``cancel_requested_at`` and ask the live orchestrator to drain.

    When the orchestrator isn't in-process (crash recovery), the DB-only
    update is enough — the reconciler / aggregator observe it on their
    next cycle and advance the sweep to cancelled.
    """

    def _load_and_stamp() -> Any:
        repo = SweepRunRepository(conn)
        row = repo.get(sweep_run_id)
        if row is None:
            raise HTTPException(
                status_code=404, detail=f"Sweep run {sweep_run_id} not found"
            )
        # ``request_cancel`` is guarded by ``cancel_requested_at IS NULL``
        # so a second call is a no-op (returns False) but we still want
        # to return the latest row to the caller.
        repo.request_cancel(sweep_run_id)
        return repo.get(sweep_run_id)

    refreshed = await anyio.to_thread.run_sync(_load_and_stamp)

    # Best-effort in-process drain. Errors here must not fail the HTTP
    # response since the DB-only stamp already guarantees eventual
    # convergence via the aggregator.
    from srunx.sweep.orchestrator import get_active_orchestrator

    orch = get_active_orchestrator(sweep_run_id)
    if orch is not None:
        try:
            await anyio.to_thread.run_sync(orch.request_cancel)
        except Exception:  # noqa: BLE001
            logger.warning(
                "Sweep %s: request_cancel raised in active orchestrator",
                sweep_run_id,
                exc_info=True,
            )

    return _serialize_sweep(refreshed)
