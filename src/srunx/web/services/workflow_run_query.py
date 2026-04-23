"""Read-only ``workflow_runs`` queries.

Wraps ``GET /api/workflows/runs`` and ``GET /api/workflows/runs/{run_id}``,
plus the shared ``_build_run_response`` helper that hydrates a
``WorkflowRun`` row with its child job statuses. The router re-exports
``_build_run_response`` so ``tests/transport/test_review_fixes.py``'s
direct import (``from srunx.web.routers.workflows import _build_run_response``)
keeps working.
"""

from __future__ import annotations

import sqlite3
from typing import Any

import anyio
from fastapi import HTTPException

from srunx.db.models import WorkflowRun as DBWorkflowRun
from srunx.db.models import WorkflowRunJob
from srunx.db.repositories.jobs import JobRepository
from srunx.db.repositories.workflow_run_jobs import WorkflowRunJobRepository
from srunx.db.repositories.workflow_runs import WorkflowRunRepository


def parse_run_id(run_id: str) -> int:
    """Parse a run_id string → int, raising 404 for non-integer ids.

    The API accepts ``run_id`` as a string for historical compatibility
    (the old UUID-keyed registry); internally we always store an int.
    """
    try:
        return int(run_id)
    except (TypeError, ValueError) as exc:
        raise HTTPException(
            status_code=404, detail=f"Run '{run_id}' not found"
        ) from exc


def serialize_run(
    run: DBWorkflowRun,
    memberships: list[WorkflowRunJob],
    jobs_by_id: dict[int, str],
) -> dict[str, Any]:
    """Build the API response for a workflow run.

    ``jobs_by_id`` maps SLURM job_id → observed status. Memberships with
    no job_id (not yet submitted) are omitted from both ``job_ids`` and
    ``job_statuses``.
    """
    job_ids: dict[str, str] = {}
    job_statuses: dict[str, str] = {}
    for wrj in memberships:
        if wrj.job_id is None:
            continue
        job_ids[wrj.job_name] = str(wrj.job_id)
        status = jobs_by_id.get(wrj.job_id)
        if status is not None:
            job_statuses[wrj.job_name] = status

    return {
        "id": str(run.id),
        "workflow_name": run.workflow_name,
        "started_at": run.started_at.isoformat() if run.started_at else None,
        "completed_at": run.completed_at.isoformat() if run.completed_at else None,
        "status": run.status,
        "job_ids": job_ids,
        "job_statuses": job_statuses,
        "error": run.error,
        "sweep_run_id": run.sweep_run_id,
    }


def build_run_response(conn: sqlite3.Connection, run: DBWorkflowRun) -> dict[str, Any]:
    """Load memberships + child job statuses and serialize."""
    if run.id is None:
        return serialize_run(run, [], {})
    memberships = WorkflowRunJobRepository(conn).list_by_run(run.id)
    job_repo = JobRepository(conn)
    jobs_by_id: dict[int, str] = {}
    for m in memberships:
        # V5+: ``jobs_row_id`` is the authoritative FK to ``jobs.id``.
        # Looking up via ``get_by_row_id`` avoids the pre-V5
        # ``scheduler_key='local'`` default, which would drop SSH
        # workflow children and miss their statuses in the API response.
        if m.jobs_row_id is None or m.job_id is None:
            continue
        job = job_repo.get_by_row_id(m.jobs_row_id)
        if job is not None:
            jobs_by_id[m.job_id] = job.status
    return serialize_run(run, memberships, jobs_by_id)


class WorkflowRunQueryService:
    """Read-only workflow_runs queries."""

    async def list_runs(
        self,
        conn: sqlite3.Connection,
        name: str | None = None,
    ) -> list[dict[str, Any]]:
        def _load() -> list[dict[str, Any]]:
            runs = WorkflowRunRepository(conn).list_all()
            if name is not None:
                runs = [r for r in runs if r.workflow_name == name]
            return [build_run_response(conn, r) for r in runs]

        return await anyio.to_thread.run_sync(_load)

    async def get_run(
        self,
        conn: sqlite3.Connection,
        run_id: str,
    ) -> dict[str, Any]:
        """Get the status and details of a single workflow run."""
        rid = parse_run_id(run_id)

        def _load() -> dict[str, Any]:
            run = WorkflowRunRepository(conn).get(rid)
            if run is None:
                raise HTTPException(status_code=404, detail=f"Run '{run_id}' not found")
            return build_run_response(conn, run)

        return await anyio.to_thread.run_sync(_load)
