"""Workflow run cancellation.

Wraps ``POST /api/workflows/runs/{run_id}/cancel`` — best-effort
``scancel`` fan-out across all job memberships, then a single atomic
terminal transition via :class:`WorkflowRunStateService`.
"""

from __future__ import annotations

import sqlite3
from typing import Any

import anyio
from fastapi import HTTPException

from srunx.observability.storage.connection import transaction
from srunx.observability.storage.repositories.base import now_iso
from srunx.observability.storage.repositories.watches import WatchRepository
from srunx.observability.storage.repositories.workflow_run_jobs import (
    WorkflowRunJobRepository,
)
from srunx.observability.storage.repositories.workflow_runs import WorkflowRunRepository
from srunx.runtime.sweep.state_service import WorkflowRunStateService
from srunx.slurm.clients.ssh import SlurmSSHClient

from .workflow_run_query import parse_run_id


class WorkflowRunCancellationService:
    """Cancel a running workflow (scancel its live children + mark the run)."""

    def __init__(self, terminal_statuses: frozenset[str]) -> None:
        self._terminal = terminal_statuses

    async def cancel(
        self,
        *,
        run_id: str,
        conn: sqlite3.Connection,
        adapter: SlurmSSHClient,
    ) -> dict[str, Any]:
        rid = parse_run_id(run_id)
        run_repo = WorkflowRunRepository(conn)
        wrj_repo = WorkflowRunJobRepository(conn)
        watch_repo = WatchRepository(conn)

        run = await anyio.to_thread.run_sync(lambda: run_repo.get(rid))
        if run is None:
            raise HTTPException(404, f"Run '{run_id}' not found")
        if run.status in self._terminal:
            raise HTTPException(422, f"Run is already {run.status}")

        memberships = await anyio.to_thread.run_sync(lambda: wrj_repo.list_by_run(rid))
        errors: list[str] = []
        for m in memberships:
            if m.job_id is None:
                continue
            try:
                await anyio.to_thread.run_sync(
                    lambda jid=m.job_id: adapter.cancel_job(int(jid))  # type: ignore[misc]
                )
            except Exception as e:
                errors.append(f"{m.job_name}: {e}")

        # Route through WorkflowRunStateService so a
        # ``workflow_run.status_changed`` event is emitted and subscribers
        # receive a delivery. ``run.status`` was captured above and is
        # guaranteed non-terminal by the 422 guard.
        current_status = run.status
        terminal = self._terminal

        def _finalize() -> None:
            with transaction(conn, "IMMEDIATE"):
                transitioned = WorkflowRunStateService.update(
                    conn=conn,
                    workflow_run_id=rid,
                    from_status=current_status,
                    to_status="cancelled",
                    completed_at=now_iso(),
                )
                if not transitioned:
                    # Race with the poller: re-read the latest status and
                    # retry once. Skip if the row has reached a terminal
                    # state in the meantime.
                    latest = run_repo.get(rid)
                    if latest is not None and latest.status not in terminal:
                        WorkflowRunStateService.update(
                            conn=conn,
                            workflow_run_id=rid,
                            from_status=latest.status,
                            to_status="cancelled",
                            completed_at=now_iso(),
                        )
                for w in watch_repo.list_by_target(
                    kind="workflow_run",
                    target_ref=f"workflow_run:{rid}",
                    only_open=True,
                ):
                    if w.id is not None:
                        watch_repo.close(w.id)

        await anyio.to_thread.run_sync(_finalize)

        result: dict[str, Any] = {"status": "cancelled", "run_id": str(rid)}
        if errors:
            result["warnings"] = errors
        return result
