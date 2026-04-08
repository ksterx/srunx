"""In-memory workflow run registry."""

from __future__ import annotations

import threading
from datetime import UTC, datetime
from typing import Literal
from uuid import uuid4

from pydantic import BaseModel, Field

RunStatus = Literal[
    "syncing", "submitting", "running", "completed", "failed", "cancelled"
]


class WorkflowRun(BaseModel):
    """Tracks a single workflow execution."""

    id: str = Field(default_factory=lambda: str(uuid4()))
    workflow_name: str
    started_at: str = Field(default_factory=lambda: datetime.now(UTC).isoformat())
    completed_at: str | None = None
    status: RunStatus = "running"
    job_statuses: dict[str, str] = Field(default_factory=dict)
    job_ids: dict[str, str] = Field(default_factory=dict)
    error: str | None = None


_MAX_RUNS = 1000
_TERMINAL_STATUSES: set[RunStatus] = {"completed", "failed", "cancelled"}


class RunRegistry:
    """Thread-safe in-memory registry for workflow runs."""

    def __init__(self, max_runs: int = _MAX_RUNS) -> None:
        self._runs: dict[str, WorkflowRun] = {}
        self._lock = threading.Lock()
        self._max_runs = max_runs

    def create(self, workflow_name: str) -> WorkflowRun:
        run = WorkflowRun(workflow_name=workflow_name)
        with self._lock:
            self._runs[run.id] = run
            self._evict_if_needed()
        return run

    def _evict_if_needed(self) -> None:
        """Remove oldest completed runs when registry exceeds max size.

        Must be called while holding ``_lock``.
        """
        if len(self._runs) <= self._max_runs:
            return
        # Collect terminal runs sorted by completion time (oldest first)
        terminal = sorted(
            (r for r in self._runs.values() if r.status in _TERMINAL_STATUSES),
            key=lambda r: r.completed_at or "",
        )
        to_remove = len(self._runs) - self._max_runs
        for run in terminal[:to_remove]:
            del self._runs[run.id]

    def get(self, run_id: str) -> WorkflowRun | None:
        with self._lock:
            return self._runs.get(run_id)

    def list_runs(self, workflow_name: str | None = None) -> list[WorkflowRun]:
        with self._lock:
            runs = list(self._runs.values())
        if workflow_name:
            runs = [r for r in runs if r.workflow_name == workflow_name]
        return sorted(runs, key=lambda r: r.started_at, reverse=True)

    def update_job_status(self, run_id: str, job_name: str, status: str) -> None:
        """Update a single job's status within a run."""
        with self._lock:
            run = self._runs.get(run_id)
            if run:
                run.job_statuses[job_name] = status

    def set_job_ids(self, run_id: str, job_ids: dict[str, str]) -> None:
        """Set the SLURM job ID mapping for a run."""
        with self._lock:
            run = self._runs.get(run_id)
            if run:
                run.job_ids = job_ids

    def update_status(self, run_id: str, status: RunStatus) -> None:
        """Update the overall run status."""
        with self._lock:
            run = self._runs.get(run_id)
            if run:
                run.status = status

    def complete_run(self, run_id: str, status: RunStatus = "completed") -> None:
        """Mark a run as completed/failed with timestamp."""
        with self._lock:
            run = self._runs.get(run_id)
            if run:
                run.status = status
                run.completed_at = datetime.now(UTC).isoformat()

    def fail_run(self, run_id: str, error: str) -> None:
        """Mark a run as failed with an error message."""
        with self._lock:
            run = self._runs.get(run_id)
            if run:
                run.status = "failed"
                run.error = error
                run.completed_at = datetime.now(UTC).isoformat()


run_registry = RunRegistry()
