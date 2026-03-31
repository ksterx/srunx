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


class RunRegistry:
    """Thread-safe in-memory registry for workflow runs."""

    def __init__(self) -> None:
        self._runs: dict[str, WorkflowRun] = {}
        self._lock = threading.Lock()

    def create(self, workflow_name: str) -> WorkflowRun:
        run = WorkflowRun(workflow_name=workflow_name)
        with self._lock:
            self._runs[run.id] = run
        return run

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
