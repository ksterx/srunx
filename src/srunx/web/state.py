"""In-memory workflow run registry."""

from __future__ import annotations

import threading
from datetime import UTC, datetime
from typing import Literal
from uuid import uuid4

from pydantic import BaseModel, Field


class WorkflowRun(BaseModel):
    """Tracks a single workflow execution."""

    id: str = Field(default_factory=lambda: str(uuid4()))
    workflow_name: str
    started_at: str = Field(default_factory=lambda: datetime.now(UTC).isoformat())
    completed_at: str | None = None
    status: Literal["running", "completed", "failed", "cancelled"] = "running"
    job_statuses: dict[str, str] = Field(default_factory=dict)


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


run_registry = RunRegistry()
