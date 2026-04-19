"""Pydantic models mirroring the ``srunx.db`` schema.

These types are produced by the repository layer when reading rows and
consumed by callers (poller, routers, notification service). They are
deliberately close to the SQL shape — JSON columns are typed as ``dict``
and (de)serialized in the repository, timestamps are ``datetime``.

See ``.claude/specs/notification-and-state-persistence/design.md`` for
the full schema.
"""

from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict

# Reusable config: allow population by alias / from ORM-style row objects and
# permit arbitrary types so ``dict`` payload fields round-trip cleanly.
_MODEL_CONFIG = ConfigDict(extra="forbid")


# ---------------------------------------------------------------------------
# Notification domain
# ---------------------------------------------------------------------------

EndpointKind = Literal["slack_webhook", "generic_webhook", "email", "slack_bot"]
WatchKind = Literal["job", "workflow_run", "resource_threshold", "scheduled_report"]
SubscriptionPreset = Literal["terminal", "running_and_terminal", "all", "digest"]
EventKind = Literal[
    "job.submitted",
    "job.status_changed",
    "workflow_run.status_changed",
    "resource.threshold_crossed",
    "scheduled_report.due",
]
DeliveryStatus = Literal["pending", "sending", "delivered", "abandoned"]
TransitionSource = Literal["poller", "cli_monitor", "webhook"]
SubmissionSource = Literal["cli", "web", "workflow"]
WorkflowRunStatus = Literal["pending", "running", "completed", "failed", "cancelled"]
WorkflowRunTriggeredBy = Literal["cli", "web", "schedule"]


class Endpoint(BaseModel):
    model_config = _MODEL_CONFIG

    id: int | None = None
    kind: EndpointKind
    name: str
    config: dict
    created_at: datetime
    disabled_at: datetime | None = None


class Watch(BaseModel):
    model_config = _MODEL_CONFIG

    id: int | None = None
    kind: WatchKind
    target_ref: str
    filter: dict | None = None
    created_at: datetime
    closed_at: datetime | None = None


class Subscription(BaseModel):
    model_config = _MODEL_CONFIG

    id: int | None = None
    watch_id: int
    endpoint_id: int
    preset: SubscriptionPreset
    created_at: datetime


class Event(BaseModel):
    model_config = _MODEL_CONFIG

    id: int | None = None
    kind: EventKind
    source_ref: str
    payload: dict
    payload_hash: str
    observed_at: datetime


class Delivery(BaseModel):
    model_config = _MODEL_CONFIG

    id: int | None = None
    event_id: int
    subscription_id: int
    endpoint_id: int
    idempotency_key: str
    status: DeliveryStatus
    attempt_count: int = 0
    next_attempt_at: datetime
    leased_until: datetime | None = None
    worker_id: str | None = None
    last_error: str | None = None
    delivered_at: datetime | None = None
    created_at: datetime


# ---------------------------------------------------------------------------
# Workflow & job persistence
# ---------------------------------------------------------------------------


class WorkflowRun(BaseModel):
    model_config = _MODEL_CONFIG

    id: int | None = None
    workflow_name: str
    workflow_yaml_path: str | None = None
    status: WorkflowRunStatus
    started_at: datetime
    completed_at: datetime | None = None
    args: dict | None = None
    error: str | None = None
    triggered_by: WorkflowRunTriggeredBy


class WorkflowRunJob(BaseModel):
    model_config = _MODEL_CONFIG

    id: int | None = None
    workflow_run_id: int
    job_id: int | None = None  # filled after SLURM submit
    job_name: str
    depends_on: list[str] | None = None


class JobStateTransition(BaseModel):
    model_config = _MODEL_CONFIG

    id: int | None = None
    job_id: int | None = None
    from_status: str | None = None
    to_status: str
    observed_at: datetime
    source: TransitionSource


class ResourceSnapshot(BaseModel):
    model_config = _MODEL_CONFIG

    id: int | None = None
    observed_at: datetime
    partition: str | None = None
    gpus_total: int
    gpus_available: int
    gpus_in_use: int
    nodes_total: int
    nodes_idle: int
    nodes_down: int
    gpu_utilization: float | None = None  # computed column


class Job(BaseModel):
    model_config = _MODEL_CONFIG

    id: int | None = None
    job_id: int
    name: str
    command: list[str] | None = None
    status: str
    nodes: int | None = None
    gpus_per_node: int | None = None
    memory_per_node: str | None = None
    time_limit: str | None = None
    partition: str | None = None
    nodelist: str | None = None
    conda: str | None = None
    venv: str | None = None
    container: str | None = None
    env_vars: dict | None = None
    submitted_at: datetime
    started_at: datetime | None = None
    completed_at: datetime | None = None
    duration_secs: int | None = None
    workflow_run_id: int | None = None
    submission_source: SubmissionSource
    log_file: str | None = None
    metadata: dict | None = None
