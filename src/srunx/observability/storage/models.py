"""Pydantic models mirroring the ``srunx.observability.storage`` schema.

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
WatchKind = Literal[
    "job",
    "workflow_run",
    "sweep_run",
    "resource_threshold",
    "scheduled_report",
]
SubscriptionPreset = Literal["terminal", "running_and_terminal", "all", "digest"]
EventKind = Literal[
    "job.submitted",
    "job.status_changed",
    "workflow_run.status_changed",
    "sweep_run.status_changed",
    "resource.threshold_crossed",
    "scheduled_report.due",
]
DeliveryStatus = Literal["pending", "sending", "delivered", "abandoned"]
TransitionSource = Literal["poller", "cli_monitor", "webhook"]
SubmissionSource = Literal["cli", "web", "workflow"]
TransportType = Literal["local", "ssh"]
WorkflowRunStatus = Literal["pending", "running", "completed", "failed", "cancelled"]
WorkflowRunTriggeredBy = Literal["cli", "web", "schedule", "mcp"]
SweepStatus = Literal[
    "pending",
    "running",
    "draining",
    "completed",
    "failed",
    "cancelled",
]
SweepSubmissionSource = Literal["cli", "web", "mcp"]


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
    sweep_run_id: int | None = None


class SweepRun(BaseModel):
    model_config = _MODEL_CONFIG

    id: int | None = None
    name: str
    workflow_yaml_path: str | None = None
    status: SweepStatus
    matrix: dict
    args: dict | None = None
    fail_fast: bool
    max_parallel: int
    cell_count: int
    cells_pending: int = 0
    cells_running: int = 0
    cells_completed: int = 0
    cells_failed: int = 0
    cells_cancelled: int = 0
    submission_source: SweepSubmissionSource
    started_at: datetime
    completed_at: datetime | None = None
    cancel_requested_at: datetime | None = None
    error: str | None = None


class WorkflowRunJob(BaseModel):
    """Row model for ``workflow_run_jobs``.

    ``jobs_row_id`` (V5+) points at ``jobs.id`` (AUTOINCREMENT PK), not
    the SLURM ``job_id``. The legacy ``job_id`` attribute is exposed as
    a read-only mirror for backwards compatibility with callers that
    used to read the SLURM id through this model; it is populated by
    the repository via a ``LEFT JOIN jobs`` at read time.
    """

    model_config = _MODEL_CONFIG

    id: int | None = None
    workflow_run_id: int
    jobs_row_id: int | None = None  # V5: FK to jobs.id (AUTOINCREMENT PK)
    # Populated by the repo via LEFT JOIN jobs ON j.id = jobs_row_id.
    # Kept for legacy call sites that read ``membership.job_id`` as the
    # SLURM id. New code should prefer ``jobs_row_id`` + the repository's
    # join-backed accessor.
    job_id: int | None = None
    job_name: str
    depends_on: list[str] | None = None


class JobStateTransition(BaseModel):
    """Row model for ``job_state_transitions``.

    ``jobs_row_id`` (V5+) points at ``jobs.id`` (AUTOINCREMENT PK).
    """

    model_config = _MODEL_CONFIG

    id: int | None = None
    jobs_row_id: int | None = None
    # Mirror of the parent job's SLURM id, resolved by the repo via
    # ``LEFT JOIN jobs ON j.id = jobs_row_id`` on reads. Legacy callers
    # still access ``.job_id``; new code should resolve it explicitly.
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
    transport_type: TransportType = "local"
    profile_name: str | None = None
    scheduler_key: str = "local"
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
