"""Data models and types for SLURM monitoring + scheduled reporting.

Consolidates the former ``monitor/types.py`` + ``monitor/report_types.py``
(#164 Phase 8c). The scheduled-reporting models (``ReportConfig``,
``JobStats``, ``ResourceStats``, ``RunningJob``, ``Report``) previously
lived in a sibling ``report_types`` module — they now share this file so
observability types have a single home.
"""

from datetime import datetime, timedelta
from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field, computed_field, model_validator


class WatchMode(StrEnum):
    """Monitoring mode enumeration."""

    UNTIL_CONDITION = "until"
    """Monitor until condition is met, then exit"""

    CONTINUOUS = "continuous"
    """Monitor indefinitely, notify on every state change"""


class MonitorConfig(BaseModel):
    """Configuration for monitoring operations."""

    poll_interval: int = Field(
        default=60, ge=1, description="Polling interval in seconds (minimum 1)"
    )
    timeout: int | None = Field(
        default=None,
        ge=1,
        description="Maximum monitoring duration in seconds (None = unlimited)",
    )
    mode: WatchMode = Field(
        default=WatchMode.UNTIL_CONDITION,
        description="Monitoring mode (until condition met or continuous)",
    )
    notify_on_change: bool = Field(
        default=True, description="Send notifications when state changes"
    )

    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "poll_interval": 60,
                    "timeout": 3600,
                    "mode": "until",
                    "notify_on_change": True,
                },
                {
                    "poll_interval": 5,
                    "timeout": None,
                    "mode": "continuous",
                    "notify_on_change": True,
                },
            ]
        }
    )

    @property
    def is_aggressive(self) -> bool:
        """Check if polling interval is aggressive (<5 seconds)."""
        return self.poll_interval < 5


class ResourceSnapshot(BaseModel):
    """Point-in-time snapshot of SLURM partition resources."""

    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "timestamp": "2025-12-13T10:30:00",
                    "partition": "gpu",
                    "total_gpus": 16,
                    "gpus_in_use": 12,
                    "gpus_available": 4,
                    "jobs_running": 8,
                    "nodes_total": 8,
                    "nodes_idle": 2,
                    "nodes_down": 1,
                }
            ]
        }
    )

    timestamp: datetime = Field(
        default_factory=datetime.now, description="When this snapshot was taken"
    )
    partition: str | None = Field(
        default=None, description="Partition name (None = all partitions)"
    )
    total_gpus: int = Field(ge=0, description="Total GPUs in partition")
    gpus_in_use: int = Field(ge=0, description="GPUs currently allocated to jobs")
    gpus_available: int = Field(ge=0, description="GPUs available for new jobs")
    jobs_running: int = Field(ge=0, description="Number of running jobs using GPUs")
    nodes_total: int = Field(ge=0, description="Total nodes in partition")
    nodes_idle: int = Field(ge=0, description="Idle nodes ready for jobs")
    nodes_down: int = Field(
        default=0,
        ge=0,
        description="Nodes in DOWN/DRAIN/DRAINING state (excluded from availability)",
    )

    @computed_field  # type: ignore[prop-decorator]
    @property
    def gpu_utilization(self) -> float:
        """GPU utilization percentage (0.0 to 1.0)."""
        if self.total_gpus == 0:
            return 0.0
        return self.gpus_in_use / self.total_gpus

    @computed_field  # type: ignore[prop-decorator]
    @property
    def has_available_gpus(self) -> bool:
        """Check if any GPUs are available."""
        return self.gpus_available > 0

    def meets_threshold(self, min_gpus: int) -> bool:
        """
        Check if available GPUs meet minimum threshold.

        Args:
            min_gpus: Minimum required GPUs

        Returns:
            True if gpus_available >= min_gpus
        """
        return self.gpus_available >= min_gpus


class ReportConfig(BaseModel):
    """Configuration for scheduled reporting."""

    schedule: str
    include: list[str] = Field(
        default_factory=lambda: ["jobs", "resources", "user", "running"]
    )
    partition: str | None = None
    user: str | None = None
    timeframe: str = "24h"
    daemon: bool = True
    max_jobs: int = 10

    @model_validator(mode="after")
    def _validate(self) -> "ReportConfig":
        valid_include = {"jobs", "resources", "user", "running"}
        invalid = set(self.include) - valid_include
        if invalid:
            raise ValueError(f"Invalid include options: {invalid}")
        if not self.schedule:
            raise ValueError("Schedule must be specified")
        if self.max_jobs < 1:
            raise ValueError("max_jobs must be at least 1")
        return self

    def is_cron_format(self) -> bool:
        """Check if schedule is in cron format."""
        return " " in self.schedule


class JobStats(BaseModel):
    """Job queue statistics."""

    pending: int
    running: int
    completed: int
    failed: int
    cancelled: int

    @property
    def total_active(self) -> int:
        return self.pending + self.running


class ResourceStats(BaseModel):
    """GPU and node resource statistics."""

    partition: str | None
    total_gpus: int
    gpus_in_use: int
    gpus_available: int
    nodes_total: int
    nodes_idle: int
    nodes_down: int

    @property
    def utilization(self) -> float:
        if self.total_gpus == 0:
            return 0.0
        return (self.gpus_in_use / self.total_gpus) * 100


class RunningJob(BaseModel):
    """Information about a running or pending job."""

    job_id: int
    name: str
    user: str
    status: str
    partition: str | None
    runtime: timedelta | None = None
    nodes: int
    gpus: int


class Report(BaseModel):
    """Generated report containing requested statistics."""

    timestamp: datetime
    job_stats: JobStats | None = None
    resource_stats: ResourceStats | None = None
    user_stats: JobStats | None = None
    running_jobs: list[RunningJob] = Field(default_factory=list)
