"""Data types for scheduled reporting."""

from datetime import datetime, timedelta

from pydantic import BaseModel, Field, model_validator


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
