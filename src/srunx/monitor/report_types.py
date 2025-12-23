"""Data types for scheduled reporting."""

from dataclasses import dataclass, field
from datetime import datetime, timedelta


@dataclass
class ReportConfig:
    """Configuration for scheduled reporting."""

    schedule: str
    include: list[str] = field(
        default_factory=lambda: ["jobs", "resources", "user", "running"]
    )
    partition: str | None = None
    user: str | None = None
    timeframe: str = "24h"
    daemon: bool = True
    max_jobs: int = 10  # Maximum number of jobs to show in detail

    def __post_init__(self) -> None:
        """Validate configuration."""
        valid_include = {"jobs", "resources", "user", "running"}
        invalid = set(self.include) - valid_include
        if invalid:
            raise ValueError(f"Invalid include options: {invalid}")
        if not self.schedule:
            raise ValueError("Schedule must be specified")
        if self.max_jobs < 1:
            raise ValueError("max_jobs must be at least 1")

    def is_cron_format(self) -> bool:
        """Check if schedule is in cron format."""
        return " " in self.schedule


@dataclass
class JobStats:
    """Job queue statistics."""

    pending: int
    running: int
    completed: int
    failed: int
    cancelled: int

    @property
    def total_active(self) -> int:
        return self.pending + self.running


@dataclass
class ResourceStats:
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


@dataclass
class RunningJob:
    """Information about a running or pending job."""

    job_id: int
    name: str
    user: str
    status: str
    partition: str | None
    runtime: timedelta | None  # None for pending jobs
    nodes: int
    gpus: int


@dataclass
class Report:
    """Generated report containing requested statistics."""

    timestamp: datetime
    job_stats: JobStats | None = None
    resource_stats: ResourceStats | None = None
    user_stats: JobStats | None = None
    running_jobs: list[RunningJob] = field(default_factory=list)
