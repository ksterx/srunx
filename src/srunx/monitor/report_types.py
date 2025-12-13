"""Data types for scheduled reporting."""

from dataclasses import dataclass, field
from datetime import datetime


@dataclass
class ReportConfig:
    """Configuration for scheduled reporting.

    Args:
        schedule: Schedule specification (interval or cron format)
            - Interval: "1h", "30m", "1d" etc.
            - Cron: "0 * * * *" (minute hour day month weekday)
        include: List of report sections to include
            - "jobs": Job queue statistics
            - "resources": GPU/node resource statistics
            - "user": User-specific job statistics
        partition: SLURM partition to monitor (for resource stats)
        user: User to filter jobs (defaults to current user)
        timeframe: Time window for completed/failed job aggregation
        daemon: Run as background daemon process

    Raises:
        ValueError: If schedule format is invalid
    """

    schedule: str
    include: list[str] = field(default_factory=lambda: ["jobs", "resources", "user"])
    partition: str | None = None
    user: str | None = None
    timeframe: str = "24h"
    daemon: bool = True

    def __post_init__(self) -> None:
        """Validate configuration."""
        # Validate include options
        valid_include = {"jobs", "resources", "user"}
        invalid = set(self.include) - valid_include
        if invalid:
            raise ValueError(
                f"Invalid include options: {invalid}. Valid options: {valid_include}"
            )

        # Validate schedule format (basic check)
        if not self.schedule:
            raise ValueError("Schedule must be specified")

    def is_cron_format(self) -> bool:
        """Check if schedule is in cron format.

        Returns:
            True if schedule contains spaces (cron format),
            False if it's interval format (e.g., "1h", "30m")
        """
        return " " in self.schedule


@dataclass
class JobStats:
    """Job queue statistics.

    Args:
        pending: Number of pending jobs
        running: Number of running jobs
        completed: Number of completed jobs (within timeframe)
        failed: Number of failed jobs (within timeframe)
        cancelled: Number of cancelled jobs (within timeframe)
        total_active: Total active jobs (pending + running)
    """

    pending: int
    running: int
    completed: int
    failed: int
    cancelled: int

    @property
    def total_active(self) -> int:
        """Calculate total active jobs."""
        return self.pending + self.running


@dataclass
class ResourceStats:
    """GPU and node resource statistics.

    Args:
        partition: Partition name (None for default partition)
        total_gpus: Total GPU count in partition
        gpus_in_use: Number of GPUs currently allocated
        gpus_available: Number of free GPUs
        nodes_total: Total number of nodes
        nodes_idle: Number of idle nodes
        nodes_down: Number of down/offline nodes
    """

    partition: str | None
    total_gpus: int
    gpus_in_use: int
    gpus_available: int
    nodes_total: int
    nodes_idle: int
    nodes_down: int

    @property
    def utilization(self) -> float:
        """Calculate GPU utilization percentage.

        Returns:
            Utilization as percentage (0-100)
        """
        if self.total_gpus == 0:
            return 0.0
        return (self.gpus_in_use / self.total_gpus) * 100


@dataclass
class Report:
    """Generated report containing requested statistics.

    Args:
        timestamp: Report generation timestamp
        job_stats: Job queue statistics (if included)
        resource_stats: Resource statistics (if included)
        user_stats: User-specific job statistics (if included)
    """

    timestamp: datetime
    job_stats: JobStats | None = None
    resource_stats: ResourceStats | None = None
    user_stats: JobStats | None = None
