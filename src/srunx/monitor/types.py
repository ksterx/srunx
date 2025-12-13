"""Data models and types for SLURM monitoring."""

from datetime import datetime
from enum import Enum
from pydantic import BaseModel, Field, computed_field


class WatchMode(str, Enum):
    """Monitoring mode enumeration."""

    UNTIL_CONDITION = "until"
    """Monitor until condition is met, then exit"""

    CONTINUOUS = "continuous"
    """Monitor indefinitely, notify on every state change"""

    def __str__(self) -> str:
        return self.value


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

    @property
    def is_aggressive(self) -> bool:
        """Check if polling interval is aggressive (<5 seconds)."""
        return self.poll_interval < 5

    class Config:
        json_schema_extra = {
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


class ResourceSnapshot(BaseModel):
    """Point-in-time snapshot of SLURM partition resources."""

    timestamp: datetime = Field(
        default_factory=datetime.now, description="When this snapshot was taken"
    )
    partition: str | None = Field(
        default=None, description="Partition name (None = all partitions)"
    )
    total_gpus: int = Field(ge=0, description="Total GPUs in partition")
    gpus_in_use: int = Field(ge=0, description="GPUs currently allocated to jobs")
    gpus_available: int = Field(ge=0, description="GPUs available for new jobs")
    jobs_running: int = Field(
        ge=0, description="Number of running jobs using GPUs"
    )
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

    class Config:
        json_schema_extra = {
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
