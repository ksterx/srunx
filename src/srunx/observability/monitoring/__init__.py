"""
SLURM monitoring module.

This module provides job and resource monitoring capabilities for SLURM clusters,
including configurable polling, Slack notifications, and both until-condition and
continuous monitoring modes.
"""

from srunx.observability.monitoring.base import BaseMonitor
from srunx.observability.monitoring.job_monitor import JobMonitor
from srunx.observability.monitoring.resource_monitor import ResourceMonitor
from srunx.observability.monitoring.types import (
    MonitorConfig,
    ResourceSnapshot,
    WatchMode,
)

__all__ = [
    "BaseMonitor",
    "JobMonitor",
    "ResourceMonitor",
    "MonitorConfig",
    "ResourceSnapshot",
    "WatchMode",
]
