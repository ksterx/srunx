"""srunx - Python library for SLURM job management."""

__version__ = "0.7.0"
__author__ = "ksterx"
__description__ = "Python library for SLURM workload manager integration"

# Main public API
from .callbacks import Callback, SlackCallback
from .client import Slurm, cancel_job, retrieve_job, submit_job
from .logging import (
    configure_cli_logging,
    configure_logging,
    configure_workflow_logging,
    get_logger,
)
from .models import (
    BaseJob,
    Job,
    JobEnvironment,
    JobResource,
    JobStatus,
    ShellJob,
    Workflow,
    render_job_script,
)
from .monitor.job_monitor import JobMonitor
from .monitor.resource_monitor import ResourceMonitor
from .monitor.types import MonitorConfig, ResourceSnapshot, WatchMode
from .runner import WorkflowRunner

__all__ = [
    # Client
    "Slurm",
    "submit_job",
    "retrieve_job",
    "cancel_job",
    # Callbacks
    "Callback",
    "SlackCallback",
    # Models
    "BaseJob",
    "Job",
    "ShellJob",
    "JobResource",
    "JobEnvironment",
    "JobStatus",
    "Workflow",
    "render_job_script",
    # Monitoring
    "JobMonitor",
    "ResourceMonitor",
    "MonitorConfig",
    "ResourceSnapshot",
    "WatchMode",
    # Workflows
    "WorkflowRunner",
    # Logging
    "configure_logging",
    "configure_cli_logging",
    "configure_workflow_logging",
    "get_logger",
]
