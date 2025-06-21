"""srunx - Python library for SLURM job management."""

__author__ = "ksterx"
__description__ = "Python library for SLURM workload manager integration"

# Main public API
from .callbacks import Callback
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
from .runner import WorkflowRunner

__all__ = [
    # Client
    "Slurm",
    "submit_job",
    "retrieve_job",
    "cancel_job",
    "Callback",
    # Models
    "BaseJob",
    "Job",
    "ShellJob",
    "JobResource",
    "JobEnvironment",
    "JobStatus",
    "Workflow",
    "render_job_script",
    # Workflows
    "WorkflowRunner",
    # Logging
    "configure_logging",
    "configure_cli_logging",
    "configure_workflow_logging",
    "get_logger",
]
