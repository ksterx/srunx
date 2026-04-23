"""srunx - Python library for SLURM job management."""

try:
    from srunx.common._version import __version__
except ImportError:
    __version__ = "0.0.0+unknown"
__author__ = "ksterx"
__description__ = "Python library for SLURM workload manager integration"

# Main public API
from srunx.callbacks import Callback
from srunx.common.logging import (
    configure_cli_logging,
    configure_logging,
    configure_workflow_logging,
    get_logger,
)
from srunx.containers import (
    ApptainerRuntime,
    ContainerRuntime,
    LaunchSpec,
    PyxisRuntime,
    get_runtime,
)
from srunx.domain import (
    BaseJob,
    ContainerResource,
    Job,
    JobEnvironment,
    JobResource,
    JobStatus,
    ShellJob,
    Workflow,
)
from srunx.observability.monitoring.job_monitor import JobMonitor
from srunx.observability.monitoring.resource_monitor import ResourceMonitor
from srunx.observability.monitoring.types import (
    MonitorConfig,
    ResourceSnapshot,
    WatchMode,
)
from srunx.observability.notifications.legacy_slack import SlackCallback
from srunx.runtime.rendering import render_job_script
from srunx.runtime.workflow.runner import WorkflowRunner
from srunx.slurm.local import (
    LocalClient,
    Slurm,
    cancel_job,
    retrieve_job,
    submit_job,
)

__all__ = [
    # Client
    "LocalClient",
    "Slurm",
    "submit_job",
    "retrieve_job",
    "cancel_job",
    # Callbacks
    "Callback",
    "SlackCallback",
    # Containers
    "ContainerRuntime",
    "LaunchSpec",
    "PyxisRuntime",
    "ApptainerRuntime",
    "get_runtime",
    # Models
    "BaseJob",
    "ContainerResource",
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
