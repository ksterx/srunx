"""Domain layer — pure data models for SLURM jobs and workflows.

No imports from slurm/, runtime/, observability/, integrations/, or
interfaces/. Only ``support`` (logging, exceptions, config defaults) is
reachable from here.
"""

from srunx.domain.jobs import (
    BaseJob,
    ContainerResource,
    DependencyType,
    Job,
    JobDependency,
    JobEnvironment,
    JobResource,
    JobStatus,
    JobType,
    RunnableJobType,
    ShellJob,
)
from srunx.domain.workflow import Workflow

__all__ = [
    "BaseJob",
    "ContainerResource",
    "DependencyType",
    "Job",
    "JobDependency",
    "JobEnvironment",
    "JobResource",
    "JobStatus",
    "JobType",
    "RunnableJobType",
    "ShellJob",
    "Workflow",
]
