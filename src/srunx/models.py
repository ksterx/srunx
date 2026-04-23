"""Backward-compat shim. Canonical homes are :mod:`srunx.domain`
(types) and :mod:`srunx.runtime.rendering` (render functions).

External callers should migrate to the canonical paths. This shim is
kept only to avoid breaking existing imports during the restructure
(#156); it will be removed in a later phase.
"""

from srunx.domain import (
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
    Workflow,
)
from srunx.runtime.rendering import (
    _build_environment_setup,
    _render_base_script,
    render_job_script,
    render_shell_job_script,
)

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
    "_build_environment_setup",
    "_render_base_script",
    "render_job_script",
    "render_shell_job_script",
]
