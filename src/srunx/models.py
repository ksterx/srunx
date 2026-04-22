"""Data models for SLURM job management."""

import os
import re
import shlex
import subprocess
import time
from enum import Enum
from pathlib import Path
from typing import Literal, Self

import jinja2
from pydantic import BaseModel, Field, PrivateAttr, field_validator, model_validator
from rich.console import Console
from rich.syntax import Syntax

from srunx.exceptions import WorkflowValidationError
from srunx.logging import get_logger

logger = get_logger(__name__)
console = Console()


def _get_config_defaults():
    """Get configuration defaults, with lazy import to avoid circular dependencies."""
    try:
        from srunx.config import get_config

        return get_config()
    except ImportError:
        return None


class JobStatus(Enum):
    """Job status enumeration for both SLURM jobs and workflow jobs."""

    UNKNOWN = "UNKNOWN"
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"
    TIMEOUT = "TIMEOUT"


class DependencyType(Enum):
    """Dependency type enumeration for workflow job dependencies."""

    AFTER_OK = "afterok"  # Wait for successful completion (default behavior)
    AFTER = "after"  # Wait for job to start running
    AFTER_ANY = "afterany"  # Wait for job to end regardless of status
    AFTER_NOT_OK = "afternotok"  # Wait for job to fail/end unsuccessfully


class JobDependency(BaseModel):
    """Represents a job dependency with type and target job name."""

    job_name: str = Field(description="Name of the job this dependency refers to")
    dep_type: str = Field(default="afterok", description="Type of dependency")

    @field_validator("dep_type", mode="before")
    @classmethod
    def validate_dep_type(cls, v):
        """Validate dependency type, converting to string value."""
        if isinstance(v, DependencyType):
            return v.value
        elif isinstance(v, str):
            # Validate it's a valid dependency type
            valid_values = [t.value for t in DependencyType]
            if v not in valid_values:
                raise ValueError(
                    f"Invalid dependency type '{v}'. Valid types: {valid_values}"
                )
            return v
        else:
            # Handle enum instance from different module boundaries
            if hasattr(v, "value") and hasattr(v, "name"):
                value = v.value
                valid_values = [t.value for t in DependencyType]
                if value in valid_values:
                    return value
            raise ValueError(f"Invalid dependency type: {v}")

    @property
    def dependency_type(self) -> DependencyType:
        """Get the dependency type as a DependencyType enum."""
        return DependencyType(self.dep_type)

    @classmethod
    def parse(cls, dep_str: str) -> Self:
        """Parse a dependency string into a JobDependency.

        Formats supported:
        - "job_a" -> afterok:job_a (default behavior)
        - "after:job_a" -> after:job_a
        - "afterany:job_a" -> afterany:job_a
        - "afternotok:job_a" -> afternotok:job_a
        - "afterok:job_a" -> afterok:job_a (explicit)
        """
        if ":" in dep_str:
            dep_type_str, job_name = dep_str.split(":", 1)
            valid_types = [t.value for t in DependencyType]
            if dep_type_str not in valid_types:
                raise WorkflowValidationError(
                    f"Invalid dependency type '{dep_type_str}'. "
                    f"Valid types: {valid_types}"
                )
            dep_type = dep_type_str
        else:
            # Default behavior - wait for successful completion
            job_name = dep_str
            dep_type = "afterok"

        return cls(job_name=job_name, dep_type=dep_type)

    def __str__(self) -> str:
        """String representation of the dependency."""
        if self.dep_type == "afterok":
            return self.job_name  # Keep backward compatibility
        return f"{self.dep_type}:{self.job_name}"


class JobResource(BaseModel):
    """SLURM resource allocation requirements."""

    nodes: int = Field(default=1, ge=1, description="Number of compute nodes")
    gpus_per_node: int = Field(default=0, ge=0, description="Number of GPUs per node")
    ntasks_per_node: int = Field(default=1, ge=1, description="Number of jobs per node")
    cpus_per_task: int = Field(default=1, ge=1, description="Number of CPUs per task")
    memory_per_node: str | None = Field(
        default=None, description="Memory per node (e.g., '32GB')"
    )
    time_limit: str | None = Field(
        default=None, description="Time limit (e.g., '1:00:00')"
    )
    nodelist: str | None = Field(
        default=None, description="Specific nodes to use (e.g., 'node001,node002')"
    )
    partition: str | None = Field(
        default=None, description="SLURM partition to use (e.g., 'gpu', 'cpu')"
    )

    @model_validator(mode="before")
    @classmethod
    def apply_config_defaults(cls, data: dict) -> dict:
        """Apply config defaults for fields not explicitly provided."""
        if not isinstance(data, dict):
            return data
        config = _get_config_defaults()
        if config is None:
            return data
        defaults = {
            "nodes": config.resources.nodes,
            "gpus_per_node": config.resources.gpus_per_node,
            "ntasks_per_node": config.resources.ntasks_per_node,
            "cpus_per_task": config.resources.cpus_per_task,
            "memory_per_node": config.resources.memory_per_node,
            "time_limit": config.resources.time_limit,
            "nodelist": config.resources.nodelist,
            "partition": config.resources.partition,
        }
        for key, value in defaults.items():
            if key not in data and value is not None:
                data[key] = value
        return data


class ContainerResource(BaseModel):
    """Container resource allocation requirements.

    Supports Pyxis (--container-* srun flags) and Apptainer/Singularity
    (apptainer exec command wrapping) runtimes.

    Ref (Pyxis): https://github.com/NVIDIA/pyxis/blob/526f46bce2d1a51b2caab65096f6a1ab4272aaa6/README.md?plain=1#L53
    """

    runtime: Literal["pyxis", "apptainer", "singularity"] = Field(
        default="pyxis", description="Container runtime backend"
    )
    image: str | None = Field(default=None, description="Container image")
    mounts: list[str] = Field(default_factory=list, description="Container mounts")
    workdir: str | None = Field(default=None, description="Container work directory")
    # Apptainer-specific fields
    nv: bool = Field(default=False, description="NVIDIA GPU passthrough (--nv)")
    rocm: bool = Field(default=False, description="AMD GPU passthrough (--rocm)")
    cleanenv: bool = Field(default=False, description="Clean environment (--cleanenv)")
    fakeroot: bool = Field(default=False, description="Fake root (--fakeroot)")
    writable_tmpfs: bool = Field(
        default=False, description="Writable tmpfs overlay (--writable-tmpfs)"
    )
    overlay: str | None = Field(
        default=None, description="Overlay image path (--overlay)"
    )
    env: dict[str, str] = Field(
        default_factory=dict, description="Environment variables (--env KEY=VAL)"
    )

    @model_validator(mode="after")
    def validate_runtime_fields(self) -> Self:
        """Ensure Apptainer-only fields are not set for Pyxis runtime."""
        if self.runtime == "pyxis":
            apptainer_fields: dict[str, object] = {
                "nv": self.nv,
                "rocm": self.rocm,
                "cleanenv": self.cleanenv,
                "fakeroot": self.fakeroot,
                "writable_tmpfs": self.writable_tmpfs,
                "overlay": self.overlay,
                "env": self.env,
            }
            set_fields = [
                k
                for k, v in apptainer_fields.items()
                if v is not False and v is not None and v != {}
            ]
            if set_fields:
                raise ValueError(
                    f"Fields {set_fields} are only valid for apptainer/singularity "
                    f"runtime, not '{self.runtime}'"
                )
        return self


class JobEnvironment(BaseModel):
    """Job environment configuration."""

    conda: str | None = Field(default=None, description="Conda environment name")
    venv: str | None = Field(default=None, description="Virtual environment path")
    container: ContainerResource | None = Field(
        default=None, description="Container resource"
    )
    env_vars: dict[str, str] = Field(
        default_factory=dict, description="Environment variables"
    )

    @model_validator(mode="before")
    @classmethod
    def apply_config_defaults(cls, data: dict) -> dict:
        """Apply config defaults for fields not explicitly provided."""
        if not isinstance(data, dict):
            return data
        config = _get_config_defaults()
        if config is None:
            return data
        defaults = {
            "conda": config.environment.conda,
            "venv": config.environment.venv,
            "container": config.environment.container,
            "env_vars": config.environment.env_vars,
        }
        for key, value in defaults.items():
            if key not in data and value is not None:
                data[key] = value
        return data

    @model_validator(mode="after")
    def validate_environment(self) -> Self:
        envs = [self.conda, self.venv]
        non_none_count = sum(x is not None for x in envs)
        if non_none_count > 1:
            raise ValueError("Only one of conda or venv can be specified")
        return self


class BaseJob(BaseModel):
    name: str = Field(default="job", description="Job name")
    job_id: int | None = Field(default=None, description="SLURM job ID")
    depends_on: list[str] = Field(
        default_factory=list, description="Task dependencies for workflow execution"
    )
    exports: dict[str, str] = Field(
        default_factory=dict,
        description=(
            "Values this job exports for downstream jobs to reference via "
            "`{{ deps.<this_job>.<key> }}` at workflow load time."
        ),
    )

    @field_validator("exports")
    @classmethod
    def validate_export_keys(cls, v: dict[str, str]) -> dict[str, str]:
        """Ensure export names are valid identifiers."""
        for key in v:
            if not re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", key):
                raise ValueError(
                    f"Invalid export name: '{key}'. "
                    "Must be a valid identifier (letters, digits, underscores)."
                )
        return v

    retry: int = Field(
        default=0, ge=0, description="Number of retry attempts on failure"
    )
    retry_delay: int = Field(
        default=60, ge=0, description="Delay between retries in seconds"
    )

    # NEW: Runtime information for monitoring
    partition: str | None = Field(
        default=None, description="SLURM partition where job is/was running"
    )
    user: str | None = Field(default=None, description="Username of job owner")
    elapsed_time: str | None = Field(
        default=None,
        description="Elapsed time in SLURM format (e.g., '1-02:30:45')",
    )
    nodes: int | None = Field(
        default=None, ge=0, description="Number of nodes allocated to job"
    )
    nodelist: str | None = Field(
        default=None,
        description="Comma-separated list of nodes (e.g., 'node[01-04]')",
    )
    cpus: int | None = Field(
        default=None, ge=0, description="Total CPU count allocated to job"
    )
    gpus: int | None = Field(
        default=None,
        ge=0,
        description="Total GPU count allocated to job (parsed from TresPerNode)",
    )

    _status: JobStatus = PrivateAttr(default=JobStatus.PENDING)
    _parsed_dependencies: list[JobDependency] = PrivateAttr(default_factory=list)
    _retry_count: int = PrivateAttr(default=0)
    _last_refresh: float = PrivateAttr(default=0.0)

    def model_post_init(self, __context) -> None:
        """Parse string dependencies into JobDependency objects after initialization."""
        self._parsed_dependencies = [
            JobDependency.parse(dep_str) for dep_str in self.depends_on
        ]

    @property
    def parsed_dependencies(self) -> list[JobDependency]:
        """Get the parsed dependency objects."""
        if not self._parsed_dependencies and self.depends_on:
            # Lazy initialization if not already parsed
            self._parsed_dependencies = [
                JobDependency.parse(dep_str) for dep_str in self.depends_on
            ]
        return self._parsed_dependencies

    # Minimum seconds between automatic sacct queries via the status property.
    _REFRESH_INTERVAL: float = 5.0

    @property
    def status(self) -> JobStatus:
        """
        Accessing ``job.status`` triggers a sacct refresh only when the job
        is non-terminal and at least ``_REFRESH_INTERVAL`` seconds have
        elapsed since the last query, preventing excessive subprocess calls.
        """
        if self.job_id is not None and self._status not in {
            JobStatus.COMPLETED,
            JobStatus.FAILED,
            JobStatus.CANCELLED,
            JobStatus.TIMEOUT,
        }:
            now = time.time()
            if now - self._last_refresh >= self._REFRESH_INTERVAL:
                self.refresh()
        return self._status

    @status.setter
    def status(self, value: JobStatus) -> None:
        self._status = value

    def refresh(self, retries: int = 3) -> Self:
        """Query sacct and update ``_status`` in-place."""
        if self.job_id is None:
            return self
        self._last_refresh = time.time()

        try:
            for retry in range(retries):
                try:
                    result = subprocess.run(
                        [
                            "sacct",
                            "-j",
                            str(self.job_id),
                            "--format",
                            "JobID,State",
                            "--noheader",
                            "--parsable2",
                        ],
                        capture_output=True,
                        text=True,
                        check=True,
                    )
                except subprocess.CalledProcessError as e:
                    logger.debug(f"Failed to query job {self.job_id}: {e}")
                    # In test environments, sacct might not be available
                    # Don't raise error, just keep current status
                    return self

                line = (
                    result.stdout.strip().split("\n")[0]
                    if result.stdout.strip()
                    else ""
                )
                if not line:
                    if retry < retries - 1:
                        time.sleep(1)
                        continue
                    self._status = JobStatus.UNKNOWN
                    return self
                break

            if line and "|" in line:
                _, state = line.split("|", 1)
                try:
                    self._status = JobStatus(state)
                except ValueError:
                    # Unknown status, keep current status
                    pass
        except Exception as e:
            logger.debug(f"Error refreshing job {self.job_id}: {e}")
            # Don't fail on refresh errors in tests

        return self

    def dependencies_satisfied(
        self,
        completed_job_names_or_statuses: list[str] | dict[str, JobStatus],
        started_job_names: list[str] | None = None,
        completed_job_names: list[str] | None = None,
    ) -> bool:
        """Check if all dependencies are satisfied based on their types.

        Args:
            completed_job_names_or_statuses: Either list of completed job names (old interface)
                                           or dict mapping job names to their current status (new interface)
            started_job_names: List of jobs that have started (for backward compatibility - unused)
            completed_job_names: List of jobs that have completed successfully (for backward compatibility)
        """
        # Use _status directly to avoid triggering refresh in tests
        current_status = self._status if hasattr(self, "_status") else JobStatus.PENDING

        # For tests: if no job_id is set, this job is not submitted yet so dependencies should be checked
        # For real execution: only check dependencies if this job is pending and not yet submitted
        if self.job_id is not None and current_status not in {
            JobStatus.PENDING,
            JobStatus.UNKNOWN,
        }:
            return False

        # Jobs with no dependencies are always ready if they are pending
        if not self.depends_on:
            return True

        # Handle backward compatibility
        if isinstance(completed_job_names_or_statuses, list):
            # Old interface - first argument is list of completed job names
            completed_job_names = completed_job_names_or_statuses
            return all(dep in completed_job_names for dep in self.depends_on)
        elif completed_job_names is not None:
            # Old interface called with named parameter
            return all(dep in completed_job_names for dep in self.depends_on)

        # New interface - first argument is dict of job statuses
        job_statuses = completed_job_names_or_statuses

        # Ensure parsed dependencies are initialized (robust against module reloads)
        parsed_deps = self.parsed_dependencies  # This will trigger lazy init if needed

        for dep in parsed_deps:
            dep_job_status = job_statuses.get(dep.job_name, JobStatus.PENDING)

            if dep.dep_type == "afterok":
                # Wait for successful completion
                if dep_job_status.value != "COMPLETED":
                    return False

            elif dep.dep_type == "after":
                # Wait for job to start running (RUNNING, COMPLETED, FAILED, etc.)
                if dep_job_status.value == "PENDING":
                    return False

            elif dep.dep_type == "afterany":
                # Wait for job to end regardless of status (COMPLETED, FAILED, CANCELLED, TIMEOUT)
                terminal_statuses = {"COMPLETED", "FAILED", "CANCELLED", "TIMEOUT"}
                if dep_job_status.value not in terminal_statuses:
                    return False

            elif dep.dep_type == "afternotok":
                # Wait for job to fail/end unsuccessfully
                failure_statuses = {"FAILED", "CANCELLED", "TIMEOUT"}
                if dep_job_status.value not in failure_statuses:
                    return False

        return True

    @property
    def retry_count(self) -> int:
        """Get the current retry count."""
        return self._retry_count

    def can_retry(self) -> bool:
        """Check if the job can be retried."""
        return self._retry_count < self.retry

    def increment_retry(self) -> None:
        """Increment the retry count."""
        self._retry_count += 1

    def reset_retry(self) -> None:
        """Reset the retry count."""
        self._retry_count = 0

    def should_retry(self) -> bool:
        """Check if the job should be retried based on status and retry count."""
        return self._status.value == "FAILED" and self.can_retry()


class Job(BaseJob):
    """Represents a SLURM job with complete configuration."""

    command: str | list[str] = Field(description="Command to execute")
    resources: JobResource = Field(
        default_factory=JobResource, description="Resource requirements"
    )
    environment: JobEnvironment = Field(
        default_factory=JobEnvironment, description="Environment setup"
    )
    log_dir: str = Field(
        default_factory=lambda: os.getenv("SLURM_LOG_DIR", "logs"),
        description="Directory for log files",
    )
    work_dir: str = Field(
        default="",
        description=(
            "Working directory. Empty string means 'not specified' — the "
            "renderer omits ``#SBATCH --chdir`` so SLURM uses the submission "
            "directory (matches pre-Phase-2 CLI behavior where ``os.getcwd()`` "
            "default produced the same effective chdir). SSH submission "
            "contexts populate this from the configured mount's remote path "
            "via :func:`srunx.rendering.normalize_job_for_submission`."
        ),
    )

    # Render metadata — optional, used by renderer when no explicit arg given.
    # Explicit arguments to ``render_job_script`` always take precedence so the
    # existing Web non-sweep path (which passes extras directly from raw YAML)
    # keeps its behavior.
    template: str | None = Field(
        default=None,
        description=(
            "Optional template path override. When None, the default template "
            "resolved by Slurm.default_template is used. Explicit "
            "``template_path`` argument to ``render_job_script`` takes "
            "precedence."
        ),
    )
    srun_args: str | None = Field(
        default=None,
        description=(
            "Optional additional srun arguments to inject into the rendered "
            "script. Explicit ``extra_srun_args`` argument to "
            "``render_job_script`` takes precedence if given."
        ),
    )
    launch_prefix: str | None = Field(
        default=None,
        description=(
            "Optional launch-prefix (e.g. ``mpirun`` wrapper) to prepend to the "
            "job command. Explicit ``extra_launch_prefix`` argument to "
            "``render_job_script`` takes precedence if given."
        ),
    )

    @model_validator(mode="before")
    @classmethod
    def apply_config_defaults(cls, data: dict) -> dict:
        """Apply config defaults for log_dir and work_dir."""
        if not isinstance(data, dict):
            return data
        config = _get_config_defaults()
        if config is None:
            return data
        if "log_dir" not in data and config.log_dir:
            data["log_dir"] = config.log_dir
        if "work_dir" not in data and config.work_dir:
            data["work_dir"] = config.work_dir
        return data


class ShellJob(BaseJob):
    script_path: str = Field(description="Shell script path to execute")
    script_vars: dict[str, str | int | float | bool] = Field(
        default_factory=dict, description="Shell script variables"
    )


JobType = BaseJob | Job | ShellJob
RunnableJobType = Job | ShellJob


class Workflow:
    """Represents a workflow containing multiple jobs with dependencies."""

    def __init__(self, name: str, jobs: list[RunnableJobType] | None = None) -> None:
        if jobs is None:
            jobs = []

        self.name = name
        self.jobs = jobs

    def add(self, job: RunnableJobType) -> None:
        # Check that all dependency jobs exist in the workflow
        if job.depends_on:
            job_names = {j.name for j in self.jobs}
            for dep in job.depends_on:
                if dep not in job_names:
                    raise WorkflowValidationError(
                        f"Job '{job.name}' depends on unknown job '{dep}'"
                    )
        self.jobs.append(job)

    def remove(self, job: RunnableJobType) -> None:
        self.jobs.remove(job)

    def get(self, name: str) -> RunnableJobType | None:
        """Get a job by name."""
        for job in self.jobs:
            if job.name == name:
                return job
        return None

    def get_dependencies(self, job_name: str) -> list[str]:
        """Get dependencies for a specific job."""
        job = self.get(job_name)
        return job.depends_on if job else []

    def show(self):
        msg = f"""\
{" PLAN ":=^80}
Workflow: {self.name}
Jobs: {len(self.jobs)}
"""

        def add_indent(indent: int, msg: str) -> str:
            return "    " * indent + msg

        for job in self.jobs:
            msg += add_indent(1, f"Job: {job.name}\n")
            if isinstance(job, Job):
                command_str = (
                    job.command
                    if isinstance(job.command, str)
                    else " ".join(job.command or [])
                )
                msg += add_indent(2, f"{'Command:': <13} {command_str}\n")
                msg += add_indent(
                    2,
                    f"{'Resources:': <13} {job.resources.nodes} nodes, {job.resources.gpus_per_node} GPUs/node\n",
                )
                if job.environment.conda:
                    msg += add_indent(
                        2, f"{'Conda env:': <13} {job.environment.conda}\n"
                    )
                if job.environment.container:
                    msg += add_indent(
                        2, f"{'Container:': <13} {job.environment.container}\n"
                    )
                if job.environment.venv:
                    msg += add_indent(2, f"{'Venv:': <13} {job.environment.venv}\n")
            elif isinstance(job, ShellJob):
                msg += add_indent(2, f"{'Script path:': <13} {job.script_path}\n")
                if job.script_vars:
                    msg += add_indent(2, f"{'Script vars:': <13} {job.script_vars}\n")
            if job.depends_on:
                dep_strs = [str(dep) for dep in job.parsed_dependencies]
                msg += add_indent(2, f"{'Dependencies:': <13} {', '.join(dep_strs)}\n")

        msg += f"{'=' * 80}\n"
        print(msg)

    def validate(self):
        """Validate workflow job dependencies."""
        job_names = {job.name for job in self.jobs}

        if len(job_names) != len(self.jobs):
            raise WorkflowValidationError("Duplicate job names found in workflow")

        for job in self.jobs:
            # Check that all dependency job names exist
            for parsed_dep in job.parsed_dependencies:
                if parsed_dep.job_name not in job_names:
                    raise WorkflowValidationError(
                        f"Job '{job.name}' depends on unknown job '{parsed_dep.job_name}'"
                    )

        # Check for circular dependencies
        visited = set()
        rec_stack = set()

        def has_cycle(job_name: str) -> bool:
            if job_name in rec_stack:
                return True
            if job_name in visited:
                return False

            visited.add(job_name)
            rec_stack.add(job_name)

            job = self.get(job_name)
            if job:
                for parsed_dep in job.parsed_dependencies:
                    if has_cycle(parsed_dep.job_name):
                        return True

            rec_stack.remove(job_name)
            return False

        for job in self.jobs:
            if has_cycle(job.name):
                raise WorkflowValidationError(
                    f"Circular dependency detected involving job '{job.name}'"
                )


def _render_base_script(
    template_path: Path | str,
    template_vars: dict,
    output_filename: str,
    output_dir: Path | str | None = None,
    verbose: bool = False,
) -> str:
    """Base function for rendering SLURM scripts from templates.

    Args:
        template_path: Path to the Jinja template file.
        template_vars: Variables to pass to the template.
        output_filename: Name of the output file.
        output_dir: Directory where the generated script will be saved.
        verbose: Whether to print the rendered content.

    Returns:
        Path to the generated SLURM batch script.

    Raises:
        FileNotFoundError: If the template file does not exist.
        jinja2.TemplateError: If template rendering fails.
    """
    template_file = Path(template_path)
    if not template_file.is_file():
        raise FileNotFoundError(f"Template file '{template_path}' not found")

    with open(template_file, encoding="utf-8") as f:
        template_content = f.read()

    template = jinja2.Template(
        template_content,
        undefined=jinja2.StrictUndefined,
    )

    # Debug: log template variables
    logger.debug(f"Template variables: {template_vars}")

    rendered_content = template.render(template_vars)

    if verbose:
        console.print(
            Syntax(rendered_content, "bash", theme="monokai", line_numbers=True)
        )

    # Generate output file
    if output_dir is not None:
        output_path = Path(output_dir) / output_filename
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(rendered_content)

        return str(output_path)

    else:
        logger.info("`output_dir` is not specified, rendered content is not saved")
        return ""


def render_job_script(
    template_path: Path | str,
    job: Job,
    output_dir: Path | str | None = None,
    verbose: bool = False,
    extra_srun_args: str | None = None,
    extra_launch_prefix: str | None = None,
) -> str:
    """Render a SLURM job script from a template.

    Args:
        template_path: Path to the Jinja template file.
        job: Job configuration.
        output_dir: Directory where the generated script will be saved.
        verbose: Whether to print the rendered content.
        extra_srun_args: Additional srun flags to append after auto-generated ones.
        extra_launch_prefix: Additional launch prefix to append after auto-generated ones.

    Returns:
        Path to the generated SLURM batch script.

    Raises:
        FileNotFoundError: If the template file does not exist.
        jinja2.TemplateError: If template rendering fails.
    """
    # Prepare template variables.
    #
    # ``list[str]`` commands are rendered with ``shlex.join`` so shell
    # metacharacters stay quoted. The previous ``" ".join(...)`` silently
    # collapsed commands like ``["bash", "-c", "echo X; sleep 5; echo Y"]``
    # into ``bash -c echo X; sleep 5; echo Y`` — the third argument's shell
    # metacharacters escaped their intended ``-c`` payload and got
    # reinterpreted by the outer shell. ``shlex.join`` preserves the
    # original token boundaries via minimal single-quote wrapping.
    if isinstance(job.command, str):
        command_str = job.command
    else:
        command_str = shlex.join(job.command or [])
    environment_setup, srun_args, launch_prefix = _build_environment_setup(
        job.environment
    )
    # Fallback to Job-level metadata when explicit args absent.
    # Explicit args always win to preserve existing Web non-sweep path
    # (which passes extras from raw YAML directly).
    effective_extra_srun_args = (
        extra_srun_args if extra_srun_args is not None else job.srun_args
    )
    effective_extra_launch_prefix = (
        extra_launch_prefix if extra_launch_prefix is not None else job.launch_prefix
    )
    # Merge user-specified extras with auto-generated values
    if effective_extra_srun_args:
        srun_args = f"{srun_args} {effective_extra_srun_args}".strip()
    if effective_extra_launch_prefix:
        launch_prefix = f"{launch_prefix} {effective_extra_launch_prefix}".strip()

    template_vars = {
        "job_name": job.name,
        "command": command_str,
        "log_dir": job.log_dir,
        "work_dir": job.work_dir,
        "environment_setup": environment_setup,
        "srun_args": srun_args,
        "launch_prefix": launch_prefix,
        "container": job.environment.container,
        **job.resources.model_dump(),
    }

    return _render_base_script(
        template_path=template_path,
        template_vars=template_vars,
        output_filename=f"{job.name}.slurm",
        output_dir=output_dir,
        verbose=verbose,
    )


def _build_environment_setup(
    environment: JobEnvironment,
) -> tuple[str, str, str]:
    """Build environment setup script.

    Returns:
        A 3-tuple of (env_setup_lines, srun_args, launch_prefix).
        - env_setup_lines: Shell setup including env vars, conda/venv activation,
          and container prelude (if any).
        - srun_args: Flags passed to srun (Pyxis uses this).
        - launch_prefix: Command wrapper (Apptainer uses this).
    """
    from srunx.containers import get_runtime

    setup_lines: list[str] = []

    # 1. Environment variables (single-quoted to prevent shell injection)
    for key, value in environment.env_vars.items():
        escaped_value = str(value).replace("'", "'\\''")
        setup_lines.append(f"export {key}='{escaped_value}'")

    # 2. Conda/venv activation (independent of container)
    if environment.conda:
        home_dir = Path.home()
        escaped_conda = environment.conda.replace("'", "'\\''")
        setup_lines.extend(
            [
                f"source {str(home_dir)}/miniconda3/bin/activate",
                "conda deactivate",
                f"conda activate '{escaped_conda}'",
            ]
        )
    elif environment.venv:
        escaped_venv = environment.venv.replace("'", "'\\''")
        setup_lines.append(f"source '{escaped_venv}'/bin/activate")

    # 3. Container setup (independent of conda/venv)
    # Only process container if it has an image — a runtime-only container
    # (no image) is not actionable and would generate broken commands.
    srun_args = ""
    launch_prefix = ""
    if environment.container and environment.container.image:
        runtime = get_runtime(environment.container.runtime)
        spec = runtime.build_launch_spec(environment.container)
        if spec.prelude:
            setup_lines.append(spec.prelude)
        srun_args = spec.srun_args
        launch_prefix = spec.launch_prefix

    return "\n".join(setup_lines), srun_args, launch_prefix


def render_shell_job_script(
    template_path: Path | str,
    job: ShellJob,
    output_dir: Path | str | None = None,
    verbose: bool = False,
) -> str:
    """Render a SLURM shell job script from a template.

    Args:
        template_path: Path to the Jinja template file.
        job: ShellJob configuration.
        output_dir: Directory where the generated script will be saved.
        verbose: Whether to print the rendered content.

    Returns:
        Path to the generated SLURM batch script.

    Raises:
        FileNotFoundError: If the template file does not exist.
        jinja2.TemplateError: If template rendering fails.
    """
    template_file = Path(template_path)
    output_filename = f"{template_file.stem}.slurm"

    return _render_base_script(
        template_path=template_path,
        template_vars=job.script_vars,
        output_filename=output_filename,
        output_dir=output_dir,
        verbose=verbose,
    )
