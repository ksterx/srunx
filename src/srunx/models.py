"""Data models for SLURM job management."""

import os
import subprocess
import time
from enum import Enum
from pathlib import Path
from typing import Self

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
    except (ImportError, Exception):
        # Fallback if config module is not available or fails
        return None


def _default_nodes():
    """Get default nodes from config."""
    config = _get_config_defaults()
    return config.resources.nodes if config else 1


def _default_gpus_per_node():
    """Get default GPUs per node from config."""
    config = _get_config_defaults()
    return config.resources.gpus_per_node if config else 0


def _default_ntasks_per_node():
    """Get default ntasks per node from config."""
    config = _get_config_defaults()
    return config.resources.ntasks_per_node if config else 1


def _default_cpus_per_task():
    """Get default CPUs per task from config."""
    config = _get_config_defaults()
    return config.resources.cpus_per_task if config else 1


def _default_memory_per_node():
    """Get default memory per node from config."""
    config = _get_config_defaults()
    return config.resources.memory_per_node if config else None


def _default_time_limit():
    """Get default time limit from config."""
    config = _get_config_defaults()
    return config.resources.time_limit if config else None


def _default_nodelist():
    """Get default nodelist from config."""
    config = _get_config_defaults()
    return config.resources.nodelist if config else None


def _default_partition():
    """Get default partition from config."""
    config = _get_config_defaults()
    return config.resources.partition if config else None


def _default_conda():
    """Get default conda environment from config."""
    config = _get_config_defaults()
    return config.environment.conda if config else None


def _default_venv():
    """Get default venv path from config."""
    config = _get_config_defaults()
    return config.environment.venv if config else None


def _default_container():
    """Get default container resource from config."""
    config = _get_config_defaults()
    return config.environment.container if config else None


def _default_env_vars():
    """Get default environment variables from config."""
    config = _get_config_defaults()
    return config.environment.env_vars if config else {}


def _default_log_dir():
    """Get default log directory from config."""
    config = _get_config_defaults()
    return config.log_dir if config else os.getenv("SLURM_LOG_DIR", "logs")


def _default_work_dir():
    """Get default work directory from config."""
    config = _get_config_defaults()
    return config.work_dir if config else None


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

    nodes: int = Field(
        default_factory=_default_nodes, ge=1, description="Number of compute nodes"
    )
    gpus_per_node: int = Field(
        default_factory=_default_gpus_per_node,
        ge=0,
        description="Number of GPUs per node",
    )
    ntasks_per_node: int = Field(
        default_factory=_default_ntasks_per_node,
        ge=1,
        description="Number of jobs per node",
    )
    cpus_per_task: int = Field(
        default_factory=_default_cpus_per_task,
        ge=1,
        description="Number of CPUs per task",
    )
    memory_per_node: str | None = Field(
        default_factory=_default_memory_per_node,
        description="Memory per node (e.g., '32GB')",
    )
    time_limit: str | None = Field(
        default_factory=_default_time_limit, description="Time limit (e.g., '1:00:00')"
    )
    nodelist: str | None = Field(
        default_factory=_default_nodelist,
        description="Specific nodes to use (e.g., 'node001,node002')",
    )
    partition: str | None = Field(
        default_factory=_default_partition,
        description="SLURM partition to use (e.g., 'gpu', 'cpu')",
    )


class ContainerResource(BaseModel):
    """Container resource allocation requirements.

    Ref: https://github.com/NVIDIA/pyxis/blob/526f46bce2d1a51b2caab65096f6a1ab4272aaa6/README.md?plain=1#L53
    """

    image: str | None = Field(default=None, description="Container image")
    mounts: list[str] = Field(default_factory=list, description="Container mounts")
    workdir: str | None = Field(default=None, description="Container work directory")


class JobEnvironment(BaseModel):
    """Job environment configuration."""

    conda: str | None = Field(
        default_factory=_default_conda, description="Conda environment name"
    )
    venv: str | None = Field(
        default_factory=_default_venv, description="Virtual environment path"
    )
    container: ContainerResource | None = Field(
        default_factory=_default_container, description="Container resource"
    )
    env_vars: dict[str, str] = Field(
        default_factory=_default_env_vars, description="Environment variables"
    )

    @model_validator(mode="after")
    def validate_environment(self) -> Self:
        envs = [self.conda, self.venv, self.container]
        non_none_count = sum(x is not None for x in envs)
        if non_none_count == 0:
            logger.info("No virtual environment is set.")
        elif non_none_count > 1:
            raise ValueError(
                "Only one virtual environment (conda, venv, or container) can be specified"
            )
        return self


class BaseJob(BaseModel):
    name: str = Field(default="job", description="Job name")
    job_id: int | None = Field(default=None, description="SLURM job ID")
    depends_on: list[str] = Field(
        default_factory=list, description="Task dependencies for workflow execution"
    )
    retry: int = Field(
        default=0, ge=0, description="Number of retry attempts on failure"
    )
    retry_delay: int = Field(
        default=60, ge=0, description="Delay between retries in seconds"
    )

    _status: JobStatus = PrivateAttr(default=JobStatus.PENDING)
    _parsed_dependencies: list[JobDependency] = PrivateAttr(default_factory=list)
    _retry_count: int = PrivateAttr(default=0)

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

    @property
    def status(self) -> JobStatus:
        """
        Accessing ``job.status`` always triggers a lightweight refresh
        (only if we have a ``job_id`` and the status isn't terminal).
        """
        if self.job_id is not None and self._status not in {
            JobStatus.COMPLETED,
            JobStatus.FAILED,
            JobStatus.CANCELLED,
            JobStatus.TIMEOUT,
        }:
            self.refresh()
        return self._status

    @status.setter
    def status(self, value: JobStatus) -> None:
        self._status = value

    def refresh(self, retries: int = 3) -> Self:
        """Query sacct and update ``_status`` in-place."""
        if self.job_id is None:
            return self

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
        default_factory=_default_log_dir,
        description="Directory for log files",
    )
    work_dir: str = Field(
        default_factory=lambda: _default_work_dir() or os.getcwd(),
        description="Working directory",
    )


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
        # Check if job already exists
        if job.depends_on:
            for dep in job.depends_on:
                if dep not in self.jobs:
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
                return job.refresh()
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
) -> str:
    """Render a SLURM job script from a template.

    Args:
        template_path: Path to the Jinja template file.
        job: Job configuration.
        output_dir: Directory where the generated script will be saved.
        verbose: Whether to print the rendered content.

    Returns:
        Path to the generated SLURM batch script.

    Raises:
        FileNotFoundError: If the template file does not exist.
        jinja2.TemplateError: If template rendering fails.
    """
    # Prepare template variables
    command_str = (
        job.command if isinstance(job.command, str) else " ".join(job.command or [])
    )
    template_vars = {
        "job_name": job.name,
        "command": command_str,
        "log_dir": job.log_dir,
        "work_dir": job.work_dir,
        "environment_setup": _build_environment_setup(job.environment),
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


def _build_environment_setup(environment: JobEnvironment) -> str:
    """Build environment setup script."""
    setup_lines = []

    # Set environment variables
    for key, value in environment.env_vars.items():
        setup_lines.append(f"export {key}={value}")

    # Activate environments
    if environment.conda:
        home_dir = Path.home()
        setup_lines.extend(
            [
                f"source {str(home_dir)}/miniconda3/bin/activate",
                "conda deactivate",
                f"conda activate {environment.conda}",
            ]
        )
    elif environment.venv:
        setup_lines.append(f"source {environment.venv}/bin/activate")
    elif environment.container:
        container_args = []
        if environment.container.image:
            container_args.append(f"--container-image {environment.container.image}")
        if environment.container.mounts:
            container_args.append(
                f"--container-mounts {','.join(environment.container.mounts)}"
            )
        if environment.container.workdir:
            container_args.append(
                f"--container-workdir {environment.container.workdir}"
            )
        setup_lines.extend(
            [
                "declare -a CONTAINER_ARGS=(",
                *container_args,
                ")",
            ]
        )

    return "\n".join(setup_lines)


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
