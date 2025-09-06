"""Data models for SLURM job management."""

import os
import subprocess
import time
from enum import Enum
from pathlib import Path
from typing import Self

import jinja2
from pydantic import BaseModel, Field, PrivateAttr, model_validator
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
        # Fallback if config module is not available
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

    _status: JobStatus = PrivateAttr(default=JobStatus.PENDING)

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
                logger.error(f"Failed to query job {self.job_id}: {e}")
                raise

            line = result.stdout.strip().split("\n")[0] if result.stdout.strip() else ""
            if not line:
                if retry < retries - 1:
                    time.sleep(1)
                    continue
                self._status = JobStatus.UNKNOWN
                return self
            break

        _, state = line.split("|", 1)
        self._status = JobStatus(state)
        return self

    def dependencies_satisfied(self, completed_job_names: list[str]) -> bool:
        """All dependencies are completed & this job is still pending."""
        return self.status == JobStatus.PENDING and all(
            dep in completed_job_names for dep in self.depends_on
        )


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
    script_vars: dict[str, str] = Field(
        default_factory=dict, description="Shell script variables"
    )


type JobType = BaseJob | Job | ShellJob
type RunnableJobType = Job | ShellJob


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
                msg += add_indent(
                    2, f"{'Dependencies:': <13} {', '.join(job.depends_on)}\n"
                )

        msg += f"{'=' * 80}\n"
        print(msg)

    def validate(self):
        """Validate workflow job dependencies."""
        job_names = {job.name for job in self.jobs}

        if len(job_names) != len(self.jobs):
            raise WorkflowValidationError("Duplicate job names found in workflow")

        for job in self.jobs:
            for dependency in job.depends_on:
                if dependency not in job_names:
                    raise WorkflowValidationError(
                        f"Job '{job.name}' depends on unknown job '{dependency}'"
                    )

        # Check for circular dependencies (simple check)
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
                for dependency in job.depends_on:
                    if has_cycle(dependency):
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
