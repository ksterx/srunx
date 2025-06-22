"""Data models for SLURM job management."""

import os
import subprocess
import time
from enum import Enum
from pathlib import Path
from typing import Self

import jinja2
from pydantic import BaseModel, Field, PrivateAttr, model_validator

from srunx.exceptions import WorkflowValidationError
from srunx.logging import get_logger

logger = get_logger(__name__)


class JobStatus(Enum):
    """Job status enumeration for both SLURM jobs and workflow tasks."""

    UNKNOWN = "UNKNOWN"
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"
    TIMEOUT = "TIMEOUT"


class JobResource(BaseModel):
    """SLURM resource allocation requirements."""

    nodes: int = Field(default=1, ge=1, description="Number of compute nodes")
    gpus_per_node: int = Field(default=0, ge=0, description="Number of GPUs per node")
    ntasks_per_node: int = Field(
        default=1, ge=1, description="Number of tasks per node"
    )
    cpus_per_task: int = Field(default=1, ge=1, description="Number of CPUs per task")
    memory_per_node: str | None = Field(
        default=None, description="Memory per node (e.g., '32GB')"
    )
    time_limit: str | None = Field(
        default=None, description="Time limit (e.g., '1:00:00')"
    )


class JobEnvironment(BaseModel):
    """Job environment configuration."""

    conda: str | None = Field(default=None, description="Conda environment name")
    venv: str | None = Field(default=None, description="Virtual environment path")
    sqsh: str | None = Field(default=None, description="SquashFS image path")
    env_vars: dict[str, str] = Field(
        default_factory=dict, description="Environment variables"
    )

    @model_validator(mode="after")
    def validate_environment(self) -> Self:
        envs = [self.conda, self.venv, self.sqsh]
        non_none_count = sum(x is not None for x in envs)
        if non_none_count != 1:
            raise ValueError("Exactly one of 'conda', 'venv', or 'sqsh' must be set")
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

    command: list[str] = Field(description="Command to execute")
    resources: JobResource = Field(
        default_factory=JobResource, description="Resource requirements"
    )
    environment: JobEnvironment = Field(
        default_factory=JobEnvironment, description="Environment setup"
    )
    log_dir: str = Field(
        default=os.getenv("SLURM_LOG_DIR", "logs"),
        description="Directory for log files",
    )
    work_dir: str = Field(default_factory=os.getcwd, description="Working directory")


class ShellJob(BaseJob):
    path: str = Field(description="Shell script path to execute")


type JobType = BaseJob | Job | ShellJob
type RunableJobType = Job | ShellJob


class Workflow(BaseModel):
    """Represents a workflow containing multiple jobs with dependencies."""

    name: str = Field(description="Workflow name")
    jobs: list[RunableJobType] = Field(description="List of jobs in the workflow")

    def get(self, name: str) -> RunableJobType | None:
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
                msg += add_indent(
                    2, f"{'Command:': <13} {' '.join(job.command or [])}\n"
                )
                msg += add_indent(
                    2,
                    f"{'Resources:': <13} {job.resources.nodes} nodes, {job.resources.gpus_per_node} GPUs/node\n",
                )
                if job.environment.conda:
                    msg += add_indent(
                        2, f"{'Conda env:': <13} {job.environment.conda}\n"
                    )
                if job.environment.sqsh:
                    msg += add_indent(2, f"{'Sqsh:': <13} {job.environment.sqsh}\n")
                if job.environment.venv:
                    msg += add_indent(2, f"{'Venv:': <13} {job.environment.venv}\n")
            elif isinstance(job, ShellJob):
                msg += add_indent(2, f"{'Path:': <13} {job.path}\n")
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


def render_job_script(
    template_path: Path | str,
    job: Job,
    output_dir: Path | str,
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
    template_file = Path(template_path)
    if not template_file.is_file():
        raise FileNotFoundError(f"Template file '{template_path}' not found")

    with open(template_file, encoding="utf-8") as f:
        template_content = f.read()

    template = jinja2.Template(template_content, undefined=jinja2.StrictUndefined)

    # Prepare template variables
    template_vars = {
        "job_name": job.name,
        "command": " ".join(job.command or []),
        "log_dir": job.log_dir,
        "work_dir": job.work_dir,
        "environment_setup": _build_environment_setup(job.environment),
        **job.resources.model_dump(),
    }

    rendered_content = template.render(template_vars)

    if verbose:
        print(rendered_content)

    # Generate output file
    output_path = Path(output_dir) / f"{job.name}.slurm"
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(rendered_content)

    return str(output_path)


def _build_environment_setup(environment: JobEnvironment) -> str:
    """Build environment setup script."""
    setup_lines = []

    # Set environment variables
    for key, value in environment.env_vars.items():
        setup_lines.append(f"export {key}={value}")

    # Activate environments
    if environment.conda:
        setup_lines.extend(["conda deactivate", f"conda activate {environment.conda}"])
    elif environment.venv:
        setup_lines.append(f"source {environment.venv}/bin/activate")
    elif environment.sqsh:
        setup_lines.extend(
            [
                f': "${{IMAGE:={environment.sqsh}}}"',
                "declare -a CONTAINER_ARGS=(",
                '    --container-image "$IMAGE"',
                ")",
            ]
        )

    return "\n".join(setup_lines)
