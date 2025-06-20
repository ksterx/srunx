"""Data models for SLURM job management."""

import os
import subprocess
import time
from enum import Enum
from pathlib import Path
from typing import Self

import jinja2
from pydantic import BaseModel, Field, model_validator

from srunx.logging import get_logger

logger = get_logger(__name__)


class JobStatus(Enum):
    """SLURM job status enumeration."""

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
    status: JobStatus | None = Field(default=None, description="Current job status")
    dependencies: list[str] = Field(
        default_factory=list, description="Job dependencies"
    )

    def refresh(self, retries: int = 3) -> None:
        for retry in range(retries):
            try:
                result = subprocess.run(
                    [
                        "sacct",
                        "-j",
                        str(self.job_id),
                        "--format",
                        "JobID,JobName,State",
                        "--noheader",
                        "--parsable2",
                    ],
                    capture_output=True,
                    text=True,
                    check=True,
                )
            except subprocess.CalledProcessError as e:
                logger.error(f"Failed to query job {self.job_id} status: {e}")
                raise

            lines = result.stdout.strip().split("\n")
            if not lines or not lines[0]:
                if retry < retries - 1:
                    logger.info(
                        f"Waiting for job {self.job_id}. Retrying {retry + 1} of {retries}..."
                    )
                    time.sleep(1)
                    continue
                raise ValueError(f"No job information found for job {self.job_id}")
            else:
                break

        # Parse the first line (main job entry)
        job_data = lines[0].split("|")
        if len(job_data) < 3:
            error_msg = f"Cannot parse job data for job {self.job_id}"
            logger.error(error_msg)
            raise ValueError(error_msg)

        status = job_data[2]
        self.status = JobStatus(status)


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


class WorkflowTask(BaseModel):
    """Represents a single task in a workflow."""

    name: str = Field(description="Task name")
    job: BaseJob = Field(description="Job configuration")
    depends_on: list[str] = Field(default_factory=list, description="Task dependencies")

    def __repr__(self) -> str:
        return f"WorkflowTask(name={self.name}, job={self.job}, depends_on={self.depends_on})"


class Workflow(BaseModel):
    """Represents a workflow containing multiple tasks with dependencies."""

    name: str = Field(description="Workflow name")
    tasks: list[WorkflowTask] = Field(description="List of tasks in the workflow")

    def get_task(self, name: str) -> WorkflowTask | None:
        """Get a task by name."""
        for task in self.tasks:
            if task.name == name:
                return task
        return None

    def get_task_dependencies(self, task_name: str) -> list[str]:
        """Get dependencies for a specific task."""
        task = self.get_task(task_name)
        return task.depends_on if task else []

    def show(self):
        msg = f"""\
{" PLAN ":=^80}
Workflow: {self.name}
Tasks: {len(self.tasks)}
Execution: Sequential with dependency-based scheduling
"""

        for task in self.tasks:
            msg += f"    Task: {task.name}\n"
            if isinstance(task.job, Job):
                msg += f"{'        Command:': <21} {' '.join(task.job.command or [])}\n"
                msg += f"{'        Resources:': <21} {task.job.resources.nodes} nodes, {task.job.resources.gpus_per_node} GPUs/node\n"
                if task.job.environment.conda:
                    msg += f"{'        Conda env:': <21} {task.job.environment.conda}\n"
                if task.job.environment.sqsh:
                    msg += f"{'        Sqsh:': <21} {task.job.environment.sqsh}\n"
                if task.job.environment.venv:
                    msg += f"{'        Venv:': <21} {task.job.environment.venv}\n"
            elif isinstance(task.job, ShellJob):
                msg += f"{'        Path:': <21} {task.job.path}\n"
            if task.depends_on:
                msg += f"{'        Dependencies:': <21} {', '.join(task.depends_on)}\n"

        msg += f"{'=' * 80}\n"
        print(msg)

    def validate(self):
        """Validate workflow task dependencies."""
        task_names = {task.name for task in self.tasks}

        for task in self.tasks:
            for dependency in task.depends_on:
                if dependency not in task_names:
                    raise ValueError(
                        f"Task '{task.name}' depends on unknown task '{dependency}'"
                    )

        # Check for circular dependencies (simple check)
        visited = set()
        rec_stack = set()

        def has_cycle(task_name: str) -> bool:
            if task_name in rec_stack:
                return True
            if task_name in visited:
                return False

            visited.add(task_name)
            rec_stack.add(task_name)

            task = self.get_task(task_name)
            if task:
                for dependency in task.depends_on:
                    if has_cycle(dependency):
                        return True

            rec_stack.remove(task_name)
            return False

        for task in self.tasks:
            if has_cycle(task.name):
                raise ValueError(
                    f"Circular dependency detected involving task '{task.name}'"
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
