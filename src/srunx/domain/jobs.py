"""Data models for SLURM job management."""

import os
import re
import subprocess
import time
from enum import Enum
from typing import Literal, Self

from pydantic import BaseModel, Field, PrivateAttr, field_validator, model_validator

from srunx.common.exceptions import WorkflowValidationError
from srunx.common.logging import get_logger

logger = get_logger(__name__)


def _get_config_defaults():
    """Get configuration defaults, with lazy import to avoid circular dependencies."""
    try:
        from srunx.common.config import get_config

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
    time_limit: str | None = Field(
        default=None,
        description="SLURM --time request (e.g., '1:00:00', 'UNLIMITED')",
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
            "via :func:`srunx.runtime.rendering.normalize_job_for_submission`."
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
