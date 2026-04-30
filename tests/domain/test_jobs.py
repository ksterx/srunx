"""Tests for srunx.domain.jobs (Job / ShellJob / JobResource / JobEnvironment / ContainerResource / BaseJob / JobStatus + status throttle)."""

from unittest.mock import patch

import pytest
from pydantic import ValidationError

from srunx.domain import (
    BaseJob,
    ContainerResource,
    Job,
    JobEnvironment,
    JobResource,
    JobStatus,
    ShellJob,
)


class TestJobStatus:
    """Test JobStatus enum."""

    def test_job_status_values(self):
        """Test JobStatus enum values."""
        assert JobStatus.PENDING.value == "PENDING"
        assert JobStatus.RUNNING.value == "RUNNING"
        assert JobStatus.COMPLETED.value == "COMPLETED"
        assert JobStatus.FAILED.value == "FAILED"
        assert JobStatus.CANCELLED.value == "CANCELLED"
        assert JobStatus.TIMEOUT.value == "TIMEOUT"
        assert JobStatus.UNKNOWN.value == "UNKNOWN"


class TestJobResource:
    """Test JobResource model."""

    def test_job_resource_defaults(self):
        """Test JobResource default values."""
        resource = JobResource()
        assert resource.nodes == 1
        assert resource.gpus_per_node == 0
        assert resource.ntasks_per_node == 1
        assert resource.cpus_per_task == 1
        assert resource.memory_per_node is None
        assert resource.time_limit is None
        assert resource.nodelist is None
        assert resource.partition is None

    def test_job_resource_custom_values(self, sample_job_resource):
        """Test JobResource with custom values."""
        assert sample_job_resource.nodes == 2
        assert sample_job_resource.gpus_per_node == 1
        assert sample_job_resource.ntasks_per_node == 4
        assert sample_job_resource.cpus_per_task == 2
        assert sample_job_resource.memory_per_node == "32GB"
        assert sample_job_resource.time_limit == "2:00:00"

    def test_job_resource_validation(self):
        """Test JobResource validation."""
        # Test negative values
        with pytest.raises(ValidationError):
            JobResource(nodes=-1)

        with pytest.raises(ValidationError):
            JobResource(gpus_per_node=-1)

        with pytest.raises(ValidationError):
            JobResource(ntasks_per_node=0)

        with pytest.raises(ValidationError):
            JobResource(cpus_per_task=0)

    def test_job_resource_nodelist_and_partition(self):
        """Test JobResource with nodelist and partition."""
        resource = JobResource(nodelist="node001,node002", partition="gpu")
        assert resource.nodelist == "node001,node002"
        assert resource.partition == "gpu"


class TestJobEnvironment:
    """Test JobEnvironment model."""

    def test_job_environment_defaults(self):
        """Test JobEnvironment default values."""
        # Should succeed because no environment is now allowed
        env = JobEnvironment()
        assert env.conda is None
        assert env.venv is None
        assert env.container is None

    def test_job_environment_conda(self):
        """Test JobEnvironment with conda."""
        env = JobEnvironment(conda="test_env")
        assert env.conda == "test_env"
        assert env.venv is None
        assert env.container is None

    def test_job_environment_venv(self):
        """Test JobEnvironment with venv."""
        env = JobEnvironment(venv="/path/to/venv")
        assert env.venv == "/path/to/venv"
        assert env.conda is None
        assert env.container is None

    def test_job_environment_container(self):
        """Test JobEnvironment with container."""
        container = ContainerResource(image="/path/to/image.sqsh")
        env = JobEnvironment(container=container)
        assert env.container.image == "/path/to/image.sqsh"
        assert env.conda is None
        assert env.venv is None

    def test_job_environment_env_vars(self, sample_job_environment):
        """Test JobEnvironment with environment variables."""
        assert sample_job_environment.env_vars["CUDA_VISIBLE_DEVICES"] == "0,1"
        assert sample_job_environment.env_vars["OMP_NUM_THREADS"] == "4"

    def test_job_environment_validation_multiple_envs(self):
        """Test JobEnvironment validation with multiple environments."""
        with pytest.raises(ValidationError):
            JobEnvironment(conda="env1", venv="/path/to/venv")

    def test_job_environment_validation_no_env(self):
        """Test JobEnvironment validation without any environment."""
        # Should succeed because no virtual environment is now allowed
        env = JobEnvironment(env_vars={"TEST": "value"})
        assert env.conda is None
        assert env.venv is None
        assert env.container is None
        assert env.env_vars["TEST"] == "value"


class TestContainerResource:
    """Test ContainerResource model updates (T6.2)."""

    def test_default_runtime_is_pyxis(self):
        """Test that the default runtime is 'pyxis'."""
        container = ContainerResource.model_validate({"image": "test:latest"})
        assert container.runtime == "pyxis"

    def test_pyxis_with_nv_raises_validation_error(self):
        """Test that pyxis + nv=true raises ValidationError (AC-13)."""
        with pytest.raises(ValidationError, match="only valid for apptainer"):
            ContainerResource.model_validate(
                {"runtime": "pyxis", "image": "test:latest", "nv": True}
            )

    def test_pyxis_with_rocm_raises_validation_error(self):
        """Test that pyxis + rocm=true raises ValidationError."""
        with pytest.raises(ValidationError, match="only valid for apptainer"):
            ContainerResource.model_validate(
                {"runtime": "pyxis", "image": "test:latest", "rocm": True}
            )

    def test_pyxis_with_cleanenv_raises_validation_error(self):
        """Test that pyxis + cleanenv=true raises ValidationError."""
        with pytest.raises(ValidationError, match="only valid for apptainer"):
            ContainerResource.model_validate(
                {"runtime": "pyxis", "image": "test:latest", "cleanenv": True}
            )

    def test_pyxis_with_fakeroot_raises_validation_error(self):
        """Test that pyxis + fakeroot=true raises ValidationError."""
        with pytest.raises(ValidationError, match="only valid for apptainer"):
            ContainerResource.model_validate(
                {"runtime": "pyxis", "image": "test:latest", "fakeroot": True}
            )

    def test_pyxis_with_writable_tmpfs_raises_validation_error(self):
        """Test that pyxis + writable_tmpfs=true raises ValidationError."""
        with pytest.raises(ValidationError, match="only valid for apptainer"):
            ContainerResource.model_validate(
                {"runtime": "pyxis", "image": "test:latest", "writable_tmpfs": True}
            )

    def test_pyxis_with_overlay_raises_validation_error(self):
        """Test that pyxis + overlay raises ValidationError."""
        with pytest.raises(ValidationError, match="only valid for apptainer"):
            ContainerResource.model_validate(
                {
                    "runtime": "pyxis",
                    "image": "test:latest",
                    "overlay": "/path/to/overlay.img",
                }
            )

    def test_pyxis_with_env_raises_validation_error(self):
        """Test that pyxis + env raises ValidationError."""
        with pytest.raises(ValidationError, match="only valid for apptainer"):
            ContainerResource.model_validate(
                {"runtime": "pyxis", "image": "test:latest", "env": {"K": "V"}}
            )

    def test_apptainer_runtime_accepts_all_fields(self):
        """Test that apptainer runtime accepts all Apptainer-specific fields."""
        container = ContainerResource.model_validate(
            {
                "runtime": "apptainer",
                "image": "test.sif",
                "nv": True,
                "rocm": True,
                "cleanenv": True,
                "fakeroot": True,
                "writable_tmpfs": True,
                "overlay": "/overlay.img",
                "env": {"CUDA_VISIBLE_DEVICES": "0"},
                "mounts": ["/data:/data"],
                "workdir": "/workspace",
            }
        )
        assert container.runtime == "apptainer"
        assert container.nv is True
        assert container.rocm is True
        assert container.cleanenv is True
        assert container.fakeroot is True
        assert container.writable_tmpfs is True
        assert container.overlay == "/overlay.img"
        assert container.env == {"CUDA_VISIBLE_DEVICES": "0"}

    def test_singularity_runtime_accepts_apptainer_fields(self):
        """Test that singularity runtime accepts Apptainer-specific fields."""
        container = ContainerResource.model_validate(
            {
                "runtime": "singularity",
                "image": "test.sif",
                "nv": True,
                "cleanenv": True,
            }
        )
        assert container.runtime == "singularity"
        assert container.nv is True

    def test_container_with_conda_coexistence(self):
        """Test that container + conda coexistence works (AC-14 partial)."""
        env = JobEnvironment.model_validate(
            {
                "conda": "ml_env",
                "container": {
                    "runtime": "apptainer",
                    "image": "test.sif",
                    "nv": True,
                },
            }
        )
        assert env.conda == "ml_env"
        assert env.container is not None
        assert env.container.runtime == "apptainer"
        assert env.container.image == "test.sif"

    def test_container_with_venv_coexistence(self):
        """Test that container + venv coexistence works."""
        env = JobEnvironment.model_validate(
            {
                "venv": "/path/to/venv",
                "container": {"runtime": "pyxis", "image": "test:latest"},
            }
        )
        assert env.venv == "/path/to/venv"
        assert env.container is not None
        assert env.container.runtime == "pyxis"

    def test_pyxis_with_defaults_only(self):
        """Test PyxisRuntime with only default values does not raise."""
        container = ContainerResource.model_validate(
            {"runtime": "pyxis", "image": "test:latest"}
        )
        assert container.nv is False
        assert container.rocm is False
        assert container.env == {}
        assert container.overlay is None


class TestBaseJob:
    """Test BaseJob model."""

    def test_base_job_defaults(self):
        """Test BaseJob default values."""
        job = BaseJob()
        assert job.name == "job"
        assert job.job_id is None
        assert job.depends_on == []
        assert job.retry == 0
        assert job.retry_delay == 60
        assert job.status == JobStatus.PENDING
        assert job.retry_count == 0

    def test_base_job_custom_values(self):
        """Test BaseJob with custom values."""
        job = BaseJob(
            name="test_job",
            job_id=12345,
            depends_on=["job1", "job2"],
            retry=3,
            retry_delay=120,
        )
        assert job.name == "test_job"
        assert job.job_id == 12345
        assert job.depends_on == ["job1", "job2"]
        assert job.retry == 3
        assert job.retry_delay == 120

    def test_base_job_status_property(self):
        """Test BaseJob status property."""
        job = BaseJob()
        assert job.status == JobStatus.PENDING

        job.status = JobStatus.RUNNING
        assert job.status == JobStatus.RUNNING

    @patch("subprocess.run")
    @patch.object(BaseJob, "refresh", wraps=BaseJob.refresh)
    def test_base_job_refresh(self, mock_refresh, mock_run):
        """Test BaseJob refresh method."""
        mock_run.return_value.stdout = "12345|RUNNING\n"

        job = BaseJob(job_id=12345)
        # Call the actual refresh method, bypassing the global mock
        BaseJob.refresh(job)

        mock_run.assert_called_once()
        assert job._status.value == "RUNNING"

    @patch("subprocess.run")
    @patch.object(BaseJob, "refresh", wraps=BaseJob.refresh)
    def test_base_job_refresh_no_job_id(self, mock_refresh, mock_run):
        """Test BaseJob refresh with no job_id."""
        job = BaseJob()
        # Call the actual refresh method, bypassing the global mock
        result = BaseJob.refresh(job)

        mock_run.assert_not_called()
        assert result is job

    def test_dependencies_satisfied(self):
        """Test dependencies_satisfied method."""
        job = BaseJob(depends_on=["job1", "job2"])
        # Ensure job starts in PENDING status for dependencies check
        job._status = JobStatus.PENDING

        # Not satisfied - missing dependencies
        assert not job.dependencies_satisfied(["job1"])

        # Satisfied - all dependencies present
        assert job.dependencies_satisfied(["job1", "job2", "job3"])

        # Job with no dependencies should be satisfied
        job_no_deps = BaseJob()
        job_no_deps._status = JobStatus.PENDING
        assert job_no_deps.dependencies_satisfied([])

    def test_retry_methods(self):
        """Test retry-related methods."""
        job = BaseJob(retry=2)

        # Initial state
        assert job.retry_count == 0
        assert job.can_retry() is True
        assert job.should_retry() is False  # Not failed yet

        # Simulate failure - use _status directly to avoid property getter
        job._status = JobStatus.FAILED
        assert job.should_retry() is True

        # First retry
        job.increment_retry()
        assert job.retry_count == 1
        assert job.can_retry() is True
        assert job.should_retry() is True

        # Second retry
        job.increment_retry()
        assert job.retry_count == 2
        assert job.can_retry() is False
        assert job.should_retry() is False

        # Reset retry count
        job.reset_retry()
        assert job.retry_count == 0
        assert job.can_retry() is True

    def test_retry_validation(self):
        """Test retry validation."""
        # Test negative retry value
        with pytest.raises(ValidationError):
            BaseJob(retry=-1)

        # Test negative retry_delay value
        with pytest.raises(ValidationError):
            BaseJob(retry_delay=-1)

        # Test valid values
        job = BaseJob(retry=0, retry_delay=0)
        assert job.retry == 0
        assert job.retry_delay == 0


class TestJob:
    """Test Job model."""

    def test_job_creation(self, sample_job):
        """Test Job creation."""
        assert sample_job.name == "test_job"
        assert sample_job.command == ["python", "test.py"]
        assert sample_job.resources.nodes == 1
        assert sample_job.environment.conda == "test_env"
        assert sample_job.log_dir == "logs"
        assert sample_job.work_dir == "/tmp"

    def test_job_defaults(self):
        """Test Job default values.

        Env-var isolation comes from the ``reset_config`` autouse fixture
        (clears ``SRUNX_DEFAULT_*`` / ``SLURM_LOG_DIR``) and the
        ``_isolate_xdg_config_home`` autouse fixture (redirects
        ``XDG_CONFIG_HOME`` to a tmp dir so the developer's
        ``~/.config/srunx/config.json`` cannot leak in). No need to
        ``patch.dict(os.environ, {}, clear=True)`` — that was over-clearing
        and clobbered the XDG isolation.
        """
        import srunx.common.config

        srunx.common.config._config = None

        job = Job(
            command=["python", "script.py"],
            environment=JobEnvironment(conda="test_env"),
        )
        assert job.name == "job"
        assert job.resources.nodes == 1
        # Without SLURM_LOG_DIR set, should default to 'logs'
        assert job.log_dir == "logs"
        # Phase 2 render-parity fix: default changed from ``os.getcwd()`` to
        # empty string so ``SubmissionRenderContext.default_work_dir`` (SSH
        # submissions) can inject ``mount.remote`` without fighting a
        # process-CWD fallback. Empty string also renders verbatim
        # (the template's ``{% if work_dir %}`` guard omits ``--chdir``,
        # and SLURM inherits the sbatch submission directory — same
        # effective behavior as the old ``os.getcwd()`` default for CLI
        # use where ``srunx`` is invoked from the user's project dir).
        assert job.work_dir == ""

    def test_job_validation(self):
        """Test Job validation."""
        with pytest.raises(ValidationError):
            # Missing command
            Job(environment=JobEnvironment(conda="test_env"))


class TestShellJob:
    """Test ShellJob model."""

    def test_shell_job_creation(self):
        """Test ShellJob creation."""
        job = ShellJob(script_path="/path/to/script.sh")
        assert job.script_path == "/path/to/script.sh"
        assert job.name == "job"

    def test_shell_job_validation(self):
        """Test ShellJob validation."""
        with pytest.raises(ValidationError):
            # Missing path
            ShellJob()


class TestStatusThrottle:
    """Test BaseJob.status throttle behavior (M1)."""

    def test_throttle_skips_refresh_within_interval(self):
        """Status access within _REFRESH_INTERVAL should not call refresh."""
        import time

        job = BaseJob(name="test", job_id=123)
        job._status = JobStatus.RUNNING

        # Simulate a recent refresh
        job._last_refresh = time.time()

        with patch.object(BaseJob, "refresh", wraps=lambda self: self) as mock_refresh:
            _ = job.status
            mock_refresh.assert_not_called()

    def test_throttle_allows_refresh_after_interval(self):
        """Status access after _REFRESH_INTERVAL should call refresh."""
        job = BaseJob(name="test", job_id=123)
        job._status = JobStatus.RUNNING

        # Simulate stale refresh timestamp
        job._last_refresh = 0.0

        with patch.object(BaseJob, "refresh", return_value=job) as mock_refresh:
            _ = job.status
            mock_refresh.assert_called_once()

    def test_terminal_status_never_refreshes(self):
        """Terminal statuses should never trigger refresh regardless of time."""
        for terminal in [
            JobStatus.COMPLETED,
            JobStatus.FAILED,
            JobStatus.CANCELLED,
            JobStatus.TIMEOUT,
        ]:
            job = BaseJob(name="test", job_id=123)
            job._status = terminal
            job._last_refresh = 0.0  # stale, but should not matter

            with patch.object(BaseJob, "refresh") as mock_refresh:
                assert job.status == terminal
                mock_refresh.assert_not_called()

    def test_no_job_id_never_refreshes(self):
        """Jobs without a job_id should never trigger refresh."""
        job = BaseJob(name="test")
        job._status = JobStatus.RUNNING
        job._last_refresh = 0.0

        with patch.object(BaseJob, "refresh") as mock_refresh:
            assert job.status == JobStatus.RUNNING
            mock_refresh.assert_not_called()
