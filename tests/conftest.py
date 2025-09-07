"""Pytest configuration and fixtures."""

import os
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest

from srunx.models import Job, JobEnvironment, JobResource


@pytest.fixture
def temp_dir():
    """Create a temporary directory for tests."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        yield Path(tmp_dir)


@pytest.fixture
def sample_job():
    """Create a sample job for testing."""
    return Job(
        name="test_job",
        command=["python", "test.py"],
        resources=JobResource(
            nodes=1,
            gpus_per_node=0,
            ntasks_per_node=1,
            cpus_per_task=1,
        ),
        environment=JobEnvironment(conda="test_env"),
        log_dir="logs",
        work_dir="/tmp",
    )


@pytest.fixture
def sample_job_resource():
    """Create a sample job resource for testing."""
    return JobResource(
        nodes=2,
        gpus_per_node=1,
        ntasks_per_node=4,
        cpus_per_task=2,
        memory_per_node="32GB",
        time_limit="2:00:00",
    )


@pytest.fixture
def sample_job_environment():
    """Create a sample job environment for testing."""
    return JobEnvironment(
        conda="ml_env",
        env_vars={"CUDA_VISIBLE_DEVICES": "0,1", "OMP_NUM_THREADS": "4"},
    )


@pytest.fixture
def mock_subprocess_run(monkeypatch):
    """Mock subprocess.run for testing."""
    import subprocess
    from unittest.mock import Mock

    mock_result = Mock()
    mock_result.stdout = "12345"
    mock_result.returncode = 0

    def mock_run(*args, **kwargs):
        return mock_result

    monkeypatch.setattr(subprocess, "run", mock_run)
    return mock_result


@pytest.fixture(autouse=True)
def reset_config():
    """Reset global configuration and ensure clean test environment."""
    # Clear any cached config
    import srunx.config

    srunx.config._config = None

    # Also clear any module-level defaults cache
    import srunx.models

    if hasattr(srunx.models, "_cached_config"):
        srunx.models._cached_config = None

    # Clear environment variables that might affect config
    env_vars = [
        "SLURM_LOG_DIR",
        "SRUNX_DEFAULT_NODES",
        "SRUNX_DEFAULT_GPUS_PER_NODE",
        "SRUNX_DEFAULT_MEMORY_PER_NODE",
        "SRUNX_DEFAULT_PARTITION",
        "SRUNX_DEFAULT_CONDA",
        "SRUNX_DEFAULT_VENV",
        "SRUNX_DEFAULT_CONTAINER",
        "SRUNX_DEFAULT_NTASKS_PER_NODE",
        "SRUNX_DEFAULT_CPUS_PER_TASK",
        "SRUNX_DEFAULT_TIME_LIMIT",
        "SRUNX_DEFAULT_NODELIST",
        "SRUNX_DEFAULT_LOG_DIR",
        "SRUNX_DEFAULT_WORK_DIR",
    ]

    original_values = {}
    for var in env_vars:
        original_values[var] = os.environ.get(var)
        if var in os.environ:
            del os.environ[var]

    # Mock subprocess to prevent actual system calls during tests
    with patch("subprocess.run") as mock_run:

        def mock_subprocess(*args, **kwargs):
            # Mock sacct to return PENDING status
            if args and len(args[0]) > 0 and "sacct" in str(args[0]):
                from unittest.mock import Mock

                result = Mock()
                result.stdout = "12345|PENDING\n"
                result.returncode = 0
                return result
            else:
                # For other subprocess calls, simulate command not found
                from subprocess import CalledProcessError

                raise CalledProcessError(1, args[0] if args else "unknown")

        mock_run.side_effect = mock_subprocess

        # Prevent status changes during tests by mocking refresh method
        # Only mock for dependency-sensitive tests, not for refresh tests
        import inspect

        frame = inspect.currentframe()
        test_name = ""
        try:
            # Try to find the test function name
            while frame:
                if "test_" in frame.f_code.co_name:
                    test_name = frame.f_code.co_name
                    break
                frame = frame.f_back
        except Exception:
            pass

        # Don't mock refresh for refresh-specific tests
        if "refresh" not in test_name.lower():
            with patch.object(srunx.models.BaseJob, "refresh") as mock_refresh:
                mock_refresh.return_value = None
                yield
        else:
            yield

    # Restore original environment values
    for var, value in original_values.items():
        if value is not None:
            os.environ[var] = value
        elif var in os.environ:
            del os.environ[var]

    # Clear config cache again
    srunx.config._config = None
    if hasattr(srunx.models, "_cached_config"):
        srunx.models._cached_config = None


@pytest.fixture
def ensure_pending_status():
    """Ensure all jobs created in this test have PENDING status."""
    from srunx.models import JobStatus

    # Store original __post_init__ method
    original_post_init = None

    def patched_post_init(self):
        if original_post_init:
            original_post_init(self)
        # Force status to be PENDING for test consistency
        self._status = JobStatus.PENDING

    # Apply patches to all job classes
    import srunx.models

    classes_to_patch = [srunx.models.BaseJob, srunx.models.Job, srunx.models.ShellJob]

    original_methods = {}
    for cls in classes_to_patch:
        if hasattr(cls, "__post_init__"):
            original_methods[cls] = cls.__post_init__
            cls.__post_init__ = patched_post_init

    yield

    # Restore original methods
    for cls, original_method in original_methods.items():
        cls.__post_init__ = original_method
