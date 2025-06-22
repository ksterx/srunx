"""Pytest configuration and fixtures."""

import tempfile
from pathlib import Path

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
