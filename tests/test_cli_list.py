"""Tests for list command CLI."""

import json
from unittest.mock import MagicMock, patch

import pytest
from typer.testing import CliRunner

from srunx.cli.main import app
from srunx.models import Job, JobResource, JobStatus


@pytest.fixture
def runner():
    """Create CLI test runner."""
    return CliRunner()


@pytest.fixture
def mock_jobs():
    """Create mock job list for testing."""
    jobs = []

    # Job 1: With GPUs
    job1 = Job(name="gpu_job", job_id=12345, command=["python", "train.py"])
    job1._status = JobStatus.RUNNING
    job1.resources = JobResource(nodes=2, gpus_per_node=4, time_limit="4:00:00")
    jobs.append(job1)

    # Job 2: No GPUs
    job2 = Job(name="cpu_job", job_id=12346, command=["python", "process.py"])
    job2._status = JobStatus.PENDING
    job2.resources = JobResource(nodes=1, gpus_per_node=0, time_limit="1:00:00")
    jobs.append(job2)

    # Job 3: Multiple GPUs per node
    job3 = Job(name="multi_gpu", job_id=12347, command=["python", "parallel.py"])
    job3._status = JobStatus.COMPLETED
    job3.resources = JobResource(nodes=4, gpus_per_node=2, time_limit="2:00:00")
    jobs.append(job3)

    return jobs


class TestListCommand:
    """Test suite for list command."""

    def test_list_table_format_default(self, runner, mock_jobs):
        """Test list command with default table format."""
        with patch("srunx.cli.main.Slurm") as mock_slurm:
            mock_client = MagicMock()
            mock_client.queue.return_value = mock_jobs
            mock_slurm.return_value = mock_client

            result = runner.invoke(app, ["list"])

            assert result.exit_code == 0
            assert "Job Queue" in result.stdout
            assert "12345" in result.stdout
            assert "gpu_job" in result.stdout
            assert "RUNNING" in result.stdout
            assert "12346" in result.stdout
            assert "cpu_job" in result.stdout
            assert "PENDING" in result.stdout
            # Should NOT show GPUs column by default
            assert "GPUs" not in result.stdout

    def test_list_table_format_with_gpus(self, runner, mock_jobs):
        """Test list command with --show-gpus flag."""
        with patch("srunx.cli.main.Slurm") as mock_slurm:
            mock_client = MagicMock()
            mock_client.queue.return_value = mock_jobs
            mock_slurm.return_value = mock_client

            result = runner.invoke(app, ["list", "--show-gpus"])

            assert result.exit_code == 0
            assert "Job Queue" in result.stdout
            assert "GPUs" in result.stdout
            # GPU counts: job1 = 2*4=8, job2 = 1*0=0, job3 = 4*2=8
            assert "8" in result.stdout  # job1 and job3 GPUs
            assert "0" in result.stdout  # job2 GPUs

    def test_list_json_format_without_gpus(self, runner, mock_jobs):
        """Test list command with JSON format."""
        with patch("srunx.cli.main.Slurm") as mock_slurm:
            mock_client = MagicMock()
            mock_client.queue.return_value = mock_jobs
            mock_slurm.return_value = mock_client

            result = runner.invoke(app, ["list", "--format", "json"])

            assert result.exit_code == 0
            data = json.loads(result.stdout)

            assert len(data) == 3
            assert data[0]["job_id"] == 12345
            assert data[0]["name"] == "gpu_job"
            assert data[0]["status"] == "RUNNING"
            assert data[0]["nodes"] == 2
            assert data[0]["time_limit"] == "4:00:00"
            # Should NOT have gpus field without --show-gpus
            assert "gpus" not in data[0]

    def test_list_json_format_with_gpus(self, runner, mock_jobs):
        """Test list command with JSON format and --show-gpus."""
        with patch("srunx.cli.main.Slurm") as mock_slurm:
            mock_client = MagicMock()
            mock_client.queue.return_value = mock_jobs
            mock_slurm.return_value = mock_client

            result = runner.invoke(app, ["list", "--show-gpus", "--format", "json"])

            assert result.exit_code == 0
            data = json.loads(result.stdout)

            assert len(data) == 3
            # Job 1: 2 nodes * 4 GPUs/node = 8
            assert data[0]["gpus"] == 8
            # Job 2: 1 node * 0 GPUs/node = 0
            assert data[1]["gpus"] == 0
            # Job 3: 4 nodes * 2 GPUs/node = 8
            assert data[2]["gpus"] == 8

    def test_list_empty_queue(self, runner):
        """Test list command with empty job queue."""
        with patch("srunx.cli.main.Slurm") as mock_slurm:
            mock_client = MagicMock()
            mock_client.queue.return_value = []
            mock_slurm.return_value = mock_client

            result = runner.invoke(app, ["list"])

            assert result.exit_code == 0
            assert "No jobs in queue" in result.stdout

    def test_list_gpu_calculation(self, runner):
        """Test GPU calculation: nodes * gpus_per_node."""
        jobs = []
        job = Job(name="test", job_id=99999, command=["test"])
        job._status = JobStatus.RUNNING
        # 5 nodes * 3 GPUs/node = 15 total GPUs
        job.resources = JobResource(nodes=5, gpus_per_node=3)
        jobs.append(job)

        with patch("srunx.cli.main.Slurm") as mock_slurm:
            mock_client = MagicMock()
            mock_client.queue.return_value = jobs
            mock_slurm.return_value = mock_client

            result = runner.invoke(app, ["list", "--show-gpus", "--format", "json"])

            assert result.exit_code == 0
            data = json.loads(result.stdout)
            assert data[0]["gpus"] == 15

    def test_list_job_without_resources(self, runner):
        """Test list command with job that has no resources."""
        job = Job(name="no_resources", job_id=88888, command=["test"])
        job._status = JobStatus.RUNNING
        # No resources set

        with patch("srunx.cli.main.Slurm") as mock_slurm:
            mock_client = MagicMock()
            mock_client.queue.return_value = [job]
            mock_slurm.return_value = mock_client

            result = runner.invoke(app, ["list", "--show-gpus", "--format", "json"])

            assert result.exit_code == 0
            data = json.loads(result.stdout)
            # Should handle missing resources gracefully
            assert data[0]["gpus"] == 0

    def test_list_error_handling(self, runner):
        """Test list command error handling."""
        with patch("srunx.cli.main.Slurm") as mock_slurm:
            mock_client = MagicMock()
            mock_client.queue.side_effect = Exception("SLURM connection error")
            mock_slurm.return_value = mock_client

            result = runner.invoke(app, ["list"])

            assert result.exit_code == 1
