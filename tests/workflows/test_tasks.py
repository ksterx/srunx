"""Tests for srunx.workflows.tasks module."""

from unittest.mock import Mock, patch

import pytest

from srunx.models import Job, JobEnvironment, JobStatus, ShellJob
from srunx.workflows.tasks import (
    submit_and_monitor_job,
    submit_job_async,
    wait_for_job,
)


class TestSubmitAndMonitorJob:
    """Test submit_and_monitor_job task."""

    @patch("srunx.workflows.tasks.Slurm")
    def test_submit_and_monitor_job_success(self, mock_client_class: Mock) -> None:
        """Test successful job submission and monitoring."""
        # Setup
        job = Job(
            name="test_job",
            command=["python", "script.py"],
            environment=JobEnvironment(conda="test_env"),
        )

        submitted_job = Job(
            name="test_job",
            command=["python", "script.py"],
            environment=JobEnvironment(conda="test_env"),
            job_id=12345,
            status=JobStatus.PENDING,
        )

        completed_job = Job(
            name="test_job",
            command=["python", "script.py"],
            environment=JobEnvironment(conda="test_env"),
            job_id=12345,
            status=JobStatus.COMPLETED,
        )

        mock_client = Mock()
        mock_client.run.return_value = submitted_job
        mock_client.monitor.return_value = completed_job
        mock_client_class.return_value = mock_client

        # Execute
        result = submit_and_monitor_job(job, poll_interval=1)

        # Verify
        assert isinstance(result, Job)
        assert result.job_id == 12345
        assert result.status == JobStatus.COMPLETED
        assert result.name == "test_job"

        mock_client.run.assert_called_once_with(job)
        mock_client.monitor.assert_called_once_with(submitted_job, poll_interval=1)

    @patch("srunx.workflows.tasks.Slurm")
    def test_submit_and_monitor_job_failed(self, mock_client_class: Mock) -> None:
        """Test job submission and monitoring with failed job."""
        # Setup
        job = Job(
            name="failing_job",
            command=["false"],
            environment=JobEnvironment(conda="test_env"),
        )

        submitted_job = Job(
            name="failing_job",
            command=["false"],
            environment=JobEnvironment(conda="test_env"),
            job_id=54321,
            status=JobStatus.PENDING,
        )

        mock_client = Mock()
        mock_client.run.return_value = submitted_job
        mock_client.monitor.side_effect = RuntimeError("SLURM job 54321 failed")
        mock_client_class.return_value = mock_client

        # Execute and verify
        with pytest.raises(RuntimeError, match="SLURM job 54321 failed"):
            submit_and_monitor_job(job)

    @patch("srunx.workflows.tasks.Slurm")
    def test_submit_and_monitor_job_submission_error(
        self, mock_client_class: Mock
    ) -> None:
        """Test job submission error in submit_and_monitor_job."""
        # Setup
        job = Job(
            name="error_job",
            command=["python", "nonexistent.py"],
            environment=JobEnvironment(conda="test_env"),
        )

        mock_client = Mock()
        mock_client.run.side_effect = Exception("Submission failed")
        mock_client_class.return_value = mock_client

        # Execute and verify
        with pytest.raises(Exception, match="Submission failed"):
            submit_and_monitor_job(job)

    @patch("srunx.workflows.tasks.Slurm")
    def test_submit_and_monitor_job_default_poll_interval(
        self, mock_client_class: Mock
    ) -> None:
        """Test submit_and_monitor_job with default poll interval."""
        # Setup
        job = Job(
            name="test_job",
            command=["echo", "hello"],
            environment=JobEnvironment(conda="test_env"),
        )

        submitted_job = Job(
            name="test_job",
            command=["echo", "hello"],
            environment=JobEnvironment(conda="test_env"),
            job_id=12345,
            status=JobStatus.PENDING,
        )

        completed_job = Job(
            name="test_job",
            command=["echo", "hello"],
            environment=JobEnvironment(conda="test_env"),
            job_id=12345,
            status=JobStatus.COMPLETED,
        )

        mock_client = Mock()
        mock_client.run.return_value = submitted_job
        mock_client.monitor.return_value = completed_job
        mock_client_class.return_value = mock_client

        # Execute
        result = submit_and_monitor_job(job)

        # Verify default poll interval (30 seconds)
        mock_client.monitor.assert_called_once_with(submitted_job, poll_interval=30)


class TestSubmitJobAsync:
    """Test submit_job_async task."""

    @patch("srunx.workflows.tasks.Slurm")
    def test_submit_job_async_success(self, mock_client_class: Mock) -> None:
        """Test successful async job submission."""
        # Setup
        job = Job(
            name="async_job",
            command=["python", "async_script.py"],
            environment=JobEnvironment(conda="async_env"),
        )

        submitted_job = Job(
            name="async_job",
            command=["python", "async_script.py"],
            environment=JobEnvironment(conda="async_env"),
            job_id=98765,
            status=JobStatus.PENDING,
        )

        mock_client = Mock()
        mock_client.run.return_value = submitted_job
        mock_client_class.return_value = mock_client

        # Execute
        result = submit_job_async(job)

        # Verify
        assert isinstance(result, Job)
        assert result.job_id == 98765
        assert result.status == JobStatus.PENDING
        assert result.name == "async_job"

        mock_client.run.assert_called_once_with(job)
        # Should not call wait_for_completion for async jobs
        assert (
            not hasattr(mock_client, "monitor") or mock_client.monitor.call_count == 0
        )

    @patch("srunx.workflows.tasks.Slurm")
    def test_submit_job_async_error(self, mock_client_class: Mock) -> None:
        """Test async job submission with error."""
        # Setup
        job = Job(
            name="error_async_job",
            command=["invalid_command"],
            environment=JobEnvironment(conda="test_env"),
        )

        mock_client = Mock()
        mock_client.run.side_effect = Exception("Async submission failed")
        mock_client_class.return_value = mock_client

        # Execute and verify
        with pytest.raises(Exception, match="Async submission failed"):
            submit_job_async(job)


class TestWaitForJob:
    """Test wait_for_job task."""

    @patch("srunx.workflows.tasks.Slurm")
    def test_wait_for_job_success(self, mock_client_class: Mock) -> None:
        """Test successful wait for job completion."""
        # Setup
        completed_job = Job(
            name="waited_job",
            command=["echo", "completed"],
            environment=JobEnvironment(conda="test_env"),
            job_id=11111,
            status=JobStatus.COMPLETED,
        )

        mock_client = Mock()
        mock_client.monitor.return_value = completed_job
        mock_client_class.return_value = mock_client

        # Execute
        result = wait_for_job(11111, poll_interval=5)

        # Verify
        assert isinstance(result, Job)
        assert result.job_id == 11111
        assert result.status == JobStatus.COMPLETED
        assert result.name == "waited_job"

        mock_client.monitor.assert_called_once_with(11111, 5)

    @patch("srunx.workflows.tasks.Slurm")
    def test_wait_for_job_failed(self, mock_client_class: Mock) -> None:
        """Test wait for job with failed job."""
        # Setup
        mock_client = Mock()
        mock_client.monitor.side_effect = RuntimeError("SLURM job 22222 failed")
        mock_client_class.return_value = mock_client

        # Execute and verify
        with pytest.raises(RuntimeError, match="SLURM job 22222 failed"):
            wait_for_job(22222)

    @patch("srunx.workflows.tasks.Slurm")
    def test_wait_for_job_default_poll_interval(self, mock_client_class: Mock) -> None:
        """Test wait_for_job with default poll interval."""
        # Setup
        completed_job = Job(
            name="default_poll_job",
            command=["sleep", "1"],
            environment=JobEnvironment(conda="test_env"),
            job_id=33333,
            status=JobStatus.COMPLETED,
        )

        mock_client = Mock()
        mock_client.monitor.return_value = completed_job
        mock_client_class.return_value = mock_client

        # Execute
        result = wait_for_job(33333)

        # Verify default poll interval (30 seconds)
        mock_client.monitor.assert_called_once_with(33333, 30)

    @patch("srunx.workflows.tasks.Slurm")
    def test_wait_for_job_cancelled(self, mock_client_class: Mock) -> None:
        """Test wait for job with cancelled job."""
        # Setup
        mock_client = Mock()
        mock_client.monitor.side_effect = RuntimeError("SLURM job 44444 was cancelled")
        mock_client_class.return_value = mock_client

        # Execute and verify
        with pytest.raises(RuntimeError, match="SLURM job 44444 was cancelled"):
            wait_for_job(44444)

    @patch("srunx.workflows.tasks.Slurm")
    def test_wait_for_job_timeout(self, mock_client_class: Mock) -> None:
        """Test wait for job with timeout."""
        # Setup
        mock_client = Mock()
        mock_client.monitor.side_effect = RuntimeError("SLURM job 55555 was timeout")
        mock_client_class.return_value = mock_client

        # Execute and verify
        with pytest.raises(RuntimeError, match="SLURM job 55555 was timeout"):
            wait_for_job(55555)


class TestTaskIntegration:
    """Test task integration scenarios."""

    @patch("srunx.workflows.tasks.Slurm")
    def test_async_then_wait_workflow(self, mock_client_class: Mock) -> None:
        """Test workflow pattern: submit async, then wait."""
        # Setup
        job = Job(
            name="workflow_job",
            command=["python", "workflow_script.py"],
            environment=JobEnvironment(conda="workflow_env"),
        )

        submitted_job = Job(
            name="workflow_job",
            command=["python", "workflow_script.py"],
            environment=JobEnvironment(conda="workflow_env"),
            job_id=77777,
            status=JobStatus.PENDING,
        )

        completed_job = Job(
            name="workflow_job",
            command=["python", "workflow_script.py"],
            environment=JobEnvironment(conda="workflow_env"),
            job_id=77777,
            status=JobStatus.COMPLETED,
        )

        mock_client = Mock()
        mock_client.run.return_value = submitted_job
        mock_client.monitor.return_value = completed_job
        mock_client_class.return_value = mock_client

        # Execute async submission
        async_result = submit_job_async(job)
        assert async_result.job_id == 77777
        assert async_result.status == JobStatus.PENDING

        # Execute wait
        wait_result = wait_for_job(async_result.job_id)
        assert wait_result.job_id == 77777
        assert wait_result.status == JobStatus.COMPLETED

        # Verify calls
        mock_client.run.assert_called_once_with(job)
        mock_client.monitor.assert_called_once_with(77777, 30)

    @patch("srunx.workflows.tasks.Slurm")
    def test_multiple_jobs_different_envs(self, mock_client_class: Mock) -> None:
        """Test submitting multiple jobs with different environments."""
        # Setup
        conda_job = Job(
            name="conda_job",
            command=["python", "conda_script.py"],
            environment=JobEnvironment(conda="conda_env"),
        )

        venv_job = Job(
            name="venv_job",
            command=["python", "venv_script.py"],
            environment=JobEnvironment(venv="/path/to/venv"),
        )

        sqsh_job = Job(
            name="sqsh_job",
            command=["python", "sqsh_script.py"],
            environment=JobEnvironment(sqsh="/path/to/image.sqsh"),
        )

        mock_client = Mock()
        # Create mock result Jobs
        mock_result1 = Job(
            name="conda_job",
            command=["python", "conda_script.py"],
            environment=JobEnvironment(conda="conda_env"),
            job_id=101,
            status=JobStatus.PENDING,
        )
        mock_result2 = Job(
            name="venv_job",
            command=["python", "venv_script.py"],
            environment=JobEnvironment(venv="/path/to/venv"),
            job_id=102,
            status=JobStatus.PENDING,
        )
        mock_result3 = Job(
            name="sqsh_job",
            command=["python", "sqsh_script.py"],
            environment=JobEnvironment(sqsh="/path/to/image.sqsh"),
            job_id=103,
            status=JobStatus.PENDING,
        )

        mock_client.run.side_effect = [mock_result1, mock_result2, mock_result3]
        mock_client_class.return_value = mock_client

        # Execute
        result1: Job | ShellJob = submit_job_async(conda_job)
        result2: Job | ShellJob = submit_job_async(venv_job)
        result3: Job | ShellJob = submit_job_async(sqsh_job)

        # Verify
        assert result1.job_id == 101
        assert result1.environment.conda == "conda_env"  # type: ignore

        assert result2.job_id == 102
        assert result2.environment.venv == "/path/to/venv"  # type: ignore

        assert result3.job_id == 103
        assert result3.environment.sqsh == "/path/to/image.sqsh"  # type: ignore

        assert mock_client.run.call_count == 3
