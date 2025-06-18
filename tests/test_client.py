"""Tests for srunx.client module."""

import subprocess
import types
from unittest.mock import Mock, patch

import pytest

from srunx.client import Slurm, cancel_job, retrieve_job, submit_job
from srunx.models import Job, JobEnvironment, JobStatus, ShellJob


class TestSlurm:
    """Test Slurm class."""

    def test_init_default_template(self) -> None:
        """Test Slurm initialization with default template."""
        client = Slurm()
        assert client.default_template is not None
        assert "base.slurm.jinja" in client.default_template

    def test_init_custom_template(self) -> None:
        """Test Slurm initialization with custom template."""
        custom_template = "/path/to/custom.jinja"
        client = Slurm(default_template=custom_template)
        assert client.default_template == custom_template

    @patch("srunx.client.subprocess.run")
    @patch("srunx.client.render_job_script")
    def test_submit_job_with_command_success(
        self, mock_render: Mock, mock_run: Mock
    ) -> None:
        """Test successful job submission with command."""
        # Setup
        job = Job(
            name="test_job",
            command=["python", "script.py"],
            environment=JobEnvironment(conda="test_env"),
        )

        mock_render.return_value = "/tmp/test_job.slurm"
        mock_run.return_value = Mock(stdout="Submitted batch job 12345")

        client = Slurm()

        # Execute
        result = client.run(job)

        # Verify
        assert isinstance(result, Job)
        assert result.job_id == 12345
        assert result.status == JobStatus.PENDING
        assert result.name == "test_job"

        mock_render.assert_called_once()
        mock_run.assert_called_once()

        # Check sbatch command
        call_args = mock_run.call_args[0][0]
        assert call_args[0] == "sbatch"
        assert "/tmp/test_job.slurm" in call_args

    @patch("srunx.client.subprocess.run")
    def test_submit_job_with_file_success(self, mock_run: Mock) -> None:
        """Test successful job submission with file."""
        # Setup
        job = ShellJob(
            name="test_job",
            path="/path/to/script.slurm",
        )

        mock_run.return_value = Mock(stdout="Submitted batch job 54321")

        client = Slurm()

        # Execute
        result = client.run(job)

        # Verify
        assert isinstance(result, ShellJob)
        assert result.job_id == 54321
        assert result.status == JobStatus.PENDING

        mock_run.assert_called_once_with(
            ["sbatch", "/path/to/script.slurm"],
            capture_output=True,
            text=True,
            check=True,
        )

    @patch("srunx.client.subprocess.run")
    @patch("srunx.client.render_job_script")
    def test_submit_job_with_sqsh(self, mock_render: Mock, mock_run: Mock) -> None:
        """Test job submission with sqsh container."""
        # Setup
        job = Job(
            name="container_job",
            command=["python", "script.py"],
            environment=JobEnvironment(sqsh="/path/to/image.sqsh"),
        )

        mock_render.return_value = "/tmp/container_job.slurm"
        mock_run.return_value = Mock(stdout="Submitted batch job 67890")

        client = Slurm()

        # Execute
        result = client.run(job)

        # Verify
        assert result.job_id == 67890

        # Check that sqsh flag was added
        call_args = mock_run.call_args[0][0]
        assert "--sqsh" in call_args
        assert "/path/to/image.sqsh" in call_args

    @patch("srunx.client.subprocess.run")
    @patch("srunx.client.render_job_script")
    def test_submit_job_subprocess_error(
        self, mock_render: Mock, mock_run: Mock
    ) -> None:
        """Test job submission with subprocess error."""
        # Setup
        job = Job(
            name="failing_job",
            command=["python", "script.py"],
            environment=JobEnvironment(conda="test_env"),
        )

        mock_render.return_value = "/tmp/failing_job.slurm"
        mock_run.side_effect = subprocess.CalledProcessError(
            returncode=1,
            cmd=["sbatch", "/tmp/failing_job.slurm"],
        )

        client = Slurm()

        # Execute and verify
        with pytest.raises(subprocess.CalledProcessError):
            client.run(job)

    @patch("srunx.client.get_job_status")
    def test_retrieve_job_success(self, mock_get_status: Mock) -> None:
        """Test successful job status query."""
        # Setup
        expected_job = Job(
            name="test_job",
            command=["unknown"],
            environment=JobEnvironment(conda="unknown"),
            job_id=12345,
            status=JobStatus.RUNNING,
        )
        mock_get_status.return_value = expected_job

        client = Slurm()

        # Execute
        result = client.retrieve(12345)

        # Verify
        assert isinstance(result, Job)
        assert result.job_id == 12345
        assert result.name == "test_job"
        assert result.status == JobStatus.RUNNING
        assert result.environment.conda == "unknown"

        mock_get_status.assert_called_once_with(12345)

    @patch("srunx.client.get_job_status")
    def test_retrieve_job_not_found(self, mock_get_status: Mock) -> None:
        """Test job status query for non-existent job."""
        # Setup
        mock_get_status.side_effect = ValueError("No job information found")

        client = Slurm()

        # Execute and verify
        with pytest.raises(ValueError, match="No job information found"):
            client.retrieve(99999)

    @patch("srunx.client.get_job_status")
    def test_retrieve_job_invalid_data(self, mock_get_status: Mock) -> None:
        """Test job status query with invalid data format."""
        # Setup
        mock_get_status.side_effect = ValueError("Cannot parse job data")

        client = Slurm()

        # Execute and verify
        with pytest.raises(ValueError, match="Cannot parse job data"):
            client.retrieve(12345)

    @patch("srunx.client.subprocess.run")
    def test_cancel_job_success(self, mock_run: Mock) -> None:
        """Test successful job cancellation."""
        # Setup
        mock_run.return_value = Mock()

        client = Slurm()

        # Execute
        client.cancel(12345)

        # Verify
        mock_run.assert_called_once_with(
            ["scancel", "12345"],
            check=True,
        )

    @patch("srunx.client.subprocess.run")
    def test_cancel_job_error(self, mock_run: Mock) -> None:
        """Test job cancellation with error."""
        # Setup
        mock_run.side_effect = subprocess.CalledProcessError(
            returncode=1,
            cmd=["scancel", "12345"],
        )

        client = Slurm()

        # Execute and verify
        with pytest.raises(subprocess.CalledProcessError):
            client.cancel(12345)

    @patch("srunx.client.subprocess.run")
    def test_queue_jobs_success(self, mock_run: Mock) -> None:
        """Test successful job queueing."""
        # Setup
        mock_output = """12345    normal test_job1    user1      RUNNING    0:30      1:00:00      1 node1
54321    gpu    test_job2    user1      PENDING    0:00      2:00:00      1 node2"""
        mock_run.return_value = Mock(stdout=mock_output)

        client = Slurm()

        # Execute
        jobs = client.queue()

        # Verify
        assert len(jobs) == 2

        job1 = jobs[0]
        assert job1.job_id == 12345
        assert job1.name == "test_job1"
        assert job1.status == JobStatus.RUNNING
        assert hasattr(job1, "job_id")

        job2 = jobs[1]
        assert job2.job_id == 54321
        assert job2.name == "test_job2"
        assert job2.status == JobStatus.PENDING
        assert hasattr(job2, "job_id")

    @patch("srunx.client.subprocess.run")
    def test_queue_jobs_with_user(self, mock_run: Mock) -> None:
        """Test job queueing for specific user."""
        # Setup
        mock_run.return_value = Mock(stdout="")

        client = Slurm()

        # Execute
        client.queue(user="testuser")

        # Verify
        call_args = mock_run.call_args[0][0]
        assert "--user" in call_args
        assert "testuser" in call_args

    @patch("srunx.client.subprocess.run")
    def test_queue_jobs_empty(self, mock_run: Mock) -> None:
        """Test job queueing with empty result."""
        # Setup
        mock_run.return_value = Mock(stdout="")

        client = Slurm()

        # Execute
        jobs = client.queue()

        # Verify
        assert jobs == []

    @patch("time.sleep")
    def test_wait_for_completion_success(self, mock_sleep: Mock) -> None:
        """Test successful wait for job completion."""
        # Setup
        completed_job = Job(
            name="completed_job",
            command=["echo", "done"],
            environment=JobEnvironment(conda="test_env"),
            job_id=12345,
            status=JobStatus.COMPLETED,
        )

        # 状態遷移を再現するための状態リスト
        status_sequence = [JobStatus.PENDING, JobStatus.RUNNING, JobStatus.COMPLETED]
        call_count = 0

        def mock_refresh(self) -> None:  # type: ignore[no-untyped-def]
            nonlocal call_count
            if call_count < len(status_sequence):
                completed_job.status = status_sequence[call_count]
                call_count += 1

        # refresh メソッドを差し替え
        object.__setattr__(
            completed_job, "refresh", types.MethodType(mock_refresh, completed_job)
        )  # type: ignore[attr-defined]

        client = Slurm()

        # Execute
        result = client.monitor(completed_job, poll_interval=1)

        # Verify
        assert result == completed_job
        assert result.status == JobStatus.COMPLETED
        assert (
            mock_sleep.call_count == 2
        )  # PENDING → RUNNING → COMPLETED の2回ポーリング

    @patch("time.sleep")
    def test_wait_for_completion_failed(self, mock_sleep: Mock) -> None:
        """Test wait for job completion with failed job."""
        # Setup
        failed_job = Job(
            name="failed_job",
            command=["false"],
            environment=JobEnvironment(conda="test_env"),
            job_id=12345,
            status=JobStatus.FAILED,
        )

        status_sequence = [JobStatus.PENDING, JobStatus.FAILED]
        call_count = 0

        def mock_refresh(self) -> None:  # type: ignore[no-untyped-def]
            nonlocal call_count
            if call_count < len(status_sequence):
                self.status = status_sequence[call_count]
                call_count += 1

        # Mock the refresh method
        object.__setattr__(
            failed_job, "refresh", types.MethodType(mock_refresh, failed_job)
        )  # type: ignore[attr-defined]

        client = Slurm()

        # Execute and verify
        with pytest.raises(RuntimeError, match="SLURM job 12345 failed"):
            client.monitor(failed_job)

    @patch("time.sleep")
    def test_wait_for_completion_cancelled(self, mock_sleep: Mock) -> None:
        """Test wait for job completion with cancelled job."""
        # Setup
        cancelled_job = Job(
            name="cancelled_job",
            command=["sleep", "100"],
            environment=JobEnvironment(conda="test_env"),
            job_id=12345,
            status=JobStatus.CANCELLED,
        )

        status_sequence = [JobStatus.PENDING, JobStatus.CANCELLED]
        call_count = 0

        def mock_refresh(self) -> None:  # type: ignore[no-untyped-def]
            nonlocal call_count
            if call_count < len(status_sequence):
                self.status = status_sequence[call_count]
                call_count += 1

        # Mock the refresh method
        object.__setattr__(
            cancelled_job, "refresh", types.MethodType(mock_refresh, cancelled_job)
        )  # type: ignore[attr-defined]

        client = Slurm()

        # Execute and verify
        with pytest.raises(RuntimeError, match="SLURM job 12345 was cancelled"):
            client.monitor(cancelled_job)

    @patch("time.sleep")
    def test_wait_for_completion_polling(self, mock_sleep: Mock) -> None:
        """Test wait for completion with polling."""
        polling_job = Job(
            name="polling_job",
            command=["echo", "test"],
            environment=JobEnvironment(conda="test_env"),
            job_id=12345,
            status=JobStatus.PENDING,
        )

        status_sequence = [JobStatus.PENDING, JobStatus.RUNNING, JobStatus.COMPLETED]
        call_count = 0

        def mock_refresh(self) -> None:  # type: ignore[no-untyped-def]
            nonlocal call_count
            if call_count < len(status_sequence):
                self.status = status_sequence[call_count]
                call_count += 1

        object.__setattr__(
            polling_job, "refresh", types.MethodType(mock_refresh, polling_job)
        )  # type: ignore[attr-defined]

        client = Slurm()
        result = client.monitor(polling_job, poll_interval=1)

        assert result == polling_job
        assert result.status == JobStatus.COMPLETED
        assert call_count == 3
        assert mock_sleep.call_count == 2


class TestConvenienceFunctions:
    """Test convenience functions."""

    @patch("srunx.client.Slurm.run")
    def test_submit_job_convenience(self, mock_run: Mock) -> None:
        """Test submit_job convenience function."""
        job = Job(
            name="test_job",
            command=["python", "script.py"],
            environment=JobEnvironment(conda="test_env"),
        )

        expected_job = Job(
            name="test_job",
            command=["python", "script.py"],
            environment=JobEnvironment(conda="test_env"),
            job_id=12345,
            status=JobStatus.PENDING,
        )
        mock_run.return_value = expected_job

        result = submit_job(job)

        assert result == expected_job
        mock_run.assert_called_once_with(job, None)

    @patch("srunx.client.Slurm.retrieve")
    def test_retrieve_job_convenience(self, mock_retrieve: Mock) -> None:
        """Test retrieve_job convenience function."""
        expected_job = Job(
            name="test_job",
            command=["unknown"],
            environment=JobEnvironment(conda="test_env"),
            job_id=12345,
            status=JobStatus.RUNNING,
        )
        mock_retrieve.return_value = expected_job

        result = retrieve_job(12345)

        assert result == expected_job
        mock_retrieve.assert_called_once_with(12345)

    @patch("srunx.client.Slurm.cancel")
    def test_cancel_job_convenience(self, mock_cancel: Mock) -> None:
        """Test cancel_job convenience function."""
        cancel_job(12345)

        mock_cancel.assert_called_once_with(12345)
