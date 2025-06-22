"""Tests for srunx.client module."""

import subprocess
from unittest.mock import Mock, patch

import pytest

from srunx.callbacks import Callback
from srunx.client import Slurm, cancel_job, retrieve_job, submit_job
from srunx.models import BaseJob, JobEnvironment, JobStatus, ShellJob


class MockCallback(Callback):
    """Mock callback for testing."""

    def __init__(self):
        self.submitted_jobs = []
        self.completed_jobs = []
        self.failed_jobs = []
        self.cancelled_jobs = []

    def on_job_submitted(self, job):
        self.submitted_jobs.append(job)

    def on_job_completed(self, job):
        self.completed_jobs.append(job)

    def on_job_failed(self, job):
        self.failed_jobs.append(job)

    def on_job_cancelled(self, job):
        self.cancelled_jobs.append(job)


class TestSlurm:
    """Test Slurm class."""

    def test_slurm_init_defaults(self):
        """Test Slurm initialization with defaults."""
        client = Slurm()
        assert client.default_template is not None
        assert client.callbacks == []

    def test_slurm_init_custom(self):
        """Test Slurm initialization with custom values."""
        callback = MockCallback()
        client = Slurm(default_template="/custom/template.jinja", callbacks=[callback])
        assert client.default_template == "/custom/template.jinja"
        assert len(client.callbacks) == 1
        assert client.callbacks[0] is callback

    @patch("time.sleep")  # Mock sleep to speed up tests
    @patch("subprocess.run")
    @patch("srunx.client.render_job_script")
    def test_submit_job_success(self, mock_render, mock_run, mock_sleep, sample_job):
        """Test successful job submission."""
        # Mock render_job_script
        mock_render.return_value = "/tmp/test_job.slurm"

        # Mock subprocess.run for both submit and potential refresh calls
        def mock_run_side_effect(*args, **kwargs):
            if "sbatch" in args[0]:
                # Return submit response
                mock_result = Mock()
                mock_result.stdout = "Submitted batch job 12345"
                return mock_result
            elif "sacct" in args[0]:
                # Return status query response
                mock_result = Mock()
                mock_result.stdout = "12345|PENDING\n"
                return mock_result
            return Mock()

        mock_run.side_effect = mock_run_side_effect

        client = Slurm()
        result = client.submit(sample_job)

        assert result.job_id == 12345
        assert (
            result._status == JobStatus.PENDING
        )  # Access private field to avoid refresh
        mock_render.assert_called_once()
        assert mock_run.call_count >= 1  # May be called multiple times

    @patch("time.sleep")
    @patch("subprocess.run")
    @patch("srunx.client.render_job_script")
    def test_submit_job_with_callbacks(
        self, mock_render, mock_run, mock_sleep, sample_job
    ):
        """Test job submission with callbacks."""
        mock_render.return_value = "/tmp/test_job.slurm"

        def mock_run_side_effect(*args, **kwargs):
            if "sbatch" in args[0]:
                mock_result = Mock()
                mock_result.stdout = "Submitted batch job 12345"
                return mock_result
            elif "sacct" in args[0]:
                mock_result = Mock()
                mock_result.stdout = "12345|PENDING\n"
                return mock_result
            return Mock()

        mock_run.side_effect = mock_run_side_effect

        callback = MockCallback()
        client = Slurm(callbacks=[callback])

        result = client.submit(sample_job)

        assert len(callback.submitted_jobs) == 1
        assert callback.submitted_jobs[0] is result

    @patch("time.sleep")
    @patch("subprocess.run")
    def test_submit_shell_job(self, mock_run, mock_sleep):
        """Test shell job submission."""

        def mock_run_side_effect(*args, **kwargs):
            if "sbatch" in args[0]:
                mock_result = Mock()
                mock_result.stdout = "Submitted batch job 12345"
                return mock_result
            elif "sacct" in args[0]:
                mock_result = Mock()
                mock_result.stdout = "12345|PENDING\n"
                return mock_result
            return Mock()

        mock_run.side_effect = mock_run_side_effect

        shell_job = ShellJob(name="shell_test", path="/path/to/script.sh")
        client = Slurm()

        result = client.submit(shell_job)

        assert result.job_id == 12345
        assert (
            result._status == JobStatus.PENDING
        )  # Access private field to avoid refresh
        # Check that sbatch was called (might be called multiple times due to refresh)
        sbatch_calls = [
            call for call in mock_run.call_args_list if "sbatch" in call[0][0]
        ]
        assert len(sbatch_calls) >= 1
        assert sbatch_calls[0][0][0] == ["sbatch", "/path/to/script.sh"]

    @patch("time.sleep")
    @patch("subprocess.run")
    @patch("srunx.client.render_job_script")
    def test_submit_job_with_sqsh(self, mock_render, mock_run, mock_sleep, sample_job):
        """Test job submission with sqsh container."""
        mock_render.return_value = "/tmp/test_job.slurm"

        def mock_run_side_effect(*args, **kwargs):
            if "sbatch" in args[0]:
                mock_result = Mock()
                mock_result.stdout = "Submitted batch job 12345"
                return mock_result
            elif "sacct" in args[0]:
                mock_result = Mock()
                mock_result.stdout = "12345|PENDING\n"
                return mock_result
            return Mock()

        mock_run.side_effect = mock_run_side_effect

        # Modify job to use sqsh
        sample_job.environment = JobEnvironment(sqsh="/path/to/image.sqsh")

        client = Slurm()
        result = client.submit(sample_job)

        # Check that sbatch was called with --sqsh option
        sbatch_calls = [
            call for call in mock_run.call_args_list if "sbatch" in call[0][0]
        ]
        assert len(sbatch_calls) >= 1
        cmd = sbatch_calls[0][0][0]
        assert cmd[0] == "sbatch"
        assert "--sqsh" in cmd
        sqsh_index = cmd.index("--sqsh")
        assert cmd[sqsh_index + 1] == "/path/to/image.sqsh"

    @patch("time.sleep")
    @patch("subprocess.run")
    @patch("srunx.client.render_job_script")
    def test_submit_job_failure(self, mock_render, mock_run, mock_sleep, sample_job):
        """Test job submission failure."""
        mock_render.return_value = "/tmp/test_job.slurm"
        error = subprocess.CalledProcessError(1, ["sbatch"])
        error.stdout = "Error"
        error.stderr = "Submission failed"
        mock_run.side_effect = error

        client = Slurm()

        with pytest.raises(subprocess.CalledProcessError):
            client.submit(sample_job)

    @patch("srunx.client.get_job_status")
    def test_retrieve_job(self, mock_get_status):
        """Test job retrieval."""
        mock_job = BaseJob(name="test", job_id=12345)
        mock_get_status.return_value = mock_job

        result = Slurm.retrieve(12345)

        assert result is mock_job
        mock_get_status.assert_called_once_with(12345)

    @patch("subprocess.run")
    def test_cancel_job_success(self, mock_run):
        """Test successful job cancellation."""
        mock_run.return_value = None

        client = Slurm()
        client.cancel(12345)

        mock_run.assert_called_once_with(["scancel", "12345"], check=True)

    @patch("subprocess.run")
    def test_cancel_job_failure(self, mock_run):
        """Test job cancellation failure."""
        error = subprocess.CalledProcessError(1, ["scancel"])
        error.stdout = ""
        error.stderr = "Job not found"
        mock_run.side_effect = error

        client = Slurm()

        with pytest.raises(subprocess.CalledProcessError):
            client.cancel(12345)

    @patch("subprocess.run")
    def test_queue_empty(self, mock_run):
        """Test queue with no jobs."""
        mock_run.return_value.stdout = ""

        client = Slurm()
        jobs = client.queue()

        assert jobs == []

    @patch("subprocess.run")
    def test_queue_with_jobs(self, mock_run):
        """Test queue with jobs."""
        mock_run.return_value.stdout = (
            "12345 gpu test_job1 user RUNNING 5:00 1:00:00 1 node1\n"
            "12346 cpu test_job2 user PENDING 0:00 30:00 1 (Priority)\n"
        )

        client = Slurm()
        jobs = client.queue()

        assert len(jobs) == 2
        assert jobs[0].job_id == 12345
        assert jobs[0].name == "test_job1"
        # Use private _status field instead of status property to avoid refresh
        assert jobs[0]._status == JobStatus.RUNNING
        assert jobs[1].job_id == 12346
        assert jobs[1].name == "test_job2"
        assert jobs[1]._status == JobStatus.PENDING

    @patch("subprocess.run")
    def test_queue_with_user(self, mock_run):
        """Test queue with specific user."""
        mock_run.return_value.stdout = ""

        client = Slurm()
        client.queue(user="testuser")

        args, kwargs = mock_run.call_args
        assert "--user" in args[0]
        assert "testuser" in args[0]

    @patch("srunx.client.Slurm.retrieve")
    @patch("time.sleep")
    def test_monitor_job_completion(self, mock_sleep, mock_retrieve, sample_job):
        """Test monitoring job to completion."""
        # Setup job with different statuses over time
        sample_job.job_id = 12345

        # Mock the job status progression
        status_sequence = [JobStatus.PENDING, JobStatus.RUNNING, JobStatus.COMPLETED]

        # Create a mock refresh method that updates status
        call_count = [0]

        def mock_refresh(self):
            if call_count[0] < len(status_sequence):
                self._status = status_sequence[call_count[0]]
                call_count[0] += 1
            return self

        # Initially set job status
        sample_job._status = JobStatus.PENDING

        # Mock the status property to return current _status value
        with patch.object(
            type(sample_job),
            "status",
            new_callable=lambda: property(lambda self: self._status),
        ):
            with patch.object(type(sample_job), "refresh", mock_refresh):
                callback = MockCallback()
                client = Slurm(callbacks=[callback])

                result = client.monitor(sample_job, poll_interval=1)

                assert result is sample_job
                assert len(callback.completed_jobs) == 1
                assert callback.completed_jobs[0] is sample_job

    @patch("srunx.client.Slurm.retrieve")
    @patch("time.sleep")
    def test_monitor_job_failure(self, mock_sleep, mock_retrieve, sample_job):
        """Test monitoring job that fails."""
        sample_job.job_id = 12345
        sample_job._status = JobStatus.FAILED

        def mock_refresh(self):
            self._status = JobStatus.FAILED
            return self

        with patch.object(
            type(sample_job),
            "status",
            new_callable=lambda: property(lambda self: self._status),
        ):
            with patch.object(type(sample_job), "refresh", mock_refresh):
                callback = MockCallback()
                client = Slurm(callbacks=[callback])

                with pytest.raises(RuntimeError):
                    client.monitor(sample_job, poll_interval=1)

                assert len(callback.failed_jobs) == 1

    @patch("srunx.client.Slurm.retrieve")
    @patch("time.sleep")
    def test_monitor_job_cancelled(self, mock_sleep, mock_retrieve, sample_job):
        """Test monitoring job that gets cancelled."""
        sample_job.job_id = 12345
        sample_job._status = JobStatus.CANCELLED

        def mock_refresh(self):
            self._status = JobStatus.CANCELLED
            return self

        with patch.object(
            type(sample_job),
            "status",
            new_callable=lambda: property(lambda self: self._status),
        ):
            with patch.object(type(sample_job), "refresh", mock_refresh):
                callback = MockCallback()
                client = Slurm(callbacks=[callback])

                with pytest.raises(RuntimeError):
                    client.monitor(sample_job, poll_interval=1)

                assert len(callback.cancelled_jobs) == 1

    @patch("srunx.client.Slurm.monitor")
    @patch("srunx.client.Slurm.submit")
    def test_run_job(self, mock_submit, mock_monitor, sample_job):
        """Test run method (submit + monitor)."""
        submitted_job = sample_job
        submitted_job.job_id = 12345
        mock_submit.return_value = submitted_job
        mock_monitor.return_value = submitted_job

        client = Slurm()
        result = client.run(sample_job)

        assert result is submitted_job
        mock_submit.assert_called_once()
        mock_monitor.assert_called_once()


class TestConvenienceFunctions:
    """Test convenience functions."""

    @patch("srunx.client.Slurm.submit")
    def test_submit_job_function(self, mock_submit, sample_job):
        """Test submit_job convenience function."""
        mock_submit.return_value = sample_job

        result = submit_job(sample_job)

        assert result is sample_job
        mock_submit.assert_called_once()

    @patch("srunx.client.Slurm.retrieve")
    def test_retrieve_job_function(self, mock_retrieve):
        """Test retrieve_job convenience function."""
        mock_job = BaseJob(name="test", job_id=12345)
        mock_retrieve.return_value = mock_job

        result = retrieve_job(12345)

        assert result is mock_job
        mock_retrieve.assert_called_once_with(12345)

    @patch("srunx.client.Slurm.cancel")
    def test_cancel_job_function(self, mock_cancel):
        """Test cancel_job convenience function."""
        cancel_job(12345)

        mock_cancel.assert_called_once_with(12345)
