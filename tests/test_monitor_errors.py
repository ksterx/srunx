"""Tests for SLURM command failure error recovery in monitors."""

import subprocess
from unittest.mock import MagicMock, patch

import pytest

from srunx.models import Job, JobStatus
from srunx.monitor.job_monitor import JobMonitor
from srunx.monitor.resource_monitor import ResourceMonitor
from srunx.monitor.types import ResourceSnapshot


class TestJobMonitorErrorRecovery:
    """Test JobMonitor error recovery for SLURM command failures."""

    def test_get_monitored_jobs_handles_slurm_error(self):
        """Test that _get_monitored_jobs creates placeholder for failed retrievals."""
        monitor = JobMonitor(job_ids=[123, 456])
        monitor.client = MagicMock()

        # First job succeeds, second fails
        job1 = Job(name="job1", job_id=123, command=["test"])
        job1._status = JobStatus.RUNNING
        monitor.client.retrieve = MagicMock(
            side_effect=[job1, Exception("SLURM connection error")]
        )

        jobs = monitor._get_monitored_jobs()

        assert len(jobs) == 2
        assert jobs[0].job_id == 123
        assert jobs[0].status == JobStatus.RUNNING
        # Second job should be placeholder with UNKNOWN status
        assert jobs[1].job_id == 456
        assert jobs[1].status == JobStatus.UNKNOWN

    def test_get_monitored_jobs_all_fail(self):
        """Test that all jobs get placeholders when all retrievals fail."""
        monitor = JobMonitor(job_ids=[123, 456, 789])
        monitor.client = MagicMock()
        monitor.client.retrieve = MagicMock(side_effect=Exception("SLURM down"))

        jobs = monitor._get_monitored_jobs()

        assert len(jobs) == 3
        assert all(job.status == JobStatus.UNKNOWN for job in jobs)
        assert [job.job_id for job in jobs] == [123, 456, 789]

    def test_check_condition_with_error_doesnt_crash(self):
        """Test that check_condition doesn't crash on SLURM errors."""
        monitor = JobMonitor(job_ids=[123])
        monitor.client = MagicMock()
        monitor.client.retrieve = MagicMock(side_effect=Exception("Network error"))

        # Should not raise, should return False (UNKNOWN not in target statuses)
        result = monitor.check_condition()
        assert result is False

    def test_get_current_state_with_errors(self):
        """Test get_current_state handles partial failures."""
        monitor = JobMonitor(job_ids=[123, 456])
        monitor.client = MagicMock()

        job1 = Job(name="job1", job_id=123, command=["test"])
        job1._status = JobStatus.COMPLETED
        monitor.client.retrieve = MagicMock(
            side_effect=[job1, Exception("Timeout")]
        )

        state = monitor.get_current_state()

        # Both jobs should be in state (failed one gets UNKNOWN)
        assert len(state) == 2
        assert state["123"] == "COMPLETED"
        assert state["456"] == "UNKNOWN"


class TestResourceMonitorErrorRecovery:
    """Test ResourceMonitor error recovery for SLURM command failures."""

    @patch("subprocess.run")
    def test_get_node_stats_handles_timeout(self, mock_run):
        """Test _get_node_stats recovers from subprocess timeout."""
        mock_run.side_effect = subprocess.TimeoutExpired("sinfo", 30)

        monitor = ResourceMonitor(min_gpus=2)
        nodes_total, nodes_idle, nodes_down, total_gpus = monitor._get_node_stats()

        # Should return zeros on timeout
        assert nodes_total == 0
        assert nodes_idle == 0
        assert nodes_down == 0
        assert total_gpus == 0

    @patch("subprocess.run")
    def test_get_node_stats_handles_process_error(self, mock_run):
        """Test _get_node_stats recovers from subprocess.CalledProcessError."""
        mock_run.side_effect = subprocess.CalledProcessError(
            returncode=1, cmd="sinfo", stderr="SLURM not responding"
        )

        monitor = ResourceMonitor(min_gpus=2)
        nodes_total, nodes_idle, nodes_down, total_gpus = monitor._get_node_stats()

        # Should return zeros on error
        assert nodes_total == 0
        assert nodes_idle == 0
        assert nodes_down == 0
        assert total_gpus == 0

    @patch("subprocess.run")
    def test_get_gpu_usage_handles_timeout(self, mock_run):
        """Test _get_gpu_usage recovers from subprocess timeout."""
        mock_run.side_effect = subprocess.TimeoutExpired("squeue", 30)

        monitor = ResourceMonitor(min_gpus=2)
        gpus_in_use, jobs_running = monitor._get_gpu_usage()

        # Should return zeros on timeout
        assert gpus_in_use == 0
        assert jobs_running == 0

    @patch("subprocess.run")
    def test_get_gpu_usage_handles_process_error(self, mock_run):
        """Test _get_gpu_usage recovers from subprocess.CalledProcessError."""
        mock_run.side_effect = subprocess.CalledProcessError(
            returncode=1, cmd="squeue", stderr="Permission denied"
        )

        monitor = ResourceMonitor(min_gpus=2)
        gpus_in_use, jobs_running = monitor._get_gpu_usage()

        # Should return zeros on error
        assert gpus_in_use == 0
        assert jobs_running == 0

    @patch("subprocess.run")
    def test_get_partition_resources_partial_failure(self, mock_run):
        """Test get_partition_resources when one command fails."""
        # sinfo succeeds, squeue fails
        mock_run.side_effect = [
            MagicMock(
                stdout="node01 gpu:8 idle\n",
                returncode=0,
            ),
            subprocess.CalledProcessError(
                returncode=1, cmd="squeue", stderr="SLURM error"
            ),
        ]

        monitor = ResourceMonitor(min_gpus=2, partition="gpu")
        snapshot = monitor.get_partition_resources()

        # Should have node stats but no GPU usage
        assert snapshot.total_gpus == 8
        assert snapshot.gpus_in_use == 0  # Failed squeue returns 0
        assert snapshot.gpus_available == 8
        assert snapshot.jobs_running == 0

    @patch("subprocess.run")
    def test_get_partition_resources_complete_failure(self, mock_run):
        """Test get_partition_resources when all commands fail."""
        mock_run.side_effect = subprocess.TimeoutExpired("sinfo", 30)

        monitor = ResourceMonitor(min_gpus=2)
        snapshot = monitor.get_partition_resources()

        # Should return snapshot with all zeros
        assert snapshot.total_gpus == 0
        assert snapshot.gpus_in_use == 0
        assert snapshot.gpus_available == 0
        assert snapshot.jobs_running == 0
        assert snapshot.nodes_total == 0

    @patch("subprocess.run")
    def test_check_condition_with_slurm_failure(self, mock_run):
        """Test check_condition handles SLURM command failures gracefully."""
        mock_run.side_effect = subprocess.CalledProcessError(
            returncode=1, cmd="sinfo", stderr="SLURM not available"
        )

        monitor = ResourceMonitor(min_gpus=2)

        # Should not crash, should return False (no GPUs available)
        result = monitor.check_condition()
        assert result is False

    @patch("subprocess.run")
    def test_notify_callbacks_with_errors(self, mock_run):
        """Test _notify_callbacks handles errors in resource queries."""
        mock_run.side_effect = subprocess.TimeoutExpired("sinfo", 30)

        monitor = ResourceMonitor(min_gpus=2)
        callback = MagicMock()
        monitor.callbacks = [callback]

        # Should not crash when notifying with failed resource query
        monitor._notify_callbacks("state_changed")

        # Callback should not be invoked (no state transition with zeros)
        callback.on_resources_available.assert_not_called()
        callback.on_resources_exhausted.assert_not_called()


class TestCallbackErrorHandling:
    """Test callback error handling in monitors."""

    def test_job_monitor_callback_exception_doesnt_crash(self):
        """Test JobMonitor continues after callback exception."""
        monitor = JobMonitor(job_ids=[123])
        monitor.client = MagicMock()

        job = Job(name="job1", job_id=123, command=["test"])
        job._status = JobStatus.RUNNING
        monitor.client.retrieve = MagicMock(return_value=job)

        # Callback raises exception
        callback = MagicMock()
        callback.on_job_running.side_effect = Exception("Slack webhook failed")
        monitor.callbacks = [callback]

        # Should not crash
        monitor._notify_callbacks("state_changed")

    def test_resource_monitor_callback_exception_doesnt_crash(self):
        """Test ResourceMonitor continues after callback exception."""
        monitor = ResourceMonitor(min_gpus=2)

        snapshot = ResourceSnapshot(
            partition=None,
            total_gpus=10,
            gpus_in_use=6,
            gpus_available=4,
            jobs_running=3,
            nodes_total=4,
            nodes_idle=2,
            nodes_down=0,
        )
        monitor.get_partition_resources = MagicMock(return_value=snapshot)

        # Callback raises exception
        callback = MagicMock()
        callback.on_resources_available.side_effect = RuntimeError("Network error")
        monitor.callbacks = [callback]

        # Should not crash
        monitor._notify_callbacks("state_changed")
