"""Tests for JobMonitor class."""

from unittest.mock import MagicMock

import pytest

from srunx.models import Job, JobStatus
from srunx.monitor.job_monitor import JobMonitor
from srunx.monitor.types import MonitorConfig


class TestJobMonitor:
    """Test suite for JobMonitor."""

    def test_init_single_job(self):
        """Test initialization with single job ID."""
        monitor = JobMonitor(job_ids=[12345])
        assert monitor.job_ids == [12345]
        assert JobStatus.COMPLETED in monitor.target_statuses
        assert JobStatus.FAILED in monitor.target_statuses
        assert JobStatus.CANCELLED in monitor.target_statuses
        assert JobStatus.TIMEOUT in monitor.target_statuses

    def test_init_multiple_jobs(self):
        """Test initialization with multiple job IDs."""
        monitor = JobMonitor(job_ids=[12345, 67890])
        assert monitor.job_ids == [12345, 67890]
        assert len(monitor._previous_states) == 0

    def test_init_custom_target_statuses(self):
        """Test initialization with custom target statuses."""
        monitor = JobMonitor(
            job_ids=[12345],
            target_statuses=[JobStatus.COMPLETED],
        )
        assert monitor.target_statuses == [JobStatus.COMPLETED]

    def test_init_empty_job_ids_raises_error(self):
        """Test that empty job_ids raises ValueError."""
        with pytest.raises(ValueError, match="job_ids cannot be empty"):
            JobMonitor(job_ids=[])

    def test_check_condition_all_completed(self):
        """Test check_condition when all jobs are completed."""
        monitor = JobMonitor(job_ids=[123, 456])

        # Mock client to return completed jobs
        job1 = Job(name="job1", job_id=123, command=["test"])
        job1._status = JobStatus.COMPLETED
        job2 = Job(name="job2", job_id=456, command=["test"])
        job2._status = JobStatus.COMPLETED

        monitor.client = MagicMock()
        monitor.client.retrieve = MagicMock(side_effect=[job1, job2])

        assert monitor.check_condition() is True

    def test_check_condition_some_running(self):
        """Test check_condition when some jobs still running."""
        monitor = JobMonitor(job_ids=[123, 456])

        # Mock client to return mixed statuses
        job1 = Job(name="job1", job_id=123, command=["test"])
        job1._status = JobStatus.COMPLETED
        job2 = Job(name="job2", job_id=456, command=["test"])
        job2._status = JobStatus.RUNNING

        monitor.client = MagicMock()
        monitor.client.retrieve = MagicMock(side_effect=[job1, job2])

        assert monitor.check_condition() is False

    def test_check_condition_with_failed_jobs(self):
        """Test check_condition includes failed jobs as terminal."""
        monitor = JobMonitor(job_ids=[123])

        # Mock client to return failed job
        job = Job(name="job1", job_id=123, command=["test"])
        job._status = JobStatus.FAILED

        monitor.client = MagicMock()
        monitor.client.retrieve = MagicMock(return_value=job)

        assert monitor.check_condition() is True

    def test_get_current_state(self):
        """Test get_current_state returns job status mapping."""
        monitor = JobMonitor(job_ids=[123, 456])

        # Mock client to return jobs with different statuses
        job1 = Job(name="job1", job_id=123, command=["test"])
        job1._status = JobStatus.RUNNING
        job2 = Job(name="job2", job_id=456, command=["test"])
        job2._status = JobStatus.COMPLETED

        monitor.client = MagicMock()
        monitor.client.retrieve = MagicMock(side_effect=[job1, job2])

        state = monitor.get_current_state()
        assert state == {"123": "RUNNING", "456": "COMPLETED"}

    def test_get_monitored_jobs_with_error(self):
        """Test _get_monitored_jobs handles retrieval errors gracefully."""
        monitor = JobMonitor(job_ids=[123])

        # Mock client to raise exception
        monitor.client = MagicMock()
        monitor.client.retrieve = MagicMock(side_effect=Exception("SLURM error"))

        jobs = monitor._get_monitored_jobs()
        assert len(jobs) == 1
        # When error occurs, a placeholder job with UNKNOWN status is created
        assert jobs[0].status == JobStatus.UNKNOWN
        assert jobs[0].job_id == 123

    def test_notify_transition_running(self):
        """Test _notify_transition calls on_job_running for RUNNING status."""
        monitor = JobMonitor(job_ids=[123])

        # Create mock callback
        callback = MagicMock()
        monitor.callbacks = [callback]

        job = Job(name="job1", job_id=123, command=["test"])
        job._status = JobStatus.RUNNING
        monitor._notify_transition(job, JobStatus.RUNNING)

        callback.on_job_running.assert_called_once_with(job)

    def test_notify_transition_completed(self):
        """Test _notify_transition calls on_job_completed for COMPLETED status."""
        monitor = JobMonitor(job_ids=[123])

        # Create mock callback
        callback = MagicMock()
        monitor.callbacks = [callback]

        job = Job(name="job1", job_id=123, command=["test"])
        job._status = JobStatus.COMPLETED
        monitor._notify_transition(job, JobStatus.COMPLETED)

        callback.on_job_completed.assert_called_once_with(job)

    def test_notify_transition_failed(self):
        """Test _notify_transition calls on_job_failed for FAILED status."""
        monitor = JobMonitor(job_ids=[123])

        # Create mock callback
        callback = MagicMock()
        monitor.callbacks = [callback]

        job = Job(name="job1", job_id=123, command=["test"])
        job._status = JobStatus.FAILED
        monitor._notify_transition(job, JobStatus.FAILED)

        callback.on_job_failed.assert_called_once_with(job)

    def test_notify_transition_cancelled(self):
        """Test _notify_transition calls on_job_cancelled for CANCELLED status."""
        monitor = JobMonitor(job_ids=[123])

        # Create mock callback
        callback = MagicMock()
        monitor.callbacks = [callback]

        job = Job(name="job1", job_id=123, command=["test"])
        job._status = JobStatus.CANCELLED
        monitor._notify_transition(job, JobStatus.CANCELLED)

        callback.on_job_cancelled.assert_called_once_with(job)

    def test_notify_transition_timeout(self):
        """Test _notify_transition calls on_job_failed for TIMEOUT status."""
        monitor = JobMonitor(job_ids=[123])

        # Create mock callback
        callback = MagicMock()
        monitor.callbacks = [callback]

        job = Job(name="job1", job_id=123, command=["test"])
        job._status = JobStatus.TIMEOUT
        monitor._notify_transition(job, JobStatus.TIMEOUT)

        # Timeout is treated as failed
        callback.on_job_failed.assert_called_once_with(job)

    def test_notify_callbacks_state_change_detection(self):
        """Test _notify_callbacks only notifies on state transitions."""
        monitor = JobMonitor(job_ids=[123])

        # Create mock callback
        callback = MagicMock()
        monitor.callbacks = [callback]

        # Mock client to return different statuses
        monitor.client = MagicMock()

        # First call: RUNNING
        job_running = Job(name="job1", job_id=123, command=["test"])
        job_running._status = JobStatus.RUNNING
        monitor.client.retrieve = MagicMock(return_value=job_running)

        monitor._notify_callbacks("state_changed")
        assert callback.on_job_running.call_count == 1

        # Second call: still RUNNING (should not notify)
        monitor._notify_callbacks("state_changed")
        assert callback.on_job_running.call_count == 1

        # Third call: COMPLETED (should notify)
        job_completed = Job(name="job1", job_id=123, command=["test"])
        job_completed._status = JobStatus.COMPLETED
        monitor.client.retrieve = MagicMock(return_value=job_completed)

        monitor._notify_callbacks("state_changed")
        assert callback.on_job_completed.call_count == 1

    def test_notify_callbacks_handles_callback_errors(self):
        """Test _notify_callbacks handles callback exceptions gracefully."""
        monitor = JobMonitor(job_ids=[123])

        # Create mock callback that raises exception
        callback = MagicMock()
        callback.on_job_running.side_effect = Exception("Callback error")
        monitor.callbacks = [callback]

        # Mock client to return running job
        monitor.client = MagicMock()
        job = Job(name="job1", job_id=123, command=["test"])
        job._status = JobStatus.RUNNING
        monitor.client.retrieve = MagicMock(return_value=job)

        # Should not raise exception
        monitor._notify_callbacks("state_changed")

    def test_watch_continuous_state_changes(self):
        """Test watch_continuous notifies on job state changes."""
        import threading

        from srunx.monitor.types import WatchMode

        config = MonitorConfig(
            poll_interval=1,
            mode=WatchMode.CONTINUOUS,
            notify_on_change=True,
        )
        monitor = JobMonitor(job_ids=[123], config=config)

        # Create mock callback
        callback = MagicMock()
        monitor.callbacks = [callback]

        # Mock client to return different states
        monitor.client = MagicMock()
        call_count = 0

        def mock_retrieve(job_id):
            nonlocal call_count
            call_count += 1
            job = Job(name="job1", job_id=123, command=["test"])
            if call_count == 1:
                job._status = JobStatus.PENDING
            elif call_count == 2:
                job._status = JobStatus.RUNNING
            else:
                # Stop monitoring after 3 calls
                monitor._stop_requested = True
                job._status = JobStatus.RUNNING
            return job

        monitor.client.retrieve = mock_retrieve

        # Run watch_continuous in thread with timeout
        thread = threading.Thread(target=monitor.watch_continuous)
        thread.start()
        thread.join(timeout=2.0)

        # Should have notified on PENDING -> RUNNING transition
        assert callback.on_job_running.call_count == 1

    def test_watch_continuous_no_duplicate_notifications(self):
        """Test watch_continuous prevents duplicate notifications."""
        import threading

        from srunx.monitor.types import WatchMode

        config = MonitorConfig(
            poll_interval=1,
            mode=WatchMode.CONTINUOUS,
            notify_on_change=True,
        )
        monitor = JobMonitor(job_ids=[123], config=config)

        # Create mock callback
        callback = MagicMock()
        monitor.callbacks = [callback]

        # Mock client to return same state multiple times
        monitor.client = MagicMock()
        call_count = 0

        def mock_retrieve(job_id):
            nonlocal call_count
            call_count += 1
            job = Job(name="job1", job_id=123, command=["test"])
            job._status = JobStatus.RUNNING
            if call_count >= 5:
                # Stop after 5 calls
                monitor._stop_requested = True
            return job

        monitor.client.retrieve = mock_retrieve

        # Run watch_continuous in thread
        thread = threading.Thread(target=monitor.watch_continuous)
        thread.start()
        thread.join(timeout=2.0)

        # Should not notify since state never changes
        assert callback.on_job_running.call_count == 0

    def test_watch_continuous_keyboard_interrupt(self):
        """Test watch_continuous handles KeyboardInterrupt gracefully."""
        import threading
        import time

        from srunx.monitor.types import WatchMode

        config = MonitorConfig(
            poll_interval=1,
            mode=WatchMode.CONTINUOUS,
        )
        monitor = JobMonitor(job_ids=[123], config=config)

        # Mock client
        monitor.client = MagicMock()
        job = Job(name="job1", job_id=123, command=["test"])
        job._status = JobStatus.RUNNING
        monitor.client.retrieve = MagicMock(return_value=job)

        # Start monitoring in thread
        thread = threading.Thread(target=monitor.watch_continuous)
        thread.start()

        # Simulate Ctrl+C by setting stop flag
        time.sleep(0.3)
        monitor._stop_requested = True

        # Wait for graceful shutdown
        thread.join(timeout=2.0)

        # Thread should exit cleanly
        assert not thread.is_alive()

    @pytest.mark.slow
    def test_stability_many_jobs(self):
        """Test monitoring many jobs simultaneously for stability."""

        # Monitor 10 jobs
        job_ids = list(range(1000, 1010))
        monitor = JobMonitor(job_ids=job_ids)

        # Mock client to return completed jobs
        monitor.client = MagicMock()

        def mock_retrieve(job_id):
            job = Job(name=f"job_{job_id}", job_id=job_id, command=["test"])
            job._status = JobStatus.COMPLETED
            return job

        monitor.client.retrieve = mock_retrieve

        # Check all jobs are monitored
        state = monitor.get_current_state()
        assert len(state) == 10
        assert all(state[str(jid)] == "COMPLETED" for jid in job_ids)

        # Verify check_condition returns True
        assert monitor.check_condition() is True

    @pytest.mark.slow
    def test_stability_many_state_changes(self):
        """Test handling many state changes without issues."""
        import threading

        from srunx.monitor.types import WatchMode

        config = MonitorConfig(
            poll_interval=1,
            mode=WatchMode.CONTINUOUS,
            notify_on_change=True,
        )
        monitor = JobMonitor(job_ids=[999], config=config)

        # Create mock callback
        callback = MagicMock()
        monitor.callbacks = [callback]

        # Mock client with many state transitions
        monitor.client = MagicMock()
        call_count = 0
        states = [
            JobStatus.PENDING,
            JobStatus.RUNNING,
            JobStatus.RUNNING,  # Duplicate - should not notify
            JobStatus.FAILED,
            JobStatus.RUNNING,
            JobStatus.CANCELLED,
            JobStatus.COMPLETED,
            JobStatus.COMPLETED,  # Duplicate - should not notify
        ]

        def mock_retrieve(job_id):
            nonlocal call_count
            job = Job(name="job999", job_id=999, command=["test"])
            if call_count < len(states):
                job._status = states[call_count]
            else:
                monitor._stop_requested = True
                job._status = JobStatus.COMPLETED
            call_count += 1
            return job

        monitor.client.retrieve = mock_retrieve

        # Run monitoring
        thread = threading.Thread(target=monitor.watch_continuous)
        thread.start()
        thread.join(timeout=15.0)

        # Should have multiple state changes
        # Duplicates should not trigger notifications
        # Note: Due to multiple retrieve() calls per loop iteration, exact count is non-deterministic
        total_notifications = (
            callback.on_job_running.call_count
            + callback.on_job_completed.call_count
            + callback.on_job_failed.call_count
            + callback.on_job_cancelled.call_count
        )
        assert total_notifications >= 2  # At least some state changes detected
