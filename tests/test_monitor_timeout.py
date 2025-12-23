"""Tests for monitoring timeout validation."""

import time
from unittest.mock import MagicMock

import pytest

from srunx.models import Job, JobStatus
from srunx.monitor.job_monitor import JobMonitor
from srunx.monitor.resource_monitor import ResourceMonitor
from srunx.monitor.types import MonitorConfig, ResourceSnapshot, WatchMode


class TestJobMonitorTimeout:
    """Test JobMonitor timeout behavior."""

    def test_watch_until_respects_timeout(self):
        """Test that watch_until raises TimeoutError after timeout expires."""
        config = MonitorConfig(poll_interval=1, timeout=2)
        monitor = JobMonitor(job_ids=[123], config=config)

        # Mock job that never completes
        job = Job(name="job1", job_id=123, command=["test"])
        job._status = JobStatus.RUNNING
        monitor.client = MagicMock()
        monitor.client.retrieve = MagicMock(return_value=job)

        start_time = time.time()
        with pytest.raises(TimeoutError):
            monitor.watch_until()
        elapsed = time.time() - start_time

        # Should timeout after ~2 seconds
        assert 1.5 < elapsed < 3.5  # Allow some tolerance

    def test_watch_until_exits_before_timeout_on_completion(self):
        """Test that watch_until exits immediately when condition met."""
        config = MonitorConfig(poll_interval=1, timeout=10)
        monitor = JobMonitor(job_ids=[123], config=config)

        # Mock job that completes immediately
        job = Job(name="job1", job_id=123, command=["test"])
        job._status = JobStatus.COMPLETED
        monitor.client = MagicMock()
        monitor.client.retrieve = MagicMock(return_value=job)

        start_time = time.time()
        monitor.watch_until()
        elapsed = time.time() - start_time

        # Should exit quickly, not wait for timeout
        assert elapsed < 2.0

    def test_watch_until_no_timeout_waits_indefinitely(self):
        """Test that watch_until with no timeout can be stopped by signal."""
        config = MonitorConfig(poll_interval=1, timeout=None)
        monitor = JobMonitor(job_ids=[123], config=config)

        # Mock job that never completes
        job = Job(name="job1", job_id=123, command=["test"])
        job._status = JobStatus.RUNNING
        monitor.client = MagicMock()

        call_count = 0

        def mock_retrieve(job_id):
            nonlocal call_count
            call_count += 1
            if call_count >= 3:
                # Stop after 3 polls to prevent infinite loop
                monitor._stop_requested = True
            return job

        monitor.client.retrieve = mock_retrieve

        start_time = time.time()
        monitor.watch_until()
        elapsed = time.time() - start_time

        # Should have polled 3 times with 1s interval
        assert 2.0 < elapsed < 4.0
        assert call_count == 3

    def test_timeout_one_second_exits_quickly(self):
        """Test that timeout=1 causes quick exit."""
        config = MonitorConfig(poll_interval=1, timeout=1)
        monitor = JobMonitor(job_ids=[123], config=config)

        job = Job(name="job1", job_id=123, command=["test"])
        job._status = JobStatus.RUNNING
        monitor.client = MagicMock()
        monitor.client.retrieve = MagicMock(return_value=job)

        start_time = time.time()
        with pytest.raises(TimeoutError):
            monitor.watch_until()
        elapsed = time.time() - start_time

        # Should timeout after ~1 second
        assert 0.5 < elapsed < 2.0


class TestResourceMonitorTimeout:
    """Test ResourceMonitor timeout behavior."""

    def test_watch_until_respects_timeout(self):
        """Test that watch_until raises TimeoutError after timeout expires."""
        config = MonitorConfig(poll_interval=1, timeout=2)
        monitor = ResourceMonitor(min_gpus=4, config=config)

        # Mock insufficient resources
        snapshot = ResourceSnapshot(
            partition=None,
            total_gpus=10,
            gpus_in_use=8,
            gpus_available=2,  # Below threshold
            jobs_running=4,
            nodes_total=4,
            nodes_idle=0,
            nodes_down=0,
        )
        monitor.get_partition_resources = MagicMock(return_value=snapshot)

        start_time = time.time()
        with pytest.raises(TimeoutError):
            monitor.watch_until()
        elapsed = time.time() - start_time

        # Should timeout after ~2 seconds
        assert 1.5 < elapsed < 3.5

    def test_watch_until_exits_before_timeout_on_availability(self):
        """Test that watch_until exits when GPUs become available."""
        config = MonitorConfig(poll_interval=1, timeout=10)
        monitor = ResourceMonitor(min_gpus=4, config=config)

        # Mock sufficient resources
        snapshot = ResourceSnapshot(
            partition=None,
            total_gpus=10,
            gpus_in_use=4,
            gpus_available=6,  # Above threshold
            jobs_running=2,
            nodes_total=4,
            nodes_idle=2,
            nodes_down=0,
        )
        monitor.get_partition_resources = MagicMock(return_value=snapshot)

        start_time = time.time()
        monitor.watch_until()
        elapsed = time.time() - start_time

        # Should exit quickly
        assert elapsed < 2.0

    def test_watch_until_no_timeout_waits_indefinitely(self):
        """Test that watch_until with no timeout can be stopped by signal."""
        config = MonitorConfig(poll_interval=1, timeout=None)
        monitor = ResourceMonitor(min_gpus=4, config=config)

        call_count = 0

        def mock_get_resources():
            nonlocal call_count
            call_count += 1
            if call_count >= 3:
                monitor._stop_requested = True
            return ResourceSnapshot(
                partition=None,
                total_gpus=10,
                gpus_in_use=8,
                gpus_available=2,
                jobs_running=4,
                nodes_total=4,
                nodes_idle=0,
                nodes_down=0,
            )

        monitor.get_partition_resources = mock_get_resources

        start_time = time.time()
        monitor.watch_until()
        elapsed = time.time() - start_time

        # Should have polled 3 times
        assert 2.0 < elapsed < 4.0
        assert call_count == 3


class TestContinuousModeTimeout:
    """Test timeout behavior in continuous monitoring mode."""

    def test_continuous_mode_ignores_timeout(self):
        """Test that continuous mode doesn't use timeout parameter."""
        # Continuous mode should run indefinitely until stopped
        config = MonitorConfig(
            poll_interval=1,
            timeout=2,  # Should be ignored
            mode=WatchMode.CONTINUOUS,
        )
        monitor = JobMonitor(job_ids=[123], config=config)

        job = Job(name="job1", job_id=123, command=["test"])
        job._status = JobStatus.RUNNING
        monitor.client = MagicMock()

        call_count = 0

        def mock_retrieve(job_id):
            nonlocal call_count
            call_count += 1
            if call_count >= 5:
                # Stop after 5 polls (would be 2s timeout if respected)
                monitor._stop_requested = True
            return job

        monitor.client.retrieve = mock_retrieve

        start_time = time.time()
        monitor.watch_continuous()
        elapsed = time.time() - start_time

        # Should poll 5 times (>2s timeout), proving timeout is ignored
        assert elapsed > 3.0
        assert call_count == 5


class TestPollIntervalTiming:
    """Test that poll_interval is respected."""

    def test_poll_interval_timing(self):
        """Test that monitor waits poll_interval between checks."""
        config = MonitorConfig(poll_interval=2, timeout=None)
        monitor = JobMonitor(job_ids=[123], config=config)

        job = Job(name="job1", job_id=123, command=["test"])
        job._status = JobStatus.RUNNING
        monitor.client = MagicMock()

        poll_times = []

        def mock_retrieve(job_id):
            poll_times.append(time.time())
            if len(poll_times) >= 3:
                monitor._stop_requested = True
            return job

        monitor.client.retrieve = mock_retrieve

        monitor.watch_until()

        # Check intervals between polls
        assert len(poll_times) == 3
        interval1 = poll_times[1] - poll_times[0]
        interval2 = poll_times[2] - poll_times[1]

        # Intervals should be ~2 seconds (allow some tolerance)
        assert 1.8 < interval1 < 2.5
        assert 1.8 < interval2 < 2.5

    def test_fast_poll_interval(self):
        """Test monitoring with fast poll interval."""
        config = MonitorConfig(poll_interval=1, timeout=None)
        monitor = ResourceMonitor(min_gpus=2, config=config)

        call_count = 0

        def mock_get_resources():
            nonlocal call_count
            call_count += 1
            if call_count >= 4:
                monitor._stop_requested = True
            return ResourceSnapshot(
                partition=None,
                total_gpus=10,
                gpus_in_use=9,
                gpus_available=1,
                jobs_running=5,
                nodes_total=4,
                nodes_idle=0,
                nodes_down=0,
            )

        monitor.get_partition_resources = mock_get_resources

        start_time = time.time()
        monitor.watch_until()
        elapsed = time.time() - start_time

        # Should poll 4 times in ~3 seconds
        assert 2.5 < elapsed < 4.5
        assert call_count == 4
