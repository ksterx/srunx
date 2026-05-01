"""Tests for monitoring timeout validation.

All timing assertions in this module run against a virtual clock
provided by the ``fake_monitor_clock`` fixture, not wall-clock time.
:class:`BaseMonitor` reads the current time and yields between polls
via the ``time`` module imported into ``srunx.observability.monitoring.base``;
swapping that module reference for a stub means every ``time.time()``
inside ``watch_until`` / ``watch_continuous`` returns the virtual now,
and every ``time.sleep(seconds)`` advances it by exactly ``seconds``.

Result: poll counts and elapsed time become exact values rather than
"approximately N seconds, allow some tolerance for CI scheduling
jitter". Pre-fix this file relied on real ``time.sleep`` and
``time.time`` and was load-fragile (a CI runner with 0.5ms of jitter
could overshoot a tight upper bound and fail).
"""

from unittest.mock import MagicMock

import pytest

from srunx.domain import Job, JobStatus
from srunx.observability.monitoring.job_monitor import JobMonitor
from srunx.observability.monitoring.resource_monitor import ResourceMonitor
from srunx.observability.monitoring.types import (
    MonitorConfig,
    ResourceSnapshot,
    WatchMode,
)


@pytest.fixture
def fake_monitor_clock(monkeypatch):
    """Replace the ``time`` module reference in
    :mod:`srunx.observability.monitoring.base` with a virtual clock.

    Returned object exposes ``.now`` (current virtual time) so tests
    can read elapsed virtual time without a real wall-clock sample.
    """
    import srunx.observability.monitoring.base as _mon

    class _FakeTime:
        now: float = 0.0

        @classmethod
        def time(cls) -> float:
            return cls.now

        @classmethod
        def sleep(cls, seconds: float) -> None:
            cls.now += seconds

    monkeypatch.setattr(_mon, "time", _FakeTime)
    return _FakeTime


class TestJobMonitorTimeout:
    """Test JobMonitor timeout behavior."""

    def test_watch_until_respects_timeout(self, fake_monitor_clock):
        """Test that watch_until raises TimeoutError after timeout expires."""
        config = MonitorConfig(poll_interval=1, timeout=2)
        monitor = JobMonitor(job_ids=[123], config=config)

        # Mock job that never completes
        job = Job(name="job1", job_id=123, command=["test"])
        job._status = JobStatus.RUNNING
        monitor.client = MagicMock()
        monitor.client.status = MagicMock(return_value=job)

        with pytest.raises(TimeoutError):
            monitor.watch_until()

        # Sequence: t=0 check_condition false, sleep(1) → t=1; check
        # false, sleep(1) → t=2; check false, elapsed (2) >= timeout (2),
        # raise. So virtual now lands exactly at the timeout.
        assert fake_monitor_clock.now == pytest.approx(2.0)

    def test_watch_until_exits_before_timeout_on_completion(self, fake_monitor_clock):
        """Test that watch_until exits immediately when condition met."""
        config = MonitorConfig(poll_interval=1, timeout=10)
        monitor = JobMonitor(job_ids=[123], config=config)

        # Mock job that completes immediately
        job = Job(name="job1", job_id=123, command=["test"])
        job._status = JobStatus.COMPLETED
        monitor.client = MagicMock()
        monitor.client.status = MagicMock(return_value=job)

        monitor.watch_until()

        # Condition met on first check_condition call → no sleep, no
        # timeout check, virtual clock untouched.
        assert fake_monitor_clock.now == pytest.approx(0.0)

    def test_watch_until_no_timeout_waits_indefinitely(self, fake_monitor_clock):
        """Test that watch_until with no timeout can be stopped by signal."""
        config = MonitorConfig(poll_interval=1, timeout=None)
        monitor = JobMonitor(job_ids=[123], config=config)

        # Mock job that never completes
        job = Job(name="job1", job_id=123, command=["test"])
        job._status = JobStatus.RUNNING
        monitor.client = MagicMock()

        call_count = 0

        def mock_status(job_id):
            nonlocal call_count
            call_count += 1
            if call_count >= 3:
                # Stop after 3 polls to prevent infinite loop
                monitor._stop_requested = True
            return job

        monitor.client.status = mock_status

        monitor.watch_until()

        # Loop body order: check_condition → (timeout check) → sleep.
        # Stop is set inside the 3rd check; the loop still falls through
        # to sleep before the guard re-evaluates. So 3 polls + 3 sleeps.
        assert call_count == 3
        assert fake_monitor_clock.now == pytest.approx(3.0)

    def test_timeout_one_second_exits_quickly(self, fake_monitor_clock):
        """Test that timeout=1 causes quick exit."""
        config = MonitorConfig(poll_interval=1, timeout=1)
        monitor = JobMonitor(job_ids=[123], config=config)

        job = Job(name="job1", job_id=123, command=["test"])
        job._status = JobStatus.RUNNING
        monitor.client = MagicMock()
        monitor.client.status = MagicMock(return_value=job)

        with pytest.raises(TimeoutError):
            monitor.watch_until()

        # t=0 check false, sleep(1) → t=1; check false, elapsed (1) >=
        # timeout (1), raise. Virtual now = 1.0 exactly.
        assert fake_monitor_clock.now == pytest.approx(1.0)


class TestResourceMonitorTimeout:
    """Test ResourceMonitor timeout behavior."""

    def test_watch_until_respects_timeout(self, fake_monitor_clock):
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

        with pytest.raises(TimeoutError):
            monitor.watch_until()

        assert fake_monitor_clock.now == pytest.approx(2.0)

    def test_watch_until_exits_before_timeout_on_availability(self, fake_monitor_clock):
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

        monitor.watch_until()

        # Condition met on first poll → no sleep advance.
        assert fake_monitor_clock.now == pytest.approx(0.0)

    def test_watch_until_no_timeout_waits_indefinitely(self, fake_monitor_clock):
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

        monitor.watch_until()

        assert call_count == 3
        # 3 polls + 3 sleeps (sleep fires after the stop-setting poll
        # before the loop guard re-evaluates).
        assert fake_monitor_clock.now == pytest.approx(3.0)


class TestContinuousModeTimeout:
    """Test timeout behavior in continuous monitoring mode."""

    def test_continuous_mode_ignores_timeout(self, fake_monitor_clock):
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

        def mock_status(job_id):
            nonlocal call_count
            call_count += 1
            if call_count >= 5:
                # Stop after 5 polls (would be 2s timeout if respected)
                monitor._stop_requested = True
            return job

        monitor.client.status = mock_status

        monitor.watch_continuous()

        # 5 polls completed despite the 2s "timeout" — proves timeout
        # is ignored in CONTINUOUS mode. 5 sleeps (sleep fires after
        # the stop-setting poll before the loop guard re-evaluates).
        assert call_count == 5
        assert fake_monitor_clock.now == pytest.approx(5.0)


class TestPollIntervalTiming:
    """Test that poll_interval is respected."""

    def test_poll_interval_timing(self, fake_monitor_clock):
        """Test that monitor waits poll_interval between checks."""
        config = MonitorConfig(poll_interval=2, timeout=None)
        monitor = JobMonitor(job_ids=[123], config=config)

        job = Job(name="job1", job_id=123, command=["test"])
        job._status = JobStatus.RUNNING
        monitor.client = MagicMock()

        poll_times: list[float] = []

        def mock_status(job_id):
            poll_times.append(fake_monitor_clock.now)
            if len(poll_times) >= 3:
                monitor._stop_requested = True
            return job

        monitor.client.status = mock_status

        monitor.watch_until()

        # 3 polls separated by exactly poll_interval=2 seconds.
        assert poll_times == [
            pytest.approx(0.0),
            pytest.approx(2.0),
            pytest.approx(4.0),
        ]

    def test_fast_poll_interval(self, fake_monitor_clock):
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

        monitor.watch_until()

        # 4 polls + 4 sleeps (sleep fires after the stop-setting poll
        # before the loop guard re-evaluates).
        assert call_count == 4
        assert fake_monitor_clock.now == pytest.approx(4.0)
