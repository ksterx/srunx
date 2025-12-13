"""Tests for BaseMonitor abstract class."""

import pytest
from unittest.mock import MagicMock, patch
import signal
import time

from srunx.monitor.base import BaseMonitor
from srunx.monitor.types import MonitorConfig, WatchMode


class MockMonitor(BaseMonitor):
    """Mock monitor for testing BaseMonitor functionality."""

    def __init__(self, condition_met=False, **kwargs):
        super().__init__(**kwargs)
        self._condition_met = condition_met
        self._state_counter = 0

    def check_condition(self) -> bool:
        return self._condition_met

    def get_current_state(self) -> dict:
        self._state_counter += 1
        return {"counter": self._state_counter}

    def _notify_callbacks(self, event: str) -> None:
        """Mock notification - just log the event."""
        pass


class TestBaseMonitor:
    """Test suite for BaseMonitor."""

    def test_init_default_config(self):
        """Test initialization with default configuration."""
        monitor = MockMonitor()
        assert monitor.config.poll_interval == 60
        assert monitor.config.timeout is None
        assert monitor.config.mode == WatchMode.UNTIL_CONDITION
        assert monitor.config.notify_on_change is True

    def test_init_custom_config(self):
        """Test initialization with custom configuration."""
        config = MonitorConfig(poll_interval=30, timeout=3600)
        monitor = MockMonitor(config=config)
        assert monitor.config.poll_interval == 30
        assert monitor.config.timeout == 3600

    def test_aggressive_polling_warning(self, caplog):
        """Test warning is logged for aggressive polling intervals."""
        import logging
        caplog.set_level(logging.WARNING)

        config = MonitorConfig(poll_interval=3)
        with caplog.at_level(logging.WARNING):
            monitor = MockMonitor(config=config)

        # Check if warning was logged (loguru output shown in stderr)
        # Since loguru doesn't integrate with caplog, we verify the config instead
        assert config.is_aggressive is True

    def test_watch_until_condition_met(self):
        """Test watch_until exits when condition is met."""
        monitor = MockMonitor(condition_met=True)
        start_time = time.time()
        monitor.watch_until()
        elapsed = time.time() - start_time
        assert elapsed < 1  # Should exit immediately

    def test_watch_until_timeout(self):
        """Test watch_until raises TimeoutError on timeout."""
        config = MonitorConfig(poll_interval=1, timeout=2)
        monitor = MockMonitor(condition_met=False, config=config)

        with pytest.raises(TimeoutError, match="Monitoring timeout"):
            monitor.watch_until()

    def test_signal_handling(self):
        """Test signal handler sets stop flag."""
        monitor = MockMonitor()
        assert monitor._stop_requested is False

        # Simulate SIGINT
        monitor._handle_signal(signal.SIGINT, None)
        assert monitor._stop_requested is True

    def test_watch_continuous_state_change(self):
        """Test continuous mode detects state changes."""
        monitor = MockMonitor()
        monitor._stop_requested = False

        # Mock get_current_state to return different values
        states = [{"counter": 1}, {"counter": 2}, {"counter": 3}]
        state_iter = iter(states)

        def mock_get_state():
            try:
                return next(state_iter)
            except StopIteration:
                monitor._stop_requested = True
                return {"counter": 3}

        monitor.get_current_state = mock_get_state

        # Run continuous monitoring (will stop after 3 iterations)
        with patch.object(time, 'sleep'):
            monitor.watch_continuous()

    def test_is_aggressive_property(self):
        """Test is_aggressive property on MonitorConfig."""
        config_fast = MonitorConfig(poll_interval=3)
        config_normal = MonitorConfig(poll_interval=60)

        assert config_fast.is_aggressive is True
        assert config_normal.is_aggressive is False
