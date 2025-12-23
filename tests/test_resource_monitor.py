"""Tests for ResourceMonitor class."""

import subprocess
from unittest.mock import MagicMock, patch

import pytest

from srunx.monitor.resource_monitor import ResourceMonitor
from srunx.monitor.types import MonitorConfig, ResourceSnapshot


class TestResourceMonitor:
    """Test suite for ResourceMonitor."""

    def test_init_default_params(self):
        """Test initialization with default parameters."""
        monitor = ResourceMonitor(min_gpus=2)
        assert monitor.min_gpus == 2
        assert monitor.partition is None
        assert monitor._was_available is None  # Uninitialized until first check

    def test_init_with_partition(self):
        """Test initialization with specific partition."""
        monitor = ResourceMonitor(min_gpus=4, partition="gpu")
        assert monitor.min_gpus == 4
        assert monitor.partition == "gpu"

    def test_init_negative_min_gpus_raises_error(self):
        """Test that negative min_gpus raises ValueError."""
        with pytest.raises(ValueError, match="min_gpus must be >= 0"):
            ResourceMonitor(min_gpus=-1)

    def test_check_condition_threshold_met(self):
        """Test check_condition when threshold is met."""
        monitor = ResourceMonitor(min_gpus=2)

        # Mock get_partition_resources to return snapshot with enough GPUs
        snapshot = ResourceSnapshot(
            partition=None,
            total_gpus=10,
            gpus_in_use=6,
            gpus_available=4,
            jobs_running=3,
            nodes_total=4,
            nodes_idle=2,
        )
        monitor.get_partition_resources = MagicMock(return_value=snapshot)

        assert monitor.check_condition() is True

    def test_check_condition_threshold_not_met(self):
        """Test check_condition when threshold is not met."""
        monitor = ResourceMonitor(min_gpus=5)

        # Mock get_partition_resources to return snapshot with insufficient GPUs
        snapshot = ResourceSnapshot(
            partition=None,
            total_gpus=10,
            gpus_in_use=8,
            gpus_available=2,
            jobs_running=4,
            nodes_total=4,
            nodes_idle=0,
        )
        monitor.get_partition_resources = MagicMock(return_value=snapshot)

        assert monitor.check_condition() is False

    def test_get_current_state(self):
        """Test get_current_state returns correct state dictionary."""
        monitor = ResourceMonitor(min_gpus=3)

        # Mock get_partition_resources
        snapshot = ResourceSnapshot(
            partition="gpu",
            total_gpus=8,
            gpus_in_use=4,
            gpus_available=4,
            jobs_running=2,
            nodes_total=2,
            nodes_idle=1,
        )
        monitor.get_partition_resources = MagicMock(return_value=snapshot)

        state = monitor.get_current_state()
        assert state == {
            "partition": "gpu",
            "gpus_available": 4,
            "gpus_total": 8,
            "meets_threshold": True,  # 4 >= 3
        }

    @patch("subprocess.run")
    def test_get_node_stats_with_partition(self, mock_run):
        """Test _get_node_stats with specific partition."""
        mock_run.return_value = MagicMock(
            stdout="node01 gpu:4 idle\nnode02 gpu:4 idle\n",
            returncode=0,
        )

        monitor = ResourceMonitor(min_gpus=2, partition="gpu")
        nodes_total, nodes_idle, nodes_down, total_gpus = monitor._get_node_stats()

        assert nodes_total == 2
        assert nodes_idle == 2
        assert nodes_down == 0
        assert total_gpus == 8
        # Verify command includes partition filter
        mock_run.assert_called_once()
        cmd = mock_run.call_args[0][0]
        assert "-p" in cmd
        assert "gpu" in cmd

    @patch("subprocess.run")
    def test_get_node_stats_excludes_down_nodes(self, mock_run):
        """Test _get_node_stats excludes DOWN/DRAIN nodes from GPU count."""
        mock_run.return_value = MagicMock(
            stdout="node01 gpu:4 idle\nnode02 gpu:4 down\nnode03 gpu:4 drain\n",
            returncode=0,
        )

        monitor = ResourceMonitor(min_gpus=2)
        nodes_total, nodes_idle, nodes_down, total_gpus = monitor._get_node_stats()

        assert nodes_total == 3
        assert nodes_idle == 1
        assert nodes_down == 2
        # Only node01 GPUs should be counted
        assert total_gpus == 4

    @patch("subprocess.run")
    def test_get_node_stats_handles_timeout(self, mock_run):
        """Test _get_node_stats handles timeout gracefully."""
        mock_run.side_effect = subprocess.TimeoutExpired("sinfo", 30)

        monitor = ResourceMonitor(min_gpus=2)
        nodes_total, nodes_idle, nodes_down, total_gpus = monitor._get_node_stats()

        assert nodes_total == 0
        assert nodes_idle == 0
        assert nodes_down == 0
        assert total_gpus == 0

    @patch("subprocess.run")
    def test_get_gpu_usage(self, mock_run):
        """Test _get_gpu_usage counts RUNNING jobs only."""
        mock_run.return_value = MagicMock(
            stdout="123 RUNNING gpu:2\n456 PENDING gpu:4\n789 RUNNING gpu:1\n",
            returncode=0,
        )

        monitor = ResourceMonitor(min_gpus=2)
        gpus_in_use, jobs_running = monitor._get_gpu_usage()

        # Only RUNNING jobs: 2 + 1 = 3 GPUs, 2 jobs
        assert gpus_in_use == 3
        assert jobs_running == 2

    @patch("subprocess.run")
    def test_get_gpu_usage_with_partition(self, mock_run):
        """Test _get_gpu_usage with specific partition."""
        mock_run.return_value = MagicMock(
            stdout="123 RUNNING gpu:2\n",
            returncode=0,
        )

        monitor = ResourceMonitor(min_gpus=2, partition="gpu")
        gpus_in_use, jobs_running = monitor._get_gpu_usage()

        assert gpus_in_use == 2
        assert jobs_running == 1
        # Verify command includes partition filter
        mock_run.assert_called_once()
        cmd = mock_run.call_args[0][0]
        assert "-p" in cmd
        assert "gpu" in cmd

    def test_notify_callbacks_availability_change(self):
        """Test _notify_callbacks notifies on availability transitions."""
        monitor = ResourceMonitor(min_gpus=2)

        # Create mock callback
        callback = MagicMock()
        monitor.callbacks = [callback]

        # First call: resources available (initializes by setting opposite, then detects transition)
        snapshot_available = ResourceSnapshot(
            partition=None,
            total_gpus=10,
            gpus_in_use=6,
            gpus_available=4,
            jobs_running=3,
            nodes_total=4,
            nodes_idle=2,
        )
        monitor.get_partition_resources = MagicMock(return_value=snapshot_available)

        monitor._notify_callbacks("state_changed")
        assert callback.on_resources_available.call_count == 1  # Notifies on first call

        # Second call: still available (should not notify)
        monitor._notify_callbacks("state_changed")
        assert callback.on_resources_available.call_count == 1

        # Third call: resources exhausted (transition detected, should notify)
        snapshot_exhausted = ResourceSnapshot(
            partition=None,
            total_gpus=10,
            gpus_in_use=9,
            gpus_available=1,
            jobs_running=5,
            nodes_total=4,
            nodes_idle=0,
        )
        monitor.get_partition_resources = MagicMock(return_value=snapshot_exhausted)

        monitor._notify_callbacks("state_changed")
        assert callback.on_resources_exhausted.call_count == 1

    def test_notify_callbacks_handles_callback_errors(self):
        """Test _notify_callbacks handles callback exceptions gracefully."""
        monitor = ResourceMonitor(min_gpus=2)

        # Create mock callback that raises exception
        callback = MagicMock()
        callback.on_resources_available.side_effect = Exception("Callback error")
        monitor.callbacks = [callback]

        # Mock get_partition_resources
        snapshot = ResourceSnapshot(
            partition=None,
            total_gpus=10,
            gpus_in_use=6,
            gpus_available=4,
            jobs_running=3,
            nodes_total=4,
            nodes_idle=2,
        )
        monitor.get_partition_resources = MagicMock(return_value=snapshot)

        # Should not raise exception
        monitor._notify_callbacks("state_changed")

    @patch("subprocess.run")
    def test_get_partition_resources_integration(self, mock_run):
        """Test get_partition_resources integrates sinfo and squeue correctly."""
        # Mock sinfo: 2 nodes, 2 idle, 0 down, 16 total GPUs
        # Mock squeue: 10 GPUs in use, 2 jobs running
        mock_run.side_effect = [
            MagicMock(
                stdout="node01 gpu:8 idle\nnode02 gpu:8 idle\n",
                returncode=0,
            ),
            MagicMock(
                stdout="123 RUNNING gpu:6\n456 RUNNING gpu:4\n",
                returncode=0,
            ),
        ]

        monitor = ResourceMonitor(min_gpus=4)
        snapshot = monitor.get_partition_resources()

        assert snapshot.total_gpus == 16
        assert snapshot.gpus_in_use == 10
        assert snapshot.gpus_available == 6
        assert snapshot.jobs_running == 2
        assert snapshot.nodes_total == 2
        assert snapshot.nodes_idle == 2
        assert snapshot.nodes_down == 0
        assert snapshot.meets_threshold(4) is True

    def test_watch_continuous_resource_changes(self):
        """Test watch_continuous notifies on resource availability changes."""
        import threading

        from srunx.monitor.types import WatchMode

        config = MonitorConfig(
            poll_interval=1,
            mode=WatchMode.CONTINUOUS,
            notify_on_change=True,
        )
        monitor = ResourceMonitor(min_gpus=4, config=config)

        # Create mock callback
        callback = MagicMock()
        monitor.callbacks = [callback]

        # Mock subprocess with changing availability
        call_count = 0

        def mock_run(cmd, **kwargs):
            nonlocal call_count
            call_count += 1

            if "sinfo" in cmd:
                # Always return same node stats
                return MagicMock(
                    stdout="node01 gpu:8 idle\nnode02 gpu:8 idle\n",
                    returncode=0,
                )
            elif "squeue" in cmd:
                # Change GPU usage to simulate availability changes
                if call_count <= 2:
                    # Initially 14 GPUs in use (2 available, below threshold)
                    return MagicMock(
                        stdout="123 RUNNING gpu:14\n",
                        returncode=0,
                    )
                elif call_count <= 4:
                    # Then 10 GPUs in use (6 available, above threshold)
                    return MagicMock(
                        stdout="123 RUNNING gpu:10\n",
                        returncode=0,
                    )
                else:
                    # Stop monitoring
                    monitor._stop_requested = True
                    return MagicMock(
                        stdout="123 RUNNING gpu:10\n",
                        returncode=0,
                    )

        with patch("subprocess.run", side_effect=mock_run):
            # Run watch_continuous in thread
            thread = threading.Thread(target=monitor.watch_continuous)
            thread.start()
            thread.join(timeout=10.0)

            # Should have notified when resources became available
            assert callback.on_resources_available.call_count >= 1

    def test_watch_continuous_duplicate_notification_prevention(self):
        """Test watch_continuous prevents duplicate notifications."""
        import threading

        from srunx.monitor.types import WatchMode

        config = MonitorConfig(
            poll_interval=1,
            mode=WatchMode.CONTINUOUS,
            notify_on_change=True,
        )
        monitor = ResourceMonitor(min_gpus=4, config=config)

        # Create mock callback
        callback = MagicMock()
        monitor.callbacks = [callback]

        # Mock subprocess with consistent availability (always above threshold)
        call_count = 0

        def mock_run(cmd, **kwargs):
            nonlocal call_count
            call_count += 1

            if "sinfo" in cmd:
                return MagicMock(
                    stdout="node01 gpu:8 idle\n",
                    returncode=0,
                )
            elif "squeue" in cmd:
                # Always 2 GPUs in use (6 available, always above threshold)
                if call_count >= 10:
                    monitor._stop_requested = True
                return MagicMock(
                    stdout="123 RUNNING gpu:2\n",
                    returncode=0,
                )

        with patch("subprocess.run", side_effect=mock_run):
            # Run watch_continuous in thread
            thread = threading.Thread(target=monitor.watch_continuous)
            thread.start()
            thread.join(timeout=15.0)

            # Should not notify since availability never changes
            # (first state is ignored, no transition)
            assert callback.on_resources_available.call_count == 0
            assert callback.on_resources_exhausted.call_count == 0

    def test_watch_continuous_keyboard_interrupt(self):
        """Test watch_continuous handles KeyboardInterrupt gracefully."""
        import threading
        import time

        from srunx.monitor.types import WatchMode

        config = MonitorConfig(
            poll_interval=1,
            mode=WatchMode.CONTINUOUS,
        )
        monitor = ResourceMonitor(min_gpus=2, config=config)

        # Mock subprocess
        def mock_run(cmd, **kwargs):
            if "sinfo" in cmd:
                return MagicMock(
                    stdout="node01 gpu:4 idle\n",
                    returncode=0,
                )
            elif "squeue" in cmd:
                return MagicMock(
                    stdout="123 RUNNING gpu:1\n",
                    returncode=0,
                )

        with patch("subprocess.run", side_effect=mock_run):
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
            # Verify stop flag is set
            assert monitor._stop_requested is True

    @patch("subprocess.run")
    def test_get_node_stats_unparseable_gpu_count(self, mock_run):
        """Test _get_node_stats handles unparseable GPU count in gres."""
        # Node has "gpu" in gres but count cannot be parsed
        mock_run.return_value = MagicMock(
            stdout="node01 gpu:unknown idle\nnode02 gpu:nvidia idle\n",
            returncode=0,
        )

        monitor = ResourceMonitor(min_gpus=2)
        nodes_total, nodes_idle, nodes_down, total_gpus = monitor._get_node_stats()

        # Both nodes counted, but no GPUs since format is unparseable
        assert nodes_total == 2
        assert nodes_idle == 2
        assert nodes_down == 0
        assert total_gpus == 0

    @patch("subprocess.run")
    def test_get_node_stats_nonexistent_partition(self, mock_run):
        """Test _get_node_stats handles nonexistent partition gracefully."""
        # Simulate empty output when partition doesn't exist
        mock_run.return_value = MagicMock(
            stdout="",
            returncode=0,
        )

        monitor = ResourceMonitor(min_gpus=2, partition="nonexistent")
        nodes_total, nodes_idle, nodes_down, total_gpus = monitor._get_node_stats()

        # Should return zeros without crashing
        assert nodes_total == 0
        assert nodes_idle == 0
        assert nodes_down == 0
        assert total_gpus == 0
