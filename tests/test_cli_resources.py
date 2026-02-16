"""Tests for resources command CLI."""

import json
from unittest.mock import MagicMock, patch

import pytest
from typer.testing import CliRunner

from srunx.cli.main import app
from srunx.monitor.types import ResourceSnapshot


@pytest.fixture
def runner():
    """Create CLI test runner."""
    return CliRunner()


@pytest.fixture
def mock_snapshot():
    """Create mock resource snapshot."""
    return ResourceSnapshot(
        partition="gpu",
        total_gpus=16,
        gpus_in_use=10,
        gpus_available=6,
        jobs_running=3,
        nodes_total=4,
        nodes_idle=1,
        nodes_down=0,
    )


@pytest.fixture
def mock_snapshot_all_partitions():
    """Create mock resource snapshot for all partitions."""
    return ResourceSnapshot(
        partition=None,
        total_gpus=32,
        gpus_in_use=20,
        gpus_available=12,
        jobs_running=5,
        nodes_total=8,
        nodes_idle=2,
        nodes_down=1,
    )


class TestResourcesCommand:
    """Test suite for resources command."""

    def test_resources_table_format_default(self, runner, mock_snapshot_all_partitions):
        """Test resources command with default table format."""
        with patch(
            "srunx.monitor.resource_monitor.ResourceMonitor"
        ) as mock_monitor_class:
            mock_monitor = MagicMock()
            mock_monitor.get_partition_resources.return_value = (
                mock_snapshot_all_partitions
            )
            mock_monitor_class.return_value = mock_monitor

            result = runner.invoke(app, ["resources"])

            assert result.exit_code == 0
            assert "GPU Resources" in result.stdout
            assert "all" in result.stdout  # "all partitions" in title
            assert "Total GPUs" in result.stdout
            assert "32" in result.stdout  # total_gpus
            assert "20" in result.stdout  # gpus_in_use
            assert "12" in result.stdout  # gpus_available
            assert "Running Jobs" in result.stdout
            assert "5" in result.stdout  # jobs_running
            assert "Total Nodes" in result.stdout
            assert "8" in result.stdout  # nodes_total
            assert "Idle Nodes" in result.stdout
            assert "2" in result.stdout  # nodes_idle
            assert "Down Nodes" in result.stdout
            assert "1" in result.stdout  # nodes_down

    def test_resources_table_format_with_partition(self, runner, mock_snapshot):
        """Test resources command with specific partition."""
        with patch(
            "srunx.monitor.resource_monitor.ResourceMonitor"
        ) as mock_monitor_class:
            mock_monitor = MagicMock()
            mock_monitor.get_partition_resources.return_value = mock_snapshot
            mock_monitor_class.return_value = mock_monitor

            result = runner.invoke(app, ["resources", "--partition", "gpu"])

            assert result.exit_code == 0
            assert "GPU Resources - gpu" in result.stdout
            assert "16" in result.stdout  # total_gpus
            assert "10" in result.stdout  # gpus_in_use
            assert "6" in result.stdout  # gpus_available
            assert "3" in result.stdout  # jobs_running
            assert "4" in result.stdout  # nodes_total
            assert "1" in result.stdout  # nodes_idle
            assert "0" in result.stdout  # nodes_down

            # Verify ResourceMonitor was created with correct partition
            mock_monitor_class.assert_called_once_with(min_gpus=0, partition="gpu")

    def test_resources_json_format_default(self, runner, mock_snapshot_all_partitions):
        """Test resources command with JSON format."""
        with patch(
            "srunx.monitor.resource_monitor.ResourceMonitor"
        ) as mock_monitor_class:
            mock_monitor = MagicMock()
            mock_monitor.get_partition_resources.return_value = (
                mock_snapshot_all_partitions
            )
            mock_monitor_class.return_value = mock_monitor

            result = runner.invoke(app, ["resources", "--format", "json"])

            assert result.exit_code == 0
            data = json.loads(result.stdout)

            assert data["partition"] is None
            assert data["gpus_total"] == 32
            assert data["gpus_in_use"] == 20
            assert data["gpus_available"] == 12
            assert data["jobs_running"] == 5
            assert data["nodes_total"] == 8
            assert data["nodes_idle"] == 2
            assert data["nodes_down"] == 1

    def test_resources_json_format_with_partition(self, runner, mock_snapshot):
        """Test resources command with JSON format and partition."""
        with patch(
            "srunx.monitor.resource_monitor.ResourceMonitor"
        ) as mock_monitor_class:
            mock_monitor = MagicMock()
            mock_monitor.get_partition_resources.return_value = mock_snapshot
            mock_monitor_class.return_value = mock_monitor

            result = runner.invoke(app, ["resources", "-p", "gpu", "-f", "json"])

            assert result.exit_code == 0
            data = json.loads(result.stdout)

            assert data["partition"] == "gpu"
            assert data["gpus_total"] == 16
            assert data["gpus_in_use"] == 10
            assert data["gpus_available"] == 6
            assert data["jobs_running"] == 3
            assert data["nodes_total"] == 4
            assert data["nodes_idle"] == 1
            assert data["nodes_down"] == 0

            # Verify ResourceMonitor was created with correct partition
            mock_monitor_class.assert_called_once_with(min_gpus=0, partition="gpu")

    def test_resources_zero_gpus_available(self, runner):
        """Test resources command when no GPUs available."""
        snapshot = ResourceSnapshot(
            partition="gpu",
            total_gpus=16,
            gpus_in_use=16,
            gpus_available=0,
            jobs_running=8,
            nodes_total=4,
            nodes_idle=0,
            nodes_down=0,
        )

        with patch(
            "srunx.monitor.resource_monitor.ResourceMonitor"
        ) as mock_monitor_class:
            mock_monitor = MagicMock()
            mock_monitor.get_partition_resources.return_value = snapshot
            mock_monitor_class.return_value = mock_monitor

            result = runner.invoke(app, ["resources", "--format", "json"])

            assert result.exit_code == 0
            data = json.loads(result.stdout)
            assert data["gpus_available"] == 0
            assert data["gpus_total"] == data["gpus_in_use"]

    def test_resources_with_down_nodes(self, runner):
        """Test resources command with some nodes down."""
        snapshot = ResourceSnapshot(
            partition="gpu",
            total_gpus=8,  # Only from available nodes
            gpus_in_use=4,
            gpus_available=4,
            jobs_running=2,
            nodes_total=4,
            nodes_idle=1,
            nodes_down=2,  # 2 nodes down
        )

        with patch(
            "srunx.monitor.resource_monitor.ResourceMonitor"
        ) as mock_monitor_class:
            mock_monitor = MagicMock()
            mock_monitor.get_partition_resources.return_value = snapshot
            mock_monitor_class.return_value = mock_monitor

            result = runner.invoke(app, ["resources", "--format", "json"])

            assert result.exit_code == 0
            data = json.loads(result.stdout)
            assert data["nodes_total"] == 4
            assert data["nodes_down"] == 2
            # Total GPUs should only count available nodes
            assert data["gpus_total"] == 8

    def test_resources_error_handling(self, runner):
        """Test resources command error handling."""
        with patch(
            "srunx.monitor.resource_monitor.ResourceMonitor"
        ) as mock_monitor_class:
            mock_monitor = MagicMock()
            mock_monitor.get_partition_resources.side_effect = Exception(
                "SLURM connection error"
            )
            mock_monitor_class.return_value = mock_monitor

            result = runner.invoke(app, ["resources"])

            assert result.exit_code == 1
            assert "Error" in result.stdout

    def test_resources_partition_not_found(self, runner):
        """Test resources command with non-existent partition."""
        # ResourceMonitor will return zeros for non-existent partition
        snapshot = ResourceSnapshot(
            partition="nonexistent",
            total_gpus=0,
            gpus_in_use=0,
            gpus_available=0,
            jobs_running=0,
            nodes_total=0,
            nodes_idle=0,
            nodes_down=0,
        )

        with patch(
            "srunx.monitor.resource_monitor.ResourceMonitor"
        ) as mock_monitor_class:
            mock_monitor = MagicMock()
            mock_monitor.get_partition_resources.return_value = snapshot
            mock_monitor_class.return_value = mock_monitor

            result = runner.invoke(
                app, ["resources", "-p", "nonexistent", "-f", "json"]
            )

            assert result.exit_code == 0
            data = json.loads(result.stdout)
            # Should return zeros for non-existent partition
            assert data["gpus_total"] == 0
            assert data["nodes_total"] == 0

    def test_resources_short_flags(self, runner, mock_snapshot):
        """Test resources command with short flag variants."""
        with patch(
            "srunx.monitor.resource_monitor.ResourceMonitor"
        ) as mock_monitor_class:
            mock_monitor = MagicMock()
            mock_monitor.get_partition_resources.return_value = mock_snapshot
            mock_monitor_class.return_value = mock_monitor

            # Test -p and -f short flags
            result = runner.invoke(app, ["resources", "-p", "gpu", "-f", "json"])

            assert result.exit_code == 0
            data = json.loads(result.stdout)
            assert data["partition"] == "gpu"

            # Verify ResourceMonitor was created correctly
            mock_monitor_class.assert_called_once_with(min_gpus=0, partition="gpu")

    def test_resources_high_utilization(self, runner):
        """Test resources command with high GPU utilization."""
        snapshot = ResourceSnapshot(
            partition="gpu",
            total_gpus=100,
            gpus_in_use=95,
            gpus_available=5,
            jobs_running=20,
            nodes_total=25,
            nodes_idle=1,
            nodes_down=0,
        )

        with patch(
            "srunx.monitor.resource_monitor.ResourceMonitor"
        ) as mock_monitor_class:
            mock_monitor = MagicMock()
            mock_monitor.get_partition_resources.return_value = snapshot
            mock_monitor_class.return_value = mock_monitor

            result = runner.invoke(app, ["resources", "--format", "json"])

            assert result.exit_code == 0
            data = json.loads(result.stdout)
            # 95% utilization
            assert data["gpus_in_use"] / data["gpus_total"] == 0.95
            assert data["gpus_available"] == 5
