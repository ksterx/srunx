"""Tests for srunx.mcp.tools.resources."""

from unittest.mock import MagicMock, patch

from srunx.mcp.tools.resources import get_resources


class TestGetResources:
    """Test get_resources tool."""

    def test_invalid_partition(self):
        result = get_resources(partition="gpu; whoami")
        assert result["success"] is False
        assert "Invalid partition name" in result["error"]

    @patch("srunx.observability.monitoring.resource_monitor.ResourceMonitor")
    def test_local_resources(self, mock_monitor_cls):
        mock_monitor = MagicMock()
        snapshot = MagicMock()
        snapshot.partition = "gpu"
        snapshot.total_gpus = 8
        snapshot.gpus_in_use = 3
        snapshot.gpus_available = 5
        snapshot.gpu_utilization = 0.375
        snapshot.jobs_running = 2
        snapshot.nodes_total = 4
        snapshot.nodes_idle = 2
        snapshot.nodes_down = 0
        mock_monitor.get_partition_resources.return_value = snapshot
        mock_monitor_cls.return_value = mock_monitor

        result = get_resources(partition="gpu")
        assert result["success"] is True
        assert result["total_gpus"] == 8
        assert result["gpus_available"] == 5
        assert result["gpu_utilization"] == 0.375

    @patch("srunx.mcp.tools.resources.get_ssh_client")
    def test_ssh_resources(self, mock_get_client):
        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client._execute_slurm_command.return_value = (
            "node001 gpu:4 idle gpu*\n",
            "",
            0,
        )
        mock_get_client.return_value = mock_client

        result = get_resources(use_ssh=True)
        assert result["success"] is True
        assert "node001" in result["raw_output"]

    @patch("srunx.mcp.tools.resources.get_ssh_client")
    def test_ssh_resources_sinfo_fails(self, mock_get_client):
        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client._execute_slurm_command.return_value = ("", "sinfo error", 1)
        mock_get_client.return_value = mock_client

        result = get_resources(use_ssh=True)
        assert result["success"] is False
        assert "sinfo failed" in result["error"]
