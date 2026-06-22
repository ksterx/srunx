"""Tests for srunx.mcp.tools.resources.

The local branch still goes through ``ResourceMonitor`` (snapshot), so that
patch is preserved. The SSH branch routes through ``mcp_transport``: we patch
it at the tool's lookup site with a contextmanager yielding an ``rt`` whose
``transport_type == "ssh"`` and whose ``job_ops.get_resources`` returns the
adapter payload.
"""

from contextlib import contextmanager
from unittest.mock import MagicMock, patch

from srunx.mcp.tools.resources import get_resources


def _fake_transport(rt):
    @contextmanager
    def _cm(transport, *, mount_name=None):
        yield rt

    return _cm


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

    def test_ssh_resources(self):
        rt = MagicMock()
        rt.transport_type = "ssh"
        rt.job_ops.get_resources.return_value = {"gpu": {"total": 4, "available": 2}}
        with patch("srunx.mcp.tools.resources.mcp_transport", _fake_transport(rt)):
            result = get_resources(partition="gpu", transport="prod")
        assert result["success"] is True
        assert result["partition"] == "gpu"
        assert result["resources"] == {"gpu": {"total": 4, "available": 2}}
        rt.job_ops.get_resources.assert_called_once_with("gpu")

    def test_ssh_resources_no_partition(self):
        rt = MagicMock()
        rt.transport_type = "ssh"
        rt.job_ops.get_resources.return_value = {"all": {"total": 8}}
        with patch("srunx.mcp.tools.resources.mcp_transport", _fake_transport(rt)):
            result = get_resources(transport="prod")
        assert result["success"] is True
        assert result["partition"] is None
        rt.job_ops.get_resources.assert_called_once_with(None)
