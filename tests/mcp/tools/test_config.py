"""Tests for srunx.mcp.tools.config."""

from unittest.mock import MagicMock, patch

from srunx.mcp.tools.config import get_config, list_ssh_profiles


class TestGetConfig:
    """Test get_config tool."""

    @patch("srunx.common.config.get_config")
    def test_returns_config(self, mock_get_cfg):
        mock_config = MagicMock()
        mock_config.resources.model_dump.return_value = {
            "nodes": 1,
            "gpus_per_node": 0,
        }
        mock_config.environment.conda = "ml_env"
        mock_config.environment.venv = None
        mock_config.environment.env_vars = {}
        mock_config.log_dir = "logs"
        mock_config.work_dir = None
        mock_get_cfg.return_value = mock_config

        result = get_config()
        assert result["success"] is True
        assert result["resources"]["nodes"] == 1
        assert result["environment"]["conda"] == "ml_env"
        assert result["log_dir"] == "logs"

    def test_catches_exception(self):
        with patch(
            "srunx.common.config.get_config",
            side_effect=RuntimeError("config broken"),
        ):
            result = get_config()
            assert result["success"] is False
            assert "config broken" in result["error"]


class TestListSshProfiles:
    """Test list_ssh_profiles tool."""

    @patch("srunx.ssh.core.config.ConfigManager")
    def test_returns_profiles(self, mock_cm_cls):
        mock_cm = MagicMock()
        mock_profile = MagicMock()
        mock_profile.hostname = "dgx.example.com"
        mock_profile.username = "researcher"
        mock_profile.port = 22
        mock_profile.description = "DGX server"
        mock_mount = MagicMock()
        mock_mount.name = "project"
        mock_mount.local = "/home/user/project"
        mock_mount.remote = "/remote/project"
        mock_profile.mounts = [mock_mount]

        mock_cm.list_profiles.return_value = {"dgx": mock_profile}
        mock_cm.get_current_profile_name.return_value = "dgx"
        mock_cm_cls.return_value = mock_cm

        result = list_ssh_profiles()
        assert result["success"] is True
        assert result["count"] == 1
        assert result["current"] == "dgx"
        assert result["profiles"][0]["name"] == "dgx"
        assert result["profiles"][0]["hostname"] == "dgx.example.com"
        assert result["profiles"][0]["is_current"] is True
        assert len(result["profiles"][0]["mounts"]) == 1

    @patch("srunx.ssh.core.config.ConfigManager")
    def test_empty_profiles(self, mock_cm_cls):
        mock_cm = MagicMock()
        mock_cm.list_profiles.return_value = {}
        mock_cm.get_current_profile_name.return_value = None
        mock_cm_cls.return_value = mock_cm

        result = list_ssh_profiles()
        assert result["success"] is True
        assert result["count"] == 0
        assert result["profiles"] == []

    def test_catches_exception(self):
        with patch(
            "srunx.ssh.core.config.ConfigManager",
            side_effect=RuntimeError("config error"),
        ):
            result = list_ssh_profiles()
            assert result["success"] is False
            assert "config error" in result["error"]
