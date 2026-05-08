"""Tests for srunx.mcp.tools.sync."""

from unittest.mock import MagicMock, patch

from srunx.mcp.tools.sync import sync_files


class TestSyncFiles:
    """Test sync_files tool."""

    def test_no_arguments_returns_error(self):
        """Neither mount_name nor local_path → error."""
        result = sync_files()
        assert result["success"] is False
        assert "mount_name or local_path" in result["error"]

    @patch("srunx.ssh.core.config.ConfigManager")
    def test_mount_no_current_profile(self, mock_cm_cls):
        """mount_name set but no current profile + no profile_name → error."""
        mock_cm = MagicMock()
        mock_cm.get_current_profile_name.return_value = None
        mock_cm_cls.return_value = mock_cm

        result = sync_files(mount_name="ml")
        assert result["success"] is False
        assert "No SSH profile" in result["error"]

    @patch("srunx.ssh.core.config.ConfigManager")
    def test_mount_unknown_profile(self, mock_cm_cls):
        """profile_name doesn't resolve → error."""
        mock_cm = MagicMock()
        mock_cm.get_current_profile_name.return_value = None
        mock_cm.get_profile.return_value = None
        mock_cm_cls.return_value = mock_cm

        result = sync_files(profile_name="missing", mount_name="ml")
        assert result["success"] is False
        assert "missing" in result["error"]

    @patch("srunx.ssh.core.config.ConfigManager")
    def test_mount_unknown_mount_name(self, mock_cm_cls):
        """profile resolved but mount_name not in profile → error with available list."""
        mock_profile = MagicMock()
        existing = MagicMock()
        existing.name = "data"
        mock_profile.mounts = [existing]

        mock_cm = MagicMock()
        mock_cm.get_current_profile_name.return_value = "prod"
        mock_cm.get_profile.return_value = mock_profile
        mock_cm_cls.return_value = mock_cm

        result = sync_files(mount_name="missing")
        assert result["success"] is False
        assert "missing" in result["error"]
        assert "data" in result["error"]  # available mounts surfaced

    @patch("srunx.web.sync_utils.build_rsync_client")
    @patch("srunx.ssh.core.config.ConfigManager")
    def test_mount_success(self, mock_cm_cls, mock_build):
        """mount_name resolves + rsync succeeds → success payload."""
        mount = MagicMock()
        mount.name = "ml"
        mount.local = "/local/ml"
        mount.remote = "/remote/ml"
        mount.exclude_patterns = ["*.log"]

        profile = MagicMock()
        profile.mounts = [mount]

        cm = MagicMock()
        cm.get_current_profile_name.return_value = "prod"
        cm.get_profile.return_value = profile
        mock_cm_cls.return_value = cm

        rsync = MagicMock()
        rsync.push.return_value = MagicMock(
            success=True, returncode=0, stdout="sent 100 files", stderr=""
        )
        mock_build.return_value = rsync

        result = sync_files(mount_name="ml")
        assert result["success"] is True
        assert result["mount"] == "ml"
        assert result["local"] == "/local/ml"
        assert result["remote"] == "/remote/ml"
        assert result["dry_run"] is False
        assert "sent 100 files" in result["output"]

        # exclude_patterns + mount paths threaded through.
        rsync.push.assert_called_once_with(
            "/local/ml",
            "/remote/ml",
            dry_run=False,
            exclude_patterns=["*.log"],
        )

    @patch("srunx.web.sync_utils.build_rsync_client")
    @patch("srunx.ssh.core.config.ConfigManager")
    def test_mount_rsync_failure(self, mock_cm_cls, mock_build):
        """rsync non-zero exit → error payload with truncated stderr."""
        mount = MagicMock()
        mount.name = "ml"
        mount.local = "/l"
        mount.remote = "/r"
        mount.exclude_patterns = []

        profile = MagicMock()
        profile.mounts = [mount]

        cm = MagicMock()
        cm.get_current_profile_name.return_value = "prod"
        cm.get_profile.return_value = profile
        mock_cm_cls.return_value = cm

        rsync = MagicMock()
        rsync.push.return_value = MagicMock(
            success=False, returncode=23, stdout="", stderr="permission denied"
        )
        mock_build.return_value = rsync

        result = sync_files(mount_name="ml")
        assert result["success"] is False
        assert "exit 23" in result["error"]
        assert "permission denied" in result["error"]

    @patch("srunx.web.sync_utils.build_rsync_client")
    @patch("srunx.ssh.core.config.ConfigManager")
    def test_local_path_explicit_remote(self, mock_cm_cls, mock_build):
        """local_path + remote_path override mount lookup."""
        profile = MagicMock()
        cm = MagicMock()
        cm.get_current_profile_name.return_value = "prod"
        cm.get_profile.return_value = profile
        mock_cm_cls.return_value = cm

        rsync = MagicMock()
        rsync.push.return_value = MagicMock(
            success=True, returncode=0, stdout="ok", stderr=""
        )
        mock_build.return_value = rsync

        result = sync_files(local_path="/src", remote_path="/dst", dry_run=True)
        assert result["success"] is True
        assert result["local"] == "/src"
        assert result["remote"] == "/dst"
        assert result["dry_run"] is True
        rsync.push.assert_called_once_with("/src", "/dst", dry_run=True)

    @patch("srunx.web.sync_utils.build_rsync_client")
    @patch("srunx.ssh.core.config.ConfigManager")
    def test_local_path_default_remote(self, mock_cm_cls, mock_build):
        """remote_path omitted → falls back to RsyncClient.get_default_remote_path."""
        profile = MagicMock()
        cm = MagicMock()
        cm.get_current_profile_name.return_value = "prod"
        cm.get_profile.return_value = profile
        mock_cm_cls.return_value = cm

        rsync = MagicMock()
        rsync.push.return_value = MagicMock(
            success=True, returncode=0, stdout="", stderr=""
        )
        rsync.get_default_remote_path.return_value = "/remote/auto"
        mock_build.return_value = rsync

        result = sync_files(local_path="/src")
        assert result["success"] is True
        assert result["remote"] == "/remote/auto"
        rsync.get_default_remote_path.assert_called_once_with("/src")

    def test_catches_exception(self):
        """Any unexpected exception → error payload with the message."""
        with patch(
            "srunx.ssh.core.config.ConfigManager",
            side_effect=RuntimeError("config broken"),
        ):
            result = sync_files(mount_name="ml")
            assert result["success"] is False
            assert "config broken" in result["error"]
