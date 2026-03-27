"""Tests for the rsync-based file synchronization module."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from srunx.sync.rsync import RsyncClient, RsyncResult

# ---------------------------------------------------------------------------
# RsyncResult
# ---------------------------------------------------------------------------


class TestRsyncResult:
    def test_success_on_zero_returncode(self):
        result = RsyncResult(returncode=0, stdout="done", stderr="")
        assert result.success is True

    def test_failure_on_nonzero_returncode(self):
        result = RsyncResult(returncode=1, stdout="", stderr="error")
        assert result.success is False

    def test_fields(self):
        result = RsyncResult(returncode=23, stdout="out", stderr="err")
        assert result.returncode == 23
        assert result.stdout == "out"
        assert result.stderr == "err"


# ---------------------------------------------------------------------------
# RsyncClient.__init__
# ---------------------------------------------------------------------------


class TestRsyncClientInit:
    @patch("srunx.sync.rsync.shutil.which", return_value=None)
    def test_raises_when_rsync_missing(self, mock_which: MagicMock):
        with pytest.raises(RuntimeError, match="rsync is not installed"):
            RsyncClient(hostname="host", username="user")

    @patch("srunx.sync.rsync.shutil.which", return_value="/usr/bin/rsync")
    def test_stores_params(self, mock_which: MagicMock):
        client = RsyncClient(
            hostname="h",
            username="u",
            port=2222,
            key_filename="~/.ssh/id_rsa",
            proxy_jump="jump",
            ssh_config_path="/etc/ssh/config",
        )
        assert client.hostname == "h"
        assert client.username == "u"
        assert client.port == 2222
        assert client.key_filename == "~/.ssh/id_rsa"
        assert client.proxy_jump == "jump"
        assert client.ssh_config_path == "/etc/ssh/config"

    @patch("srunx.sync.rsync.shutil.which", return_value="/usr/bin/rsync")
    def test_default_excludes(self, mock_which: MagicMock):
        client = RsyncClient(hostname="h", username="u")
        assert ".git/" in client.exclude_patterns
        assert "__pycache__/" in client.exclude_patterns
        assert ".venv/" in client.exclude_patterns

    @patch("srunx.sync.rsync.shutil.which", return_value="/usr/bin/rsync")
    def test_custom_excludes_merged(self, mock_which: MagicMock):
        client = RsyncClient(
            hostname="h", username="u", exclude_patterns=["data/", ".git/"]
        )
        assert "data/" in client.exclude_patterns
        # .git/ should not be duplicated
        assert client.exclude_patterns.count(".git/") == 1


# ---------------------------------------------------------------------------
# _build_ssh_cmd
# ---------------------------------------------------------------------------


class TestBuildSshCmd:
    @patch("srunx.sync.rsync.shutil.which", return_value="/usr/bin/rsync")
    def test_default_port(self, mock_which: MagicMock):
        client = RsyncClient(hostname="h", username="u")
        cmd = client._build_ssh_cmd()
        assert cmd[0] == "ssh"
        assert "-p" not in cmd  # port 22 is default, not added

    @patch("srunx.sync.rsync.shutil.which", return_value="/usr/bin/rsync")
    def test_custom_port(self, mock_which: MagicMock):
        client = RsyncClient(hostname="h", username="u", port=2222)
        cmd = client._build_ssh_cmd()
        idx = cmd.index("-p")
        assert cmd[idx + 1] == "2222"

    @patch("srunx.sync.rsync.shutil.which", return_value="/usr/bin/rsync")
    def test_key_filename(self, mock_which: MagicMock):
        client = RsyncClient(hostname="h", username="u", key_filename="~/.ssh/mykey")
        cmd = client._build_ssh_cmd()
        idx = cmd.index("-i")
        assert "mykey" in cmd[idx + 1]  # expanduser applied

    @patch("srunx.sync.rsync.shutil.which", return_value="/usr/bin/rsync")
    def test_proxy_jump(self, mock_which: MagicMock):
        client = RsyncClient(hostname="h", username="u", proxy_jump="jumphost")
        cmd = client._build_ssh_cmd()
        idx = cmd.index("-J")
        assert cmd[idx + 1] == "jumphost"

    @patch("srunx.sync.rsync.shutil.which", return_value="/usr/bin/rsync")
    def test_ssh_config(self, mock_which: MagicMock):
        client = RsyncClient(
            hostname="h", username="u", ssh_config_path="/my/ssh/config"
        )
        cmd = client._build_ssh_cmd()
        idx = cmd.index("-F")
        assert cmd[idx + 1] == "/my/ssh/config"

    @patch("srunx.sync.rsync.shutil.which", return_value="/usr/bin/rsync")
    def test_strict_host_key_checking(self, mock_which: MagicMock):
        client = RsyncClient(hostname="h", username="u")
        cmd = client._build_ssh_cmd()
        assert "StrictHostKeyChecking=accept-new" in cmd

    @patch("srunx.sync.rsync.shutil.which", return_value="/usr/bin/rsync")
    def test_batch_mode(self, mock_which: MagicMock):
        client = RsyncClient(hostname="h", username="u")
        cmd = client._build_ssh_cmd()
        assert "BatchMode=yes" in cmd

    @patch("srunx.sync.rsync.shutil.which", return_value="/usr/bin/rsync")
    def test_all_options_combined(self, mock_which: MagicMock):
        client = RsyncClient(
            hostname="h",
            username="u",
            port=2222,
            key_filename="~/.ssh/key",
            proxy_jump="jump",
            ssh_config_path="/cfg",
        )
        cmd = client._build_ssh_cmd()
        assert "-p" in cmd
        assert "-i" in cmd
        assert "-J" in cmd
        assert "-F" in cmd


# ---------------------------------------------------------------------------
# _build_rsync_cmd
# ---------------------------------------------------------------------------


class TestBuildRsyncCmd:
    @patch("srunx.sync.rsync.shutil.which", return_value="/usr/bin/rsync")
    def test_basic_cmd(self, mock_which: MagicMock):
        client = RsyncClient(hostname="h", username="u")
        cmd = client._build_rsync_cmd(
            "src/", "u@h:dst/", delete=False, dry_run=False, excludes=[]
        )
        assert cmd[0] == "rsync"
        assert "-az" in cmd
        assert "--protect-args" in cmd
        assert "-e" in cmd
        assert cmd[-2] == "src/"
        assert cmd[-1] == "u@h:dst/"

    @patch("srunx.sync.rsync.shutil.which", return_value="/usr/bin/rsync")
    def test_delete_flag(self, mock_which: MagicMock):
        client = RsyncClient(hostname="h", username="u")
        cmd = client._build_rsync_cmd("s", "d", delete=True, dry_run=False, excludes=[])
        assert "--delete" in cmd

    @patch("srunx.sync.rsync.shutil.which", return_value="/usr/bin/rsync")
    def test_no_delete_flag(self, mock_which: MagicMock):
        client = RsyncClient(hostname="h", username="u")
        cmd = client._build_rsync_cmd(
            "s", "d", delete=False, dry_run=False, excludes=[]
        )
        assert "--delete" not in cmd

    @patch("srunx.sync.rsync.shutil.which", return_value="/usr/bin/rsync")
    def test_dry_run_flag(self, mock_which: MagicMock):
        client = RsyncClient(hostname="h", username="u")
        cmd = client._build_rsync_cmd("s", "d", delete=False, dry_run=True, excludes=[])
        assert "-n" in cmd

    @patch("srunx.sync.rsync.shutil.which", return_value="/usr/bin/rsync")
    def test_exclude_patterns(self, mock_which: MagicMock):
        client = RsyncClient(hostname="h", username="u")
        cmd = client._build_rsync_cmd(
            "s", "d", delete=False, dry_run=False, excludes=[".git/", "*.pyc"]
        )
        # Check --exclude pairs
        exclude_indices = [i for i, v in enumerate(cmd) if v == "--exclude"]
        assert len(exclude_indices) == 2
        assert cmd[exclude_indices[0] + 1] == ".git/"
        assert cmd[exclude_indices[1] + 1] == "*.pyc"


# ---------------------------------------------------------------------------
# get_default_remote_path
# ---------------------------------------------------------------------------


class TestGetDefaultRemotePath:
    @patch("srunx.sync.rsync.subprocess.run")
    def test_git_repo(self, mock_run: MagicMock):
        mock_run.return_value = MagicMock(
            returncode=0, stdout="/home/user/projects/myrepo\n"
        )
        path = RsyncClient.get_default_remote_path()
        assert path == "~/.config/srunx/workspace/myrepo/"

    @patch("srunx.sync.rsync.subprocess.run")
    def test_not_git_repo(self, mock_run: MagicMock):
        mock_run.return_value = MagicMock(returncode=128, stdout="")
        path = RsyncClient.get_default_remote_path()
        # Falls back to cwd name
        assert path.startswith("~/.config/srunx/workspace/")
        assert path.endswith("/")

    @patch("srunx.sync.rsync.subprocess.run", side_effect=FileNotFoundError)
    def test_git_not_installed(self, mock_run: MagicMock):
        path = RsyncClient.get_default_remote_path()
        assert path.startswith("~/.config/srunx/workspace/")
        assert path.endswith("/")


# ---------------------------------------------------------------------------
# _format_remote
# ---------------------------------------------------------------------------


class TestFormatRemote:
    @patch("srunx.sync.rsync.shutil.which", return_value="/usr/bin/rsync")
    def test_format(self, mock_which: MagicMock):
        client = RsyncClient(hostname="server.example.com", username="researcher")
        result = client._format_remote("~/.config/srunx/workspace/proj/")
        assert result == "researcher@server.example.com:~/.config/srunx/workspace/proj/"

    @patch("srunx.sync.rsync.shutil.which", return_value="/usr/bin/rsync")
    def test_tilde_not_quoted(self, mock_which: MagicMock):
        client = RsyncClient(hostname="h", username="u")
        result = client._format_remote("~/path")
        # Tilde should appear literally, not escaped/quoted
        assert "~" in result
        assert "'" not in result
        assert '"' not in result


# ---------------------------------------------------------------------------
# push / pull (mocked subprocess)
# ---------------------------------------------------------------------------


class TestPush:
    @patch("srunx.sync.rsync.shutil.which", return_value="/usr/bin/rsync")
    @patch("srunx.sync.rsync.subprocess.run")
    def test_push_directory(
        self, mock_run: MagicMock, mock_which: MagicMock, tmp_path: Path
    ):
        mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
        client = RsyncClient(hostname="h", username="u")

        result = client.push(tmp_path, "~/.config/srunx/workspace/test/")
        assert result.success

        call_args = mock_run.call_args[0][0]
        # Source should have trailing slash for directory
        assert call_args[-2].endswith("/")
        # Destination should be formatted as user@host:path
        assert call_args[-1] == "u@h:~/.config/srunx/workspace/test/"
        # Default delete=True
        assert "--delete" in call_args

    @patch("srunx.sync.rsync.shutil.which", return_value="/usr/bin/rsync")
    @patch("srunx.sync.rsync.subprocess.run")
    def test_push_file(
        self, mock_run: MagicMock, mock_which: MagicMock, tmp_path: Path
    ):
        mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
        client = RsyncClient(hostname="h", username="u")

        test_file = tmp_path / "script.py"
        test_file.write_text("print('hello')")

        result = client.push(test_file, "~/.config/srunx/workspace/test/script.py")
        assert result.success

        call_args = mock_run.call_args[0][0]
        # File source should NOT have trailing slash
        assert not call_args[-2].endswith("/")

    @patch("srunx.sync.rsync.shutil.which", return_value="/usr/bin/rsync")
    @patch("srunx.sync.rsync.subprocess.run")
    @patch(
        "srunx.sync.rsync.RsyncClient.get_default_remote_path",
        return_value="~/.config/srunx/workspace/myrepo/",
    )
    def test_push_default_remote_path(
        self,
        mock_path: MagicMock,
        mock_run: MagicMock,
        mock_which: MagicMock,
        tmp_path: Path,
    ):
        mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
        client = RsyncClient(hostname="h", username="u")

        client.push(tmp_path)

        call_args = mock_run.call_args[0][0]
        assert "~/.config/srunx/workspace/myrepo/" in call_args[-1]

    @patch("srunx.sync.rsync.shutil.which", return_value="/usr/bin/rsync")
    @patch("srunx.sync.rsync.subprocess.run")
    def test_push_no_delete(
        self, mock_run: MagicMock, mock_which: MagicMock, tmp_path: Path
    ):
        mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
        client = RsyncClient(hostname="h", username="u")

        client.push(tmp_path, "~/dst/", delete=False)

        call_args = mock_run.call_args[0][0]
        assert "--delete" not in call_args

    @patch("srunx.sync.rsync.shutil.which", return_value="/usr/bin/rsync")
    @patch("srunx.sync.rsync.subprocess.run")
    def test_push_dry_run(
        self, mock_run: MagicMock, mock_which: MagicMock, tmp_path: Path
    ):
        mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
        client = RsyncClient(hostname="h", username="u")

        client.push(tmp_path, "~/dst/", dry_run=True)

        call_args = mock_run.call_args[0][0]
        assert "-n" in call_args

    @patch("srunx.sync.rsync.shutil.which", return_value="/usr/bin/rsync")
    @patch("srunx.sync.rsync.subprocess.run")
    def test_push_failure(
        self, mock_run: MagicMock, mock_which: MagicMock, tmp_path: Path
    ):
        mock_run.return_value = MagicMock(
            returncode=12, stdout="", stderr="connection refused"
        )
        client = RsyncClient(hostname="h", username="u")

        result = client.push(tmp_path, "~/dst/")
        assert not result.success
        assert result.returncode == 12


class TestPull:
    @patch("srunx.sync.rsync.shutil.which", return_value="/usr/bin/rsync")
    @patch("srunx.sync.rsync.subprocess.run")
    def test_pull_basic(
        self, mock_run: MagicMock, mock_which: MagicMock, tmp_path: Path
    ):
        mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
        client = RsyncClient(hostname="h", username="u")

        result = client.pull("~/remote/results/", tmp_path)
        assert result.success

        call_args = mock_run.call_args[0][0]
        # Source is remote
        assert call_args[-2] == "u@h:~/remote/results/"
        # Destination is local
        assert call_args[-1] == str(tmp_path)
        # Default delete=False for pull
        assert "--delete" not in call_args

    @patch("srunx.sync.rsync.shutil.which", return_value="/usr/bin/rsync")
    @patch("srunx.sync.rsync.subprocess.run")
    def test_pull_with_delete(
        self, mock_run: MagicMock, mock_which: MagicMock, tmp_path: Path
    ):
        mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
        client = RsyncClient(hostname="h", username="u")

        client.pull("~/remote/", tmp_path, delete=True)

        call_args = mock_run.call_args[0][0]
        assert "--delete" in call_args

    @patch("srunx.sync.rsync.shutil.which", return_value="/usr/bin/rsync")
    @patch("srunx.sync.rsync.subprocess.run")
    def test_pull_dry_run(
        self, mock_run: MagicMock, mock_which: MagicMock, tmp_path: Path
    ):
        mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
        client = RsyncClient(hostname="h", username="u")

        client.pull("~/remote/", tmp_path, dry_run=True)

        call_args = mock_run.call_args[0][0]
        assert "-n" in call_args


class TestPushWithExcludePatterns:
    @patch("srunx.sync.rsync.shutil.which", return_value="/usr/bin/rsync")
    @patch("srunx.sync.rsync.subprocess.run")
    def test_push_per_call_excludes(
        self, mock_run: MagicMock, mock_which: MagicMock, tmp_path: Path
    ):
        mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
        client = RsyncClient(hostname="h", username="u")

        client.push(tmp_path, "~/dst/", exclude_patterns=["data/", "*.log"])

        call_args = mock_run.call_args[0][0]
        exclude_values = [
            call_args[i + 1] for i, v in enumerate(call_args) if v == "--exclude"
        ]
        assert "data/" in exclude_values
        assert "*.log" in exclude_values
        # Default excludes should also be present
        assert ".git/" in exclude_values

    @patch("srunx.sync.rsync.shutil.which", return_value="/usr/bin/rsync")
    @patch("srunx.sync.rsync.subprocess.run")
    def test_pull_per_call_excludes(
        self, mock_run: MagicMock, mock_which: MagicMock, tmp_path: Path
    ):
        mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
        client = RsyncClient(hostname="h", username="u")

        client.pull("~/remote/", tmp_path, exclude_patterns=["artifacts/"])

        call_args = mock_run.call_args[0][0]
        exclude_values = [
            call_args[i + 1] for i, v in enumerate(call_args) if v == "--exclude"
        ]
        assert "artifacts/" in exclude_values
        assert ".git/" in exclude_values


class TestMkpath:
    @patch("srunx.sync.rsync.shutil.which", return_value="/usr/bin/rsync")
    def test_mkpath_in_rsync_cmd(self, mock_which: MagicMock):
        client = RsyncClient(hostname="h", username="u")
        cmd = client._build_rsync_cmd(
            "s", "d", delete=False, dry_run=False, excludes=[]
        )
        assert "--mkpath" in cmd


# ---------------------------------------------------------------------------
# SSHSlurmClient.sync_project
# ---------------------------------------------------------------------------


class TestSyncProject:
    @patch("srunx.sync.rsync.shutil.which", return_value="/usr/bin/rsync")
    @patch("srunx.ssh.core.client.subprocess.run")
    def test_sync_project_returns_remote_path(
        self, mock_git_run: MagicMock, mock_which: MagicMock
    ):
        from srunx.ssh.core.client import SSHSlurmClient

        mock_git_run.return_value = MagicMock(
            returncode=0, stdout="/home/user/myproject\n"
        )

        client = SSHSlurmClient(
            hostname="server",
            username="user",
            key_filename="~/.ssh/id_rsa",
        )

        with patch.object(client._rsync_client, "push") as mock_push:
            mock_push.return_value = RsyncResult(returncode=0, stdout="", stderr="")
            remote_path = client.sync_project()

        assert "~/.config/srunx/workspace/" in remote_path

    def test_sync_project_no_key_raises(self):
        from srunx.ssh.core.client import SSHSlurmClient

        client = SSHSlurmClient(
            hostname="server",
            username="user",
            password="pass",
        )

        with pytest.raises(RuntimeError, match="key-based SSH auth"):
            client.sync_project()

    @patch("srunx.sync.rsync.shutil.which", return_value="/usr/bin/rsync")
    @patch("srunx.ssh.core.client.subprocess.run")
    def test_sync_project_rsync_failure_raises(
        self, mock_git_run: MagicMock, mock_which: MagicMock
    ):
        from srunx.ssh.core.client import SSHSlurmClient

        mock_git_run.return_value = MagicMock(
            returncode=0, stdout="/home/user/myproject\n"
        )

        client = SSHSlurmClient(
            hostname="server",
            username="user",
            key_filename="~/.ssh/id_rsa",
        )

        with patch.object(client._rsync_client, "push") as mock_push:
            mock_push.return_value = RsyncResult(
                returncode=12, stdout="", stderr="connection refused"
            )
            with pytest.raises(RuntimeError, match="rsync failed"):
                client.sync_project()
