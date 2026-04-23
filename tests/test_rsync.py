"""Tests for the rsync-based file synchronization module."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from srunx.sync.rsync import RsyncClient, RsyncResult

# GNU rsync --help output stub that includes --protect-args and --mkpath
_GNU_RSYNC_HELP = "--protect-args --mkpath"


def _make_rsync_client(**kwargs: object) -> RsyncClient:
    """Create an RsyncClient with mocked binary detection (GNU rsync features)."""
    with (
        patch("srunx.sync.rsync.shutil.which", return_value="/usr/bin/rsync"),
        patch(
            "srunx.sync.rsync.subprocess.run",
            return_value=MagicMock(stdout=_GNU_RSYNC_HELP, stderr=""),
        ),
    ):
        return RsyncClient(**kwargs)  # type: ignore[arg-type]


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

    def test_stores_params(self):
        client = _make_rsync_client(
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

    def test_default_excludes(self):
        client = _make_rsync_client(hostname="h", username="u")
        assert ".git/" in client.exclude_patterns
        assert "__pycache__/" in client.exclude_patterns
        assert ".venv/" in client.exclude_patterns

    def test_custom_excludes_merged(self):
        client = _make_rsync_client(
            hostname="h", username="u", exclude_patterns=["data/", ".git/"]
        )
        assert "data/" in client.exclude_patterns
        # .git/ should not be duplicated
        assert client.exclude_patterns.count(".git/") == 1

    def test_detects_gnu_rsync_capabilities(self):
        client = _make_rsync_client(hostname="h", username="u")
        assert client._supports_protect_args is True
        assert client._supports_mkpath is True

    @patch(
        "srunx.sync.rsync.subprocess.run",
        return_value=MagicMock(stdout="", stderr="openrsync: protocol version 29"),
    )
    @patch("srunx.sync.rsync.shutil.which", return_value="/usr/bin/rsync")
    def test_detects_openrsync_no_capabilities(
        self, mock_which: MagicMock, mock_run: MagicMock
    ):
        client = RsyncClient(hostname="h", username="u")
        assert client._supports_protect_args is False
        assert client._supports_mkpath is False


# ---------------------------------------------------------------------------
# _build_ssh_cmd
# ---------------------------------------------------------------------------


class TestBuildSshCmd:
    def test_default_port(self):
        client = _make_rsync_client(hostname="h", username="u")
        cmd = client._build_ssh_cmd()
        assert cmd[0] == "ssh"
        assert "-p" not in cmd  # port 22 is default, not added

    def test_custom_port(self):
        client = _make_rsync_client(hostname="h", username="u", port=2222)
        cmd = client._build_ssh_cmd()
        idx = cmd.index("-p")
        assert cmd[idx + 1] == "2222"

    def test_key_filename(self):
        client = _make_rsync_client(
            hostname="h", username="u", key_filename="~/.ssh/mykey"
        )
        cmd = client._build_ssh_cmd()
        idx = cmd.index("-i")
        assert "mykey" in cmd[idx + 1]  # expanduser applied

    def test_proxy_jump(self):
        client = _make_rsync_client(hostname="h", username="u", proxy_jump="jumphost")
        cmd = client._build_ssh_cmd()
        idx = cmd.index("-J")
        assert cmd[idx + 1] == "jumphost"

    def test_ssh_config(self):
        client = _make_rsync_client(
            hostname="h", username="u", ssh_config_path="/my/ssh/config"
        )
        cmd = client._build_ssh_cmd()
        idx = cmd.index("-F")
        assert cmd[idx + 1] == "/my/ssh/config"

    def test_strict_host_key_checking(self):
        client = _make_rsync_client(hostname="h", username="u")
        cmd = client._build_ssh_cmd()
        assert "StrictHostKeyChecking=accept-new" in cmd

    def test_batch_mode(self):
        client = _make_rsync_client(hostname="h", username="u")
        cmd = client._build_ssh_cmd()
        assert "BatchMode=yes" in cmd

    def test_all_options_combined(self):
        client = _make_rsync_client(
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
    def test_basic_cmd(self):
        client = _make_rsync_client(hostname="h", username="u")
        cmd = client._build_rsync_cmd(
            "src/", "u@h:dst/", delete=False, dry_run=False, excludes=[]
        )
        assert cmd[0] == "rsync"
        assert "-az" in cmd
        assert "--protect-args" in cmd
        assert "-e" in cmd
        # -- separator before src/dst
        assert "--" in cmd
        sep_idx = cmd.index("--")
        assert cmd[sep_idx + 1] == "src/"
        assert cmd[sep_idx + 2] == "u@h:dst/"

    def test_delete_flag(self):
        client = _make_rsync_client(hostname="h", username="u")
        cmd = client._build_rsync_cmd("s", "d", delete=True, dry_run=False, excludes=[])
        assert "--delete" in cmd

    def test_no_delete_flag(self):
        client = _make_rsync_client(hostname="h", username="u")
        cmd = client._build_rsync_cmd(
            "s", "d", delete=False, dry_run=False, excludes=[]
        )
        assert "--delete" not in cmd

    def test_dry_run_flag(self):
        client = _make_rsync_client(hostname="h", username="u")
        cmd = client._build_rsync_cmd("s", "d", delete=False, dry_run=True, excludes=[])
        assert "-n" in cmd

    def test_exclude_patterns(self):
        client = _make_rsync_client(hostname="h", username="u")
        cmd = client._build_rsync_cmd(
            "s", "d", delete=False, dry_run=False, excludes=[".git/", "*.pyc"]
        )
        exclude_indices = [i for i, v in enumerate(cmd) if v == "--exclude"]
        assert len(exclude_indices) == 2
        assert cmd[exclude_indices[0] + 1] == ".git/"
        assert cmd[exclude_indices[1] + 1] == "*.pyc"

    def test_no_protect_args_on_openrsync(self):
        """openrsync doesn't support --protect-args or --mkpath."""
        with (
            patch("srunx.sync.rsync.shutil.which", return_value="/usr/bin/rsync"),
            patch(
                "srunx.sync.rsync.subprocess.run",
                return_value=MagicMock(stdout="", stderr="openrsync"),
            ),
        ):
            client = RsyncClient(hostname="h", username="u")
        cmd = client._build_rsync_cmd(
            "s", "d", delete=False, dry_run=False, excludes=[]
        )
        assert "--protect-args" not in cmd
        assert "--mkpath" not in cmd


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
        assert path.startswith("~/.config/srunx/workspace/")
        assert path.endswith("/")

    @patch("srunx.sync.rsync.subprocess.run", side_effect=FileNotFoundError)
    def test_git_not_installed(self, mock_run: MagicMock):
        path = RsyncClient.get_default_remote_path()
        assert path.startswith("~/.config/srunx/workspace/")
        assert path.endswith("/")

    @patch("srunx.sync.rsync.subprocess.run")
    def test_uses_local_path_for_git_detection(self, mock_run: MagicMock):
        mock_run.return_value = MagicMock(
            returncode=0, stdout="/other/project/otherrepo\n"
        )
        path = RsyncClient.get_default_remote_path("/other/project/otherrepo")
        assert path == "~/.config/srunx/workspace/otherrepo/"
        # Verify cwd was passed to subprocess
        assert mock_run.call_args[1]["cwd"] == "/other/project/otherrepo"


# ---------------------------------------------------------------------------
# _format_remote
# ---------------------------------------------------------------------------


class TestFormatRemote:
    def test_format(self):
        client = _make_rsync_client(
            hostname="server.example.com", username="researcher"
        )
        result = client._format_remote("~/.config/srunx/workspace/proj/")
        assert result == "researcher@server.example.com:~/.config/srunx/workspace/proj/"

    def test_tilde_not_quoted(self):
        client = _make_rsync_client(hostname="h", username="u")
        result = client._format_remote("~/path")
        assert "~" in result
        assert "'" not in result
        assert '"' not in result


# ---------------------------------------------------------------------------
# push / pull (mocked subprocess)
# ---------------------------------------------------------------------------


class TestPush:
    def test_push_directory(self, tmp_path: Path):
        client = _make_rsync_client(hostname="h", username="u")

        with patch("srunx.sync.rsync.subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
            result = client.push(tmp_path, "~/.config/srunx/workspace/test/")

        assert result.success
        call_args = mock_run.call_args[0][0]
        sep_idx = call_args.index("--")
        assert call_args[sep_idx + 1].endswith("/")
        assert call_args[sep_idx + 2] == "u@h:~/.config/srunx/workspace/test/"
        assert "--delete" in call_args

    def test_push_file(self, tmp_path: Path):
        client = _make_rsync_client(hostname="h", username="u")
        test_file = tmp_path / "script.py"
        test_file.write_text("print('hello')")

        with patch("srunx.sync.rsync.subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
            result = client.push(test_file, "~/.config/srunx/workspace/test/script.py")

        assert result.success
        call_args = mock_run.call_args[0][0]
        sep_idx = call_args.index("--")
        assert not call_args[sep_idx + 1].endswith("/")

    def test_push_default_remote_path(self, tmp_path: Path):
        client = _make_rsync_client(hostname="h", username="u")

        with (
            patch("srunx.sync.rsync.subprocess.run") as mock_run,
            patch(
                "srunx.sync.rsync.RsyncClient.get_default_remote_path",
                return_value="~/.config/srunx/workspace/myrepo/",
            ),
        ):
            mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
            client.push(tmp_path)

        call_args = mock_run.call_args[0][0]
        assert "~/.config/srunx/workspace/myrepo/" in call_args[-1]

    def test_push_no_delete(self, tmp_path: Path):
        client = _make_rsync_client(hostname="h", username="u")

        with patch("srunx.sync.rsync.subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
            client.push(tmp_path, "~/dst/", delete=False)

        assert "--delete" not in mock_run.call_args[0][0]

    def test_push_dry_run(self, tmp_path: Path):
        client = _make_rsync_client(hostname="h", username="u")

        with patch("srunx.sync.rsync.subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
            client.push(tmp_path, "~/dst/", dry_run=True)

        assert "-n" in mock_run.call_args[0][0]

    def test_push_itemize(self, tmp_path: Path):
        """``itemize=True`` adds rsync's ``-i`` flag.

        Required for the dry-run preview path (#137 part 2): without
        ``-i`` rsync emits no per-file output, so the CLI can't show
        the user what *would* change.
        """
        client = _make_rsync_client(hostname="h", username="u")

        with patch("srunx.sync.rsync.subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
            client.push(tmp_path, "~/dst/", dry_run=True, itemize=True)

        cmd = mock_run.call_args[0][0]
        assert "-n" in cmd and "-i" in cmd

    def test_push_no_itemize_by_default(self, tmp_path: Path):
        """``-i`` is opt-in — default push doesn't add it.

        A successful real sync should not spam stdout with per-file
        change lines. ``itemize=True`` is the explicit opt-in for
        callers that want the listing.
        """
        client = _make_rsync_client(hostname="h", username="u")

        with patch("srunx.sync.rsync.subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
            client.push(tmp_path, "~/dst/")

        assert "-i" not in mock_run.call_args[0][0]

    def test_push_failure(self, tmp_path: Path):
        client = _make_rsync_client(hostname="h", username="u")

        with patch("srunx.sync.rsync.subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(
                returncode=12, stdout="", stderr="connection refused"
            )
            result = client.push(tmp_path, "~/dst/")

        assert not result.success
        assert result.returncode == 12


class TestPull:
    def test_pull_basic(self, tmp_path: Path):
        client = _make_rsync_client(hostname="h", username="u")

        with patch("srunx.sync.rsync.subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
            result = client.pull("~/remote/results/", tmp_path)

        assert result.success
        call_args = mock_run.call_args[0][0]
        sep_idx = call_args.index("--")
        assert call_args[sep_idx + 1] == "u@h:~/remote/results/"
        assert call_args[sep_idx + 2] == str(tmp_path)
        assert "--delete" not in call_args

    def test_pull_with_delete(self, tmp_path: Path):
        client = _make_rsync_client(hostname="h", username="u")

        with patch("srunx.sync.rsync.subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
            client.pull("~/remote/", tmp_path, delete=True)

        assert "--delete" in mock_run.call_args[0][0]

    def test_pull_dry_run(self, tmp_path: Path):
        client = _make_rsync_client(hostname="h", username="u")

        with patch("srunx.sync.rsync.subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
            client.pull("~/remote/", tmp_path, dry_run=True)

        assert "-n" in mock_run.call_args[0][0]


class TestPushWithExcludePatterns:
    def test_push_per_call_excludes(self, tmp_path: Path):
        client = _make_rsync_client(hostname="h", username="u")

        with patch("srunx.sync.rsync.subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
            client.push(tmp_path, "~/dst/", exclude_patterns=["data/", "*.log"])

        call_args = mock_run.call_args[0][0]
        exclude_values = [
            call_args[i + 1] for i, v in enumerate(call_args) if v == "--exclude"
        ]
        assert "data/" in exclude_values
        assert "*.log" in exclude_values
        assert ".git/" in exclude_values

    def test_pull_per_call_excludes(self, tmp_path: Path):
        client = _make_rsync_client(hostname="h", username="u")

        with patch("srunx.sync.rsync.subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
            client.pull("~/remote/", tmp_path, exclude_patterns=["artifacts/"])

        call_args = mock_run.call_args[0][0]
        exclude_values = [
            call_args[i + 1] for i, v in enumerate(call_args) if v == "--exclude"
        ]
        assert "artifacts/" in exclude_values
        assert ".git/" in exclude_values

    def test_constructor_excludes_merged_with_defaults(self):
        """Exclude patterns passed at construction are merged with DEFAULT_EXCLUDES."""
        client = _make_rsync_client(
            hostname="h", username="u", exclude_patterns=["data/", "*.bin"]
        )
        assert "data/" in client.exclude_patterns
        assert "*.bin" in client.exclude_patterns
        # Defaults still present
        assert ".git/" in client.exclude_patterns
        assert "__pycache__/" in client.exclude_patterns

    def test_constructor_excludes_no_duplicates(self):
        """Passing a pattern already in DEFAULT_EXCLUDES doesn't create duplicates."""
        client = _make_rsync_client(
            hostname="h", username="u", exclude_patterns=[".git/", "data/"]
        )
        assert client.exclude_patterns.count(".git/") == 1
        assert "data/" in client.exclude_patterns

    def test_constructor_and_per_call_excludes_combined(self, tmp_path: Path):
        """Constructor-level and per-call excludes are both present in the command."""
        client = _make_rsync_client(
            hostname="h", username="u", exclude_patterns=["weights/"]
        )

        with patch("srunx.sync.rsync.subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
            client.push(tmp_path, "~/dst/", exclude_patterns=["logs/"])

        call_args = mock_run.call_args[0][0]
        exclude_values = [
            call_args[i + 1] for i, v in enumerate(call_args) if v == "--exclude"
        ]
        assert "weights/" in exclude_values  # from constructor
        assert "logs/" in exclude_values  # from per-call
        assert ".git/" in exclude_values  # from defaults


class TestMkpath:
    def test_mkpath_in_rsync_cmd_when_supported(self):
        client = _make_rsync_client(hostname="h", username="u")
        cmd = client._build_rsync_cmd(
            "s", "d", delete=False, dry_run=False, excludes=[]
        )
        assert "--mkpath" in cmd

    def test_no_mkpath_falls_back_to_ssh_mkdir(self, tmp_path: Path):
        """When rsync lacks --mkpath, push() calls _ensure_remote_dir via ssh."""
        with (
            patch("srunx.sync.rsync.shutil.which", return_value="/usr/bin/rsync"),
            patch(
                "srunx.sync.rsync.subprocess.run",
                return_value=MagicMock(stdout="", stderr="openrsync"),
            ),
        ):
            client = RsyncClient(hostname="h", username="u")

        assert not client._supports_mkpath

        with patch("srunx.sync.rsync.subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
            client.push(tmp_path, "~/dst/")

        # First call is _ensure_remote_dir (ssh mkdir -p), second is rsync
        assert mock_run.call_count == 2
        mkdir_cmd = mock_run.call_args_list[0][0][0]
        assert "mkdir" in " ".join(mkdir_cmd)


# ---------------------------------------------------------------------------
# SSHSlurmClient.sync_project
# ---------------------------------------------------------------------------


class TestSyncProject:
    @patch("srunx.sync.rsync.shutil.which", return_value="/usr/bin/rsync")
    @patch("srunx.sync.rsync.subprocess.run")
    @patch("srunx.ssh.core.client.subprocess.run")
    def test_sync_project_returns_remote_path(
        self,
        mock_client_run: MagicMock,
        mock_rsync_run: MagicMock,
        mock_which: MagicMock,
    ):
        from srunx.ssh.core.client import SSHSlurmClient

        mock_rsync_run.return_value = MagicMock(stdout=_GNU_RSYNC_HELP, stderr="")
        mock_client_run.return_value = MagicMock(
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
    @patch("srunx.sync.rsync.subprocess.run")
    @patch("srunx.ssh.core.client.subprocess.run")
    def test_sync_project_rsync_failure_raises(
        self,
        mock_client_run: MagicMock,
        mock_rsync_run: MagicMock,
        mock_which: MagicMock,
    ):
        from srunx.ssh.core.client import SSHSlurmClient

        mock_rsync_run.return_value = MagicMock(stdout=_GNU_RSYNC_HELP, stderr="")
        mock_client_run.return_value = MagicMock(
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
