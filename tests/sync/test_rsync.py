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

    def test_strict_host_key_checking_default_is_strict(self, monkeypatch):
        monkeypatch.delenv("SRUNX_SSH_HOST_KEY_POLICY", raising=False)
        client = _make_rsync_client(hostname="h", username="u")
        cmd = client._build_ssh_cmd()
        assert "StrictHostKeyChecking=yes" in cmd

    def test_strict_host_key_checking_accept_new_opt_in(self, monkeypatch):
        monkeypatch.setenv("SRUNX_SSH_HOST_KEY_POLICY", "accept-new")
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
        # A plain push adds and updates only. Mirror semantics are opt-in:
        # inheriting --delete by default is what silently ate remote-only
        # checkpoints before, so no caller gets it without asking.
        assert "--delete" not in call_args

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

    def test_push_delete_is_opt_in(self, tmp_path: Path):
        client = _make_rsync_client(hostname="h", username="u")

        with patch("srunx.sync.rsync.subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
            client.push(tmp_path, "~/dst/", delete=True)

        assert "--delete" in mock_run.call_args[0][0]

    def test_push_max_delete_caps_a_mirror(self, tmp_path: Path):
        client = _make_rsync_client(hostname="h", username="u")

        with patch("srunx.sync.rsync.subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
            client.push(tmp_path, "~/dst/", delete=True, max_delete=5)

        call_args = mock_run.call_args[0][0]
        assert "--delete" in call_args
        assert "--max-delete=5" in call_args

    def test_push_max_delete_omitted_without_delete(self, tmp_path: Path):
        """Without --delete, rsync deletes nothing — the cap would be noise."""
        client = _make_rsync_client(hostname="h", username="u")

        with patch("srunx.sync.rsync.subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
            client.push(tmp_path, "~/dst/", delete=False, max_delete=5)

        assert not any(a.startswith("--max-delete") for a in mock_run.call_args[0][0])

    def test_push_rejects_zero_max_delete(self, tmp_path: Path):
        """``max_delete=0`` is ambiguous and unsafe, so it is refused outright.

        Forwarding it would invert the cap: on rsync 2.6.x / openrsync (stock
        on macOS) 0 means *unlimited*, and a real ``--delete --max-delete=0``
        run against openrsync deleted every destination-only file and exited 0.
        Silently dropping ``--delete`` instead is also wrong — the caller asked
        for a mirror that refuses, not one that quietly leaves extra files.
        """
        client = _make_rsync_client(hostname="h", username="u")

        with patch("srunx.sync.rsync.subprocess.run") as mock_run:
            with pytest.raises(ValueError, match="max_delete must be >= 1"):
                client.push(tmp_path, "~/dst/", delete=True, max_delete=0)
            mock_run.assert_not_called()

    def test_push_rejects_negative_max_delete(self, tmp_path: Path):
        client = _make_rsync_client(hostname="h", username="u")

        with pytest.raises(ValueError, match="max_delete must be >= 1"):
            client.push(tmp_path, "~/dst/", delete=True, max_delete=-1)

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

    def _openrsync_client(self) -> RsyncClient:
        """A client whose rsync lacks ``--mkpath`` (stock macOS/openrsync)."""
        with (
            patch("srunx.sync.rsync.shutil.which", return_value="/usr/bin/rsync"),
            patch(
                "srunx.sync.rsync.subprocess.run",
                return_value=MagicMock(stdout="", stderr="openrsync"),
            ),
        ):
            return RsyncClient(hostname="h", username="u")

    def test_dry_run_never_creates_the_destination(self, tmp_path: Path):
        """A preview must not modify the remote — not even an empty directory.

        Creating it would make "nothing was changed" false in the mirror
        preflight's refusal message, and for a *file* destination the mkdir
        would land a directory exactly where the file belongs. A first mirror
        against a missing destination therefore fails instead; that is a safe,
        explicit failure with a documented workaround (sync once with
        ``delete=False``).
        """
        client = self._openrsync_client()

        with patch("srunx.sync.rsync.subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
            client.push(tmp_path, "~/dst/", dry_run=True, delete=True, itemize=True)

        # Exactly one call: the rsync itself, and it is a dry run.
        mock_run.assert_called_once()
        assert "mkdir" not in " ".join(mock_run.call_args[0][0])
        assert "-n" in mock_run.call_args[0][0]

    def test_file_destination_creates_parent_not_the_file_path(self, tmp_path: Path):
        """``mkdir -p`` on a file path would block the file forever."""
        client = self._openrsync_client()
        script = tmp_path / "train.sh"
        script.write_text("echo hi\n")

        with patch("srunx.sync.rsync.subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
            client.push(script, "~/jobs/train.sh")

        mkdir_cmd = " ".join(mock_run.call_args_list[0][0][0])
        assert "mkdir" in mkdir_cmd
        assert "~/jobs" in mkdir_cmd
        assert "train.sh" not in mkdir_cmd


# ---------------------------------------------------------------------------
# Verbose streaming (#137 part 3)
# ---------------------------------------------------------------------------


class TestVerboseStreaming:
    """``verbose=True`` switches push() onto the streaming Popen path."""

    def test_verbose_adds_progress_flag(self):
        """``--info=progress2`` is rsync's single-line progress mode.

        Single-line is the only progress form that doesn't drown the
        terminal in per-file output for thousand-file syncs.
        """
        client = _make_rsync_client(hostname="h", username="u")
        cmd = client._build_rsync_cmd(
            "s", "d", delete=False, dry_run=False, verbose=True, excludes=[]
        )
        assert "--info=progress2" in cmd

    def test_default_does_not_add_progress_flag(self):
        client = _make_rsync_client(hostname="h", username="u")
        cmd = client._build_rsync_cmd(
            "s", "d", delete=False, dry_run=False, excludes=[]
        )
        assert "--info=progress2" not in cmd

    def test_default_uses_subprocess_run(self, tmp_path: Path):
        """``verbose=False`` keeps the historical capture-and-quiet path.

        Bit-for-bit no behaviour change for the default — guarded so we
        notice if a future refactor accidentally rewires the default
        path through Popen.
        """
        client = _make_rsync_client(hostname="h", username="u")

        with (
            patch("srunx.sync.rsync.subprocess.run") as mock_run,
            patch("srunx.sync.rsync.subprocess.Popen") as mock_popen,
        ):
            mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
            client.push(tmp_path, "~/dst/")

        mock_run.assert_called_once()
        mock_popen.assert_not_called()

    def test_verbose_uses_popen_and_streams_to_stderr(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ):
        """Streaming path drains both pipes and surfaces lines on stderr."""
        client = _make_rsync_client(hostname="h", username="u")

        # Fake Popen: stdout yields three progress lines, stderr stays empty.
        fake_proc = MagicMock()
        fake_proc.stdout = iter(
            [
                "  1,234,567  10%   1.23MB/s    0:00:42\n",
                "  2,345,678  20%   1.45MB/s    0:00:30\n",
                "  3,456,789 100%   1.50MB/s    0:00:00 (xfr#1, to-chk=0/1)\n",
            ]
        )
        fake_proc.stderr = iter([])
        fake_proc.wait.return_value = 0

        with (
            patch("srunx.sync.rsync.subprocess.run") as mock_run,
            patch(
                "srunx.sync.rsync.subprocess.Popen", return_value=fake_proc
            ) as mock_popen,
        ):
            mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
            result = client.push(tmp_path, "~/dst/", verbose=True)

        # Popen was used (not run) for the actual rsync invocation.
        # ``subprocess.run`` may still be called for the ``_ensure_remote_dir``
        # fallback when --mkpath isn't supported, so we only assert the
        # Popen invocation here.
        mock_popen.assert_called_once()
        cmd = mock_popen.call_args[0][0]
        assert "--info=progress2" in cmd

        # Streamed lines reached stderr verbatim.
        captured = capsys.readouterr()
        assert "10%" in captured.err
        assert "100%" in captured.err

        # The accumulated stdout is also returned in the result so
        # callers that rely on result.stdout (error messages, etc.)
        # keep working.
        assert "10%" in result.stdout
        assert "100%" in result.stdout
        assert result.success

    def test_verbose_returncode_propagates(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ):
        """Non-zero exit on the streaming path produces an unsuccessful result."""
        client = _make_rsync_client(hostname="h", username="u")

        fake_proc = MagicMock()
        fake_proc.stdout = iter([])
        fake_proc.stderr = iter(["rsync: connection unexpectedly closed\n"])
        fake_proc.wait.return_value = 12

        with (
            patch("srunx.sync.rsync.subprocess.run") as mock_run,
            patch("srunx.sync.rsync.subprocess.Popen", return_value=fake_proc),
        ):
            mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
            result = client.push(tmp_path, "~/dst/", verbose=True)

        assert result.returncode == 12
        assert "connection unexpectedly closed" in result.stderr


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


# ---------------------------------------------------------------------------
# remote_sha256 (#137 part 5)
# ---------------------------------------------------------------------------


class TestRemoteSha256:
    """``remote_sha256`` is the cluster-side half of post-rsync verification.

    The wire-level shape is a single ssh round-trip running a small
    shell snippet that:

    1. ``test -f`` — distinct exit code for ""file missing"".
    2. Tries ``sha256sum`` (Linux), falls back to ``shasum -a 256``
       (macOS) — distinct exit code for ""no tool available"".
    3. Otherwise prints the digest followed by the path.

    Each branch maps to a distinct return / raise so the caller in
    :mod:`srunx.sync.hash_verify` can apply the right policy without
    grepping stderr.
    """

    def test_happy_path_parses_sha256sum_output(self) -> None:
        """Standard ``sha256sum`` output: ``<64 hex>  <path>``."""
        client = _make_rsync_client(hostname="h", username="u")
        digest = "a" * 64
        with patch("srunx.sync.rsync.subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(
                returncode=0,
                stdout=f"{digest}  /r/ml/train.sbatch\n",
                stderr="",
            )
            result = client.remote_sha256("/r/ml/train.sbatch")
        assert result == digest
        # The remote command must have shell-quoted the path.
        ssh_cmd = mock_run.call_args[0][0]
        joined = " ".join(ssh_cmd)
        assert "/r/ml/train.sbatch" in joined
        assert "sha256sum" in joined
        assert "shasum" in joined  # fallback also wired in

    def test_uppercase_hex_is_normalised(self) -> None:
        """``shasum`` on some BSDs prints upper-case — normalise to lower.

        Comparison against :func:`hashlib.sha256().hexdigest()`
        (always lowercase) needs both sides in the same case.
        """
        client = _make_rsync_client(hostname="h", username="u")
        digest_upper = "A" * 64
        with patch("srunx.sync.rsync.subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(
                returncode=0, stdout=f"{digest_upper}  /r/ml/x\n", stderr=""
            )
            assert client.remote_sha256("/r/ml/x") == "a" * 64

    def test_missing_file_returns_none(self) -> None:
        """Custom exit 10 (``test -f`` failed) → ``None`` (file gone)."""
        client = _make_rsync_client(hostname="h", username="u")
        with patch("srunx.sync.rsync.subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=10, stdout="", stderr="")
            assert client.remote_sha256("/r/ml/missing.sbatch") is None

    def test_missing_tool_returns_none(self) -> None:
        """Custom exit 11 (no sha256sum, no shasum) → ``None`` (skip).

        The debug-log emission itself isn't asserted (loguru +
        caplog don't compose without a sink) — the contract that
        matters is that the caller gets None to feed the
        ``hash_verify`` skip-silently policy.
        """
        client = _make_rsync_client(hostname="h", username="u")
        with patch("srunx.sync.rsync.subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=11, stdout="", stderr="")
            assert client.remote_sha256("/r/ml/script.sbatch") is None

    def test_ssh_failure_raises_runtime_error(self) -> None:
        """ssh exit 255 (genuine network failure) → ``RuntimeError``."""
        client = _make_rsync_client(hostname="h", username="u")
        with patch("srunx.sync.rsync.subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(
                returncode=255,
                stdout="",
                stderr="ssh: connect to host h: connection refused",
            )
            with pytest.raises(RuntimeError, match="connection refused"):
                client.remote_sha256("/r/ml/train.sbatch")

    def test_unparseable_output_raises(self) -> None:
        """Exit 0 but no hex digest in stdout → fail loud, don't fall through.

        sha256sum / shasum surfacing something we don't recognise is
        more concerning than ""no hash"" — silently returning None
        would let a buggy remote mask itself as ""tool missing"".
        """
        client = _make_rsync_client(hostname="h", username="u")
        with patch("srunx.sync.rsync.subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(
                returncode=0,
                stdout="garbage not-a-digest\n",
                stderr="",
            )
            with pytest.raises(RuntimeError, match="parse sha256"):
                client.remote_sha256("/r/ml/train.sbatch")


# ---------------------------------------------------------------------------
# write_remote_file (owner marker / control files)
# ---------------------------------------------------------------------------


class TestWriteRemoteFile:
    """Control-file writes must be atomic and must not follow symlinks.

    The previous implementation piped ``tee`` straight at the target while
    documenting that a reader "never sees a half-written file". ``tee`` empties
    the destination first, so a reader in that window saw an empty file — and a
    symlink at the target redirected the write outside the mount.
    """

    def _client_with_ssh(self, *results: MagicMock) -> tuple[RsyncClient, MagicMock]:
        client = _make_rsync_client(hostname="h", username="u")
        ssh = MagicMock(side_effect=list(results))
        return client, ssh

    @staticmethod
    def _ok(stdout: str = "OK\n", returncode: int = 0) -> MagicMock:
        return MagicMock(returncode=returncode, stdout=stdout, stderr="")

    def test_runs_as_a_single_ssh_invocation(self):
        """One connection, not several.

        The marker is rewritten on *every* synced submission, so splitting this
        into probe/create/write/publish cost that many key exchanges — and that
        many hardware-key touches — each time. One shell also leaves almost no
        window in which the target could be swapped between check and rename.
        """
        client, ssh = self._client_with_ssh(self._ok(""))
        with patch.object(client, "_ssh_run", ssh):
            client.write_remote_file("/remote/ml/.srunx-owner.json", '{"a":1}')

        ssh.assert_called_once()
        script = ssh.call_args.args[0]
        assert ssh.call_args.kwargs["stdin"] == '{"a":1}'
        # Content lands in the temp, never at the target.
        assert 'cat > "$f"' in script
        # Published by a same-directory rename, which is atomic.
        assert 'mv -f -- "$f" ' in script
        assert "/remote/ml/.srunx-owner.json" in script

    def test_temp_lives_in_an_exclusively_created_private_directory(self):
        """``mkdir`` is the only exclusive-create covering every inode type.

        Two earlier attempts failed, both verified against real shells:

        * ``mktemp`` + ``chmod`` + ``cat`` reopened the temp *by name*, so a
          symlink planted in that gap redirected the write and destroyed an
          unrelated file — arbitrary overwrite, not just a broken marker.
        * ``set -C`` alone only refuses an existing *regular* file (POSIX XCU
          2.7.2). With a FIFO at the predictable name the redirection did not
          fail — it **hung** in open(), stalling the sync indefinitely.

        ``mkdir -m 700`` was verified to refuse a planted symlink, FIFO,
        directory and regular file alike (exit 5, marker never written), and
        mode 700 stops anyone swapping the file created inside it.
        """
        client, ssh = self._client_with_ssh(self._ok(""))
        with patch.object(client, "_ssh_run", ssh):
            client.write_remote_file("/remote/ml/.srunx-owner.json", "x")

        script = ssh.call_args.args[0]
        # mktemp is used, but only *inside* our own directory — the earlier
        # failure was calling it in the untrusted mount root.
        assert 'mkdir -m 700 -- "$d"' in script
        # Anchored by cd: mode 700 guards the contents, but write permission on
        # the parent still lets a watcher swap the directory entry itself. A
        # working directory follows the inode, so nothing reopens a path an
        # attacker controls.
        assert 'cd -- "$d"' in script
        assert script.index('cd -- "$d"') < script.index("mktemp")
        # Random basename from mktemp, safe here because the directory is ours.
        assert "mktemp -- ./w.XXXXXX" in script
        assert script.index("mktemp") < script.index('cat > "$f"')
        # Cleanup is targeted, never recursive.
        assert "rm -rf" not in script
        assert 'rmdir -- "$d"' in script

    def test_verifies_it_entered_its_own_directory(self):
        """``mkdir`` then ``cd`` is a TOCTOU on the directory *entry*.

        Write permission on the parent lets a peer rename our entry away and put
        their own directory under the same name before we open it — after which
        we would create, chmod and write inside theirs, where they can swap the
        file for a symlink. A peer can only create directories owned by
        themselves, so a uid match rules that out.
        """
        client, ssh = self._client_with_ssh(self._ok(""))
        with patch.object(client, "_ssh_run", ssh):
            client.write_remote_file("/remote/ml/.srunx-owner.json", "x")

        script = ssh.call_args.args[0]
        assert '"$(id -u)"' in script
        assert "ls -ldn ." in script
        # Checked after entering and before anything is created inside.
        assert script.index('cd -- "$d"') < script.index("ls -ldn .")
        assert script.index("ls -ldn .") < script.index("mktemp")
        # ``test -O`` is a bash/ksh extension, absent from dash (/bin/sh on most
        # Linux clusters), so it must not be relied on.
        assert "-O ." not in script

    def test_relative_destination_is_rejected(self):
        """Anchoring the working directory changes how a relative path resolves.

        The rename would land the replacement inside the temp directory and the
        verification would look elsewhere, so the call would report failure and
        leave the requested file unwritten. Refusing up front beats writing to a
        path the caller did not ask for.
        """
        client = _make_rsync_client(hostname="h", username="u")
        with patch("srunx.sync.rsync.subprocess.run") as mock_run:
            with pytest.raises(ValueError, match="must be absolute"):
                client.write_remote_file("marker.json", "x")
            mock_run.assert_not_called()

    def test_tilde_destination_is_accepted(self):
        """Mount remotes are commonly written ``~/work/...``."""
        client, ssh = self._client_with_ssh(self._ok(""))
        with patch.object(client, "_ssh_run", ssh):
            client.write_remote_file("~/work/.srunx-owner.json", "x")

        assert "~/work/.srunx-owner.json" in ssh.call_args.args[0]

    def test_temp_is_created_beside_the_target(self):
        """A temp elsewhere could be another filesystem, where mv copies."""
        client, ssh = self._client_with_ssh(self._ok(""))
        with patch.object(client, "_ssh_run", ssh):
            client.write_remote_file("/remote/ml/sub/marker.json", "x")

        assert "/remote/ml/sub/.srunx-write." in ssh.call_args.args[0]

    def test_checks_target_again_immediately_before_rename(self):
        """Guards a target swapped in after the first check.

        Without the second check, ``mv`` would follow a symlink planted in
        between and write outside the mount while reporting success.
        """
        client, ssh = self._client_with_ssh(self._ok(""))
        with patch.object(client, "_ssh_run", ssh):
            client.write_remote_file("/remote/ml/marker.json", "x")

        script = ssh.call_args.args[0]
        mv_at = script.index("mv -f --")
        # Guarded up front...
        assert script.index("-h ") < script.index('cat > "$f"')
        # ...and again immediately before the rename (the last -d test).
        assert script.index('cat > "$f"') < script.rindex("-d ") < mv_at
        # The trailing -h test is the post-rename verification, not a guard.
        assert script.rindex("-h ") > mv_at

    def test_refuses_symlink_or_directory_target(self):
        """Exit 3 is the remote guard refusing to follow / rename onto it."""
        client, ssh = self._client_with_ssh(
            MagicMock(returncode=3, stdout="", stderr="")
        )
        with patch.object(client, "_ssh_run", ssh):
            with pytest.raises(RuntimeError, match="symlink or a directory"):
                client.write_remote_file("/remote/ml/.srunx-owner.json", "x")

    def test_refuses_directory_target(self):
        """``mv temp dir`` succeeds by moving the file *inside* the directory.

        Without this guard the write reports success while the control file
        stays unreadable — and the rsync exclusion then keeps that bad
        directory around indefinitely.
        """
        client, ssh = self._client_with_ssh(
            MagicMock(returncode=4, stdout="", stderr="")
        )
        with patch.object(client, "_ssh_run", ssh):
            with pytest.raises(RuntimeError, match="exists as a directory"):
                client.write_remote_file("/remote/ml/.srunx-owner.json", "x")

    def test_connection_failure_fails_closed(self):
        """An ssh failure proves nothing about the target, so it must not pass."""
        client, ssh = self._client_with_ssh(
            MagicMock(returncode=255, stdout="", stderr="connection closed")
        )
        with patch.object(client, "_ssh_run", ssh):
            with pytest.raises(RuntimeError, match="connection closed"):
                client.write_remote_file("/remote/ml/marker.json", "x")

    def test_occupied_temp_name_reported_distinctly(self):
        """Exit 5 is the exclusive mkdir refusing whatever was planted there."""
        client, ssh = self._client_with_ssh(
            MagicMock(returncode=5, stdout="", stderr="File exists")
        )
        with patch.object(client, "_ssh_run", ssh):
            with pytest.raises(RuntimeError, match="private temp directory"):
                client.write_remote_file("/remote/ml/marker.json", "x")

    def test_write_failure_reported_distinctly(self):
        client, ssh = self._client_with_ssh(
            MagicMock(returncode=6, stdout="", stderr="disk quota exceeded")
        )
        with patch.object(client, "_ssh_run", ssh):
            with pytest.raises(RuntimeError, match="could not write the temp file"):
                client.write_remote_file("/remote/ml/marker.json", "x")

    def test_publish_failure_reported_distinctly(self):
        client, ssh = self._client_with_ssh(
            MagicMock(returncode=7, stdout="", stderr="permission denied")
        )
        with patch.object(client, "_ssh_run", ssh):
            with pytest.raises(RuntimeError, match="could not publish"):
                client.write_remote_file("/remote/ml/marker.json", "x")

    def test_verifies_the_result_of_the_rename(self):
        """The rename's outcome is checked, not assumed.

        A target swapped for a directory between the last check and the rename
        makes ``mv`` land the temp inside it and exit 0. That cannot be
        prevented portably, so it is detected instead.
        """
        client, ssh = self._client_with_ssh(self._ok(""))
        with patch.object(client, "_ssh_run", ssh):
            client.write_remote_file("/remote/ml/marker.json", "x")

        script = ssh.call_args.args[0]
        assert script.rindex("-f ") > script.index("mv -f --")
        assert "exit 8" in script

    def test_post_rename_verification_failure_is_not_success(self):
        client, ssh = self._client_with_ssh(
            MagicMock(returncode=8, stdout="", stderr="")
        )
        with patch.object(client, "_ssh_run", ssh):
            with pytest.raises(RuntimeError, match="was NOT\n?\\s*updated|was NOT"):
                client.write_remote_file("/remote/ml/marker.json", "x")

    def test_replacement_is_readable_by_other_accounts(self):
        """The mode is set at creation, not by a separate chmod.

        On a mount shared between accounts an unreadable marker reads as
        "no owner", so an owner-only marker would stop protecting the
        multi-account case the marker exists for. Doing it with ``umask``
        rather than a follow-up ``chmod`` also removes a step that would have
        reopened the temp by name — the gap a symlink swap exploits.
        """
        client, ssh = self._client_with_ssh(self._ok(""))
        with patch.object(client, "_ssh_run", ssh):
            client.write_remote_file("/remote/ml/.srunx-owner.json", "x")

        script = ssh.call_args.args[0]
        assert 'chmod 644 "$f"' in script
        # No ``--``: BSD chmod rejects it, which broke every write on macOS
        # remotes. Safe to omit since mktemp's template starts with "./".
        assert "chmod 644 --" not in script
        # Applied before the content, and before the rename publishes the inode.
        assert script.index("chmod 644") < script.index('cat > "$f"')
        # The private directory stays 700; only the published marker is 644.
        assert "mkdir -m 700" in script

    def test_detected_race_deletes_nothing(self):
        """Cleaning up through a swapped target would delete the wrong file.

        An earlier version removed ``<target>/marker`` so the relocated file
        wouldn't linger. But if the target was swapped for a symlink to another
        directory, that path resolves to an unrelated pre-existing file, and the
        cleanup deletes it — under the syncing user's credentials, outside the
        mount. A leftover costs disk space; a wrong ``rm`` costs data, so this
        path reports and touches nothing.
        """
        client, ssh = self._client_with_ssh(self._ok(""))
        with patch.object(client, "_ssh_run", ssh):
            client.write_remote_file("/remote/ml/marker.json", "x")

        script = ssh.call_args.args[0]
        # After the last rename branch, nothing is removed at all.
        tail = script[script.rindex("exit 7") :]
        assert "rm -f" not in tail
        assert tail.rstrip().endswith("exit 8; fi")

    def test_temp_is_cleaned_up_on_every_failure_path(self):
        """A temp left beside the marker would accumulate on each failure."""
        client, ssh = self._client_with_ssh(self._ok(""))
        with patch.object(client, "_ssh_run", ssh):
            client.write_remote_file("/remote/ml/marker.json", "x")

        script = ssh.call_args.args[0]
        # Everything inside the private directory is certainly ours, so every
        # failure after it exists removes the file and then the directory:
        # write failure, pre-rename recheck, failed rename.
        # Rather than count call sites (brittle), pin the invariant: every
        # deletion names the temp we created inside our own directory. Naming a
        # path under the target is the bug this replaced — if the target was
        # swapped for a symlink, deleting under it removes an unrelated file.
        assert script.count("rm -f") == script.count('rm -f -- "$f"')
        assert "/remote/ml/marker.json/" not in script
        # And the directory is only ever removed by name, never recursively.
        assert script.count("rmdir") == script.count('rmdir -- "$d"')
        assert "rm -rf" not in script


class TestRemoteCommandShell:
    """Remote commands must not depend on the account's login shell.

    OpenSSH evaluates ``ssh host "cmd"`` with the remote user's login shell.
    The ``if ...; then ...; fi`` scripts used for the owner marker and the
    post-sync hash check are not valid csh/tcsh — still the default on some
    HPC accounts — and both callers downgrade failures, so the breakage would
    be silent rather than loud.
    """

    def test_commands_are_wrapped_in_sh(self):
        client = _make_rsync_client(hostname="h", username="u")
        with patch("srunx.sync.rsync.subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
            client._ssh_run("if [ -f x ]; then echo y; fi")

        assert mock_run.call_args[0][0][-1].startswith("sh -c ")

    def test_hash_check_is_also_wrapped(self):
        """``remote_sha256`` had the same latent problem before the wrapper."""
        client = _make_rsync_client(hostname="h", username="u")
        with patch("srunx.sync.rsync.subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(
                returncode=0, stdout=f"{'a' * 64}  /r/x\n", stderr=""
            )
            client.remote_sha256("/r/x")

        remote_arg = mock_run.call_args[0][0][-1]
        assert remote_arg.startswith("sh -c ")
        assert "sha256sum" in remote_arg

    def test_hash_check_is_single_line(self):
        """csh cannot carry a newline through single quotes.

        Verified against tcsh: the multi-line form produced ``Unmatched '``,
        ``Ambiguous output redirect`` and ``else: endif not found`` — and ran
        part of the script as separate commands — even inside ``sh -c '...'``.
        """
        client = _make_rsync_client(hostname="h", username="u")
        with patch("srunx.sync.rsync.subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(
                returncode=0, stdout=f"{'a' * 64}  /r/x\n", stderr=""
            )
            client.remote_sha256("/r/x")

        assert "\n" not in mock_run.call_args[0][0][-1]

    def test_multiline_command_is_rejected(self):
        """A newline must fail loudly rather than break only on csh accounts."""
        client = _make_rsync_client(hostname="h", username="u")
        with patch("srunx.sync.rsync.subprocess.run") as mock_run:
            with pytest.raises(ValueError, match="single line"):
                client._ssh_run("echo a\necho b")
            mock_run.assert_not_called()

    def test_marker_write_is_single_line(self):
        client = _make_rsync_client(hostname="h", username="u")
        with patch("srunx.sync.rsync.subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
            client.write_remote_file("/remote/ml/.srunx-owner.json", "x")

        assert "\n" not in mock_run.call_args[0][0][-1]


class TestControlFileExcluded:
    def test_owner_marker_is_excluded_by_default(self):
        """Otherwise a mirror deletes it: there is no local counterpart.

        Losing it silently disables the guard that stops one machine from
        syncing over a mount another machine owns.
        """
        client = _make_rsync_client(hostname="h", username="u")
        assert "/.srunx-owner.json" in client.exclude_patterns

    def test_marker_survives_a_mirror(self, tmp_path: Path):
        client = _make_rsync_client(hostname="h", username="u")

        with patch("srunx.sync.rsync.subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
            client.push(tmp_path, "~/dst/", delete=True)

        cmd = mock_run.call_args[0][0]
        assert "--delete" in cmd
        # An --exclude'd path is also protected from deletion.
        idx = cmd.index("/.srunx-owner.json")
        assert cmd[idx - 1] == "--exclude"


class TestEffectiveExcludes:
    """The merged view of what a call would actually filter on.

    ``push``/``pull`` apply per-call patterns for the invocation without
    storing them, so the instance attribute alone omits exactly the mount-level
    patterns a user configured. Anything reporting the filter back to a user
    needs the merge, since an excluded path is invisible to an inspection *and*
    protected from a mirror's deletions.
    """

    def test_merges_per_call_patterns(self):
        client = _make_rsync_client(hostname="h", username="u")
        merged = client.effective_excludes(["data/raw/", "*.h5"])

        assert "data/raw/" in merged
        assert "*.h5" in merged
        # Defaults are still there.
        assert ".git/" in merged
        # And the instance attribute is left alone.
        assert "data/raw/" not in client.exclude_patterns

    def test_no_extra_returns_the_instance_patterns(self):
        client = _make_rsync_client(hostname="h", username="u")
        assert client.effective_excludes() == client.exclude_patterns
        assert client.effective_excludes(None) == client.exclude_patterns

    def test_does_not_duplicate(self):
        client = _make_rsync_client(hostname="h", username="u")
        merged = client.effective_excludes([".git/", "data/"])
        assert merged.count(".git/") == 1

    def test_returns_a_copy(self):
        """Mutating the result must not corrupt the client's own list."""
        client = _make_rsync_client(hostname="h", username="u")
        merged = client.effective_excludes()
        merged.append("injected/")
        assert "injected/" not in client.exclude_patterns

    def test_matches_what_push_actually_passes(self):
        """The reported merge must equal the flags rsync receives."""
        client = _make_rsync_client(hostname="h", username="u")
        extra = ["data/raw/", "*.h5"]

        with patch("srunx.sync.rsync.subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
            client.push("/tmp", "~/d/", exclude_patterns=extra)

        cmd = mock_run.call_args[0][0]
        passed = [cmd[i + 1] for i, a in enumerate(cmd) if a == "--exclude"]
        assert passed == client.effective_excludes(extra)
