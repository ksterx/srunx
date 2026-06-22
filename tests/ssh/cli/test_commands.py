"""Tests for the Typer-based SSH CLI commands (``srunx ssh ...``)."""

from __future__ import annotations

import re
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from typer.testing import CliRunner

from srunx.ssh.cli.commands import ssh_app
from srunx.ssh.core.config import ConfigManager, MountConfig, ServerProfile

# Rich/Typer styles help text with SGR escapes, which can split ``--pull``
# into ``-`` + escape + ``-pull`` depending on terminal width/colour. Strip
# them before asserting so the test doesn't depend on the rendering env.
_ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")


def _strip_ansi(text: str) -> str:
    return _ANSI_RE.sub("", text)


@pytest.fixture
def runner() -> CliRunner:
    return CliRunner()


@pytest.fixture
def config_with_mount(tmp_path: Path) -> tuple[str, str, str]:
    """Write a config with one profile + one mount.

    Returns ``(config_path, resolved_local, remote)`` so tests can assert
    the exact paths the command hands to rsync.
    """
    cfg_path = tmp_path / "config.json"
    local_dir = tmp_path / "project"
    local_dir.mkdir()
    remote = "/remote/project"

    cm = ConfigManager(str(cfg_path))
    profile = ServerProfile(
        hostname="dgx.example.com",
        username="researcher",
        key_filename="~/.ssh/id_rsa",
        mounts=[MountConfig(name="mnt", local=str(local_dir), remote=remote)],
    )
    cm.add_profile("prof", profile)

    # local is resolved by MountConfig's validator; read it back so
    # assertions compare against the exact value the command will use.
    stored = cm.get_profile("prof")
    assert stored is not None
    resolved_local = stored.mounts[0].local
    return str(cfg_path), resolved_local, remote


@pytest.fixture
def mock_rsync():
    """Patch RsyncClient so no real rsync/ssh runs; expose the instance."""
    with patch("srunx.sync.rsync.RsyncClient") as mock_cls:
        instance = mock_cls.return_value
        instance.push.return_value = MagicMock(returncode=0, stdout="", stderr="")
        instance.pull.return_value = MagicMock(returncode=0, stdout="", stderr="")
        yield instance


class TestSyncDirection:
    def test_default_is_push_local_to_remote(
        self,
        runner: CliRunner,
        config_with_mount: tuple[str, str, str],
        mock_rsync: MagicMock,
    ):
        cfg, local, remote = config_with_mount
        result = runner.invoke(ssh_app, ["sync", "prof", "mnt", "--config", cfg])

        assert result.exit_code == 0, result.output
        mock_rsync.push.assert_called_once()
        mock_rsync.pull.assert_not_called()
        args, kwargs = mock_rsync.push.call_args
        assert args == (local, remote)
        assert kwargs["dry_run"] is False
        assert kwargs["itemize"] is False

    def test_pull_reverses_to_remote_to_local(
        self,
        runner: CliRunner,
        config_with_mount: tuple[str, str, str],
        mock_rsync: MagicMock,
    ):
        cfg, local, remote = config_with_mount
        result = runner.invoke(
            ssh_app, ["sync", "prof", "mnt", "--config", cfg, "--pull"]
        )

        assert result.exit_code == 0, result.output
        mock_rsync.pull.assert_called_once()
        mock_rsync.push.assert_not_called()
        # pull(remote_path, local_path): source is remote, dest is local.
        # Remote source carries a trailing slash so rsync copies the
        # mount's contents into local rather than nesting it a level deeper.
        args, kwargs = mock_rsync.pull.call_args
        assert args == (remote.rstrip("/") + "/", local)
        assert "remote → local (pull)" in result.output


class TestSyncDryRun:
    def test_push_dry_run_enables_itemize(
        self,
        runner: CliRunner,
        config_with_mount: tuple[str, str, str],
        mock_rsync: MagicMock,
    ):
        cfg, _, _ = config_with_mount
        result = runner.invoke(
            ssh_app, ["sync", "prof", "mnt", "--config", cfg, "--dry-run"]
        )

        assert result.exit_code == 0, result.output
        kwargs = mock_rsync.push.call_args.kwargs
        assert kwargs["dry_run"] is True
        assert kwargs["itemize"] is True

    def test_pull_dry_run_enables_itemize(
        self,
        runner: CliRunner,
        config_with_mount: tuple[str, str, str],
        mock_rsync: MagicMock,
    ):
        cfg, _, _ = config_with_mount
        result = runner.invoke(
            ssh_app, ["sync", "prof", "mnt", "--config", cfg, "--pull", "-n"]
        )

        assert result.exit_code == 0, result.output
        kwargs = mock_rsync.pull.call_args.kwargs
        assert kwargs["dry_run"] is True
        assert kwargs["itemize"] is True


class TestSyncHelp:
    def test_help_lists_pull_flag(self, runner: CliRunner):
        result = runner.invoke(ssh_app, ["sync", "--help"])
        assert result.exit_code == 0
        assert "--pull" in _strip_ansi(result.output)
