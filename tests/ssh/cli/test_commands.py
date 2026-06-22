"""CLI-invocation tests for the flattened ``srunx ssh`` command group.

These lock the post-unification surface: flat verbs directly under ``ssh``
(no ``profile`` sub-app), ``--profile`` everywhere (no positional, no ``-p``),
``--mount`` for mounts, and ``ssh test`` stripped of its ad-hoc connection
flags. State-mutating commands run against an isolated ``--config`` file.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from typer.testing import CliRunner

from srunx.ssh.cli.commands import ssh_app
from srunx.ssh.core.config import ConfigManager, MountConfig, ServerProfile

runner = CliRunner()

# Rich/Typer styles help text with SGR escapes that can split ``--pull``;
# strip them before asserting so the test doesn't depend on the render env.
_ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")


def _strip_ansi(text: str) -> str:
    return _ANSI_RE.sub("", text)


def _cfg(tmp_path: Path) -> str:
    return str(tmp_path / "config.json")


class TestFlatStructure:
    def test_top_level_verbs_present(self):
        result = runner.invoke(ssh_app, ["--help"])
        assert result.exit_code == 0
        for verb in ("add", "list", "show", "use", "remove", "update", "test", "sync"):
            assert verb in result.output
        # Sub-entity groups survive as one-level groups.
        assert "mount" in result.output
        assert "env" in result.output

    def test_profile_subcommand_is_gone(self):
        # The old `srunx ssh profile ...` nesting must no longer exist.
        result = runner.invoke(ssh_app, ["profile", "--help"])
        assert result.exit_code != 0


class TestProfileFlag:
    def test_use_requires_profile(self):
        result = runner.invoke(ssh_app, ["use"])
        assert result.exit_code != 0  # missing required --profile

    def test_add_requires_profile(self):
        result = runner.invoke(ssh_app, ["add"])
        assert result.exit_code != 0

    def test_use_accepts_profile_flag(self, tmp_path: Path):
        # Seed a profile, then `use` it via --profile.
        cfg = _cfg(tmp_path)
        add = runner.invoke(
            ssh_app,
            ["add", "--profile", "p1", "--ssh-host", "p1-host", "--config", cfg],
        )
        assert add.exit_code == 0, add.output
        used = runner.invoke(ssh_app, ["use", "--profile", "p1", "--config", cfg])
        assert used.exit_code == 0, used.output
        data = json.loads(Path(cfg).read_text())
        assert data["current_profile"] == "p1"


class TestSshTestFlags:
    def test_no_transient_connection_flags(self):
        result = runner.invoke(ssh_app, ["test", "--help"])
        assert result.exit_code == 0
        assert "--hostname" not in result.output
        assert "--username" not in result.output
        assert "--key-file" not in result.output
        # --profile present, --host alias kept.
        assert "--profile" in result.output
        assert "--host" in result.output

    def test_profile_has_no_short_p_flag(self):
        # `-p` is reserved for --partition across srunx; ssh test must not bind it.
        result = runner.invoke(ssh_app, ["test", "--help"])
        assert " -p " not in result.output
        assert "-p," not in result.output


class TestRoundTrip:
    def test_add_list_show(self, tmp_path: Path):
        cfg = _cfg(tmp_path)
        assert (
            runner.invoke(
                ssh_app,
                ["add", "--profile", "dgx", "--ssh-host", "dgx-host", "--config", cfg],
            ).exit_code
            == 0
        )
        listed = runner.invoke(ssh_app, ["list", "--config", cfg])
        assert listed.exit_code == 0
        assert "dgx" in listed.output

        shown = runner.invoke(ssh_app, ["show", "--profile", "dgx", "--config", cfg])
        assert shown.exit_code == 0
        assert "dgx-host" in shown.output

    def test_mount_add_list_via_flags(self, tmp_path: Path):
        cfg = _cfg(tmp_path)
        runner.invoke(
            ssh_app,
            ["add", "--profile", "dgx", "--ssh-host", "dgx-host", "--config", cfg],
        )
        added = runner.invoke(
            ssh_app,
            [
                "mount",
                "add",
                "--profile",
                "dgx",
                "--mount",
                "data",
                "--local",
                str(tmp_path),
                "--remote",
                "/remote/data",
                "--config",
                cfg,
            ],
        )
        assert added.exit_code == 0, added.output
        listed = runner.invoke(
            ssh_app, ["mount", "list", "--profile", "dgx", "--config", cfg]
        )
        assert listed.exit_code == 0
        assert "data" in listed.output

    def test_env_set_list_via_flags(self, tmp_path: Path):
        cfg = _cfg(tmp_path)
        runner.invoke(
            ssh_app,
            ["add", "--profile", "dgx", "--ssh-host", "dgx-host", "--config", cfg],
        )
        setres = runner.invoke(
            ssh_app,
            ["env", "set", "--profile", "dgx", "FOO", "bar", "--config", cfg],
        )
        assert setres.exit_code == 0, setres.output
        listed = runner.invoke(
            ssh_app, ["env", "list", "--profile", "dgx", "--config", cfg]
        )
        assert listed.exit_code == 0
        assert "FOO" in listed.output


class TestCurrentProfileFallback:
    """Optional-profile commands fall back to the current profile, honouring
    the cli.use_current_profile opt-out."""

    def _seed_current(self, tmp_path: Path) -> str:
        cfg = _cfg(tmp_path)
        runner.invoke(
            ssh_app,
            ["add", "--profile", "dgx", "--ssh-host", "dgx-host", "--config", cfg],
        )
        runner.invoke(ssh_app, ["use", "--profile", "dgx", "--config", cfg])
        return cfg

    def test_show_falls_back_to_current(self, tmp_path: Path):
        cfg = self._seed_current(tmp_path)
        # Default config has use_current_profile=True -> show w/o --profile works.
        res = runner.invoke(ssh_app, ["show", "--config", cfg])
        assert res.exit_code == 0, res.output
        assert "dgx-host" in res.output

    def test_show_honors_optout(self, tmp_path: Path, monkeypatch):
        cfg = self._seed_current(tmp_path)
        # Disable implicit current-profile selection: show w/o --profile errors.
        import srunx.common.config as cc

        fake = MagicMock()
        fake.cli.use_current_profile = False
        monkeypatch.setattr(cc, "get_config", lambda: fake)
        res = runner.invoke(ssh_app, ["show", "--config", cfg])
        assert res.exit_code != 0


@pytest.fixture
def config_with_mount(tmp_path: Path) -> tuple[str, str, str]:
    """Write a config with one profile + one mount.

    Returns ``(config_path, resolved_local, remote)`` so tests can assert the
    exact paths the command hands to rsync.
    """
    cfg_path = tmp_path / "sync_config.json"
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
    """`ssh sync` pushes local→remote by default and pulls with --pull (#208),
    exercised through the flattened --profile/--mount surface."""

    def test_default_is_push_local_to_remote(self, config_with_mount, mock_rsync):
        cfg, local, remote = config_with_mount
        result = runner.invoke(
            ssh_app, ["sync", "--profile", "prof", "--mount", "mnt", "--config", cfg]
        )
        assert result.exit_code == 0, result.output
        mock_rsync.push.assert_called_once()
        mock_rsync.pull.assert_not_called()
        args, kwargs = mock_rsync.push.call_args
        assert args == (local, remote)
        assert kwargs["dry_run"] is False
        assert kwargs["itemize"] is False

    def test_pull_reverses_to_remote_to_local(self, config_with_mount, mock_rsync):
        cfg, local, remote = config_with_mount
        result = runner.invoke(
            ssh_app,
            ["sync", "--profile", "prof", "--mount", "mnt", "--config", cfg, "--pull"],
        )
        assert result.exit_code == 0, result.output
        mock_rsync.pull.assert_called_once()
        mock_rsync.push.assert_not_called()
        # pull(remote_path, local_path): remote source carries a trailing
        # slash so rsync copies the mount's contents into local.
        args, kwargs = mock_rsync.pull.call_args
        assert args == (remote.rstrip("/") + "/", local)
        assert "remote → local (pull)" in result.output


class TestSyncDryRun:
    def test_push_dry_run_enables_itemize(self, config_with_mount, mock_rsync):
        cfg, _, _ = config_with_mount
        result = runner.invoke(
            ssh_app,
            [
                "sync",
                "--profile",
                "prof",
                "--mount",
                "mnt",
                "--config",
                cfg,
                "--dry-run",
            ],
        )
        assert result.exit_code == 0, result.output
        kwargs = mock_rsync.push.call_args.kwargs
        assert kwargs["dry_run"] is True
        assert kwargs["itemize"] is True

    def test_pull_dry_run_enables_itemize(self, config_with_mount, mock_rsync):
        cfg, _, _ = config_with_mount
        result = runner.invoke(
            ssh_app,
            [
                "sync",
                "--profile",
                "prof",
                "--mount",
                "mnt",
                "--config",
                cfg,
                "--pull",
                "-n",
            ],
        )
        assert result.exit_code == 0, result.output
        kwargs = mock_rsync.pull.call_args.kwargs
        assert kwargs["dry_run"] is True
        assert kwargs["itemize"] is True


class TestSyncHelp:
    def test_help_lists_pull_flag(self):
        result = runner.invoke(ssh_app, ["sync", "--help"])
        assert result.exit_code == 0
        assert "--pull" in _strip_ansi(result.output)
