"""CLI-invocation tests for the flattened ``srunx ssh`` command group.

These lock the post-unification surface: flat verbs directly under ``ssh``
(no ``profile`` sub-app), ``--profile`` everywhere (no positional, no ``-p``),
``--mount`` for mounts, and ``ssh test`` stripped of its ad-hoc connection
flags. State-mutating commands run against an isolated ``--config`` file.
"""

from __future__ import annotations

import json
from pathlib import Path

from typer.testing import CliRunner

from srunx.ssh.cli.commands import ssh_app

runner = CliRunner()


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
            ["mount", "add", "--profile", "dgx", "--mount", "data",
             "--local", str(tmp_path), "--remote", "/remote/data", "--config", cfg],
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
