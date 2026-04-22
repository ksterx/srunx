"""Phase 7: ssh subcommand deprecation warnings."""

from __future__ import annotations

from typer.testing import CliRunner


class TestSshSubmitDeprecation:
    def test_warning_on_stderr(self, tmp_path):
        """AC-9.1: ssh submit emits deprecation warning to stderr."""
        from srunx.cli.main import app

        script = tmp_path / "foo.sh"
        script.write_text("#!/bin/bash\necho hi\n")
        runner = CliRunner()
        # Use an invalid profile so the command fails fast without touching
        # real SSH. The deprecation warning fires before any profile lookup,
        # so it should appear on stderr regardless of the eventual exit code.
        result = runner.invoke(
            app,
            ["ssh", "submit", str(script), "--profile", "nonexistent_profile_xyz"],
            catch_exceptions=False,
        )
        assert "deprecated" in result.stderr.lower()


class TestSshLogsDeprecation:
    def test_warning_on_stderr(self):
        from srunx.cli.main import app

        runner = CliRunner()
        result = runner.invoke(
            app,
            ["ssh", "logs", "12345", "--profile", "nonexistent_profile_xyz"],
            catch_exceptions=False,
        )
        assert "deprecated" in result.stderr.lower()


class TestNoDeprecationOnOtherCommands:
    """AC-9.2, AC-9.3: ssh profile list / ssh sync have no warning."""

    def test_ssh_profile_list_no_warning(self):
        from srunx.cli.main import app

        runner = CliRunner()
        result = runner.invoke(app, ["ssh", "profile", "list"], catch_exceptions=False)
        assert "deprecated" not in result.stderr.lower()

    def test_ssh_sync_help_no_warning(self):
        from srunx.cli.main import app

        runner = CliRunner()
        # --help avoids needing real SSH.
        result = runner.invoke(app, ["ssh", "sync", "--help"], catch_exceptions=False)
        assert "deprecated" not in result.stderr.lower()
