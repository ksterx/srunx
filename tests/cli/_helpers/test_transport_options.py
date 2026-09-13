"""Tests for the shared ``QuietOpt`` alias (transport_options.py).

Moved here (from ``tests/cli/commands/jobs/test_sbatch.py``) to follow the
1:1 test-layout mirror rule: ``QuietOpt`` is defined in
``transport_options.py``, so its regression guard belongs in the test file
that mirrors that module, not in a single command's test file.
"""

from __future__ import annotations

import pytest
from typer.testing import CliRunner

from srunx.cli.main import app


@pytest.fixture
def runner() -> CliRunner:
    return CliRunner()


@pytest.mark.parametrize(
    "cli_args",
    [
        ["squeue"],
        ["scancel", "123"],
        ["sinfo"],
        ["gpus"],
        ["tail", "123"],
        ["history"],
        ["sacct"],
        ["flow", "run", "nonexistent.yaml"],
        ["watch", "jobs"],
    ],
)
def test_quiet_short_flag_propagates_to_all_commands(runner, cli_args):
    """``QuietOpt`` is a single shared Annotated alias (transport_options.py);
    this guards that every command using it keeps ``-Q`` / rejects ``-q``,
    catching a regression where one command re-declares its own option."""
    result = runner.invoke(app, [*cli_args, "-Q"])
    assert "No such option" not in result.output

    result = runner.invoke(app, [*cli_args, "-q"])
    assert result.exit_code == 2, result.output
