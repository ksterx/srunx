"""Phase 5a: CLI main.py transport integration tests.

Covers the cross-cutting transport surface added to ``sbatch`` / ``squeue``
in :mod:`srunx.cli.main`:

* Positional script vs ``--wrap`` mutual exclusion (sbatch parity).
* ``--profile`` + ``--local`` conflict detection (AC-1.2).
* ``--format json --quiet`` stdout purity (AC-7.1) — also guards the
  incidental fix where the pre-existing "No jobs in queue" banner used
  to leak into JSON output.
"""

from __future__ import annotations

import json

import pytest
from typer.testing import CliRunner

from srunx.cli.main import app


class TestSbatchScriptVsWrap:
    def test_script_and_wrap_mutually_exclusive(self, tmp_path) -> None:
        runner = CliRunner()
        script = tmp_path / "run.sh"
        script.write_text("#!/bin/bash\necho hi\n")
        result = runner.invoke(app, ["sbatch", str(script), "--wrap", "echo hi"])
        assert result.exit_code != 0
        # Typer emits the BadParameter message to either output stream
        # depending on how the Click error formatter is configured;
        # accept both so the test stays stable.
        combined = (result.stderr or "") + (result.output or "")
        assert "mutually exclusive" in combined.lower()

    def test_no_script_no_wrap_errors(self) -> None:
        runner = CliRunner()
        result = runner.invoke(app, ["sbatch"])
        assert result.exit_code != 0


class TestTransportConflict:
    def test_profile_and_local_conflict(self) -> None:
        runner = CliRunner()
        result = runner.invoke(
            app,
            ["sbatch", "--profile", "foo", "--local", "--wrap", "echo hi"],
        )
        assert result.exit_code != 0


class TestSqueueJsonBannerIsolation:
    """AC-7.1: --format json --quiet stdout must be pure JSON."""

    def test_json_quiet_stdout_is_pure(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Mock queue so no SLURM call happens."""
        from srunx.client import Slurm

        monkeypatch.setattr(Slurm, "queue", lambda self, user=None: [])
        runner = CliRunner()
        result = runner.invoke(app, ["squeue", "--format", "json", "--quiet"])
        assert result.exit_code == 0
        # Empty queue should emit ``[]`` now that the pre-existing bug
        # (human-readable "No jobs in queue" printed before the JSON
        # branch) is fixed.
        parsed = json.loads(result.stdout)
        assert parsed == []
