"""Phase 5b: workflow CLI transport integration.

Covers the ``--profile`` / ``--local`` conflict check (REQ-1, AC-1.2)
for ``srunx flow run`` — the shared ``resolve_transport`` entry point
raises :class:`typer.BadParameter` before any SLURM work happens, so
the runner exits non-zero without touching the network.
"""

from __future__ import annotations

from pathlib import Path

from typer.testing import CliRunner

from srunx.cli.main import app


class TestFlowRunTransport:
    """``srunx flow run`` transport flag plumbing."""

    def test_profile_and_local_conflict(self, tmp_path: Path) -> None:
        yaml_file = tmp_path / "wf.yaml"
        yaml_file.write_text(
            "name: test\njobs:\n  - name: a\n    command: ['echo', 'hi']\n"
        )
        runner = CliRunner()
        result = runner.invoke(
            app,
            ["flow", "run", "--profile", "foo", "--local", str(yaml_file)],
        )
        assert result.exit_code != 0
