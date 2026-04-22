"""Phase 5b: monitor CLI transport integration.

Covers the ``--profile`` / ``--local`` conflict check (REQ-1, AC-1.2)
for ``srunx monitor jobs`` — the shared ``resolve_transport`` entry
point raises :class:`typer.BadParameter` before any SLURM work happens,
so the runner exits non-zero without touching the network.
"""

from __future__ import annotations

from typer.testing import CliRunner

from srunx.cli.main import app


class TestMonitorJobsTransport:
    """``srunx monitor jobs`` transport flag plumbing."""

    def test_profile_and_local_conflict(self) -> None:
        runner = CliRunner()
        result = runner.invoke(
            app,
            ["monitor", "jobs", "--profile", "foo", "--local", "12345"],
        )
        assert result.exit_code != 0


class TestMonitorResourcesTransport:
    """``srunx monitor resources`` accepts the options as future no-ops.

    Phase 5b documents that the flags are available for CLI parity even
    though ResourceMonitor itself still queries the local cluster. The
    conflict check must still fire on ``--profile`` + ``--local``.
    """

    def test_profile_and_local_conflict(self) -> None:
        runner = CliRunner()
        result = runner.invoke(
            app,
            [
                "monitor",
                "resources",
                "--min-gpus",
                "1",
                "--profile",
                "foo",
                "--local",
            ],
        )
        assert result.exit_code != 0
