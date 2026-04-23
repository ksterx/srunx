"""CLI backward-compatibility regression tests.

Captures the AC-10.2 / AC-6.x contracts the transport unification spec
promises: default flag-less invocations stay byte-compatible with
pre-transport CLI, and explicit ``--profile`` routes sbatch / scancel
through the SSH adapter path (not the local ``Slurm`` singleton).
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from typer.testing import CliRunner

from srunx.cli.main import app


class TestDefaultPathSilentBanner:
    """AC-10.2: flag-less CLI must not emit a transport banner.

    The banner would change stderr byte-for-byte compared to pre-transport
    CLI output; legacy scripts that diff stderr (and our own golden tests)
    must not see any new line.
    """

    def test_sbatch_emits_no_banner_on_default_path(self):
        runner = CliRunner()
        with patch("srunx.cli.main.Slurm") as slurm_cls:
            client = MagicMock()
            client.submit.return_value = MagicMock(
                job_id=12345, name="job", command=["echo", "hi"]
            )
            slurm_cls.return_value = client
            result = runner.invoke(app, ["sbatch", "--wrap", "echo hi"])
        assert result.exit_code == 0
        # Banner suppression: no ``●`` bullet + no ``via`` source string.
        assert "via" not in result.stderr
        assert "via" not in result.output

    def test_squeue_emits_no_banner_on_default_path(self):
        runner = CliRunner()
        with patch("srunx.cli.main.Slurm") as slurm_cls:
            client = MagicMock()
            client.queue.return_value = []
            slurm_cls.return_value = client
            result = runner.invoke(app, ["squeue"])
        assert result.exit_code == 0
        assert "via" not in result.stderr


class TestExplicitSourceEmitsBanner:
    """AC-7.3: explicit transport sources print a banner on stderr."""

    def test_local_flag_emits_banner(self):
        runner = CliRunner()
        with patch("srunx.cli.main.Slurm") as slurm_cls:
            client = MagicMock()
            client.queue.return_value = []
            slurm_cls.return_value = client
            result = runner.invoke(app, ["squeue", "--local"])
        assert result.exit_code == 0
        assert "local" in result.stderr
        assert "via --local" in result.stderr


class TestSSHHappyPath:
    """AC-6.1: --profile routes through the SSH adapter.

    We swap ``_build_ssh_handle`` for a handle whose ``job_ops`` is a mock,
    so the CLI exercises the SSH branch of :func:`resolve_transport` without
    spinning up paramiko.
    """

    def _fake_handle(self, scheduler_key: str = "ssh:dgx"):
        from srunx.transport.registry import TransportHandle

        job_ops = MagicMock()
        job_ops.submit.side_effect = lambda job, **_: job
        job_ops.cancel.return_value = None
        job_ops.queue.return_value = []
        handle = TransportHandle(
            scheduler_key=scheduler_key,
            profile_name="dgx",
            transport_type="ssh",
            job_ops=job_ops,
            queue_client=job_ops,
            executor_factory=None,
            submission_context=None,
        )
        pool = MagicMock()
        pool.close.return_value = None
        return handle, pool, job_ops

    def test_scancel_with_profile_routes_through_ssh_adapter(self):
        """AC-6.1: srunx scancel --profile foo 12345 hits adapter.cancel()."""
        handle, pool, job_ops = self._fake_handle()
        runner = CliRunner()
        with patch(
            "srunx.transport.registry._build_ssh_handle",
            return_value=(handle, pool),
        ):
            result = runner.invoke(app, ["scancel", "--profile", "dgx", "12345"])
        assert result.exit_code == 0, result.output + result.stderr
        job_ops.cancel.assert_called_once_with(12345)


class TestProfileWhitespaceNormalized:
    """B fix: whitespace in --profile is stripped; empty→BadParameter."""

    def test_whitespace_only_profile_rejected(self):
        runner = CliRunner()
        result = runner.invoke(app, ["squeue", "--profile", "   "])
        assert result.exit_code != 0
        # Error message from typer.BadParameter or similar
        combined = (result.output + result.stderr).lower()
        assert "empty" in combined or "whitespace" in combined
