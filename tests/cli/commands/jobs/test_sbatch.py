"""Tests for ``srunx sbatch`` job-building (env forwarding).

Focus: REQ-3 / AC-4 — a positional-script submission must forward the
built ``JobEnvironment`` into the ``ShellJob`` instead of dropping it, so
``--env`` is effective for scripts identically to ``--wrap``.

We patch ``_submit_via_transport`` to capture the *built* job (and patch
``resolve_transport`` to a no-op context) so the assertion is on the model
the CLI constructed, not on any real SLURM/SSH I/O.
"""

from __future__ import annotations

from contextlib import contextmanager
from unittest.mock import MagicMock, patch

import pytest
from typer.testing import CliRunner

from srunx.cli.main import app
from srunx.domain import ShellJob


@pytest.fixture
def runner() -> CliRunner:
    return CliRunner()


@contextmanager
def _fake_resolve_transport(*args, **kwargs):
    rt = MagicMock()
    rt.transport_type = "ssh"  # keeps `client` None (no local Slurm build)
    rt.profile_name = "test-profile"
    rt.scheduler_key = "ssh:test-profile"
    yield rt


def test_positional_script_forwards_env_to_shell_job(runner, tmp_path):
    script = tmp_path / "run.sh"
    script.write_text("#!/bin/bash\necho hi\n")

    captured: dict[str, object] = {}

    def fake_submit(*, job, **kwargs):
        captured["job"] = job
        job.job_id = 12345
        return job

    with (
        patch(
            "srunx.cli.commands.jobs.sbatch.resolve_transport",
            _fake_resolve_transport,
        ),
        patch(
            "srunx.cli.commands.jobs.sbatch._submit_via_transport",
            side_effect=fake_submit,
        ),
    ):
        result = runner.invoke(
            app,
            ["sbatch", str(script), "--env", "FOO=bar", "--profile", "test-profile"],
        )

    assert result.exit_code == 0, result.output
    built = captured["job"]
    assert isinstance(built, ShellJob)
    assert built.environment.env_vars == {"FOO": "bar"}


def test_quiet_short_flag_is_Q_not_q(runner, tmp_path):
    """``-q`` is real SLURM's short form of ``--qos`` (a value option), so
    srunx's own ``--quiet`` must not shadow it. ``-Q`` is the real sbatch
    short form of ``--quiet``.

    NOTE: this will be rewritten in Phase 3 to assert
    ``-q normal`` -> ``extra_sbatch_args == ["--qos=normal"]`` once ``-q``
    is repurposed as a passthrough for real sbatch ``--qos``.
    """
    script = tmp_path / "run.sh"
    script.write_text("#!/bin/bash\necho hi\n")

    def fake_submit(*, job, **kwargs):
        job.job_id = 12345
        return job

    with (
        patch(
            "srunx.cli.commands.jobs.sbatch.resolve_transport",
            _fake_resolve_transport,
        ),
        patch(
            "srunx.cli.commands.jobs.sbatch._submit_via_transport",
            side_effect=fake_submit,
        ),
    ):
        result = runner.invoke(
            app,
            ["sbatch", str(script), "-Q", "--profile", "test-profile"],
        )
        assert result.exit_code == 0, result.output
        assert "No such option" not in result.output

        result = runner.invoke(
            app,
            ["sbatch", str(script), "-q", "--profile", "test-profile"],
        )
        assert result.exit_code == 2, result.output


@pytest.mark.parametrize(
    "alias",
    ["--name", "--time-limit", "--memory", "--work-dir"],
)
def test_removed_alias_exits_2(runner, tmp_path, alias):
    """These long-form aliases duplicated a shorter/native flag
    (--job-name, --time, --mem, --chdir respectively) and are removed to
    match real sbatch's option surface (R4)."""
    script = tmp_path / "run.sh"
    script.write_text("#!/bin/bash\necho hi\n")

    def fake_submit(*, job, **kwargs):
        job.job_id = 12345
        return job

    with (
        patch(
            "srunx.cli.commands.jobs.sbatch.resolve_transport",
            _fake_resolve_transport,
        ),
        patch(
            "srunx.cli.commands.jobs.sbatch._submit_via_transport",
            side_effect=fake_submit,
        ),
    ):
        result = runner.invoke(
            app,
            ["sbatch", str(script), alias, "x", "--profile", "test-profile"],
        )
    assert result.exit_code == 2, result.output


def test_wrap_mode_does_not_forward_resource_flags(runner):
    """--wrap mode: resource flags are baked into the rendered template's
    #SBATCH directives, so they must NOT also appear on extra_sbatch_args
    (R2.8) — duplicating them on the command line risks drift."""
    captured: dict[str, object] = {}

    def fake_submit(*, extra_sbatch_args=None, job, **kwargs):
        captured["extra_sbatch_args"] = extra_sbatch_args
        job.job_id = 12345
        return job

    with (
        patch(
            "srunx.cli.commands.jobs.sbatch.resolve_transport",
            _fake_resolve_transport,
        ),
        patch(
            "srunx.cli.commands.jobs.sbatch._submit_via_transport",
            side_effect=fake_submit,
        ),
    ):
        result = runner.invoke(
            app,
            [
                "sbatch",
                "--wrap",
                "echo hi",
                "-t",
                "5:00",
                "--profile",
                "test-profile",
            ],
        )

    assert result.exit_code == 0, result.output
    assert not captured["extra_sbatch_args"]
