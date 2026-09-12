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
    srunx's own ``--quiet`` must not shadow it: ``-q normal`` is now a
    passthrough for real sbatch ``--qos=normal``. ``-Q`` is the real
    sbatch short form of ``--quiet`` and stays srunx's own quiet flag."""
    script = tmp_path / "run.sh"
    script.write_text("#!/bin/bash\necho hi\n")

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
            ["sbatch", str(script), "-Q", "--profile", "test-profile"],
        )
        assert result.exit_code == 0, result.output
        assert "No such option" not in result.output

        result = runner.invoke(
            app,
            ["sbatch", str(script), "-q", "normal", "--profile", "test-profile"],
        )
        assert result.exit_code == 0, result.output
        assert captured["extra_sbatch_args"] == ["--qos=normal"]


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


@pytest.mark.parametrize(
    "extra_argv",
    [
        ["--array=1-10"],
        ["--array", "1-10"],
        ["-a", "1-10"],
        ["-a1-10"],
    ],
)
def test_passthrough_forms_reach_extra_sbatch_args(runner, tmp_path, extra_argv):
    """All native sbatch spellings for --array normalize to the same
    extra_sbatch_args entry, reaching the CLI end-to-end (AC4)."""
    script = tmp_path / "run.sh"
    script.write_text("#!/bin/bash\necho hi\n")

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
            ["sbatch", str(script), *extra_argv, "--profile", "test-profile"],
        )

    assert result.exit_code == 0, result.output
    assert captured["extra_sbatch_args"] == ["--array=1-10"]


def test_typo_option_exits_2(runner, tmp_path):
    """A typo'd own-looking long option must not silently flow to sbatch;
    it should be rejected the same way Click rejects any unknown option."""
    script = tmp_path / "run.sh"
    script.write_text("#!/bin/bash\necho hi\n")

    result = runner.invoke(
        app,
        ["sbatch", "--profil", "dgx", str(script)],
    )
    assert result.exit_code == 2, result.output
    assert "No such option" in result.output


def test_log_dir_output_precedence(runner, tmp_path):
    """--log-dir expands first; a passthrough --output wins the later
    sbatch "last wins" precedence, while --error still comes from
    --log-dir (R5 / AC8)."""
    script = tmp_path / "run.sh"
    script.write_text("#!/bin/bash\necho hi\n")

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
                str(script),
                "--log-dir",
                "L",
                "--output",
                "O",
                "--profile",
                "test-profile",
            ],
        )

    assert result.exit_code == 0, result.output
    assert captured["extra_sbatch_args"] == [
        "--output=L/%x_%j.log",
        "--error=L/%x_%j.log",
        "--output=O",
    ]


def test_wrap_mode_forwards_passthrough_only(runner):
    """--wrap mode forwards passthrough tokens but never resource flags
    (R2.8 continued into Phase 3)."""
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
                "--array=1-10",
                "--profile",
                "test-profile",
            ],
        )

    assert result.exit_code == 0, result.output
    assert captured["extra_sbatch_args"] == ["--array=1-10"]


class TestRejectedPassthroughExitsViaCli:
    @pytest.mark.parametrize(
        "extra_argv",
        [
            ["--test-only"],
            ["--parsable"],
            ["--usage"],
            ["--version"],
            ["-V"],
        ],
    )
    def test_bare_rejected_spellings(self, runner, tmp_path, extra_argv):
        script = tmp_path / "run.sh"
        script.write_text("#!/bin/bash\necho hi\n")
        result = runner.invoke(app, ["sbatch", str(script), *extra_argv])
        assert result.exit_code == 2, result.output

    @pytest.mark.parametrize(
        "value",
        ["--quiet", "-Q", "--wrap", "--help"],
    )
    def test_sbatch_arg_rejected_spellings(self, runner, tmp_path, value):
        script = tmp_path / "run.sh"
        script.write_text("#!/bin/bash\necho hi\n")
        result = runner.invoke(
            app,
            ["sbatch", str(script), f"--sbatch-arg={value}"],
        )
        assert result.exit_code == 2, result.output


def test_help_still_works(runner, tmp_path):
    """I5, the most important regression test: the whole design rides on
    ``SbatchCommand.parse_args``; if bare --help/-h/-Q break, every own
    option silently stopped reaching Click's normal handling."""
    for argv in (["sbatch", "--help"], ["sbatch", "-h"]):
        result = runner.invoke(app, argv)
        assert result.exit_code == 0, result.output

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
            app, ["sbatch", str(script), "-Q", "--profile", "test-profile"]
        )
    assert result.exit_code == 0, result.output

    result = runner.invoke(app, ["sbatch", "--sbatch-arg=--help"])
    assert result.exit_code == 2, result.output
