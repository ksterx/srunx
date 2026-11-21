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
            ["sbatch", "--env", "FOO=bar", "--cluster", "test-profile", str(script)],
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
            ["sbatch", "-Q", "--cluster", "test-profile", str(script)],
        )
        assert result.exit_code == 0, result.output
        assert "No such option" not in result.output

        result = runner.invoke(
            app,
            ["sbatch", "-q", "normal", "--cluster", "test-profile", str(script)],
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
            ["sbatch", alias, "x", "--cluster", "test-profile", str(script)],
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
                "--cluster",
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
            ["sbatch", *extra_argv, "--cluster", "test-profile", str(script)],
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
                "--log-dir",
                "L",
                "--output",
                "O",
                "--cluster",
                "test-profile",
                str(script),
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
                "--cluster",
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
        result = runner.invoke(app, ["sbatch", *extra_argv, str(script)])
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
            ["sbatch", f"--sbatch-arg={value}", str(script)],
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
            app, ["sbatch", "-Q", "--cluster", "test-profile", str(script)]
        )
    assert result.exit_code == 0, result.output

    result = runner.invoke(app, ["sbatch", "--sbatch-arg=--help"])
    assert result.exit_code == 2, result.output


# -- --snapshot / --snapshot-rev ---------------------------------------------


def _snapshot_plan():
    from pathlib import Path

    from srunx.snapshot import SnapshotPlan

    return SnapshotPlan(
        repo_top=Path("/repo"),
        repo_id="repo-123456789abc",
        sha="b" * 40,
        script_rel="job.sbatch",
        script_text="#!/bin/bash\n#SBATCH --job-name=from-commit\necho\n",
        cwd_rel=None,
        warnings=(),
    )


@pytest.fixture
def snapshot_env(monkeypatch, tmp_path):
    """Fake plan + service + profile so snapshot mode runs without git or SSH."""
    from contextlib import contextmanager as cm

    from srunx.cli._helpers import snapshot_submit as ss
    from srunx.snapshot import PreparedSnapshot

    monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path / "xdg"))
    monkeypatch.delenv("SRUNX_CLUSTER", raising=False)
    state: dict[str, object] = {"plan_calls": [], "service_calls": 0}

    def fake_plan(*, script, rev, cwd):
        state["plan_calls"].append({"script": script, "rev": rev})  # type: ignore[attr-defined]
        return _snapshot_plan()

    monkeypatch.setattr("srunx.cli.commands.jobs.sbatch.build_snapshot_plan", fake_plan)

    class _Handle:
        def __init__(self, prepared):
            self.prepared = prepared

        def metadata(self):
            return {"snapshot": {}}

        def bind(self, job_id):
            state["bound"] = job_id

    class _Service:
        def __init__(self, **_kw):
            state["service_calls"] += 1  # type: ignore[operator]

        @cm
        def leased_snapshot(self, plan):
            yield _Handle(
                PreparedSnapshot(
                    plan=plan,
                    root="/r",
                    repo_dir="/r/x",
                    path="/r/x/trees/" + plan.sha,
                    status="created",
                )
            )

    monkeypatch.setattr(ss, "SnapshotService", _Service)
    profile = MagicMock()
    profile.snapshot_root = "/snaps"
    cfg = MagicMock()
    cfg.return_value.get_profile.return_value = profile
    monkeypatch.setattr("srunx.ssh.core.config.ConfigManager", cfg)
    return state


def _rt_with_ops():
    rt = MagicMock()
    rt.transport_type = "ssh"
    rt.profile_name = "test-profile"
    rt.scheduler_key = "ssh:test-profile"

    def _remote(path, *, callbacks_job, **_kw):
        callbacks_job.job_id = 31
        return callbacks_job

    rt.job_ops.submit_remote_sbatch.side_effect = _remote
    return rt


@pytest.mark.parametrize(
    ("extra", "flag"),
    [
        (["--template", "t"], "--template"),
        (["--sync"], "--sync"),
        (["--force-sync"], "--force-sync"),
        (["--cluster", "local"], "local"),
    ],
)
def test_snapshot_rejects_conflicting_option(
    runner, snapshot_env, tmp_path, extra, flag
):
    script = tmp_path / "job.sbatch"
    script.write_text("echo\n")
    args = ["sbatch", "--snapshot", *extra, str(script)]
    if flag != "local":
        args += ["--cluster", "test-profile"]
    result = runner.invoke(app, args)
    assert result.exit_code == 2, result.output
    assert snapshot_env["plan_calls"] == []


def test_snapshot_rejects_wrap(runner, snapshot_env):
    result = runner.invoke(
        app, ["sbatch", "--snapshot", "--wrap", "x", "--cluster", "test-profile"]
    )
    assert result.exit_code == 2


def test_snapshot_requires_script(runner, snapshot_env):
    result = runner.invoke(app, ["sbatch", "--snapshot", "--cluster", "test-profile"])
    assert result.exit_code == 2


def test_snapshot_rejects_transport_resolving_to_local(runner, snapshot_env, tmp_path):
    script = tmp_path / "job.sbatch"
    script.write_text("echo\n")
    result = runner.invoke(app, ["sbatch", "--snapshot", str(script)])
    assert result.exit_code == 2
    assert snapshot_env["plan_calls"] == []


def test_snapshot_rev_implies_snapshot_and_keeps_script_positional(
    runner, snapshot_env, tmp_path
):
    script = tmp_path / "job.sbatch"
    script.write_text("echo\n")
    rt = _rt_with_ops()

    @contextmanager
    def fake_rt(*a, **k):
        yield rt

    with patch("srunx.cli.commands.jobs.sbatch.resolve_transport", fake_rt):
        result = runner.invoke(
            app,
            [
                "sbatch",
                "--snapshot-rev",
                "v1",
                "--cluster",
                "test-profile",
                str(script),
            ],
        )
    assert result.exit_code == 0, result.output
    call = snapshot_env["plan_calls"][0]  # type: ignore[index]
    assert call["rev"] == "v1"
    assert str(call["script"]) == str(script)


def test_snapshot_submit_never_syncs_or_uses_tmp_upload_and_names_from_commit(
    runner, snapshot_env, tmp_path
):
    # The local file has a different directive: the commit's content must win.
    script = tmp_path / "job.sbatch"
    script.write_text("#SBATCH --job-name=local-name\necho\n")
    rt = _rt_with_ops()

    @contextmanager
    def fake_rt(*a, **k):
        yield rt

    with (
        patch("srunx.cli.commands.jobs.sbatch.resolve_transport", fake_rt),
        patch("srunx.sync.mount_helpers.sync_mount_by_name") as sync_mock,
        patch("srunx.sync.service.mount_sync_session") as session_mock,
    ):
        result = runner.invoke(
            app, ["sbatch", "--snapshot", "--cluster", "test-profile", str(script)]
        )
    assert result.exit_code == 0, result.output
    sync_mock.assert_not_called()
    session_mock.assert_not_called()
    rt.job_ops.submit.assert_not_called()
    kwargs = rt.job_ops.submit_remote_sbatch.call_args.kwargs
    assert kwargs["callbacks_job"].name == "from-commit"
    # the commit's script declares its own name, so it is not injected
    assert kwargs["job_name"] is None
    assert snapshot_env["bound"] == 31
    assert "from-commit" in result.output


def test_snapshot_dry_run_makes_no_remote_call(runner, snapshot_env, tmp_path):
    script = tmp_path / "job.sbatch"
    script.write_text("echo\n")

    with patch("srunx.cli.commands.jobs.sbatch.resolve_transport") as rt_mock:
        result = runner.invoke(
            app,
            [
                "sbatch",
                "--snapshot",
                "--dry-run",
                "--cluster",
                "test-profile",
                str(script),
            ],
        )
    assert result.exit_code == 0, result.output
    rt_mock.assert_not_called()
    assert snapshot_env["service_calls"] == 0
    assert "b" * 40 in result.output
    assert "/snaps" in result.output
    assert "/trees/" + "b" * 40 in result.output
    # The plan's script has no --output/--chdir, so the dry run must warn
    # that the job would write into the read-only snapshot (REQ-16).
    assert "read-only" in result.output


def test_snapshot_dry_run_no_output_warning_with_absolute_output(
    runner, snapshot_env, tmp_path
):
    script = tmp_path / "job.sbatch"
    script.write_text("echo\n")

    with patch("srunx.cli.commands.jobs.sbatch.resolve_transport"):
        result = runner.invoke(
            app,
            [
                "sbatch",
                "--snapshot",
                "--dry-run",
                "--cluster",
                "test-profile",
                "--output=/abs/logs/%j.out",
                str(script),
            ],
        )
    assert result.exit_code == 0, result.output
    assert "read-only" not in result.output


@pytest.mark.parametrize(
    "extra",
    [
        ["-M", "other"],
        ["--clusters", "other"],
        ["--clusters=other"],
        ["-Mother"],
        ["--sbatch-arg=--clusters=other"],
    ],
)
def test_snapshot_rejects_clusters_passthrough(runner, snapshot_env, tmp_path, extra):
    script = tmp_path / "job.sbatch"
    script.write_text("echo\n")
    result = runner.invoke(
        app, ["sbatch", "--snapshot", "--cluster", "test-profile", *extra, str(script)]
    )
    assert result.exit_code == 2, result.output
    assert "clusters" in result.output.lower()
    assert snapshot_env["plan_calls"] == [] and snapshot_env["service_calls"] == 0


def test_snapshot_rejects_clusters_directive_in_the_committed_script(
    runner, snapshot_env, monkeypatch, tmp_path
):
    from dataclasses import replace

    plan = replace(
        _snapshot_plan(),
        script_text="#!/bin/bash\n#SBATCH --clusters=other\necho\n",
    )
    monkeypatch.setattr(
        "srunx.cli.commands.jobs.sbatch.build_snapshot_plan",
        lambda *, script, rev, cwd: plan,
    )
    script = tmp_path / "job.sbatch"
    script.write_text("echo\n")
    result = runner.invoke(
        app, ["sbatch", "--snapshot", "--cluster", "test-profile", str(script)]
    )
    assert result.exit_code == 2, result.output
    assert "clusters" in result.output.lower()
    assert snapshot_env["service_calls"] == 0


def test_clusters_passthrough_is_still_accepted_without_snapshot(runner, tmp_path):
    from srunx.cli._helpers.snapshot_submit import validate_snapshot_options

    assert (
        validate_snapshot_options(
            snapshot=False,
            snapshot_rev=None,
            script=None,
            wrap=None,
            template=None,
            sync=None,
            force_sync=False,
            sbatch_args=["--clusters=other"],
        )
        is False
    )
