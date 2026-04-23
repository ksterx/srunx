"""Integration tests for ``srunx sbatch`` with mount-aware in-place execution.

These exercise the CLI command end-to-end with the SSH adapter +
the rsync helper mocked out, so we never spawn paramiko or rsync.
The lock layer is left real (``XDG_CONFIG_HOME`` is sandboxed by the
autouse fixture) so the lock-held-across-submit invariant from
Codex blocker #3 is exercised by the same code path it protects.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from typer.testing import CliRunner

from srunx.cli.main import app
from srunx.ssh.core.config import MountConfig, ServerProfile


@pytest.fixture(autouse=True)
def isolated_config_home(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / "xdg"))
    monkeypatch.delenv("SRUNX_SSH_PROFILE", raising=False)
    # Disable the per-machine owner marker check (#137 part 4) for
    # the existing in-place tests — they predate the marker and only
    # care about the rsync / sbatch / lock interactions. Owner-marker
    # behaviour is exercised separately in
    # ``tests/sync/test_owner_marker.py``.
    monkeypatch.setenv("SRUNX_SYNC_OWNER_CHECK", "0")


def _stub_profile(tmp_path: Path, mount_local: Path, remote: str) -> ServerProfile:
    key = tmp_path / "id_rsa"
    key.write_text("dummy")
    return ServerProfile(
        hostname="h",
        username="u",
        key_filename=str(key),
        mounts=(MountConfig(name="ml", local=str(mount_local), remote=remote),),
    )


def _patch_transport(
    monkeypatch: pytest.MonkeyPatch,
    profile: ServerProfile,
    profile_name: str = "ml-cluster",
):
    """Wire up an SSH-flavoured ResolvedTransport with mock job_ops.

    The mock ``job_ops`` exposes both the legacy ``submit`` (tmp-upload
    path) and the new ``submit_remote_sbatch`` (in-place path) so each
    test can assert exactly which one fired. ``submit_remote_sbatch``
    mutates and returns the supplied ``callbacks_job`` to mimic the
    real adapter's contract.
    """
    from srunx.models import JobStatus
    from srunx.rendering import SubmissionRenderContext
    from srunx.transport.registry import TransportHandle

    job_ops = MagicMock(name="JobOperations")
    job_ops.submit.side_effect = lambda j, **_: type(j)(
        **{**j.model_dump(), "job_id": 99}
    )

    def _fake_remote_submit(remote_path, *, callbacks_job, **_kwargs):
        callbacks_job.job_id = 42
        callbacks_job.status = JobStatus.PENDING
        if hasattr(callbacks_job, "script_path"):
            callbacks_job.script_path = remote_path
        return callbacks_job

    job_ops.submit_remote_sbatch.side_effect = _fake_remote_submit

    handle = TransportHandle(
        scheduler_key=f"ssh:{profile_name}",
        profile_name=profile_name,
        transport_type="ssh",
        job_ops=job_ops,
        queue_client=job_ops,
        executor_factory=None,
        submission_context=SubmissionRenderContext(
            mount_name=None,
            mounts=tuple(profile.mounts),
            default_work_dir=None,
        ),
    )

    def _fake_build(
        profile_name_arg,
        *,
        callbacks=None,
        submission_source="web",
        mount_name=None,
        pool_size=2,
    ):
        return handle, MagicMock(name="pool")

    monkeypatch.setattr("srunx.transport.registry._build_ssh_handle", _fake_build)

    from srunx.ssh.core.config import ConfigManager

    monkeypatch.setattr(ConfigManager, "get_profile", lambda self, name: profile)

    return job_ops


def test_sbatch_in_place_under_mount_calls_remote_submit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``srunx sbatch <mount-resident-script> --profile X`` runs in place."""
    mount_local = tmp_path / "ml-project"
    mount_local.mkdir()
    script = mount_local / "train.sbatch"
    script.write_text("#!/bin/bash\n#SBATCH --job-name=train\necho hi\n")

    profile = _stub_profile(
        tmp_path, mount_local=mount_local, remote="/cluster/share/ml-project"
    )
    job_ops = _patch_transport(monkeypatch, profile)

    rsync_calls: list[tuple] = []

    def _record_rsync(prof, name, *, delete=False, verbose=False):  # type: ignore[no-untyped-def]
        rsync_calls.append((prof, name, delete, verbose))

    monkeypatch.setattr("srunx.sync.service.sync_mount_by_name", _record_rsync)

    runner = CliRunner()
    result = runner.invoke(app, ["sbatch", str(script), "--profile", "ml-cluster"])

    assert result.exit_code == 0, result.stdout + result.stderr

    # rsync ran for the right mount, and used delete=False (auto-sync
    # must not wipe remote-only outputs — Codex blocker #4). Default
    # ``verbose=False`` since the user did not pass ``--verbose``.
    assert len(rsync_calls) == 1
    assert rsync_calls[0][1] == "ml"
    assert rsync_calls[0][2] is False
    assert rsync_calls[0][3] is False

    # Protocol method (no _adapter reach-in) was invoked with the
    # translated remote path and a remote cwd under the mount.
    job_ops.submit_remote_sbatch.assert_called_once()
    call_kwargs = job_ops.submit_remote_sbatch.call_args
    assert call_kwargs.args[0] == "/cluster/share/ml-project/train.sbatch"
    assert call_kwargs.kwargs["submit_cwd"].startswith("/cluster/share/ml-project")

    # The legacy temp-upload path must NOT have fired.
    job_ops.submit.assert_not_called()


def test_sbatch_outside_mount_falls_back_to_temp_upload(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A script outside any mount continues to use the legacy upload path."""
    mount_local = tmp_path / "ml-project"
    mount_local.mkdir()
    outside_script = tmp_path / "scratch" / "throwaway.sbatch"
    outside_script.parent.mkdir()
    outside_script.write_text("#!/bin/bash\necho hi\n")

    profile = _stub_profile(
        tmp_path, mount_local=mount_local, remote="/cluster/share/ml-project"
    )
    job_ops = _patch_transport(monkeypatch, profile)

    runner = CliRunner()
    result = runner.invoke(
        app, ["sbatch", str(outside_script), "--profile", "ml-cluster"]
    )

    assert result.exit_code == 0, result.stdout + result.stderr
    job_ops.submit.assert_called_once()
    job_ops.submit_remote_sbatch.assert_not_called()


def test_no_sync_skips_rsync_but_still_runs_in_place(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``--no-sync`` skips rsync but does NOT downgrade to tmp upload."""
    mount_local = tmp_path / "ml-project"
    mount_local.mkdir()
    script = mount_local / "train.sbatch"
    script.write_text("#!/bin/bash\necho hi\n")

    profile = _stub_profile(tmp_path, mount_local=mount_local, remote="/r/ml-project")
    job_ops = _patch_transport(monkeypatch, profile)

    with patch("srunx.sync.service.sync_mount_by_name") as fake_rsync:
        runner = CliRunner()
        result = runner.invoke(
            app,
            [
                "sbatch",
                str(script),
                "--profile",
                "ml-cluster",
                "--no-sync",
            ],
        )

    assert result.exit_code == 0, result.stdout + result.stderr
    # rsync skipped, but the in-place sbatch still ran.
    fake_rsync.assert_not_called()
    job_ops.submit_remote_sbatch.assert_called_once()


def test_template_flag_forces_temp_upload(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``--template`` produces a generated artifact, never in-place."""
    mount_local = tmp_path / "ml-project"
    mount_local.mkdir()
    profile = _stub_profile(tmp_path, mount_local=mount_local, remote="/r/ml-project")
    job_ops = _patch_transport(monkeypatch, profile)

    runner = CliRunner()
    result = runner.invoke(
        app,
        [
            "sbatch",
            "--wrap",
            "echo hi",
            "--template",
            "base",
            "--profile",
            "ml-cluster",
        ],
    )

    assert result.exit_code == 0, result.stdout + result.stderr
    job_ops.submit.assert_called_once()
    job_ops.submit_remote_sbatch.assert_not_called()


def test_sync_failure_aborts_submission(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """rsync failure must abort — never silently submit a stale workspace."""
    mount_local = tmp_path / "ml-project"
    mount_local.mkdir()
    script = mount_local / "train.sbatch"
    script.write_text("#!/bin/bash\necho hi\n")

    profile = _stub_profile(tmp_path, mount_local=mount_local, remote="/r/ml-project")
    job_ops = _patch_transport(monkeypatch, profile)

    def _boom(profile_arg, mount_name, *, delete=False, verbose=False):  # type: ignore[no-untyped-def]
        raise RuntimeError("rsync exited 23: permission denied")

    monkeypatch.setattr("srunx.sync.service.sync_mount_by_name", _boom)

    runner = CliRunner()
    result = runner.invoke(app, ["sbatch", str(script), "--profile", "ml-cluster"])

    assert result.exit_code != 0
    combined = (result.stdout or "") + (result.stderr or "")
    assert "rsync" in combined.lower()
    job_ops.submit_remote_sbatch.assert_not_called()


def test_extra_sbatch_args_forwarded_in_place(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """CLI resource flags reach sbatch in IN_PLACE mode (Codex blocker #1)."""
    mount_local = tmp_path / "ml-project"
    mount_local.mkdir()
    script = mount_local / "train.sbatch"
    script.write_text("#!/bin/bash\necho hi\n")

    profile = _stub_profile(tmp_path, mount_local=mount_local, remote="/r/ml-project")
    job_ops = _patch_transport(monkeypatch, profile)

    monkeypatch.setattr("srunx.sync.service.sync_mount_by_name", lambda *a, **k: None)

    runner = CliRunner()
    result = runner.invoke(
        app,
        [
            "sbatch",
            str(script),
            "--profile",
            "ml-cluster",
            "-N",
            "4",
            "--gres",
            "gpu:2",
            "-t",
            "1:00:00",
        ],
    )

    assert result.exit_code == 0, result.stdout + result.stderr
    extra = job_ops.submit_remote_sbatch.call_args.kwargs["extra_sbatch_args"]
    assert "--nodes=4" in extra
    assert "--gpus-per-node=2" in extra  # --gres parsed into gpus_per_node
    assert "--time=1:00:00" in extra


def test_lock_is_held_during_submit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The per-mount sync lock is held while ``submit_remote_sbatch`` runs.

    Codex follow-up #4 on PR #134: the previous review pointed out
    that no test directly proves this. We assert the contract by
    checking that, *during* the adapter call, a parallel
    ``acquire_sync_lock`` would time out — i.e. the file lock is
    actually held the whole time the submission is in flight.
    """
    from srunx.sync.lock import SyncLockTimeoutError, acquire_sync_lock

    mount_local = tmp_path / "ml-project"
    mount_local.mkdir()
    script = mount_local / "train.sbatch"
    script.write_text("#!/bin/bash\necho hi\n")

    profile = _stub_profile(tmp_path, mount_local=mount_local, remote="/r/ml-project")
    job_ops = _patch_transport(monkeypatch, profile)

    monkeypatch.setattr("srunx.sync.service.sync_mount_by_name", lambda *a, **k: None)

    held_during_submit: dict[str, bool] = {}

    def _check_lock_held(remote_path, *, callbacks_job, **_kwargs):
        # Try to acquire the same lock with a tiny timeout. If the
        # outer mount_sync_session is honouring the contract this
        # second acquire MUST time out, because the OS lock is held.
        try:
            with acquire_sync_lock("ml-cluster", "ml", timeout=0.2):
                held_during_submit["acquired"] = True
        except SyncLockTimeoutError:
            held_during_submit["acquired"] = False

        callbacks_job.job_id = 99
        from srunx.models import JobStatus

        callbacks_job.status = JobStatus.PENDING
        return callbacks_job

    job_ops.submit_remote_sbatch.side_effect = _check_lock_held

    runner = CliRunner()
    result = runner.invoke(app, ["sbatch", str(script), "--profile", "ml-cluster"])

    assert result.exit_code == 0, result.stdout + result.stderr
    # The contended acquire timed out → the outer lock was held the
    # entire time submit_remote_sbatch was running.
    assert held_during_submit["acquired"] is False, (
        "mount_sync_session released the lock before submit_remote_sbatch "
        "finished — a concurrent invocation could rsync stale bytes "
        "during the sbatch handoff window. (Codex blocker #3 regressed.)"
    )


def test_sbatch_failure_after_sync_uses_distinct_message(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A sbatch failure must NOT wear an "rsync failed" error message.

    Codex follow-up regression #1 on PR #134: the original fix
    wrapped both the sync and the submit in one ``except RuntimeError
    as exc: raise BadParameter(f"rsync failed: ...")`` block, so a
    sbatch error claimed the rsync failed.
    """
    mount_local = tmp_path / "ml-project"
    mount_local.mkdir()
    script = mount_local / "train.sbatch"
    script.write_text("#!/bin/bash\necho hi\n")

    profile = _stub_profile(tmp_path, mount_local=mount_local, remote="/r/ml-project")
    job_ops = _patch_transport(monkeypatch, profile)

    monkeypatch.setattr("srunx.sync.service.sync_mount_by_name", lambda *a, **k: None)

    job_ops.submit_remote_sbatch.side_effect = RuntimeError(
        "remote sbatch submission failed"
    )

    runner = CliRunner()
    result = runner.invoke(app, ["sbatch", str(script), "--profile", "ml-cluster"])

    assert result.exit_code != 0
    combined = (result.stdout or "") + (result.stderr or "")
    assert "sbatch failed" in combined.lower()
    assert "rsync failed" not in combined.lower()


def test_default_resource_flags_not_forwarded(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Unstated CLI flags don't shadow the script's own ``#SBATCH`` directives.

    The script may set ``#SBATCH --nodes=8``; we must not stomp on that
    just because the CLI defaults to ``--nodes=1``.
    """
    mount_local = tmp_path / "ml-project"
    mount_local.mkdir()
    script = mount_local / "train.sbatch"
    script.write_text("#!/bin/bash\n#SBATCH --nodes=8\necho hi\n")

    profile = _stub_profile(tmp_path, mount_local=mount_local, remote="/r/ml-project")
    job_ops = _patch_transport(monkeypatch, profile)

    monkeypatch.setattr("srunx.sync.service.sync_mount_by_name", lambda *a, **k: None)

    runner = CliRunner()
    result = runner.invoke(app, ["sbatch", str(script), "--profile", "ml-cluster"])

    assert result.exit_code == 0, result.stdout + result.stderr
    extra = job_ops.submit_remote_sbatch.call_args.kwargs["extra_sbatch_args"]
    # ``extra_sbatch_args`` is None or empty when no flags were typed.
    assert not extra


def test_explicit_default_override_is_forwarded(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``-N 1`` typed explicitly must override a script's ``#SBATCH --nodes=8``.

    Codex follow-up #1 to PR #134's first round: the original
    "default !=" check would skip explicit default-valued flags
    because the typed value matches the default. The new
    ``get_parameter_source``-based check fixes that.
    """
    mount_local = tmp_path / "ml-project"
    mount_local.mkdir()
    script = mount_local / "train.sbatch"
    script.write_text("#!/bin/bash\n#SBATCH --nodes=8\necho hi\n")

    profile = _stub_profile(tmp_path, mount_local=mount_local, remote="/r/ml-project")
    job_ops = _patch_transport(monkeypatch, profile)
    monkeypatch.setattr("srunx.sync.service.sync_mount_by_name", lambda *a, **k: None)

    runner = CliRunner()
    result = runner.invoke(
        app,
        ["sbatch", str(script), "--profile", "ml-cluster", "-N", "1"],
    )

    assert result.exit_code == 0, result.stdout + result.stderr
    extra = job_ops.submit_remote_sbatch.call_args.kwargs["extra_sbatch_args"]
    # The user typed ``-N 1`` explicitly. It must be forwarded so
    # SLURM overrides the script's ``#SBATCH --nodes=8``.
    assert "--nodes=1" in extra


def test_config_workdir_does_not_inject_chdir(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Config-default ``work_dir`` must NOT be forwarded as ``--chdir``.

    Codex follow-up #1 (continued): without ParameterSource, a
    config-injected ``work_dir`` looked indistinguishable from a
    user-typed ``-D``, so the script's own ``#SBATCH --chdir=`` got
    silently overridden. ``ParameterSource`` distinguishes the two.
    """
    mount_local = tmp_path / "ml-project"
    mount_local.mkdir()
    script = mount_local / "train.sbatch"
    script.write_text(
        "#!/bin/bash\n#SBATCH --chdir=/cluster/share/ml-project\necho hi\n"
    )

    # Inject a config-default work_dir that differs from the script's
    # explicit value. Without the fix the planner forwards the config
    # default and clobbers the script's choice.
    # ``srunx.cli/__init__.py`` re-exports the ``main`` function as
    # ``srunx.cli.main``, shadowing the module attribute lookup
    # so ``import srunx.cli.main as X`` returns the function. Reach
    # for the module via ``sys.modules`` directly so the patch lands
    # on the module's namespace where ``get_config`` is rebound.
    import sys

    from srunx.config import SrunxConfig

    cli_main_module = sys.modules["srunx.cli.main"]
    monkeypatch.setattr(
        cli_main_module,
        "get_config",
        lambda: SrunxConfig.model_validate(
            {
                "work_dir": "/some/config/default",
                # Match the autouse env-var override — the injected
                # config bypasses ``load_config`` so the env var
                # doesn't reach this construction path.
                "sync": {"owner_check": False},
            }
        ),
    )

    profile = _stub_profile(tmp_path, mount_local=mount_local, remote="/r/ml-project")
    job_ops = _patch_transport(monkeypatch, profile)
    monkeypatch.setattr("srunx.sync.service.sync_mount_by_name", lambda *a, **k: None)

    runner = CliRunner()
    result = runner.invoke(app, ["sbatch", str(script), "--profile", "ml-cluster"])

    assert result.exit_code == 0, result.stdout + result.stderr
    extra = job_ops.submit_remote_sbatch.call_args.kwargs["extra_sbatch_args"]
    assert not any(a.startswith("--chdir") for a in (extra or [])), (
        "Config-default work_dir leaked into sbatch CLI args, would have "
        "overridden the script's own #SBATCH --chdir=."
    )


# ── Dry-run sync preview (#137 part 2) ────────────────────────────


def test_dry_run_shows_in_place_sync_preview(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``sbatch --dry-run`` against a mount-resident script previews rsync.

    Lets the user spot a stray ``build/`` they forgot to gitignore
    BEFORE the actual sync ships it. The preview lines come from
    rsync's ``-n -i`` output (mocked here so we don't actually shell
    out).
    """
    mount_local = tmp_path / "ml-project"
    mount_local.mkdir()
    script = mount_local / "train.sbatch"
    script.write_text("#!/bin/bash\necho hi\n")

    profile = _stub_profile(tmp_path, mount_local=mount_local, remote="/r/ml-project")
    _patch_transport(monkeypatch, profile)

    # Mock the mount-helper rsync to return three itemize lines as
    # though rsync had real changes to report.
    fake_output = (
        ">f.st...... train.sbatch\ncd+++++++++ build/\n>f+++++++++ build/output.bin\n"
    )
    sync_calls: list[dict[str, object]] = []

    def _fake_sync(
        prof: ServerProfile,
        name: str,
        *,
        delete: bool = False,
        dry_run: bool = False,
    ) -> str:
        sync_calls.append({"name": name, "delete": delete, "dry_run": dry_run})
        return fake_output if dry_run else ""

    monkeypatch.setattr("srunx.sync.mount_helpers.sync_mount_by_name", _fake_sync)

    runner = CliRunner()
    result = runner.invoke(
        app,
        ["sbatch", str(script), "--profile", "ml-cluster", "--dry-run"],
    )

    assert result.exit_code == 0, result.stdout + result.stderr
    assert "Dry run mode" in result.stdout
    assert "Sync preview for mount" in result.stdout
    # Each itemize line surfaces verbatim under the preview header.
    for line in fake_output.splitlines():
        assert line in result.stdout

    # The preview path uses dry_run=True so nothing is actually pushed.
    assert sync_calls == [{"name": "ml", "delete": False, "dry_run": True}]


def test_dry_run_no_sync_message_when_sync_disabled(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``--no-sync --dry-run`` skips the rsync subprocess but still tells the user.

    Important so the user knows *why* there's no preview — without
    the explicit message it'd look like the mount auto-detection
    failed.
    """
    mount_local = tmp_path / "ml-project"
    mount_local.mkdir()
    script = mount_local / "train.sbatch"
    script.write_text("#!/bin/bash\necho hi\n")

    profile = _stub_profile(tmp_path, mount_local=mount_local, remote="/r/ml-project")
    _patch_transport(monkeypatch, profile)

    sync_called = False

    def _should_not_run(*a: object, **k: object) -> str:
        nonlocal sync_called
        sync_called = True
        return ""

    monkeypatch.setattr("srunx.sync.mount_helpers.sync_mount_by_name", _should_not_run)

    runner = CliRunner()
    result = runner.invoke(
        app,
        [
            "sbatch",
            str(script),
            "--profile",
            "ml-cluster",
            "--dry-run",
            "--no-sync",
        ],
    )

    assert result.exit_code == 0, result.stdout + result.stderr
    assert "skipped (--no-sync)" in result.stdout
    assert not sync_called, "rsync must not run when --no-sync is set"


def test_verbose_forwards_to_sync_mount_by_name(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``srunx sbatch --verbose`` reaches sync_mount_by_name with verbose=True.

    The actual streaming behaviour is exercised in ``tests/test_rsync.py``
    — here we only assert the wiring from the CLI flag through
    ``mount_sync_session`` lands on the rsync helper.
    """
    mount_local = tmp_path / "ml-project"
    mount_local.mkdir()
    script = mount_local / "train.sbatch"
    script.write_text("#!/bin/bash\necho hi\n")

    profile = _stub_profile(tmp_path, mount_local=mount_local, remote="/r/ml-project")
    _patch_transport(monkeypatch, profile)

    rsync_calls: list[dict[str, object]] = []

    def _record(prof, name, *, delete=False, verbose=False):  # type: ignore[no-untyped-def]
        rsync_calls.append({"name": name, "delete": delete, "verbose": verbose})

    monkeypatch.setattr("srunx.sync.service.sync_mount_by_name", _record)

    runner = CliRunner()
    result = runner.invoke(
        app,
        ["sbatch", str(script), "--profile", "ml-cluster", "--verbose"],
    )

    assert result.exit_code == 0, result.stdout + result.stderr
    assert rsync_calls == [{"name": "ml", "delete": False, "verbose": True}]


def test_no_verbose_keeps_quiet_default(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Without ``--verbose`` the CLI passes ``verbose=False`` (legacy path)."""
    mount_local = tmp_path / "ml-project"
    mount_local.mkdir()
    script = mount_local / "train.sbatch"
    script.write_text("#!/bin/bash\necho hi\n")

    profile = _stub_profile(tmp_path, mount_local=mount_local, remote="/r/ml-project")
    _patch_transport(monkeypatch, profile)

    rsync_calls: list[dict[str, object]] = []

    def _record(prof, name, *, delete=False, verbose=False):  # type: ignore[no-untyped-def]
        rsync_calls.append({"name": name, "delete": delete, "verbose": verbose})

    monkeypatch.setattr("srunx.sync.service.sync_mount_by_name", _record)

    runner = CliRunner()
    result = runner.invoke(app, ["sbatch", str(script), "--profile", "ml-cluster"])

    assert result.exit_code == 0, result.stdout + result.stderr
    assert rsync_calls == [{"name": "ml", "delete": False, "verbose": False}]


def test_dry_run_preview_silent_for_local_transport(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Local sbatch dry-run shows job info only — no SSH/sync chatter.

    The preview is an SSH-only feature; running a local sbatch
    against a script that *happens* to share a path with some mount
    must NOT trigger rsync calls or print mount-related lines.
    """
    script = tmp_path / "train.sbatch"
    script.write_text("#!/bin/bash\necho hi\n")

    sync_called = False

    def _record(*a: object, **k: object) -> None:
        nonlocal sync_called
        sync_called = True

    monkeypatch.setattr("srunx.sync.mount_helpers.sync_mount_by_name", _record)

    runner = CliRunner()
    result = runner.invoke(app, ["sbatch", str(script), "--local", "--dry-run"])

    assert result.exit_code == 0, result.stdout + result.stderr
    assert "Sync preview" not in result.stdout
    assert "skipped (--no-sync)" not in result.stdout
    assert not sync_called
