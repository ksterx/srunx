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

    def _record_rsync(prof, name, *, delete=False):  # type: ignore[no-untyped-def]
        rsync_calls.append((prof, name, delete))

    monkeypatch.setattr("srunx.sync.service.sync_mount_by_name", _record_rsync)

    runner = CliRunner()
    result = runner.invoke(app, ["sbatch", str(script), "--profile", "ml-cluster"])

    assert result.exit_code == 0, result.stdout + result.stderr

    # rsync ran for the right mount, and used delete=False (auto-sync
    # must not wipe remote-only outputs — Codex blocker #4).
    assert len(rsync_calls) == 1
    assert rsync_calls[0][1] == "ml"
    assert rsync_calls[0][2] is False

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

    def _boom(profile_arg, mount_name, *, delete=False):  # type: ignore[no-untyped-def]
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
