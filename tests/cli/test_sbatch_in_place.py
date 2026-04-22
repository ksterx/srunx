"""Integration tests for ``srunx sbatch`` with mount-aware in-place execution.

These exercise the CLI command end-to-end with the SSH adapter +
sync service mocked out, so we never spawn paramiko or rsync. The
goal is to lock in the *routing* logic: which adapter method gets
called, with what remote path, after which sync invocation.
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
    # Ensure CLI doesn't accidentally pick up the developer's local config.
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
    """Wire up an SSH-flavoured ResolvedTransport with mock job_ops + adapter.

    Returns ``(adapter_mock, job_ops_mock)`` so individual tests can
    assert exactly which adapter method was called.
    """
    from srunx.rendering import SubmissionRenderContext
    from srunx.transport.registry import TransportHandle

    adapter = MagicMock(name="SlurmSSHAdapter")
    adapter.submit_remote_sbatch.return_value = {
        "name": "job",
        "job_id": 42,
        "status": "PENDING",
        "script_path": None,
        "depends_on": [],
        "command": [],
        "resources": {},
    }
    job_ops = MagicMock(name="JobOperations")
    job_ops._adapter = adapter
    job_ops.submit.side_effect = lambda j, **_: type(j)(
        **{**j.model_dump(), "job_id": 99}
    )

    handle = TransportHandle(
        scheduler_key=f"ssh:{profile_name}",
        profile_name=profile_name,
        transport_type="ssh",
        job_ops=job_ops,
        queue_client=adapter,
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

    # Make ConfigManager.get_profile return our stub regardless of
    # the on-disk config (the CLI looks the profile up to feed mounts
    # into the planner).
    from srunx.ssh.core.config import ConfigManager

    monkeypatch.setattr(ConfigManager, "get_profile", lambda self, name: profile)

    return adapter, job_ops


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
    adapter, job_ops = _patch_transport(monkeypatch, profile)

    sync_called: list[dict] = []

    def _record_sync(**kw):  # type: ignore[no-untyped-def]
        sync_called.append(kw)
        return _ok_outcome(kw["mount"])

    monkeypatch.setattr("srunx.sync.service.ensure_mount_synced", _record_sync)

    runner = CliRunner()
    result = runner.invoke(app, ["sbatch", str(script), "--profile", "ml-cluster"])

    assert result.exit_code == 0, result.stdout + result.stderr

    # sync ran for the right mount.
    assert len(sync_called) == 1
    assert sync_called[0]["mount"].name == "ml"

    # adapter.submit_remote_sbatch was invoked with the translated
    # remote path and a remote cwd that sits under the mount.
    adapter.submit_remote_sbatch.assert_called_once()
    call_kwargs = adapter.submit_remote_sbatch.call_args
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
    adapter, job_ops = _patch_transport(monkeypatch, profile)

    runner = CliRunner()
    result = runner.invoke(
        app, ["sbatch", str(outside_script), "--profile", "ml-cluster"]
    )

    assert result.exit_code == 0, result.stdout + result.stderr
    # Legacy path: rt.job_ops.submit (not submit_remote_sbatch).
    job_ops.submit.assert_called_once()
    adapter.submit_remote_sbatch.assert_not_called()


def test_no_sync_skips_rsync_but_still_runs_in_place(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``--no-sync`` skips rsync but does NOT downgrade to tmp upload.

    The translated remote path is still used so the user's own
    ``#SBATCH --output=`` directives keep working — they may just be
    running against an older copy of the script if the workstation
    has unsynced edits.
    """
    mount_local = tmp_path / "ml-project"
    mount_local.mkdir()
    script = mount_local / "train.sbatch"
    script.write_text("#!/bin/bash\necho hi\n")

    profile = _stub_profile(tmp_path, mount_local=mount_local, remote="/r/ml-project")
    adapter, _ = _patch_transport(monkeypatch, profile)

    with patch("srunx.sync.service.ensure_mount_synced") as fake_sync:
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
    fake_sync.assert_not_called()
    adapter.submit_remote_sbatch.assert_called_once()


def test_template_flag_forces_temp_upload(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``--template`` produces a generated artifact, never in-place."""
    mount_local = tmp_path / "ml-project"
    mount_local.mkdir()
    profile = _stub_profile(tmp_path, mount_local=mount_local, remote="/r/ml-project")
    adapter, job_ops = _patch_transport(monkeypatch, profile)

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
    # --template forces TEMP_UPLOAD even though --wrap had no source
    # file to consider.
    job_ops.submit.assert_called_once()
    adapter.submit_remote_sbatch.assert_not_called()


def test_sync_failure_aborts_submission(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """rsync failure must abort — never silently submit a stale workspace."""
    mount_local = tmp_path / "ml-project"
    mount_local.mkdir()
    script = mount_local / "train.sbatch"
    script.write_text("#!/bin/bash\necho hi\n")

    profile = _stub_profile(tmp_path, mount_local=mount_local, remote="/r/ml-project")
    adapter, _ = _patch_transport(monkeypatch, profile)

    def _boom(**_kwargs):  # type: ignore[no-untyped-def]
        raise RuntimeError("rsync exited 23: permission denied")

    monkeypatch.setattr("srunx.sync.service.ensure_mount_synced", _boom)

    runner = CliRunner()
    result = runner.invoke(app, ["sbatch", str(script), "--profile", "ml-cluster"])

    assert result.exit_code != 0
    combined = (result.stdout or "") + (result.stderr or "")
    assert "rsync" in combined.lower()
    # Must NOT have submitted anything after rsync failed.
    adapter.submit_remote_sbatch.assert_not_called()


def _ok_outcome(mount):  # type: ignore[no-untyped-def]
    """Build a synthetic SyncOutcome for the mock to return."""
    from srunx.sync.service import SyncOutcome

    return SyncOutcome(mount_name=mount.name, performed=True, warnings=())
