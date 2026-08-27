"""Tests for job-name handling in ``srunx sbatch``.

Covers two coupled behaviours introduced to fix the SSH job-name bug:

* **Injection suppression** — when the user does NOT type ``-J`` on a
  positional script, srunx must not inject ``--job-name`` on the sbatch
  command line (which would override the script's own ``#SBATCH
  --job-name``). This applies to both SSH submission paths: TEMP_UPLOAD
  (``submit``) and IN_PLACE (``submit_remote_sbatch``).
* **Offline name resolution (系統2)** — srunx's logical ``job.name``
  (shown in CLI output / recorded in history) is resolved before submit
  the way SLURM itself would: explicit ``-J`` wins, else the script's
  own ``#SBATCH --job-name`` / ``-J`` directive, else the script's
  basename. No post-submit ``squeue``/``sacct`` query.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest
from typer.testing import CliRunner

from srunx.cli._helpers.sbatch_helpers import (
    _resolve_job_name,
    _script_job_name,
)
from srunx.cli.main import app
from srunx.ssh.core.config import MountConfig, ServerProfile

# ── Unit: offline #SBATCH job-name parser ─────────────────────────────


def _write(tmp_path: Path, body: str, name: str = "job.sbatch") -> Path:
    p = tmp_path / name
    p.write_text(body)
    return p


def test_script_job_name_equals_form(tmp_path: Path) -> None:
    script = _write(tmp_path, "#!/bin/bash\n#SBATCH --job-name=g4_26b\necho hi\n")
    assert _script_job_name(script) == "g4_26b"


def test_script_job_name_space_form(tmp_path: Path) -> None:
    script = _write(tmp_path, "#!/bin/bash\n#SBATCH --job-name my_job\necho hi\n")
    assert _script_job_name(script) == "my_job"


def test_script_job_name_short_flag_forms(tmp_path: Path) -> None:
    assert _script_job_name(_write(tmp_path, "#SBATCH -J short1\n")) == "short1"
    assert _script_job_name(_write(tmp_path, "#SBATCH -Jshort2\n")) == "short2"


def test_script_job_name_last_directive_wins(tmp_path: Path) -> None:
    script = _write(
        tmp_path,
        "#!/bin/bash\n#SBATCH --job-name=first\n#SBATCH --job-name=second\necho hi\n",
    )
    assert _script_job_name(script) == "second"


def test_script_job_name_stops_at_first_command(tmp_path: Path) -> None:
    # A ``--job-name`` appearing AFTER the first executable line is not a
    # directive SLURM would honour, so we must not pick it up.
    script = _write(
        tmp_path,
        "#!/bin/bash\necho start\n#SBATCH --job-name=too_late\n",
    )
    assert _script_job_name(script) is None


def test_script_job_name_none_when_absent(tmp_path: Path) -> None:
    script = _write(tmp_path, "#!/bin/bash\n#SBATCH --nodes=2\necho hi\n")
    assert _script_job_name(script) is None


def test_script_job_name_unreadable_returns_none(tmp_path: Path) -> None:
    assert _script_job_name(tmp_path / "does-not-exist.sbatch") is None


# ── Unit: name resolution + injection decision ────────────────────────


def test_resolve_explicit_wins_and_injects(tmp_path: Path) -> None:
    # Explicit -J overrides even a script directive, and is injected.
    script = _write(tmp_path, "#SBATCH --job-name=from_script\n")
    assert _resolve_job_name(script, "cli_name", cli_name_explicit=True) == (
        "cli_name",
        True,
    )


def test_resolve_from_directive_suppresses_injection(tmp_path: Path) -> None:
    # Script declares its own name and no -J → use it AND don't inject
    # (let the directive win on the scheduler).
    script = _write(tmp_path, "#!/bin/bash\n#SBATCH --job-name=from_script\necho hi\n")
    assert _resolve_job_name(script, "job", cli_name_explicit=False) == (
        "from_script",
        False,
    )


def test_resolve_basename_fallback_injects(tmp_path: Path) -> None:
    # No directive, no -J → basename, and DO inject it so the scheduler
    # agrees with srunx (the temp-upload path would otherwise name the job
    # after a random job_<uuid>.sh).
    script = _write(tmp_path, "#!/bin/bash\necho hi\n", name="train.sbatch")
    assert _resolve_job_name(script, "job", cli_name_explicit=False) == (
        "train.sbatch",
        True,
    )


# ── Integration harness (SSH transport with mocked job_ops) ───────────


def _stub_profile(tmp_path: Path, mount_local: Path, remote: str) -> ServerProfile:
    key = tmp_path / "id_rsa"
    key.write_text("dummy")
    return ServerProfile(
        hostname="h",
        username="u",
        key_filename=str(key),
        mounts=(MountConfig(name="ml", local=str(mount_local), remote=remote),),
    )


@pytest.fixture(autouse=True)
def _isolated(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / "xdg"))
    monkeypatch.delenv("SRUNX_SSH_PROFILE", raising=False)
    monkeypatch.setenv("SRUNX_SYNC_OWNER_CHECK", "0")


def _patch_transport(
    monkeypatch: pytest.MonkeyPatch,
    profile: ServerProfile,
    profile_name: str = "ml-cluster",
) -> MagicMock:
    from srunx.domain import JobStatus
    from srunx.runtime.rendering import SubmissionRenderContext
    from srunx.transport.registry import TransportHandle

    job_ops = MagicMock(name="JobOperations")
    # ``submit`` (TEMP_UPLOAD) returns a copy of the job with a job_id so
    # the CLI's post-submit display reads the resolved ``job.name``.
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
        allow_cwd_mount=False,
        pool_size=2,
    ):
        return handle, MagicMock(name="pool")

    monkeypatch.setattr("srunx.transport.registry._build_ssh_handle", _fake_build)
    monkeypatch.setattr("srunx.sync.service.sync_mount_by_name", lambda *a, **k: None)

    from srunx.ssh.core.config import ConfigManager

    monkeypatch.setattr(ConfigManager, "get_profile", lambda self, name: profile)
    return job_ops


# ── IN_PLACE path (script under a mount) ──────────────────────────────


def test_in_place_no_explicit_j_suppresses_job_name(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    mount_local = tmp_path / "ml-project"
    mount_local.mkdir()
    script = mount_local / "train.sbatch"
    script.write_text("#!/bin/bash\n#SBATCH --job-name=g4_26b\necho hi\n")
    profile = _stub_profile(tmp_path, mount_local, "/r/ml-project")
    job_ops = _patch_transport(monkeypatch, profile)

    result = CliRunner().invoke(app, ["sbatch", str(script), "--profile", "ml-cluster"])
    assert result.exit_code == 0, result.stdout + result.stderr

    # No -J → don't inject --job-name; let the script's directive win.
    assert job_ops.submit_remote_sbatch.call_args.kwargs["job_name"] is None
    # 系統2: srunx's displayed name mirrors the script's directive.
    assert "Job name: g4_26b" in result.stdout


def test_in_place_explicit_j_injects_job_name(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    mount_local = tmp_path / "ml-project"
    mount_local.mkdir()
    script = mount_local / "train.sbatch"
    script.write_text("#!/bin/bash\n#SBATCH --job-name=g4_26b\necho hi\n")
    profile = _stub_profile(tmp_path, mount_local, "/r/ml-project")
    job_ops = _patch_transport(monkeypatch, profile)

    result = CliRunner().invoke(
        app, ["sbatch", str(script), "--profile", "ml-cluster", "-J", "explicit"]
    )
    assert result.exit_code == 0, result.stdout + result.stderr
    assert job_ops.submit_remote_sbatch.call_args.kwargs["job_name"] == "explicit"
    assert "Job name: explicit" in result.stdout


def test_in_place_no_directive_injects_basename(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # No #SBATCH --job-name and no -J: nothing to protect, so inject the
    # resolved basename (matches SLURM's own default for an in-place run).
    mount_local = tmp_path / "ml-project"
    mount_local.mkdir()
    script = mount_local / "plain.sbatch"
    script.write_text("#!/bin/bash\necho hi\n")
    profile = _stub_profile(tmp_path, mount_local, "/r/ml-project")
    job_ops = _patch_transport(monkeypatch, profile)

    result = CliRunner().invoke(app, ["sbatch", str(script), "--profile", "ml-cluster"])
    assert result.exit_code == 0, result.stdout + result.stderr
    assert job_ops.submit_remote_sbatch.call_args.kwargs["job_name"] == "plain.sbatch"
    assert "Job name: plain.sbatch" in result.stdout


# ── TEMP_UPLOAD path (script outside every mount) ─────────────────────


def test_temp_upload_no_explicit_j_suppresses_job_name(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    mount_local = tmp_path / "ml-project"
    mount_local.mkdir()
    outside = tmp_path / "scratch" / "run.sbatch"
    outside.parent.mkdir()
    outside.write_text("#!/bin/bash\n#SBATCH --job-name=g4_26b\necho hi\n")
    profile = _stub_profile(tmp_path, mount_local, "/r/ml-project")
    job_ops = _patch_transport(monkeypatch, profile)

    result = CliRunner().invoke(
        app, ["sbatch", str(outside), "--profile", "ml-cluster"]
    )
    assert result.exit_code == 0, result.stdout + result.stderr

    job_ops.submit.assert_called_once()
    assert job_ops.submit.call_args.kwargs["inject_job_name"] is False
    assert "Job name: g4_26b" in result.stdout


def test_temp_upload_explicit_j_injects_job_name(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    mount_local = tmp_path / "ml-project"
    mount_local.mkdir()
    outside = tmp_path / "scratch" / "run.sbatch"
    outside.parent.mkdir()
    outside.write_text("#!/bin/bash\n#SBATCH --job-name=g4_26b\necho hi\n")
    profile = _stub_profile(tmp_path, mount_local, "/r/ml-project")
    job_ops = _patch_transport(monkeypatch, profile)

    result = CliRunner().invoke(
        app, ["sbatch", str(outside), "--profile", "ml-cluster", "-J", "explicit"]
    )
    assert result.exit_code == 0, result.stdout + result.stderr
    job_ops.submit.assert_called_once()
    assert job_ops.submit.call_args.kwargs["inject_job_name"] is True
    assert "Job name: explicit" in result.stdout


def test_temp_upload_no_directive_injects_basename(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Outside every mount (TEMP_UPLOAD), no directive, no -J: the script is
    # uploaded to a random job_<uuid>.sh, so SLURM's default name would be
    # that opaque temp name. Inject the resolved basename so the scheduler
    # matches what srunx records.
    mount_local = tmp_path / "ml-project"
    mount_local.mkdir()
    outside = tmp_path / "scratch" / "run.sbatch"
    outside.parent.mkdir()
    outside.write_text("#!/bin/bash\necho hi\n")
    profile = _stub_profile(tmp_path, mount_local, "/r/ml-project")
    job_ops = _patch_transport(monkeypatch, profile)

    result = CliRunner().invoke(
        app, ["sbatch", str(outside), "--profile", "ml-cluster"]
    )
    assert result.exit_code == 0, result.stdout + result.stderr
    job_ops.submit.assert_called_once()
    assert job_ops.submit.call_args.kwargs["inject_job_name"] is True
    assert "Job name: run.sbatch" in result.stdout
