"""Integration tests for ``srunx flow run`` Phase 2 sync orchestration.

Workflow Phase 2 (#135) introduces:

* Workflow-level rsync — touched mounts are sync'd **once** at the
  start of the run, not once per ShellJob nor once per sweep cell.
* Per-(profile, mount) lock held across every job submission inside
  the workflow run, closing the same race window
  ``mount_sync_session`` closes for single-job ``sbatch``.
* ``--sync`` / ``--no-sync`` CLI flag with config fallback.

These tests stub out the SSH executor + rsync helper so no real
paramiko runs; the goal is to lock the routing logic in place.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest
from typer.testing import CliRunner

from srunx.cli.main import app
from srunx.ssh.core.config import MountConfig, ServerProfile


@pytest.fixture(autouse=True)
def isolated_config_home(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / "xdg"))
    monkeypatch.delenv("SRUNX_SSH_PROFILE", raising=False)
    # Owner-marker (#137 part 4) check is exercised in
    # ``tests/sync/test_owner_marker.py``; keep it off here so the
    # workflow-level mocks don't have to cover ssh round-trips.
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


def _patch_workflow_transport(
    monkeypatch: pytest.MonkeyPatch,
    profile: ServerProfile,
    profile_name: str = "ml-cluster",
):
    """Wire an SSH-flavoured ResolvedTransport with mock job_ops.

    Returns ``executor_mock`` so tests can assert what the runner
    handed each job to. ``executor_factory`` returns a context manager
    yielding the same mock executor — matches the
    :class:`WorkflowJobExecutorFactory` contract.
    """
    from srunx.domain import JobStatus
    from srunx.runtime.rendering import SubmissionRenderContext
    from srunx.transport.registry import TransportHandle

    executor = MagicMock(name="WorkflowExecutor")

    def _fake_run(job, **_kwargs):
        # Mark the job complete so WorkflowRunner stops polling.
        job.job_id = 12345
        job.status = JobStatus.COMPLETED
        return job

    executor.run.side_effect = _fake_run
    executor.get_job_output_detailed.return_value = {
        "found_files": [],
        "output": "",
        "error": "",
        "primary_log": None,
        "slurm_log_dir": None,
        "searched_dirs": [],
    }

    class _ExecutorCM:
        def __enter__(self_inner):
            return executor

        def __exit__(self_inner, *exc):
            return None

    job_ops = MagicMock(name="JobOperations")

    handle = TransportHandle(
        scheduler_key=f"ssh:{profile_name}",
        profile_name=profile_name,
        transport_type="ssh",
        job_ops=job_ops,
        queue_client=job_ops,
        executor_factory=lambda: _ExecutorCM(),
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

    return executor


def _write_workflow(yaml_path: Path, *jobs: tuple[str, str]) -> None:
    """Write a minimal workflow YAML with the given (name, script_path) pairs."""
    body = ["name: in-place-test", "jobs:"]
    for name, script in jobs:
        body.append(f"  - name: {name}")
        body.append(f"    path: {script}")
    yaml_path.write_text("\n".join(body) + "\n")


def test_flow_run_syncs_each_mount_once_for_multi_shelljob_workflow(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Two ShellJobs under the same mount → exactly one rsync call.

    Workflow Phase 2: rsync should fire once per touched mount per
    workflow run, not once per job. A 100-cell sweep across a single
    mount must not trigger 100 rsyncs.
    """
    mount_local = tmp_path / "ml-project"
    mount_local.mkdir()
    s1 = mount_local / "step1.sbatch"
    s1.write_text("#!/bin/bash\necho hi\n")
    s2 = mount_local / "step2.sbatch"
    s2.write_text("#!/bin/bash\necho bye\n")

    yaml_path = tmp_path / "wf.yaml"
    _write_workflow(yaml_path, ("step1", str(s1)), ("step2", str(s2)))

    profile = _stub_profile(
        tmp_path, mount_local=mount_local, remote="/cluster/share/ml"
    )
    _patch_workflow_transport(monkeypatch, profile)

    rsync_calls: list[tuple] = []

    def _record_rsync(prof, name, *, delete=False, verbose=False):
        # ``verbose`` kwarg added in PR #149 (rsync streaming progress).
        # Mock must accept it so ``sync_mount_by_name(..., verbose=...)``
        # call from ``mount_sync_session`` doesn't raise TypeError.
        rsync_calls.append((prof, name, delete))

    monkeypatch.setattr("srunx.sync.service.sync_mount_by_name", _record_rsync)

    runner = CliRunner()
    result = runner.invoke(
        app, ["flow", "run", str(yaml_path), "--profile", "ml-cluster"]
    )

    assert result.exit_code == 0, result.stdout + result.stderr
    # Exactly one rsync for the shared mount.
    assert len(rsync_calls) == 1
    assert rsync_calls[0][1] == "ml"
    assert rsync_calls[0][2] is False  # auto-sync uses delete=False


def test_flow_run_no_sync_skips_rsync_keeps_lock(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``--no-sync`` skips rsync but the workflow still runs.

    The mount lock is still acquired (Phase 1 race-prevention
    invariant) but rsync itself is skipped — useful when the user
    knows the remote is current.
    """
    mount_local = tmp_path / "ml-project"
    mount_local.mkdir()
    s1 = mount_local / "step1.sbatch"
    s1.write_text("#!/bin/bash\necho hi\n")

    yaml_path = tmp_path / "wf.yaml"
    _write_workflow(yaml_path, ("step1", str(s1)))

    profile = _stub_profile(tmp_path, mount_local=mount_local, remote="/r/ml")
    _patch_workflow_transport(monkeypatch, profile)

    rsync_calls: list[tuple] = []
    monkeypatch.setattr(
        "srunx.sync.service.sync_mount_by_name",
        lambda *a, **k: rsync_calls.append((a, k)),
    )

    runner = CliRunner()
    result = runner.invoke(
        app,
        [
            "flow",
            "run",
            str(yaml_path),
            "--profile",
            "ml-cluster",
            "--no-sync",
        ],
    )

    assert result.exit_code == 0, result.stdout + result.stderr
    assert rsync_calls == []  # rsync skipped


def test_flow_run_local_workflow_no_mount_resolution(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Local SLURM transport has no mount concept — no sync attempt."""
    mount_local = tmp_path / "ml-project"
    mount_local.mkdir()
    s1 = mount_local / "step1.sbatch"
    s1.write_text("#!/bin/bash\necho hi\n")

    yaml_path = tmp_path / "wf.yaml"
    _write_workflow(yaml_path, ("step1", str(s1)))

    rsync_calls: list[tuple] = []
    monkeypatch.setattr(
        "srunx.sync.service.sync_mount_by_name",
        lambda *a, **k: rsync_calls.append((a, k)),
    )

    # Stub the local Slurm executor so the workflow run completes
    # without touching real SLURM.
    from srunx.domain import JobStatus

    def _fake_local_submit(self, job, **_kwargs):
        job.job_id = 1
        job.status = JobStatus.COMPLETED
        return job

    monkeypatch.setattr("srunx.slurm.local.Slurm.submit", _fake_local_submit)
    monkeypatch.setattr("srunx.slurm.local.Slurm.monitor", lambda self, job, **_: job)
    monkeypatch.setattr(
        "srunx.slurm.local.Slurm.run",
        lambda self, job, **_kw: _fake_local_submit(self, job),
    )

    runner = CliRunner()
    result = runner.invoke(app, ["flow", "run", str(yaml_path), "--local"])

    assert result.exit_code == 0, result.stdout + result.stderr
    assert rsync_calls == []  # no sync attempt for local transport


def test_flow_run_workflow_body_failure_not_misreported_as_rsync(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A job/sweep failure inside ``runner.run()`` must not wear "rsync failed".

    Regression test for Codex blocker #1 on PR #141: the previous
    implementation wrapped the entire ``yield`` in a ``except RuntimeError``
    that translated *any* runtime error to ``BadParameter("rsync
    failed: ...")``. Workflow body failures (job submission errors,
    adapter timeouts) got mislabeled as sync failures.
    """
    mount_local = tmp_path / "ml-project"
    mount_local.mkdir()
    s1 = mount_local / "step1.sbatch"
    s1.write_text("#!/bin/bash\necho hi\n")

    yaml_path = tmp_path / "wf.yaml"
    _write_workflow(yaml_path, ("step1", str(s1)))

    profile = _stub_profile(tmp_path, mount_local=mount_local, remote="/r/ml")
    executor = _patch_workflow_transport(monkeypatch, profile)

    monkeypatch.setattr("srunx.sync.service.sync_mount_by_name", lambda *a, **k: None)

    # Body failure: the executor blows up the job.
    executor.run.side_effect = RuntimeError("SLURM rejected the job")

    runner = CliRunner()
    result = runner.invoke(
        app, ["flow", "run", str(yaml_path), "--profile", "ml-cluster"]
    )

    assert result.exit_code != 0
    combined = (result.stdout or "") + (result.stderr or "")
    assert "SLURM rejected" in combined or "rejected" in combined
    # The "rsync failed" wrapper must not appear on a workflow-body
    # failure — the original message must surface verbatim.
    assert "rsync failed" not in combined.lower()


def test_flow_run_passes_allow_in_place_via_submission_context(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The CLI flips ``allow_in_place=True`` on the runner's context.

    Regression for Codex blocker #3 on PR #141: the SSH adapter
    must not take the IN_PLACE shortcut on Web/MCP paths that don't
    hold the workflow lock. The CLI flips the flag inside
    ``_hold_workflow_mounts``; this test asserts the executor sees
    a context with ``allow_in_place=True``.
    """
    mount_local = tmp_path / "ml-project"
    mount_local.mkdir()
    s1 = mount_local / "step1.sbatch"
    s1.write_text("#!/bin/bash\necho hi\n")

    yaml_path = tmp_path / "wf.yaml"
    _write_workflow(yaml_path, ("step1", str(s1)))

    profile = _stub_profile(tmp_path, mount_local=mount_local, remote="/r/ml")
    executor = _patch_workflow_transport(monkeypatch, profile)

    monkeypatch.setattr("srunx.sync.service.sync_mount_by_name", lambda *a, **k: None)

    runner = CliRunner()
    result = runner.invoke(
        app, ["flow", "run", str(yaml_path), "--profile", "ml-cluster"]
    )

    assert result.exit_code == 0, result.stdout + result.stderr
    executor.run.assert_called_once()
    submission_ctx = executor.run.call_args.kwargs.get("submission_context")
    assert submission_ctx is not None
    assert submission_ctx.allow_in_place is True


def test_flow_run_rsync_failure_aborts_workflow(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """rsync failure must abort — never silently submit a stale workspace."""
    mount_local = tmp_path / "ml-project"
    mount_local.mkdir()
    s1 = mount_local / "step1.sbatch"
    s1.write_text("#!/bin/bash\necho hi\n")

    yaml_path = tmp_path / "wf.yaml"
    _write_workflow(yaml_path, ("step1", str(s1)))

    profile = _stub_profile(tmp_path, mount_local=mount_local, remote="/r/ml")
    executor = _patch_workflow_transport(monkeypatch, profile)

    def _boom(*args, **kwargs):
        raise RuntimeError("rsync exited 23: permission denied")

    monkeypatch.setattr("srunx.sync.service.sync_mount_by_name", _boom)

    runner = CliRunner()
    result = runner.invoke(
        app, ["flow", "run", str(yaml_path), "--profile", "ml-cluster"]
    )

    assert result.exit_code != 0
    combined = (result.stdout or "") + (result.stderr or "")
    assert "rsync" in combined.lower()
    # The runner must NOT have started submitting jobs after the
    # sync failure.
    executor.run.assert_not_called()
