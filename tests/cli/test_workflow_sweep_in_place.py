"""Tests for sweep cell-aware mount aggregation + IN_PLACE wiring (#143).

Sweep cells re-render the workflow YAML with cell-specific Jinja
overrides, so axes that influence ``ShellJob.script_path`` can move a
cell to a mount the base render never touches. These tests cover:

* The pure helper ``collect_touched_mounts_across_cells`` — verifies
  matrix expansion + per-cell render + mount union.
* CLI integration: a sweep whose ``script_path`` depends on a sweep
  axis runs IN_PLACE for every cell, with one rsync per touched
  mount (not one per cell).
* CLI integration: sweep where the cell-resolved ``script_path``
  lives outside any mount falls back to TEMP_UPLOAD for that cell
  (the per-job ``_resolve_in_place_target`` logic still applies).
* The locked_mount_names defence-in-depth: a hypothetical cell that
  somehow escapes the aggregation hits a clear "mount X not locked"
  error in the SSH adapter rather than silently racing rsync.
* The IN_PLACE flag is flipped on the submission_context for sweeps
  (the executor sees ``allow_in_place=True``), proving #143's net
  user-visible win.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest
from typer.testing import CliRunner

from srunx.cli.main import app
from srunx.cli.submission_plan import collect_touched_mounts_across_cells
from srunx.ssh.core.client_types import SlurmJob
from srunx.ssh.core.config import MountConfig, ServerProfile


@pytest.fixture(autouse=True)
def isolated_config_home(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / "xdg"))
    monkeypatch.delenv("SRUNX_SSH_PROFILE", raising=False)
    monkeypatch.setenv("SRUNX_SYNC_OWNER_CHECK", "0")


def _make_profile(*mounts: MountConfig) -> ServerProfile:
    """Build a stub profile with the supplied mounts (no real key on disk)."""
    return ServerProfile(
        hostname="h",
        username="u",
        key_filename="/tmp/never-read",
        mounts=list(mounts),
    )


# ─────────────────────────────────────────────────────────────────────────
# Unit tests for collect_touched_mounts_across_cells (no SLURM, no SSH)
# ─────────────────────────────────────────────────────────────────────────


class TestCollectTouchedMountsAcrossCells:
    """Pure-Python coverage of the per-cell mount aggregation helper."""

    def test_static_script_path_resolves_single_mount(self, tmp_path: Path) -> None:
        """A non-parameterised script_path → single mount in union."""
        mount_local = tmp_path / "ml"
        mount_local.mkdir()
        s = mount_local / "train.sh"
        s.write_text("#!/bin/bash\n")

        yaml_path = tmp_path / "wf.yaml"
        yaml_path.write_text(
            f"name: t\nargs:\n  seed: 1\njobs:\n  - name: j\n    path: {s}\n"
        )

        profile = _make_profile(
            MountConfig(name="ml", local=str(mount_local), remote="/r/ml")
        )
        cells = [{"seed": 1}, {"seed": 2}, {"seed": 3}]

        mounts = collect_touched_mounts_across_cells(yaml_path, None, cells, profile)
        assert [m.name for m in mounts] == ["ml"]

    def test_axis_driven_script_path_resolves_same_mount_per_cell(
        self, tmp_path: Path
    ) -> None:
        """``path: ".../{{ seed }}/run.sh"`` under one mount → still 1 entry.

        The canonical positive #143 case: every cell resolves under
        the *same* mount, so the union has one element. The base
        render alone would also see this mount — but the helper has
        to handle it correctly, so prove it.
        """
        mount_local = tmp_path / "scratch"
        mount_local.mkdir()
        for seed in (1, 2, 3):
            d = mount_local / str(seed)
            d.mkdir()
            (d / "run.sh").write_text("#!/bin/bash\n")

        yaml_path = tmp_path / "wf.yaml"
        yaml_path.write_text(
            "name: t\n"
            "args:\n  seed: 1\n"
            "jobs:\n  - name: j\n"
            f'    path: "{mount_local}/{{{{ seed }}}}/run.sh"\n'
        )

        profile = _make_profile(
            MountConfig(name="scratch", local=str(mount_local), remote="/r/sc")
        )
        cells = [{"seed": 1}, {"seed": 2}, {"seed": 3}]

        mounts = collect_touched_mounts_across_cells(yaml_path, None, cells, profile)
        assert [m.name for m in mounts] == ["scratch"]

    def test_axis_driven_script_path_unions_multiple_mounts(
        self, tmp_path: Path
    ) -> None:
        """A sweep axis that picks between mounts → both end up in union.

        The bug #143 fixes: without per-cell aggregation we'd lock
        only one mount; with it both appear and the lock-set is sound.
        """
        mount_a = tmp_path / "data_a"
        mount_a.mkdir()
        (mount_a / "run.sh").write_text("#!/bin/bash\n")
        mount_b = tmp_path / "data_b"
        mount_b.mkdir()
        (mount_b / "run.sh").write_text("#!/bin/bash\n")

        yaml_path = tmp_path / "wf.yaml"
        yaml_path.write_text(
            "name: t\n"
            "args:\n  dataset: a\n"
            "jobs:\n  - name: j\n"
            f'    path: "{tmp_path}/data_{{{{ dataset }}}}/run.sh"\n'
        )

        profile = _make_profile(
            MountConfig(name="data_a", local=str(mount_a), remote="/r/a"),
            MountConfig(name="data_b", local=str(mount_b), remote="/r/b"),
        )
        cells = [{"dataset": "a"}, {"dataset": "b"}]

        mounts = collect_touched_mounts_across_cells(yaml_path, None, cells, profile)
        # Sorted by profile.mounts insertion order.
        assert [m.name for m in mounts] == ["data_a", "data_b"]

    def test_failing_cell_render_does_not_drop_other_cells(
        self, tmp_path: Path
    ) -> None:
        """A cell whose Jinja render explodes does not hide its peers' mounts.

        The validator + per-cell submission both re-render later, so
        the user still sees the failure. The aggregation step must
        not let one broken cell prevent the lock for everyone else.
        """
        mount_local = tmp_path / "ml"
        mount_local.mkdir()
        (mount_local / "run.sh").write_text("#!/bin/bash\n")

        yaml_path = tmp_path / "wf.yaml"
        # Job's ``path`` interpolates ``{{ optional_var }}``; cell 0
        # has no ``optional_var`` so StrictUndefined explodes, cell 1
        # provides it and renders fine. ShellJobs can't combine
        # ``path`` + ``command`` (mutually exclusive at parse time)
        # so the divergence has to live inside ``path`` itself.
        yaml_path.write_text(
            "name: t\n"
            "args: {}\n"
            "jobs:\n  - name: j\n"
            f'    path: "{mount_local}/{{{{ optional_var }}}}.sh"\n'
        )
        # Pre-create the cell-1 script so the post-render path
        # actually resolves.
        (mount_local / "ok.sh").write_text("#!/bin/bash\n")

        profile = _make_profile(
            MountConfig(name="ml", local=str(mount_local), remote="/r/ml")
        )
        cells = [{}, {"optional_var": "ok"}]

        mounts = collect_touched_mounts_across_cells(yaml_path, None, cells, profile)
        # Cell 0 explodes (StrictUndefined on `optional_var`); cell 1
        # renders fine and contributes the mount.
        assert [m.name for m in mounts] == ["ml"]

    def test_paths_outside_every_mount_contribute_nothing(self, tmp_path: Path) -> None:
        """Cells whose script_path lives outside every mount → empty union."""
        outside = tmp_path / "outside"
        outside.mkdir()
        (outside / "run.sh").write_text("#!/bin/bash\n")

        yaml_path = tmp_path / "wf.yaml"
        yaml_path.write_text(
            "name: t\n"
            "args:\n  seed: 1\n"
            "jobs:\n  - name: j\n"
            f"    path: {outside}/run.sh\n"
        )

        # Mount points elsewhere — the script doesn't fall under it.
        elsewhere = tmp_path / "ml"
        elsewhere.mkdir()
        profile = _make_profile(
            MountConfig(name="ml", local=str(elsewhere), remote="/r/ml")
        )

        mounts = collect_touched_mounts_across_cells(
            yaml_path, None, [{"seed": 1}], profile
        )
        assert mounts == []


# ─────────────────────────────────────────────────────────────────────────
# CLI integration: sweep flips allow_in_place + sees per-cell mounts
# ─────────────────────────────────────────────────────────────────────────


def _patch_sweep_transport(
    monkeypatch: pytest.MonkeyPatch,
    profile: ServerProfile,
    profile_name: str = "ml-cluster",
):
    """Stub SSH transport for sweep tests; capture per-cell submission_context.

    Mirrors ``test_workflow_in_place._patch_workflow_transport`` but
    keeps the executor visible across cells so we can assert how
    many times each method was called.
    """
    from srunx.models import JobStatus
    from srunx.rendering import SubmissionRenderContext
    from srunx.transport.registry import TransportHandle

    executor = MagicMock(name="WorkflowExecutor")

    def _fake_run(job, **_kwargs):
        job.job_id = 9999
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


def _stub_profile(tmp_path: Path, mount_local: Path, remote: str) -> ServerProfile:
    key = tmp_path / "id_rsa"
    key.write_text("dummy")
    return ServerProfile(
        hostname="h",
        username="u",
        key_filename=str(key),
        mounts=(MountConfig(name="ml", local=str(mount_local), remote=remote),),
    )


class TestCLISweepInPlace:
    """End-to-end coverage of the sweep IN_PLACE wiring (#143)."""

    def test_static_script_path_sweep_runs_in_place(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Sweep with non-parameterised script_path → IN_PLACE for every cell."""
        mount_local = tmp_path / "ml-project"
        mount_local.mkdir()
        s = mount_local / "step.sbatch"
        s.write_text("#!/bin/bash\necho hi\n")

        yaml_path = tmp_path / "wf.yaml"
        yaml_path.write_text(
            "name: sweep_in_place\n"
            "args:\n  seed: 1\n"
            "sweep:\n"
            "  matrix:\n    seed: [1, 2]\n"
            "  max_parallel: 2\n"
            f"jobs:\n  - name: j\n    path: {s}\n"
        )

        profile = _stub_profile(tmp_path, mount_local=mount_local, remote="/r/ml")
        executor = _patch_sweep_transport(monkeypatch, profile)

        rsync_calls: list[tuple] = []
        monkeypatch.setattr(
            "srunx.sync.service.sync_mount_by_name",
            lambda *a, **k: rsync_calls.append((a, k)),
        )

        runner = CliRunner()
        result = runner.invoke(
            app, ["flow", "run", str(yaml_path), "--profile", "ml-cluster"]
        )

        assert result.exit_code == 0, result.stdout + (result.stderr or "")
        # One rsync for the shared mount, regardless of cell count.
        assert len(rsync_calls) == 1
        # Each cell runs once; both saw allow_in_place=True with the
        # locked mount-set populated.
        assert executor.run.call_count == 2
        for call in executor.run.call_args_list:
            ctx = call.kwargs.get("submission_context")
            assert ctx is not None
            assert ctx.allow_in_place is True
            assert "ml" in ctx.locked_mount_names

    def test_axis_driven_script_path_unions_mounts_and_syncs_each_once(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Sweep across two mounts → one rsync per mount, IN_PLACE everywhere.

        Canonical #143 win: without per-cell aggregation we'd only
        lock the base-render mount; here both mounts get locked and
        the executor sees a context that includes both names.
        """
        mount_a = tmp_path / "data_a"
        mount_a.mkdir()
        (mount_a / "run.sh").write_text("#!/bin/bash\n")
        mount_b = tmp_path / "data_b"
        mount_b.mkdir()
        (mount_b / "run.sh").write_text("#!/bin/bash\n")

        yaml_path = tmp_path / "wf.yaml"
        yaml_path.write_text(
            "name: t\n"
            "args:\n  dataset: a\n"
            "sweep:\n"
            "  matrix:\n    dataset: [a, b]\n"
            "  max_parallel: 2\n"
            "jobs:\n  - name: j\n"
            f'    path: "{tmp_path}/data_{{{{ dataset }}}}/run.sh"\n'
        )

        # Profile carries BOTH mounts so they can be aggregated.
        key = tmp_path / "id_rsa"
        key.write_text("dummy")
        profile = ServerProfile(
            hostname="h",
            username="u",
            key_filename=str(key),
            mounts=[
                MountConfig(name="data_a", local=str(mount_a), remote="/r/a"),
                MountConfig(name="data_b", local=str(mount_b), remote="/r/b"),
            ],
        )
        executor = _patch_sweep_transport(monkeypatch, profile)

        rsync_calls: list[str] = []
        monkeypatch.setattr(
            "srunx.sync.service.sync_mount_by_name",
            lambda prof, name, **kw: rsync_calls.append(name),
        )

        runner = CliRunner()
        result = runner.invoke(
            app, ["flow", "run", str(yaml_path), "--profile", "ml-cluster"]
        )

        assert result.exit_code == 0, result.stdout + (result.stderr or "")
        # Both mounts locked + rsynced exactly once each.
        assert sorted(rsync_calls) == ["data_a", "data_b"]
        # Both cells executed; both contexts list both mounts.
        assert executor.run.call_count == 2
        for call in executor.run.call_args_list:
            ctx = call.kwargs.get("submission_context")
            assert ctx is not None
            assert ctx.allow_in_place is True
            assert set(ctx.locked_mount_names) == {"data_a", "data_b"}


# ─────────────────────────────────────────────────────────────────────────
# Defence-in-depth: SSH adapter rejects IN_PLACE for unlocked mount
# ─────────────────────────────────────────────────────────────────────────


class TestLockedMountNamesSafetyNet:
    """The SSH adapter must refuse IN_PLACE for mounts outside the locked set.

    Tests the ``locked_mount_names`` field on
    :class:`SubmissionRenderContext` — defence-in-depth for the
    sweep aggregation. A buggy / racy cell that somehow targets a
    mount we never locked must fail loudly rather than silently
    race rsync.
    """

    def test_unlocked_mount_raises_clear_error(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Adapter rejects IN_PLACE when ``mount.name`` not in locked set."""
        from srunx.models import JobStatus, ShellJob
        from srunx.rendering import SubmissionRenderContext
        from srunx.slurm.ssh import SlurmSSHAdapter

        # Build a real ShellJob that lives under the adapter's mount.
        mount_local = tmp_path / "ml"
        mount_local.mkdir()
        s = mount_local / "run.sh"
        s.write_text("#!/bin/bash\necho hi\n")

        adapter = SlurmSSHAdapter(
            hostname="h",
            username="u",
            key_filename="/tmp/none",
            mounts=[MountConfig(name="ml", local=str(mount_local), remote="/r/ml")],
        )

        # Stop ``_ensure_connected`` from reaching the wire. Use
        # monkeypatch so the adapter's bound-method type contract is
        # respected (no type-ignore band-aids on direct attr writes).
        monkeypatch.setattr(adapter, "_ensure_connected", lambda: None)

        # The locked-mount guard fires before sbatch — if it didn't,
        # this would assert.
        def _fake_remote_submit(*a, **kw):
            raise AssertionError(
                "submit_remote_sbatch_file must not be called when "
                "the locked-mount guard rejects the IN_PLACE path"
            )

        monkeypatch.setattr(
            adapter._client, "submit_remote_sbatch_file", _fake_remote_submit
        )

        ctx = SubmissionRenderContext(
            mounts=(MountConfig(name="ml", local=str(mount_local), remote="/r/ml"),),
            allow_in_place=True,
            # Locked set deliberately does NOT include 'ml'.
            locked_mount_names=("data_a",),
        )

        job = ShellJob(name="r", script_path=str(s))
        job.status = JobStatus.PENDING

        with pytest.raises(RuntimeError) as exc_info:
            adapter.run(job, submission_context=ctx)

        msg = str(exc_info.value)
        assert "IN_PLACE rejected" in msg
        assert "ml" in msg  # the offending mount name
        assert "not in the locked mount set" in msg

    def test_empty_locked_mount_names_disables_enforcement(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Empty locked-mount tuple = no enforcement (preserves pre-#143).

        Non-sweep workflows compute their lock-set off the base render
        and don't need the safety net (their lock-set is the only
        possible lock-set). Empty ``locked_mount_names`` must therefore
        leave the IN_PLACE path open for them.
        """
        from srunx.models import JobStatus, ShellJob
        from srunx.rendering import SubmissionRenderContext
        from srunx.slurm.ssh import SlurmSSHAdapter

        mount_local = tmp_path / "ml"
        mount_local.mkdir()
        s = mount_local / "run.sh"
        s.write_text("#!/bin/bash\necho hi\n")

        adapter = SlurmSSHAdapter(
            hostname="h",
            username="u",
            key_filename="/tmp/none",
            mounts=[MountConfig(name="ml", local=str(mount_local), remote="/r/ml")],
        )
        monkeypatch.setattr(adapter, "_ensure_connected", lambda: None)

        called: list[tuple] = []

        def _fake_remote_submit(remote_path, **kw):
            called.append((remote_path, kw))
            return SlurmJob(job_id="42", name="r")

        monkeypatch.setattr(
            adapter._client, "submit_remote_sbatch_file", _fake_remote_submit
        )
        monkeypatch.setattr(
            adapter, "_monitor_until_terminal", lambda *a, **kw: "COMPLETED"
        )
        monkeypatch.setattr(
            SlurmSSHAdapter,
            "_record_job_submission",
            staticmethod(lambda *a, **kw: None),
        )
        monkeypatch.setattr(adapter, "_record_completion_safe", lambda *a, **kw: None)

        ctx = SubmissionRenderContext(
            mounts=(MountConfig(name="ml", local=str(mount_local), remote="/r/ml"),),
            allow_in_place=True,
            locked_mount_names=(),  # empty → no enforcement
        )

        job = ShellJob(name="r", script_path=str(s))
        job.status = JobStatus.PENDING
        adapter.run(job, submission_context=ctx)

        # IN_PLACE took effect (remote_submit called with the mount-translated path).
        assert len(called) == 1
        assert called[0][0] == "/r/ml/run.sh"
