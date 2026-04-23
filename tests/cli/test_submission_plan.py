"""Unit tests for the submission planner.

The planner is intentionally pure (no IO), so these tests just feed it
synthetic ``ServerProfile`` / ``MountConfig`` objects and assert on the
returned :class:`SubmissionPlan`. Real-world sync + sbatch behaviour
is covered by the integration tests under ``tests/cli/test_sbatch_in_place.py``.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from srunx.runtime.submission_plan import (
    SubmissionMode,
    plan_sbatch_submission,
    resolve_mount_for_path,
    translate_local_to_remote,
)
from srunx.ssh.core.config import MountConfig, ServerProfile


def _make_profile(tmp_path: Path, mounts: list[MountConfig]) -> ServerProfile:
    """Build a minimal ServerProfile with no real SSH credentials.

    Only the ``mounts`` field matters for planner tests, but
    ``ServerProfile`` requires hostname/username/key_filename. We
    point key_filename at a writable tmp location so Pydantic's
    validators (which expand ``~``) don't reject it.
    """
    key_path = tmp_path / "id_rsa"
    key_path.write_text("dummy")
    return ServerProfile(
        hostname="test.example.com",
        username="alice",
        key_filename=str(key_path),
        mounts=tuple(mounts),
    )


def _make_mount(local: Path, remote: str = "/cluster/share/ml") -> MountConfig:
    return MountConfig(name="ml", local=str(local), remote=remote)


class TestResolveMountForPath:
    def test_returns_none_when_no_mounts(self, tmp_path: Path) -> None:
        profile = _make_profile(tmp_path, mounts=[])
        assert resolve_mount_for_path(tmp_path / "x.sh", profile) is None

    def test_returns_mount_when_path_under_root(self, tmp_path: Path) -> None:
        local = tmp_path / "ml-project"
        local.mkdir()
        mount = _make_mount(local)
        profile = _make_profile(tmp_path, mounts=[mount])

        result = resolve_mount_for_path(local / "train.sh", profile)
        assert result == mount

    def test_returns_none_when_outside(self, tmp_path: Path) -> None:
        local = tmp_path / "ml-project"
        local.mkdir()
        outside = tmp_path / "other"
        outside.mkdir()
        profile = _make_profile(tmp_path, mounts=[_make_mount(local)])

        assert resolve_mount_for_path(outside / "x.sh", profile) is None

    def test_longest_prefix_wins_for_nested_mounts(self, tmp_path: Path) -> None:
        outer = tmp_path / "outer"
        inner = outer / "inner"
        inner.mkdir(parents=True)
        outer_mount = MountConfig(name="outer", local=str(outer), remote="/r/outer")
        inner_mount = MountConfig(name="inner", local=str(inner), remote="/r/inner")
        profile = _make_profile(tmp_path, mounts=[outer_mount, inner_mount])

        # Nested file should bind to the deeper mount.
        result = resolve_mount_for_path(inner / "x.sh", profile)
        assert result == inner_mount

    def test_resolves_through_symlinks(self, tmp_path: Path) -> None:
        local = tmp_path / "ml-project"
        local.mkdir()
        target = local / "real.sh"
        target.write_text("#!/bin/bash\n")
        link = tmp_path / "link.sh"
        link.symlink_to(target)
        profile = _make_profile(tmp_path, mounts=[_make_mount(local)])

        # Symlink leaving / entering the mount should still bind to it.
        result = resolve_mount_for_path(link, profile)
        assert result is not None
        assert result.name == "ml"


class TestTranslateLocalToRemote:
    def test_basic_translation(self, tmp_path: Path) -> None:
        local = tmp_path / "ml-project"
        local.mkdir()
        mount = _make_mount(local, remote="/cluster/share/ml")
        out = translate_local_to_remote(local / "runs/exp1/train.sh", mount)
        assert out == "/cluster/share/ml/runs/exp1/train.sh"

    def test_root_returns_remote_root(self, tmp_path: Path) -> None:
        local = tmp_path / "ml-project"
        local.mkdir()
        mount = _make_mount(local, remote="/cluster/share/ml")
        assert translate_local_to_remote(local, mount) == "/cluster/share/ml"

    def test_strips_trailing_slash(self, tmp_path: Path) -> None:
        local = tmp_path / "ml"
        local.mkdir()
        mount = MountConfig(name="ml", local=str(local), remote="/r/ml/")
        assert translate_local_to_remote(local / "x.sh", mount) == "/r/ml/x.sh"


class TestPlanSbatchSubmission:
    def test_no_script_means_temp_upload(self, tmp_path: Path) -> None:
        # ``--wrap`` case: script_path is None.
        profile = _make_profile(tmp_path, mounts=[])
        plan = plan_sbatch_submission(
            script_path=None,
            profile=profile,
            cwd=None,
            sync_enabled=True,
            is_rendered_artifact=False,
        )
        assert plan.mode == SubmissionMode.TEMP_UPLOAD
        assert plan.mount is None
        assert plan.sync_required is False

    def test_no_profile_means_temp_upload(self, tmp_path: Path) -> None:
        # Local SLURM has no SSH profile / mount concept.
        plan = plan_sbatch_submission(
            script_path=tmp_path / "x.sh",
            profile=None,
            cwd=None,
            sync_enabled=True,
            is_rendered_artifact=False,
        )
        assert plan.mode == SubmissionMode.TEMP_UPLOAD

    def test_template_artifact_means_temp_upload(self, tmp_path: Path) -> None:
        # Even when the script path lives under a mount, a freshly
        # rendered template is a generated artifact and must not run
        # in place — running the on-disk source would discard the
        # template's substitutions.
        local = tmp_path / "ml"
        local.mkdir()
        script = local / "train.sh"
        script.write_text("#!/bin/bash\n")
        profile = _make_profile(tmp_path, mounts=[_make_mount(local)])

        plan = plan_sbatch_submission(
            script_path=script,
            profile=profile,
            cwd=None,
            sync_enabled=True,
            is_rendered_artifact=True,
        )
        assert plan.mode == SubmissionMode.TEMP_UPLOAD

    def test_outside_mount_is_temp_upload(self, tmp_path: Path) -> None:
        local = tmp_path / "ml"
        local.mkdir()
        outside = tmp_path / "scratch"
        outside.mkdir()
        script = outside / "x.sh"
        script.write_text("#!/bin/bash\n")
        profile = _make_profile(tmp_path, mounts=[_make_mount(local)])

        plan = plan_sbatch_submission(
            script_path=script,
            profile=profile,
            cwd=None,
            sync_enabled=True,
            is_rendered_artifact=False,
        )
        assert plan.mode == SubmissionMode.TEMP_UPLOAD

    def test_in_place_with_sync_required(self, tmp_path: Path) -> None:
        local = tmp_path / "ml"
        local.mkdir()
        script = local / "train.sh"
        script.write_text("#!/bin/bash\n")
        mount = _make_mount(local, remote="/cluster/share/ml")
        profile = _make_profile(tmp_path, mounts=[mount])

        plan = plan_sbatch_submission(
            script_path=script,
            profile=profile,
            cwd=None,
            sync_enabled=True,
            is_rendered_artifact=False,
        )
        assert plan.mode == SubmissionMode.IN_PLACE
        assert plan.mount == mount
        assert plan.remote_script_path == "/cluster/share/ml/train.sh"
        assert plan.sync_required is True
        assert plan.warnings == ()

    def test_in_place_with_sync_disabled_warns(self, tmp_path: Path) -> None:
        local = tmp_path / "ml"
        local.mkdir()
        script = local / "train.sh"
        script.write_text("#!/bin/bash\n")
        profile = _make_profile(tmp_path, mounts=[_make_mount(local)])

        plan = plan_sbatch_submission(
            script_path=script,
            profile=profile,
            cwd=None,
            sync_enabled=False,
            is_rendered_artifact=False,
        )
        assert plan.mode == SubmissionMode.IN_PLACE
        assert plan.sync_required is False
        # User asked for --no-sync but is invoking against a mount;
        # plan must surface the "remote may be stale" caveat.
        assert any("without syncing" in w for w in plan.warnings)

    def test_submit_cwd_uses_translated_cwd_when_under_mount(
        self, tmp_path: Path
    ) -> None:
        local = tmp_path / "ml"
        runs = local / "runs/exp1"
        runs.mkdir(parents=True)
        script = runs / "train.sh"
        script.write_text("#!/bin/bash\n")
        mount = _make_mount(local, remote="/cluster/share/ml")
        profile = _make_profile(tmp_path, mounts=[mount])

        plan = plan_sbatch_submission(
            script_path=script,
            profile=profile,
            cwd=runs,
            sync_enabled=True,
            is_rendered_artifact=False,
        )
        # cwd was the same dir as the script — translated cwd should
        # match script's parent on the remote.
        assert plan.submit_cwd == "/cluster/share/ml/runs/exp1"

    def test_submit_cwd_falls_back_to_script_parent(self, tmp_path: Path) -> None:
        local = tmp_path / "ml"
        local.mkdir()
        script = local / "runs" / "train.sh"
        script.parent.mkdir()
        script.write_text("#!/bin/bash\n")
        outside_cwd = tmp_path / "outside"
        outside_cwd.mkdir()
        mount = _make_mount(local, remote="/cluster/share/ml")
        profile = _make_profile(tmp_path, mounts=[mount])

        plan = plan_sbatch_submission(
            script_path=script,
            profile=profile,
            cwd=outside_cwd,
            sync_enabled=True,
            is_rendered_artifact=False,
        )
        # cwd is outside any mount, so submit_cwd should fall back to
        # the script's enclosing directory on the remote.
        assert plan.submit_cwd == "/cluster/share/ml/runs"


class TestCollectTouchedMounts:
    """Workflow Phase 2 (#135): touched-mount aggregation across ShellJobs."""

    def test_empty_workflow_returns_empty_list(self, tmp_path: Path) -> None:
        """A workflow with no jobs touches no mounts."""
        from srunx.runtime.submission_plan import collect_touched_mounts

        local = tmp_path / "ml"
        local.mkdir()
        profile = _make_profile(tmp_path, mounts=[_make_mount(local)])

        class _StubWorkflow:
            jobs = ()

        assert collect_touched_mounts(_StubWorkflow(), profile) == []

    def test_dedup_across_multiple_shelljobs_same_mount(self, tmp_path: Path) -> None:
        """Two ShellJobs under the same mount yield one mount, not two."""
        from srunx.domain import ShellJob
        from srunx.runtime.submission_plan import collect_touched_mounts

        local = tmp_path / "ml"
        local.mkdir()
        for name in ("a.sh", "b.sh"):
            (local / name).write_text("#!/bin/bash\n")
        mount = _make_mount(local)
        profile = _make_profile(tmp_path, mounts=[mount])

        class _Wf:
            jobs = (
                ShellJob(name="a", script_path=str(local / "a.sh")),
                ShellJob(name="b", script_path=str(local / "b.sh")),
            )

        result = collect_touched_mounts(_Wf(), profile)
        assert result == [mount]

    def test_skips_jobs_outside_any_mount(self, tmp_path: Path) -> None:
        """ShellJobs outside the mount roots are dropped (will go via tmp)."""
        from srunx.domain import ShellJob
        from srunx.runtime.submission_plan import collect_touched_mounts

        local = tmp_path / "ml"
        local.mkdir()
        (local / "in.sh").write_text("#!/bin/bash\n")
        outside = tmp_path / "scratch"
        outside.mkdir()
        (outside / "out.sh").write_text("#!/bin/bash\n")
        mount = _make_mount(local)
        profile = _make_profile(tmp_path, mounts=[mount])

        class _Wf:
            jobs = (
                ShellJob(name="i", script_path=str(local / "in.sh")),
                ShellJob(name="o", script_path=str(outside / "out.sh")),
            )

        assert collect_touched_mounts(_Wf(), profile) == [mount]

    def test_skips_command_jobs(self, tmp_path: Path) -> None:
        """``Job`` (command-style, no script_path) contributes nothing."""
        from srunx.domain import Job
        from srunx.runtime.submission_plan import collect_touched_mounts

        local = tmp_path / "ml"
        local.mkdir()
        profile = _make_profile(tmp_path, mounts=[_make_mount(local)])

        class _Wf:
            jobs = (Job(name="cmd", command=["echo", "hi"]),)

        assert collect_touched_mounts(_Wf(), profile) == []


class TestRenderMatchesSource:
    def test_identical_bytes_match(self, tmp_path: Path) -> None:
        from srunx.runtime.submission_plan import render_matches_source

        a = tmp_path / "a"
        a.write_bytes(b"#!/bin/bash\necho hi\n")
        b = tmp_path / "b"
        b.write_bytes(b"#!/bin/bash\necho hi\n")
        assert render_matches_source(a, b) is True

    def test_different_bytes_do_not_match(self, tmp_path: Path) -> None:
        from srunx.runtime.submission_plan import render_matches_source

        a = tmp_path / "a"
        a.write_bytes(b"#!/bin/bash\necho hi\n")
        b = tmp_path / "b"
        b.write_bytes(b"#!/bin/bash\necho HELLO\n")
        assert render_matches_source(a, b) is False

    def test_missing_file_returns_false(self, tmp_path: Path) -> None:
        """Missing file → False (caller must fall back to tmp upload)."""
        from srunx.runtime.submission_plan import render_matches_source

        a = tmp_path / "a"
        a.write_bytes(b"x")
        b = tmp_path / "missing"
        assert render_matches_source(a, b) is False


@pytest.mark.parametrize("sync_enabled", [True, False])
def test_in_place_preserves_remote_path_regardless_of_sync(
    tmp_path: Path, sync_enabled: bool
) -> None:
    """The remote path translation is independent of sync_enabled.

    Disabling sync only changes whether rsync runs and whether we add
    a "running stale" warning — it must not change the remote path
    we end up sbatching against.
    """
    local = tmp_path / "ml"
    local.mkdir()
    script = local / "train.sh"
    script.write_text("#!/bin/bash\n")
    profile = _make_profile(tmp_path, mounts=[_make_mount(local)])

    plan = plan_sbatch_submission(
        script_path=script,
        profile=profile,
        cwd=None,
        sync_enabled=sync_enabled,
        is_rendered_artifact=False,
    )
    assert plan.remote_script_path == "/cluster/share/ml/train.sh"
