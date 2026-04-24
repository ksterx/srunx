"""Tests for the canonical render helper (:mod:`srunx.runtime.rendering`).

These tests exercise the new :func:`render_workflow_for_submission` that
unifies the Web non-sweep, Web sweep, and MCP render paths. Phase 2
Batch 1b scope — the helper is standalone; its callers still live in
``web/routers/workflows.py``, ``web/ssh_adapter.py``, and ``mcp/server.py``
(Batch 2 migration).

Mount registry entries are constructed via a tiny :class:`_FakeMount`
duck-typed on ``.name / .local / .remote`` so tests stay independent of
``srunx.ssh.core.config.MountConfig`` (which runs ``Path.resolve()`` on
``local`` at validation time and would otherwise force real filesystem
paths for every fixture).
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pytest
import yaml

from srunx.domain import Job
from srunx.runtime.rendering import (
    RenderedJob,
    RenderedWorkflow,
    SubmissionRenderContext,
    render_job_script,
    render_workflow_for_submission,
)
from srunx.runtime.templates import get_template_path

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class _FakeMount:
    """Lightweight stand-in for ``MountConfig`` (no path resolution)."""

    name: str
    local: str
    remote: str


def _write_wf(path: Path, doc: dict) -> Path:
    path.write_text(yaml.dump(doc, default_flow_style=False))
    return path


def _find_rendered(rw: RenderedWorkflow, name: str) -> RenderedJob:
    for rj in rw.jobs:
        if rj.job.name == name:
            return rj
    raise AssertionError(
        f"No rendered job named {name!r} in {[rj.job.name for rj in rw.jobs]}"
    )


# ---------------------------------------------------------------------------
# 1. Context-less (local CLI semantics) — no mount translation
# ---------------------------------------------------------------------------


def test_context_none_skips_all_mount_translation(tmp_path: Path) -> None:
    """With ``context=None`` the helper must not touch work_dir / log_dir.

    The rendered script text must match a direct ``render_job_script``
    call for the same Job — this is the contract that keeps existing
    CLI behaviour bit-identical.
    """
    wf_path = _write_wf(
        tmp_path / "wf.yaml",
        {
            "name": "ctxless",
            "jobs": [
                {
                    "name": "j1",
                    "command": ["echo", "hi"],
                    "work_dir": "/tmp/cli-work",
                    "log_dir": "/tmp/cli-logs",
                }
            ],
        },
    )

    rw = render_workflow_for_submission(wf_path, context=None)
    assert len(rw.jobs) == 1
    rj = rw.jobs[0]

    # work_dir + log_dir round-trip verbatim.
    assert isinstance(rj.job, Job)
    assert rj.job.work_dir == "/tmp/cli-work"
    assert rj.job.log_dir == "/tmp/cli-logs"

    # Script text should contain the chdir + log dirs from the YAML.
    assert "--chdir=/tmp/cli-work" in rj.script_text
    assert "/tmp/cli-logs/%x_%j.log" in rj.script_text

    # Parity with direct render_job_script (same Job, same template).
    reference_dir = tmp_path / "ref"
    reference_dir.mkdir()
    template_path = get_template_path("base")
    reference_script = Path(
        render_job_script(template_path, rj.job, output_dir=reference_dir)
    ).read_text(encoding="utf-8")
    assert rj.script_text == reference_script


def test_context_none_with_default_workdir_injection_disabled(tmp_path: Path) -> None:
    """When ``context`` is ``None`` we must NOT fill in a default work_dir.

    Even if the job's ``work_dir`` is empty, the helper should leave it
    alone in the ``context=None`` case (CLI local semantics).
    """
    # Directly exercise the mount-normalization helper with a Job whose
    # work_dir is empty. Going through YAML would be filtered by
    # ``WorkflowRunner.parse_job`` (which swallows falsy ``work_dir`` so
    # the Pydantic default fires), so we construct the Job in-memory.
    from srunx.runtime.rendering import _normalize_paths_for_mount

    job = Job(name="j1", command=["echo", "hi"], work_dir="", log_dir="")
    # Sanity: the field accepts empty strings.
    assert job.work_dir == ""

    # context=None path is guarded at the top of render_workflow_for_submission,
    # so here we assert the invariant on the helper that IS called when
    # context is set: with default_work_dir=None the empty work_dir stays.
    ctx = SubmissionRenderContext()  # no default_work_dir, no mount
    resolved = _normalize_paths_for_mount(job, ctx)
    assert isinstance(resolved, Job)
    # No default injected, no translation — same object, untouched.
    assert resolved.work_dir == ""


# ---------------------------------------------------------------------------
# 2. default_work_dir injection when work_dir is empty
# ---------------------------------------------------------------------------


def test_default_work_dir_injected_when_job_has_empty_work_dir() -> None:
    """Empty ``work_dir`` + ``context.default_work_dir`` → chdir to default.

    Constructs the Job directly (not via YAML) so we can exercise the
    empty-work_dir → default-injection path without fighting
    ``WorkflowRunner.parse_job``'s falsy-value filter (which swallows
    ``work_dir: ""`` and falls back to the Pydantic default factory,
    currently ``os.getcwd()``; Batch 1a switches the field to
    ``str | None = None`` — the helper already treats both falsy cases
    identically via ``if not current``).
    """
    from srunx.runtime.rendering import _normalize_paths_for_mount

    job = Job(name="j1", command=["echo", "ok"], work_dir="", log_dir="")
    ctx = SubmissionRenderContext(default_work_dir="/mnt/remote")
    resolved = _normalize_paths_for_mount(job, ctx)
    assert isinstance(resolved, Job)
    assert resolved.work_dir == "/mnt/remote"

    # End-to-end render surfaces the chdir in the rendered script.
    import tempfile as _tempfile

    with _tempfile.TemporaryDirectory() as tmpdir:
        script_path = render_job_script(
            get_template_path("base"), resolved, output_dir=tmpdir
        )
        content = Path(script_path).read_text(encoding="utf-8")
    assert "#SBATCH --chdir=/mnt/remote" in content


def test_default_work_dir_not_applied_when_job_declares_its_own(tmp_path: Path) -> None:
    """When the job has an explicit ``work_dir`` the default is ignored."""
    wf_path = _write_wf(
        tmp_path / "wf.yaml",
        {
            "name": "has_wd",
            "jobs": [
                {
                    "name": "j1",
                    "command": ["echo", "ok"],
                    "work_dir": "/explicit/work",
                }
            ],
        },
    )

    ctx = SubmissionRenderContext(default_work_dir="/mnt/remote")
    rw = render_workflow_for_submission(wf_path, context=ctx)
    rj = rw.jobs[0]
    assert isinstance(rj.job, Job)
    assert rj.job.work_dir == "/explicit/work"
    assert "#SBATCH --chdir=/explicit/work" in rj.script_text
    assert "/mnt/remote" not in rj.script_text


# ---------------------------------------------------------------------------
# 3. Absolute local path → remote mount translation
# ---------------------------------------------------------------------------


def test_absolute_local_workdir_translated_to_remote(tmp_path: Path) -> None:
    """Absolute ``work_dir`` under ``mount.local`` is rewritten to ``mount.remote``.

    We materialize a real directory for ``mount.local`` so that
    ``Path.resolve()`` inside ``_translate_abs_path`` produces a stable
    comparison (otherwise on macOS ``/tmp`` vs ``/private/tmp`` would
    mismatch).
    """
    local_root = tmp_path / "projects" / "ml"
    local_root.mkdir(parents=True)
    mount = _FakeMount(name="ml", local=str(local_root), remote="/home/user/ml")

    wf_path = _write_wf(
        tmp_path / "wf.yaml",
        {
            "name": "trans",
            "jobs": [
                {
                    "name": "j1",
                    "command": ["echo", "ok"],
                    "work_dir": str(local_root / "subdir"),
                }
            ],
        },
    )

    ctx = SubmissionRenderContext(mount_name="ml", mounts=(mount,))
    rw = render_workflow_for_submission(wf_path, context=ctx)
    rj = rw.jobs[0]
    assert isinstance(rj.job, Job)
    assert rj.job.work_dir == "/home/user/ml/subdir"
    assert "#SBATCH --chdir=/home/user/ml/subdir" in rj.script_text


def test_absolute_local_workdir_exact_match_translates_to_remote_root(
    tmp_path: Path,
) -> None:
    """Exact ``work_dir == mount.local`` maps to bare ``mount.remote``."""
    local_root = tmp_path / "projects" / "ml"
    local_root.mkdir(parents=True)
    mount = _FakeMount(name="ml", local=str(local_root), remote="/home/user/ml")

    wf_path = _write_wf(
        tmp_path / "wf.yaml",
        {
            "name": "exact",
            "jobs": [
                {
                    "name": "j1",
                    "command": ["echo", "ok"],
                    "work_dir": str(local_root),
                }
            ],
        },
    )

    ctx = SubmissionRenderContext(mount_name="ml", mounts=(mount,))
    rw = render_workflow_for_submission(wf_path, context=ctx)
    rj = rw.jobs[0]
    assert isinstance(rj.job, Job)
    assert rj.job.work_dir == "/home/user/ml"


# ---------------------------------------------------------------------------
# 4. Remote-only path preserved (not under any mount)
# ---------------------------------------------------------------------------


def test_remote_only_absolute_path_preserved(tmp_path: Path) -> None:
    """Absolute path not under any mount stays verbatim."""
    local_root = tmp_path / "projects" / "ml"
    local_root.mkdir(parents=True)
    mount = _FakeMount(name="ml", local=str(local_root), remote="/home/user/ml")

    wf_path = _write_wf(
        tmp_path / "wf.yaml",
        {
            "name": "remote_only",
            "jobs": [
                {
                    "name": "j1",
                    "command": ["echo", "ok"],
                    "work_dir": "/opt/remote-only",
                }
            ],
        },
    )

    ctx = SubmissionRenderContext(mount_name="ml", mounts=(mount,))
    rw = render_workflow_for_submission(wf_path, context=ctx)
    rj = rw.jobs[0]
    assert isinstance(rj.job, Job)
    assert rj.job.work_dir == "/opt/remote-only"


# ---------------------------------------------------------------------------
# 5. Relative path preserved
# ---------------------------------------------------------------------------


def test_relative_workdir_preserved(tmp_path: Path) -> None:
    """Relative ``work_dir`` stays as-is (resolved at run time)."""
    local_root = tmp_path / "projects" / "ml"
    local_root.mkdir(parents=True)
    mount = _FakeMount(name="ml", local=str(local_root), remote="/home/user/ml")

    wf_path = _write_wf(
        tmp_path / "wf.yaml",
        {
            "name": "rel",
            "jobs": [
                {
                    "name": "j1",
                    "command": ["echo", "ok"],
                    "work_dir": "subdir",
                }
            ],
        },
    )

    ctx = SubmissionRenderContext(mount_name="ml", mounts=(mount,))
    rw = render_workflow_for_submission(wf_path, context=ctx)
    rj = rw.jobs[0]
    assert isinstance(rj.job, Job)
    assert rj.job.work_dir == "subdir"


# ---------------------------------------------------------------------------
# 6. log_dir normalization
# ---------------------------------------------------------------------------


def test_absolute_local_log_dir_translated(tmp_path: Path) -> None:
    """Absolute ``log_dir`` under a mount is translated to remote."""
    local_root = tmp_path / "projects" / "ml"
    local_root.mkdir(parents=True)
    mount = _FakeMount(name="ml", local=str(local_root), remote="/home/user/ml")

    wf_path = _write_wf(
        tmp_path / "wf.yaml",
        {
            "name": "logtrans",
            "jobs": [
                {
                    "name": "j1",
                    "command": ["echo", "ok"],
                    "work_dir": str(local_root),
                    "log_dir": str(local_root / "logs"),
                }
            ],
        },
    )

    ctx = SubmissionRenderContext(mount_name="ml", mounts=(mount,))
    rw = render_workflow_for_submission(wf_path, context=ctx)
    rj = rw.jobs[0]
    assert isinstance(rj.job, Job)
    assert rj.job.log_dir == "/home/user/ml/logs"
    assert "/home/user/ml/logs/%x_%j.log" in rj.script_text


def test_relative_log_dir_preserved(tmp_path: Path) -> None:
    """Relative ``log_dir`` is left alone (relative to work_dir at runtime)."""
    local_root = tmp_path / "projects" / "ml"
    local_root.mkdir(parents=True)
    mount = _FakeMount(name="ml", local=str(local_root), remote="/home/user/ml")

    wf_path = _write_wf(
        tmp_path / "wf.yaml",
        {
            "name": "rellog",
            "jobs": [
                {
                    "name": "j1",
                    "command": ["echo", "ok"],
                    "work_dir": str(local_root),
                    "log_dir": "logs",
                }
            ],
        },
    )

    ctx = SubmissionRenderContext(mount_name="ml", mounts=(mount,))
    rw = render_workflow_for_submission(wf_path, context=ctx)
    rj = rw.jobs[0]
    assert isinstance(rj.job, Job)
    assert rj.job.log_dir == "logs"


def test_empty_log_dir_stays_empty(tmp_path: Path) -> None:
    """Empty ``log_dir`` is not replaced by anything (template fallback)."""
    local_root = tmp_path / "projects" / "ml"
    local_root.mkdir(parents=True)
    mount = _FakeMount(name="ml", local=str(local_root), remote="/home/user/ml")

    wf_path = _write_wf(
        tmp_path / "wf.yaml",
        {
            "name": "nolog",
            "jobs": [
                {
                    "name": "j1",
                    "command": ["echo", "ok"],
                    "work_dir": str(local_root),
                    "log_dir": "",
                }
            ],
        },
    )

    ctx = SubmissionRenderContext(mount_name="ml", mounts=(mount,))
    rw = render_workflow_for_submission(wf_path, context=ctx)
    rj = rw.jobs[0]
    assert isinstance(rj.job, Job)
    # ``WorkflowRunner.parse_job`` treats falsy (empty-string) ``log_dir`` as
    # "not specified" and falls back to Job's default factory ("logs"). This
    # is pre-Phase-2 behaviour and out of scope for the rendering helper.
    # What we care about here: (a) the rendering helper does not inject any
    # extra default of its own, (b) the script still emits a valid
    # ``#SBATCH --output=`` line (via the ``logs/`` default prefix).
    assert rj.job.log_dir == "logs"
    assert "#SBATCH --output=logs/%x_%j.log" in rj.script_text


# ---------------------------------------------------------------------------
# 7. Shell quoting (command list) — Batch 1a's shlex.join fix surfaces here
# ---------------------------------------------------------------------------


def test_complex_command_shell_quoting(tmp_path: Path) -> None:
    """A complex command with shell metachars must end up correctly quoted.

    Before Batch 1a the renderer joined argv with a bare space, which
    broke arguments containing spaces or semicolons. Once Batch 1a
    swaps to ``shlex.join``, this test verifies the quoting is applied.

    Until Batch 1a lands, this test is marked ``xfail`` with
    ``strict=False`` so it passes when the fix arrives without blocking
    the current CI.
    """
    wf_path = _write_wf(
        tmp_path / "wf.yaml",
        {
            "name": "quote",
            "jobs": [
                {
                    "name": "j1",
                    "command": ["bash", "-c", "echo a; sleep 1"],
                    "work_dir": "/tmp",
                    "log_dir": "",
                }
            ],
        },
    )

    rw = render_workflow_for_submission(wf_path, context=None)
    rj = rw.jobs[0]

    # Post-Batch-1a: shlex.join quotes the "echo a; sleep 1" arg.
    # Pre-Batch-1a: the raw joined command is present unquoted.
    # Use xfail so the test flips green as soon as the fix arrives.
    if "'echo a; sleep 1'" in rj.script_text:
        # Post-fix path: the third argv entry is quoted atomically.
        assert "bash -c 'echo a; sleep 1'" in rj.script_text
    else:
        pytest.xfail(
            "shlex.join quoting not yet applied — Batch 1a needs to land. "
            f"Got: {rj.script_text!r}"
        )


# ---------------------------------------------------------------------------
# 8. args_override threading through Jinja
# ---------------------------------------------------------------------------


def test_args_override_flows_into_rendered_command(tmp_path: Path) -> None:
    """``args_override`` substitutes into command arguments via Jinja."""
    wf_path = _write_wf(
        tmp_path / "wf.yaml",
        {
            "name": "argstest",
            "args": {"lr": 0.001},
            "jobs": [
                {
                    "name": "train",
                    "command": ["python", "train.py", "--lr", "{{ lr }}"],
                    "work_dir": "/tmp",
                    "log_dir": "",
                }
            ],
        },
    )

    rw = render_workflow_for_submission(
        wf_path,
        args_override={"lr": 0.05},
    )
    rj = rw.jobs[0]
    assert isinstance(rj.job, Job)
    assert "0.05" in rj.script_text
    assert "0.001" not in rj.script_text


# ---------------------------------------------------------------------------
# 9. single_job filter
# ---------------------------------------------------------------------------


def test_single_job_filter(tmp_path: Path) -> None:
    """``single_job='train'`` → only ``train`` is in the rendered output."""
    wf_path = _write_wf(
        tmp_path / "wf.yaml",
        {
            "name": "multi",
            "jobs": [
                {
                    "name": "prep",
                    "command": ["python", "prep.py"],
                    "work_dir": "/tmp",
                    "log_dir": "",
                },
                {
                    "name": "train",
                    "command": ["python", "train.py"],
                    "work_dir": "/tmp",
                    "log_dir": "",
                    "depends_on": ["prep"],
                },
            ],
        },
    )

    rw = render_workflow_for_submission(wf_path, single_job="train")
    assert len(rw.jobs) == 1
    assert rw.jobs[0].job.name == "train"
    assert rw.jobs[0].script_filename == "train.slurm"


# ---------------------------------------------------------------------------
# 10. mount_name=None with mounts populated — no translation
# ---------------------------------------------------------------------------


def test_mount_name_none_with_mounts_populated_does_not_translate(
    tmp_path: Path,
) -> None:
    """Without ``mount_name`` even a matching path prefix is NOT rewritten.

    Translation requires an explicit mount selection. This keeps
    behaviour unambiguous when multiple mounts share an ancestor prefix.
    """
    local_root = tmp_path / "projects" / "ml"
    local_root.mkdir(parents=True)
    mount = _FakeMount(name="ml", local=str(local_root), remote="/home/user/ml")

    wf_path = _write_wf(
        tmp_path / "wf.yaml",
        {
            "name": "nomount",
            "jobs": [
                {
                    "name": "j1",
                    "command": ["echo", "ok"],
                    "work_dir": str(local_root / "subdir"),
                }
            ],
        },
    )

    ctx = SubmissionRenderContext(mount_name=None, mounts=(mount,))
    rw = render_workflow_for_submission(wf_path, context=ctx)
    rj = rw.jobs[0]
    assert isinstance(rj.job, Job)
    # Path stays as the local path — no remote translation.
    assert rj.job.work_dir == str(local_root / "subdir")
    assert "/home/user/ml" not in rj.script_text


def test_unknown_mount_name_does_not_translate(tmp_path: Path) -> None:
    """A ``mount_name`` that doesn't exist in the registry is a no-op."""
    local_root = tmp_path / "projects" / "ml"
    local_root.mkdir(parents=True)
    mount = _FakeMount(name="ml", local=str(local_root), remote="/home/user/ml")

    wf_path = _write_wf(
        tmp_path / "wf.yaml",
        {
            "name": "badmount",
            "jobs": [
                {
                    "name": "j1",
                    "command": ["echo", "ok"],
                    "work_dir": str(local_root / "subdir"),
                }
            ],
        },
    )

    ctx = SubmissionRenderContext(mount_name="does-not-exist", mounts=(mount,))
    rw = render_workflow_for_submission(wf_path, context=ctx)
    rj = rw.jobs[0]
    assert isinstance(rj.job, Job)
    assert rj.job.work_dir == str(local_root / "subdir")


# ---------------------------------------------------------------------------
# Immutability contract
# ---------------------------------------------------------------------------


def test_rendered_job_and_workflow_are_frozen() -> None:
    """The dataclasses are ``frozen=True`` so callers can't mutate results."""
    from srunx.domain import Workflow

    job = Job(name="j", command=["echo"], work_dir="/tmp", log_dir="")
    rj = RenderedJob(job=job, script_text="x", script_filename="j.slurm")
    with pytest.raises((AttributeError, TypeError)):
        rj.script_text = "y"  # type: ignore[misc]

    rw = RenderedWorkflow(workflow=Workflow(name="wf", jobs=[job]), jobs=(rj,))
    with pytest.raises((AttributeError, TypeError)):
        rw.jobs = ()  # type: ignore[misc]


def test_context_is_frozen() -> None:
    """``SubmissionRenderContext`` is immutable."""
    ctx = SubmissionRenderContext(mount_name="x")
    with pytest.raises((AttributeError, TypeError)):
        ctx.mount_name = "y"  # type: ignore[misc]


# ---------------------------------------------------------------------------
# Smoke: multi-job sweep-style render (parity with sweep adapter path)
# ---------------------------------------------------------------------------


def test_smoke_multi_job_sweep_style_render(tmp_path: Path) -> None:
    """End-to-end smoke: args_override + mount translation + 2 jobs.

    Mirrors the shape of a sweep cell (same workflow rendered N times
    with different ``args_override``) so the Batch 2 migration can
    verify this helper is a drop-in replacement for the sweep render
    path in ``SlurmSSHAdapter.run``.
    """
    local_root = tmp_path / "projects" / "sweep"
    local_root.mkdir(parents=True)
    mount = _FakeMount(name="sweep", local=str(local_root), remote="/home/user/sweep")

    wf_path = _write_wf(
        tmp_path / "wf.yaml",
        {
            "name": "smoke_sweep",
            "args": {"lr": 0.01, "seed": 0},
            "jobs": [
                {
                    "name": "prep",
                    "command": ["python", "prep.py", "--seed", "{{ seed }}"],
                    "work_dir": str(local_root),
                    "log_dir": str(local_root / "logs"),
                },
                {
                    "name": "train",
                    "command": ["python", "train.py", "--lr", "{{ lr }}"],
                    "work_dir": str(local_root),
                    "log_dir": str(local_root / "logs"),
                    "depends_on": ["prep"],
                },
            ],
        },
    )

    ctx = SubmissionRenderContext(mount_name="sweep", mounts=(mount,))
    # Simulate one sweep cell's args_override.
    rw = render_workflow_for_submission(
        wf_path,
        args_override={"lr": 0.5, "seed": 42},
        context=ctx,
    )

    assert [rj.job.name for rj in rw.jobs] == ["prep", "train"]

    prep = _find_rendered(rw, "prep")
    train = _find_rendered(rw, "train")

    # Path translation applied to both jobs.
    assert isinstance(prep.job, Job)
    assert isinstance(train.job, Job)
    assert prep.job.work_dir == "/home/user/sweep"
    assert prep.job.log_dir == "/home/user/sweep/logs"
    assert train.job.work_dir == "/home/user/sweep"
    assert train.job.log_dir == "/home/user/sweep/logs"

    # Override substituted into rendered commands.
    assert "42" in prep.script_text
    assert "0.5" in train.script_text
    assert "#SBATCH --chdir=/home/user/sweep" in prep.script_text
    assert "#SBATCH --chdir=/home/user/sweep" in train.script_text

    # script_filename is derived from job name.
    assert prep.script_filename == "prep.slurm"
    assert train.script_filename == "train.slurm"

    # The rebuilt Workflow exposes the same mount-resolved Jobs.
    assert len(rw.workflow.jobs) == 2
    assert rw.workflow.jobs[0] is prep.job
    assert rw.workflow.jobs[1] is train.job
