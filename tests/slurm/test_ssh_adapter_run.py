"""Tests for :meth:`SlurmSSHAdapter.run` — Step 4a of the SSH sweep wiring.

Verifies the wrapper composes render → submit → monitor correctly, fires
callbacks on terminal transitions, and propagates ``workflow_run_id`` into
the state DB helper.
"""

from __future__ import annotations

import threading
from unittest.mock import MagicMock

import pytest

from srunx.callbacks import Callback
from srunx.domain import Job, JobStatus
from srunx.slurm.ssh import (
    SlurmSSHAdapter,
    SlurmSSHAdapterSpec,
    SSHMonitorTimeoutError,
)

# --- Helpers ---------------------------------------------------------


def _bare_adapter(callbacks: list[Callback] | None = None) -> SlurmSSHAdapter:
    """Build a fully-populated adapter that bypasses real SSH I/O."""
    adapter = object.__new__(SlurmSSHAdapter)
    adapter._io_lock = threading.RLock()
    adapter._client = MagicMock()
    adapter.callbacks = list(callbacks) if callbacks else []
    adapter._profile_name = None
    adapter._hostname = "testhost"
    adapter._username = "tester"
    adapter._key_filename = None
    adapter._port = 22
    adapter._proxy_jump = None
    adapter._env_vars = {}
    adapter._mounts = ()
    # Review fix #7: submission_source is mutable per-handle state the
    # transport registry sets; tests that bypass __init__ must provide
    # a default so submit()/run() branches that rely on it don't AttributeError.
    adapter.submission_source = "web"

    transport = MagicMock()
    transport.is_active.return_value = True
    ssh = MagicMock()
    ssh.get_transport.return_value = transport
    adapter._client.connection.ssh_client = ssh

    return adapter


class _RecordingCallback(Callback):
    """In-memory recorder used by the callback-firing tests."""

    def __init__(self) -> None:
        self.submitted: list[str] = []
        self.completed: list[str] = []
        self.failed: list[str] = []
        self.cancelled: list[str] = []

    def on_job_submitted(self, job) -> None:  # type: ignore[override]
        self.submitted.append(job.name)

    def on_job_completed(self, job) -> None:  # type: ignore[override]
        self.completed.append(job.name)

    def on_job_failed(self, job) -> None:  # type: ignore[override]
        self.failed.append(job.name)

    def on_job_cancelled(self, job) -> None:  # type: ignore[override]
        self.cancelled.append(job.name)


# --- Tests -----------------------------------------------------------


class TestSSHAdapterRun:
    def test_run_happy_path_invokes_render_submit_monitor(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """run() renders → submits → monitors → returns COMPLETED job."""
        adapter = _bare_adapter()

        job = Job(
            name="happy",
            command=["echo", "ok"],
            log_dir="",
            work_dir="",
        )

        submit_calls: list[dict[str, object]] = []

        def fake_submit(script_content: str, *, job_name=None, dependency=None):
            submit_calls.append({"content": script_content, "name": job_name})
            sj = MagicMock()
            sj.job_id = "42"
            sj.name = job_name
            return sj

        adapter._client.slurm.submit_sbatch_job = fake_submit  # type: ignore[method-assign,assignment]

        monkeypatch.setattr(
            adapter, "_monitor_until_terminal", lambda _jid: "COMPLETED"
        )
        monkeypatch.setattr(
            SlurmSSHAdapter,
            "_record_job_submission",
            staticmethod(lambda *a, **k: None),
        )
        monkeypatch.setattr(
            SlurmSSHAdapter,
            "_record_completion_safe",
            staticmethod(lambda *a, **k: None),
        )

        result = adapter.run(job, workflow_name="wf", workflow_run_id=7)

        assert result is job
        assert result.job_id == 42
        assert result.status == JobStatus.COMPLETED
        assert len(submit_calls) == 1
        assert submit_calls[0]["name"] == "happy"
        assert "SBATCH --job-name=happy" in submit_calls[0]["content"]  # type: ignore[operator]

    def test_callbacks_fire_on_submit_and_completion(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        cb = _RecordingCallback()
        adapter = _bare_adapter(callbacks=[cb])

        job = Job(name="cbok", command=["true"], log_dir="", work_dir="")

        sj = MagicMock()
        sj.job_id = "101"
        sj.name = "cbok"
        adapter._client.slurm.submit_sbatch_job = MagicMock(return_value=sj)  # type: ignore[method-assign]

        monkeypatch.setattr(
            adapter, "_monitor_until_terminal", lambda _jid: "COMPLETED"
        )
        monkeypatch.setattr(
            SlurmSSHAdapter,
            "_record_job_submission",
            staticmethod(lambda *a, **k: None),
        )
        monkeypatch.setattr(
            SlurmSSHAdapter,
            "_record_completion_safe",
            staticmethod(lambda *a, **k: None),
        )

        adapter.run(job)

        assert cb.submitted == ["cbok"]
        assert cb.completed == ["cbok"]
        assert cb.failed == []
        assert cb.cancelled == []

    def test_callbacks_fire_on_failed_and_run_raises(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        cb = _RecordingCallback()
        adapter = _bare_adapter(callbacks=[cb])

        job = Job(name="bad", command=["false"], log_dir="", work_dir="")

        sj = MagicMock()
        sj.job_id = "202"
        sj.name = "bad"
        adapter._client.slurm.submit_sbatch_job = MagicMock(return_value=sj)  # type: ignore[method-assign]

        monkeypatch.setattr(adapter, "_monitor_until_terminal", lambda _jid: "FAILED")
        monkeypatch.setattr(
            SlurmSSHAdapter,
            "_record_job_submission",
            staticmethod(lambda *a, **k: None),
        )
        monkeypatch.setattr(
            SlurmSSHAdapter,
            "_record_completion_safe",
            staticmethod(lambda *a, **k: None),
        )

        with pytest.raises(RuntimeError, match="FAILED"):
            adapter.run(job)

        assert cb.submitted == ["bad"]
        assert cb.failed == ["bad"]
        assert cb.completed == []
        assert job.status == JobStatus.FAILED

    def test_cancelled_callback_and_raise(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        cb = _RecordingCallback()
        adapter = _bare_adapter(callbacks=[cb])

        job = Job(name="cancelme", command=["true"], log_dir="", work_dir="")

        sj = MagicMock()
        sj.job_id = "303"
        sj.name = "cancelme"
        adapter._client.slurm.submit_sbatch_job = MagicMock(return_value=sj)  # type: ignore[method-assign]

        monkeypatch.setattr(
            adapter, "_monitor_until_terminal", lambda _jid: "CANCELLED"
        )
        monkeypatch.setattr(
            SlurmSSHAdapter,
            "_record_job_submission",
            staticmethod(lambda *a, **k: None),
        )
        monkeypatch.setattr(
            SlurmSSHAdapter,
            "_record_completion_safe",
            staticmethod(lambda *a, **k: None),
        )

        with pytest.raises(RuntimeError, match="CANCELLED"):
            adapter.run(job)

        assert cb.cancelled == ["cancelme"]
        assert job.status == JobStatus.CANCELLED

    def test_workflow_run_id_propagates_to_db_helper(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """run() forwards workflow_run_id to the best-effort DB recorder."""
        adapter = _bare_adapter()
        job = Job(name="wfjob", command=["echo"], log_dir="", work_dir="")

        sj = MagicMock()
        sj.job_id = "404"
        sj.name = "wfjob"
        adapter._client.slurm.submit_sbatch_job = MagicMock(return_value=sj)  # type: ignore[method-assign]

        monkeypatch.setattr(
            adapter, "_monitor_until_terminal", lambda _jid: "COMPLETED"
        )

        recorded: dict[str, object] = {}

        def fake_record(job, *, workflow_name=None, workflow_run_id=None):
            recorded["name"] = job.name
            recorded["workflow_name"] = workflow_name
            recorded["workflow_run_id"] = workflow_run_id

        # Patch the best-effort DB import site (inlined inside
        # SlurmSSHAdapter._record_job_submission).
        monkeypatch.setattr(
            "srunx.observability.storage.cli_helpers.record_submission_from_job",
            fake_record,
        )
        monkeypatch.setattr(
            "srunx.observability.storage.cli_helpers.record_completion",
            lambda *a, **k: None,
        )

        adapter.run(job, workflow_name="mywf", workflow_run_id=123)

        assert recorded == {
            "name": "wfjob",
            "workflow_name": "mywf",
            "workflow_run_id": 123,
        }

    def test_submit_failure_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        adapter = _bare_adapter()
        adapter._client.slurm.submit_sbatch_job = MagicMock(return_value=None)  # type: ignore[method-assign]

        job = Job(name="nope", command=["echo"], log_dir="", work_dir="")
        with pytest.raises(RuntimeError, match="Failed to submit"):
            adapter.run(job)


class TestSSHAdapterRunUnknownStatus:
    """Phase 3 A-1: NOT_FOUND / unrecognised states must not silently FAIL.

    Pre-fix behaviour: any non-COMPLETED/FAILED/CANCELLED/TIMEOUT status
    hit ``JobStatus(terminal_status)`` → ``ValueError`` → silent FAILED,
    turning successful-but-dropped-from-sacct jobs into false failures
    on pyxis clusters. The fix maps these to :attr:`JobStatus.UNKNOWN`
    with a warning log and skips the failure-raise path.
    """

    def test_not_found_becomes_unknown_not_silent_failed(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        cb = _RecordingCallback()
        adapter = _bare_adapter(callbacks=[cb])

        job = Job(name="disappeared", command=["true"], log_dir="", work_dir="")

        sj = MagicMock()
        sj.job_id = "501"
        sj.name = "disappeared"
        adapter._client.slurm.submit_sbatch_job = MagicMock(return_value=sj)  # type: ignore[method-assign]

        monkeypatch.setattr(
            adapter, "_monitor_until_terminal", lambda _jid: "NOT_FOUND"
        )
        monkeypatch.setattr(
            SlurmSSHAdapter,
            "_record_job_submission",
            staticmethod(lambda *a, **k: None),
        )
        monkeypatch.setattr(
            SlurmSSHAdapter,
            "_record_completion_safe",
            staticmethod(lambda *a, **k: None),
        )

        # run() must return without raising — the pre-fix behaviour would
        # have silently tagged the job FAILED and raised RuntimeError.
        result = adapter.run(job)

        assert result.status == JobStatus.UNKNOWN
        # Neither completed nor failed callbacks fired — UNKNOWN is
        # explicitly outside the terminal-callback set.
        assert cb.completed == []
        assert cb.failed == []

    def test_unrecognised_slurm_state_becomes_unknown(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        adapter = _bare_adapter()
        job = Job(name="weird", command=["true"], log_dir="", work_dir="")

        sj = MagicMock()
        sj.job_id = "502"
        sj.name = "weird"
        adapter._client.slurm.submit_sbatch_job = MagicMock(return_value=sj)  # type: ignore[method-assign]

        monkeypatch.setattr(
            adapter, "_monitor_until_terminal", lambda _jid: "SOME_FUTURE_STATE"
        )
        monkeypatch.setattr(
            SlurmSSHAdapter,
            "_record_job_submission",
            staticmethod(lambda *a, **k: None),
        )
        monkeypatch.setattr(
            SlurmSSHAdapter,
            "_record_completion_safe",
            staticmethod(lambda *a, **k: None),
        )

        result = adapter.run(job)
        assert result.status == JobStatus.UNKNOWN


class TestMonitorTimeout:
    """Phase 3 A-2: ``_monitor_until_terminal`` must honour a timeout."""

    def test_timeout_raises_ssh_monitor_timeout_error(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        adapter = _bare_adapter()

        # Job is always RUNNING — the loop never escapes on its own.
        monkeypatch.setattr(adapter, "get_job_status", lambda _jid: "RUNNING")

        with pytest.raises(SSHMonitorTimeoutError, match="Timed out"):
            adapter._monitor_until_terminal(123, poll_interval=0, timeout=0.1)

    def test_timeout_none_waits_until_terminal(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Explicit timeout=None disables the bound even if the env-var default would."""
        adapter = _bare_adapter()

        # Env var would otherwise force a very short timeout; None overrides.
        monkeypatch.setenv("SRUNX_SSH_MONITOR_TIMEOUT", "0.01")

        calls = {"n": 0}

        def _status(_jid):
            calls["n"] += 1
            return "COMPLETED" if calls["n"] >= 3 else "RUNNING"

        monkeypatch.setattr(adapter, "get_job_status", _status)

        result = adapter._monitor_until_terminal(42, poll_interval=0, timeout=None)
        assert result == "COMPLETED"
        assert calls["n"] == 3

    def test_env_var_default_bounds_wait_when_unspecified(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Unset ``timeout`` kwarg picks up SRUNX_SSH_MONITOR_TIMEOUT."""
        adapter = _bare_adapter()
        monkeypatch.setenv("SRUNX_SSH_MONITOR_TIMEOUT", "0.1")
        monkeypatch.setattr(adapter, "get_job_status", lambda _jid: "RUNNING")

        with pytest.raises(SSHMonitorTimeoutError):
            # No explicit timeout kwarg; env var should kick in.
            adapter._monitor_until_terminal(99, poll_interval=0)

    def test_invalid_env_var_falls_back_to_no_timeout(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Garbage env var is ignored (warning logged) — no timeout applied."""
        adapter = _bare_adapter()
        monkeypatch.setenv("SRUNX_SSH_MONITOR_TIMEOUT", "not-a-number")

        # Terminate quickly to avoid hanging the test on the pass-through path.
        calls = {"n": 0}

        def _status(_jid):
            calls["n"] += 1
            return "COMPLETED" if calls["n"] >= 2 else "RUNNING"

        monkeypatch.setattr(adapter, "get_job_status", _status)

        result = adapter._monitor_until_terminal(1, poll_interval=0)
        assert result == "COMPLETED"


class TestSSHAdapterRunInPlace:
    """Phase 2 (#135): adapter.run picks IN_PLACE only with caller permission.

    The IN_PLACE submission path is gated on
    ``submission_context.allow_in_place=True`` so callers that do
    NOT hold the per-(profile, mount) sync lock cannot race a
    concurrent rsync. CLI workflow runs flip the flag inside
    ``_hold_workflow_mounts``; Web/MCP paths leave it ``False``.
    Tests cover both halves of that gate plus the eligibility checks
    inside the IN_PLACE branch (mount membership, render-vs-source
    bytes equality).
    """

    def _setup_in_place_adapter(self, tmp_path, monkeypatch: pytest.MonkeyPatch):
        """Build an adapter with one mount, mocked submit + recording."""
        from srunx.ssh.core.config import MountConfig

        mount_local = tmp_path / "ml"
        mount_local.mkdir()
        script = mount_local / "train.sh"
        script.write_text("#!/bin/bash\necho hi\n")

        adapter = _bare_adapter()
        adapter._mounts = (
            MountConfig(name="ml", local=str(mount_local), remote="/r/ml"),
        )

        # Mock both submit paths so we can assert which fired.
        sbatch_job_mock = MagicMock()
        sbatch_job_mock.job_id = "1"
        sbatch_job_mock.name = "train"
        adapter._client.slurm.submit_sbatch_job = MagicMock(  # type: ignore[method-assign]
            return_value=sbatch_job_mock
        )

        remote_mock = MagicMock()
        remote_mock.job_id = "2"
        remote_mock.name = "train"
        adapter._client.slurm.submit_remote_sbatch_file = MagicMock(  # type: ignore[method-assign]
            return_value=remote_mock
        )

        monkeypatch.setattr(
            adapter, "_monitor_until_terminal", lambda _jid: "COMPLETED"
        )
        monkeypatch.setattr(
            SlurmSSHAdapter,
            "_record_job_submission",
            staticmethod(lambda *a, **k: None),
        )
        monkeypatch.setattr(
            SlurmSSHAdapter,
            "_record_completion_safe",
            staticmethod(lambda *a, **k: None),
        )

        return adapter, script

    def test_allow_in_place_false_keeps_temp_upload(
        self, tmp_path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Default ``allow_in_place=False`` → temp-upload, never in-place.

        This is the Web/MCP safety contract: without the workflow
        lock, the adapter must NOT take the IN_PLACE shortcut even
        when the script is mount-resident.
        """
        from srunx.domain import ShellJob
        from srunx.runtime.rendering import SubmissionRenderContext

        adapter, script = self._setup_in_place_adapter(tmp_path, monkeypatch)
        job = ShellJob(name="train", script_path=str(script))

        ctx = SubmissionRenderContext(mounts=adapter._mounts, allow_in_place=False)
        adapter.run(job, submission_context=ctx)

        adapter._client.slurm.submit_sbatch_job.assert_called_once()  # type: ignore[attr-defined]
        adapter._client.slurm.submit_remote_sbatch_file.assert_not_called()  # type: ignore[attr-defined]

    def test_allow_in_place_true_takes_in_place_path(
        self, tmp_path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """``allow_in_place=True`` + mount-resident + render==source → IN_PLACE.

        Proves the full IN_PLACE eligibility chain works end-to-end
        through ``adapter.run`` — covers the ``rendered bytes ==
        source bytes`` check, the mount membership check, and the
        ``submit_remote_sbatch_file`` dispatch.
        """
        from srunx.domain import ShellJob
        from srunx.runtime.rendering import SubmissionRenderContext

        adapter, script = self._setup_in_place_adapter(tmp_path, monkeypatch)
        job = ShellJob(name="train", script_path=str(script))

        ctx = SubmissionRenderContext(mounts=adapter._mounts, allow_in_place=True)
        adapter.run(job, submission_context=ctx)

        adapter._client.slurm.submit_remote_sbatch_file.assert_called_once()  # type: ignore[attr-defined]
        # The temp-upload path must NOT fire when IN_PLACE took over.
        adapter._client.slurm.submit_sbatch_job.assert_not_called()  # type: ignore[attr-defined]
        # The IN_PLACE call passed the translated remote path + a
        # submit_cwd under the mount.
        call = adapter._client.slurm.submit_remote_sbatch_file.call_args  # type: ignore[attr-defined]
        assert call.args[0] == "/r/ml/train.sh"
        assert call.kwargs["submit_cwd"] == "/r/ml"

    def test_in_place_skipped_when_render_differs_from_source(
        self, tmp_path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """``allow_in_place=True`` + Jinja substitution → temp-upload anyway.

        The IN_PLACE eligibility chain has three checks (allow flag,
        mount membership, rendered==source). When Jinja substitution
        actually changed bytes (the script had ``{{ ... }}`` tokens
        and ``script_vars`` filled them), the rendered output is a
        new artifact with no home in the mount and must take the
        temp-upload path even though the source path is mount-resident.
        """
        from srunx.domain import ShellJob
        from srunx.runtime.rendering import SubmissionRenderContext

        adapter, script = self._setup_in_place_adapter(tmp_path, monkeypatch)
        # Rewrite the source so it contains an actual Jinja variable.
        script.write_text("#!/bin/bash\necho '{{ greeting }}'\n", encoding="utf-8")

        # Provide script_vars so render produces different bytes.
        job = ShellJob(
            name="train",
            script_path=str(script),
            script_vars={"greeting": "hi"},
        )

        ctx = SubmissionRenderContext(mounts=adapter._mounts, allow_in_place=True)
        adapter.run(job, submission_context=ctx)

        # Render did substitute → temp-upload is the only safe path.
        adapter._client.slurm.submit_sbatch_job.assert_called_once()  # type: ignore[attr-defined]
        adapter._client.slurm.submit_remote_sbatch_file.assert_not_called()  # type: ignore[attr-defined]

    def test_in_place_skipped_when_script_outside_mount(
        self, tmp_path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Even with ``allow_in_place=True``, scripts outside any mount go via temp."""
        from srunx.domain import ShellJob
        from srunx.runtime.rendering import SubmissionRenderContext

        adapter, _script = self._setup_in_place_adapter(tmp_path, monkeypatch)

        outside = tmp_path / "scratch"
        outside.mkdir()
        outside_script = outside / "x.sh"
        outside_script.write_text("#!/bin/bash\necho hi\n")
        job = ShellJob(name="x", script_path=str(outside_script))

        ctx = SubmissionRenderContext(mounts=adapter._mounts, allow_in_place=True)
        adapter.run(job, submission_context=ctx)

        adapter._client.slurm.submit_sbatch_job.assert_called_once()  # type: ignore[attr-defined]
        adapter._client.slurm.submit_remote_sbatch_file.assert_not_called()  # type: ignore[attr-defined]


class TestSSHAdapterRunSubmissionContext:
    """Batch 2a: ``submission_context`` drives mount-aware path rewriting.

    These tests target the ``normalize_job_for_submission`` hook at the
    top of :meth:`SlurmSSHAdapter.run`. They verify that the rendered
    script sent to ``submit_sbatch_job`` reflects the *translated* paths
    when a context is supplied, and is unchanged when ``submission_context``
    is ``None`` (pre-Batch-2a semantics).
    """

    def test_none_context_preserves_local_workdir_in_rendered_script(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        adapter = _bare_adapter()
        job = Job(
            name="legacy",
            command=["echo", "ok"],
            work_dir="/tmp/local-only",
            log_dir="",
        )

        captured: dict[str, object] = {}

        def fake_submit(script_content: str, *, job_name=None, dependency=None):
            captured["content"] = script_content
            sj = MagicMock()
            sj.job_id = "1"
            sj.name = job_name
            return sj

        adapter._client.slurm.submit_sbatch_job = fake_submit  # type: ignore[method-assign,assignment]

        monkeypatch.setattr(
            adapter, "_monitor_until_terminal", lambda _jid: "COMPLETED"
        )
        monkeypatch.setattr(
            SlurmSSHAdapter,
            "_record_job_submission",
            staticmethod(lambda *a, **k: None),
        )
        monkeypatch.setattr(
            SlurmSSHAdapter,
            "_record_completion_safe",
            staticmethod(lambda *a, **k: None),
        )

        # Explicitly pass submission_context=None — legacy semantics.
        adapter.run(job, submission_context=None)

        # Rendered script uses the raw local path.
        assert "#SBATCH --chdir=/tmp/local-only" in captured["content"]  # type: ignore[operator]

    def test_context_translates_absolute_local_workdir_to_remote(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path
    ) -> None:
        """Absolute ``work_dir`` under a mount is rewritten before render."""
        from dataclasses import dataclass

        from srunx.runtime.rendering import SubmissionRenderContext

        @dataclass(frozen=True)
        class _FakeMount:
            name: str
            local: str
            remote: str

        local_root = tmp_path / "proj"
        local_root.mkdir()
        mount = _FakeMount(name="ml", local=str(local_root), remote="/home/remote/proj")
        ctx = SubmissionRenderContext(mount_name="ml", mounts=(mount,))

        adapter = _bare_adapter()
        job = Job(
            name="translated",
            command=["echo", "ok"],
            work_dir=str(local_root / "subdir"),
            log_dir="",
        )

        captured: dict[str, object] = {}

        def fake_submit(script_content: str, *, job_name=None, dependency=None):
            captured["content"] = script_content
            sj = MagicMock()
            sj.job_id = "2"
            sj.name = job_name
            return sj

        adapter._client.slurm.submit_sbatch_job = fake_submit  # type: ignore[method-assign,assignment]

        monkeypatch.setattr(
            adapter, "_monitor_until_terminal", lambda _jid: "COMPLETED"
        )
        monkeypatch.setattr(
            SlurmSSHAdapter,
            "_record_job_submission",
            staticmethod(lambda *a, **k: None),
        )
        monkeypatch.setattr(
            SlurmSSHAdapter,
            "_record_completion_safe",
            staticmethod(lambda *a, **k: None),
        )

        adapter.run(job, submission_context=ctx)

        # Rendered script has the REMOTE path, not the local path.
        assert "#SBATCH --chdir=/home/remote/proj/subdir" in captured["content"]  # type: ignore[operator]
        assert str(local_root) not in captured["content"]  # type: ignore[operator]

    def test_context_default_work_dir_fills_missing_workdir(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Empty ``work_dir`` + ``default_work_dir`` → chdir to default."""
        from srunx.runtime.rendering import SubmissionRenderContext

        ctx = SubmissionRenderContext(default_work_dir="/mnt/injected")

        adapter = _bare_adapter()
        job = Job(
            name="defwd",
            command=["echo", "ok"],
            work_dir="",
            log_dir="",
        )

        captured: dict[str, object] = {}

        def fake_submit(script_content: str, *, job_name=None, dependency=None):
            captured["content"] = script_content
            sj = MagicMock()
            sj.job_id = "3"
            sj.name = job_name
            return sj

        adapter._client.slurm.submit_sbatch_job = fake_submit  # type: ignore[method-assign,assignment]

        monkeypatch.setattr(
            adapter, "_monitor_until_terminal", lambda _jid: "COMPLETED"
        )
        monkeypatch.setattr(
            SlurmSSHAdapter,
            "_record_job_submission",
            staticmethod(lambda *a, **k: None),
        )
        monkeypatch.setattr(
            SlurmSSHAdapter,
            "_record_completion_safe",
            staticmethod(lambda *a, **k: None),
        )

        adapter.run(job, submission_context=ctx)

        assert "#SBATCH --chdir=/mnt/injected" in captured["content"]  # type: ignore[operator]

    def test_original_job_gets_terminal_status_when_context_forces_copy(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Regression: ``WorkflowRunner.all_jobs`` holds the caller's ``Job``;
        when ``normalize_job_for_submission`` returns a ``model_copy``
        the terminal status must still propagate back to the caller's
        instance so the runner doesn't declare the cell "incomplete"
        even after SLURM reports COMPLETED.
        """
        from srunx.runtime.rendering import SubmissionRenderContext

        ctx = SubmissionRenderContext(default_work_dir="/mnt/forced_copy")

        adapter = _bare_adapter()
        original = Job(
            name="propagate",
            command=["echo", "ok"],
            work_dir="",  # triggers normalize → model_copy
            log_dir="",
        )

        def fake_submit(script_content: str, *, job_name=None, dependency=None):
            sj = MagicMock()
            sj.job_id = "4242"
            sj.name = job_name
            return sj

        adapter._client.slurm.submit_sbatch_job = fake_submit  # type: ignore[method-assign,assignment]
        monkeypatch.setattr(
            adapter, "_monitor_until_terminal", lambda _jid: "COMPLETED"
        )
        monkeypatch.setattr(
            SlurmSSHAdapter,
            "_record_job_submission",
            staticmethod(lambda *a, **k: None),
        )
        monkeypatch.setattr(
            SlurmSSHAdapter,
            "_record_completion_safe",
            staticmethod(lambda *a, **k: None),
        )

        returned = adapter.run(original, submission_context=ctx)

        assert returned is original
        assert original.status == JobStatus.COMPLETED
        assert original.job_id == 4242


class TestAdapterFromSpec:
    def test_from_spec_creates_disconnected_clone(self) -> None:
        spec = SlurmSSHAdapterSpec(
            profile_name=None,
            hostname="clone.example.com",
            username="user",
            key_filename="/tmp/key",
            port=2222,
            proxy_jump=None,
            env_vars=(("FOO", "bar"),),
            mounts=(),
        )

        adapter = SlurmSSHAdapter.from_spec(spec)

        # Connection spec reconstruction is identical.
        round_trip = adapter.connection_spec
        assert round_trip.hostname == "clone.example.com"
        assert round_trip.username == "user"
        assert round_trip.port == 2222
        assert round_trip.env_vars == (("FOO", "bar"),)

    def test_from_spec_attaches_callbacks(self) -> None:
        cb = _RecordingCallback()
        spec = SlurmSSHAdapterSpec(
            profile_name=None,
            hostname="h",
            username="u",
            key_filename=None,
            port=22,
        )
        adapter = SlurmSSHAdapter.from_spec(spec, callbacks=[cb])
        assert adapter.callbacks == [cb]

    def test_spec_with_mounts_is_hashable(self, tmp_path) -> None:
        """MountConfig is frozen → the entire spec is hashable end-to-end.

        Regression lock: before Phase 4 refactor, ``MountConfig`` held a
        mutable ``list[str]`` so embedding one in a frozen dataclass gave
        a non-hashable spec despite the ``@dataclass(frozen=True)`` decorator.
        """
        from srunx.ssh.core.config import MountConfig

        mount = MountConfig(
            name="proj",
            local=str(tmp_path),
            remote="/remote/proj",
            exclude_patterns=["data/", "*.bin"],
        )
        spec = SlurmSSHAdapterSpec(
            profile_name=None,
            hostname="h",
            username="u",
            key_filename=None,
            port=22,
            mounts=(mount,),
        )
        # Must not raise — proves deep immutability of the mount chain.
        assert hash(spec) == hash(spec)
        assert {spec} == {spec}


class TestIsConnected:
    def test_is_connected_true_with_active_transport(self) -> None:
        adapter = _bare_adapter()
        assert adapter.is_connected is True

    def test_is_connected_false_when_ssh_client_none(self) -> None:
        adapter = _bare_adapter()
        adapter._client.connection.ssh_client = None
        assert adapter.is_connected is False

    def test_is_connected_false_when_transport_inactive(self) -> None:
        adapter = _bare_adapter()
        assert adapter._client.connection.ssh_client is not None
        adapter._client.connection.ssh_client.get_transport.return_value.is_active.return_value = False
        assert adapter.is_connected is False

    def test_is_connected_false_on_exception(self) -> None:
        adapter = _bare_adapter()
        assert adapter._client.connection.ssh_client is not None
        adapter._client.connection.ssh_client.get_transport.side_effect = RuntimeError(
            "boom"
        )
        assert adapter.is_connected is False
