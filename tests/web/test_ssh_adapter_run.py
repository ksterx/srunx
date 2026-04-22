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
from srunx.models import Job, JobStatus
from srunx.web.ssh_adapter import SlurmSSHAdapter, SlurmSSHAdapterSpec

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

    transport = MagicMock()
    transport.is_active.return_value = True
    ssh = MagicMock()
    ssh.get_transport.return_value = transport
    adapter._client.ssh_client = ssh

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

        adapter._client.submit_sbatch_job = fake_submit  # type: ignore[method-assign,assignment]

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
        adapter._client.submit_sbatch_job = MagicMock(return_value=sj)  # type: ignore[method-assign]

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
        adapter._client.submit_sbatch_job = MagicMock(return_value=sj)  # type: ignore[method-assign]

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
        adapter._client.submit_sbatch_job = MagicMock(return_value=sj)  # type: ignore[method-assign]

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
        adapter._client.submit_sbatch_job = MagicMock(return_value=sj)  # type: ignore[method-assign]

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
            "srunx.db.cli_helpers.record_submission_from_job", fake_record
        )
        monkeypatch.setattr(
            "srunx.db.cli_helpers.record_completion",
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
        adapter._client.submit_sbatch_job = MagicMock(return_value=None)  # type: ignore[method-assign]

        job = Job(name="nope", command=["echo"], log_dir="", work_dir="")
        with pytest.raises(RuntimeError, match="Failed to submit"):
            adapter.run(job)


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

        adapter._client.submit_sbatch_job = fake_submit  # type: ignore[method-assign,assignment]

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

        from srunx.rendering import SubmissionRenderContext

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

        adapter._client.submit_sbatch_job = fake_submit  # type: ignore[method-assign,assignment]

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
        from srunx.rendering import SubmissionRenderContext

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

        adapter._client.submit_sbatch_job = fake_submit  # type: ignore[method-assign,assignment]

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


class TestIsConnected:
    def test_is_connected_true_with_active_transport(self) -> None:
        adapter = _bare_adapter()
        assert adapter.is_connected is True

    def test_is_connected_false_when_ssh_client_none(self) -> None:
        adapter = _bare_adapter()
        adapter._client.ssh_client = None
        assert adapter.is_connected is False

    def test_is_connected_false_when_transport_inactive(self) -> None:
        adapter = _bare_adapter()
        assert adapter._client.ssh_client is not None
        adapter._client.ssh_client.get_transport.return_value.is_active.return_value = (
            False
        )
        assert adapter.is_connected is False

    def test_is_connected_false_on_exception(self) -> None:
        adapter = _bare_adapter()
        assert adapter._client.ssh_client is not None
        adapter._client.ssh_client.get_transport.side_effect = RuntimeError("boom")
        assert adapter.is_connected is False
