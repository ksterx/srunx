"""Tests for :meth:`SlurmSSHAdapter.submit_remote_sbatch`.

Codex follow-up #1 on PR #134 flagged that the existing CLI tests
mock the adapter, leaving no direct proof that ``submit_remote_sbatch``
actually records to the state DB and fires ``on_job_submitted``
callbacks. These tests pin both contracts down at the adapter
boundary.
"""

from __future__ import annotations

import threading
from unittest.mock import MagicMock

import pytest

from srunx.callbacks import Callback
from srunx.domain import ShellJob
from srunx.slurm.ssh import SlurmSSHAdapter


def _bare_adapter(
    callbacks: list[Callback] | None = None,
    *,
    profile_name: str | None = "ml-cluster",
) -> SlurmSSHAdapter:
    """Build a fully-populated adapter that bypasses real SSH I/O."""
    adapter = object.__new__(SlurmSSHAdapter)
    adapter._io_lock = threading.RLock()
    adapter._client = MagicMock()
    adapter.callbacks = list(callbacks) if callbacks else []
    adapter._profile_name = profile_name
    adapter._hostname = "testhost"
    adapter._username = "tester"
    adapter._key_filename = None
    adapter._port = 22
    adapter._proxy_jump = None
    adapter._env_vars = {}
    adapter._mounts = ()
    adapter.submission_source = "cli"

    transport = MagicMock()
    transport.is_active.return_value = True
    ssh = MagicMock()
    ssh.get_transport.return_value = transport
    adapter._client.ssh_client = ssh
    return adapter


class _RecordingCallback(Callback):
    def __init__(self) -> None:
        self.submitted: list[str] = []

    def on_job_submitted(self, job) -> None:  # type: ignore[override]
        self.submitted.append(job.name)


def _stub_inner_submit(adapter: SlurmSSHAdapter, *, job_id: str, name: str) -> None:
    """Wire ``adapter._client.slurm.submit_remote_sbatch_file`` to a MagicMock.

    The class-level ``_client`` is typed as :class:`SSHSlurmClient`,
    so mypy refuses ``return_value =`` on a real-typed method. The
    helper coerces through ``MagicMock`` once and then the typed
    descriptor doesn't get in the way.
    """
    mock_method = MagicMock()
    submitted_inner = MagicMock()
    submitted_inner.job_id = job_id
    submitted_inner.name = name
    mock_method.return_value = submitted_inner
    adapter._client.slurm.submit_remote_sbatch_file = mock_method  # type: ignore[method-assign]


def test_submit_remote_sbatch_fires_on_job_submitted_callback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``on_job_submitted`` must fire so notification watches see the job.

    Codex follow-up #1: the CLI integration test mocks the whole
    adapter and never observes this. Pin it directly here.
    """
    cb = _RecordingCallback()
    adapter = _bare_adapter(callbacks=[cb])
    _stub_inner_submit(adapter, job_id="12345", name="train")

    # Don't bother with the real DB — patch out the recorder helper
    # so this test stays pure-unit. The recording invariant is
    # asserted in the next test.
    monkeypatch.setattr(adapter, "_record_job_submission", lambda *a, **kw: None)

    job = ShellJob(name="train", script_path="/cluster/share/ml/train.sh")
    result = adapter.submit_remote_sbatch(
        "/cluster/share/ml/train.sh",
        submit_cwd="/cluster/share/ml",
        job_name="train",
        callbacks_job=job,
    )

    assert result is job  # mutated in place
    assert job.job_id == 12345
    # The callback fired against the real Job object the caller owns.
    assert cb.submitted == ["train"]


def test_submit_remote_sbatch_records_to_db(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Recording goes through ``_record_job_submission`` with the SSH triple."""
    adapter = _bare_adapter(profile_name="ml-cluster")
    _stub_inner_submit(adapter, job_id="98765", name="train")

    record_calls: list[dict] = []

    def _capture_record(job, **kwargs):  # type: ignore[no-untyped-def]
        record_calls.append({"job": job, **kwargs})

    monkeypatch.setattr(adapter, "_record_job_submission", _capture_record)

    job = ShellJob(name="train", script_path="/cluster/share/ml/train.sh")
    adapter.submit_remote_sbatch(
        "/cluster/share/ml/train.sh",
        submit_cwd="/cluster/share/ml",
        job_name="train",
        callbacks_job=job,
    )

    assert len(record_calls) == 1
    call = record_calls[0]
    # Recorded under the SSH transport with the right scheduler key
    # so the active-watch poller picks it up under the same axis the
    # job was submitted on.
    assert call["transport_type"] == "ssh"
    assert call["profile_name"] == "ml-cluster"
    assert call["scheduler_key"] == "ssh:ml-cluster"
    assert call["submission_source"] == "cli"
    assert call["job"] is job
    assert job.job_id == 98765


def test_submit_remote_sbatch_no_profile_falls_back_to_default_record(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Without a bound profile we still record, just without the SSH triple.

    Some legacy paths (direct hostname constructor, sweep tests)
    build the adapter without a profile name. Recording shouldn't
    fail in that path; it should fall through to the default
    behaviour the existing ``submit()`` method also uses.
    """
    adapter = _bare_adapter(profile_name=None)
    _stub_inner_submit(adapter, job_id="1", name="x")

    record_calls: list[dict] = []

    def _capture_record(job, **kwargs):  # type: ignore[no-untyped-def]
        record_calls.append({"job": job, **kwargs})

    monkeypatch.setattr(adapter, "_record_job_submission", _capture_record)

    job = ShellJob(name="x", script_path="/r/x.sh")
    adapter.submit_remote_sbatch("/r/x.sh", callbacks_job=job)

    assert len(record_calls) == 1
    # No SSH-triple kwargs in the no-profile path.
    assert "transport_type" not in record_calls[0]
    assert "scheduler_key" not in record_calls[0]


def test_submit_remote_sbatch_raises_when_inner_returns_none(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Inner failure surfaces as RuntimeError so the CLI catches it cleanly."""
    adapter = _bare_adapter()
    fail_method = MagicMock(return_value=None)
    adapter._client.slurm.submit_remote_sbatch_file = fail_method  # type: ignore[method-assign]
    monkeypatch.setattr(adapter, "_record_job_submission", lambda *a, **kw: None)

    with pytest.raises(RuntimeError, match="remote sbatch submission failed"):
        adapter.submit_remote_sbatch("/r/x.sh")


def test_submit_remote_sbatch_forwards_extra_args() -> None:
    """``extra_sbatch_args`` reach the inner client unchanged."""
    adapter = _bare_adapter()
    inner = MagicMock()
    # ``MagicMock(name=...)`` sets the mock's *display* name, not the
    # ``.name`` attribute we want pydantic to read. Set it explicitly.
    inner_result = MagicMock(spec=["job_id", "name"])
    inner_result.job_id = "7"
    inner_result.name = "j"
    inner.return_value = inner_result
    adapter._client.slurm.submit_remote_sbatch_file = inner  # type: ignore[method-assign]

    adapter.submit_remote_sbatch(
        "/r/x.sh",
        submit_cwd="/r",
        job_name="j",
        dependency=None,
        extra_sbatch_args=["--nodes=4", "--gpus-per-node=2"],
    )

    call_kwargs = inner.call_args.kwargs
    assert call_kwargs["extra_sbatch_args"] == ["--nodes=4", "--gpus-per-node=2"]
    assert call_kwargs["submit_cwd"] == "/r"
