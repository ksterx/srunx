"""Tests for :meth:`SlurmSSHClient.submit_remote_sbatch`.

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
from srunx.common.exceptions import SbatchIndeterminateError, SbatchRejectedError
from srunx.domain import ShellJob
from srunx.slurm.clients.ssh import SlurmSSHClient


def _bare_adapter(
    callbacks: list[Callback] | None = None,
    *,
    profile_name: str | None = "ml-cluster",
) -> SlurmSSHClient:
    """Build a fully-populated adapter that bypasses real SSH I/O."""
    adapter = object.__new__(SlurmSSHClient)
    adapter._io_lock = threading.RLock()
    adapter._client = MagicMock()
    adapter.callbacks = list(callbacks) if callbacks else []
    adapter._profile_name = profile_name
    adapter._hostname = "testhost"
    adapter._username = "tester"
    adapter._key_filename = None
    adapter._port = 22
    adapter._proxy_jump = None
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


def _stub_inner_submit(adapter: SlurmSSHClient, *, job_id: str, name: str) -> None:
    """Wire ``adapter._client.slurm.submit_remote_sbatch_file_checked`` to a MagicMock.

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
    adapter._client.slurm.submit_remote_sbatch_file_checked = mock_method  # type: ignore[method-assign]


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
    """A falsy inner result surfaces as RuntimeError so the CLI catches it cleanly."""
    adapter = _bare_adapter()
    fail_method = MagicMock(return_value=None)
    adapter._client.slurm.submit_remote_sbatch_file_checked = fail_method  # type: ignore[method-assign]
    monkeypatch.setattr(adapter, "_record_job_submission", lambda *a, **kw: None)

    with pytest.raises(RuntimeError, match="remote sbatch submission failed"):
        adapter.submit_remote_sbatch("/r/x.sh")


def test_submit_remote_sbatch_propagates_rejected_error() -> None:
    """The typed rejection reaches the caller unchanged (still a RuntimeError)."""
    adapter = _bare_adapter()
    adapter._client.slurm.submit_remote_sbatch_file_checked = MagicMock(  # type: ignore[method-assign]
        side_effect=SbatchRejectedError("sbatch: invalid partition")
    )

    with pytest.raises(SbatchRejectedError, match="invalid partition") as excinfo:
        adapter.submit_remote_sbatch("/r/x.sh")
    assert isinstance(excinfo.value, RuntimeError)


def test_submit_remote_sbatch_propagates_indeterminate_error() -> None:
    adapter = _bare_adapter()
    adapter._client.slurm.submit_remote_sbatch_file_checked = MagicMock(  # type: ignore[method-assign]
        side_effect=SbatchIndeterminateError("channel lost")
    )

    with pytest.raises(SbatchIndeterminateError, match="channel lost"):
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
    adapter._client.slurm.submit_remote_sbatch_file_checked = inner  # type: ignore[method-assign]

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


def test_submit_remote_sbatch_forwards_job_env_vars(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``--env`` must reach the in-place remote sbatch (regression guard).

    The IN_PLACE CLI path goes through ``submit_remote_sbatch`` (it passes
    ``callbacks_job=job``); the job's ``environment.env_vars`` must be forwarded
    to ``submit_remote_sbatch_file_checked`` or ``--env`` is silently dropped on the
    primary CLI SSH path.
    """
    from srunx.domain import JobEnvironment

    adapter = _bare_adapter()
    inner = MagicMock()
    inner_result = MagicMock(spec=["job_id", "name"])
    inner_result.job_id = "55"
    inner_result.name = "train"
    inner.return_value = inner_result
    adapter._client.slurm.submit_remote_sbatch_file_checked = inner  # type: ignore[method-assign]
    monkeypatch.setattr(adapter, "_record_job_submission", lambda *a, **kw: None)

    job = ShellJob(
        name="train",
        script_path="/cluster/share/ml/train.sh",
        environment=JobEnvironment(env_vars={"FOO": "bar"}),
    )
    adapter.submit_remote_sbatch(
        "/cluster/share/ml/train.sh",
        submit_cwd="/cluster/share/ml",
        job_name="train",
        callbacks_job=job,
    )

    assert inner.call_args.kwargs["job_env_vars"] == {"FOO": "bar"}


def test_submit_remote_sbatch_no_job_forwards_none_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Legacy callers without a job carry no job-level env (None, not crash)."""
    adapter = _bare_adapter()
    inner = MagicMock()
    inner_result = MagicMock(spec=["job_id", "name"])
    inner_result.job_id = "56"
    inner_result.name = "x"
    inner.return_value = inner_result
    adapter._client.slurm.submit_remote_sbatch_file_checked = inner  # type: ignore[method-assign]
    monkeypatch.setattr(adapter, "_record_job_submission", lambda *a, **kw: None)

    adapter.submit_remote_sbatch("/r/x.sh")

    assert inner.call_args.kwargs["job_env_vars"] is None


def test_submit_remote_sbatch_forwards_metadata_to_recorder(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter = _bare_adapter(profile_name="ml-cluster")
    _stub_inner_submit(adapter, job_id="11", name="train")
    record_calls: list[dict] = []
    monkeypatch.setattr(
        adapter,
        "_record_job_submission",
        lambda job, **kw: record_calls.append(kw),
    )

    meta = {"snapshot": {"repo_id": "r-1", "sha": "a" * 40}}
    adapter.submit_remote_sbatch(
        "/x/train.sh",
        callbacks_job=ShellJob(name="train", script_path="/x/train.sh"),
        metadata=meta,
    )

    assert record_calls[0]["metadata"] == meta


def test_submit_remote_sbatch_passes_metadata_none_when_absent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``metadata`` is always forwarded, as None when the caller gave none."""
    for profile in ("ml-cluster", None):
        adapter = _bare_adapter(profile_name=profile)
        _stub_inner_submit(adapter, job_id="12", name="train")
        record_calls: list[dict] = []
        monkeypatch.setattr(
            adapter,
            "_record_job_submission",
            lambda job, _c=record_calls, **kw: _c.append(kw),
        )

        adapter.submit_remote_sbatch(
            "/x/train.sh",
            callbacks_job=ShellJob(name="train", script_path="/x/train.sh"),
        )

        assert record_calls[0]["metadata"] is None


def test_submit_remote_sbatch_connect_failure_is_a_confirmed_rejection() -> None:
    """No connection means nothing was sent, so no job exists."""
    adapter = _bare_adapter()
    adapter._client.connection.ssh_client = None
    adapter._client.connect = MagicMock(return_value=False)  # type: ignore[method-assign]
    with pytest.raises(SbatchRejectedError, match="SSH connection failed"):
        adapter.submit_remote_sbatch("/x/train.sh")
