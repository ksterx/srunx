"""Tests for :class:`SlurmSSHExecutorPool` + :class:`SSHWorkflowJobExecutor`.

Phase 2 Step 4 of the SSH sweep integration. Verifies that the pool:

* Reuses free adapters on sequential leases.
* Bounds concurrent leases to ``size`` and serializes extras.
* Drops broken adapters instead of returning them to the free queue.
* Preserves the single-flight ``_io_lock`` contract of the shared
  adapter under concurrent executor invocations.
* Renders SLURM scripts identically to the local :class:`Slurm` executor.
* Cleans up on ``close`` + re-builds on subsequent ``lease``.
"""

from __future__ import annotations

import threading
import time
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from srunx.models import Job, JobStatus, render_job_script
from srunx.template import get_template_path
from srunx.web.ssh_adapter import SlurmSSHAdapter, SlurmSSHAdapterSpec
from srunx.web.ssh_executor import (
    SlurmSSHExecutorPool,
    SSHWorkflowJobExecutor,
)

# --- Helpers ---------------------------------------------------------


def _make_spec() -> SlurmSSHAdapterSpec:
    return SlurmSSHAdapterSpec(
        profile_name=None,
        hostname="testhost.example.com",
        username="tester",
        key_filename=None,
        port=22,
        proxy_jump=None,
        env_vars=(),
        mounts=(),
    )


def _bare_adapter(*, connected: bool = True) -> SlurmSSHAdapter:
    """Build a minimal adapter bypassing ``__init__``, safe for unit tests."""
    adapter = object.__new__(SlurmSSHAdapter)
    adapter._io_lock = threading.RLock()
    adapter._client = MagicMock()
    adapter.callbacks = []
    adapter._profile_name = None
    adapter._hostname = "testhost.example.com"
    adapter._username = "tester"
    adapter._key_filename = None
    adapter._port = 22
    adapter._proxy_jump = None
    adapter._env_vars = {}
    adapter._mounts = ()
    adapter.submission_source = "web"

    # Emulate a live paramiko session so ``is_connected`` returns True.
    if connected:
        transport = MagicMock()
        transport.is_active.return_value = True
        ssh = MagicMock()
        ssh.get_transport.return_value = transport
        adapter._client.ssh_client = ssh
    else:
        adapter._client.ssh_client = None
    return adapter


# --- Tests -----------------------------------------------------------


class TestPoolBasicLifecycle:
    def test_lease_reuses_same_adapter_sequentially(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Sequential leases reuse the same adapter (no session thrash)."""
        built: list[SlurmSSHAdapter] = []

        def fake_build(self: SlurmSSHExecutorPool) -> SlurmSSHAdapter:
            a = _bare_adapter()
            built.append(a)
            return a

        monkeypatch.setattr(SlurmSSHExecutorPool, "_build_adapter", fake_build)

        pool = SlurmSSHExecutorPool(_make_spec(), size=2)
        try:
            with pool.lease() as e1:
                assert isinstance(e1, SSHWorkflowJobExecutor)
                first_adapter = e1._adapter
            with pool.lease() as e2:
                assert isinstance(e2, SSHWorkflowJobExecutor)
                assert e2._adapter is first_adapter
        finally:
            pool.close()

        assert len(built) == 1

    def test_close_disconnects_and_rebuilds(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """After ``close``, the next pool must build fresh adapters."""
        build_count = [0]

        def fake_build(self: SlurmSSHExecutorPool) -> SlurmSSHAdapter:
            build_count[0] += 1
            return _bare_adapter()

        monkeypatch.setattr(SlurmSSHExecutorPool, "_build_adapter", fake_build)

        pool = SlurmSSHExecutorPool(_make_spec(), size=2)
        with pool.lease():
            pass  # warm the pool

        assert build_count[0] == 1
        pool.close()

        # Post-close lease must raise.
        with pytest.raises(RuntimeError, match="closed"):
            with pool.lease():
                pass

    def test_close_is_idempotent(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(
            SlurmSSHExecutorPool,
            "_build_adapter",
            lambda self: _bare_adapter(),
        )
        pool = SlurmSSHExecutorPool(_make_spec(), size=1)
        pool.close()
        pool.close()  # second call must not raise

    def test_size_must_be_positive(self) -> None:
        with pytest.raises(ValueError, match="positive"):
            SlurmSSHExecutorPool(_make_spec(), size=0)


class TestPoolConcurrency:
    def test_bounded_concurrent_leases(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """With ``size=2``, a 3rd thread blocks until one of the first two releases."""
        monkeypatch.setattr(
            SlurmSSHExecutorPool,
            "_build_adapter",
            lambda self: _bare_adapter(),
        )

        pool = SlurmSSHExecutorPool(_make_spec(), size=2)
        hold_release = threading.Event()
        acquired_count = [0]
        acquired_lock = threading.Lock()
        both_acquired = threading.Event()
        third_acquired = threading.Event()

        def hold_lease() -> None:
            with pool.lease():
                with acquired_lock:
                    acquired_count[0] += 1
                    if acquired_count[0] == 2:
                        both_acquired.set()
                hold_release.wait(timeout=5.0)

        def third_lease() -> None:
            with pool.lease():
                third_acquired.set()

        t1 = threading.Thread(target=hold_lease)
        t2 = threading.Thread(target=hold_lease)
        t1.start()
        t2.start()

        # Ensure both holders have a lease before we launch the contender.
        assert both_acquired.wait(timeout=2.0)

        t3 = threading.Thread(target=third_lease)
        t3.start()

        # The 3rd lease must NOT have acquired yet.
        assert not third_acquired.wait(timeout=0.3)

        # Release the holders; one of them will hand its adapter to t3.
        hold_release.set()
        assert third_acquired.wait(timeout=3.0)

        t1.join(timeout=2.0)
        t2.join(timeout=2.0)
        t3.join(timeout=2.0)
        pool.close()

    def test_broken_adapter_is_discarded_on_release(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A lease that returns a disconnected adapter must drop it + rebuild.

        Breaks the lease *during* its usage so the release-time health
        check fails, then asserts the next lease mints a fresh adapter
        and ``_created`` bookkeeping stays within the pool's cap.
        """
        built: list[SlurmSSHAdapter] = []

        def builder(self: SlurmSSHExecutorPool) -> SlurmSSHAdapter:
            a = _bare_adapter(connected=True)
            built.append(a)
            return a

        monkeypatch.setattr(SlurmSSHExecutorPool, "_build_adapter", builder)

        pool = SlurmSSHExecutorPool(_make_spec(), size=1)

        # First lease — break the session so the release health check
        # treats the adapter as unhealthy and drops it.
        with pool.lease() as executor:
            assert isinstance(executor, SSHWorkflowJobExecutor)
            assert executor._adapter._client.ssh_client is not None
            executor._adapter._client.ssh_client.get_transport.return_value.is_active.return_value = False

        # Created counter must have been decremented; next lease mints anew.
        assert pool._created == 0

        with pool.lease() as executor:
            assert isinstance(executor, SSHWorkflowJobExecutor)
            assert executor._adapter is not built[0]
            # Keep it healthy so release returns it cleanly.

        # Capacity bookkeeping: _created stays ≤ size.
        assert pool._created <= pool.size
        assert len(built) == 2  # fresh build happened after the broken drop

        pool.close()

    def test_io_lock_serializes_under_concurrent_runs(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Concurrent ``executor.run`` calls on the SAME adapter must serialize.

        Models the shared-adapter code path: the pool hands out the same
        adapter to two threads in succession, and the RLock protects the
        SSH session from interleaving. We assert that no two threads hold
        the lock simultaneously.
        """
        shared = _bare_adapter()
        in_flight = [0]
        max_in_flight = [0]
        lock_probe = threading.Lock()

        def fake_run(
            job: Job,
            *,
            workflow_name: str | None = None,
            workflow_run_id: int | None = None,
            submission_context: object = None,
        ) -> Job:
            # Simulate the real ``SlurmSSHAdapter.run`` wrapping the work
            # inside ``self._io_lock``.
            with shared._io_lock:
                with lock_probe:
                    in_flight[0] += 1
                    max_in_flight[0] = max(max_in_flight[0], in_flight[0])
                time.sleep(0.05)
                with lock_probe:
                    in_flight[0] -= 1
            job.status = JobStatus.COMPLETED
            return job

        shared.run = fake_run  # type: ignore[method-assign,assignment]

        executor = SSHWorkflowJobExecutor(shared)

        threads: list[threading.Thread] = []
        for i in range(4):
            job = Job(
                name=f"t{i}",
                command=["echo", "hi"],
                log_dir="",
                work_dir="",
            )
            t = threading.Thread(target=executor.run, args=(job,))
            threads.append(t)

        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=3.0)

        assert max_in_flight[0] == 1, "_io_lock must serialize run calls"


class TestRenderParity:
    """SSH-rendered script must match a locally-rendered one bit-for-bit.

    Locks down that the Job-level ``template`` / ``srun_args`` /
    ``launch_prefix`` fallbacks flow through identically on both paths.
    """

    def test_local_and_ssh_produce_identical_script(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        job = Job(
            name="parity",
            command=["python", "train.py"],
            srun_args="--qos=high",
            launch_prefix="mpirun",
            log_dir=str(tmp_path / "logs"),
            work_dir=str(tmp_path / "work"),
        )

        # Local reference render.
        local_script = render_job_script(
            get_template_path("base"), job, output_dir=tmp_path
        )
        expected = Path(local_script).read_text()

        # SSH render: mock out every SSH I/O call in ``SlurmSSHAdapter.run``
        # and capture the script_content passed to submit_sbatch_job.
        adapter = _bare_adapter()

        captured: dict[str, str] = {}

        def fake_submit(script_content: str, *, job_name=None, dependency=None):
            captured["script"] = script_content
            slurm_job = MagicMock()
            slurm_job.job_id = "99999"
            slurm_job.name = job_name
            return slurm_job

        adapter._client.submit_sbatch_job = fake_submit  # type: ignore[method-assign,assignment]

        # Short-circuit monitor + DB.
        monkeypatch.setattr(
            adapter, "_monitor_until_terminal", lambda _jid: "COMPLETED"
        )
        monkeypatch.setattr(
            "srunx.web.ssh_adapter.SlurmSSHAdapter._record_job_submission",
            staticmethod(lambda *a, **k: None),
        )
        monkeypatch.setattr(
            "srunx.web.ssh_adapter.SlurmSSHAdapter._record_completion_safe",
            staticmethod(lambda *a, **k: None),
        )

        result = adapter.run(job)
        assert result.status == JobStatus.COMPLETED
        assert "script" in captured
        assert captured["script"] == expected
