"""Phase 2 Step 5 integration tests: SSH executor factory wiring.

Verifies the thin glue between :class:`SweepOrchestrator` and
:class:`~srunx.slurm.ssh_executor.SlurmSSHExecutorPool` introduced in Step 5:

* ``executor_factory=None`` (CLI default) keeps the legacy local
  :class:`Slurm` singleton path.
* A sweep run with a pool-backed ``executor_factory`` leases a fresh
  executor per cell and runs its ``run(...)`` through that lease.
* The Web ``_dispatch_sweep`` helper constructs a pool from the adapter's
  ``connection_spec``, wires ``pool.lease`` into the orchestrator, and
  closes the pool once the background task finishes.
* A cell failure still leaves the sweep in a ``failed`` terminal state
  (pool close happens regardless).
"""

from __future__ import annotations

import threading
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import anyio
import pytest
import yaml

from srunx.client_protocol import WorkflowJobExecutorProtocol
from srunx.db.connection import open_connection, transaction
from srunx.db.repositories.sweep_runs import SweepRunRepository
from srunx.models import JobStatus, RunnableJobType
from srunx.sweep import SweepSpec
from srunx.sweep.orchestrator import SweepOrchestrator
from srunx.sweep.state_service import WorkflowRunStateService

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _write_wf(tmp_path: Path) -> Path:
    path = tmp_path / "wf.yaml"
    path.write_text(
        yaml.dump(
            {
                "name": "ssh_sweep",
                "args": {"lr": 0.1},
                "jobs": [
                    {
                        "name": "train",
                        "command": ["train.py"],
                        "environment": {"conda": "env"},
                    }
                ],
            }
        )
    )
    return path


def _now_iso() -> str:
    from srunx.db.repositories.base import now_iso

    return now_iso()


def _drive_workflow_run(workflow_run_id: int, final_status: str = "completed") -> None:
    """Mirror what :class:`WorkflowRunner` does for a single cell.

    We do not want to depend on the real runner here — the integration
    contract we care about is ``SweepOrchestrator`` → ``executor_factory``.
    So we emulate the workflow_run state transitions directly while the
    orchestrator still drives its own aggregation counters.
    """
    conn = open_connection()
    try:
        with transaction(conn, "IMMEDIATE"):
            WorkflowRunStateService.update(
                conn=conn,
                workflow_run_id=workflow_run_id,
                from_status="pending",
                to_status="running",
            )
        with transaction(conn, "IMMEDIATE"):
            WorkflowRunStateService.update(
                conn=conn,
                workflow_run_id=workflow_run_id,
                from_status="running",
                to_status=final_status,
                completed_at=_now_iso(),
            )
    finally:
        conn.close()


def _read_sweep(sweep_run_id: int) -> dict[str, Any]:
    conn = open_connection()
    try:
        row = conn.execute(
            "SELECT status, cell_count, cells_completed, cells_failed "
            "FROM sweep_runs WHERE id = ?",
            (sweep_run_id,),
        ).fetchone()
    finally:
        conn.close()
    assert row is not None
    return dict(row)


class _FakeExecutor:
    """Minimal :class:`WorkflowJobExecutorProtocol` stub.

    Records every ``run`` invocation and returns the job flipped to
    the configured terminal status so ``WorkflowRunner`` semantics are
    preserved for the parts of the code that inspect ``job._status``.
    """

    def __init__(self, final_status: JobStatus = JobStatus.COMPLETED) -> None:
        self.final_status = final_status
        self.calls: list[str] = []

    def run(
        self,
        job: RunnableJobType,
        *,
        workflow_name: str | None = None,
        workflow_run_id: int | None = None,
        submission_context: object = None,
    ) -> RunnableJobType:
        self.calls.append(job.name)
        job.job_id = 999
        job._status = self.final_status
        return job

    def get_job_output_detailed(
        self,
        job_id: int | str,
        job_name: str | None = None,
        skip_content: bool = False,
    ) -> dict[str, str | list[str] | None]:
        return {"output": "", "error": "", "found_files": []}


class _FakePool:
    """Stand-in for :class:`SlurmSSHExecutorPool`.

    Produces a shared :class:`_FakeExecutor` on every lease and records
    lease / close events so tests can assert the Web dispatcher's
    lifecycle contract without touching paramiko.
    """

    def __init__(self, final_status: JobStatus = JobStatus.COMPLETED) -> None:
        self.executor = _FakeExecutor(final_status=final_status)
        self.lease_count = 0
        self.close_count = 0
        self._lock = threading.Lock()

    @contextmanager
    def lease(self) -> Iterator[WorkflowJobExecutorProtocol]:
        with self._lock:
            self.lease_count += 1
        yield self.executor

    def close(self) -> None:
        with self._lock:
            self.close_count += 1


def _build_orch(
    tmp_path: Path,
    *,
    matrix: dict[str, list[Any]],
    executor_factory: Any | None = None,
    max_parallel: int = 2,
    fail_fast: bool = False,
    submission_source: str = "web",
) -> SweepOrchestrator:
    return SweepOrchestrator(
        workflow_yaml_path=_write_wf(tmp_path),
        workflow_data={"name": "ssh_sweep", "args": {"lr": 0.1}},
        args_override=None,
        sweep_spec=SweepSpec(
            matrix=matrix,
            fail_fast=fail_fast,
            max_parallel=max_parallel,
        ),
        submission_source=submission_source,  # type: ignore[arg-type]
        executor_factory=executor_factory,
    )


# ---------------------------------------------------------------------------
# Orchestrator ← executor_factory wiring
# ---------------------------------------------------------------------------


class TestOrchestratorExecutorFactoryWiring:
    def test_default_factory_is_none(self, tmp_path: Path) -> None:
        """Backward-compat guard: omitting ``executor_factory`` keeps it None."""
        orch = _build_orch(tmp_path, matrix={"lr": [0.1]}, submission_source="cli")
        assert orch.executor_factory is None

    def test_factory_is_stored_on_instance(self, tmp_path: Path) -> None:
        pool = _FakePool()
        lease_fn = pool.lease
        orch = _build_orch(
            tmp_path,
            matrix={"lr": [0.1]},
            executor_factory=lease_fn,
        )
        # Using ``==`` (not ``is``) because each bound-method access
        # returns a fresh ``MethodType`` instance in CPython; equality
        # still holds because bound methods compare by underlying
        # function + instance.
        assert orch.executor_factory == lease_fn
        assert orch.executor_factory is lease_fn

    def test_run_cell_sync_forwards_factory_to_runner(
        self,
        isolated_db: Path,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """``_run_cell_sync`` must pass ``executor_factory`` into ``from_yaml``."""
        pool = _FakePool()
        lease_fn = pool.lease
        orch = _build_orch(
            tmp_path,
            matrix={"lr": [0.1]},
            executor_factory=lease_fn,
        )

        captured: dict[str, Any] = {}

        def fake_from_yaml(*args: Any, **kwargs: Any) -> Any:
            captured["kwargs"] = kwargs

            class _Stub:
                def run(
                    self_inner, *, workflow_run_id: int | None = None
                ) -> dict[str, Any]:
                    assert workflow_run_id is not None
                    _drive_workflow_run(workflow_run_id, "completed")
                    return {}

            return _Stub()

        from srunx import runner as runner_mod

        monkeypatch.setattr(
            runner_mod.WorkflowRunner, "from_yaml", staticmethod(fake_from_yaml)
        )
        orch.run()

        assert "kwargs" in captured
        assert captured["kwargs"].get("executor_factory") == lease_fn


# ---------------------------------------------------------------------------
# End-to-end orchestrator + fake pool: all cells complete
# ---------------------------------------------------------------------------


class TestSweepWithFakePool:
    """Drive a full sweep through the orchestrator with a fake SSH pool.

    The fake pool exposes the same ``lease`` contract the real pool does,
    so the orchestrator path exercises ``executor_factory`` for real.
    """

    def test_six_cells_all_complete(
        self,
        isolated_db: Path,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        pool = _FakePool()
        orch = _build_orch(
            tmp_path,
            matrix={"lr": [0.1, 0.01, 0.001], "seed": [1, 2]},
            executor_factory=pool.lease,
            max_parallel=3,
        )

        # Emulate the runner by driving the workflow_run directly; the
        # orchestrator still invokes executor_factory via
        # ``from_yaml(executor_factory=...).run(...)``. We stub from_yaml
        # to a minimal object that calls into the leased executor once
        # per cell, matching the real runner's single-job behaviour.
        def _fake_from_yaml(*args: Any, **kwargs: Any) -> Any:
            factory = kwargs.get("executor_factory")

            class _Stub:
                def run(self_inner, *, workflow_run_id: int) -> dict[str, Any]:
                    # Exercise the factory so its lease counter increments.
                    assert factory is not None
                    with factory() as executor:
                        from srunx.models import Job

                        _job = Job(name="train", command=["train.py"])
                        executor.run(_job, workflow_run_id=workflow_run_id)
                    _drive_workflow_run(workflow_run_id, "completed")
                    return {}

            return _Stub()

        from srunx import runner as runner_mod

        monkeypatch.setattr(
            runner_mod.WorkflowRunner, "from_yaml", staticmethod(_fake_from_yaml)
        )

        sweep = orch.run()
        assert sweep.id is not None
        row = _read_sweep(sweep.id)
        assert row["cell_count"] == 6
        assert row["cells_completed"] == 6
        assert row["cells_failed"] == 0
        assert row["status"] == "completed"

        # Every cell leased an executor and called ``run`` on the fake
        # executor once. The pool itself is not closed by the
        # orchestrator — that's the Web dispatcher's responsibility.
        assert pool.lease_count == 6
        assert len(pool.executor.calls) == 6
        assert pool.close_count == 0

    def test_one_cell_fails_sweep_failed(
        self,
        isolated_db: Path,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        pool = _FakePool()
        orch = _build_orch(
            tmp_path,
            matrix={"lr": [0.1, 0.01]},
            executor_factory=pool.lease,
            max_parallel=2,
        )

        def _fake_from_yaml(*args: Any, **kwargs: Any) -> Any:
            factory = kwargs.get("executor_factory")
            override = kwargs.get("args_override") or {}

            class _Stub:
                def run(self_inner, *, workflow_run_id: int) -> dict[str, Any]:
                    assert factory is not None
                    with factory() as executor:
                        from srunx.models import Job

                        _job = Job(name="train", command=["train.py"])
                        executor.run(_job, workflow_run_id=workflow_run_id)
                    # Fail the 2nd cell (lr=0.01) so the sweep aggregates
                    # to ``failed``.
                    if override.get("lr") == 0.01:
                        _drive_workflow_run(workflow_run_id, "failed")
                        raise RuntimeError("simulated cell failure")
                    _drive_workflow_run(workflow_run_id, "completed")
                    return {}

            return _Stub()

        from srunx import runner as runner_mod

        monkeypatch.setattr(
            runner_mod.WorkflowRunner, "from_yaml", staticmethod(_fake_from_yaml)
        )

        sweep = orch.run()
        assert sweep.id is not None
        row = _read_sweep(sweep.id)
        assert row["cell_count"] == 2
        assert row["cells_completed"] == 1
        assert row["cells_failed"] == 1
        assert row["status"] == "failed"


# ---------------------------------------------------------------------------
# Web _dispatch_sweep lifecycle
# ---------------------------------------------------------------------------


class TestWebDispatchPoolLifecycle:
    """Guard the Step 5b contract inside ``_dispatch_sweep``.

    We don't spin up a full FastAPI app here — those paths are covered by
    ``tests/web/test_sweep_runs_api.py``. Instead we assert three
    invariants directly:

    1. ``_dispatch_sweep`` constructs a :class:`SlurmSSHExecutorPool` from
       the adapter's ``connection_spec``.
    2. That pool's ``lease`` method is handed to ``SweepOrchestrator`` as
       the ``executor_factory`` kwarg.
    3. ``_run_sweep_background`` closes the pool once the orchestrator
       task finishes (success or failure).
    """

    def _seed_sweep_row(self, *, cell_count: int = 2) -> int:
        conn = open_connection()
        try:
            return SweepRunRepository(conn).create(
                name="sweep-test",
                matrix={"lr": [0.01, 0.1]},
                args=None,
                fail_fast=False,
                max_parallel=2,
                cell_count=cell_count,
                submission_source="web",
                status="pending",
            )
        finally:
            conn.close()

    def test_dispatch_builds_pool_and_wires_factory(
        self,
        isolated_db: Path,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        from srunx.slurm.ssh import SlurmSSHAdapterSpec
        from srunx.web.routers import workflows as wf_mod

        yaml_path = _write_wf(tmp_path)
        spec = SlurmSSHAdapterSpec(
            profile_name=None,
            hostname="fake.example.com",
            username="tester",
            key_filename=None,
            port=22,
        )
        adapter = MagicMock()
        adapter.connection_spec = spec

        # Capture the pool instance and SweepOrchestrator kwargs without
        # running the actual sweep loop. Materialize is stubbed to a
        # seeded row.
        seeded_id = self._seed_sweep_row()

        pool_instances: list[Any] = []

        class _SpyPool:
            def __init__(
                self,
                spec_arg: Any,
                *,
                callbacks: Any = None,
                size: int = 8,
                submission_source: str = "web",
            ) -> None:
                self.spec = spec_arg
                self.size = size
                self.submission_source = submission_source
                self.close_calls = 0
                pool_instances.append(self)

            def lease(self) -> Any:  # pragma: no cover — not called
                raise RuntimeError("lease should not be invoked in this test")

            def close(self) -> None:
                self.close_calls += 1

        monkeypatch.setattr(wf_mod, "SlurmSSHExecutorPool", _SpyPool)

        captured: dict[str, Any] = {}

        class _FakeOrchestrator:
            def __init__(self, **kwargs: Any) -> None:
                captured["init_kwargs"] = kwargs

            def materialize(self) -> int:
                return seeded_id

            async def arun_from_materialized(self, sweep_run_id: int) -> None:
                return None

        monkeypatch.setattr(wf_mod, "SweepOrchestrator", _FakeOrchestrator)

        # Build a minimal request object with a fake app/state. The
        # dispatcher falls back to ``asyncio.create_task`` when
        # ``request.app.state.task_group`` is absent.
        fake_request = MagicMock()
        fake_request.app.state.task_group = None
        fake_request.app.state.background_tasks = None

        body = wf_mod.WorkflowRunRequest(
            sweep=wf_mod.SweepSpecRequest(matrix={"lr": [0.01, 0.1]}, max_parallel=2)
        )

        async def _call() -> dict[str, Any]:
            return await wf_mod._dispatch_sweep(
                yaml_path=yaml_path,
                name="ssh_sweep",
                body=body,
                request=fake_request,
                adapter=adapter,
            )

        response = anyio.run(_call)
        assert response["sweep_run_id"] == seeded_id

        # Pool built once from the adapter's connection_spec.
        assert len(pool_instances) == 1
        built_pool = pool_instances[0]
        assert built_pool.spec is spec
        # max_parallel=2 → min(2, 8) = 2
        assert built_pool.size == 2

        # Orchestrator received pool.lease as executor_factory.
        init_kwargs = captured["init_kwargs"]
        assert init_kwargs["executor_factory"] == built_pool.lease
        assert init_kwargs["submission_source"] == "web"

    def test_dispatch_pool_close_on_background_finish(
        self,
        isolated_db: Path,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """``_run_sweep_background`` must invoke ``pool.close`` after arun."""
        from srunx.web.routers import workflows as wf_mod

        close_events: list[str] = []

        class _Pool:
            def close(self) -> None:
                close_events.append("closed")

        class _Orch:
            def __init__(self) -> None:
                self.arun_called = False

            async def arun_from_materialized(self, sweep_run_id: int) -> None:
                self.arun_called = True

        orch = _Orch()
        pool = _Pool()

        async def _driver() -> None:
            await wf_mod._run_sweep_background(orch, 123, pool)  # type: ignore[arg-type]

        anyio.run(_driver)

        assert orch.arun_called is True
        assert close_events == ["closed"]

    def test_dispatch_pool_close_even_if_arun_raises(
        self,
        isolated_db: Path,
        tmp_path: Path,
    ) -> None:
        """Pool close is in a ``finally`` — arun exceptions don't skip it."""
        from srunx.web.routers import workflows as wf_mod

        close_events: list[str] = []

        class _Pool:
            def close(self) -> None:
                close_events.append("closed")

        class _Orch:
            async def arun_from_materialized(self, sweep_run_id: int) -> None:
                raise RuntimeError("boom")

        # ``_run_sweep_background`` swallows the exception — close still fires.
        orch = _Orch()
        pool = _Pool()

        async def _driver() -> None:
            await wf_mod._run_sweep_background(orch, 123, pool)  # type: ignore[arg-type]

        anyio.run(_driver)
        assert close_events == ["closed"]

    def test_dispatch_pool_size_capped_at_8(
        self,
        isolated_db: Path,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """``max_parallel=20`` → pool size clamped to 8."""
        from srunx.slurm.ssh import SlurmSSHAdapterSpec
        from srunx.web.routers import workflows as wf_mod

        yaml_path = _write_wf(tmp_path)
        spec = SlurmSSHAdapterSpec(
            profile_name=None,
            hostname="fake.example.com",
            username="tester",
            key_filename=None,
            port=22,
        )
        adapter = MagicMock()
        adapter.connection_spec = spec
        seeded_id = self._seed_sweep_row()

        sizes: list[int] = []

        class _SpyPool:
            def __init__(
                self,
                spec_arg: Any,
                *,
                callbacks: Any = None,
                size: int = 8,
                submission_source: str = "web",
            ):
                sizes.append(size)

            def lease(self) -> Any:  # pragma: no cover
                raise RuntimeError

            def close(self) -> None:
                pass

        monkeypatch.setattr(wf_mod, "SlurmSSHExecutorPool", _SpyPool)

        class _FakeOrchestrator:
            def __init__(self, **kwargs: Any) -> None:
                pass

            def materialize(self) -> int:
                return seeded_id

            async def arun_from_materialized(self, sweep_run_id: int) -> None:
                return None

        monkeypatch.setattr(wf_mod, "SweepOrchestrator", _FakeOrchestrator)

        fake_request = MagicMock()
        fake_request.app.state.task_group = None
        fake_request.app.state.background_tasks = None

        body = wf_mod.WorkflowRunRequest(
            sweep=wf_mod.SweepSpecRequest(matrix={"lr": [0.01, 0.1]}, max_parallel=20)
        )

        async def _call() -> dict[str, Any]:
            return await wf_mod._dispatch_sweep(
                yaml_path=yaml_path,
                name="ssh_sweep",
                body=body,
                request=fake_request,
                adapter=adapter,
            )

        anyio.run(_call)
        assert sizes == [8]


# ---------------------------------------------------------------------------
# Backward-compat: executor_factory=None still works
# ---------------------------------------------------------------------------


class TestBackwardCompat:
    """CLI / MCP path uses ``executor_factory=None`` — must keep working."""

    def test_orchestrator_run_without_factory(
        self,
        isolated_db: Path,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        orch = _build_orch(
            tmp_path,
            matrix={"lr": [0.1, 0.01]},
            executor_factory=None,
            submission_source="cli",
        )
        assert orch.executor_factory is None

        captured_kwargs: list[dict[str, Any]] = []

        def _fake_from_yaml(*args: Any, **kwargs: Any) -> Any:
            captured_kwargs.append(kwargs)

            class _Stub:
                def run(self_inner, *, workflow_run_id: int) -> dict[str, Any]:
                    _drive_workflow_run(workflow_run_id, "completed")
                    return {}

            return _Stub()

        from srunx import runner as runner_mod

        monkeypatch.setattr(
            runner_mod.WorkflowRunner, "from_yaml", staticmethod(_fake_from_yaml)
        )

        sweep = orch.run()
        assert sweep.id is not None
        row = _read_sweep(sweep.id)
        assert row["cells_completed"] == 2
        assert row["status"] == "completed"

        # Every from_yaml invocation received ``executor_factory=None``.
        for kwargs in captured_kwargs:
            assert kwargs.get("executor_factory") is None


# ---------------------------------------------------------------------------
# Phase 2 Batch 2b: submission_context passthrough
# ---------------------------------------------------------------------------


class TestSubmissionContextPassthrough:
    """Orchestrator must forward ``submission_context`` into every cell's runner.

    The Web dispatcher builds a :class:`SubmissionRenderContext` from the
    configured SSH profile + selected mount and hands it to
    :class:`SweepOrchestrator`. Each cell's :meth:`WorkflowRunner.from_yaml`
    call must receive that same context so the SSH adapter can translate
    local mount paths before rendering.
    """

    def test_default_submission_context_is_none(self, tmp_path: Path) -> None:
        """Backward-compat guard: omitting ``submission_context`` keeps it None."""
        orch = _build_orch(
            tmp_path,
            matrix={"lr": [0.1]},
            submission_source="cli",
        )
        assert orch.submission_context is None

    def test_submission_context_is_stored_on_instance(self, tmp_path: Path) -> None:
        from srunx.rendering import SubmissionRenderContext

        ctx = SubmissionRenderContext(
            mount_name="proj",
            mounts=(),
            default_work_dir="/remote/proj",
        )
        orch = SweepOrchestrator(
            workflow_yaml_path=_write_wf(tmp_path),
            workflow_data={"name": "ssh_sweep", "args": {"lr": 0.1}},
            args_override=None,
            sweep_spec=SweepSpec(
                matrix={"lr": [0.1]},
                fail_fast=False,
                max_parallel=1,
            ),
            submission_source="web",
            submission_context=ctx,
        )
        assert orch.submission_context is ctx

    def test_run_cell_sync_forwards_submission_context_to_runner(
        self,
        isolated_db: Path,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """``_run_cell_sync`` must pass ``submission_context`` into ``from_yaml``."""
        from srunx.rendering import SubmissionRenderContext

        ctx = SubmissionRenderContext(
            mount_name="proj",
            mounts=(),
            default_work_dir="/remote/proj",
        )
        orch = SweepOrchestrator(
            workflow_yaml_path=_write_wf(tmp_path),
            workflow_data={"name": "ssh_sweep", "args": {"lr": 0.1}},
            args_override=None,
            sweep_spec=SweepSpec(
                matrix={"lr": [0.1]},
                fail_fast=False,
                max_parallel=1,
            ),
            submission_source="web",
            submission_context=ctx,
        )

        captured: dict[str, Any] = {}

        def fake_from_yaml(*args: Any, **kwargs: Any) -> Any:
            captured["kwargs"] = kwargs

            class _Stub:
                def run(
                    self_inner, *, workflow_run_id: int | None = None
                ) -> dict[str, Any]:
                    assert workflow_run_id is not None
                    _drive_workflow_run(workflow_run_id, "completed")
                    return {}

            return _Stub()

        from srunx import runner as runner_mod

        monkeypatch.setattr(
            runner_mod.WorkflowRunner, "from_yaml", staticmethod(fake_from_yaml)
        )
        orch.run()

        assert "kwargs" in captured
        assert captured["kwargs"].get("submission_context") is ctx

    def test_none_submission_context_still_passes_through(
        self,
        isolated_db: Path,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """``submission_context=None`` (CLI default) reaches ``from_yaml`` verbatim."""
        orch = _build_orch(
            tmp_path,
            matrix={"lr": [0.1]},
            submission_source="cli",
        )
        assert orch.submission_context is None

        captured_kwargs: list[dict[str, Any]] = []

        def fake_from_yaml(*args: Any, **kwargs: Any) -> Any:
            captured_kwargs.append(kwargs)

            class _Stub:
                def run(
                    self_inner, *, workflow_run_id: int | None = None
                ) -> dict[str, Any]:
                    assert workflow_run_id is not None
                    _drive_workflow_run(workflow_run_id, "completed")
                    return {}

            return _Stub()

        from srunx import runner as runner_mod

        monkeypatch.setattr(
            runner_mod.WorkflowRunner, "from_yaml", staticmethod(fake_from_yaml)
        )
        orch.run()

        assert captured_kwargs, "from_yaml was never called"
        for kwargs in captured_kwargs:
            assert kwargs.get("submission_context") is None
