"""Integration tests for the sweep Web API surface.

Covers:
- ``POST /api/workflows/{name}/run`` with ``sweep`` body (Phase G dispatch).
- ``args_override`` on the non-sweep path.
- ``python:`` rejection in args_override and sweep.matrix values.
- ``GET /api/sweep_runs`` / ``GET /api/sweep_runs/{id}`` / ``/cells``.
- ``POST /api/sweep_runs/{id}/cancel``.
"""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient
from srunx.db.connection import init_db

from srunx.web.app import create_app
from srunx.web.deps import get_adapter

MOUNT = "test-project"


@pytest.fixture
def mock_adapter() -> MagicMock:
    adapter = MagicMock()
    adapter.submit_job.return_value = {
        "name": "job",
        "job_id": 55555,
        "status": "PENDING",
        "depends_on": [],
        "command": [],
        "resources": {},
    }
    adapter.scheduler_key = "local"
    return adapter


@pytest.fixture
def client(  # type: ignore[misc]
    mock_adapter: MagicMock,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> Iterator[TestClient]:
    import srunx.web.config as config_mod
    from srunx.ssh.core.config import MountConfig, ServerProfile
    from srunx.web.config import get_web_config

    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / "xdg"))
    init_db(delete_legacy=False)

    original = config_mod._config
    config_mod._config = None
    cfg = get_web_config()
    config_mod._config = cfg

    mount_local = tmp_path / "project"
    mount_local.mkdir()

    fake_mount = MountConfig(
        name=MOUNT, local=str(mount_local), remote="/home/user/project"
    )
    fake_profile = ServerProfile(
        hostname="test.example.com",
        username="tester",
        key_filename="~/.ssh/id_rsa",
        mounts=[fake_mount],
    )

    app = create_app()
    app.dependency_overrides[get_adapter] = lambda: mock_adapter

    with patch(
        "srunx.web.routers.workflows._get_current_profile",
        return_value=fake_profile,
    ):
        yield TestClient(app, raise_server_exceptions=False)

    config_mod._config = original


def _create_workflow(client: TestClient, *, name: str = "sweep-wf") -> None:
    payload = {
        "name": name,
        "default_project": MOUNT,
        "args": {"lr": "0.01", "seed": "1"},
        "jobs": [
            {
                "name": "train",
                "command": ["echo", "{{ lr }}", "{{ seed }}"],
            }
        ],
    }
    resp = client.post("/api/workflows/create", json=payload)
    assert resp.status_code == 200, resp.text


class _FakeSweepRun:
    """Drop-in for ``SweepRun`` the orchestrator would have returned."""

    def __init__(self, sweep_id: int = 101, cell_count: int = 2) -> None:
        self.id = sweep_id
        self.cell_count = cell_count
        self.status = "completed"


class TestRunWorkflowWithSweep:
    def test_run_with_sweep_returns_sweep_run_id(
        self, client: TestClient, mock_adapter: MagicMock
    ) -> None:
        _create_workflow(client)

        with (
            patch("srunx.web.routers.workflows.SweepOrchestrator") as orch_cls,
            patch("srunx.web.routers.workflows.SweepSpec") as spec_cls,
        ):
            # Let the real SweepSpec through; we only stub the orchestrator.
            from srunx.sweep import SweepSpec as _RealSweepSpec

            spec_cls.side_effect = _RealSweepSpec

            # C4: the router now calls ``materialize()`` synchronously
            # (to obtain a sweep_run_id for the 202) and spawns
            # ``arun_from_materialized`` on the app's task group. We
            # stub both.
            mock_orch = MagicMock()
            mock_orch.materialize.return_value = 101

            async def fake_arun_from_materialized(_sweep_run_id: int) -> _FakeSweepRun:
                return _FakeSweepRun(sweep_id=101, cell_count=2)

            mock_orch.arun_from_materialized = fake_arun_from_materialized
            orch_cls.return_value = mock_orch

            # Also seed the sweep_runs row so the response can load
            # the freshly-materialized row (materialize is stubbed so
            # no real row exists).
            from srunx.db.connection import open_connection
            from srunx.db.repositories.sweep_runs import SweepRunRepository

            conn = open_connection()
            try:
                seeded_id = SweepRunRepository(conn).create(
                    name="sweep-wf",
                    matrix={"lr": [0.01, 0.1]},
                    args=None,
                    fail_fast=False,
                    max_parallel=2,
                    cell_count=2,
                    submission_source="web",
                    status="pending",
                )
            finally:
                conn.close()
            mock_orch.materialize.return_value = seeded_id

            resp = client.post(
                "/api/workflows/sweep-wf/run",
                params={"mount": MOUNT},
                json={
                    "sweep": {
                        "matrix": {"lr": [0.01, 0.1]},
                        "max_parallel": 2,
                    }
                },
            )

        assert resp.status_code == 202, resp.text
        data = resp.json()
        assert data["sweep_run_id"] == seeded_id
        assert data["status"] == "pending"
        assert data["cell_count"] == 2
        # Orchestrator was invoked with submission_source='web'
        kwargs = orch_cls.call_args.kwargs
        assert kwargs["submission_source"] == "web"
        # materialize() was called synchronously.
        mock_orch.materialize.assert_called_once()

    def test_run_with_sweep_wires_submission_context(
        self, client: TestClient, mock_adapter: MagicMock
    ) -> None:
        """Phase 2 Batch 2b: the dispatcher hands a mount-aware
        ``SubmissionRenderContext`` to ``SweepOrchestrator`` so each cell's
        runner sees the same mount translation as the non-sweep path.
        """
        _create_workflow(client, name="ctx-sweep")

        with (
            patch("srunx.web.routers.workflows.SweepOrchestrator") as orch_cls,
            patch("srunx.web.routers.workflows.SweepSpec") as spec_cls,
        ):
            from srunx.sweep import SweepSpec as _RealSweepSpec

            spec_cls.side_effect = _RealSweepSpec

            from srunx.db.connection import open_connection
            from srunx.db.repositories.sweep_runs import SweepRunRepository

            conn = open_connection()
            try:
                seeded_id = SweepRunRepository(conn).create(
                    name="ctx-sweep",
                    matrix={"lr": [0.01, 0.1]},
                    args=None,
                    fail_fast=False,
                    max_parallel=2,
                    cell_count=2,
                    submission_source="web",
                    status="pending",
                )
            finally:
                conn.close()

            mock_orch = MagicMock()
            mock_orch.materialize.return_value = seeded_id

            async def fake_arun_from_materialized(_sweep_run_id: int) -> _FakeSweepRun:
                return _FakeSweepRun(sweep_id=seeded_id, cell_count=2)

            mock_orch.arun_from_materialized = fake_arun_from_materialized
            orch_cls.return_value = mock_orch

            resp = client.post(
                "/api/workflows/ctx-sweep/run",
                params={"mount": MOUNT},
                json={
                    "sweep": {
                        "matrix": {"lr": [0.01, 0.1]},
                        "max_parallel": 2,
                    }
                },
            )

        assert resp.status_code == 202, resp.text

        kwargs = orch_cls.call_args.kwargs
        assert "submission_context" in kwargs

        from srunx.rendering import SubmissionRenderContext

        ctx = kwargs["submission_context"]
        assert isinstance(ctx, SubmissionRenderContext)
        # Mount was resolved from the fake profile's single mount; default
        # work_dir mirrors the selected mount's ``remote`` path.
        assert ctx.mount_name == MOUNT
        assert ctx.default_work_dir == "/home/user/project"
        # ``mounts`` is materialized as a tuple so the context stays
        # immutable/hashable.
        assert isinstance(ctx.mounts, tuple)
        assert len(ctx.mounts) == 1
        assert ctx.mounts[0].name == MOUNT

    def test_run_with_sweep_returns_fast_without_blocking_on_cells(
        self, client: TestClient, mock_adapter: MagicMock
    ) -> None:
        """Regression for C4: Web sweep must spawn the execution loop
        off-request so the 202 comes back immediately (< 2 seconds) even
        when ``arun_from_materialized`` would block.
        """
        import time

        _create_workflow(client, name="fast-sweep")

        blocked = {"flag": False}

        with (
            patch("srunx.web.routers.workflows.SweepOrchestrator") as orch_cls,
            patch("srunx.web.routers.workflows.SweepSpec") as spec_cls,
        ):
            from srunx.sweep import SweepSpec as _RealSweepSpec

            spec_cls.side_effect = _RealSweepSpec

            from srunx.db.connection import open_connection
            from srunx.db.repositories.sweep_runs import SweepRunRepository

            conn = open_connection()
            try:
                seeded_id = SweepRunRepository(conn).create(
                    name="fast-sweep",
                    matrix={"lr": [0.01, 0.1]},
                    args=None,
                    fail_fast=False,
                    max_parallel=2,
                    cell_count=2,
                    submission_source="web",
                    status="pending",
                )
            finally:
                conn.close()

            mock_orch = MagicMock()
            mock_orch.materialize.return_value = seeded_id

            async def slow_arun(_id: int) -> None:
                # Simulate a long-running sweep; if the endpoint awaits
                # this, the test's timing assertion below will fail.
                import anyio

                await anyio.sleep(2.0)
                blocked["flag"] = True

            mock_orch.arun_from_materialized = slow_arun
            orch_cls.return_value = mock_orch

            start = time.monotonic()
            resp = client.post(
                "/api/workflows/fast-sweep/run",
                params={"mount": MOUNT},
                json={
                    "sweep": {
                        "matrix": {"lr": [0.01, 0.1]},
                        "max_parallel": 2,
                    }
                },
            )
            elapsed = time.monotonic() - start

        assert resp.status_code == 202, resp.text
        # Endpoint MUST NOT have awaited the slow arun (sleep=2.0s).
        # Allowing 1s of slack for TestClient overhead on slow CI;
        # real HTTP should be well under 500ms.
        assert elapsed < 1.0, (
            f"response took {elapsed:.2f}s; arun blocked the HTTP handler"
        )
        assert blocked["flag"] is False

    def test_run_with_args_override_non_sweep(
        self, client: TestClient, mock_adapter: MagicMock
    ) -> None:
        _create_workflow(client, name="args-only")
        with patch("srunx.web.routers.workflows.WorkflowRunner") as runner_cls:
            mock_runner = MagicMock()
            runner_cls.from_yaml.return_value = mock_runner
            mock_runner.workflow.jobs = []
            mock_runner.workflow.name = "args-only"

            resp = client.post(
                "/api/workflows/args-only/run",
                params={"mount": MOUNT},
                json={"args_override": {"lr": "0.5"}},
            )

        # Request reached the non-sweep path. Some downstream steps may
        # still fail against the mocked runner (render/submit), but
        # the from_yaml call with args_override is the invariant.
        assert runner_cls.from_yaml.called
        call = runner_cls.from_yaml.call_args
        assert call.kwargs["args_override"] == {"lr": "0.5"}

    def test_python_prefix_in_args_override_rejected(
        self, client: TestClient, mock_adapter: MagicMock
    ) -> None:
        _create_workflow(client, name="reject-args")
        resp = client.post(
            "/api/workflows/reject-args/run",
            params={"mount": MOUNT},
            json={
                "args_override": {
                    "cmd": "python: os.system('x')",
                }
            },
        )
        assert resp.status_code == 422, resp.text
        assert "python:" in resp.text

    def test_python_prefix_in_sweep_matrix_rejected(
        self, client: TestClient, mock_adapter: MagicMock
    ) -> None:
        _create_workflow(client, name="reject-matrix")
        resp = client.post(
            "/api/workflows/reject-matrix/run",
            params={"mount": MOUNT},
            json={
                "sweep": {
                    "matrix": {"cmd": ["python: os.system('x')"]},
                    "max_parallel": 2,
                }
            },
        )
        assert resp.status_code == 422, resp.text


class TestDispatchSweepShellJobGuard:
    """C3: the sweep dispatch path must enforce the ShellJob script-root
    traversal guard before materializing cells or spawning the orchestrator.

    These tests write a sweep-eligible workflow YAML directly to the
    mount's workflow directory (bypassing /api/workflows/create so the
    ``script_path`` isn't validated at creation time) and then submit
    it via /api/workflows/{name}/run.
    """

    def _write_sweep_yaml(
        self,
        tmp_path: Path,
        *,
        name: str,
        script_path: str,
    ) -> None:
        import yaml

        wf_dir = tmp_path / "project" / ".srunx" / "workflows"
        wf_dir.mkdir(parents=True, exist_ok=True)
        body = {
            "name": name,
            "args": {"lr": "0.01"},
            "jobs": [
                {
                    "name": "train",
                    "script_path": script_path,
                }
            ],
        }
        (wf_dir / f"{name}.yaml").write_text(yaml.dump(body))

    def test_rejects_shell_job_traversal_outside_mount(
        self,
        client: TestClient,
        tmp_path: Path,
    ) -> None:
        """``script_path`` resolving outside the mount → 403 before
        materialize."""
        self._write_sweep_yaml(
            tmp_path,
            name="evil-traversal",
            script_path="../../../etc/passwd",
        )

        with patch("srunx.web.routers.workflows.SweepOrchestrator") as orch_cls:
            resp = client.post(
                "/api/workflows/evil-traversal/run",
                params={"mount": MOUNT},
                json={
                    "sweep": {
                        "matrix": {"lr": [0.01, 0.1]},
                        "max_parallel": 2,
                    }
                },
            )

        assert resp.status_code == 403, resp.text
        assert "outside allowed directories" in resp.text
        # Orchestrator must NOT have been constructed — guard runs before
        # materialize.
        orch_cls.assert_not_called()

    def test_rejects_shell_job_absolute_path_outside_mount_root(
        self,
        client: TestClient,
        tmp_path: Path,
    ) -> None:
        """An absolute ``script_path`` outside every mount → 403."""
        self._write_sweep_yaml(
            tmp_path,
            name="abs-outside",
            script_path="/tmp/attacker.sh",
        )

        with patch("srunx.web.routers.workflows.SweepOrchestrator") as orch_cls:
            resp = client.post(
                "/api/workflows/abs-outside/run",
                params={"mount": MOUNT},
                json={
                    "sweep": {
                        "matrix": {"lr": [0.01, 0.1]},
                        "max_parallel": 2,
                    }
                },
            )

        assert resp.status_code == 403, resp.text
        orch_cls.assert_not_called()

    def test_accepts_shell_job_within_mount(
        self,
        client: TestClient,
        tmp_path: Path,
    ) -> None:
        """Legitimate ``script_path`` under the mount passes the guard."""
        # Place a real script inside the mount's local directory.
        mount_local = tmp_path / "project"
        script = mount_local / "train.sh"
        script.write_text("#!/bin/bash\necho hi\n")
        self._write_sweep_yaml(
            tmp_path,
            name="legit-shell",
            script_path=str(script),
        )

        with (
            patch("srunx.web.routers.workflows.SweepOrchestrator") as orch_cls,
            patch("srunx.web.routers.workflows.SweepSpec") as spec_cls,
        ):
            from srunx.sweep import SweepSpec as _RealSweepSpec

            spec_cls.side_effect = _RealSweepSpec

            from srunx.db.connection import open_connection
            from srunx.db.repositories.sweep_runs import SweepRunRepository

            conn = open_connection()
            try:
                seeded_id = SweepRunRepository(conn).create(
                    name="legit-shell",
                    matrix={"lr": [0.01, 0.1]},
                    args=None,
                    fail_fast=False,
                    max_parallel=2,
                    cell_count=2,
                    submission_source="web",
                    status="pending",
                )
            finally:
                conn.close()

            mock_orch = MagicMock()
            mock_orch.materialize.return_value = seeded_id

            async def _fake_arun(_sweep_run_id: int) -> _FakeSweepRun:
                return _FakeSweepRun(sweep_id=seeded_id, cell_count=2)

            mock_orch.arun_from_materialized = _fake_arun
            orch_cls.return_value = mock_orch

            resp = client.post(
                "/api/workflows/legit-shell/run",
                params={"mount": MOUNT},
                json={
                    "sweep": {
                        "matrix": {"lr": [0.01, 0.1]},
                        "max_parallel": 2,
                    }
                },
            )

        # Guard passed → normal sweep dispatch happened.
        assert resp.status_code == 202, resp.text
        orch_cls.assert_called_once()


class TestSweepRunsReadAPI:
    def _seed_sweep(self, *, name: str = "wf") -> int:
        """Create a minimal sweep_runs row directly in the DB for list tests."""
        from srunx.db.connection import open_connection
        from srunx.db.repositories.sweep_runs import SweepRunRepository

        conn = open_connection()
        try:
            sweep_id = SweepRunRepository(conn).create(
                name=name,
                matrix={"lr": [0.1, 0.01]},
                args=None,
                fail_fast=False,
                max_parallel=2,
                cell_count=2,
                submission_source="web",
            )
        finally:
            conn.close()
        return sweep_id

    def _seed_cell(self, sweep_run_id: int, *, args: dict[str, Any]) -> int:
        from srunx.db.connection import open_connection
        from srunx.db.repositories.workflow_runs import WorkflowRunRepository

        conn = open_connection()
        try:
            return WorkflowRunRepository(conn).create(
                workflow_name="wf",
                yaml_path=None,
                args=args,
                triggered_by="web",
                sweep_run_id=sweep_run_id,
            )
        finally:
            conn.close()

    def test_list_sweep_runs_empty(self, client: TestClient) -> None:
        resp = client.get("/api/sweep_runs")
        assert resp.status_code == 200
        assert resp.json() == []

    def test_list_sweep_runs_returns_seeded(self, client: TestClient) -> None:
        sweep_id = self._seed_sweep()
        resp = client.get("/api/sweep_runs")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 1
        assert data[0]["id"] == sweep_id
        assert data[0]["cell_count"] == 2

    def test_get_sweep_run_404(self, client: TestClient) -> None:
        resp = client.get("/api/sweep_runs/999999")
        assert resp.status_code == 404

    def test_get_sweep_run_ok(self, client: TestClient) -> None:
        sweep_id = self._seed_sweep()
        resp = client.get(f"/api/sweep_runs/{sweep_id}")
        assert resp.status_code == 200
        body = resp.json()
        assert body["id"] == sweep_id
        assert body["submission_source"] == "web"

    def test_list_cells_returns_rows(self, client: TestClient) -> None:
        sweep_id = self._seed_sweep()
        self._seed_cell(sweep_id, args={"lr": 0.1})
        self._seed_cell(sweep_id, args={"lr": 0.01})

        resp = client.get(f"/api/sweep_runs/{sweep_id}/cells")
        assert resp.status_code == 200
        cells = resp.json()
        assert len(cells) == 2
        assert {c["args"]["lr"] for c in cells} == {0.1, 0.01}

    def test_cancel_marks_cancel_requested_at(self, client: TestClient) -> None:
        sweep_id = self._seed_sweep()

        resp = client.post(f"/api/sweep_runs/{sweep_id}/cancel")
        assert resp.status_code == 202, resp.text
        body = resp.json()
        assert body["id"] == sweep_id
        assert body["cancel_requested_at"] is not None

    def test_cancel_db_only_drains_pending_cells(self, client: TestClient) -> None:
        """Regression for I1: when no in-process orchestrator is alive,
        the cancel endpoint must drain pending cells via SQL directly.

        The drain itself flips the sweep to ``draining`` and invokes
        the aggregator in the same TX; because every cell is already
        terminal after the drain, the aggregator immediately advances
        the sweep to ``cancelled``.
        """
        sweep_id = self._seed_sweep()
        self._seed_cell(sweep_id, args={"lr": 0.1})
        self._seed_cell(sweep_id, args={"lr": 0.01})

        resp = client.post(f"/api/sweep_runs/{sweep_id}/cancel")
        assert resp.status_code == 202, resp.text

        from srunx.db.connection import open_connection

        conn = open_connection()
        try:
            sweep_row = conn.execute(
                "SELECT status, cells_pending, cells_cancelled, "
                "       cancel_requested_at "
                "FROM sweep_runs WHERE id = ?",
                (sweep_id,),
            ).fetchone()
            cell_statuses = [
                r["status"]
                for r in conn.execute(
                    "SELECT status FROM workflow_runs WHERE sweep_run_id = ?",
                    (sweep_id,),
                ).fetchall()
            ]
        finally:
            conn.close()

        # Without the DB-only drain the cells would remain ``pending``
        # and the sweep would stay ``pending`` indefinitely.
        assert sweep_row["status"] == "cancelled"
        assert sweep_row["cells_pending"] == 0
        assert sweep_row["cells_cancelled"] == 2
        assert sweep_row["cancel_requested_at"] is not None
        assert sorted(cell_statuses) == ["cancelled", "cancelled"]

    def test_cancel_404(self, client: TestClient) -> None:
        resp = client.post("/api/sweep_runs/424242/cancel")
        assert resp.status_code == 404
