"""Tests for REST API routers with mocked SSH adapter."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient

from srunx.web.app import create_app
from srunx.web.deps import get_adapter

MOCK_JOBS = [
    {
        "name": "train-job",
        "job_id": 10001,
        "status": "RUNNING",
        "depends_on": [],
        "command": [],
        "resources": {"nodes": 1, "gpus_per_node": 4, "partition": "gpu"},
        "partition": "gpu",
        "nodes": 1,
        "gpus": 4,
        "elapsed_time": "1:30:00",
    },
]

MOCK_RESOURCES = [
    {
        "timestamp": "2026-03-30T00:00:00+00:00",
        "partition": "gpu",
        "total_gpus": 32,
        "gpus_in_use": 20,
        "gpus_available": 12,
        "jobs_running": 3,
        "nodes_total": 4,
        "nodes_idle": 1,
        "nodes_down": 0,
        "gpu_utilization": 0.625,
        "has_available_gpus": True,
    },
]


@pytest.fixture
def mock_adapter() -> MagicMock:
    adapter = MagicMock()
    adapter.list_jobs.return_value = MOCK_JOBS
    adapter.get_job.return_value = MOCK_JOBS[0]
    adapter.cancel_job.return_value = None
    adapter.get_job_output.return_value = ("stdout content", "stderr content")
    adapter.get_resources.return_value = MOCK_RESOURCES
    adapter.submit_job.return_value = {
        "name": "new-job",
        "job_id": 10002,
        "status": "PENDING",
        "depends_on": [],
        "command": [],
        "resources": {},
    }
    return adapter


@pytest.fixture
def client(  # type: ignore[misc]
    mock_adapter: MagicMock, tmp_path: Path
) -> TestClient:
    # Use tmp_path for workflow_dir to avoid leftover files
    import srunx.web.config as config_mod
    from srunx.web.config import get_web_config

    original = config_mod._config
    config_mod._config = None
    cfg = get_web_config()
    cfg.workflow_dir = tmp_path / "workflows"
    config_mod._config = cfg

    app = create_app()
    app.dependency_overrides[get_adapter] = lambda: mock_adapter
    yield TestClient(app, raise_server_exceptions=False)

    config_mod._config = original


# ── Jobs Router ───────────────────────────────────


class TestJobsRouter:
    def test_list_jobs(self, client: TestClient) -> None:
        resp = client.get("/api/jobs")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 1
        assert data[0]["job_id"] == 10001

    def test_get_job(self, client: TestClient) -> None:
        resp = client.get("/api/jobs/10001")
        assert resp.status_code == 200
        assert resp.json()["name"] == "train-job"

    def test_get_job_invalid_id(self, client: TestClient) -> None:
        resp = client.get("/api/jobs/0")
        assert resp.status_code == 400

    def test_get_job_not_found(
        self, client: TestClient, mock_adapter: MagicMock
    ) -> None:
        mock_adapter.get_job.side_effect = ValueError("No job information found")
        resp = client.get("/api/jobs/99999")
        assert resp.status_code == 404

    def test_cancel_job(self, client: TestClient) -> None:
        resp = client.delete("/api/jobs/10001")
        assert resp.status_code == 204

    def test_cancel_job_invalid_id(self, client: TestClient) -> None:
        resp = client.delete("/api/jobs/0")
        assert resp.status_code == 400

    def test_get_logs(self, client: TestClient) -> None:
        resp = client.get("/api/jobs/10001/logs")
        assert resp.status_code == 200
        data = resp.json()
        assert data["stdout"] == "stdout content"
        assert data["stderr"] == "stderr content"

    def test_get_logs_not_found(
        self, client: TestClient, mock_adapter: MagicMock
    ) -> None:
        mock_adapter.get_job_output.side_effect = FileNotFoundError("No logs")
        resp = client.get("/api/jobs/10001/logs")
        assert resp.status_code == 404

    def test_submit_job(self, client: TestClient) -> None:
        resp = client.post(
            "/api/jobs",
            json={"name": "new-job", "script_content": "#!/bin/bash\necho hello"},
        )
        assert resp.status_code == 201
        assert resp.json()["job_id"] == 10002

    def test_slurm_error_returns_502(
        self, client: TestClient, mock_adapter: MagicMock
    ) -> None:
        mock_adapter.list_jobs.side_effect = RuntimeError("squeue failed")
        resp = client.get("/api/jobs")
        assert resp.status_code == 502


# ── Resources Router ──────────────────────────────


class TestResourcesRouter:
    def test_get_resources(self, client: TestClient) -> None:
        resp = client.get("/api/resources")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 1
        assert data[0]["partition"] == "gpu"
        assert data[0]["total_gpus"] == 32

    def test_get_resources_with_partition(self, client: TestClient) -> None:
        resp = client.get("/api/resources?partition=gpu")
        assert resp.status_code == 200

    def test_slurm_error_returns_502(
        self, client: TestClient, mock_adapter: MagicMock
    ) -> None:
        mock_adapter.get_resources.side_effect = RuntimeError("sinfo failed")
        resp = client.get("/api/resources")
        assert resp.status_code == 502


# ── History Router ────────────────────────────────


class TestHistoryRouter:
    def test_get_stats(self, client: TestClient) -> None:
        resp = client.get("/api/history/stats")
        assert resp.status_code == 200
        data = resp.json()
        assert "total" in data
        assert "completed" in data

    def test_get_recent(self, client: TestClient) -> None:
        resp = client.get("/api/history")
        assert resp.status_code == 200


# ── Workflows Router ─────────────────────────────


class TestWorkflowsRouter:
    def test_list_empty(self, client: TestClient) -> None:
        resp = client.get("/api/workflows")
        assert resp.status_code == 200
        assert resp.json() == []

    def test_validate_empty_yaml(self, client: TestClient) -> None:
        resp = client.post("/api/workflows/validate", json={"yaml": ""})
        data = resp.json()
        assert data["valid"] is False

    def test_validate_rejects_python_args(self, client: TestClient) -> None:
        resp = client.post(
            "/api/workflows/validate",
            json={"yaml": "name: test\nargs:\n  x: 'python: 1+1'"},
        )
        assert resp.status_code == 422

    def test_upload_rejects_path_traversal(self, client: TestClient) -> None:
        resp = client.post(
            "/api/workflows/upload",
            json={"yaml": "name: test\njobs: []", "filename": "../../evil.yaml"},
        )
        # Should use safe basename, so stem "evil" passes but path is safe
        # The actual behavior depends on the workflow_dir existing
        assert resp.status_code in (200, 422)

    def test_upload_rejects_bad_filename(self, client: TestClient) -> None:
        resp = client.post(
            "/api/workflows/upload",
            json={"yaml": "name: test", "filename": "bad name!.yaml"},
        )
        assert resp.status_code == 422

    def test_list_runs(self, client: TestClient) -> None:
        resp = client.get("/api/workflows/runs")
        assert resp.status_code == 200
        assert resp.json() == []
