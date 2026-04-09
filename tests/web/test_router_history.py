"""Tests for history router: /api/history/*"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient

from srunx.web.app import create_app
from srunx.web.deps import get_adapter, get_history_db


@pytest.fixture
def mock_adapter() -> MagicMock:
    return MagicMock()


@pytest.fixture
def mock_history() -> MagicMock:
    history = MagicMock()
    history.get_job_stats.return_value = {
        "total_jobs": 42,
        "jobs_by_status": {"COMPLETED": 30, "FAILED": 10, "CANCELLED": 2},
        "avg_duration_seconds": 3600.5,
        "total_gpu_hours": 120.0,
        "from_date": None,
        "to_date": None,
    }
    history.get_recent_jobs.return_value = [
        {
            "job_id": 100,
            "job_name": "train",
            "command": "python train.py",
            "status": "COMPLETED",
            "submitted_at": "2026-01-01T00:00:00",
            "completed_at": "2026-01-01T01:00:00",
            "workflow_name": None,
            "partition": "gpu",
            "nodes": 1,
            "gpus": 4,
        },
        {
            "job_id": 101,
            "job_name": "eval",
            "command": "python eval.py",
            "status": "FAILED",
            "submitted_at": "2026-01-02T00:00:00",
            "completed_at": None,
            "workflow_name": "pipeline",
            "partition": "gpu",
            "nodes": 2,
            "gpus": 8,
        },
    ]
    return history


@pytest.fixture
def client(mock_adapter: MagicMock, mock_history: MagicMock):  # type: ignore[misc]
    import srunx.web.config as config_mod

    original = config_mod._config
    config_mod._config = None
    config_mod.get_web_config()

    app = create_app()
    app.dependency_overrides[get_adapter] = lambda: mock_adapter
    app.dependency_overrides[get_history_db] = lambda: mock_history

    yield TestClient(app, raise_server_exceptions=False)

    app.dependency_overrides.clear()
    config_mod._config = original


class TestHistoryStats:
    def test_get_stats(self, client: TestClient, mock_history: MagicMock) -> None:
        resp = client.get("/api/history/stats")
        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] == 42
        assert data["completed"] == 30
        assert data["failed"] == 10
        assert data["cancelled"] == 2
        assert data["avg_runtime_seconds"] == 3600.5
        mock_history.get_job_stats.assert_called_once_with(None, None)

    def test_get_stats_with_date_range(
        self, client: TestClient, mock_history: MagicMock
    ) -> None:
        resp = client.get(
            "/api/history/stats", params={"from": "2026-01-01", "to": "2026-01-31"}
        )
        assert resp.status_code == 200
        mock_history.get_job_stats.assert_called_once_with("2026-01-01", "2026-01-31")

    def test_get_stats_error_returns_502(
        self, client: TestClient, mock_history: MagicMock
    ) -> None:
        mock_history.get_job_stats.side_effect = RuntimeError("DB connection lost")
        resp = client.get("/api/history/stats")
        assert resp.status_code == 502
        assert "DB connection lost" in resp.json()["detail"]


class TestHistoryRecent:
    def test_get_recent(self, client: TestClient, mock_history: MagicMock) -> None:
        resp = client.get("/api/history")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 2
        assert data[0]["job_id"] == 100
        assert data[0]["job_name"] == "train"
        assert data[0]["status"] == "COMPLETED"
        assert data[1]["job_id"] == 101
        assert data[1]["completed_at"] is None
        mock_history.get_recent_jobs.assert_called_once_with(50)

    def test_get_recent_with_limit(
        self, client: TestClient, mock_history: MagicMock
    ) -> None:
        resp = client.get("/api/history", params={"limit": 10})
        assert resp.status_code == 200
        mock_history.get_recent_jobs.assert_called_once_with(10)

    def test_get_recent_error_returns_502(
        self, client: TestClient, mock_history: MagicMock
    ) -> None:
        mock_history.get_recent_jobs.side_effect = RuntimeError("disk full")
        resp = client.get("/api/history")
        assert resp.status_code == 502
        assert "disk full" in resp.json()["detail"]
