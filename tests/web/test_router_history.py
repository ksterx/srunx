"""Tests for history router: /api/history/*

Post-cutover (P2-4 #A) the router reads from
:class:`~srunx.db.repositories.jobs.JobRepository` via a per-request
SQLite connection, not the legacy ``JobHistory``. These tests seed
the new DB directly so no dependency override is needed.
"""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient

from srunx.db.connection import init_db, open_connection
from srunx.db.repositories.jobs import JobRepository
from srunx.web.app import create_app
from srunx.web.deps import get_adapter


@pytest.fixture
def mock_adapter() -> MagicMock:
    return MagicMock()


@pytest.fixture
def client_and_db(
    mock_adapter: MagicMock,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> Iterator[tuple[TestClient, Path]]:
    """Fresh app instance + tmp-isolated state DB."""
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path))
    db_path = init_db(delete_legacy=False)

    import srunx.web.config as config_mod

    original = config_mod._config
    config_mod._config = None
    config_mod.get_web_config()

    app = create_app()
    app.dependency_overrides[get_adapter] = lambda: mock_adapter

    client = TestClient(app, raise_server_exceptions=False)
    try:
        yield client, db_path
    finally:
        app.dependency_overrides.clear()
        config_mod._config = original


def _seed(db_path: Path) -> None:
    """Populate ``jobs`` + ``workflow_runs`` with a realistic mix."""
    conn = open_connection(db_path)
    try:
        # A workflow_run so the LEFT JOIN on workflow_name has a match.
        conn.execute(
            "INSERT INTO workflow_runs (id, workflow_name, status, started_at, triggered_by) "
            "VALUES (1, 'ml_pipeline', 'running', '2026-01-01T00:00:00Z', 'web')"
        )
        conn.commit()

        repo = JobRepository(conn)
        repo.record_submission(
            job_id=100,
            name="train",
            status="COMPLETED",
            submission_source="cli",
            command=["python", "train.py"],
            nodes=1,
            gpus_per_node=4,
            partition="gpu",
            submitted_at="2026-01-01T00:00:00Z",
        )
        repo.update_status(
            100,
            "COMPLETED",
            completed_at="2026-01-01T01:00:00Z",
            duration_secs=3600,
        )
        repo.record_submission(
            job_id=101,
            name="eval",
            status="FAILED",
            submission_source="workflow",
            workflow_run_id=1,
            nodes=2,
            gpus_per_node=4,
            partition="gpu",
            submitted_at="2026-01-02T00:00:00Z",
        )
        repo.record_submission(
            job_id=102,
            name="cancelled-job",
            status="CANCELLED",
            submission_source="cli",
            submitted_at="2026-01-03T00:00:00Z",
        )
    finally:
        conn.close()


class TestHistoryStats:
    def test_get_stats_aggregates_from_jobs_table(
        self, client_and_db: tuple[TestClient, Path]
    ) -> None:
        client, db_path = client_and_db
        _seed(db_path)

        resp = client.get("/api/history/stats")
        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] == 3
        assert data["completed"] == 1
        assert data["failed"] == 1
        assert data["cancelled"] == 1
        # avg_duration over only rows with duration_secs set (just job 100)
        assert data["avg_runtime_seconds"] == 3600.0

    def test_get_stats_empty_db(self, client_and_db: tuple[TestClient, Path]) -> None:
        client, _ = client_and_db
        resp = client.get("/api/history/stats")
        assert resp.status_code == 200
        assert resp.json()["total"] == 0

    def test_get_stats_accepts_date_range_aliases(
        self, client_and_db: tuple[TestClient, Path]
    ) -> None:
        """``?from=`` / ``?to=`` are recognized (Python-keyword aliasing)."""
        client, db_path = client_and_db
        _seed(db_path)

        resp = client.get(
            "/api/history/stats",
            params={"from": "2026-01-02", "to": "2026-01-02"},
        )
        assert resp.status_code == 200
        # Only job 101 (submitted 2026-01-02) matches [from, to+23:59:59).
        # Jobs 100 (Jan 1) and 102 (Jan 3) fall outside.
        assert resp.json()["total"] == 1

    def test_get_stats_error_returns_502(
        self, client_and_db: tuple[TestClient, Path]
    ) -> None:
        """DB-level failure surfaces as 502 with the upstream detail."""
        client, db_path = client_and_db

        import srunx.web.routers.history as history_router

        def boom(*args: object, **kwargs: object) -> None:
            raise RuntimeError("DB connection lost")

        original = history_router.JobRepository.compute_stats
        history_router.JobRepository.compute_stats = boom  # type: ignore[assignment,method-assign]
        try:
            resp = client.get("/api/history/stats")
        finally:
            history_router.JobRepository.compute_stats = original  # type: ignore[method-assign]
        assert resp.status_code == 502
        assert "DB connection lost" in resp.json()["detail"]


class TestHistoryRecent:
    def test_get_recent_returns_newest_first(
        self, client_and_db: tuple[TestClient, Path]
    ) -> None:
        client, db_path = client_and_db
        _seed(db_path)

        resp = client.get("/api/history")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 3
        assert data[0]["job_id"] == 102  # newest first
        # name → job_name alias round-trips
        assert data[0]["job_name"] == "cancelled-job"
        # workflow_name comes from the LEFT JOIN
        workflow_entry = next(e for e in data if e["job_id"] == 101)
        assert workflow_entry["workflow_name"] == "ml_pipeline"

    def test_get_recent_respects_limit(
        self, client_and_db: tuple[TestClient, Path]
    ) -> None:
        client, db_path = client_and_db
        _seed(db_path)

        resp = client.get("/api/history", params={"limit": 1})
        assert resp.status_code == 200
        assert len(resp.json()) == 1

    def test_get_recent_empty_db(self, client_and_db: tuple[TestClient, Path]) -> None:
        client, _ = client_and_db
        resp = client.get("/api/history")
        assert resp.status_code == 200
        assert resp.json() == []

    def test_get_recent_error_returns_502(
        self, client_and_db: tuple[TestClient, Path]
    ) -> None:
        client, _ = client_and_db

        import srunx.web.routers.history as history_router

        def boom(*args: object, **kwargs: object) -> None:
            raise RuntimeError("disk full")

        original = history_router.JobRepository.list_recent_as_dict
        history_router.JobRepository.list_recent_as_dict = boom  # type: ignore[assignment,method-assign]
        try:
            resp = client.get("/api/history")
        finally:
            history_router.JobRepository.list_recent_as_dict = original  # type: ignore[method-assign]
        assert resp.status_code == 502
        assert "disk full" in resp.json()["detail"]
