"""Integration tests for the watches router.

Covers the POST endpoints added for Jobs-page notification toggling:

- ``POST /api/watches`` (kind=job) — idempotent attach, error mapping
  for missing / disabled endpoint, preset validation.
- ``POST /api/watches/{id}/close`` — close on known id, 404 on unknown.

Read-only GET paths already exercised elsewhere; we only add coverage
for the new mutations here.
"""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient

from srunx.observability.storage.connection import init_db, open_connection
from srunx.web.app import create_app
from srunx.web.deps import get_adapter


@pytest.fixture
def mock_adapter() -> MagicMock:
    adapter = MagicMock()
    # Match the scheduler_key shape the real adapter exposes so the
    # target_ref lands in the ``job:local:<id>`` form.
    adapter.scheduler_key = "local"
    return adapter


@pytest.fixture
def client_and_db(
    mock_adapter: MagicMock,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> Iterator[tuple[TestClient, Path]]:
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path))
    db_path = init_db(delete_legacy=False)

    app = create_app()
    app.dependency_overrides[get_adapter] = lambda: mock_adapter
    client = TestClient(app, raise_server_exceptions=False)
    try:
        yield client, db_path
    finally:
        app.dependency_overrides.clear()


def _seed_endpoint(
    db_path: Path, *, name: str = "primary", disabled: bool = False
) -> int:
    from srunx.observability.storage.repositories.endpoints import EndpointRepository

    conn = open_connection(db_path)
    try:
        endpoint_id = EndpointRepository(conn).create(
            kind="slack_webhook",
            name=name,
            config={"webhook_url": "https://hooks.slack.com/services/A/B/C"},
        )
        if disabled:
            EndpointRepository(conn).disable(endpoint_id)
        return endpoint_id
    finally:
        conn.close()


def _seed_job(db_path: Path, *, job_id: int = 123) -> None:
    from srunx.observability.storage.repositories.jobs import JobRepository

    conn = open_connection(db_path)
    try:
        JobRepository(conn).record_submission(
            job_id=job_id,
            name=f"job_{job_id}",
            status="PENDING",
            submission_source="web",
        )
    finally:
        conn.close()


# --- POST /api/watches ------------------------------------------------------


def test_post_creates_watch_and_subscription(
    client_and_db: tuple[TestClient, Path],
) -> None:
    client, db = client_and_db
    endpoint_id = _seed_endpoint(db)
    _seed_job(db)

    r = client.post(
        "/api/watches",
        json={
            "kind": "job",
            "job_id": 123,
            "endpoint_id": endpoint_id,
            "preset": "terminal",
        },
    )
    assert r.status_code == 201, r.text
    body = r.json()
    assert body["created"] is True
    assert isinstance(body["watch_id"], int)
    assert isinstance(body["subscription_id"], int)

    # Verify the watch is actually in the DB and keyed by scheduler_key.
    from srunx.observability.storage.repositories.watches import WatchRepository

    conn = open_connection(db)
    try:
        watch = WatchRepository(conn).get(body["watch_id"])
        assert watch is not None
        assert watch.kind == "job"
        assert watch.target_ref == "job:local:123"
    finally:
        conn.close()


def test_post_is_idempotent(client_and_db: tuple[TestClient, Path]) -> None:
    client, db = client_and_db
    endpoint_id = _seed_endpoint(db)
    _seed_job(db)

    first = client.post(
        "/api/watches",
        json={
            "kind": "job",
            "job_id": 123,
            "endpoint_id": endpoint_id,
            "preset": "terminal",
        },
    ).json()
    second = client.post(
        "/api/watches",
        json={
            "kind": "job",
            "job_id": 123,
            "endpoint_id": endpoint_id,
            "preset": "terminal",
        },
    ).json()

    assert first["created"] is True
    assert second["created"] is False
    assert second["watch_id"] == first["watch_id"]
    assert second["subscription_id"] == first["subscription_id"]


def test_post_rejects_digest_preset(
    client_and_db: tuple[TestClient, Path],
) -> None:
    client, db = client_and_db
    endpoint_id = _seed_endpoint(db)
    _seed_job(db)

    r = client.post(
        "/api/watches",
        json={
            "kind": "job",
            "job_id": 123,
            "endpoint_id": endpoint_id,
            "preset": "digest",
        },
    )
    assert r.status_code == 422
    detail = r.json()["detail"].lower()
    assert "digest" in detail
    assert "not implemented" in detail


def test_post_rejects_bogus_preset_with_invalid_message(
    client_and_db: tuple[TestClient, Path],
) -> None:
    """Nonsense presets get a distinct "Invalid" message, not "not implemented"."""
    client, db = client_and_db
    endpoint_id = _seed_endpoint(db)
    _seed_job(db)

    r = client.post(
        "/api/watches",
        json={
            "kind": "job",
            "job_id": 123,
            "endpoint_id": endpoint_id,
            "preset": "bogus",
        },
    )
    assert r.status_code == 422
    detail = r.json()["detail"].lower()
    assert "invalid preset" in detail


def test_post_rejects_invalid_job_id(
    client_and_db: tuple[TestClient, Path],
) -> None:
    client, db = client_and_db
    endpoint_id = _seed_endpoint(db)

    r = client.post(
        "/api/watches",
        json={
            "kind": "job",
            "job_id": 0,
            "endpoint_id": endpoint_id,
            "preset": "terminal",
        },
    )
    # Pydantic validation: gt=0
    assert r.status_code == 422


def test_post_unknown_endpoint_returns_404(
    client_and_db: tuple[TestClient, Path],
) -> None:
    client, _ = client_and_db

    r = client.post(
        "/api/watches",
        json={
            "kind": "job",
            "job_id": 123,
            "endpoint_id": 99999,
            "preset": "terminal",
        },
    )
    assert r.status_code == 404
    assert "not found" in r.json()["detail"].lower()


def test_post_disabled_endpoint_returns_422(
    client_and_db: tuple[TestClient, Path],
) -> None:
    client, db = client_and_db
    endpoint_id = _seed_endpoint(db, name="off", disabled=True)

    r = client.post(
        "/api/watches",
        json={
            "kind": "job",
            "job_id": 123,
            "endpoint_id": endpoint_id,
            "preset": "terminal",
        },
    )
    assert r.status_code == 422
    assert "disabled" in r.json()["detail"].lower()


def test_post_uses_adapter_scheduler_key(
    client_and_db: tuple[TestClient, Path],
    mock_adapter: MagicMock,
) -> None:
    """target_ref must embed adapter.scheduler_key (not a client-supplied string)."""
    client, db = client_and_db
    mock_adapter.scheduler_key = "ssh:staging"

    from srunx.observability.storage.repositories.endpoints import EndpointRepository
    from srunx.observability.storage.repositories.jobs import JobRepository
    from srunx.observability.storage.repositories.watches import WatchRepository

    conn = open_connection(db)
    try:
        endpoint_id = EndpointRepository(conn).create(
            kind="slack_webhook",
            name="ssh-endpoint",
            config={"webhook_url": "https://hooks.slack.com/services/X/Y/Z"},
        )
        JobRepository(conn).record_submission(
            job_id=77,
            name="job_ssh",
            status="PENDING",
            submission_source="web",
            transport_type="ssh",
            profile_name="staging",
            scheduler_key="ssh:staging",
        )
    finally:
        conn.close()

    r = client.post(
        "/api/watches",
        json={
            "kind": "job",
            "job_id": 77,
            "endpoint_id": endpoint_id,
            "preset": "terminal",
        },
    )
    assert r.status_code == 201, r.text
    watch_id = r.json()["watch_id"]

    conn = open_connection(db)
    try:
        watch = WatchRepository(conn).get(watch_id)
        assert watch is not None
        assert watch.target_ref == "job:ssh:staging:77"
    finally:
        conn.close()


# --- POST /api/watches/{id}/close ------------------------------------------


def test_close_closes_an_open_watch(
    client_and_db: tuple[TestClient, Path],
) -> None:
    client, db = client_and_db
    endpoint_id = _seed_endpoint(db)
    _seed_job(db)

    create = client.post(
        "/api/watches",
        json={
            "kind": "job",
            "job_id": 123,
            "endpoint_id": endpoint_id,
            "preset": "terminal",
        },
    ).json()
    watch_id = create["watch_id"]

    r = client.post(f"/api/watches/{watch_id}/close")
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["id"] == watch_id
    assert body["closed_at"] is not None


def test_close_unknown_watch_returns_404(
    client_and_db: tuple[TestClient, Path],
) -> None:
    client, _ = client_and_db

    r = client.post("/api/watches/9999/close")
    assert r.status_code == 404
