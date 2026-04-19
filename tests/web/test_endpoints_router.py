"""Integration tests for the new notification CRUD routers.

Uses the FastAPI ``TestClient`` against an app whose DB is pointed at a
file-backed tmp SQLite (via the ``tmp_srunx_db`` fixture + ``XDG_CONFIG_HOME``).
"""

from __future__ import annotations

from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from srunx.db.connection import init_db


@pytest.fixture
def app_client(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> TestClient:
    """A fresh app instance with an isolated tmp DB."""
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path))
    # Pre-initialize the DB so the first API call doesn't race on schema creation.
    init_db(delete_legacy=False)

    from srunx.web.app import create_app

    app = create_app()
    return TestClient(app)


# --- endpoints CRUD ---


def test_list_endpoints_empty(app_client: TestClient) -> None:
    r = app_client.get("/api/endpoints")
    assert r.status_code == 200
    assert r.json() == []


def test_create_endpoint_happy_path(app_client: TestClient) -> None:
    r = app_client.post(
        "/api/endpoints",
        json={
            "kind": "slack_webhook",
            "name": "default",
            "config": {"webhook_url": "https://hooks.slack.com/services/A/B/C"},
        },
    )
    assert r.status_code == 201, r.text
    body = r.json()
    assert body["kind"] == "slack_webhook"
    assert body["name"] == "default"
    assert body["disabled_at"] is None
    assert isinstance(body["id"], int)


def test_create_endpoint_invalid_kind(app_client: TestClient) -> None:
    r = app_client.post(
        "/api/endpoints",
        json={"kind": "email", "name": "x", "config": {}},
    )
    assert r.status_code == 422


def test_create_endpoint_invalid_webhook_url(app_client: TestClient) -> None:
    r = app_client.post(
        "/api/endpoints",
        json={
            "kind": "slack_webhook",
            "name": "x",
            "config": {"webhook_url": "https://example.com/hook"},
        },
    )
    assert r.status_code == 422


def test_disable_then_enable_endpoint(app_client: TestClient) -> None:
    created = app_client.post(
        "/api/endpoints",
        json={
            "kind": "slack_webhook",
            "name": "d",
            "config": {"webhook_url": "https://hooks.slack.com/services/A/B/C"},
        },
    ).json()
    eid = created["id"]

    r = app_client.post(f"/api/endpoints/{eid}/disable")
    assert r.status_code == 200
    assert r.json()["disabled_at"] is not None

    r = app_client.post(f"/api/endpoints/{eid}/enable")
    assert r.status_code == 200
    assert r.json()["disabled_at"] is None


def test_delete_endpoint(app_client: TestClient) -> None:
    created = app_client.post(
        "/api/endpoints",
        json={
            "kind": "slack_webhook",
            "name": "d2",
            "config": {"webhook_url": "https://hooks.slack.com/services/A/B/C"},
        },
    ).json()
    r = app_client.delete(f"/api/endpoints/{created['id']}")
    assert r.status_code == 204
    r = app_client.get("/api/endpoints")
    assert r.json() == []


# --- subscriptions CRUD (requires endpoint + watch) ---


def test_subscription_requires_filter(app_client: TestClient) -> None:
    r = app_client.get("/api/subscriptions")
    assert r.status_code == 400


# --- deliveries observability ---


def test_deliveries_stuck_count(app_client: TestClient) -> None:
    r = app_client.get("/api/deliveries/stuck")
    assert r.status_code == 200
    body = r.json()
    assert body["count"] == 0
    assert "older_than_sec" in body


def test_deliveries_list_recent_empty(app_client: TestClient) -> None:
    """Without ``subscription_id`` the endpoint returns recent deliveries globally."""
    r = app_client.get("/api/deliveries")
    assert r.status_code == 200
    assert r.json() == []


def test_deliveries_list_recent_limit_validated(app_client: TestClient) -> None:
    """``limit`` must stay within [1, 500]."""
    r = app_client.get("/api/deliveries?limit=0")
    assert r.status_code == 422
    r = app_client.get("/api/deliveries?limit=501")
    assert r.status_code == 422


# --- watches observability ---


def test_watches_list_open(app_client: TestClient) -> None:
    r = app_client.get("/api/watches?open=true")
    assert r.status_code == 200
    assert r.json() == []
