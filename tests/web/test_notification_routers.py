"""Coverage for subscription / watch / delivery CRUD routers."""

from __future__ import annotations

from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from srunx.db.connection import init_db, open_connection
from srunx.db.repositories.endpoints import EndpointRepository
from srunx.db.repositories.watches import WatchRepository


@pytest.fixture
def app_client_and_db(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> tuple[TestClient, Path]:
    """Fresh app instance + DB path."""
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path))
    db_path = init_db(delete_legacy=False)

    from srunx.web.app import create_app

    return TestClient(create_app()), db_path


def _seed_endpoint_and_watch(db_path: Path) -> tuple[int, int]:
    conn = open_connection(db_path)
    try:
        endpoint_id = EndpointRepository(conn).create(
            kind="slack_webhook",
            name="test",
            config={"webhook_url": "https://hooks.slack.com/services/A/B/C"},
        )
        watch_id = WatchRepository(conn).create(kind="job", target_ref="job:1001")
    finally:
        conn.close()
    return endpoint_id, watch_id


# --- subscriptions router ---


def test_subscriptions_crud_full_flow(
    app_client_and_db: tuple[TestClient, Path],
) -> None:
    client, db_path = app_client_and_db
    endpoint_id, watch_id = _seed_endpoint_and_watch(db_path)

    # POST
    r = client.post(
        "/api/subscriptions",
        json={"watch_id": watch_id, "endpoint_id": endpoint_id, "preset": "terminal"},
    )
    assert r.status_code == 201, r.text
    sub_id = r.json()["id"]

    # List by watch_id
    r = client.get(f"/api/subscriptions?watch_id={watch_id}")
    assert r.status_code == 200
    assert len(r.json()) == 1

    # List by endpoint_id
    r = client.get(f"/api/subscriptions?endpoint_id={endpoint_id}")
    assert r.status_code == 200
    assert len(r.json()) == 1

    # Duplicate insert returns 409
    r = client.post(
        "/api/subscriptions",
        json={"watch_id": watch_id, "endpoint_id": endpoint_id, "preset": "all"},
    )
    assert r.status_code == 409

    # Delete
    r = client.delete(f"/api/subscriptions/{sub_id}")
    assert r.status_code == 204

    # List empty
    r = client.get(f"/api/subscriptions?watch_id={watch_id}")
    assert r.json() == []

    # Delete missing → 404
    r = client.delete(f"/api/subscriptions/{sub_id}")
    assert r.status_code == 404


def test_subscriptions_rejects_invalid_preset(
    app_client_and_db: tuple[TestClient, Path],
) -> None:
    client, db_path = app_client_and_db
    endpoint_id, watch_id = _seed_endpoint_and_watch(db_path)

    r = client.post(
        "/api/subscriptions",
        json={"watch_id": watch_id, "endpoint_id": endpoint_id, "preset": "bogus"},
    )
    assert r.status_code == 422


def test_subscriptions_rejects_digest_preset(
    app_client_and_db: tuple[TestClient, Path],
) -> None:
    """P1-3: ``digest`` is schema-valid but not yet implemented.

    Accepting a new subscription under ``preset='digest'`` would
    silently deliver zero notifications because
    ``should_deliver('digest', ...)`` returns False. Reject the create
    with a 422 that explains the accepted presets.
    """
    client, db_path = app_client_and_db
    endpoint_id, watch_id = _seed_endpoint_and_watch(db_path)

    r = client.post(
        "/api/subscriptions",
        json={
            "watch_id": watch_id,
            "endpoint_id": endpoint_id,
            "preset": "digest",
        },
    )
    assert r.status_code == 422
    assert "not implemented" in r.json()["detail"]


# --- watches router ---


def test_watches_list_by_target(
    app_client_and_db: tuple[TestClient, Path],
) -> None:
    client, db_path = app_client_and_db
    _, watch_id = _seed_endpoint_and_watch(db_path)

    r = client.get("/api/watches?kind=job&target_ref=job:1001")
    assert r.status_code == 200
    rows = r.json()
    assert len(rows) == 1
    assert rows[0]["id"] == watch_id


def test_watches_unfiltered_closed_listing_rejected(
    app_client_and_db: tuple[TestClient, Path],
) -> None:
    client, _ = app_client_and_db
    r = client.get("/api/watches?open=false")
    assert r.status_code == 400


def test_watches_get_by_id(
    app_client_and_db: tuple[TestClient, Path],
) -> None:
    client, db_path = app_client_and_db
    _, watch_id = _seed_endpoint_and_watch(db_path)

    r = client.get(f"/api/watches/{watch_id}")
    assert r.status_code == 200
    assert r.json()["target_ref"] == "job:1001"

    r = client.get("/api/watches/999999")
    assert r.status_code == 404


# --- deliveries router ---


def test_deliveries_list_by_subscription_empty(
    app_client_and_db: tuple[TestClient, Path],
) -> None:
    client, db_path = app_client_and_db
    endpoint_id, watch_id = _seed_endpoint_and_watch(db_path)
    sub = client.post(
        "/api/subscriptions",
        json={"watch_id": watch_id, "endpoint_id": endpoint_id, "preset": "terminal"},
    ).json()
    r = client.get(f"/api/deliveries?subscription_id={sub['id']}")
    assert r.status_code == 200
    assert r.json() == []


def test_deliveries_get_missing_returns_404(
    app_client_and_db: tuple[TestClient, Path],
) -> None:
    client, _ = app_client_and_db
    r = client.get("/api/deliveries/999999")
    assert r.status_code == 404


# --- endpoints router extra coverage ---


def test_endpoints_patch_updates_name_and_config(
    app_client_and_db: tuple[TestClient, Path],
) -> None:
    client, _ = app_client_and_db
    created = client.post(
        "/api/endpoints",
        json={
            "kind": "slack_webhook",
            "name": "orig",
            "config": {"webhook_url": "https://hooks.slack.com/services/A/B/C"},
        },
    ).json()
    eid = created["id"]

    r = client.patch(
        f"/api/endpoints/{eid}",
        json={"name": "renamed"},
    )
    assert r.status_code == 200
    assert r.json()["name"] == "renamed"

    # Invalid config rejected
    r = client.patch(
        f"/api/endpoints/{eid}",
        json={"config": {"webhook_url": "not-a-url"}},
    )
    assert r.status_code == 422


def test_endpoints_patch_404_when_missing(
    app_client_and_db: tuple[TestClient, Path],
) -> None:
    client, _ = app_client_and_db
    r = client.patch("/api/endpoints/99999", json={"name": "x"})
    assert r.status_code == 404


def test_endpoints_disable_missing_returns_404(
    app_client_and_db: tuple[TestClient, Path],
) -> None:
    client, _ = app_client_and_db
    r = client.post("/api/endpoints/99999/disable")
    assert r.status_code == 404
