"""Smoke: real DeliveryPoller posts a real HTTP request to a local server.

Bypasses the Slack URL regex by redirecting ``slack_sdk.WebhookClient``
to ``http://127.0.0.1:<port>`` at runtime. Proves the adapter → HTTP
→ ``mark_delivered`` chain end-to-end without actually hitting Slack.
"""

from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Any

import anyio
import pytest
from srunx.pollers.delivery_poller import DeliveryPoller

from srunx.db.connection import open_connection
from srunx.db.migrations import apply_migrations
from srunx.db.repositories.deliveries import DeliveryRepository
from srunx.db.repositories.endpoints import EndpointRepository
from srunx.db.repositories.events import EventRepository
from srunx.db.repositories.subscriptions import SubscriptionRepository
from srunx.db.repositories.watches import WatchRepository
from srunx.notifications.service import NotificationService

_received: list[dict[str, Any]] = []


class _RecordingHandler(BaseHTTPRequestHandler):
    def do_POST(self) -> None:  # noqa: N802 (stdlib naming)
        length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(length).decode("utf-8")
        try:
            parsed = json.loads(body)
        except Exception:
            parsed = {"raw": body}
        _received.append(parsed)
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"ok")

    def log_message(self, *args: Any, **kwargs: Any) -> None:  # silence stderr spam
        pass


@pytest.fixture
def local_http() -> Any:
    _received.clear()
    server = HTTPServer(("127.0.0.1", 0), _RecordingHandler)
    port = server.server_port
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield port
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


def test_delivery_poller_posts_to_local_http(
    tmp_path: Path, local_http: int, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Full pipeline through the real SlackWebhookDeliveryAdapter → local HTTP."""
    # --- Patch slack_sdk.WebhookClient so the adapter targets our local server.
    from slack_sdk.webhook import WebhookClient

    original_init = WebhookClient.__init__

    def patched_init(self: WebhookClient, url: str, *args: Any, **kwargs: Any) -> None:
        # Redirect any hooks.slack.com URL to the local recording server.
        original_init(self, f"http://127.0.0.1:{local_http}/hook", *args, **kwargs)

    monkeypatch.setattr(WebhookClient, "__init__", patched_init)

    # Seed DB: endpoint + watch + subscription + event.
    db_path = tmp_path / "srunx.db"
    conn = open_connection(db_path)
    apply_migrations(conn)

    endpoint_id = EndpointRepository(conn).create(
        kind="slack_webhook",
        name="smoke",
        config={
            # URL must pass the Slack regex at the router level — for this smoke
            # we bypass the router and write directly to the repo.
            "webhook_url": "https://hooks.slack.com/services/A/B/C"
        },
    )
    watch_id = WatchRepository(conn).create(kind="job", target_ref="job:9001")
    SubscriptionRepository(conn).create(watch_id, endpoint_id, "terminal")

    event_id = EventRepository(conn).insert(
        kind="job.status_changed",
        source_ref="job:9001",
        payload={"from_status": "RUNNING", "to_status": "COMPLETED"},
    )
    assert event_id is not None

    service = NotificationService(
        watch_repo=WatchRepository(conn),
        subscription_repo=SubscriptionRepository(conn),
        event_repo=EventRepository(conn),
        delivery_repo=DeliveryRepository(conn),
        endpoint_repo=EndpointRepository(conn),
    )
    event = EventRepository(conn).get(event_id)
    assert event is not None
    service.fan_out(event, conn)
    conn.close()

    # --- Run one DeliveryPoller cycle → adapter.send → HTTP POST → local server.
    poller = DeliveryPoller(db_path=db_path, worker_id="smoke-w", max_retries=3)
    anyio.run(poller.run_cycle)

    # --- Verify: local server got exactly one POST with Slack block payload.
    assert len(_received) == 1
    payload = _received[0]
    assert "blocks" in payload
    # Rendered text should mention the source ref and new status.
    rendered = json.dumps(payload)
    assert "COMPLETED" in rendered
    assert "9001" in rendered

    # Delivery row is terminal.
    conn2 = open_connection(db_path)
    try:
        row = conn2.execute(
            "SELECT status, delivered_at, last_error FROM deliveries"
        ).fetchone()
    finally:
        conn2.close()
    assert row["status"] == "delivered"
    assert row["delivered_at"] is not None
    assert row["last_error"] is None


def test_delivery_poller_retries_on_local_500(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Local server returns 500 → DeliveryError → mark_retry."""
    _received.clear()

    class _FailingHandler(_RecordingHandler):
        def do_POST(self) -> None:  # noqa: N802
            length = int(self.headers.get("Content-Length", "0"))
            self.rfile.read(length)
            _received.append({"500": True})
            self.send_response(500)
            self.end_headers()
            self.wfile.write(b"fail")

    server = HTTPServer(("127.0.0.1", 0), _FailingHandler)
    port = server.server_port
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        from slack_sdk.webhook import WebhookClient

        original_init = WebhookClient.__init__

        def patched_init(self: WebhookClient, url: str, *a: Any, **k: Any) -> None:
            original_init(self, f"http://127.0.0.1:{port}/hook", *a, **k)

        monkeypatch.setattr(WebhookClient, "__init__", patched_init)

        # Seed.
        db_path = tmp_path / "srunx.db"
        conn = open_connection(db_path)
        apply_migrations(conn)

        endpoint_id = EndpointRepository(conn).create(
            kind="slack_webhook",
            name="fail-smoke",
            config={"webhook_url": "https://hooks.slack.com/services/A/B/C"},
        )
        watch_id = WatchRepository(conn).create(kind="job", target_ref="job:9002")
        SubscriptionRepository(conn).create(watch_id, endpoint_id, "terminal")
        event_id = EventRepository(conn).insert(
            kind="job.status_changed",
            source_ref="job:9002",
            payload={"from_status": "RUNNING", "to_status": "FAILED"},
        )
        assert event_id is not None
        event = EventRepository(conn).get(event_id)
        assert event is not None
        NotificationService(
            watch_repo=WatchRepository(conn),
            subscription_repo=SubscriptionRepository(conn),
            event_repo=EventRepository(conn),
            delivery_repo=DeliveryRepository(conn),
            endpoint_repo=EndpointRepository(conn),
        ).fan_out(event, conn)
        conn.close()

        poller = DeliveryPoller(db_path=db_path, worker_id="fail-w", max_retries=5)
        anyio.run(poller.run_cycle)

        assert len(_received) == 1  # server received the failing POST
        conn2 = open_connection(db_path)
        try:
            row = conn2.execute(
                "SELECT status, attempt_count, last_error FROM deliveries"
            ).fetchone()
        finally:
            conn2.close()
        assert row["status"] == "pending"  # marked for retry, NOT abandoned
        assert row["attempt_count"] == 1
        assert row["last_error"] is not None
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)
