"""End-to-end integration test for the full notification data flow.

Proves: submit → watch → subscription → event fan-out → delivery queued →
DeliveryPoller sends → mock adapter receives payload → delivery marked
delivered.

Distinct from ``tests/pollers/test_delivery_poller.py`` which mocks at
the adapter level in isolation; this test exercises the producer side
(events + NotificationService.fan_out) together with the consumer side
(DeliveryPoller) in one flow, against a real tmp DB.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import anyio
import pytest
from srunx.db.connection import open_connection
from srunx.db.migrations import apply_migrations
from srunx.db.models import Event
from srunx.db.repositories.deliveries import DeliveryRepository
from srunx.db.repositories.endpoints import EndpointRepository
from srunx.db.repositories.events import EventRepository
from srunx.db.repositories.subscriptions import SubscriptionRepository
from srunx.db.repositories.watches import WatchRepository

from srunx.notifications.adapters.base import DeliveryAdapter, DeliveryError
from srunx.notifications.service import NotificationService
from srunx.pollers.delivery_poller import DeliveryPoller


class RecordingAdapter:
    """Stand-in for ``SlackWebhookDeliveryAdapter`` that records calls."""

    kind = "slack_webhook"

    def __init__(self) -> None:
        self.sent: list[tuple[Event, dict[str, Any]]] = []
        self.fail_next = False

    def send(self, event: Event, endpoint_config: dict[str, Any]) -> None:
        if self.fail_next:
            self.fail_next = False
            raise DeliveryError("synthetic failure")
        self.sent.append((event, dict(endpoint_config)))


@pytest.fixture
def db_path(tmp_path: Path) -> Path:
    """File-backed tmp DB with full schema applied."""
    path = tmp_path / "srunx.db"
    conn = open_connection(path)
    apply_migrations(conn)
    conn.close()
    return path


def _seed_watch_subscription(conn: Any) -> tuple[int, int, int]:
    """Returns ``(endpoint_id, watch_id, subscription_id)``."""
    endpoint_id = EndpointRepository(conn).create(
        kind="slack_webhook",
        name="e2e",
        config={"webhook_url": "https://hooks.slack.com/services/X/Y/Z"},
    )
    watch_id = WatchRepository(conn).create(kind="job", target_ref="job:12345")
    subscription_id = SubscriptionRepository(conn).create(
        watch_id=watch_id, endpoint_id=endpoint_id, preset="terminal"
    )
    return endpoint_id, watch_id, subscription_id


def test_full_flow_event_to_delivered(db_path: Path) -> None:
    """event → fan_out → delivery → adapter sends → mark_delivered."""
    # --- arrange ---
    conn = open_connection(db_path)
    endpoint_id, watch_id, subscription_id = _seed_watch_subscription(conn)

    service = NotificationService(
        watch_repo=WatchRepository(conn),
        subscription_repo=SubscriptionRepository(conn),
        event_repo=EventRepository(conn),
        delivery_repo=DeliveryRepository(conn),
        endpoint_repo=EndpointRepository(conn),
    )

    # Producer-side: insert a job.status_changed → COMPLETED event.
    event_id = EventRepository(conn).insert(
        kind="job.status_changed",
        source_ref="job:12345",
        payload={"from_status": "RUNNING", "to_status": "COMPLETED"},
    )
    assert event_id is not None
    event = EventRepository(conn).get(event_id)
    assert event is not None

    delivery_ids = service.fan_out(event, conn)
    assert len(delivery_ids) == 1, "exactly one delivery should fan out"
    conn.close()

    # --- act: run one DeliveryPoller cycle against the tmp DB ---
    adapter = RecordingAdapter()

    # Patch registry for this test.
    from srunx.notifications.adapters import registry

    original = registry.ADAPTERS.copy()
    registry.ADAPTERS["slack_webhook"] = adapter  # type: ignore[assignment]
    try:
        poller = DeliveryPoller(
            db_path=db_path,
            worker_id="test-worker",
            max_retries=3,
        )
        anyio.run(poller.run_cycle)
    finally:
        registry.ADAPTERS.clear()
        registry.ADAPTERS.update(original)

    # --- assert ---
    assert len(adapter.sent) == 1, "exactly one Slack payload should be sent"
    sent_event, sent_config = adapter.sent[0]
    assert sent_event.kind == "job.status_changed"
    assert sent_event.source_ref == "job:12345"
    assert sent_event.payload["to_status"] == "COMPLETED"
    assert sent_config["webhook_url"] == "https://hooks.slack.com/services/X/Y/Z"

    # Delivery row should be terminal.
    conn2 = open_connection(db_path)
    try:
        rows = conn2.execute(
            "SELECT status, delivered_at, last_error FROM deliveries"
        ).fetchall()
    finally:
        conn2.close()
    assert len(rows) == 1
    assert rows[0]["status"] == "delivered"
    assert rows[0]["delivered_at"] is not None
    assert rows[0]["last_error"] is None


def test_full_flow_retry_then_deliver(db_path: Path) -> None:
    """Transient failure → retry → success."""
    conn = open_connection(db_path)
    _seed_watch_subscription(conn)
    service = NotificationService(
        watch_repo=WatchRepository(conn),
        subscription_repo=SubscriptionRepository(conn),
        event_repo=EventRepository(conn),
        delivery_repo=DeliveryRepository(conn),
        endpoint_repo=EndpointRepository(conn),
    )
    event_id = EventRepository(conn).insert(
        kind="job.status_changed",
        source_ref="job:12345",
        payload={"from_status": "RUNNING", "to_status": "FAILED"},
    )
    assert event_id is not None
    event = EventRepository(conn).get(event_id)
    assert event is not None
    service.fan_out(event, conn)
    conn.close()

    # Adapter fails once then succeeds.
    adapter = RecordingAdapter()
    adapter.fail_next = True

    from srunx.notifications.adapters import registry

    original = registry.ADAPTERS.copy()
    registry.ADAPTERS["slack_webhook"] = adapter  # type: ignore[assignment]
    try:
        # Force immediate retry by using 0-second lease duration.
        poller = DeliveryPoller(db_path=db_path, worker_id="w", max_retries=5)

        anyio.run(poller.run_cycle)  # first cycle -> failure, mark_retry
        # Reset next_attempt_at so the row is immediately claimable again.
        conn3 = open_connection(db_path)
        try:
            from srunx.db.repositories.base import now_iso

            conn3.execute("UPDATE deliveries SET next_attempt_at = ?", (now_iso(),))
        finally:
            conn3.close()

        anyio.run(poller.run_cycle)  # second cycle -> success
    finally:
        registry.ADAPTERS.clear()
        registry.ADAPTERS.update(original)

    assert len(adapter.sent) == 1  # adapter succeeded once after the failure.

    conn4 = open_connection(db_path)
    try:
        row = conn4.execute("SELECT status, attempt_count FROM deliveries").fetchone()
    finally:
        conn4.close()
    assert row["status"] == "delivered"
    assert row["attempt_count"] == 1  # one failed attempt before success


def test_full_flow_preset_terminal_skips_job_submitted(db_path: Path) -> None:
    """preset='terminal' must NOT fan-out ``job.submitted`` events."""
    conn = open_connection(db_path)
    _seed_watch_subscription(conn)  # preset='terminal'
    service = NotificationService(
        watch_repo=WatchRepository(conn),
        subscription_repo=SubscriptionRepository(conn),
        event_repo=EventRepository(conn),
        delivery_repo=DeliveryRepository(conn),
        endpoint_repo=EndpointRepository(conn),
    )

    event_id = EventRepository(conn).insert(
        kind="job.submitted",
        source_ref="job:12345",
        payload={"job_id": 12345},
    )
    assert event_id is not None
    event = EventRepository(conn).get(event_id)
    assert event is not None

    delivery_ids = service.fan_out(event, conn)
    assert delivery_ids == []
    conn.close()


def test_full_flow_disabled_endpoint_skipped(db_path: Path) -> None:
    """Disabled endpoint must not receive deliveries."""
    conn = open_connection(db_path)
    endpoint_id, _, _ = _seed_watch_subscription(conn)
    EndpointRepository(conn).disable(endpoint_id)

    service = NotificationService(
        watch_repo=WatchRepository(conn),
        subscription_repo=SubscriptionRepository(conn),
        event_repo=EventRepository(conn),
        delivery_repo=DeliveryRepository(conn),
        endpoint_repo=EndpointRepository(conn),
    )
    event_id = EventRepository(conn).insert(
        kind="job.status_changed",
        source_ref="job:12345",
        payload={"to_status": "COMPLETED"},
    )
    assert event_id is not None
    event = EventRepository(conn).get(event_id)
    assert event is not None
    delivery_ids = service.fan_out(event, conn)
    assert delivery_ids == []
    conn.close()


def test_full_flow_idempotency_on_replayed_event(db_path: Path) -> None:
    """Same (kind, source_ref, logical key) fan_out twice → one delivery.

    Proves the deterministic idempotency_key / UNIQUE contract between
    ``EventRepository._compute_payload_hash`` and
    ``deliveries(endpoint_id, idempotency_key)``.
    """
    conn = open_connection(db_path)
    _seed_watch_subscription(conn)
    service = NotificationService(
        watch_repo=WatchRepository(conn),
        subscription_repo=SubscriptionRepository(conn),
        event_repo=EventRepository(conn),
        delivery_repo=DeliveryRepository(conn),
        endpoint_repo=EndpointRepository(conn),
    )

    event_id = EventRepository(conn).insert(
        kind="job.status_changed",
        source_ref="job:12345",
        payload={"to_status": "COMPLETED"},
    )
    assert event_id is not None
    event = EventRepository(conn).get(event_id)
    assert event is not None

    first = service.fan_out(event, conn)
    second = service.fan_out(event, conn)
    assert len(first) == 1
    assert second == []  # UNIQUE prevented a second row

    row_count = conn.execute("SELECT COUNT(*) FROM deliveries").fetchone()[0]
    assert row_count == 1
    conn.close()


def _verify_adapter_satisfies_protocol(adapter: RecordingAdapter) -> None:
    """Static Protocol satisfaction check (for documentation)."""
    x: DeliveryAdapter = adapter
    assert x.kind == "slack_webhook"
