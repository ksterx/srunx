"""Unit tests for :class:`srunx.pollers.delivery_poller.DeliveryPoller`.

Every test uses the ``tmp_srunx_db`` fixture for a real, file-backed SQLite
schema and drives the poller via ``anyio.run``. Adapters are swapped into
:data:`srunx.notifications.adapters.registry.ADAPTERS` per-test so delivery
outcomes are deterministic without going near a real Slack webhook.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import anyio
import pytest
from srunx.notifications.adapters.base import DeliveryError

from srunx.db.models import Event
from srunx.db.repositories.deliveries import DeliveryRepository
from srunx.db.repositories.endpoints import EndpointRepository
from srunx.db.repositories.events import EventRepository
from srunx.db.repositories.subscriptions import SubscriptionRepository
from srunx.db.repositories.watches import WatchRepository
from srunx.notifications.adapters import registry as adapter_registry
from srunx.pollers.delivery_poller import DeliveryPoller

# ---------------------------------------------------------------------------
# Test fixtures / stubs
# ---------------------------------------------------------------------------


class _RecordingAdapter:
    """Adapter stub that records each send() call."""

    kind: str = "slack_webhook"

    def __init__(self) -> None:
        self.calls: list[tuple[Event, dict]] = []

    def send(self, event: Event, endpoint_config: dict) -> None:
        self.calls.append((event, endpoint_config))


class _FailingAdapter:
    """Adapter stub that always raises ``DeliveryError``."""

    kind: str = "slack_webhook"

    def __init__(self, message: str = "boom") -> None:
        self.calls: int = 0
        self.message = message

    def send(self, event: Event, endpoint_config: dict) -> None:
        self.calls += 1
        raise DeliveryError(self.message)


@pytest.fixture(autouse=True)
def _isolate_adapter_registry() -> Iterator[None]:
    """Snapshot/restore the module-level adapter registry around each test."""
    saved = dict(adapter_registry.ADAPTERS)
    try:
        yield
    finally:
        adapter_registry.ADAPTERS.clear()
        adapter_registry.ADAPTERS.update(saved)


def _seed_pending_delivery(
    conn: sqlite3.Connection,
    *,
    endpoint_kind: str = "slack_webhook",
    endpoint_name: str = "default",
    disabled: bool = False,
) -> tuple[int, int, int]:
    """Seed a watch/subscription/endpoint/event/delivery chain.

    Returns ``(delivery_id, endpoint_id, event_id)``.
    """
    endpoints = EndpointRepository(conn)
    watches = WatchRepository(conn)
    subscriptions = SubscriptionRepository(conn)
    events = EventRepository(conn)
    deliveries = DeliveryRepository(conn)

    endpoint_id = endpoints.create(
        endpoint_kind, endpoint_name, {"webhook_url": "https://example"}
    )
    if disabled:
        assert endpoints.disable(endpoint_id)

    watch_id = watches.create("job", "job:123")
    subscription_id = subscriptions.create(watch_id, endpoint_id, "terminal")

    event_id = events.insert(
        kind="job.status_changed",
        source_ref="job:123",
        payload={"from_status": "RUNNING", "to_status": "COMPLETED"},
    )
    assert event_id is not None

    delivery_id = deliveries.insert(
        event_id=event_id,
        subscription_id=subscription_id,
        endpoint_id=endpoint_id,
        idempotency_key=f"job:123:status:COMPLETED:{endpoint_id}",
    )
    assert delivery_id is not None

    return delivery_id, endpoint_id, event_id


def _run(poller: DeliveryPoller) -> None:
    anyio.run(poller.run_cycle)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestSuccessfulDelivery:
    def test_successful_delivery_marks_delivered(
        self, tmp_srunx_db: tuple[sqlite3.Connection, Path]
    ) -> None:
        conn, db_path = tmp_srunx_db
        delivery_id, _, _ = _seed_pending_delivery(conn)

        adapter = _RecordingAdapter()
        adapter_registry.ADAPTERS["slack_webhook"] = adapter

        poller = DeliveryPoller(db_path=db_path, worker_id="test-worker")
        _run(poller)

        assert len(adapter.calls) == 1

        delivery = DeliveryRepository(conn).get(delivery_id)
        assert delivery is not None
        assert delivery.status == "delivered"
        assert delivery.delivered_at is not None
        assert delivery.leased_until is None
        assert delivery.worker_id is None

    def test_batch_processes_multiple_in_one_cycle(
        self, tmp_srunx_db: tuple[sqlite3.Connection, Path]
    ) -> None:
        conn, db_path = tmp_srunx_db

        endpoints = EndpointRepository(conn)
        watches = WatchRepository(conn)
        subscriptions = SubscriptionRepository(conn)
        events = EventRepository(conn)
        deliveries = DeliveryRepository(conn)

        endpoint_id = endpoints.create("slack_webhook", "default", {"webhook_url": "x"})
        watch_id = watches.create("job", "job:1")
        subscription_id = subscriptions.create(watch_id, endpoint_id, "terminal")

        delivery_ids: list[int] = []
        for idx in range(3):
            event_id = events.insert(
                kind="job.status_changed",
                source_ref=f"job:{idx}",
                payload={"to_status": f"COMPLETED-{idx}"},
            )
            assert event_id is not None
            delivery_id = deliveries.insert(
                event_id=event_id,
                subscription_id=subscription_id,
                endpoint_id=endpoint_id,
                idempotency_key=f"k-{idx}",
            )
            assert delivery_id is not None
            delivery_ids.append(delivery_id)

        adapter = _RecordingAdapter()
        adapter_registry.ADAPTERS["slack_webhook"] = adapter

        poller = DeliveryPoller(db_path=db_path, worker_id="test-worker")
        _run(poller)

        assert len(adapter.calls) == 3
        repo = DeliveryRepository(conn)
        for did in delivery_ids:
            d = repo.get(did)
            assert d is not None and d.status == "delivered"


class TestFailureRetryAbandon:
    def test_failing_adapter_marks_retry_with_incremented_attempt(
        self, tmp_srunx_db: tuple[sqlite3.Connection, Path]
    ) -> None:
        conn, db_path = tmp_srunx_db
        delivery_id, _, _ = _seed_pending_delivery(conn)

        adapter_registry.ADAPTERS["slack_webhook"] = _FailingAdapter("slack 500")

        poller = DeliveryPoller(db_path=db_path, worker_id="test-worker", max_retries=5)
        _run(poller)

        delivery = DeliveryRepository(conn).get(delivery_id)
        assert delivery is not None
        assert delivery.status == "pending"
        assert delivery.attempt_count == 1
        assert delivery.last_error == "slack 500"
        assert delivery.leased_until is None
        assert delivery.worker_id is None

    def test_reaches_max_retries_marks_abandoned(
        self, tmp_srunx_db: tuple[sqlite3.Connection, Path]
    ) -> None:
        """Preloaded attempt_count=4 with max_retries=5 → next failure abandons."""
        conn, db_path = tmp_srunx_db
        delivery_id, _, _ = _seed_pending_delivery(conn)

        # Bump attempt_count so the next failure is the final attempt.
        conn.execute(
            "UPDATE deliveries SET attempt_count = 4 WHERE id = ?",
            (delivery_id,),
        )

        adapter_registry.ADAPTERS["slack_webhook"] = _FailingAdapter("final")

        poller = DeliveryPoller(db_path=db_path, worker_id="test-worker", max_retries=5)
        _run(poller)

        delivery = DeliveryRepository(conn).get(delivery_id)
        assert delivery is not None
        assert delivery.status == "abandoned"
        assert delivery.last_error == "final"
        assert delivery.leased_until is None
        assert delivery.worker_id is None

    def test_generic_exception_is_retried(
        self, tmp_srunx_db: tuple[sqlite3.Connection, Path]
    ) -> None:
        """A bare Exception (not DeliveryError) is also retried, not crashed on."""
        conn, db_path = tmp_srunx_db
        delivery_id, _, _ = _seed_pending_delivery(conn)

        class _BoomAdapter:
            kind: str = "slack_webhook"

            def send(self, event: Event, endpoint_config: dict) -> None:
                raise ValueError("network unreachable")

        adapter_registry.ADAPTERS["slack_webhook"] = _BoomAdapter()

        poller = DeliveryPoller(db_path=db_path, worker_id="test-worker", max_retries=5)
        _run(poller)

        delivery = DeliveryRepository(conn).get(delivery_id)
        assert delivery is not None
        assert delivery.status == "pending"
        assert delivery.attempt_count == 1
        assert delivery.last_error is not None
        assert "ValueError" in delivery.last_error
        assert "network unreachable" in delivery.last_error


class TestAbandonedPreflight:
    def test_missing_event_marks_abandoned(
        self, tmp_srunx_db: tuple[sqlite3.Connection, Path]
    ) -> None:
        conn, db_path = tmp_srunx_db
        delivery_id, _, _ = _seed_pending_delivery(conn)

        # The FK deliveries.event_id → events has ON DELETE CASCADE, so we
        # cannot drop the event without also dropping the delivery. Disable
        # FKs on THIS connection (the poller opens its own connection with
        # FKs on) and sever the link, producing a dangling event_id.
        conn.execute("PRAGMA foreign_keys = OFF")
        try:
            conn.execute(
                "UPDATE deliveries SET event_id = 999999 WHERE id = ?",
                (delivery_id,),
            )
        finally:
            conn.execute("PRAGMA foreign_keys = ON")

        adapter_registry.ADAPTERS["slack_webhook"] = _RecordingAdapter()

        poller = DeliveryPoller(db_path=db_path, worker_id="test-worker")
        _run(poller)

        delivery = DeliveryRepository(conn).get(delivery_id)
        assert delivery is not None
        assert delivery.status == "abandoned"
        assert delivery.last_error == "referenced event or endpoint vanished"

    def test_disabled_endpoint_marks_abandoned(
        self,
        tmp_srunx_db: tuple[sqlite3.Connection, Path],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Claim step filters disabled endpoints, but a race can slip through
        (endpoint disabled after claim). The poller must still abandon.
        """
        conn, db_path = tmp_srunx_db
        delivery_id, endpoint_id, _ = _seed_pending_delivery(conn)

        adapter = _RecordingAdapter()
        adapter_registry.ADAPTERS["slack_webhook"] = adapter

        # Monkeypatch claim_one to disable the endpoint *after* the row is
        # handed out, so _process_delivery re-reads a disabled endpoint.
        original_claim = DeliveryRepository.claim_one

        def _claim_then_disable(
            self: DeliveryRepository,
            worker_id: str,
            lease_duration_secs: int = 300,
        ) -> Any:
            result = original_claim(self, worker_id, lease_duration_secs)
            if result is not None:
                EndpointRepository(self.conn).disable(endpoint_id)
            return result

        monkeypatch.setattr(DeliveryRepository, "claim_one", _claim_then_disable)

        poller = DeliveryPoller(db_path=db_path, worker_id="test-worker")
        _run(poller)

        assert adapter.calls == []
        delivery = DeliveryRepository(conn).get(delivery_id)
        assert delivery is not None
        assert delivery.status == "abandoned"
        assert delivery.last_error == "endpoint disabled"

    def test_unknown_adapter_kind_marks_abandoned(
        self, tmp_srunx_db: tuple[sqlite3.Connection, Path]
    ) -> None:
        conn, db_path = tmp_srunx_db
        delivery_id, _, _ = _seed_pending_delivery(conn)

        # Remove the adapter so get_adapter raises KeyError.
        adapter_registry.ADAPTERS.pop("slack_webhook", None)

        poller = DeliveryPoller(db_path=db_path, worker_id="test-worker")
        _run(poller)

        delivery = DeliveryRepository(conn).get(delivery_id)
        assert delivery is not None
        assert delivery.status == "abandoned"
        assert delivery.last_error is not None
        assert "unknown adapter kind" in delivery.last_error


class TestCycleControl:
    def test_reclaim_expired_leases_is_called(
        self,
        tmp_srunx_db: tuple[sqlite3.Connection, Path],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """``reclaim_expired_leases`` runs at the very start of each cycle."""
        conn, db_path = tmp_srunx_db

        # Seed an expired-lease row so we can observe the reclaim path.
        delivery_id, _, _ = _seed_pending_delivery(conn)
        conn.execute(
            """
            UPDATE deliveries
               SET status = 'sending',
                   leased_until = '2000-01-01T00:00:00.000Z',
                   worker_id = 'zombie'
             WHERE id = ?
            """,
            (delivery_id,),
        )

        calls: list[str] = []
        original = DeliveryRepository.reclaim_expired_leases

        def _tracking(self: DeliveryRepository) -> int:
            calls.append("reclaim")
            return original(self)

        monkeypatch.setattr(DeliveryRepository, "reclaim_expired_leases", _tracking)

        adapter_registry.ADAPTERS["slack_webhook"] = _RecordingAdapter()

        poller = DeliveryPoller(db_path=db_path, worker_id="test-worker")
        _run(poller)

        assert calls == ["reclaim"]
        # The zombie row should now be delivered.
        delivery = DeliveryRepository(conn).get(delivery_id)
        assert delivery is not None
        assert delivery.status == "delivered"

    def test_empty_outbox_is_noop(
        self,
        tmp_srunx_db: tuple[sqlite3.Connection, Path],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _, db_path = tmp_srunx_db

        claim_calls: list[str] = []
        original = DeliveryRepository.claim_one

        def _counting(
            self: DeliveryRepository,
            worker_id: str,
            lease_duration_secs: int = 300,
        ) -> Any:
            claim_calls.append(worker_id)
            return original(self, worker_id, lease_duration_secs)

        monkeypatch.setattr(DeliveryRepository, "claim_one", _counting)

        poller = DeliveryPoller(db_path=db_path, worker_id="test-worker")
        _run(poller)

        # Exactly one claim attempt, which returned None and broke the loop.
        assert claim_calls == ["test-worker"]

    def test_default_worker_id_embeds_pid(self) -> None:
        poller = DeliveryPoller()
        assert poller.worker_id.startswith("delivery-")
        assert poller.worker_id.split("-", 1)[1].isdigit()
