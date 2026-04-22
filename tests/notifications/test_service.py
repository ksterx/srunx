"""Tests for :class:`srunx.notifications.service.NotificationService`.

Uses the ``tmp_srunx_db`` fixture (file-backed SQLite) so we get a fully
migrated schema and real FKs. Every test drives the service through
already-persisted events and verifies the deliveries table outcome.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from srunx.db.models import Event
from srunx.db.repositories.deliveries import DeliveryRepository
from srunx.db.repositories.endpoints import EndpointRepository
from srunx.db.repositories.events import EventRepository
from srunx.db.repositories.subscriptions import SubscriptionRepository
from srunx.db.repositories.watches import WatchRepository
from srunx.notifications.service import NotificationService

# ---------------------------------------------------------------------------
# Test helpers
# ---------------------------------------------------------------------------


def _build_service(
    conn: sqlite3.Connection,
) -> tuple[
    NotificationService,
    WatchRepository,
    SubscriptionRepository,
    EventRepository,
    DeliveryRepository,
    EndpointRepository,
]:
    watches = WatchRepository(conn)
    subscriptions = SubscriptionRepository(conn)
    events = EventRepository(conn)
    deliveries = DeliveryRepository(conn)
    endpoints = EndpointRepository(conn)
    service = NotificationService(
        watch_repo=watches,
        subscription_repo=subscriptions,
        event_repo=events,
        delivery_repo=deliveries,
        endpoint_repo=endpoints,
    )
    return service, watches, subscriptions, events, deliveries, endpoints


def _insert_event(
    events: EventRepository,
    *,
    kind: str,
    source_ref: str,
    payload: dict,
) -> Event:
    event_id = events.insert(kind=kind, source_ref=source_ref, payload=payload)
    assert event_id is not None, "fixture event should insert cleanly"
    stored = events.get(event_id)
    assert stored is not None
    return stored


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestFanOut:
    """End-to-end fan-out behaviour."""

    def test_matching_open_watch_gets_delivery(
        self,
        tmp_srunx_db: tuple[sqlite3.Connection, Path],
    ) -> None:
        conn, _ = tmp_srunx_db
        service, watches, subscriptions, events, deliveries, endpoints = _build_service(
            conn
        )

        endpoint_id = endpoints.create("slack_webhook", "default", {"webhook_url": "x"})
        watch_id = watches.create("job", "job:42")
        subscriptions.create(watch_id, endpoint_id, "terminal")

        event = _insert_event(
            events,
            kind="job.status_changed",
            source_ref="job:42",
            payload={"from_status": "RUNNING", "to_status": "COMPLETED"},
        )

        created = service.fan_out(event, conn)
        assert len(created) == 1

        delivery = deliveries.get(created[0])
        assert delivery is not None
        assert delivery.endpoint_id == endpoint_id
        assert delivery.subscription_id is not None
        assert delivery.status == "pending"

    def test_non_matching_source_ref_skipped(
        self,
        tmp_srunx_db: tuple[sqlite3.Connection, Path],
    ) -> None:
        conn, _ = tmp_srunx_db
        service, watches, subscriptions, events, _, endpoints = _build_service(conn)

        endpoint_id = endpoints.create("slack_webhook", "default", {"webhook_url": "x"})
        # Watch is for job:42 but the event is for job:99 — no match.
        watch_id = watches.create("job", "job:42")
        subscriptions.create(watch_id, endpoint_id, "terminal")

        event = _insert_event(
            events,
            kind="job.status_changed",
            source_ref="job:99",
            payload={"to_status": "COMPLETED"},
        )

        created = service.fan_out(event, conn)
        assert created == []

    def test_closed_watch_skipped(
        self,
        tmp_srunx_db: tuple[sqlite3.Connection, Path],
    ) -> None:
        conn, _ = tmp_srunx_db
        service, watches, subscriptions, events, _, endpoints = _build_service(conn)

        endpoint_id = endpoints.create("slack_webhook", "default", {"webhook_url": "x"})
        watch_id = watches.create("job", "job:1")
        subscriptions.create(watch_id, endpoint_id, "terminal")
        assert watches.close(watch_id) is True

        event = _insert_event(
            events,
            kind="job.status_changed",
            source_ref="job:1",
            payload={"to_status": "COMPLETED"},
        )

        assert service.fan_out(event, conn) == []

    def test_disabled_endpoint_skipped(
        self,
        tmp_srunx_db: tuple[sqlite3.Connection, Path],
    ) -> None:
        conn, _ = tmp_srunx_db
        service, watches, subscriptions, events, _, endpoints = _build_service(conn)

        endpoint_id = endpoints.create("slack_webhook", "default", {"webhook_url": "x"})
        watch_id = watches.create("job", "job:1")
        subscriptions.create(watch_id, endpoint_id, "terminal")

        # Disable the endpoint. The watch/subscription remain but the
        # fan-out must skip the delivery.
        assert endpoints.disable(endpoint_id) is True

        event = _insert_event(
            events,
            kind="job.status_changed",
            source_ref="job:1",
            payload={"to_status": "COMPLETED"},
        )

        assert service.fan_out(event, conn) == []

    def test_preset_filter_applied(
        self,
        tmp_srunx_db: tuple[sqlite3.Connection, Path],
    ) -> None:
        """terminal preset should skip RUNNING events."""
        conn, _ = tmp_srunx_db
        service, watches, subscriptions, events, _, endpoints = _build_service(conn)

        endpoint_id = endpoints.create("slack_webhook", "default", {"webhook_url": "x"})
        watch_id = watches.create("job", "job:7")
        subscriptions.create(watch_id, endpoint_id, "terminal")

        event_running = _insert_event(
            events,
            kind="job.status_changed",
            source_ref="job:7",
            payload={"to_status": "RUNNING"},
        )
        assert service.fan_out(event_running, conn) == []

        event_completed = _insert_event(
            events,
            kind="job.status_changed",
            source_ref="job:7",
            payload={"to_status": "COMPLETED"},
        )
        assert len(service.fan_out(event_completed, conn)) == 1

    def test_idempotency_dedup_on_repeated_fan_out(
        self,
        tmp_srunx_db: tuple[sqlite3.Connection, Path],
    ) -> None:
        """Calling fan_out twice with the same event must not double-insert."""
        conn, _ = tmp_srunx_db
        service, watches, subscriptions, events, deliveries, endpoints = _build_service(
            conn
        )

        endpoint_id = endpoints.create("slack_webhook", "default", {"webhook_url": "x"})
        watch_id = watches.create("job", "job:5")
        subscriptions.create(watch_id, endpoint_id, "terminal")

        event = _insert_event(
            events,
            kind="job.status_changed",
            source_ref="job:5",
            payload={"to_status": "COMPLETED"},
        )

        first = service.fan_out(event, conn)
        second = service.fan_out(event, conn)
        assert len(first) == 1
        # Second call should hit INSERT OR IGNORE on the
        # (endpoint_id, idempotency_key) UNIQUE and return no new ids.
        assert second == []

        # Only one delivery row in the DB for this subscription.
        rows = deliveries.list_by_subscription(
            # list_by_watch returned a Subscription whose id we know.
            subscriptions.list_by_watch(watch_id)[0].id or 0
        )
        assert len(rows) == 1

    def test_requires_persisted_event(
        self,
        tmp_srunx_db: tuple[sqlite3.Connection, Path],
    ) -> None:
        conn, _ = tmp_srunx_db
        service, *_ = _build_service(conn)

        unsaved = Event(
            id=None,
            kind="job.status_changed",
            source_ref="job:1",
            payload={"to_status": "COMPLETED"},
            payload_hash="dummy",
            observed_at=_now(),
        )
        with pytest.raises(ValueError, match="persisted id"):
            service.fan_out(unsaved, conn)


class TestCreateWatchForWorkflowRun:
    """``create_watch_for_workflow_run`` auto-watch behaviour."""

    def test_without_endpoint_creates_watch_only(
        self,
        tmp_srunx_db: tuple[sqlite3.Connection, Path],
    ) -> None:
        conn, _ = tmp_srunx_db
        service, watches, subscriptions, *_ = _build_service(conn)

        watch_id = service.create_watch_for_workflow_run(
            run_id=11, endpoint_id=None, preset=None
        )
        watch = watches.get(watch_id)
        assert watch is not None
        assert watch.kind == "workflow_run"
        assert watch.target_ref == "workflow_run:11"
        # No subscription was created.
        assert subscriptions.list_by_watch(watch_id) == []

    def test_with_endpoint_creates_subscription(
        self,
        tmp_srunx_db: tuple[sqlite3.Connection, Path],
    ) -> None:
        conn, _ = tmp_srunx_db
        service, _, subscriptions, _, _, endpoints = _build_service(conn)

        endpoint_id = endpoints.create("slack_webhook", "default", {"webhook_url": "x"})
        watch_id = service.create_watch_for_workflow_run(
            run_id=12, endpoint_id=endpoint_id, preset="terminal"
        )
        subs = subscriptions.list_by_watch(watch_id)
        assert len(subs) == 1
        assert subs[0].preset == "terminal"
        assert subs[0].endpoint_id == endpoint_id

    def test_endpoint_without_preset_creates_watch_only(
        self,
        tmp_srunx_db: tuple[sqlite3.Connection, Path],
    ) -> None:
        """Per contract: both endpoint_id AND preset required to subscribe."""
        conn, _ = tmp_srunx_db
        service, _, subscriptions, _, _, endpoints = _build_service(conn)

        endpoint_id = endpoints.create("slack_webhook", "default", {"webhook_url": "x"})
        watch_id = service.create_watch_for_workflow_run(
            run_id=13, endpoint_id=endpoint_id, preset=None
        )
        assert subscriptions.list_by_watch(watch_id) == []


class TestCreateWatchForJob:
    """``create_watch_for_job`` shape parity with workflow version."""

    def test_without_endpoint(
        self,
        tmp_srunx_db: tuple[sqlite3.Connection, Path],
    ) -> None:
        conn, _ = tmp_srunx_db
        service, watches, subscriptions, *_ = _build_service(conn)

        watch_id = service.create_watch_for_job(
            job_id=99, endpoint_id=None, preset="terminal"
        )
        watch = watches.get(watch_id)
        assert watch is not None
        assert watch.kind == "job"
        # V5 grammar: ``job:<scheduler_key>:<id>``; default is local.
        assert watch.target_ref == "job:local:99"
        assert subscriptions.list_by_watch(watch_id) == []

    def test_with_endpoint(
        self,
        tmp_srunx_db: tuple[sqlite3.Connection, Path],
    ) -> None:
        conn, _ = tmp_srunx_db
        service, _, subscriptions, _, _, endpoints = _build_service(conn)

        endpoint_id = endpoints.create("slack_webhook", "default", {"webhook_url": "x"})
        watch_id = service.create_watch_for_job(
            job_id=100, endpoint_id=endpoint_id, preset="running_and_terminal"
        )
        subs = subscriptions.list_by_watch(watch_id)
        assert len(subs) == 1
        assert subs[0].preset == "running_and_terminal"


# ---------------------------------------------------------------------------
# Local helper (avoid importing private helper)
# ---------------------------------------------------------------------------


def _now():
    from datetime import UTC, datetime

    return datetime.now(UTC)
