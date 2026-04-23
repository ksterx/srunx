"""Tests for :meth:`NotificationService.create_watch_for_sweep_run`."""

from __future__ import annotations

import sqlite3
from pathlib import Path

from srunx.db.repositories.deliveries import DeliveryRepository
from srunx.db.repositories.endpoints import EndpointRepository
from srunx.db.repositories.events import EventRepository
from srunx.db.repositories.subscriptions import SubscriptionRepository
from srunx.db.repositories.watches import WatchRepository

from srunx.notifications.service import NotificationService


def _build_service(
    conn: sqlite3.Connection,
) -> tuple[
    NotificationService,
    WatchRepository,
    SubscriptionRepository,
    EndpointRepository,
]:
    watches = WatchRepository(conn)
    subscriptions = SubscriptionRepository(conn)
    endpoints = EndpointRepository(conn)
    service = NotificationService(
        watch_repo=watches,
        subscription_repo=subscriptions,
        event_repo=EventRepository(conn),
        delivery_repo=DeliveryRepository(conn),
        endpoint_repo=endpoints,
    )
    return service, watches, subscriptions, endpoints


def test_without_endpoint_creates_watch_only(
    tmp_srunx_db: tuple[sqlite3.Connection, Path],
) -> None:
    conn, _ = tmp_srunx_db
    service, watches, subscriptions, _ = _build_service(conn)

    watch_id = service.create_watch_for_sweep_run(sweep_run_id=7)
    watch = watches.get(watch_id)
    assert watch is not None
    assert watch.kind == "sweep_run"
    assert watch.target_ref == "sweep_run:7"
    assert subscriptions.list_by_watch(watch_id) == []


def test_with_endpoint_creates_subscription(
    tmp_srunx_db: tuple[sqlite3.Connection, Path],
) -> None:
    conn, _ = tmp_srunx_db
    service, _, subscriptions, endpoints = _build_service(conn)

    endpoint_id = endpoints.create(
        "slack_webhook", "default", {"webhook_url": "https://example/x"}
    )
    watch_id = service.create_watch_for_sweep_run(
        sweep_run_id=8,
        endpoint_id=endpoint_id,
        preset="running_and_terminal",
    )
    subs = subscriptions.list_by_watch(watch_id)
    assert len(subs) == 1
    assert subs[0].endpoint_id == endpoint_id
    assert subs[0].preset == "running_and_terminal"


def test_default_preset_is_terminal(
    tmp_srunx_db: tuple[sqlite3.Connection, Path],
) -> None:
    conn, _ = tmp_srunx_db
    service, _, subscriptions, endpoints = _build_service(conn)

    endpoint_id = endpoints.create(
        "slack_webhook", "default", {"webhook_url": "https://example/x"}
    )
    watch_id = service.create_watch_for_sweep_run(
        sweep_run_id=9, endpoint_id=endpoint_id
    )
    subs = subscriptions.list_by_watch(watch_id)
    assert len(subs) == 1
    assert subs[0].preset == "terminal"
