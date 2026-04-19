"""Tests for :class:`srunx.db.repositories.events.EventRepository`."""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from pathlib import Path

import pytest

from srunx.db.connection import open_connection
from srunx.db.migrations import apply_migrations
from srunx.db.repositories.events import EventRepository


@pytest.fixture
def conn(tmp_path: Path) -> Iterator[sqlite3.Connection]:
    db = tmp_path / "srunx.db"
    connection = open_connection(db)
    apply_migrations(connection)
    try:
        yield connection
    finally:
        connection.close()


@pytest.fixture
def repo(conn: sqlite3.Connection) -> EventRepository:
    return EventRepository(conn)


# ---------------------------------------------------------------------------
# _compute_payload_hash
# ---------------------------------------------------------------------------


def test_compute_payload_hash_is_deterministic() -> None:
    h1 = EventRepository._compute_payload_hash(
        "job.status_changed",
        "job:12345",
        {"from_status": "PENDING", "to_status": "RUNNING"},
    )
    h2 = EventRepository._compute_payload_hash(
        "job.status_changed",
        "job:12345",
        {"from_status": "PENDING", "to_status": "RUNNING"},
    )
    assert h1 == h2
    # Sanity: SHA-256 hex digest is 64 chars
    assert len(h1) == 64


def test_compute_payload_hash_same_status_stable_across_extra_fields() -> None:
    """Hash only uses the logical key — extra payload fields don't change it.

    The design chose a logical-key hash (not a full-payload hash) so
    that the same transition observed twice with slightly different
    metadata still dedupes.
    """
    h1 = EventRepository._compute_payload_hash(
        "job.status_changed",
        "job:42",
        {"to_status": "COMPLETED"},
    )
    h2 = EventRepository._compute_payload_hash(
        "job.status_changed",
        "job:42",
        {"to_status": "COMPLETED", "extra": "ignored"},
    )
    assert h1 == h2


def test_compute_payload_hash_differs_across_kinds() -> None:
    payload = {"to_status": "COMPLETED"}
    h_status = EventRepository._compute_payload_hash(
        "job.status_changed", "job:1", payload
    )
    h_submitted = EventRepository._compute_payload_hash(
        "job.submitted", "job:1", payload
    )
    h_workflow = EventRepository._compute_payload_hash(
        "workflow_run.status_changed", "workflow_run:1", payload
    )
    assert h_status != h_submitted
    assert h_status != h_workflow
    assert h_submitted != h_workflow


def test_compute_payload_hash_differs_across_to_status() -> None:
    h_running = EventRepository._compute_payload_hash(
        "job.status_changed", "job:5", {"to_status": "RUNNING"}
    )
    h_completed = EventRepository._compute_payload_hash(
        "job.status_changed", "job:5", {"to_status": "COMPLETED"}
    )
    assert h_running != h_completed


def test_compute_payload_hash_resource_threshold() -> None:
    h1 = EventRepository._compute_payload_hash(
        "resource.threshold_crossed",
        "resource:gpu",
        {
            "partition": "gpu",
            "threshold_id": "t1",
            "window_iso": "2026-04-18T00:00:00Z",
        },
    )
    h2 = EventRepository._compute_payload_hash(
        "resource.threshold_crossed",
        "resource:gpu",
        {
            "partition": "gpu",
            "threshold_id": "t1",
            "window_iso": "2026-04-18T00:00:00Z",
        },
    )
    h3 = EventRepository._compute_payload_hash(
        "resource.threshold_crossed",
        "resource:gpu",
        {
            "partition": "gpu",
            "threshold_id": "t1",
            "window_iso": "2026-04-18T00:05:00Z",  # different window
        },
    )
    assert h1 == h2
    assert h1 != h3


def test_compute_payload_hash_scheduled_report() -> None:
    h1 = EventRepository._compute_payload_hash(
        "scheduled_report.due",
        "scheduled_report:99",
        {
            "schedule_id": "99",
            "scheduled_run_at_iso": "2026-04-18T00:00:00Z",
        },
    )
    h2 = EventRepository._compute_payload_hash(
        "scheduled_report.due",
        "scheduled_report:99",
        {
            "schedule_id": "99",
            "scheduled_run_at_iso": "2026-04-18T00:00:00Z",
        },
    )
    assert h1 == h2


def test_compute_payload_hash_unknown_kind_uses_fallback() -> None:
    # Two calls with the same dict content must collide; reordering
    # keys must not matter because json.dumps uses sort_keys=True.
    h1 = EventRepository._compute_payload_hash("unknown.kind", "x:1", {"a": 1, "b": 2})
    h2 = EventRepository._compute_payload_hash("unknown.kind", "x:1", {"b": 2, "a": 1})
    assert h1 == h2


# ---------------------------------------------------------------------------
# insert
# ---------------------------------------------------------------------------


def test_insert_returns_positive_id(repo: EventRepository) -> None:
    event_id = repo.insert(
        "job.submitted",
        "job:1001",
        {"job_id": 1001},
    )
    assert event_id is not None
    assert event_id > 0


def test_insert_round_trips_payload_json(repo: EventRepository) -> None:
    payload = {"from_status": "PENDING", "to_status": "RUNNING", "extra": [1, 2, 3]}
    event_id = repo.insert("job.status_changed", "job:7", payload)
    assert event_id is not None

    loaded = repo.get(event_id)
    assert loaded is not None
    assert loaded.payload == payload
    assert loaded.kind == "job.status_changed"
    assert loaded.source_ref == "job:7"


def test_insert_duplicate_returns_none(repo: EventRepository) -> None:
    payload = {"to_status": "COMPLETED"}
    first = repo.insert("job.status_changed", "job:42", payload)
    second = repo.insert("job.status_changed", "job:42", payload)
    assert first is not None
    assert second is None


def test_insert_different_to_status_not_duplicate(repo: EventRepository) -> None:
    first = repo.insert("job.status_changed", "job:42", {"to_status": "RUNNING"})
    second = repo.insert("job.status_changed", "job:42", {"to_status": "COMPLETED"})
    assert first is not None
    assert second is not None
    assert first != second


def test_insert_same_kind_different_source_ref_not_duplicate(
    repo: EventRepository,
) -> None:
    first = repo.insert("job.status_changed", "job:1", {"to_status": "RUNNING"})
    second = repo.insert("job.status_changed", "job:2", {"to_status": "RUNNING"})
    assert first is not None
    assert second is not None
    assert first != second


def test_insert_uses_provided_observed_at(repo: EventRepository) -> None:
    event_id = repo.insert(
        "job.submitted",
        "job:900",
        {"job_id": 900},
        observed_at="2026-04-18T12:00:00.000Z",
    )
    assert event_id is not None
    loaded = repo.get(event_id)
    assert loaded is not None
    assert loaded.observed_at.year == 2026
    assert loaded.observed_at.month == 4


# ---------------------------------------------------------------------------
# get / list_recent
# ---------------------------------------------------------------------------


def test_get_missing_returns_none(repo: EventRepository) -> None:
    assert repo.get(9999) is None


def test_list_recent_orders_by_observed_at_desc(repo: EventRepository) -> None:
    repo.insert(
        "job.submitted",
        "job:1",
        {"job_id": 1},
        observed_at="2026-04-18T00:00:00.000Z",
    )
    repo.insert(
        "job.submitted",
        "job:2",
        {"job_id": 2},
        observed_at="2026-04-18T00:01:00.000Z",
    )
    repo.insert(
        "job.submitted",
        "job:3",
        {"job_id": 3},
        observed_at="2026-04-18T00:02:00.000Z",
    )

    recent = repo.list_recent(limit=10)
    assert [e.source_ref for e in recent] == ["job:3", "job:2", "job:1"]


def test_list_recent_respects_limit(repo: EventRepository) -> None:
    for i in range(5):
        repo.insert(
            "job.submitted",
            f"job:{i}",
            {"job_id": i},
        )

    recent = repo.list_recent(limit=2)
    assert len(recent) == 2


def test_list_recent_empty(repo: EventRepository) -> None:
    assert repo.list_recent() == []
