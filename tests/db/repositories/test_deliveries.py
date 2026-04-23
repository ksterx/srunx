"""Tests for :class:`srunx.db.repositories.deliveries.DeliveryRepository`.

Covers the outbox claim/lease mechanics (the hot path of the delivery
poller) plus the straightforward CRUD + state-transition methods.
"""

from __future__ import annotations

import sqlite3
import time
from collections.abc import Iterator
from pathlib import Path

import pytest

from srunx.db.connection import open_connection
from srunx.db.migrations import apply_migrations
from srunx.db.repositories.base import now_iso
from srunx.db.repositories.deliveries import DeliveryRepository

# ---------------------------------------------------------------------------
# Fixtures: seed an endpoint + watch + subscription + event so we can insert
# deliveries against valid FKs. All helpers write rows directly via SQL to
# keep these tests isolated from the other repositories.
# ---------------------------------------------------------------------------


@pytest.fixture
def conn(tmp_path: Path) -> Iterator[sqlite3.Connection]:
    db = tmp_path / "srunx.db"
    connection = open_connection(db)
    apply_migrations(connection)
    try:
        yield connection
    finally:
        connection.close()


def _seed_endpoint(
    conn: sqlite3.Connection,
    *,
    name: str = "default",
    disabled: bool = False,
) -> int:
    disabled_at = now_iso() if disabled else None
    cur = conn.execute(
        "INSERT INTO endpoints (kind, name, config, created_at, disabled_at) "
        "VALUES (?, ?, ?, ?, ?)",
        ("slack_webhook", name, "{}", now_iso(), disabled_at),
    )
    conn.commit()
    return int(cur.lastrowid or 0)


def _seed_watch(conn: sqlite3.Connection, target_ref: str = "job:1") -> int:
    cur = conn.execute(
        "INSERT INTO watches (kind, target_ref, filter, created_at) "
        "VALUES (?, ?, NULL, ?)",
        ("job", target_ref, now_iso()),
    )
    conn.commit()
    return int(cur.lastrowid or 0)


def _seed_subscription(
    conn: sqlite3.Connection,
    watch_id: int,
    endpoint_id: int,
    preset: str = "terminal",
) -> int:
    cur = conn.execute(
        "INSERT INTO subscriptions (watch_id, endpoint_id, preset, created_at) "
        "VALUES (?, ?, ?, ?)",
        (watch_id, endpoint_id, preset, now_iso()),
    )
    conn.commit()
    return int(cur.lastrowid or 0)


def _seed_event(
    conn: sqlite3.Connection,
    *,
    source_ref: str = "job:1",
    payload_hash: str = "hash-1",
) -> int:
    cur = conn.execute(
        "INSERT INTO events (kind, source_ref, payload, payload_hash, observed_at) "
        "VALUES (?, ?, ?, ?, ?)",
        (
            "job.status_changed",
            source_ref,
            '{"to_status": "COMPLETED"}',
            payload_hash,
            now_iso(),
        ),
    )
    conn.commit()
    return int(cur.lastrowid or 0)


@pytest.fixture
def setup_fks(conn: sqlite3.Connection) -> dict[str, int]:
    """Return a dict of seeded FK ids usable by the tests."""
    endpoint_id = _seed_endpoint(conn)
    watch_id = _seed_watch(conn)
    subscription_id = _seed_subscription(conn, watch_id, endpoint_id)
    event_id = _seed_event(conn)
    return {
        "endpoint_id": endpoint_id,
        "watch_id": watch_id,
        "subscription_id": subscription_id,
        "event_id": event_id,
    }


@pytest.fixture
def repo(conn: sqlite3.Connection) -> DeliveryRepository:
    return DeliveryRepository(conn)


# ---------------------------------------------------------------------------
# _backoff_secs truth table
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "attempt_count,expected",
    [
        (0, 10),
        (1, 20),
        (2, 40),
        (3, 80),
        (4, 160),
        (10, 3600),  # capped
        (100, 3600),  # still capped
    ],
)
def test_backoff_secs_truth_table(attempt_count: int, expected: int) -> None:
    assert DeliveryRepository._backoff_secs(attempt_count) == expected


def test_backoff_secs_custom_base_and_cap() -> None:
    assert DeliveryRepository._backoff_secs(0, base=5, factor=3, cap=100) == 5
    assert DeliveryRepository._backoff_secs(1, base=5, factor=3, cap=100) == 15
    assert DeliveryRepository._backoff_secs(10, base=5, factor=3, cap=100) == 100


# ---------------------------------------------------------------------------
# insert
# ---------------------------------------------------------------------------


def test_insert_returns_positive_id(
    repo: DeliveryRepository, setup_fks: dict[str, int]
) -> None:
    delivery_id = repo.insert(
        setup_fks["event_id"],
        setup_fks["subscription_id"],
        setup_fks["endpoint_id"],
        "job:1:status:COMPLETED",
    )
    assert delivery_id is not None
    assert delivery_id > 0


def test_insert_sets_pending_defaults(
    repo: DeliveryRepository, setup_fks: dict[str, int]
) -> None:
    delivery_id = repo.insert(
        setup_fks["event_id"],
        setup_fks["subscription_id"],
        setup_fks["endpoint_id"],
        "job:1:status:COMPLETED",
    )
    assert delivery_id is not None
    loaded = repo.get(delivery_id)
    assert loaded is not None
    assert loaded.status == "pending"
    assert loaded.attempt_count == 0
    assert loaded.leased_until is None
    assert loaded.worker_id is None
    assert loaded.last_error is None
    assert loaded.delivered_at is None


def test_insert_duplicate_idempotency_key_returns_none(
    repo: DeliveryRepository,
    setup_fks: dict[str, int],
    conn: sqlite3.Connection,
) -> None:
    first = repo.insert(
        setup_fks["event_id"],
        setup_fks["subscription_id"],
        setup_fks["endpoint_id"],
        "dup-key",
    )
    # Add a second event so we can try to insert a second delivery row
    # against the same endpoint + idempotency key — the UNIQUE
    # (endpoint_id, idempotency_key) should absorb it.
    other_event = _seed_event(conn, source_ref="job:2", payload_hash="hash-2")
    second = repo.insert(
        other_event,
        setup_fks["subscription_id"],
        setup_fks["endpoint_id"],
        "dup-key",
    )
    assert first is not None
    assert second is None


def test_insert_accepts_explicit_next_attempt_at(
    repo: DeliveryRepository, setup_fks: dict[str, int]
) -> None:
    delivery_id = repo.insert(
        setup_fks["event_id"],
        setup_fks["subscription_id"],
        setup_fks["endpoint_id"],
        "k",
        next_attempt_at="2099-01-01T00:00:00.000Z",
    )
    assert delivery_id is not None
    loaded = repo.get(delivery_id)
    assert loaded is not None
    assert loaded.next_attempt_at.year == 2099


# ---------------------------------------------------------------------------
# get / list_by_subscription
# ---------------------------------------------------------------------------


def test_get_missing_returns_none(repo: DeliveryRepository) -> None:
    assert repo.get(9999) is None


def test_list_by_subscription_filters_and_orders(
    repo: DeliveryRepository,
    setup_fks: dict[str, int],
    conn: sqlite3.Connection,
) -> None:
    repo.insert(
        setup_fks["event_id"],
        setup_fks["subscription_id"],
        setup_fks["endpoint_id"],
        "k1",
    )
    time.sleep(0.02)
    other_event = _seed_event(conn, source_ref="job:3", payload_hash="hash-3")
    repo.insert(
        other_event,
        setup_fks["subscription_id"],
        setup_fks["endpoint_id"],
        "k2",
    )

    rows = repo.list_by_subscription(setup_fks["subscription_id"])
    assert len(rows) == 2
    # newest first by created_at — the timestamps are millisecond-precision
    # ISO strings so a 20 ms gap guarantees a stable lexicographic order.
    idempotency_keys = {r.idempotency_key for r in rows}
    assert idempotency_keys == {"k1", "k2"}
    # And ORDER BY created_at DESC should put k2 first.
    assert rows[0].idempotency_key == "k2"
    assert rows[1].idempotency_key == "k1"

    # filter by status
    pending_rows = repo.list_by_subscription(
        setup_fks["subscription_id"], status="pending"
    )
    assert len(pending_rows) == 2

    delivered_rows = repo.list_by_subscription(
        setup_fks["subscription_id"], status="delivered"
    )
    assert delivered_rows == []


def test_list_recent_returns_all_subscriptions(
    repo: DeliveryRepository,
    setup_fks: dict[str, int],
    conn: sqlite3.Connection,
) -> None:
    """``list_recent`` returns deliveries across subscriptions, newest first."""
    repo.insert(
        setup_fks["event_id"],
        setup_fks["subscription_id"],
        setup_fks["endpoint_id"],
        "k1",
    )
    time.sleep(0.02)
    other_event = _seed_event(conn, source_ref="job:99", payload_hash="hash-99")
    other_watch = _seed_watch(conn, target_ref="job:99")
    other_sub = _seed_subscription(conn, other_watch, setup_fks["endpoint_id"])
    repo.insert(other_event, other_sub, setup_fks["endpoint_id"], "k2")

    rows = repo.list_recent()
    assert len(rows) == 2
    assert rows[0].idempotency_key == "k2"  # newest first
    assert rows[1].idempotency_key == "k1"


def test_list_recent_respects_status_and_limit(
    repo: DeliveryRepository,
    setup_fks: dict[str, int],
) -> None:
    for i in range(5):
        repo.insert(
            setup_fks["event_id"],
            setup_fks["subscription_id"],
            setup_fks["endpoint_id"],
            f"key-{i}",
        )
    assert len(repo.list_recent(limit=3)) == 3
    assert repo.list_recent(status="delivered") == []
    assert len(repo.list_recent(status="pending")) == 5


# ---------------------------------------------------------------------------
# claim_one
# ---------------------------------------------------------------------------


def test_claim_one_marks_sending(
    repo: DeliveryRepository,
    setup_fks: dict[str, int],
    conn: sqlite3.Connection,
) -> None:
    delivery_id = repo.insert(
        setup_fks["event_id"],
        setup_fks["subscription_id"],
        setup_fks["endpoint_id"],
        "k",
    )
    assert delivery_id is not None
    conn.commit()  # close the implicit Python transaction before claim_one

    claimed = repo.claim_one(worker_id="worker-1", lease_duration_secs=300)
    assert claimed is not None
    assert claimed.id == delivery_id
    assert claimed.status == "sending"
    assert claimed.worker_id == "worker-1"
    assert claimed.leased_until is not None


def test_claim_one_returns_none_when_no_pending(repo: DeliveryRepository) -> None:
    assert repo.claim_one(worker_id="worker-1") is None


def test_claim_one_skips_disabled_endpoints(
    repo: DeliveryRepository, conn: sqlite3.Connection
) -> None:
    disabled_endpoint = _seed_endpoint(conn, name="disabled-ep", disabled=True)
    watch_id = _seed_watch(conn, target_ref="job:50")
    sub_id = _seed_subscription(conn, watch_id, disabled_endpoint)
    event_id = _seed_event(conn, source_ref="job:50", payload_hash="disabled-hash")

    delivery_id = repo.insert(event_id, sub_id, disabled_endpoint, "k-disabled")
    assert delivery_id is not None
    conn.commit()

    # claim_one must skip this because the endpoint is disabled
    assert repo.claim_one(worker_id="worker-1") is None


def test_claim_one_skips_future_next_attempt_at(
    repo: DeliveryRepository,
    setup_fks: dict[str, int],
    conn: sqlite3.Connection,
) -> None:
    delivery_id = repo.insert(
        setup_fks["event_id"],
        setup_fks["subscription_id"],
        setup_fks["endpoint_id"],
        "future-k",
        next_attempt_at="2099-01-01T00:00:00.000Z",
    )
    assert delivery_id is not None
    conn.commit()
    assert repo.claim_one(worker_id="worker-1") is None


def test_claim_one_concurrent_second_connection_misses(
    tmp_path: Path, setup_fks: dict[str, int], conn: sqlite3.Connection
) -> None:
    """When the same pending row is visible to two connections, only one
    claim_one call should return it; the other should return ``None``.

    We can't run the two calls truly in parallel inside a single pytest
    process without threads, but the correctness invariant is that the
    UPDATE's ``WHERE status = 'pending'`` clause will fail for any
    worker that races in after another worker already flipped the row
    to ``sending``. To exercise that path we emulate the race by:

    1. inserting one pending delivery on connection A
    2. opening a second connection B to the same on-disk DB (WAL mode
       with busy_timeout PRAGMAs from ``open_connection``)
    3. calling claim_one() from connection A (commits the flip)
    4. calling claim_one() from connection B (must see status='sending'
       and return None)

    This verifies both that the SELECT+UPDATE+RETURNING pattern is
    visible across connections AND that the ``WHERE status='pending'``
    in the UPDATE filters out already-claimed rows.
    """
    # Seed a pending delivery on the shared DB via conn A.
    repo_a = DeliveryRepository(conn)
    delivery_id = repo_a.insert(
        setup_fks["event_id"],
        setup_fks["subscription_id"],
        setup_fks["endpoint_id"],
        "race-k",
    )
    assert delivery_id is not None
    conn.commit()  # make row visible to the second connection + close tx

    # Find the DB path by reading from the connection's pragma;
    # simpler: tmp_path-based fixture doesn't expose it, so reopen
    # against the same path used by ``conn`` fixture. We rebuild it
    # here using the same tmp_path.
    db_path = tmp_path / "srunx.db"
    conn_b = open_connection(db_path)
    try:
        repo_b = DeliveryRepository(conn_b)

        # Worker A wins first.
        claimed_a = repo_a.claim_one(worker_id="worker-A")
        assert claimed_a is not None
        assert claimed_a.id == delivery_id

        # Worker B, running the same claim_one, must not see any
        # pending row any more.
        claimed_b = repo_b.claim_one(worker_id="worker-B")
        assert claimed_b is None
    finally:
        conn_b.close()


# ---------------------------------------------------------------------------
# reclaim_expired_leases
# ---------------------------------------------------------------------------


def test_reclaim_expired_leases_reverts_stale_sending(
    repo: DeliveryRepository,
    setup_fks: dict[str, int],
    conn: sqlite3.Connection,
) -> None:
    # Insert a pending row, then flip it to 'sending' with a leased_until
    # in the past to simulate a crashed worker.
    delivery_id = repo.insert(
        setup_fks["event_id"],
        setup_fks["subscription_id"],
        setup_fks["endpoint_id"],
        "expired-k",
    )
    assert delivery_id is not None
    conn.execute(
        """
        UPDATE deliveries
           SET status = 'sending',
               leased_until = ?,
               worker_id = 'crashed-worker'
         WHERE id = ?
        """,
        ("2000-01-01T00:00:00.000Z", delivery_id),
    )
    conn.commit()

    reclaimed = repo.reclaim_expired_leases()
    assert reclaimed == 1

    loaded = repo.get(delivery_id)
    assert loaded is not None
    assert loaded.status == "pending"
    assert loaded.leased_until is None
    assert loaded.worker_id is None


def test_reclaim_expired_leases_leaves_active_leases_alone(
    repo: DeliveryRepository,
    setup_fks: dict[str, int],
    conn: sqlite3.Connection,
) -> None:
    delivery_id = repo.insert(
        setup_fks["event_id"],
        setup_fks["subscription_id"],
        setup_fks["endpoint_id"],
        "active-k",
    )
    assert delivery_id is not None
    # Future leased_until — active lease.
    conn.execute(
        """
        UPDATE deliveries
           SET status = 'sending',
               leased_until = ?,
               worker_id = 'worker-1'
         WHERE id = ?
        """,
        ("2099-01-01T00:00:00.000Z", delivery_id),
    )
    conn.commit()

    reclaimed = repo.reclaim_expired_leases()
    assert reclaimed == 0

    loaded = repo.get(delivery_id)
    assert loaded is not None
    assert loaded.status == "sending"
    assert loaded.worker_id == "worker-1"


def test_reclaim_expired_leases_empty_returns_zero(
    repo: DeliveryRepository,
) -> None:
    assert repo.reclaim_expired_leases() == 0


# ---------------------------------------------------------------------------
# mark_delivered / mark_retry / mark_abandoned
# ---------------------------------------------------------------------------


def test_mark_delivered_sets_status_and_timestamp(
    repo: DeliveryRepository,
    setup_fks: dict[str, int],
    conn: sqlite3.Connection,
) -> None:
    delivery_id = repo.insert(
        setup_fks["event_id"],
        setup_fks["subscription_id"],
        setup_fks["endpoint_id"],
        "k",
    )
    assert delivery_id is not None
    conn.commit()

    # Lease it first so we can prove mark_delivered clears lease fields.
    claimed = repo.claim_one(worker_id="w")
    assert claimed is not None

    assert repo.mark_delivered(delivery_id) is True

    loaded = repo.get(delivery_id)
    assert loaded is not None
    assert loaded.status == "delivered"
    assert loaded.delivered_at is not None
    assert loaded.leased_until is None
    assert loaded.worker_id is None


def test_mark_delivered_returns_false_on_missing(
    repo: DeliveryRepository,
) -> None:
    assert repo.mark_delivered(9999) is False


def test_mark_retry_increments_attempt_and_schedules_backoff(
    repo: DeliveryRepository,
    setup_fks: dict[str, int],
    conn: sqlite3.Connection,
) -> None:
    delivery_id = repo.insert(
        setup_fks["event_id"],
        setup_fks["subscription_id"],
        setup_fks["endpoint_id"],
        "k",
    )
    assert delivery_id is not None
    conn.commit()

    claimed = repo.claim_one(worker_id="w")
    assert claimed is not None

    assert repo.mark_retry(delivery_id, "HTTP 500", backoff_secs=30) is True
    conn.commit()

    loaded = repo.get(delivery_id)
    assert loaded is not None
    assert loaded.status == "pending"
    assert loaded.attempt_count == 1
    assert loaded.leased_until is None
    assert loaded.worker_id is None
    assert loaded.last_error == "HTTP 500"

    # next_attempt_at was scheduled by backoff_secs=30 — but the second
    # claim needs the row to be due, so rewrite it to "now-ish".
    conn.execute(
        "UPDATE deliveries SET next_attempt_at = "
        "strftime('%Y-%m-%dT%H:%M:%fZ','now') WHERE id = ?",
        (delivery_id,),
    )
    conn.commit()

    # Second retry: attempt_count should compound.
    claimed2 = repo.claim_one(worker_id="w")
    assert claimed2 is not None
    assert repo.mark_retry(delivery_id, "HTTP 502", backoff_secs=60) is True
    loaded2 = repo.get(delivery_id)
    assert loaded2 is not None
    assert loaded2.attempt_count == 2


def test_mark_abandoned_sets_terminal_status(
    repo: DeliveryRepository, setup_fks: dict[str, int]
) -> None:
    delivery_id = repo.insert(
        setup_fks["event_id"],
        setup_fks["subscription_id"],
        setup_fks["endpoint_id"],
        "k",
    )
    assert delivery_id is not None
    assert repo.mark_abandoned(delivery_id, "too many failures") is True

    loaded = repo.get(delivery_id)
    assert loaded is not None
    assert loaded.status == "abandoned"
    assert loaded.last_error == "too many failures"


def test_mark_abandoned_returns_false_on_missing(
    repo: DeliveryRepository,
) -> None:
    assert repo.mark_abandoned(9999, "x") is False


# ---------------------------------------------------------------------------
# count_stuck_pending
# ---------------------------------------------------------------------------


def test_count_stuck_pending_counts_old_rows(
    repo: DeliveryRepository,
    setup_fks: dict[str, int],
    conn: sqlite3.Connection,
) -> None:
    # Row 1: stuck (very old next_attempt_at).
    stuck_id = repo.insert(
        setup_fks["event_id"],
        setup_fks["subscription_id"],
        setup_fks["endpoint_id"],
        "stuck-k",
        next_attempt_at="2000-01-01T00:00:00.000Z",
    )
    assert stuck_id is not None

    # Row 2: fresh pending (next_attempt_at in the near future / now).
    other_event = _seed_event(conn, source_ref="job:99", payload_hash="hash-99")
    fresh_id = repo.insert(
        other_event,
        setup_fks["subscription_id"],
        setup_fks["endpoint_id"],
        "fresh-k",
    )
    assert fresh_id is not None

    # older_than_sec=60 → only the 2000-era row qualifies.
    assert repo.count_stuck_pending(older_than_sec=60) == 1


def test_count_stuck_pending_empty(repo: DeliveryRepository) -> None:
    assert repo.count_stuck_pending() == 0
