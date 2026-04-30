"""Tests for ``srunx.observability.storage.repositories.watches``."""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from pathlib import Path

import pytest

from srunx.observability.storage.connection import open_connection
from srunx.observability.storage.migrations import apply_migrations
from srunx.observability.storage.repositories.watches import WatchRepository


@pytest.fixture()
def conn(tmp_path: Path) -> Iterator[sqlite3.Connection]:
    db = tmp_path / "srunx.observability.storage"
    c = open_connection(db)
    apply_migrations(c)
    try:
        yield c
    finally:
        c.close()


def test_create_and_get_roundtrip_with_filter(conn: sqlite3.Connection) -> None:
    repo = WatchRepository(conn)
    row_id = repo.create("job", "job:42", filter={"min_duration": 60})
    conn.commit()

    got = repo.get(row_id)
    assert got is not None
    assert got.kind == "job"
    assert got.target_ref == "job:42"
    assert got.filter == {"min_duration": 60}
    assert got.created_at is not None
    assert got.closed_at is None


def test_create_without_filter(conn: sqlite3.Connection) -> None:
    repo = WatchRepository(conn)
    row_id = repo.create("workflow_run", "workflow_run:1")
    conn.commit()

    got = repo.get(row_id)
    assert got is not None
    assert got.filter is None


def test_get_returns_none_for_missing_row(conn: sqlite3.Connection) -> None:
    repo = WatchRepository(conn)
    assert repo.get(999) is None


def test_create_rejects_invalid_kind_via_check_constraint(
    conn: sqlite3.Connection,
) -> None:
    repo = WatchRepository(conn)
    with pytest.raises(sqlite3.IntegrityError):
        repo.create("bogus_kind", "job:1")


def test_list_open_returns_only_open_watches(conn: sqlite3.Connection) -> None:
    repo = WatchRepository(conn)
    open1 = repo.create("job", "job:1")
    to_close = repo.create("job", "job:2")
    open2 = repo.create("workflow_run", "workflow_run:1")
    repo.close(to_close)
    conn.commit()

    rows = repo.list_open()
    ids = [w.id for w in rows]
    assert open1 in ids
    assert open2 in ids
    assert to_close not in ids


def test_list_open_ordered_ascending_by_created_at(conn: sqlite3.Connection) -> None:
    repo = WatchRepository(conn)
    first = repo.create("job", "job:1")
    second = repo.create("job", "job:2")
    third = repo.create("job", "job:3")
    conn.commit()

    rows = repo.list_open()
    assert [w.id for w in rows] == [first, second, third]


def test_list_by_target_only_open_by_default(conn: sqlite3.Connection) -> None:
    repo = WatchRepository(conn)
    open_id = repo.create("job", "job:7")
    closed_id = repo.create("job", "job:7")
    repo.close(closed_id)
    other = repo.create("job", "job:8")
    conn.commit()

    rows = repo.list_by_target("job", "job:7")
    ids = [w.id for w in rows]
    assert ids == [open_id]

    # Sanity: other target_ref not returned
    assert all(w.id != other for w in rows)


def test_list_by_target_includes_closed_when_requested(
    conn: sqlite3.Connection,
) -> None:
    repo = WatchRepository(conn)
    open_id = repo.create("job", "job:7")
    closed_id = repo.create("job", "job:7")
    repo.close(closed_id)
    conn.commit()

    rows = repo.list_by_target("job", "job:7", only_open=False)
    assert {w.id for w in rows} == {open_id, closed_id}


def test_close_sets_closed_at(conn: sqlite3.Connection) -> None:
    repo = WatchRepository(conn)
    row_id = repo.create("job", "job:1")
    conn.commit()
    assert repo.close(row_id) is True
    conn.commit()

    got = repo.get(row_id)
    assert got is not None
    assert got.closed_at is not None


def test_close_missing_row_returns_false(conn: sqlite3.Connection) -> None:
    repo = WatchRepository(conn)
    assert repo.close(9999) is False


def test_close_does_not_delete_watch_or_subscriptions(
    conn: sqlite3.Connection,
) -> None:
    """Closing a watch must leave the row and its subscriptions intact.

    Watches are lifecycle-managed via ``closed_at``; CASCADE-deletion only
    fires on explicit DELETE of the watch row.
    """
    repo = WatchRepository(conn)
    watch_id = repo.create("job", "job:1")

    # Seed an endpoint + subscription via raw SQL
    conn.execute(
        "INSERT INTO endpoints (kind, name, config, created_at) VALUES (?, ?, ?, ?)",
        ("slack_webhook", "ep", "{}", "2026-04-18T00:00:00.000Z"),
    )
    endpoint_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    conn.execute(
        "INSERT INTO subscriptions (watch_id, endpoint_id, preset, created_at) "
        "VALUES (?, ?, ?, ?)",
        (watch_id, endpoint_id, "terminal", "2026-04-18T00:00:00.000Z"),
    )
    conn.commit()

    assert repo.close(watch_id) is True
    conn.commit()

    # Watch row still present
    assert repo.get(watch_id) is not None
    # Subscription still present
    sub_count = conn.execute(
        "SELECT COUNT(*) FROM subscriptions WHERE watch_id = ?",
        (watch_id,),
    ).fetchone()[0]
    assert sub_count == 1
