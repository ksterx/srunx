"""Tests for ``srunx.db.repositories.subscriptions``."""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from pathlib import Path

import pytest
from srunx.db.connection import open_connection
from srunx.db.migrations import apply_migrations
from srunx.db.repositories.subscriptions import SubscriptionRepository


@pytest.fixture()
def conn(tmp_path: Path) -> Iterator[sqlite3.Connection]:
    db = tmp_path / "srunx.db"
    c = open_connection(db)
    apply_migrations(c)
    try:
        yield c
    finally:
        c.close()


def _seed_endpoint(conn: sqlite3.Connection, name: str = "ep") -> int:
    conn.execute(
        "INSERT INTO endpoints (kind, name, config, created_at) VALUES (?, ?, ?, ?)",
        ("slack_webhook", name, "{}", "2026-04-18T00:00:00.000Z"),
    )
    row = conn.execute("SELECT last_insert_rowid()").fetchone()
    return int(row[0])


def _seed_watch(conn: sqlite3.Connection, target_ref: str = "job:1") -> int:
    conn.execute(
        "INSERT INTO watches (kind, target_ref, created_at) VALUES (?, ?, ?)",
        ("job", target_ref, "2026-04-18T00:00:00.000Z"),
    )
    row = conn.execute("SELECT last_insert_rowid()").fetchone()
    return int(row[0])


def test_create_and_get_roundtrip(conn: sqlite3.Connection) -> None:
    endpoint_id = _seed_endpoint(conn)
    watch_id = _seed_watch(conn)
    conn.commit()

    repo = SubscriptionRepository(conn)
    sub_id = repo.create(watch_id, endpoint_id, "terminal")
    conn.commit()

    got = repo.get(sub_id)
    assert got is not None
    assert got.watch_id == watch_id
    assert got.endpoint_id == endpoint_id
    assert got.preset == "terminal"
    assert got.created_at is not None


def test_get_returns_none_for_missing_row(conn: sqlite3.Connection) -> None:
    repo = SubscriptionRepository(conn)
    assert repo.get(999) is None


def test_create_rejects_invalid_preset(conn: sqlite3.Connection) -> None:
    endpoint_id = _seed_endpoint(conn)
    watch_id = _seed_watch(conn)
    conn.commit()

    repo = SubscriptionRepository(conn)
    with pytest.raises(sqlite3.IntegrityError):
        repo.create(watch_id, endpoint_id, "bogus")


def test_create_rejects_duplicate_watch_endpoint_pair(
    conn: sqlite3.Connection,
) -> None:
    endpoint_id = _seed_endpoint(conn)
    watch_id = _seed_watch(conn)
    conn.commit()

    repo = SubscriptionRepository(conn)
    repo.create(watch_id, endpoint_id, "terminal")
    conn.commit()

    with pytest.raises(sqlite3.IntegrityError):
        repo.create(watch_id, endpoint_id, "all")


def test_create_fails_on_unknown_watch_id_fk(conn: sqlite3.Connection) -> None:
    endpoint_id = _seed_endpoint(conn)
    conn.commit()

    repo = SubscriptionRepository(conn)
    with pytest.raises(sqlite3.IntegrityError):
        repo.create(9999, endpoint_id, "terminal")
        conn.commit()


def test_list_by_watch_returns_only_matching(conn: sqlite3.Connection) -> None:
    e1 = _seed_endpoint(conn, name="a")
    e2 = _seed_endpoint(conn, name="b")
    w1 = _seed_watch(conn, target_ref="job:1")
    w2 = _seed_watch(conn, target_ref="job:2")
    conn.commit()

    repo = SubscriptionRepository(conn)
    s1 = repo.create(w1, e1, "terminal")
    s2 = repo.create(w1, e2, "all")
    other = repo.create(w2, e1, "terminal")
    conn.commit()

    rows = repo.list_by_watch(w1)
    ids = {s.id for s in rows}
    assert ids == {s1, s2}
    assert other not in ids


def test_list_by_endpoint_returns_only_matching(conn: sqlite3.Connection) -> None:
    e1 = _seed_endpoint(conn, name="a")
    e2 = _seed_endpoint(conn, name="b")
    w1 = _seed_watch(conn, target_ref="job:1")
    w2 = _seed_watch(conn, target_ref="job:2")
    conn.commit()

    repo = SubscriptionRepository(conn)
    s1 = repo.create(w1, e1, "terminal")
    s2 = repo.create(w2, e1, "all")
    other = repo.create(w1, e2, "terminal")
    conn.commit()

    rows = repo.list_by_endpoint(e1)
    ids = {s.id for s in rows}
    assert ids == {s1, s2}
    assert other not in ids


def test_list_empty_for_unknown_parent(conn: sqlite3.Connection) -> None:
    repo = SubscriptionRepository(conn)
    assert repo.list_by_watch(9999) == []
    assert repo.list_by_endpoint(9999) == []


def test_delete_removes_row(conn: sqlite3.Connection) -> None:
    endpoint_id = _seed_endpoint(conn)
    watch_id = _seed_watch(conn)
    conn.commit()

    repo = SubscriptionRepository(conn)
    sub_id = repo.create(watch_id, endpoint_id, "terminal")
    conn.commit()

    assert repo.delete(sub_id) is True
    conn.commit()
    assert repo.get(sub_id) is None


def test_delete_missing_row_returns_false(conn: sqlite3.Connection) -> None:
    repo = SubscriptionRepository(conn)
    assert repo.delete(999) is False
