"""Tests for ``srunx.db.repositories.endpoints``."""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from pathlib import Path

import pytest

from srunx.db.connection import open_connection
from srunx.db.migrations import apply_migrations
from srunx.db.repositories.endpoints import EndpointRepository


@pytest.fixture()
def conn(tmp_path: Path) -> Iterator[sqlite3.Connection]:
    db = tmp_path / "srunx.db"
    c = open_connection(db)
    apply_migrations(c)
    try:
        yield c
    finally:
        c.close()


def test_create_and_get_roundtrip(conn: sqlite3.Connection) -> None:
    repo = EndpointRepository(conn)
    row_id = repo.create(
        "slack_webhook",
        "default",
        {"webhook_url": "https://hooks.slack.com/services/A/B/C"},
    )
    conn.commit()

    got = repo.get(row_id)
    assert got is not None
    assert got.id == row_id
    assert got.kind == "slack_webhook"
    assert got.name == "default"
    assert got.config == {"webhook_url": "https://hooks.slack.com/services/A/B/C"}
    assert got.created_at is not None
    assert got.disabled_at is None


def test_get_returns_none_for_missing_row(conn: sqlite3.Connection) -> None:
    repo = EndpointRepository(conn)
    assert repo.get(99999) is None


def test_get_by_name_returns_endpoint(conn: sqlite3.Connection) -> None:
    repo = EndpointRepository(conn)
    repo.create("slack_webhook", "default", {"webhook_url": "https://example.com"})
    conn.commit()

    got = repo.get_by_name("slack_webhook", "default")
    assert got is not None
    assert got.name == "default"

    miss = repo.get_by_name("slack_webhook", "nope")
    assert miss is None


def test_create_unique_violation_on_duplicate_kind_name(
    conn: sqlite3.Connection,
) -> None:
    repo = EndpointRepository(conn)
    repo.create("slack_webhook", "default", {"webhook_url": "https://example.com"})
    conn.commit()

    with pytest.raises(sqlite3.IntegrityError):
        repo.create("slack_webhook", "default", {"webhook_url": "https://other"})


def test_list_respects_include_disabled(conn: sqlite3.Connection) -> None:
    repo = EndpointRepository(conn)
    active_id = repo.create("slack_webhook", "active", {"webhook_url": "a"})
    disabled_id = repo.create("slack_webhook", "old", {"webhook_url": "b"})
    repo.disable(disabled_id)
    conn.commit()

    all_rows = repo.list(include_disabled=True)
    assert {e.id for e in all_rows} == {active_id, disabled_id}

    active_only = repo.list(include_disabled=False)
    assert [e.id for e in active_only] == [active_id]


def test_list_orders_by_created_at(conn: sqlite3.Connection) -> None:
    repo = EndpointRepository(conn)
    first = repo.create("slack_webhook", "first", {"u": "a"})
    second = repo.create("slack_webhook", "second", {"u": "b"})
    conn.commit()

    rows = repo.list()
    ids = [e.id for e in rows]
    assert ids.index(first) < ids.index(second)


def test_update_partial_fields(conn: sqlite3.Connection) -> None:
    repo = EndpointRepository(conn)
    row_id = repo.create("slack_webhook", "name1", {"webhook_url": "a"})
    conn.commit()

    assert repo.update(row_id, name="name2") is True
    conn.commit()
    got = repo.get(row_id)
    assert got is not None
    assert got.name == "name2"
    assert got.config == {"webhook_url": "a"}

    assert repo.update(row_id, config={"webhook_url": "b"}) is True
    conn.commit()
    got = repo.get(row_id)
    assert got is not None
    assert got.config == {"webhook_url": "b"}


def test_update_no_fields_returns_false(conn: sqlite3.Connection) -> None:
    repo = EndpointRepository(conn)
    row_id = repo.create("slack_webhook", "x", {"u": "a"})
    conn.commit()
    assert repo.update(row_id) is False


def test_update_missing_row_returns_false(conn: sqlite3.Connection) -> None:
    repo = EndpointRepository(conn)
    assert repo.update(999, name="nope") is False


def test_disable_and_enable(conn: sqlite3.Connection) -> None:
    repo = EndpointRepository(conn)
    row_id = repo.create("slack_webhook", "x", {"u": "a"})
    conn.commit()

    assert repo.disable(row_id) is True
    conn.commit()
    got = repo.get(row_id)
    assert got is not None
    assert got.disabled_at is not None

    assert repo.enable(row_id) is True
    conn.commit()
    got = repo.get(row_id)
    assert got is not None
    assert got.disabled_at is None


def test_disable_missing_row_returns_false(conn: sqlite3.Connection) -> None:
    repo = EndpointRepository(conn)
    assert repo.disable(999) is False


def test_enable_missing_row_returns_false(conn: sqlite3.Connection) -> None:
    repo = EndpointRepository(conn)
    assert repo.enable(999) is False


def test_delete_removes_row(conn: sqlite3.Connection) -> None:
    repo = EndpointRepository(conn)
    row_id = repo.create("slack_webhook", "x", {"u": "a"})
    conn.commit()

    assert repo.delete(row_id) is True
    conn.commit()
    assert repo.get(row_id) is None


def test_delete_missing_row_returns_false(conn: sqlite3.Connection) -> None:
    repo = EndpointRepository(conn)
    assert repo.delete(999) is False


def test_delete_cascades_to_subscriptions(conn: sqlite3.Connection) -> None:
    """Endpoint delete must CASCADE-delete related subscriptions."""
    repo = EndpointRepository(conn)
    endpoint_id = repo.create("slack_webhook", "x", {"u": "a"})

    # Create a watch + subscription referencing this endpoint via raw SQL
    # (we intentionally don't import sibling repos here to keep the test
    # focused on EndpointRepository behavior).
    conn.execute(
        "INSERT INTO watches (kind, target_ref, created_at) VALUES (?, ?, ?)",
        ("job", "job:1", "2026-04-18T00:00:00.000Z"),
    )
    watch_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    conn.execute(
        "INSERT INTO subscriptions (watch_id, endpoint_id, preset, created_at) "
        "VALUES (?, ?, ?, ?)",
        (watch_id, endpoint_id, "terminal", "2026-04-18T00:00:00.000Z"),
    )
    conn.commit()

    assert repo.delete(endpoint_id) is True
    conn.commit()

    remaining = conn.execute(
        "SELECT COUNT(*) FROM subscriptions WHERE endpoint_id = ?",
        (endpoint_id,),
    ).fetchone()[0]
    assert remaining == 0
