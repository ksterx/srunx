"""Tests for ``srunx.db.connection``."""

from __future__ import annotations

import os
import sqlite3
import stat
from pathlib import Path

import pytest

from srunx.db import connection as conn_mod
from srunx.db.connection import (
    get_config_dir,
    get_db_path,
    init_db,
    open_connection,
    transaction,
)

# ---- Path resolution ----


def test_get_config_dir_uses_xdg_when_set(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path))
    assert get_config_dir() == tmp_path / "srunx"


def test_get_config_dir_falls_back_without_xdg(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("XDG_CONFIG_HOME", raising=False)
    got = get_config_dir()
    # Don't assert exact path (platform-dependent); just assert it ends with
    # 'srunx' and includes the user's home.
    assert got.name == "srunx"
    assert str(Path.home()) in str(got)


def test_get_db_path_is_under_config_dir(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path))
    assert get_db_path() == tmp_path / "srunx" / "srunx.db"


# ---- open_connection ----


def test_open_connection_applies_pragmas(tmp_path: Path) -> None:
    db = tmp_path / "srunx.db"
    conn = open_connection(db)
    try:
        fk = conn.execute("PRAGMA foreign_keys").fetchone()[0]
        journal = conn.execute("PRAGMA journal_mode").fetchone()[0]
        busy = conn.execute("PRAGMA busy_timeout").fetchone()[0]
    finally:
        conn.close()

    assert fk == 1
    assert journal.lower() == "wal"
    assert busy == 5000


def test_open_connection_creates_file_with_mode_0600(tmp_path: Path) -> None:
    db = tmp_path / "sub" / "srunx.db"
    conn = open_connection(db)
    conn.close()
    assert db.exists()
    if os.name == "posix":
        mode = stat.S_IMODE(db.stat().st_mode)
        assert mode == 0o600


def test_open_connection_sets_row_factory(tmp_path: Path) -> None:
    db = tmp_path / "srunx.db"
    conn = open_connection(db)
    try:
        conn.execute("CREATE TABLE t (id INTEGER, name TEXT)")
        conn.execute("INSERT INTO t VALUES (1, 'hello')")
        row = conn.execute("SELECT id, name FROM t").fetchone()
    finally:
        conn.close()

    # sqlite3.Row allows both index and name access.
    assert row["id"] == 1
    assert row["name"] == "hello"


# ---- transaction() ----


def test_transaction_commits_on_success(tmp_path: Path) -> None:
    db = tmp_path / "srunx.db"
    conn = open_connection(db)
    conn.execute("CREATE TABLE t (id INTEGER)")
    with transaction(conn, "IMMEDIATE"):
        conn.execute("INSERT INTO t VALUES (1)")
    rows = conn.execute("SELECT id FROM t").fetchall()
    conn.close()
    assert [r["id"] for r in rows] == [1]


def test_transaction_rollback_on_exception(tmp_path: Path) -> None:
    db = tmp_path / "srunx.db"
    conn = open_connection(db)
    conn.execute("CREATE TABLE t (id INTEGER)")
    with pytest.raises(RuntimeError):
        with transaction(conn):
            conn.execute("INSERT INTO t VALUES (1)")
            raise RuntimeError("boom")
    rows = conn.execute("SELECT id FROM t").fetchall()
    conn.close()
    assert rows == []


def test_transaction_rejects_unknown_mode(tmp_path: Path) -> None:
    db = tmp_path / "srunx.db"
    conn = open_connection(db)
    with pytest.raises(ValueError):
        with transaction(conn, "BOGUS"):
            pass
    conn.close()


# ---- init_db ----


def test_init_db_applies_schema_and_removes_legacy(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path))

    # Seed a fake legacy DB to prove it gets removed.
    legacy_dir = tmp_path / "legacy_home" / ".srunx"
    legacy_dir.mkdir(parents=True)
    legacy_db = legacy_dir / "history.db"
    legacy_db.touch()
    monkeypatch.setattr(conn_mod, "LEGACY_HISTORY_DB_PATH", legacy_db)

    db_path = init_db()
    assert db_path == tmp_path / "srunx" / "srunx.db"
    assert db_path.exists()
    assert not legacy_db.exists()

    # Schema was applied — v1 migration row exists.
    conn = sqlite3.connect(db_path)
    try:
        names = {
            r[0] for r in conn.execute("SELECT name FROM schema_version").fetchall()
        }
    finally:
        conn.close()
    assert "v1_initial" in names


def test_init_db_is_idempotent(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path))
    monkeypatch.setattr(conn_mod, "LEGACY_HISTORY_DB_PATH", tmp_path / "nope.db")

    init_db()
    init_db()  # must not raise

    conn = sqlite3.connect(tmp_path / "srunx" / "srunx.db")
    try:
        rows = conn.execute(
            "SELECT COUNT(*) FROM schema_version WHERE name = 'v1_initial'"
        ).fetchone()
    finally:
        conn.close()
    assert rows[0] == 1


def test_connection_usable_from_another_thread(tmp_path: Path) -> None:
    """Regression: FastAPI routes pass the request-bound connection into
    ``anyio.to_thread.run_sync`` worker threads. The stock ``sqlite3``
    driver rejects cross-thread use unless ``check_same_thread=False``
    was set at open time — which a real uvicorn run would hit on every
    /api/endpoints, /api/deliveries, /api/subscriptions, /api/watches
    call.

    This test exercises the exact cross-thread pattern (connection
    opened on thread A, read on thread B) to prevent the bug from
    regressing.
    """
    import threading

    db = tmp_path / "srunx.db"
    conn = open_connection(db)
    try:
        conn.execute("CREATE TABLE t (id INTEGER)")
        conn.execute("INSERT INTO t VALUES (1)")

        result: list[int] = []
        error: list[BaseException] = []

        def read_on_other_thread() -> None:
            try:
                row = conn.execute("SELECT id FROM t").fetchone()
                result.append(row[0])
            except BaseException as exc:  # noqa: BLE001
                error.append(exc)

        t = threading.Thread(target=read_on_other_thread)
        t.start()
        t.join(timeout=5)

        assert not error, f"cross-thread read raised: {error[0]!r}"
        assert result == [1]
    finally:
        conn.close()
