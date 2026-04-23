"""Tests for ``apply_migrations`` handling of the V4 FK-off migration.

Mirrors ``test_migration_v3_apply.py``: verifies the Phase 3 A-3 CHECK
widening migration is applied via ``_apply_fk_off_migration``, is
idempotent, restores the ``foreign_keys`` pragma, and — most
importantly — rolls back atomically on partial failure. The
``workflow_runs`` table must not be left half-rebuilt (missing columns,
missing indexes, or missing ``sweep_run_id`` data) if any DDL
statement inside the V4 script raises.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
from srunx.db.connection import open_connection
from srunx.db.migrations import apply_migrations


def _fk_pragma(conn: Any) -> int:
    row = conn.execute("PRAGMA foreign_keys").fetchone()
    return int(row[0])


def test_apply_migrations_runs_v4_on_fresh_db(tmp_path: Path) -> None:
    db = tmp_path / "srunx.db"
    conn = open_connection(db)
    try:
        applied = apply_migrations(conn)
    finally:
        conn.close()
    assert "v1_initial" in applied
    assert "v2_dashboard_indexes" in applied
    assert "v3_sweep_runs" in applied
    assert "v4_widen_triggered_by_mcp" in applied


def test_apply_migrations_is_idempotent_with_v4(tmp_path: Path) -> None:
    db = tmp_path / "srunx.db"
    conn = open_connection(db)
    try:
        first = apply_migrations(conn)
        second = apply_migrations(conn)
        count = conn.execute(
            "SELECT COUNT(*) FROM schema_version "
            "WHERE name = 'v4_widen_triggered_by_mcp'"
        ).fetchone()[0]
    finally:
        conn.close()
    assert "v4_widen_triggered_by_mcp" in first
    assert second == []
    assert count == 1


def test_apply_migrations_restores_foreign_keys_pragma_after_v4(
    tmp_path: Path,
) -> None:
    """After V4 toggles FK OFF/ON the pragma must be restored."""
    db = tmp_path / "srunx.db"
    conn = open_connection(db)
    try:
        apply_migrations(conn)
        assert _fk_pragma(conn) == 1
    finally:
        conn.close()

    # A fresh connection must also observe FK ON (the default).
    conn2 = open_connection(db)
    try:
        assert _fk_pragma(conn2) == 1
    finally:
        conn2.close()


def test_v4_migration_rolls_back_on_partial_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """V4 must be atomic: a mid-script failure leaves the schema at V3.

    Same rollback contract as the V3 atomicity test — if the migration
    raises after CREATE TABLE workflow_runs_v4 but before the DROP +
    RENAME, the rollback must undo the new table so the original
    ``workflow_runs`` (with the V3 CHECK constraint) remains intact and
    no ``v4_widen_triggered_by_mcp`` row is recorded in
    ``schema_version``.
    """
    from srunx.db import migrations as mig

    db = tmp_path / "srunx.db"
    conn = open_connection(db)
    try:
        # Apply V1..V3 first so V4 is the pending migration that the
        # patched ``apply_migrations`` call exercises.
        original_migrations = list(mig.MIGRATIONS)
        monkeypatch.setattr(mig, "MIGRATIONS", original_migrations[:3])
        mig.apply_migrations(conn)
        monkeypatch.setattr(mig, "MIGRATIONS", original_migrations)

        # Inject a broken statement right after CREATE TABLE
        # workflow_runs_v4 so the new table exists momentarily but the
        # DROP + RENAME never runs. The BEGIN IMMEDIATE wrapper must
        # roll everything back — including the CREATE.
        real_split = mig._split_sql_statements

        def exploding_split(sql: str) -> list[str]:
            statements = real_split(sql)
            if "CREATE TABLE workflow_runs_v4" in sql:
                statements.insert(1, "THIS IS NOT VALID SQL;")
            return statements

        monkeypatch.setattr(mig, "_split_sql_statements", exploding_split)

        with pytest.raises(Exception):
            mig.apply_migrations(conn)

        # The half-built shadow table must be gone.
        shadow = conn.execute(
            "SELECT name FROM sqlite_master "
            "WHERE type='table' AND name='workflow_runs_v4'"
        ).fetchall()
        assert shadow == []

        # The original workflow_runs (with the V3 CHECK) must still
        # exist and still reject ``triggered_by='mcp'``.
        existing = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='workflow_runs'"
        ).fetchall()
        assert len(existing) == 1

        import sqlite3 as _sqlite3

        with pytest.raises(_sqlite3.IntegrityError):
            conn.execute(
                "INSERT INTO workflow_runs "
                "(workflow_name, status, started_at, triggered_by) "
                "VALUES (?, ?, ?, ?)",
                ("rollback_check", "pending", "2026-04-22T10:00:00.000Z", "mcp"),
            )

        # No V4 row in schema_version.
        recorded = conn.execute(
            "SELECT name FROM schema_version WHERE name = 'v4_widen_triggered_by_mcp'"
        ).fetchall()
        assert recorded == []

        # FK enforcement restored regardless of outcome.
        assert _fk_pragma(conn) == 1
    finally:
        conn.close()
