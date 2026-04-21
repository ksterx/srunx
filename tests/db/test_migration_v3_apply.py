"""Tests for ``apply_migrations`` handling of the V3 FK-off migration."""

from __future__ import annotations

from pathlib import Path

from srunx.db.connection import open_connection
from srunx.db.migrations import apply_migrations


def _fk_pragma(conn: object) -> int:
    row = conn.execute("PRAGMA foreign_keys").fetchone()  # type: ignore[attr-defined]
    return int(row[0])


def test_apply_migrations_runs_v3_on_fresh_db(tmp_path: Path) -> None:
    db = tmp_path / "srunx.db"
    conn = open_connection(db)
    try:
        applied = apply_migrations(conn)
    finally:
        conn.close()
    assert "v1_initial" in applied
    assert "v2_dashboard_indexes" in applied
    assert "v3_sweep_runs" in applied


def test_apply_migrations_is_idempotent_with_v3(tmp_path: Path) -> None:
    db = tmp_path / "srunx.db"
    conn = open_connection(db)
    try:
        first = apply_migrations(conn)
        second = apply_migrations(conn)
        count = conn.execute(
            "SELECT COUNT(*) FROM schema_version WHERE name = 'v3_sweep_runs'"
        ).fetchone()[0]
    finally:
        conn.close()
    assert "v3_sweep_runs" in first
    assert second == []
    assert count == 1


def test_apply_migrations_restores_foreign_keys_pragma(tmp_path: Path) -> None:
    """After V3 toggles FK OFF/ON the pragma is restored on the connection."""
    db = tmp_path / "srunx.db"
    conn = open_connection(db)
    try:
        apply_migrations(conn)
        assert _fk_pragma(conn) == 1
    finally:
        conn.close()

    # A fresh connection must also observe FK ON (the default from
    # open_connection).
    conn2 = open_connection(db)
    try:
        assert _fk_pragma(conn2) == 1
    finally:
        conn2.close()


def test_apply_migrations_creates_sweep_runs_table(tmp_path: Path) -> None:
    db = tmp_path / "srunx.db"
    conn = open_connection(db)
    try:
        apply_migrations(conn)
        rows = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='sweep_runs'"
        ).fetchall()
    finally:
        conn.close()
    assert len(rows) == 1


def test_workflow_runs_has_sweep_run_id_column(tmp_path: Path) -> None:
    db = tmp_path / "srunx.db"
    conn = open_connection(db)
    try:
        apply_migrations(conn)
        cols = {
            row[1]
            for row in conn.execute("PRAGMA table_info(workflow_runs)").fetchall()
        }
    finally:
        conn.close()
    assert "sweep_run_id" in cols
