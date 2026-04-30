"""Tests for ``apply_migrations`` handling of the V3 FK-off migration."""

from __future__ import annotations

from pathlib import Path

from srunx.observability.storage.connection import open_connection
from srunx.observability.storage.migrations import apply_migrations


def _fk_pragma(conn: object) -> int:
    row = conn.execute("PRAGMA foreign_keys").fetchone()  # type: ignore[attr-defined]
    return int(row[0])


def test_apply_migrations_runs_v3_on_fresh_db(tmp_path: Path) -> None:
    db = tmp_path / "srunx.observability.storage"
    conn = open_connection(db)
    try:
        applied = apply_migrations(conn)
    finally:
        conn.close()
    assert "v1_initial" in applied
    assert "v2_dashboard_indexes" in applied
    assert "v3_sweep_runs" in applied


def test_apply_migrations_is_idempotent_with_v3(tmp_path: Path) -> None:
    db = tmp_path / "srunx.observability.storage"
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
    db = tmp_path / "srunx.observability.storage"
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
    db = tmp_path / "srunx.observability.storage"
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
    db = tmp_path / "srunx.observability.storage"
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


def test_v3_migration_rolls_back_on_partial_failure(
    tmp_path: Path, monkeypatch
) -> None:
    """Regression for C5: V3 must be atomic — injected mid-script failure
    leaves no ``sweep_runs`` / ``sweep_run_id`` / widened CHECKs behind.

    The original implementation used ``conn.executescript`` which issues
    an implicit ``COMMIT`` at the start, silently terminating the
    enclosing ``BEGIN IMMEDIATE`` and dropping subsequent statements
    into autocommit — so a crash halfway through would leave the schema
    half-migrated.
    """
    from srunx.observability.storage import migrations as mig

    db = tmp_path / "srunx.observability.storage"
    conn = open_connection(db)
    try:
        # Run only v1 + v2 first by slicing MIGRATIONS. This leaves v3
        # (the FK-off, table-rebuild migration) pending so the patched
        # apply call exercises ``_apply_fk_off_migration``.
        original_migrations = list(mig.MIGRATIONS)
        monkeypatch.setattr(mig, "MIGRATIONS", original_migrations[:2])
        mig.apply_migrations(conn)
        monkeypatch.setattr(mig, "MIGRATIONS", original_migrations)

        # Patch _split_sql_statements so the V3 script fails mid-stream,
        # after sweep_runs has been CREATE'd but before the INSERT into
        # schema_version. The rollback must undo the CREATE.
        real_split = mig._split_sql_statements

        def exploding_split(sql: str):
            statements = real_split(sql)
            if "CREATE TABLE sweep_runs" in sql:
                # Insert a broken statement after the first one so
                # sweep_runs is CREATE'd but execution halts before
                # workflow_runs.sweep_run_id gets added.
                statements.insert(1, "THIS IS NOT VALID SQL;")
            return statements

        monkeypatch.setattr(mig, "_split_sql_statements", exploding_split)

        import pytest as _pytest

        with _pytest.raises(Exception):
            mig.apply_migrations(conn)

        # The V3 migration must have rolled back fully: no sweep_runs
        # table, no sweep_run_id column on workflow_runs, no v3 row in
        # schema_version.
        sweep_tables = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='sweep_runs'"
        ).fetchall()
        assert sweep_tables == []

        cols = {
            row[1]
            for row in conn.execute("PRAGMA table_info(workflow_runs)").fetchall()
        }
        assert "sweep_run_id" not in cols

        recorded = conn.execute(
            "SELECT name FROM schema_version WHERE name = 'v3_sweep_runs'"
        ).fetchall()
        assert recorded == []

        # FK enforcement must be restored regardless of outcome.
        fk_row = conn.execute("PRAGMA foreign_keys").fetchone()
        assert int(fk_row[0]) == 1
    finally:
        conn.close()
