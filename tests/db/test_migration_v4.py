"""Integration tests for the V4 migration: widen workflow_runs.triggered_by.

Covers the Phase 3 A-3 change: the ``triggered_by`` CHECK allowlist is
extended from ``('cli','web','schedule')`` to
``('cli','web','schedule','mcp')`` via a full table rebuild. The rebuild
must preserve every V3-era column (notably ``sweep_run_id``), every
index, and every inbound foreign-key reference.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest
from srunx.db.connection import open_connection
from srunx.db.migrations import (
    MIGRATIONS,
    _apply_fk_off_migration,
    _apply_tx_migration,
    _ensure_schema_version_table,
    apply_migrations,
)

# ---------------------------------------------------------------------------
# Helpers — mirror tests/db/test_migration_v3.py style
# ---------------------------------------------------------------------------


def _apply_through_version(conn: sqlite3.Connection, target: int) -> None:
    """Apply every registered migration with ``version <= target``."""
    _ensure_schema_version_table(conn)
    applied = {
        row[0] for row in conn.execute("SELECT name FROM schema_version").fetchall()
    }
    for mig in MIGRATIONS:
        if mig.version > target:
            break
        if mig.name in applied:
            continue
        if mig.requires_fk_off:
            _apply_fk_off_migration(conn, mig)
        else:
            _apply_tx_migration(conn, mig)


def _index_names(conn: sqlite3.Connection, table: str) -> set[str]:
    rows = conn.execute(f"PRAGMA index_list('{table}')").fetchall()
    return {r["name"] for r in rows if not r["name"].startswith("sqlite_autoindex_")}


# ---------------------------------------------------------------------------
# Apply scenarios
# ---------------------------------------------------------------------------


def test_v4_applies_on_top_of_v3_db(tmp_path: Path) -> None:
    """V4 must apply cleanly on a DB that was originally migrated to V3."""
    db = tmp_path / "srunx.db"
    conn = open_connection(db)
    try:
        _apply_through_version(conn, 3)
        applied_v3 = {
            row[0] for row in conn.execute("SELECT name FROM schema_version").fetchall()
        }
        assert "v3_sweep_runs" in applied_v3
        assert "v4_widen_triggered_by_mcp" not in applied_v3

        apply_migrations(conn)
        applied_all = {
            row[0] for row in conn.execute("SELECT name FROM schema_version").fetchall()
        }
    finally:
        conn.close()

    assert "v4_widen_triggered_by_mcp" in applied_all


def test_v4_idempotent(tmp_path: Path) -> None:
    """A second ``apply_migrations`` call is a no-op."""
    db = tmp_path / "srunx.db"
    conn = open_connection(db)
    try:
        apply_migrations(conn)
        second = apply_migrations(conn)
        count = conn.execute(
            "SELECT COUNT(*) FROM schema_version "
            "WHERE name = 'v4_widen_triggered_by_mcp'"
        ).fetchone()[0]
    finally:
        conn.close()

    assert second == []
    assert count == 1


# ---------------------------------------------------------------------------
# CHECK constraint behaviour
# ---------------------------------------------------------------------------


def test_workflow_runs_triggered_by_mcp_allowed(tmp_path: Path) -> None:
    """After V4, ``triggered_by='mcp'`` is a valid CHECK value."""
    db = tmp_path / "srunx.db"
    conn = open_connection(db)
    try:
        apply_migrations(conn)
        conn.execute(
            "INSERT INTO workflow_runs "
            "(workflow_name, status, started_at, triggered_by) "
            "VALUES (?, ?, ?, ?)",
            ("mcp_sweep_cell", "pending", "2026-04-22T10:00:00.000Z", "mcp"),
        )
        row = conn.execute(
            "SELECT triggered_by FROM workflow_runs WHERE workflow_name = ?",
            ("mcp_sweep_cell",),
        ).fetchone()
    finally:
        conn.close()

    assert row is not None
    assert row["triggered_by"] == "mcp"


def test_workflow_runs_triggered_by_invalid_rejected(tmp_path: Path) -> None:
    """Values outside the V4 allowlist still fail the CHECK constraint."""
    db = tmp_path / "srunx.db"
    conn = open_connection(db)
    try:
        apply_migrations(conn)
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                "INSERT INTO workflow_runs "
                "(workflow_name, status, started_at, triggered_by) "
                "VALUES (?, ?, ?, ?)",
                ("bogus", "pending", "2026-04-22T10:00:00.000Z", "garbage"),
            )
    finally:
        conn.close()


def test_workflow_runs_triggered_by_schedule_still_allowed(tmp_path: Path) -> None:
    """The reserved ``'schedule'`` value survives V4 for forward compat."""
    db = tmp_path / "srunx.db"
    conn = open_connection(db)
    try:
        apply_migrations(conn)
        conn.execute(
            "INSERT INTO workflow_runs "
            "(workflow_name, status, started_at, triggered_by) "
            "VALUES (?, ?, ?, ?)",
            ("scheduled_wf", "pending", "2026-04-22T10:00:00.000Z", "schedule"),
        )
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Data + index preservation through table rebuild
# ---------------------------------------------------------------------------


def test_existing_workflow_runs_row_survives_v4(tmp_path: Path) -> None:
    """A workflow_runs row written before V4 must round-trip unchanged."""
    db = tmp_path / "srunx.db"
    conn = open_connection(db)
    try:
        _apply_through_version(conn, 3)
        conn.execute(
            "INSERT INTO workflow_runs "
            "(workflow_name, workflow_yaml_path, status, started_at, "
            " completed_at, args, error, triggered_by, sweep_run_id) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                "pre_v4_wf",
                "/tmp/wf.yaml",
                "completed",
                "2026-04-20T10:00:00.000Z",
                "2026-04-20T10:05:00.000Z",
                '{"lr": 0.01}',
                None,
                "web",
                None,
            ),
        )
        apply_migrations(conn)

        row = conn.execute(
            "SELECT workflow_name, workflow_yaml_path, status, started_at, "
            "completed_at, args, error, triggered_by, sweep_run_id "
            "FROM workflow_runs WHERE workflow_name = ?",
            ("pre_v4_wf",),
        ).fetchone()
    finally:
        conn.close()

    assert row is not None
    assert row["workflow_name"] == "pre_v4_wf"
    assert row["workflow_yaml_path"] == "/tmp/wf.yaml"
    assert row["status"] == "completed"
    assert row["started_at"] == "2026-04-20T10:00:00.000Z"
    assert row["completed_at"] == "2026-04-20T10:05:00.000Z"
    assert row["args"] == '{"lr": 0.01}'
    assert row["error"] is None
    assert row["triggered_by"] == "web"
    assert row["sweep_run_id"] is None


def test_workflow_runs_indexes_preserved_after_v4(tmp_path: Path) -> None:
    """The three indexes defined in V1+V3 must survive the V4 rebuild."""
    db = tmp_path / "srunx.db"
    conn = open_connection(db)
    try:
        apply_migrations(conn)
        names = _index_names(conn, "workflow_runs")
    finally:
        conn.close()

    assert {
        "idx_workflow_runs_status",
        "idx_workflow_runs_started_at",
        "idx_workflow_runs_sweep_run_id",
    }.issubset(names)


def test_workflow_runs_sweep_run_id_fk_preserved(tmp_path: Path) -> None:
    """workflow_runs.sweep_run_id → sweep_runs(id) ON DELETE SET NULL must persist."""
    db = tmp_path / "srunx.db"
    conn = open_connection(db)
    try:
        apply_migrations(conn)
        row = conn.execute(
            'SELECT "table", "from", "to", on_delete '
            "FROM pragma_foreign_key_list('workflow_runs') "
            "WHERE \"table\" = 'sweep_runs'"
        ).fetchone()
    finally:
        conn.close()

    assert row is not None
    assert row["from"] == "sweep_run_id"
    assert row["to"] == "id"
    assert row["on_delete"] == "SET NULL"


def test_workflow_runs_fks_globally_consistent_after_v4(tmp_path: Path) -> None:
    """``PRAGMA foreign_key_check`` must report no violations after V4."""
    db = tmp_path / "srunx.db"
    conn = open_connection(db)
    try:
        apply_migrations(conn)
        violations = conn.execute("PRAGMA foreign_key_check").fetchall()
    finally:
        conn.close()

    assert list(violations) == []


# ---------------------------------------------------------------------------
# Inbound FK references still resolve after the rebuild
# ---------------------------------------------------------------------------


def test_jobs_workflow_run_id_fk_still_resolves(tmp_path: Path) -> None:
    """jobs.workflow_run_id → workflow_runs(id) must survive the rebuild.

    The V4 migration drops and re-creates ``workflow_runs``; without
    ``PRAGMA foreign_keys=OFF`` during the rebuild, inbound references
    from ``jobs`` would be invalidated.
    """
    db = tmp_path / "srunx.db"
    conn = open_connection(db)
    try:
        apply_migrations(conn)
        cur = conn.execute(
            "INSERT INTO workflow_runs "
            "(workflow_name, status, started_at, triggered_by) "
            "VALUES (?, ?, ?, ?)",
            ("inbound_fk_wf", "pending", "2026-04-22T10:00:00.000Z", "mcp"),
        )
        wr_id = cur.lastrowid

        conn.execute(
            "INSERT INTO jobs "
            "(job_id, name, status, submitted_at, workflow_run_id, submission_source) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (123_456, "j1", "PENDING", "2026-04-22T10:00:01.000Z", wr_id, "workflow"),
        )
        row = conn.execute(
            "SELECT workflow_run_id FROM jobs WHERE job_id = ?", (123_456,)
        ).fetchone()
    finally:
        conn.close()

    assert row is not None
    assert row["workflow_run_id"] == wr_id


def test_workflow_run_jobs_cascade_delete_still_works(tmp_path: Path) -> None:
    """workflow_run_jobs.workflow_run_id ON DELETE CASCADE must still fire."""
    db = tmp_path / "srunx.db"
    conn = open_connection(db)
    try:
        apply_migrations(conn)
        cur = conn.execute(
            "INSERT INTO workflow_runs "
            "(workflow_name, status, started_at, triggered_by) "
            "VALUES (?, ?, ?, ?)",
            ("cascade_wf", "pending", "2026-04-22T10:00:00.000Z", "mcp"),
        )
        wr_id = cur.lastrowid

        conn.execute(
            "INSERT INTO workflow_run_jobs (workflow_run_id, job_name) VALUES (?, ?)",
            (wr_id, "step1"),
        )

        # Deleting the parent must cascade.
        conn.execute("DELETE FROM workflow_runs WHERE id = ?", (wr_id,))
        remaining = conn.execute(
            "SELECT COUNT(*) FROM workflow_run_jobs WHERE workflow_run_id = ?",
            (wr_id,),
        ).fetchone()[0]
    finally:
        conn.close()

    assert remaining == 0
