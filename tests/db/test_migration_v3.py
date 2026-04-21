"""Integration tests for the V3 migration: table rebuild integrity.

Covers R5.3: after V3 applies, the widened events/watches tables must
retain every pre-existing UNIQUE constraint, FOREIGN KEY and INDEX,
and every previously-stored row must survive the migration.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

from srunx.db.connection import open_connection
from srunx.db.migrations import (
    MIGRATIONS,
    Migration,
    _apply_fk_off_migration,
    _apply_tx_migration,
    _ensure_schema_version_table,
    apply_migrations,
)

# ---------------------------------------------------------------------------
# Helpers: drive the migration runner one version at a time so we can
# simulate a DB that was originally created at V1 or V2 and then
# upgraded to V3 in a separate pass.
# ---------------------------------------------------------------------------


def _apply_through_version(conn: sqlite3.Connection, target: int) -> None:
    """Apply every registered migration with ``version <= target``.

    Does not mutate the module-level ``MIGRATIONS`` list (keeps parallel
    tests safe): it calls the per-migration internals directly.
    """
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
    # Skip auto-generated indexes (sqlite_autoindex_*) which back UNIQUE
    # constraints -- those are covered by `_unique_index_columns`.
    return {r["name"] for r in rows if not r["name"].startswith("sqlite_autoindex_")}


def _unique_index_columns(conn: sqlite3.Connection, table: str) -> set[tuple[str, ...]]:
    """Return the set of column tuples for every UNIQUE index on ``table``."""
    result: set[tuple[str, ...]] = set()
    for row in conn.execute(f"PRAGMA index_list('{table}')").fetchall():
        if not row["unique"]:
            continue
        cols = conn.execute(f"PRAGMA index_info('{row['name']}')").fetchall()
        result.add(tuple(c["name"] for c in cols))
    return result


def _foreign_keys(conn: sqlite3.Connection, table: str) -> set[tuple[str, str, str]]:
    """Return (referenced_table, from_col, to_col) tuples for ``table``'s FKs."""
    result: set[tuple[str, str, str]] = set()
    for row in conn.execute(f"PRAGMA foreign_key_list('{table}')").fetchall():
        result.add((row["table"], row["from"], row["to"]))
    return result


# ---------------------------------------------------------------------------
# Migration apply scenarios
# ---------------------------------------------------------------------------


def test_v3_applies_cleanly_on_top_of_v1_only_db(tmp_path: Path) -> None:
    db = tmp_path / "srunx.db"
    conn = open_connection(db)
    try:
        _apply_through_version(conn, 1)
        # Sanity: V1 present, V3 absent.
        applied_v1 = {
            row[0] for row in conn.execute("SELECT name FROM schema_version").fetchall()
        }
        assert "v1_initial" in applied_v1
        assert "v3_sweep_runs" not in applied_v1

        # Now apply the full set (V2 + V3).
        apply_migrations(conn)
        applied_all = {
            row[0] for row in conn.execute("SELECT name FROM schema_version").fetchall()
        }
    finally:
        conn.close()

    assert "v2_dashboard_indexes" in applied_all
    assert "v3_sweep_runs" in applied_all


def test_v3_idempotent_when_already_applied(tmp_path: Path) -> None:
    db = tmp_path / "srunx.db"
    conn = open_connection(db)
    try:
        apply_migrations(conn)
        second = apply_migrations(conn)
        count = conn.execute(
            "SELECT COUNT(*) FROM schema_version WHERE name='v3_sweep_runs'"
        ).fetchone()[0]
    finally:
        conn.close()

    assert second == []
    assert count == 1


# ---------------------------------------------------------------------------
# Constraint + index preservation
# ---------------------------------------------------------------------------


def test_events_unique_dedup_survives_v3(tmp_path: Path) -> None:
    """(kind, source_ref, payload_hash) UNIQUE must exist after rebuild."""
    db = tmp_path / "srunx.db"
    conn = open_connection(db)
    try:
        apply_migrations(conn)
        uniques = _unique_index_columns(conn, "events")
    finally:
        conn.close()
    assert ("kind", "source_ref", "payload_hash") in uniques


def test_events_has_all_expected_indexes(tmp_path: Path) -> None:
    db = tmp_path / "srunx.db"
    conn = open_connection(db)
    try:
        apply_migrations(conn)
        names = _index_names(conn, "events")
    finally:
        conn.close()
    assert {
        "idx_events_dedup",
        "idx_events_source_ref",
        "idx_events_kind",
    }.issubset(names)


def test_watches_has_all_expected_indexes(tmp_path: Path) -> None:
    db = tmp_path / "srunx.db"
    conn = open_connection(db)
    try:
        apply_migrations(conn)
        names = _index_names(conn, "watches")
    finally:
        conn.close()
    assert {"idx_watches_kind_target", "idx_watches_open"}.issubset(names)


def test_deliveries_foreign_keys_still_point_at_events_and_subscriptions(
    tmp_path: Path,
) -> None:
    """After rebuilding events, deliveries.event_id FK must still resolve."""
    db = tmp_path / "srunx.db"
    conn = open_connection(db)
    try:
        apply_migrations(conn)
        fks = _foreign_keys(conn, "deliveries")
    finally:
        conn.close()
    assert ("events", "event_id", "id") in fks
    assert ("subscriptions", "subscription_id", "id") in fks
    assert ("endpoints", "endpoint_id", "id") in fks


def test_subscriptions_foreign_key_to_watches_survives(tmp_path: Path) -> None:
    """After rebuilding watches, subscriptions.watch_id FK must still resolve."""
    db = tmp_path / "srunx.db"
    conn = open_connection(db)
    try:
        apply_migrations(conn)
        fks = _foreign_keys(conn, "subscriptions")
    finally:
        conn.close()
    assert ("watches", "watch_id", "id") in fks


def test_workflow_runs_sweep_run_id_fk_is_set_null(tmp_path: Path) -> None:
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


# ---------------------------------------------------------------------------
# Data preservation through table rebuild
# ---------------------------------------------------------------------------


def test_existing_events_row_survives_v3(tmp_path: Path) -> None:
    db = tmp_path / "srunx.db"
    conn = open_connection(db)
    try:
        _apply_through_version(conn, 2)
        conn.execute(
            "INSERT INTO events (kind, source_ref, payload, payload_hash, observed_at) "
            "VALUES (?, ?, ?, ?, ?)",
            (
                "resource.threshold_crossed",
                "resource:gpu",
                '{"ok":true}',
                "hash-1",
                "2026-04-20T10:00:00.000Z",
            ),
        )

        # Now apply V3.
        apply_migrations(conn)

        row = conn.execute(
            "SELECT kind, source_ref, payload, payload_hash, observed_at "
            "FROM events WHERE payload_hash = ?",
            ("hash-1",),
        ).fetchone()
    finally:
        conn.close()

    assert row is not None
    assert row["kind"] == "resource.threshold_crossed"
    assert row["source_ref"] == "resource:gpu"


def test_existing_watches_row_survives_v3(tmp_path: Path) -> None:
    db = tmp_path / "srunx.db"
    conn = open_connection(db)
    try:
        _apply_through_version(conn, 2)
        conn.execute(
            "INSERT INTO watches (kind, target_ref, filter, created_at) "
            "VALUES (?, ?, ?, ?)",
            (
                "workflow_run",
                "workflow_run:42",
                None,
                "2026-04-20T10:00:00.000Z",
            ),
        )

        apply_migrations(conn)

        row = conn.execute(
            "SELECT kind, target_ref FROM watches WHERE target_ref = ?",
            ("workflow_run:42",),
        ).fetchone()
    finally:
        conn.close()

    assert row is not None
    assert row["kind"] == "workflow_run"


def test_new_event_kind_sweep_run_status_changed_is_allowed(tmp_path: Path) -> None:
    db = tmp_path / "srunx.db"
    conn = open_connection(db)
    try:
        apply_migrations(conn)
        conn.execute(
            "INSERT INTO events (kind, source_ref, payload, payload_hash, observed_at) "
            "VALUES (?, ?, ?, ?, ?)",
            (
                "sweep_run.status_changed",
                "sweep_run:1",
                "{}",
                "h",
                "2026-04-20T10:00:00.000Z",
            ),
        )
    finally:
        conn.close()


def test_new_watch_kind_sweep_run_is_allowed(tmp_path: Path) -> None:
    db = tmp_path / "srunx.db"
    conn = open_connection(db)
    try:
        apply_migrations(conn)
        conn.execute(
            "INSERT INTO watches (kind, target_ref, created_at) VALUES (?, ?, ?)",
            ("sweep_run", "sweep_run:1", "2026-04-20T10:00:00.000Z"),
        )
    finally:
        conn.close()


def test_v3_foreign_keys_globally_consistent(tmp_path: Path) -> None:
    """``PRAGMA foreign_key_check`` must report no violations after V3."""
    db = tmp_path / "srunx.db"
    conn = open_connection(db)
    try:
        apply_migrations(conn)
        violations = conn.execute("PRAGMA foreign_key_check").fetchall()
    finally:
        conn.close()
    assert list(violations) == []


# ---------------------------------------------------------------------------
# Explicit guard: the dataclass still defaults requires_fk_off=False
# ---------------------------------------------------------------------------


def test_migration_dataclass_default_preserves_existing_semantics() -> None:
    m = Migration(version=99, name="scratch", sql="SELECT 1;")
    assert m.requires_fk_off is False
