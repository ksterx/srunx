"""Tests for ``srunx.observability.storage.migrations``."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest

from srunx.observability.storage.connection import open_connection
from srunx.observability.storage.migrations import (
    apply_migrations,
    bootstrap_from_config,
)

# ---- apply_migrations ----


def test_apply_migrations_applies_v1_on_fresh_db(tmp_path: Path) -> None:
    db = tmp_path / "srunx.observability.storage"
    conn = open_connection(db)
    try:
        applied = apply_migrations(conn)
    finally:
        conn.close()
    assert "v1_initial" in applied


def test_apply_migrations_is_idempotent(tmp_path: Path) -> None:
    db = tmp_path / "srunx.observability.storage"
    conn = open_connection(db)
    try:
        first = apply_migrations(conn)
        second = apply_migrations(conn)
        count = conn.execute(
            "SELECT COUNT(*) FROM schema_version WHERE name = 'v1_initial'"
        ).fetchone()[0]
    finally:
        conn.close()
    assert "v1_initial" in first
    assert second == []
    assert count == 1


def test_apply_migrations_no_double_apply_under_pre_check_race(
    tmp_path: Path,
) -> None:
    """Regression: two callers passing the pre-check before either holds
    the IMMEDIATE write lock must not both apply the same migration.

    The race we care about: caller A passes the outside-lock pre-check
    in :func:`apply_migrations` (``if mig.name in _applied_names(conn):
    continue``), then before A reaches its ``BEGIN IMMEDIATE``, peer B
    runs ``apply_migrations`` to completion on its own connection. A's
    ``BEGIN IMMEDIATE`` then serialises *after* B's commit, and A's
    recheck inside the lock must see ``v1_initial`` already in
    ``schema_version`` and roll back without re-running any DDL.

    Forcing this race deterministically: we drive A from a custom
    :class:`sqlite3.Connection` subclass that intercepts the first
    ``BEGIN IMMEDIATE`` on A's connection, runs B's
    ``apply_migrations`` to completion at that exact point, and then
    lets A continue. No threads, no barriers, no ``time.sleep`` — the
    interleaving is structurally enforced.

    Production-side, the closing of the race window depends on
    :func:`_apply_tx_migration` running its DDL via
    :func:`_split_sql_statements` rather than
    :meth:`sqlite3.Connection.executescript`. The latter issues an
    implicit ``COMMIT`` that would release the IMMEDIATE before the
    ``schema_version`` row is INSERTed, re-opening the very window
    this test guards. If ``_apply_tx_migration`` ever switches back to
    ``executescript``, this test fails with a duplicate ``CREATE TABLE``
    error.
    """
    import sqlite3

    db = tmp_path / "srunx.db"

    # Connection B: vanilla connection that B will use to race ahead.
    conn_b = sqlite3.connect(str(db), isolation_level=None, check_same_thread=False)

    # Connection A factory: trigger conn_b's apply_migrations inside the
    # very first BEGIN IMMEDIATE that A issues.
    class _RaceTriggerConnection(sqlite3.Connection):
        _peer_already_ran = False

        def execute(self, sql: str, parameters: Any = ()) -> sqlite3.Cursor:
            if (
                not _RaceTriggerConnection._peer_already_ran
                and sql.lstrip().upper().startswith("BEGIN IMMEDIATE")
            ):
                # First time A is about to take the write lock: let B
                # finish first. A's actual BEGIN IMMEDIATE below
                # serialises naturally (sqlite blocks the second
                # BEGIN IMMEDIATE until the first COMMIT releases it).
                _RaceTriggerConnection._peer_already_ran = True
                apply_migrations(conn_b)
            return super().execute(sql, parameters)

    conn_a = sqlite3.connect(
        str(db),
        isolation_level=None,
        check_same_thread=False,
        factory=_RaceTriggerConnection,
    )

    try:
        applied_by_a = apply_migrations(conn_a)
    finally:
        conn_a.close()
        conn_b.close()

    # A's pre-check passed (cold DB), B raced ahead and committed
    # v1_initial, then A's BEGIN IMMEDIATE acquired the lock and the
    # recheck inside the lock saw v1_initial already applied →
    # rollback, no DDL replay, no duplicate row. ``apply_migrations``
    # now reports "what was actually applied by this call" (the
    # outer-loop append is gated on the helper's bool return), so A's
    # return value is empty.
    assert applied_by_a == [], (
        f"A reported applying migrations ({applied_by_a!r}) but B raced "
        "ahead — A's recheck inside BEGIN IMMEDIATE should have rolled "
        "back."
    )

    verify = sqlite3.connect(str(db))
    try:
        count = verify.execute(
            "SELECT COUNT(*) FROM schema_version WHERE name = 'v1_initial'"
        ).fetchone()[0]
    finally:
        verify.close()
    assert count == 1


def test_apply_migrations_creates_all_tables(tmp_path: Path) -> None:
    db = tmp_path / "srunx.observability.storage"
    conn = open_connection(db)
    try:
        apply_migrations(conn)
        rows = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        ).fetchall()
        tables = {r[0] for r in rows}
    finally:
        conn.close()

    expected = {
        "schema_version",
        "workflow_runs",
        "jobs",
        "workflow_run_jobs",
        "job_state_transitions",
        "resource_snapshots",
        "endpoints",
        "watches",
        "subscriptions",
        "events",
        "deliveries",
    }
    missing = expected - tables
    assert not missing, f"missing tables: {missing}"


def test_resource_snapshots_gpu_utilization_is_null_when_total_zero(
    tmp_path: Path,
) -> None:
    db = tmp_path / "srunx.observability.storage"
    conn = open_connection(db)
    try:
        apply_migrations(conn)
        conn.execute(
            "INSERT INTO resource_snapshots "
            "(observed_at, partition, gpus_total, gpus_available, gpus_in_use, "
            "nodes_total, nodes_idle, nodes_down) "
            "VALUES (?, NULL, 0, 0, 0, 1, 1, 0)",
            ("2026-04-18T00:00:00Z",),
        )
        conn.execute(
            "INSERT INTO resource_snapshots "
            "(observed_at, partition, gpus_total, gpus_available, gpus_in_use, "
            "nodes_total, nodes_idle, nodes_down) "
            "VALUES (?, NULL, 8, 2, 6, 1, 0, 0)",
            ("2026-04-18T00:05:00Z",),
        )
        conn.commit()
        rows = conn.execute(
            "SELECT gpus_total, gpu_utilization FROM resource_snapshots "
            "ORDER BY observed_at"
        ).fetchall()
    finally:
        conn.close()

    assert rows[0]["gpus_total"] == 0
    assert rows[0]["gpu_utilization"] is None
    assert rows[1]["gpus_total"] == 8
    assert rows[1]["gpu_utilization"] == pytest.approx(0.75)


# ---- bootstrap_from_config ----


@dataclass
class _FakeNotifications:
    slack_webhook_url: str | None = None


@dataclass
class _FakeConfig:
    notifications: _FakeNotifications


def _open_migrated(db: Path):
    conn = open_connection(db)
    apply_migrations(conn)
    return conn


def test_bootstrap_no_webhook_records_guard_only(tmp_path: Path) -> None:
    db = tmp_path / "srunx.observability.storage"
    conn = _open_migrated(db)
    try:
        inserted = bootstrap_from_config(
            conn, _FakeConfig(_FakeNotifications(slack_webhook_url=None))
        )
        endpoint_count = conn.execute("SELECT COUNT(*) FROM endpoints").fetchone()[0]
        guard_count = conn.execute(
            "SELECT COUNT(*) FROM schema_version WHERE name = 'bootstrap_slack_webhook_url'"
        ).fetchone()[0]
    finally:
        conn.close()
    assert inserted is False
    assert endpoint_count == 0
    assert guard_count == 1


def test_bootstrap_inserts_endpoint_and_guard(tmp_path: Path) -> None:
    db = tmp_path / "srunx.observability.storage"
    conn = _open_migrated(db)
    try:
        inserted = bootstrap_from_config(
            conn,
            _FakeConfig(
                _FakeNotifications(
                    slack_webhook_url="https://hooks.slack.com/services/A/B/C"
                )
            ),
        )
        rows = conn.execute("SELECT kind, name, config FROM endpoints").fetchall()
        guard_count = conn.execute(
            "SELECT COUNT(*) FROM schema_version WHERE name = 'bootstrap_slack_webhook_url'"
        ).fetchone()[0]
    finally:
        conn.close()

    assert inserted is True
    assert len(rows) == 1
    assert rows[0]["kind"] == "slack_webhook"
    assert rows[0]["name"] == "default"
    cfg = json.loads(rows[0]["config"])
    assert cfg == {"webhook_url": "https://hooks.slack.com/services/A/B/C"}
    assert guard_count == 1


def test_bootstrap_is_idempotent(tmp_path: Path) -> None:
    db = tmp_path / "srunx.observability.storage"
    conn = _open_migrated(db)
    try:
        cfg = _FakeConfig(
            _FakeNotifications(
                slack_webhook_url="https://hooks.slack.com/services/A/B/C"
            )
        )
        first = bootstrap_from_config(conn, cfg)
        second = bootstrap_from_config(conn, cfg)
        endpoint_count = conn.execute("SELECT COUNT(*) FROM endpoints").fetchone()[0]
    finally:
        conn.close()
    assert first is True
    assert second is False
    assert endpoint_count == 1


def test_bootstrap_rolls_back_guard_on_insert_failure(tmp_path: Path) -> None:
    db = tmp_path / "srunx.observability.storage"
    conn = _open_migrated(db)
    try:
        # Pre-create an endpoint with the name 'default' to force UNIQUE violation.
        conn.execute(
            "INSERT INTO endpoints (kind, name, config, created_at) "
            "VALUES ('slack_webhook', 'default', '{}', '2026-04-18T00:00:00Z')"
        )
        conn.commit()

        inserted = bootstrap_from_config(
            conn,
            _FakeConfig(
                _FakeNotifications(
                    slack_webhook_url="https://hooks.slack.com/services/A/B/C"
                )
            ),
        )
        guard_count = conn.execute(
            "SELECT COUNT(*) FROM schema_version WHERE name = 'bootstrap_slack_webhook_url'"
        ).fetchone()[0]
    finally:
        conn.close()

    assert inserted is False
    # Guard NOT recorded — so next startup can retry.
    assert guard_count == 0
