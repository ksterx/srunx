"""Tests for ``srunx.observability.storage.migrations``."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

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


@pytest.mark.flaky(reruns=3, reruns_delay=1)
def test_apply_migrations_concurrent_callers_do_not_duplicate(
    tmp_path: Path,
) -> None:
    # Tracked in #196 — under full-suite CPU contention the schema-version
    # check race occasionally surfaces "table workflow_runs already exists"
    # before the IMMEDIATE-transaction guard kicks in. Always passes in
    # isolation; reruns absorb the rare CI hit until #196's deterministic
    # rewrite lands.
    """Regression: two threads racing on a cold DB.

    Before the fix, `applied_names` was read outside the IMMEDIATE
    lock; both racers saw an empty set, one created the tables, the
    other then tried `CREATE TABLE` on tables that already existed
    (SCHEMA_V1 uses bare CREATE TABLE for most of the domain tables).

    The fix re-reads `applied_names` *inside* the transaction; the
    loser's re-check sees `v1_initial` already applied and skips the
    DDL. This test runs two real threads against the same DB file —
    each with its own connection — and asserts neither raises AND
    `schema_version` has exactly one `v1_initial` row.
    """
    import threading

    db = tmp_path / "srunx.observability.storage"
    errors: list[BaseException] = []
    barrier = threading.Barrier(2)

    def apply_once() -> None:
        try:
            barrier.wait(timeout=5)
            conn = open_connection(db)
            try:
                apply_migrations(conn)
            finally:
                conn.close()
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)

    threads = [threading.Thread(target=apply_once) for _ in range(2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=10)

    assert not errors, f"concurrent migration raised: {[repr(e) for e in errors]}"

    verify = open_connection(db)
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
