"""Tests for ``srunx.db.repositories.resource_snapshots``."""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path

import pytest

from srunx.db.connection import open_connection
from srunx.db.migrations import apply_migrations
from srunx.db.models import ResourceSnapshot
from srunx.db.repositories.resource_snapshots import ResourceSnapshotRepository


@pytest.fixture()
def conn(tmp_path: Path) -> Iterator[sqlite3.Connection]:
    db = tmp_path / "srunx.db"
    c = open_connection(db)
    apply_migrations(c)
    try:
        yield c
    finally:
        c.close()


def _make_snapshot(
    observed_at: datetime,
    *,
    partition: str | None = None,
    gpus_total: int = 8,
    gpus_available: int = 2,
    gpus_in_use: int = 6,
    nodes_total: int = 2,
    nodes_idle: int = 0,
    nodes_down: int = 0,
) -> ResourceSnapshot:
    return ResourceSnapshot(
        observed_at=observed_at,
        partition=partition,
        gpus_total=gpus_total,
        gpus_available=gpus_available,
        gpus_in_use=gpus_in_use,
        nodes_total=nodes_total,
        nodes_idle=nodes_idle,
        nodes_down=nodes_down,
    )


def test_insert_assigns_rowid_and_computes_utilization(
    conn: sqlite3.Connection,
) -> None:
    repo = ResourceSnapshotRepository(conn)
    snap = _make_snapshot(datetime(2026, 4, 18, 12, 0, tzinfo=UTC))

    row_id = repo.insert(snap)
    conn.commit()

    assert row_id > 0
    row = conn.execute(
        "SELECT gpus_total, gpu_utilization FROM resource_snapshots WHERE id = ?",
        (row_id,),
    ).fetchone()
    assert row["gpus_total"] == 8
    assert row["gpu_utilization"] == pytest.approx(0.75)


def test_insert_zero_total_yields_null_utilization(conn: sqlite3.Connection) -> None:
    repo = ResourceSnapshotRepository(conn)
    snap = _make_snapshot(
        datetime(2026, 4, 18, 12, 0, tzinfo=UTC),
        gpus_total=0,
        gpus_available=0,
        gpus_in_use=0,
        nodes_total=1,
        nodes_idle=1,
    )
    row_id = repo.insert(snap)
    conn.commit()

    row = conn.execute(
        "SELECT gpus_total, gpu_utilization FROM resource_snapshots WHERE id = ?",
        (row_id,),
    ).fetchone()
    assert row["gpus_total"] == 0
    assert row["gpu_utilization"] is None


def test_insert_serializes_datetime_to_canonical_iso(conn: sqlite3.Connection) -> None:
    repo = ResourceSnapshotRepository(conn)
    snap = _make_snapshot(datetime(2026, 4, 18, 12, 0, 0, 123000, tzinfo=UTC))
    row_id = repo.insert(snap)
    conn.commit()

    raw = conn.execute(
        "SELECT observed_at FROM resource_snapshots WHERE id = ?", (row_id,)
    ).fetchone()
    assert raw["observed_at"].endswith("Z")
    assert "2026-04-18T12:00:00" in raw["observed_at"]


def test_list_range_filters_by_partition_none_means_cluster_wide(
    conn: sqlite3.Connection,
) -> None:
    repo = ResourceSnapshotRepository(conn)
    # 3 snapshots in range: 2 cluster-wide (NULL), 1 on partition 'gpu'
    repo.insert(
        _make_snapshot(datetime(2026, 4, 18, 10, 0, tzinfo=UTC), partition=None)
    )
    repo.insert(
        _make_snapshot(datetime(2026, 4, 18, 11, 0, tzinfo=UTC), partition=None)
    )
    repo.insert(
        _make_snapshot(datetime(2026, 4, 18, 11, 30, tzinfo=UTC), partition="gpu")
    )
    # 1 outside range
    repo.insert(
        _make_snapshot(datetime(2026, 4, 18, 15, 0, tzinfo=UTC), partition=None)
    )
    conn.commit()

    cluster = repo.list_range(
        "2026-04-18T09:00:00.000Z",
        "2026-04-18T12:00:00.000Z",
        partition=None,
    )
    assert len(cluster) == 2
    assert all(s.partition is None for s in cluster)

    gpu = repo.list_range(
        "2026-04-18T09:00:00.000Z",
        "2026-04-18T12:00:00.000Z",
        partition="gpu",
    )
    assert len(gpu) == 1
    assert gpu[0].partition == "gpu"


def test_list_range_orders_ascending(conn: sqlite3.Connection) -> None:
    repo = ResourceSnapshotRepository(conn)
    for h in (11, 9, 10):
        repo.insert(_make_snapshot(datetime(2026, 4, 18, h, 0, tzinfo=UTC)))
    conn.commit()

    results = repo.list_range(
        "2026-04-18T00:00:00.000Z", "2026-04-18T23:59:00.000Z", partition=None
    )
    hours = [s.observed_at.hour for s in results if s.observed_at is not None]
    assert hours == sorted(hours)


def test_list_range_empty_when_no_matches(conn: sqlite3.Connection) -> None:
    repo = ResourceSnapshotRepository(conn)
    assert (
        repo.list_range(
            "2026-04-18T00:00:00.000Z",
            "2026-04-18T01:00:00.000Z",
        )
        == []
    )


def test_delete_older_than_removes_old_rows(conn: sqlite3.Connection) -> None:
    # Insert one clearly old row (raw SQL, past timestamp) and one current one.
    conn.execute(
        """
        INSERT INTO resource_snapshots
            (observed_at, partition, gpus_total, gpus_available, gpus_in_use,
             nodes_total, nodes_idle, nodes_down)
        VALUES (?, NULL, 8, 2, 6, 2, 0, 0)
        """,
        ("2020-01-01T00:00:00.000Z",),
    )
    conn.execute(
        """
        INSERT INTO resource_snapshots
            (observed_at, partition, gpus_total, gpus_available, gpus_in_use,
             nodes_total, nodes_idle, nodes_down)
        VALUES (strftime('%Y-%m-%dT%H:%M:%fZ','now'), NULL, 8, 2, 6, 2, 0, 0)
        """,
    )
    conn.commit()

    repo = ResourceSnapshotRepository(conn)
    deleted = repo.delete_older_than(30)
    conn.commit()
    assert deleted == 1

    remaining = conn.execute("SELECT COUNT(*) FROM resource_snapshots").fetchone()[0]
    assert remaining == 1


def test_delete_older_than_zero_matches_returns_zero(conn: sqlite3.Connection) -> None:
    repo = ResourceSnapshotRepository(conn)
    repo.insert(_make_snapshot(datetime.now(UTC)))
    conn.commit()

    # Very generous window — nothing should be deleted.
    assert repo.delete_older_than(3650) == 0
