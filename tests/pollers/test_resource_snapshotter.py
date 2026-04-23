"""Unit tests for :class:`srunx.pollers.resource_snapshotter.ResourceSnapshotter`.

Drives the poller with a stub monitor so no SLURM commands are invoked.
Verifies both happy-path persistence and that failures propagate out of
``run_cycle`` (so the supervisor's backoff kicks in).
"""

from __future__ import annotations

import sqlite3
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import anyio
import pytest
from srunx.db.models import ResourceSnapshot as DbResourceSnapshot

from srunx.monitor.types import ResourceSnapshot as MonitorResourceSnapshot
from srunx.pollers.resource_snapshotter import ResourceSnapshotter

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class _StubMonitor:
    """Resource monitor stub that returns a pre-baked snapshot or raises."""

    def __init__(self, snapshot: Any = None, exception: Exception | None = None):
        self._snapshot = snapshot
        self._exception = exception
        self.calls = 0

    def get_current_snapshot(self) -> Any:
        self.calls += 1
        if self._exception is not None:
            raise self._exception
        return self._snapshot


def _all_rows(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    return list(
        conn.execute(
            "SELECT partition, gpus_total, gpus_available, gpus_in_use, "
            "nodes_total, nodes_idle, nodes_down, observed_at "
            "FROM resource_snapshots ORDER BY id"
        )
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestHappyPath:
    def test_one_cycle_inserts_a_row(
        self, tmp_srunx_db: tuple[sqlite3.Connection, Path]
    ) -> None:
        conn, db_path = tmp_srunx_db

        snapshot = MonitorResourceSnapshot(
            partition="gpu",
            total_gpus=16,
            gpus_in_use=4,
            gpus_available=12,
            jobs_running=2,
            nodes_total=8,
            nodes_idle=6,
            nodes_down=0,
        )
        monitor = _StubMonitor(snapshot=snapshot)

        poller = ResourceSnapshotter(monitor, db_path=db_path)
        anyio.run(poller.run_cycle)

        rows = _all_rows(conn)
        assert len(rows) == 1
        row = rows[0]
        assert row["partition"] == "gpu"
        assert row["gpus_total"] == 16
        assert row["gpus_available"] == 12
        assert row["gpus_in_use"] == 4
        assert row["nodes_total"] == 8
        assert row["nodes_idle"] == 6
        assert row["nodes_down"] == 0
        assert monitor.calls == 1

    def test_cluster_wide_partition_none_stores_null(
        self, tmp_srunx_db: tuple[sqlite3.Connection, Path]
    ) -> None:
        conn, db_path = tmp_srunx_db

        # Monitor returns cluster-wide snapshot (partition=None).
        snapshot = MonitorResourceSnapshot(
            partition=None,
            total_gpus=32,
            gpus_in_use=8,
            gpus_available=24,
            jobs_running=3,
            nodes_total=16,
            nodes_idle=13,
            nodes_down=0,
        )
        monitor = _StubMonitor(snapshot=snapshot)

        poller = ResourceSnapshotter(monitor, db_path=db_path, partition=None)
        anyio.run(poller.run_cycle)

        rows = _all_rows(conn)
        assert len(rows) == 1
        assert rows[0]["partition"] is None
        assert rows[0]["gpus_total"] == 32
        assert rows[0]["gpus_available"] == 24

    def test_db_snapshot_passthrough(
        self, tmp_srunx_db: tuple[sqlite3.Connection, Path]
    ) -> None:
        """A monitor that already returns the DB shape is persisted verbatim."""
        conn, db_path = tmp_srunx_db

        snap = DbResourceSnapshot(
            observed_at=datetime(2026, 4, 18, 12, 0, 0, tzinfo=UTC),
            partition="debug",
            gpus_total=4,
            gpus_available=4,
            gpus_in_use=0,
            nodes_total=2,
            nodes_idle=2,
            nodes_down=0,
        )
        monitor = _StubMonitor(snapshot=snap)

        poller = ResourceSnapshotter(monitor, db_path=db_path)
        anyio.run(poller.run_cycle)

        rows = _all_rows(conn)
        assert len(rows) == 1
        assert rows[0]["partition"] == "debug"
        assert rows[0]["gpus_total"] == 4
        assert rows[0]["gpus_available"] == 4

    def test_partition_override_when_monitor_output_missing(
        self, tmp_srunx_db: tuple[sqlite3.Connection, Path]
    ) -> None:
        """Monitor output without ``partition`` falls back to constructor arg."""
        conn, db_path = tmp_srunx_db

        raw = SimpleNamespace(
            gpus_total=8,
            gpus_available=8,
            gpus_in_use=0,
            nodes_total=4,
            nodes_idle=4,
            nodes_down=0,
        )
        monitor = _StubMonitor(snapshot=raw)

        poller = ResourceSnapshotter(monitor, db_path=db_path, partition="big")
        anyio.run(poller.run_cycle)

        rows = _all_rows(conn)
        assert len(rows) == 1
        assert rows[0]["partition"] == "big"


class TestFailurePropagation:
    def test_monitor_exception_propagates_so_supervisor_backs_off(
        self, tmp_srunx_db: tuple[sqlite3.Connection, Path]
    ) -> None:
        """Exceptions from ``get_current_snapshot`` must escape ``run_cycle``.

        The :class:`PollerSupervisor` relies on exceptions to apply
        backoff — swallowing them here would mask transient SLURM outages.
        """
        _, db_path = tmp_srunx_db

        monitor = _StubMonitor(exception=RuntimeError("sinfo unreachable"))
        poller = ResourceSnapshotter(monitor, db_path=db_path)

        with pytest.raises(RuntimeError, match="sinfo unreachable"):
            anyio.run(poller.run_cycle)

        assert monitor.calls == 1

    def test_no_row_inserted_on_failure(
        self, tmp_srunx_db: tuple[sqlite3.Connection, Path]
    ) -> None:
        conn, db_path = tmp_srunx_db

        monitor = _StubMonitor(exception=RuntimeError("fail"))
        poller = ResourceSnapshotter(monitor, db_path=db_path)

        with pytest.raises(RuntimeError):
            anyio.run(poller.run_cycle)

        assert _all_rows(conn) == []


class TestProtocolShape:
    def test_implements_poller_protocol(self) -> None:
        from srunx.pollers.supervisor import Poller

        poller = ResourceSnapshotter(_StubMonitor(snapshot=None))
        assert isinstance(poller, Poller)
        assert poller.name == "resource_snapshotter"
        assert poller.interval_seconds == 300.0

    def test_interval_override(self) -> None:
        poller = ResourceSnapshotter(_StubMonitor(snapshot=None), interval_seconds=60.0)
        assert poller.interval_seconds == 60.0
