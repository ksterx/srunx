"""Repository for the ``resource_snapshots`` table.

See design.md § ``ResourceSnapshotRepository``. Writes are performed by
:class:`~srunx.pollers.resource_snapshotter.ResourceSnapshotter`; reads
serve history APIs and the Phase-2 scheduled reporter. The
``gpu_utilization`` column is a SQL generated column and never written
directly.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from srunx.observability.storage.models import ResourceSnapshot
from srunx.observability.storage.repositories.base import BaseRepository, now_iso


class ResourceSnapshotRepository(BaseRepository):
    """CRUD for the ``resource_snapshots`` table."""

    DATETIME_FIELDS = ("observed_at",)

    _COLUMNS = (
        "id",
        "observed_at",
        "partition",
        "gpus_total",
        "gpus_available",
        "gpus_in_use",
        "nodes_total",
        "nodes_idle",
        "nodes_down",
        "gpu_utilization",
    )

    def insert(self, snapshot: ResourceSnapshot) -> int:
        """Insert a snapshot row and return its ``id``.

        All columns are written except ``gpu_utilization``, which is a
        STORED generated column. ``observed_at`` is serialized via
        :func:`now_iso` semantics when it arrives as a ``datetime``.
        """
        observed_at: Any = snapshot.observed_at
        if isinstance(observed_at, datetime):
            observed_at = observed_at.isoformat(timespec="milliseconds").replace(
                "+00:00", "Z"
            )
        elif observed_at is None:
            observed_at = now_iso()

        cur = self.conn.execute(
            """
            INSERT INTO resource_snapshots (
                observed_at, partition,
                gpus_total, gpus_available, gpus_in_use,
                nodes_total, nodes_idle, nodes_down
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                observed_at,
                snapshot.partition,
                snapshot.gpus_total,
                snapshot.gpus_available,
                snapshot.gpus_in_use,
                snapshot.nodes_total,
                snapshot.nodes_idle,
                snapshot.nodes_down,
            ),
        )
        return int(cur.lastrowid or 0)

    def list_range(
        self,
        from_at: str,
        to_at: str,
        partition: str | None = None,
    ) -> list[ResourceSnapshot]:
        """Return snapshots in ``[from_at, to_at)`` for the given partition.

        ``partition=None`` selects cluster-wide snapshots (rows where the
        ``partition`` column is NULL), not all partitions.
        """
        if partition is None:
            sql = (
                f"SELECT {', '.join(self._COLUMNS)} FROM resource_snapshots "
                "WHERE observed_at >= ? AND observed_at < ? "
                "AND partition IS NULL "
                "ORDER BY observed_at ASC"
            )
            params: list[Any] = [from_at, to_at]
        else:
            sql = (
                f"SELECT {', '.join(self._COLUMNS)} FROM resource_snapshots "
                "WHERE observed_at >= ? AND observed_at < ? "
                "AND partition = ? "
                "ORDER BY observed_at ASC"
            )
            params = [from_at, to_at, partition]

        rows = self.conn.execute(sql, params).fetchall()
        return [self._row_to_model(r, ResourceSnapshot) for r in rows if r is not None]  # type: ignore[misc]

    def delete_older_than(self, days: int) -> int:
        """Delete snapshots older than ``days`` days. Returns deleted count.

        Uses SQLite's ``strftime`` to compute the cutoff so the comparison
        is symmetric with :func:`now_iso`.
        """
        cur = self.conn.execute(
            """
            DELETE FROM resource_snapshots
            WHERE observed_at < strftime('%Y-%m-%dT%H:%M:%fZ','now',?)
            """,
            (f"-{int(days)} days",),
        )
        return cur.rowcount
