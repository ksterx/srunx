"""Periodic GPU resource snapshotter (PR 2 / C.3).

Calls :meth:`ResourceMonitor.get_current_snapshot` (a blocking SLURM shell-out)
on a worker thread and persists the result into the ``resource_snapshots``
table. Exceptions propagate out of :meth:`run_cycle` so the
:class:`~srunx.observability.monitoring.pollers.supervisor.PollerSupervisor` can apply its exponential
backoff — per design.md § "Error Scenarios" #2, transient SLURM outages must
not take down the lifespan.
"""

from __future__ import annotations

import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol

import anyio

from srunx.common.logging import get_logger
from srunx.observability.storage.connection import open_connection
from srunx.observability.storage.models import ResourceSnapshot as DbResourceSnapshot
from srunx.observability.storage.repositories.resource_snapshots import (
    ResourceSnapshotRepository,
)

logger = get_logger(__name__)


class _MonitorLike(Protocol):
    """Structural contract for the injected resource monitor.

    The real implementation is
    :class:`srunx.observability.monitoring.resource_monitor.ResourceMonitor`. Tests inject
    stubs with the same shape.
    """

    def get_current_snapshot(self) -> Any: ...


class ResourceSnapshotter:
    """Capture cluster GPU state at a fixed cadence.

    Implements the :class:`~srunx.observability.monitoring.pollers.supervisor.Poller` protocol.
    """

    name: str = "resource_snapshotter"
    interval_seconds: float = 300.0

    def __init__(
        self,
        resource_monitor: _MonitorLike,
        *,
        db_path: Path | None = None,
        interval_seconds: float = 300.0,
        partition: str | None = None,
    ) -> None:
        """Initialize the snapshotter.

        Args:
            resource_monitor: Object exposing ``get_current_snapshot()``.
                Typically :class:`~srunx.observability.monitoring.resource_monitor.ResourceMonitor`.
                Construct the monitor with the target ``partition`` — this
                class does not re-scope the query.
            db_path: Override for the sqlite DB path. ``None`` resolves the
                default XDG path at connection time.
            interval_seconds: Sleep between cycles. Default 300s matches the
                design budget for time-series density.
            partition: Override for the ``partition`` column when the monitor
                output doesn't carry one. ``None`` yields a NULL column
                (cluster-wide snapshot).
        """
        self.resource_monitor = resource_monitor
        self._db_path = db_path
        self.interval_seconds = interval_seconds
        self.partition = partition

    async def run_cycle(self) -> None:
        """Fetch one snapshot from SLURM and persist it.

        Raises:
            Exception: Any exception from the resource monitor or DB layer
                is re-raised so :class:`PollerSupervisor` can back off.
        """
        start_ns = time.monotonic_ns()

        # get_current_snapshot shells out to sinfo/squeue — strictly blocking.
        raw_snapshot = await anyio.to_thread.run_sync(
            self.resource_monitor.get_current_snapshot
        )
        db_snapshot = self._to_db_snapshot(raw_snapshot)

        conn = await anyio.to_thread.run_sync(open_connection, self._db_path)
        try:
            repo = ResourceSnapshotRepository(conn)
            await anyio.to_thread.run_sync(repo.insert, db_snapshot)
        finally:
            await anyio.to_thread.run_sync(conn.close)

        elapsed_ms = (time.monotonic_ns() - start_ns) // 1_000_000
        logger.bind(
            poller=self.name,
            partition=db_snapshot.partition,
            gpus_total=db_snapshot.gpus_total,
            gpus_available=db_snapshot.gpus_available,
            elapsed_ms=elapsed_ms,
        ).info("resource snapshotter cycle complete")

    def _to_db_snapshot(self, raw: Any) -> DbResourceSnapshot:
        """Coerce the monitor output into the DB model.

        Handles two legitimate shapes:

        * A :class:`srunx.observability.monitoring.types.ResourceSnapshot` with
          ``total_gpus`` / ``timestamp`` naming.
        * An already-correctly-shaped :class:`DbResourceSnapshot`.

        Missing ``partition`` falls back to the constructor override.
        """
        if isinstance(raw, DbResourceSnapshot):
            return raw

        # Prefer attribute access so the same code path works for pydantic
        # models, dataclasses, SimpleNamespace, etc.
        gpus_total = int(self._pick(raw, ("gpus_total", "total_gpus"), default=0) or 0)
        gpus_available = int(self._pick(raw, ("gpus_available",), default=0) or 0)
        gpus_in_use = int(self._pick(raw, ("gpus_in_use",), default=0) or 0)
        nodes_total = int(self._pick(raw, ("nodes_total",), default=0) or 0)
        nodes_idle = int(self._pick(raw, ("nodes_idle",), default=0) or 0)
        nodes_down = int(self._pick(raw, ("nodes_down",), default=0) or 0)

        observed_at = self._pick(raw, ("observed_at", "timestamp"), default=None)
        if not isinstance(observed_at, datetime):
            observed_at = datetime.now(UTC)

        partition = self._pick(raw, ("partition",), default=None)
        if partition is None:
            partition = self.partition

        return DbResourceSnapshot(
            observed_at=observed_at,
            partition=partition,
            gpus_total=gpus_total,
            gpus_available=gpus_available,
            gpus_in_use=gpus_in_use,
            nodes_total=nodes_total,
            nodes_idle=nodes_idle,
            nodes_down=nodes_down,
        )

    @staticmethod
    def _pick(obj: Any, names: tuple[str, ...], default: Any = None) -> Any:
        """Return the first attribute from ``names`` present on ``obj``."""
        for name in names:
            if hasattr(obj, name):
                return getattr(obj, name)
            if isinstance(obj, dict) and name in obj:
                return obj[name]
        return default
