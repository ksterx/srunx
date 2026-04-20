"""Pluggable resource-query backend for :class:`ResourceMonitor`.

Before this module, ``ResourceMonitor`` always shelled out to local
``sinfo`` / ``squeue`` via ``subprocess.run``. That assumed srunx was
running on a SLURM head/login node — false on a developer laptop that
talks to a remote cluster over SSH. ``srunx ui`` would then spam
``FileNotFoundError: 'sinfo'`` every ``ResourceSnapshotter`` cycle.

This module decouples "fetch cluster state" from "interpret it":

* :class:`ResourceSource` is a structural contract.
* :class:`SSHAdapterResourceSource` adapts the existing
  :class:`srunx.web.ssh_adapter.SlurmSSHAdapter.get_resources` so
  remote clusters flow through the same code path already used by
  ``/api/resources``. When ``partition=None`` it sums across every
  partition so the cluster-wide snapshot matches the local subprocess
  behaviour.

``ResourceMonitor`` keeps its local subprocess path as the default.
Callers (notably :func:`srunx.web.app.create_app`) inject an
adapter-backed source when one is available, and the snapshotter
starts producing rows against the remote cluster.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

from srunx.monitor.types import ResourceSnapshot

if TYPE_CHECKING:
    from srunx.web.ssh_adapter import SlurmSSHAdapter


@runtime_checkable
class ResourceSource(Protocol):
    """Pluggable backend for :class:`ResourceMonitor` partition queries.

    Implementations hide the transport (local subprocess vs remote
    SSH) so ``ResourceMonitor`` can stay ignorant of how the numbers
    are produced. ``partition=None`` means "cluster-wide"; concrete
    implementations aggregate across partitions.
    """

    def get_snapshot(self, partition: str | None) -> ResourceSnapshot: ...


class SSHAdapterResourceSource:
    """``ResourceSource`` implementation that reuses ``SlurmSSHAdapter``.

    The adapter's ``get_resources`` already parses ``sinfo`` / ``squeue``
    output on the remote side and returns a list of per-partition
    dicts. For the cluster-wide case (``partition=None``) we sum those
    dicts; for a single partition we take the one entry.
    """

    def __init__(self, adapter: SlurmSSHAdapter) -> None:
        self._adapter = adapter

    def get_snapshot(self, partition: str | None) -> ResourceSnapshot:
        raw = self._adapter.get_resources(partition)
        if not raw:
            return ResourceSnapshot(
                partition=partition,
                total_gpus=0,
                gpus_in_use=0,
                gpus_available=0,
                jobs_running=0,
                nodes_total=0,
                nodes_idle=0,
                nodes_down=0,
            )

        if partition is not None:
            # Single-partition queries return a one-element list.
            return _dict_to_snapshot(raw[0], partition)

        # Cluster-wide: the adapter returns one dict per partition. Sum
        # across them so the downstream snapshot matches the semantics
        # of ``sinfo`` without a ``-p`` filter (which is what the local
        # subprocess path produces).
        totals: dict[str, int] = {
            "total_gpus": 0,
            "gpus_in_use": 0,
            "gpus_available": 0,
            "jobs_running": 0,
            "nodes_total": 0,
            "nodes_idle": 0,
            "nodes_down": 0,
        }
        for row in raw:
            for key in totals:
                value = row.get(key, 0) or 0
                totals[key] += int(value)

        return ResourceSnapshot(partition=None, **totals)


def _dict_to_snapshot(row: dict[str, Any], partition: str | None) -> ResourceSnapshot:
    """Coerce a ``SlurmSSHAdapter.get_resources`` row into a snapshot."""
    return ResourceSnapshot(
        partition=row.get("partition", partition),
        total_gpus=int(row.get("total_gpus", 0) or 0),
        gpus_in_use=int(row.get("gpus_in_use", 0) or 0),
        gpus_available=int(row.get("gpus_available", 0) or 0),
        jobs_running=int(row.get("jobs_running", 0) or 0),
        nodes_total=int(row.get("nodes_total", 0) or 0),
        nodes_idle=int(row.get("nodes_idle", 0) or 0),
        nodes_down=int(row.get("nodes_down", 0) or 0),
    )
