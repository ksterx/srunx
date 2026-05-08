"""Pluggable resource-query backend for :class:`ResourceMonitor`.

Before this module, ``ResourceMonitor`` always shelled out to local
``sinfo`` / ``squeue`` via ``subprocess.run``. That assumed srunx was
running on a SLURM head/login node — false on a developer laptop that
talks to a remote cluster over SSH. ``srunx ui`` would then spam
``FileNotFoundError: 'sinfo'`` every ``ResourceSnapshotter`` cycle.

This module decouples "fetch cluster state" from "interpret it":

* :class:`ResourceSource` is a structural contract.
* :class:`SSHAdapterResourceSource` adapts the existing
  :class:`srunx.slurm.clients.ssh.SlurmSSHClient.get_resources` so
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

from collections.abc import Callable
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

from srunx.observability.monitoring.types import ResourceSnapshot

if TYPE_CHECKING:
    from srunx.slurm.clients.ssh import SlurmSSHClient


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
    """``ResourceSource`` implementation that reuses ``SlurmSSHClient``.

    Takes an **adapter provider** (a callable returning the current
    adapter or ``None``), not a captured reference. That matters
    because ``/api/config/ssh/profiles/.../connect`` atomically swaps
    the process-global adapter via ``deps.swap_adapter``; a cached
    reference would keep pointing at the disconnected old adapter
    while ``/api/resources`` and every other route use the new one.
    Resolving per-call keeps the snapshotter in sync with profile
    switches.

    Semantics:

    * Single partition → ``adapter.get_resources(p)`` returns a
      one-element list; we coerce it to a snapshot. The adapter does
      **not** catch per-partition errors for this path, so an SSH or
      SLURM failure propagates to the poller supervisor (desired —
      transient errors should trigger backoff, not silently zero).
    * Cluster-wide (``partition=None``) → we delegate to
      ``adapter.get_cluster_snapshot()``, which runs a single
      cluster-wide ``sinfo`` + ``squeue`` pair with cross-partition
      node dedup. The previous "sum per-partition dicts" approach
      had two bugs: (a) nodes listed under multiple partitions were
      double-counted; (b) the multi-partition path catches errors per
      partition and returns partial data, so transient failures got
      silently persisted as understated totals.
    """

    def __init__(
        self,
        adapter_provider: Callable[[], SlurmSSHClient | None],
    ) -> None:
        self._provider = adapter_provider

    def get_snapshot(self, partition: str | None) -> ResourceSnapshot:
        adapter = self._provider()
        if adapter is None:
            # Raise so the ``PollerSupervisor`` backs off. Returning a
            # zero snapshot would silently persist a wrong row every
            # cycle until a profile is re-attached.
            raise RuntimeError("No SLURM adapter available for ResourceSource")

        if partition is None:
            return _dict_to_snapshot(adapter.get_cluster_snapshot(), None)

        raw = adapter.get_resources(partition)
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
        return _dict_to_snapshot(raw[0], partition)


def _dict_to_snapshot(row: dict[str, Any], partition: str | None) -> ResourceSnapshot:
    """Coerce a ``SlurmSSHClient.get_resources`` row into a snapshot."""
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
