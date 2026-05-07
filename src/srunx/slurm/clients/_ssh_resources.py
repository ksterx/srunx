"""Cluster resource queries (sinfo + squeue) for the SSH SLURM client.

Free functions taking the :class:`SlurmSSHClient` as the first argument.
The client's bound methods (``get_resources`` / ``get_cluster_snapshot``)
are 1-line forwards into here so the queries can be reused (and tested)
without spinning up a class instance — the SLURM CLI parsing is the
substance, the connection lock is plumbing.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from srunx.common.logging import get_logger
from srunx.slurm.clients._ssh_helpers import (
    _UNAVAILABLE_STATES,
    _run_slurm_cmd,
    _validate_identifier,
)
from srunx.slurm.parsing import GPU_TRES_RE

if TYPE_CHECKING:
    from srunx.slurm.clients.ssh import SlurmSSHClient

logger = get_logger(__name__)


def list_partition_resources(
    client: SlurmSSHClient, partition: str | None = None
) -> list[dict[str, Any]]:
    """Get cluster resource information via sinfo + squeue.

    The ``partition=None`` case is the per-partition listing used
    by ``/api/resources`` — errors are caught per-partition so a
    single broken partition doesn't sink the whole dashboard call.
    Callers that need a single aggregated cluster-wide snapshot
    (e.g. the resource snapshotter) should use
    :func:`cluster_snapshot` instead — that path fails closed
    and dedups nodes across partitions, which summing this list
    does not.
    """
    if partition:
        _validate_identifier(partition, "partition")
        return [partition_resources(client, partition)]

    output = _run_slurm_cmd(client, "sinfo -o '%P' --noheader")
    partitions = {
        line.strip().rstrip("*") for line in output.strip().splitlines() if line.strip()
    }

    results: list[dict[str, Any]] = []
    for p in sorted(partitions):
        try:
            results.append(partition_resources(client, p))
        except Exception as e:  # noqa: BLE001 — diagnostic
            logger.warning("Failed to get resources for partition %s: %s", p, e)
            continue
    return results


def cluster_snapshot(client: SlurmSSHClient) -> dict[str, Any]:
    """Return a single cluster-wide resource snapshot dict.

    Used by the resource snapshotter for a single row in
    ``resource_snapshots``. Differs from
    :func:`list_partition_resources` (with ``partition=None``) in two
    important ways:

    1. Runs ONE ``sinfo`` (no ``-p`` filter) and ONE ``squeue``,
       then dedups nodes by name via ``seen_nodes``. Summing the
       per-partition output would double-count any node that belongs
       to multiple partitions (a common SLURM setup — e.g. ``debug``
       and ``gpu`` sharing the same physical nodes).
    2. Exceptions propagate instead of being swallowed per
       partition. Transient SSH/SLURM failures surface to the
       poller supervisor for exponential backoff rather than
       silently writing understated totals to the DB.

    Returned keys match :func:`partition_resources` with
    ``partition=None``.
    """
    sinfo_output = _run_slurm_cmd(client, 'sinfo -o "%n %G %T" --noheader')

    nodes_total = 0
    nodes_idle = 0
    nodes_down = 0
    total_gpus = 0
    seen_nodes: set[str] = set()

    for line in sinfo_output.strip().splitlines():
        parts = line.split()
        if len(parts) < 3:
            continue
        node_name, gres, state = parts[0], parts[1], parts[2].lower()
        if node_name in seen_nodes:
            continue
        seen_nodes.add(node_name)
        nodes_total += 1

        if any(s in state for s in _UNAVAILABLE_STATES):
            nodes_down += 1
            continue
        if "idle" in state:
            nodes_idle += 1

        if gres and gres != "(null)":
            for entry in gres.split(","):
                gpu_match = GPU_TRES_RE.search(entry)
                if gpu_match:
                    total_gpus += int(gpu_match.group(1))

    squeue_output = _run_slurm_cmd(client, 'squeue -o "%i %T %b %D" --noheader')
    gpus_in_use = 0
    jobs_running = 0
    for line in squeue_output.strip().splitlines():
        parts = line.split()
        if len(parts) < 2 or parts[1] != "RUNNING":
            continue
        jobs_running += 1
        if len(parts) >= 3:
            gpu_match = GPU_TRES_RE.search(parts[2])
            if gpu_match:
                per_node_gpus = int(gpu_match.group(1))
                num_nodes = 1
                if len(parts) >= 4 and parts[3].isdigit():
                    num_nodes = int(parts[3])
                gpus_in_use += per_node_gpus * num_nodes

    gpus_available = max(0, total_gpus - gpus_in_use)
    gpu_utilization = gpus_in_use / total_gpus if total_gpus > 0 else 0.0

    return {
        "timestamp": datetime.now(UTC).isoformat(),
        "partition": None,
        "total_gpus": total_gpus,
        "gpus_in_use": gpus_in_use,
        "gpus_available": gpus_available,
        "jobs_running": jobs_running,
        "nodes_total": nodes_total,
        "nodes_idle": nodes_idle,
        "nodes_down": nodes_down,
        "gpu_utilization": gpu_utilization,
        "has_available_gpus": gpus_available > 0,
    }


def partition_resources(client: SlurmSSHClient, partition: str) -> dict[str, Any]:
    """Get resources for a single partition."""
    _validate_identifier(partition, "partition")

    sinfo_output = _run_slurm_cmd(
        client,
        f'sinfo -o "%n %G %T" --noheader -p {partition}',
    )

    nodes_total = 0
    nodes_idle = 0
    nodes_down = 0
    total_gpus = 0
    seen_nodes: set[str] = set()

    for line in sinfo_output.strip().splitlines():
        parts = line.split()
        if len(parts) < 3:
            continue
        node_name, gres, state = parts[0], parts[1], parts[2].lower()
        if node_name in seen_nodes:
            continue
        seen_nodes.add(node_name)
        nodes_total += 1

        if any(s in state for s in _UNAVAILABLE_STATES):
            nodes_down += 1
            continue
        if "idle" in state:
            nodes_idle += 1

        if gres and gres != "(null)":
            for entry in gres.split(","):
                gpu_match = GPU_TRES_RE.search(entry)
                if gpu_match:
                    total_gpus += int(gpu_match.group(1))

    # squeue for GPU usage — include %D (node count) to handle multi-node jobs.
    # %b is TRES_PER_NODE, so actual GPU usage = per_node_gpus * num_nodes.
    squeue_output = _run_slurm_cmd(
        client,
        f'squeue -o "%i %T %b %D" --noheader -p {partition}',
    )

    gpus_in_use = 0
    jobs_running = 0
    for line in squeue_output.strip().splitlines():
        parts = line.split()
        if len(parts) < 2 or parts[1] != "RUNNING":
            continue
        jobs_running += 1
        if len(parts) >= 3:
            gpu_match = GPU_TRES_RE.search(parts[2])
            if gpu_match:
                per_node_gpus = int(gpu_match.group(1))
                num_nodes = 1
                if len(parts) >= 4 and parts[3].isdigit():
                    num_nodes = int(parts[3])
                gpus_in_use += per_node_gpus * num_nodes

    gpus_available = max(0, total_gpus - gpus_in_use)
    gpu_utilization = gpus_in_use / total_gpus if total_gpus > 0 else 0.0

    return {
        "timestamp": datetime.now(UTC).isoformat(),
        "partition": partition,
        "total_gpus": total_gpus,
        "gpus_in_use": gpus_in_use,
        "gpus_available": gpus_available,
        "jobs_running": jobs_running,
        "nodes_total": nodes_total,
        "nodes_idle": nodes_idle,
        "nodes_down": nodes_down,
        "gpu_utilization": gpu_utilization,
        "has_available_gpus": gpus_available > 0,
    }
