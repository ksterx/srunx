"""MCP tool: GPU/node resource queries."""

from __future__ import annotations

from typing import Any, cast

from srunx.mcp.app import mcp
from srunx.mcp.helpers import err, ok, validate_partition
from srunx.mcp.transport import mcp_transport


@mcp.tool()
def get_resources(
    partition: str | None = None, transport: str | None = None
) -> dict[str, Any]:
    """Get current GPU and node resource availability on the SLURM cluster.

    Args:
        partition: Specific partition to check (None for all partitions)
        transport: Cluster selector — omit / "local" for local SLURM, or an
            SSH profile name to query that remote cluster.
    """
    try:
        if partition:
            validate_partition(partition)
        with mcp_transport(transport) as rt:
            # Resource queries are not part of the JobOperations Protocol
            # (the local client aggregates via ResourceMonitor; the SSH
            # adapter exposes get_resources). So branch on the resolved
            # transport rather than calling a uniform method.
            if rt.transport_type == "ssh":
                from srunx.slurm.clients.ssh import SlurmSSHClient

                adapter = cast(SlurmSSHClient, rt.job_ops)
                return ok(
                    partition=partition, resources=adapter.get_resources(partition)
                )

            from srunx.observability.monitoring.resource_monitor import ResourceMonitor

            monitor = ResourceMonitor(min_gpus=0, partition=partition)
            snapshot = monitor.get_partition_resources()
            return ok(
                partition=snapshot.partition,
                total_gpus=snapshot.total_gpus,
                gpus_in_use=snapshot.gpus_in_use,
                gpus_available=snapshot.gpus_available,
                gpu_utilization=round(snapshot.gpu_utilization, 3),
                jobs_running=snapshot.jobs_running,
                nodes_total=snapshot.nodes_total,
                nodes_idle=snapshot.nodes_idle,
                nodes_down=snapshot.nodes_down,
            )
    except Exception as e:
        return err(str(e))
