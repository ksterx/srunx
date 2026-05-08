"""MCP tool: GPU/node resource queries."""

from __future__ import annotations

from typing import Any

from srunx.mcp.app import mcp
from srunx.mcp.helpers import err, get_ssh_client, ok, validate_partition


@mcp.tool()
def get_resources(
    partition: str | None = None, use_ssh: bool = False
) -> dict[str, Any]:
    """Get current GPU and node resource availability on the SLURM cluster.

    Args:
        partition: Specific partition to check (None for all partitions)
        use_ssh: If true, query resources via SSH on remote cluster
    """
    try:
        if partition:
            validate_partition(partition)
        if use_ssh:
            ssh_client = get_ssh_client()
            with ssh_client:
                partition_flag = f"-p {partition}" if partition else ""
                stdout, stderr, rc = ssh_client.slurm.execute_slurm_command(
                    f'sinfo {partition_flag} -o "%n %G %T %P" --noheader'
                )
                if rc != 0:
                    return err(f"sinfo failed: {stderr}")
                return ok(partition=partition, raw_output=stdout.strip())

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
