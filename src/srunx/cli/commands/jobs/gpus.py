"""``srunx gpus`` — GPU resource availability snapshot."""

import sys
from typing import Annotated

import typer
from rich.console import Console
from rich.table import Table

from srunx.cli._helpers.transport_options import LocalOpt, ProfileOpt, QuietOpt
from srunx.common.logging import get_logger

logger = get_logger(__name__)


def gpus(
    partition: Annotated[
        str | None,
        typer.Option("--partition", "-p", help="SLURM partition to query"),
    ] = None,
    format: Annotated[
        str,
        typer.Option("--format", "-f", help="Output format: table or json"),
    ] = "table",
    profile: ProfileOpt = None,
    local: LocalOpt = False,
    quiet: QuietOpt = False,
) -> None:
    """Display current GPU resource availability (aggregate snapshot).

    Produces the GPU-focused summary that used to live under
    ``srunx sinfo``. For the native-``sinfo`` partition / state /
    nodelist listing, see ``srunx sinfo``.

    With ``--profile <name>`` (or ``$SRUNX_SSH_PROFILE`` / current
    profile) the query runs against the remote cluster via the SSH
    adapter. Local mode keeps the subprocess ``sinfo`` / ``squeue``
    path.

    Examples:
        srunx gpus
        srunx gpus --partition gpu
        srunx gpus --format json
        srunx gpus --profile dgx-server --partition gpu
    """
    import json
    from typing import cast

    from srunx.observability.monitoring.resource_monitor import ResourceMonitor
    from srunx.observability.monitoring.resource_source import (
        ResourceSource,
        SSHAdapterResourceSource,
    )
    from srunx.transport import resolve_transport

    try:
        with resolve_transport(profile=profile, local=local, quiet=quiet) as rt:
            source: ResourceSource | None = None
            if rt.transport_type == "ssh":
                from srunx.slurm.ssh import SlurmSSHAdapter

                adapter = cast(SlurmSSHAdapter, rt.job_ops)
                source = SSHAdapterResourceSource(lambda: adapter)

            monitor = ResourceMonitor(min_gpus=0, partition=partition, source=source)
            snapshot = monitor.get_partition_resources()

        if format == "json":
            data = {
                "partition": snapshot.partition,
                "gpus_total": snapshot.total_gpus,
                "gpus_in_use": snapshot.gpus_in_use,
                "gpus_available": snapshot.gpus_available,
                "jobs_running": snapshot.jobs_running,
                "nodes_total": snapshot.nodes_total,
                "nodes_idle": snapshot.nodes_idle,
                "nodes_down": snapshot.nodes_down,
            }
            Console().print(json.dumps(data, indent=2))
            return

        table = Table()
        table.add_column("Metric", style="cyan")
        table.add_column("Value", justify="right", style="green")

        table.add_row("Total GPUs", str(snapshot.total_gpus))
        table.add_row("GPUs in Use", str(snapshot.gpus_in_use))
        table.add_row("GPUs Available", str(snapshot.gpus_available))
        table.add_row("", "")
        table.add_row("Running Jobs", str(snapshot.jobs_running))
        table.add_row("", "")
        table.add_row("Total Nodes", str(snapshot.nodes_total))
        table.add_row("Idle Nodes", str(snapshot.nodes_idle))
        table.add_row("Down Nodes", str(snapshot.nodes_down))

        Console().print(table)

    except Exception as e:
        logger.error(f"Error querying GPU resources: {e}")
        Console().print(f"[red]Error: {e}[/red]")
        sys.exit(1)
