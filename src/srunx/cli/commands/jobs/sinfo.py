"""``srunx sinfo`` — partition / state / nodelist listing (native-sinfo parity)."""

import sys
from typing import Annotated, Any

import typer
from rich.console import Console
from rich.table import Table

from srunx.cli._helpers.transport_options import LocalOpt, ProfileOpt, QuietOpt
from srunx.common.logging import get_logger

logger = get_logger(__name__)


# Node state colours for ``srunx sinfo`` — disjoint from the job-state
# colour map in :mod:`srunx.cli._helpers.state_colors`. SLURM uses
# lowercase node states (idle/mixed/allocated/...) that have different
# semantics from the uppercase job states (RUNNING/COMPLETED/...), so
# the two maps intentionally live apart.
_NODE_STATE_COLORS = {
    "idle": "green",
    "mixed": "yellow",
    "mix": "yellow",
    "allocated": "red",
    "alloc": "red",
    "completing": "cyan",
    "drained": "magenta",
    "drain": "magenta",
    "draining": "magenta",
    "down": "bright_red",
    "fail": "bright_red",
    "failing": "bright_red",
    "maint": "bright_black",
    "reserved": "blue",
    "future": "bright_black",
    "unknown": "bright_black",
}


def sinfo(
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
    """Display partition / node state — same information as native ``sinfo``.

    Columns mirror the default ``sinfo`` layout: ``PARTITION`` (with
    ``*`` on the default partition), ``AVAIL`` (up/down), ``TIMELIMIT``,
    ``NODES``, ``STATE``, ``NODELIST``. For the GPU-aggregate summary
    that used to live here, see ``srunx gpus``.

    With ``--profile <name>`` (or ``$SRUNX_SSH_PROFILE`` / current
    profile) the query runs against the remote cluster via the SSH
    adapter. Local mode shells out to the head-node ``sinfo`` binary.

    Examples:
        srunx sinfo
        srunx sinfo --partition gpu
        srunx sinfo --format json
        srunx sinfo --profile dgx-server --partition gpu
    """
    import json
    from typing import cast

    from srunx.slurm.partitions import (
        PartitionRow,
        fetch_sinfo_rows_local,
        fetch_sinfo_rows_ssh,
    )
    from srunx.transport import resolve_transport

    try:
        with resolve_transport(profile=profile, local=local, quiet=quiet) as rt:
            rows: list[PartitionRow]
            if rt.transport_type == "ssh":
                # Cast Protocol → concrete ``SlurmSSHAdapter`` so we
                # can reuse the adapter-scoped ``_run_slurm_cmd`` path
                # (login-shell env, SLURM PATH, I/O lock). The
                # Protocol deliberately doesn't expose SSH primitives.
                from srunx.slurm.ssh import SlurmSSHAdapter

                adapter = cast(SlurmSSHAdapter, rt.job_ops)
                rows = fetch_sinfo_rows_ssh(adapter, partition)
            else:
                rows = fetch_sinfo_rows_local(partition)

        if format == "json":
            Console().print(json.dumps([row.to_dict() for row in rows], indent=2))
            return

        _render_sinfo_table(rows)

    except Exception as e:
        logger.error(f"Error querying partition info: {e}")
        Console().print(f"[red]Error: {e}[/red]")
        sys.exit(1)


def _render_sinfo_table(rows: list[Any]) -> None:
    """Render :class:`PartitionRow` list as a Rich table.

    The shape matches native ``sinfo`` (same columns, same order) so a
    SLURM user sees familiar output. Styling uses colour on ``STATE``
    to make node health scan-able; no column is dropped or re-ordered.
    """
    table = Table()
    table.add_column("PARTITION", style="cyan")
    table.add_column("AVAIL")
    table.add_column("TIMELIMIT")
    table.add_column("NODES", justify="right")
    table.add_column("STATE")
    table.add_column("NODELIST", overflow="fold")

    for row in rows:
        partition_display = f"{row.partition}*" if row.is_default else row.partition
        avail_color = "green" if row.avail == "up" else "red"
        state_color = _NODE_STATE_COLORS.get(row.state.lower(), "white")
        table.add_row(
            partition_display,
            f"[{avail_color}]{row.avail}[/{avail_color}]",
            row.timelimit,
            str(row.nodes),
            f"[{state_color}]{row.state}[/{state_color}]",
            row.nodelist,
        )

    Console().print(table)
