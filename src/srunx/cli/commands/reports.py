"""Job-history commands.

Two related-but-distinct surfaces live here:

* :func:`history` — srunx's own SQLite history. Only shows jobs
  submitted via srunx itself. Works even on clusters where SLURM
  accounting is disabled.
* :func:`sacct` — real-SLURM ``sacct`` wrapper. Queries the cluster
  accounting DB and thus sees every job SLURM has seen (including
  manual ``sbatch`` runs). Falls flat if the cluster has no
  ``slurmdbd``.

``sreport`` was dropped earlier; see commit history.
"""

import sys
from typing import Annotated

import typer
from rich.console import Console
from rich.table import Table

from srunx.cli._helpers.state_colors import colorize_state
from srunx.cli._helpers.transport_options import LocalOpt, ProfileOpt, QuietOpt
from srunx.common.logging import get_logger

logger = get_logger(__name__)


def history(
    job_filter: Annotated[
        list[int] | None,
        typer.Option(
            "-j",
            "--jobs",
            help=(
                "Filter to one or more specific job IDs. Replaces the old "
                "``srunx status <id>`` command for finished jobs."
            ),
        ),
    ] = None,
    limit: Annotated[
        int, typer.Option("--limit", "-n", help="Number of jobs to show")
    ] = 50,
    profile: ProfileOpt = None,
    local: LocalOpt = False,
    quiet: QuietOpt = False,
) -> None:
    """Show srunx's own submission history.

    Lists jobs that srunx itself submitted (from the local SQLite at
    ``$XDG_CONFIG_HOME/srunx/srunx.db``). Jobs submitted outside srunx
    — e.g. from a manual ``sbatch`` on the cluster — are **not**
    listed. Use the cluster's own ``sacct`` for that.

    With ``--profile <name>`` (or ``$SRUNX_SSH_PROFILE`` / current
    profile), results are filtered to jobs that ran against that
    cluster. Without a transport selector, history from every
    transport is shown — matching legacy behaviour.
    """
    try:
        from srunx.observability.storage.cli_helpers import list_recent_jobs
        from srunx.transport import (
            emit_transport_banner,
            peek_scheduler_key,
            resolve_transport_source,
        )

        # history is a pure DB query — no SSH connection needed even
        # for SSH profiles. ``peek_scheduler_key`` gives us the WHERE
        # filter without paying the round-trip cost of opening an
        # SSH adapter just to read the local SQLite history.
        source = resolve_transport_source(profile=profile, local=local)
        scheduler_key = peek_scheduler_key(profile=profile, local=local)
        emit_transport_banner(label=scheduler_key, source=source, quiet=quiet)

        # ``-j`` is pushed down into the SQL query so it finds jobs
        # older than ``--limit``. Codex follow-up #2 on PR #134.
        wanted_ids = [int(j) for j in job_filter] if job_filter else None
        jobs = list_recent_jobs(
            limit=limit, job_ids=wanted_ids, scheduler_key=scheduler_key
        )

        if not jobs:
            console = Console()
            console.print("[yellow]No job history found[/yellow]")
            return

        console = Console()
        table = Table()
        table.add_column("Job ID", style="cyan")
        table.add_column("Name", style="magenta")
        table.add_column("Status", style="green")
        table.add_column("Submitted", style="yellow")
        table.add_column("Duration", justify="right")
        table.add_column("GPUs", justify="right")

        for job in jobs:
            duration = ""
            if job["duration_seconds"]:
                mins, secs = divmod(int(job["duration_seconds"]), 60)
                hours, mins = divmod(mins, 60)
                if hours > 0:
                    duration = f"{hours}h {mins}m"
                elif mins > 0:
                    duration = f"{mins}m {secs}s"
                else:
                    duration = f"{secs}s"

            submitted_at = job["submitted_at"]
            if submitted_at:
                # Parse and format date
                from datetime import datetime

                dt = datetime.fromisoformat(submitted_at)
                submitted_at = dt.strftime("%Y-%m-%d %H:%M")

            table.add_row(
                str(job["job_id"]),
                job["job_name"],
                job["status"],
                submitted_at,
                duration,
                str(job["gpus_per_node"] or 0),
            )

        console.print(table)

    except Exception as e:
        logger.error(f"Error retrieving job history: {e}")
        sys.exit(1)


def sacct(
    job_filter: Annotated[
        list[int] | None,
        typer.Option(
            "-j",
            "--jobs",
            help="Filter to one or more specific job IDs (sacct -j).",
        ),
    ] = None,
    user: Annotated[
        str | None,
        typer.Option(
            "-u",
            "--user",
            help=(
                "Filter to a single username. Omit (and omit -a) to use "
                "sacct's implicit current-user default."
            ),
        ),
    ] = None,
    all_users: Annotated[
        bool,
        typer.Option(
            "-a",
            "--allusers",
            help="Show every user's jobs (overrides -u).",
        ),
    ] = False,
    start_time: Annotated[
        str | None,
        typer.Option(
            "-S",
            "--starttime",
            help="sacct --starttime (e.g. '2026-04-20', 'now-1day').",
        ),
    ] = None,
    end_time: Annotated[
        str | None,
        typer.Option("-E", "--endtime", help="sacct --endtime."),
    ] = None,
    state: Annotated[
        str | None,
        typer.Option(
            "-s",
            "--state",
            help="Comma-separated states (e.g. 'FAILED,TIMEOUT').",
        ),
    ] = None,
    partition: Annotated[
        str | None,
        typer.Option("-p", "--partition", help="Filter to a single partition."),
    ] = None,
    show_steps: Annotated[
        bool,
        typer.Option(
            "--show-steps",
            help=(
                "Include job-step rows (.batch / .extern / .N). Hidden by "
                "default because the parent row already summarises them."
            ),
        ),
    ] = False,
    show_account: Annotated[
        bool,
        typer.Option("--show-account", help="Add the Account column."),
    ] = False,
    show_start_end: Annotated[
        bool,
        typer.Option(
            "--show-times",
            help="Add the Submit / Start / End time columns.",
        ),
    ] = False,
    format: Annotated[
        str,
        typer.Option("--format", "-f", help="Output format: table or json."),
    ] = "table",
    profile: ProfileOpt = None,
    local: LocalOpt = False,
    quiet: QuietOpt = False,
) -> None:
    """Query SLURM accounting via the real ``sacct`` binary.

    Unlike :func:`history` (which reads srunx's own SQLite), this
    shells out to ``sacct`` on the cluster, so it sees **every** job
    SLURM has accounted for — including manual ``sbatch`` runs done
    outside srunx. Requires a cluster with ``slurmdbd`` accounting
    enabled; otherwise the output will be empty.

    Default columns: Job ID, User, Name, Partition, State, ExitCode,
    Elapsed. Use ``--show-account`` / ``--show-times`` to add more.
    ``--format json`` always emits every field.

    Examples:
        srunx sacct
        srunx sacct -j 12345
        srunx sacct -u alice -s FAILED
        srunx sacct -a -S now-1day
        srunx sacct --show-steps
        srunx sacct --profile dgx -j 9876 --format json
    """
    import json
    from typing import cast

    from srunx.slurm.accounting import (
        SacctRow,
        fetch_sacct_rows_local,
        fetch_sacct_rows_ssh,
        filter_out_steps,
    )
    from srunx.transport import resolve_transport

    try:
        job_ids = [int(j) for j in job_filter] if job_filter else None
        with resolve_transport(profile=profile, local=local, quiet=quiet) as rt:
            rows: list[SacctRow]
            if rt.transport_type == "ssh":
                from srunx.slurm.clients.ssh import SlurmSSHClient

                adapter = cast(SlurmSSHClient, rt.job_ops)
                rows = fetch_sacct_rows_ssh(
                    adapter,
                    job_ids=job_ids,
                    user=user,
                    all_users=all_users,
                    start_time=start_time,
                    end_time=end_time,
                    state=state,
                    partition=partition,
                )
            else:
                rows = fetch_sacct_rows_local(
                    job_ids=job_ids,
                    user=user,
                    all_users=all_users,
                    start_time=start_time,
                    end_time=end_time,
                    state=state,
                    partition=partition,
                )

        if not show_steps:
            rows = filter_out_steps(rows)

        if format == "json":
            Console().print(json.dumps([row.to_dict() for row in rows], indent=2))
            return

        if not rows:
            Console().print("[yellow]No accounting records[/yellow]")
            return

        table = Table()
        table.add_column("Job ID", style="cyan")
        table.add_column("User")
        table.add_column("Name", style="magenta", overflow="fold")
        table.add_column("Partition")
        if show_account:
            table.add_column("Account")
        table.add_column("State")
        table.add_column("ExitCode", justify="right")
        table.add_column("Elapsed", justify="right")
        if show_start_end:
            table.add_column("Submit")
            table.add_column("Start")
            table.add_column("End")

        for row in rows:
            cells: list[str] = [
                row.job_id,
                row.user or "-",
                row.job_name,
                row.partition or "-",
            ]
            if show_account:
                cells.append(row.account or "-")
            cells += [
                colorize_state(row.state or "UNKNOWN"),
                row.exit_code or "-",
                row.elapsed or "-",
            ]
            if show_start_end:
                cells += [
                    row.submit or "-",
                    row.start or "-",
                    row.end or "-",
                ]
            table.add_row(*cells)

        Console().print(table)

    except Exception as e:
        logger.error(f"Error running sacct: {e}")
        Console().print(f"[red]Error: {e}[/red]")
        sys.exit(1)
