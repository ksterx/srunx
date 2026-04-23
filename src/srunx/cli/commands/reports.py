"""DB-backed history / reporting commands: sacct, sreport."""

import sys
from typing import Annotated

import typer
from rich.console import Console
from rich.table import Table

from srunx.cli._helpers.transport_options import LocalOpt, ProfileOpt, QuietOpt
from srunx.logging import get_logger

logger = get_logger(__name__)


def sacct(
    job_filter: Annotated[
        list[int] | None,
        typer.Option(
            "-j",
            "--jobs",
            help=(
                "Filter to one or more specific job IDs. Replaces the old "
                "``srunx status <id>`` command for finished jobs — equivalent "
                "to ``sacct -j ID``."
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
    """Show job execution history.

    With ``--profile <name>`` (or ``$SRUNX_SSH_PROFILE`` / current
    profile), results are filtered to jobs that ran against that
    cluster. Without a transport selector, history from every
    transport is shown — matching legacy behaviour.
    """
    try:
        from srunx.db.cli_helpers import list_recent_jobs
        from srunx.transport import (
            emit_transport_banner,
            peek_scheduler_key,
            resolve_transport_source,
        )

        # sacct is a pure DB query — no SSH connection needed even
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
        table = Table(title=f"Job History (Last {len(jobs)} jobs)")
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


def sreport(
    from_date: Annotated[
        str | None, typer.Option("--from", help="Start date (YYYY-MM-DD)")
    ] = None,
    to_date: Annotated[
        str | None, typer.Option("--to", help="End date (YYYY-MM-DD)")
    ] = None,
    workflow: Annotated[
        str | None, typer.Option("--workflow", help="Workflow name")
    ] = None,
    profile: ProfileOpt = None,
    local: LocalOpt = False,
    quiet: QuietOpt = False,
) -> None:
    """Generate job execution report.

    With ``--profile <name>`` (or ``$SRUNX_SSH_PROFILE`` / current
    profile), aggregates are scoped to jobs that ran on that
    cluster. Without a transport selector, every transport is
    aggregated together — matching legacy behaviour.
    """
    try:
        from srunx.db.cli_helpers import compute_job_stats, compute_workflow_stats
        from srunx.transport import (
            emit_transport_banner,
            peek_scheduler_key,
            resolve_transport_source,
        )

        # sreport is a pure DB query — see ``sacct`` for the rationale
        # behind the ``peek_scheduler_key`` shortcut.
        source = resolve_transport_source(profile=profile, local=local)
        scheduler_key = peek_scheduler_key(profile=profile, local=local)
        emit_transport_banner(label=scheduler_key, source=source, quiet=quiet)

        if workflow:
            stats = compute_workflow_stats(workflow, scheduler_key=scheduler_key)

            console = Console()
            console.print(f"\n[bold cyan]Workflow Report: {workflow}[/bold cyan]")
            console.print(f"Total Jobs: {stats['total_jobs']}")
            if stats["avg_duration_seconds"]:
                mins = int(stats["avg_duration_seconds"] / 60)
                console.print(f"Average Duration: {mins} minutes")
            console.print(f"First Submitted: {stats['first_submitted']}")
            console.print(f"Last Submitted: {stats['last_submitted']}\n")

        else:
            stats = compute_job_stats(
                from_date=from_date,
                to_date=to_date,
                scheduler_key=scheduler_key,
            )

            console = Console()
            console.print("\n[bold cyan]Job Execution Report[/bold cyan]")

            if from_date or to_date:
                date_range = []
                if from_date:
                    date_range.append(f"From: {from_date}")
                if to_date:
                    date_range.append(f"To: {to_date}")
                console.print(f"[yellow]{' | '.join(date_range)}[/yellow]\n")

            # Summary table
            summary_table = Table(title="Summary")
            summary_table.add_column("Metric", style="cyan")
            summary_table.add_column("Value", style="green", justify="right")

            summary_table.add_row("Total Jobs", str(stats["total_jobs"]))

            if stats["avg_duration_seconds"]:
                mins = int(stats["avg_duration_seconds"] / 60)
                summary_table.add_row("Average Duration", f"{mins} minutes")

            summary_table.add_row(
                "Total GPU Hours", f"{stats['total_gpu_hours']:.1f} hours"
            )

            console.print(summary_table)

            # Status breakdown
            if stats["jobs_by_status"]:
                console.print()
                status_table = Table(title="Jobs by Status")
                status_table.add_column("Status", style="cyan")
                status_table.add_column("Count", style="green", justify="right")

                for status, count in stats["jobs_by_status"].items():
                    status_table.add_row(status, str(count))

                console.print(status_table)

            console.print()

    except Exception as e:
        logger.error(f"Error generating report: {e}")
        sys.exit(1)
