"""Monitor subcommands for jobs, resources, and cluster status."""

import sys
from typing import Annotated

import typer
from rich.console import Console

from srunx.callbacks import Callback, SlackCallback
from srunx.client import Slurm
from srunx.monitor.job_monitor import JobMonitor
from srunx.monitor.report_types import ReportConfig
from srunx.monitor.resource_monitor import ResourceMonitor
from srunx.monitor.scheduler import ScheduledReporter
from srunx.monitor.types import MonitorConfig, WatchMode

# Create monitor subcommand app
monitor_app = typer.Typer(
    name="monitor",
    help="Monitor jobs, resources, or cluster with unified subcommands",
    context_settings={"help_option_names": ["-h", "--help"]},
)


@monitor_app.command("jobs")
def monitor_jobs(
    job_ids: Annotated[
        list[int] | None,
        typer.Argument(help="Job IDs to monitor (space-separated)"),
    ] = None,
    all_jobs: Annotated[
        bool,
        typer.Option("--all", "-a", help="Monitor all user jobs"),
    ] = False,
    schedule: Annotated[
        str | None,
        typer.Option(
            "--schedule",
            "-s",
            help="Schedule for periodic reports (e.g., '10m', '1h')",
        ),
    ] = None,
    interval: Annotated[
        int,
        typer.Option("--interval", "-i", help="Polling interval in seconds"),
    ] = 60,
    timeout: Annotated[
        int | None,
        typer.Option("--timeout", "-t", help="Timeout in seconds (None = no timeout)"),
    ] = None,
    notify: Annotated[
        str | None,
        typer.Option("--notify", "-n", help="Slack webhook URL for notifications"),
    ] = None,
    continuous: Annotated[
        bool,
        typer.Option(
            "--continuous", "-c", help="Enable continuous monitoring (until Ctrl+C)"
        ),
    ] = False,
) -> None:
    """Monitor specific jobs until completion or send periodic reports.

    \b
    Modes:
        State change detection (default): Monitor until jobs complete
        - Example: srunx monitor jobs 12345
        - Example: srunx monitor jobs --all --notify $WEBHOOK

        Periodic reporting (--schedule): Send job status reports on schedule
        - Example: srunx monitor jobs 12345 67890 --schedule 10m --notify $WEBHOOK
        - Example: srunx monitor jobs --all --schedule 30m --notify $WEBHOOK
    """

    console = Console()

    # Validate: either job_ids or --all must be specified
    if not job_ids and not all_jobs:
        console.print("[red]Error: Either specify job IDs or use --all flag[/red]")
        console.print("Usage: srunx monitor jobs [JOB_IDS] or srunx monitor jobs --all")
        sys.exit(1)

    if job_ids and all_jobs:
        console.print("[red]Error: Cannot specify both job IDs and --all flag[/red]")
        sys.exit(1)

    # Scheduled reporting mode
    if schedule:
        console.print("[yellow]⚠️  Scheduled job reporting not yet implemented[/yellow]")
        console.print("Coming soon! Use cluster subcommand for now:")
        console.print("  srunx monitor cluster --schedule 1h --notify $WEBHOOK")
        sys.exit(1)

    # State change monitoring mode (existing functionality)
    client = Slurm()

    # Get job IDs if --all is specified
    if all_jobs:
        all_user_jobs = client.queue()
        job_ids = [job.job_id for job in all_user_jobs if job.job_id is not None]
        if not job_ids:
            console.print("[yellow]No jobs found for current user[/yellow]")
            sys.exit(0)
        console.print(f"📋 Monitoring {len(job_ids)} jobs for current user")

    # Setup callbacks
    callbacks: list[Callback] = []
    if notify:
        try:
            callbacks.append(SlackCallback(notify))
        except ValueError as e:
            console.print(f"[red]Invalid webhook URL: {e}[/red]")
            sys.exit(1)

    # Create monitor config
    config = MonitorConfig(
        poll_interval=interval,
        timeout=timeout if not continuous else None,
        mode=WatchMode.CONTINUOUS if continuous else WatchMode.UNTIL_CONDITION,
        notify_on_change=continuous or bool(notify),
    )

    # Create and run monitor
    assert job_ids is not None  # Type narrowing
    job_monitor = JobMonitor(
        job_ids=job_ids,
        config=config,
        callbacks=callbacks,
    )

    try:
        if continuous:
            console.print(
                f"🔄 Continuously monitoring jobs {job_ids} "
                f"(interval={interval}s, press Ctrl+C to stop)"
            )
            job_monitor.watch_continuous()
            console.print("✅ Monitoring stopped")
        else:
            console.print(
                f"🔍 Monitoring jobs {job_ids} "
                f"(interval={interval}s, timeout={timeout or 'None'}s)"
            )
            console.print("Press Ctrl+C to stop monitoring")
            job_monitor.watch_until()
            console.print("✅ All jobs reached terminal status")
    except TimeoutError as e:
        console.print(f"[red]⏱️  {e}[/red]")
        sys.exit(1)
    except KeyboardInterrupt:
        console.print("\n[yellow]Monitoring stopped by user[/yellow]")
        sys.exit(0)
    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        sys.exit(1)


@monitor_app.command("resources")
def monitor_resources(
    min_gpus: Annotated[
        int | None,
        typer.Option("--min-gpus", "-g", help="Minimum GPUs required"),
    ] = None,
    partition: Annotated[
        str | None,
        typer.Option("--partition", "-p", help="SLURM partition to monitor"),
    ] = None,
    interval: Annotated[
        int,
        typer.Option("--interval", "-i", help="Polling interval in seconds"),
    ] = 60,
    timeout: Annotated[
        int | None,
        typer.Option("--timeout", "-t", help="Timeout in seconds (None = no timeout)"),
    ] = None,
    notify: Annotated[
        str | None,
        typer.Option("--notify", "-n", help="Slack webhook URL for notifications"),
    ] = None,
    continuous: Annotated[
        bool,
        typer.Option("--continuous", "-c", help="Monitor continuously until Ctrl+C"),
    ] = False,
) -> None:
    """Monitor GPU resources until available or continuously.

    \b
    Examples:
        # Wait for 4 GPUs
        srunx monitor resources --min-gpus 4

        # Continuous monitoring with notifications
        srunx monitor resources --min-gpus 2 --continuous --notify $WEBHOOK
    """
    console = Console()

    if min_gpus is None:
        console.print("[red]Error: --min-gpus is required[/red]")
        console.print("Usage: srunx monitor resources --min-gpus N")
        sys.exit(1)

    # Setup callbacks
    callbacks: list[Callback] = []
    if notify:
        try:
            callbacks.append(SlackCallback(notify))
        except ValueError as e:
            console.print(f"[red]Invalid webhook URL: {e}[/red]")
            sys.exit(1)

    # Create monitor config
    config = MonitorConfig(
        poll_interval=interval,
        timeout=timeout if not continuous else None,
        mode=WatchMode.CONTINUOUS if continuous else WatchMode.UNTIL_CONDITION,
        notify_on_change=continuous or bool(notify),
    )

    # Create and run resource monitor
    resource_monitor = ResourceMonitor(
        min_gpus=min_gpus,
        partition=partition,
        config=config,
        callbacks=callbacks,
    )

    try:
        if continuous:
            console.print(
                f"🔄 Continuously monitoring GPU resources "
                f"(min={min_gpus}, interval={interval}s)"
            )
            resource_monitor.watch_continuous()
        else:
            console.print(
                f"🎮 Waiting for {min_gpus} GPUs to become available "
                f"(partition={partition or 'all'})"
            )
            resource_monitor.watch_until()
            console.print(f"✅ {min_gpus} GPUs now available!")
    except TimeoutError as e:
        console.print(f"[red]⏱️  {e}[/red]")
        sys.exit(1)
    except KeyboardInterrupt:
        console.print("\n[yellow]Monitoring stopped by user[/yellow]")
        sys.exit(0)
    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        sys.exit(1)


@monitor_app.command("cluster")
def monitor_cluster(
    schedule: Annotated[
        str,
        typer.Option(
            "--schedule",
            "-s",
            help="Schedule for periodic reports (e.g., '1h', '30m', '0 9 * * *')",
        ),
    ],
    notify: Annotated[
        str,
        typer.Option("--notify", "-n", help="Slack webhook URL for notifications"),
    ],
    include: Annotated[
        str | None,
        typer.Option(
            "--include",
            help="Report sections to include (comma-separated: jobs,resources,user,running)",
        ),
    ] = None,
    partition: Annotated[
        str | None,
        typer.Option("--partition", "-p", help="SLURM partition to monitor"),
    ] = None,
    user: Annotated[
        str | None,
        typer.Option("--user", "-u", help="User to filter for user stats"),
    ] = None,
    timeframe: Annotated[
        str,
        typer.Option("--timeframe", help="Timeframe for job aggregation"),
    ] = "24h",
    daemon: Annotated[
        bool,
        typer.Option("--daemon/--no-daemon", help="Run as background daemon"),
    ] = True,
) -> None:
    """Send periodic cluster status reports to Slack.

    \b
    Examples:
        # Hourly cluster reports
        srunx monitor cluster --schedule 1h --notify $WEBHOOK

        # Daily report at 9am with specific sections
        srunx monitor cluster --schedule "0 9 * * *" --notify $WEBHOOK \\
            --include jobs,resources,running
    """
    from rich.panel import Panel
    from rich.table import Table

    console = Console()

    # Parse include option
    include_list = ["jobs", "resources", "user", "running"]  # Default: all
    if include is not None:
        include_list = [s.strip() for s in include.split(",")]

    try:
        # Create configuration
        config = ReportConfig(
            schedule=schedule,
            include=include_list,
            partition=partition,
            user=user,
            timeframe=timeframe,
            daemon=daemon,
        )

        # Create client and callback
        client = Slurm()
        try:
            callback = SlackCallback(notify)
        except ValueError as e:
            console.print(f"[red]Invalid webhook URL: {e}[/red]")
            sys.exit(1)

        # Create and run reporter
        reporter = ScheduledReporter(client, callback, config)

        # Display startup info
        info_table = Table(show_header=False, box=None, padding=(0, 2))
        info_table.add_column("Key", style="cyan", no_wrap=True)
        info_table.add_column("Value", style="white")

        info_table.add_row("📅 Schedule", schedule)
        info_table.add_row("📊 Sections", ", ".join(include_list))
        if partition:
            info_table.add_row("🔧 Partition", partition)
        info_table.add_row("🔔 Webhook", f"{notify[:50]}...")

        console.print(
            Panel(
                info_table,
                title="[bold green]🚀 Scheduled Cluster Reporter[/bold green]",
                subtitle="[dim]Press Ctrl+C to stop[/dim]",
                border_style="green",
            )
        )
        console.print()

        # Run reporter (blocking)
        reporter.run()

    except ValueError as e:
        console.print(f"[red]❌ Configuration error: {e}[/red]")
        sys.exit(1)
    except KeyboardInterrupt:
        console.print("\n[green]✓ Scheduler stopped gracefully[/green]")
        sys.exit(0)
    except Exception as e:
        console.print(f"[red]❌ Error: {e}[/red]")
        sys.exit(1)
