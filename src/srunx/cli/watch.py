"""Watch subcommands for jobs, resources, and cluster status."""

import sys
from typing import Annotated, cast

import typer
from rich.console import Console

from srunx.callbacks import Callback
from srunx.cli._helpers.transport_options import LocalOpt, ProfileOpt, QuietOpt
from srunx.observability.monitoring.job_monitor import JobMonitor
from srunx.observability.monitoring.resource_monitor import ResourceMonitor
from srunx.observability.monitoring.scheduler import ScheduledReporter
from srunx.observability.monitoring.types import MonitorConfig, ReportConfig, WatchMode
from srunx.observability.notifications.legacy_slack import SlackCallback
from srunx.slurm.local import Slurm
from srunx.transport import (
    emit_transport_banner,
    peek_scheduler_key,
    resolve_transport,
    resolve_transport_source,
)

# Create watch subcommand app
watch_app = typer.Typer(
    name="watch",
    help="Watch jobs, resources, or cluster with unified subcommands",
    context_settings={"help_option_names": ["-h", "--help"]},
)


@watch_app.command("jobs")
def watch_jobs(
    job_ids: Annotated[
        list[int] | None,
        typer.Argument(help="Job IDs to watch (space-separated)"),
    ] = None,
    all_jobs: Annotated[
        bool,
        typer.Option(
            "--all",
            "-a",
            help="Watch every active job in the cluster queue (all users).",
        ),
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
    endpoint: Annotated[
        str | None,
        typer.Option(
            "--endpoint",
            help=(
                "Name of a configured notification endpoint (see "
                "`/api/endpoints` / Settings UI). Attaches a durable "
                "watch per monitored job via the poller pipeline."
            ),
        ),
    ] = None,
    preset: Annotated[
        str | None,
        typer.Option(
            "--preset",
            help=(
                "Subscription preset for --endpoint: terminal (default), "
                "running_and_terminal, or all."
            ),
        ),
    ] = None,
    continuous: Annotated[
        bool,
        typer.Option(
            "--continuous", "-c", help="Enable continuous watching (until Ctrl+C)"
        ),
    ] = False,
    profile: ProfileOpt = None,
    local: LocalOpt = False,
    quiet: QuietOpt = False,
) -> None:
    """Watch specific jobs until completion or send periodic reports.

    \b
    Modes:
        State change detection (default): Watch until jobs complete
        - Example: srunx watch jobs 12345
        - Example: srunx watch jobs --all --notify $WEBHOOK

        Periodic reporting (--schedule): Send job status reports on schedule
        - Example: srunx watch jobs 12345 67890 --schedule 10m --notify $WEBHOOK
        - Example: srunx watch jobs --all --schedule 30m --notify $WEBHOOK
    """

    console = Console()

    # Validate: either job_ids or --all must be specified
    if not job_ids and not all_jobs:
        console.print("[red]Error: Either specify job IDs or use --all flag[/red]")
        console.print("Usage: srunx watch jobs [JOB_IDS] or srunx watch jobs --all")
        sys.exit(1)

    if job_ids and all_jobs:
        console.print("[red]Error: Cannot specify both job IDs and --all flag[/red]")
        sys.exit(1)

    # Scheduled reporting mode
    if schedule:
        console.print("[yellow]⚠️  Scheduled job reporting not yet implemented[/yellow]")
        console.print("Coming soon! Use cluster subcommand for now:")
        console.print("  srunx watch cluster --schedule 1h --notify $WEBHOOK")
        sys.exit(1)

    with resolve_transport(profile=profile, local=local, quiet=quiet) as rt:
        # State change watching mode (existing functionality). Use the
        # resolved transport's CLI-facing job ops so --profile picks up
        # SSH via resolve_transport rather than silently falling back to
        # a local Slurm singleton.
        if all_jobs:
            # ``queue()`` with no ``user=`` follows native ``squeue``
            # semantics (all users). ``--all`` here means "every active
            # job on the cluster", not just the calling user's jobs.
            queue_jobs = rt.job_ops.queue()
            job_ids = [job.job_id for job in queue_jobs if job.job_id is not None]
            if not job_ids:
                console.print("[yellow]No active jobs in queue[/yellow]")
                sys.exit(0)
            console.print(f"📋 Watching {len(job_ids)} active jobs (all users)")

        # Setup callbacks.
        #
        # Resolution order:
        #   --endpoint → attach a durable watch per monitored job (poller
        #                pipeline takes over delivery)
        #   --notify   → in-process SlackCallback fallback (deprecated; kept
        #                so endpoint-attach failures don't silently drop
        #                notifications the user asked for)
        callbacks: list[Callback] = []
        if endpoint:
            from srunx.cli._helpers.notification_setup import attach_notification_watch
            from srunx.common.config import get_config

            effective_preset = preset or get_config().notifications.default_preset
            # Attach per-job watches upfront: watch_jobs does not resubmit
            # jobs, so the one-shot attach here is the equivalent of the
            # Callback.on_job_submitted hook used by submit flows.
            assert job_ids is not None
            for _jid in job_ids:
                attach_notification_watch(
                    job_id=int(_jid),
                    endpoint_name=endpoint,
                    preset=effective_preset,
                    scheduler_key=rt.scheduler_key,
                )
        if notify:
            console.print(
                "[yellow]⚠️  --notify is deprecated; use --endpoint instead.[/yellow]"
            )
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
            notify_on_change=continuous or bool(notify) or bool(endpoint),
        )

        # Create and run monitor. Pass the resolved transport's job ops
        # explicitly so JobMonitor doesn't fall back to ``Slurm()`` when
        # --profile selects an SSH transport. ``JobMonitor.client`` is
        # typed ``Slurm | None`` for historical reasons, but both
        # ``Slurm`` and ``SlurmSSHClient`` satisfy the subset of methods
        # (``retrieve``) the monitor actually uses; the cast keeps mypy
        # happy without forcing a JobMonitor refactor that belongs to a
        # later phase.
        assert job_ids is not None  # Type narrowing
        job_monitor = JobMonitor(
            job_ids=job_ids,
            config=config,
            callbacks=callbacks,
            client=cast(Slurm, rt.job_ops),
            scheduler_key=rt.scheduler_key,
        )

        try:
            jobs_str = ", ".join(f"[bold cyan]{jid}[/bold cyan]" for jid in job_ids)
            if continuous:
                console.print(
                    f"[yellow]🔄[/yellow] Continuously watching {jobs_str} "
                    f"[dim](interval={interval}s · Ctrl+C to stop)[/dim]"
                )
                job_monitor.watch_continuous()
                console.print("[green]✅[/green] Watching stopped")
            else:
                timeout_display = f"{timeout}s" if timeout else "no timeout"
                console.print(
                    f"[yellow]🔍[/yellow] Watching {jobs_str} "
                    f"[dim](interval={interval}s · timeout={timeout_display})[/dim]"
                )
                console.print("[dim]Press Ctrl+C to stop watching[/dim]")
                job_monitor.watch_until()
                console.print("[green]✅[/green] All jobs reached terminal status")
        except TimeoutError as e:
            console.print(f"[red]⏱️  {e}[/red]")
            raise typer.Exit(code=1) from e
        except KeyboardInterrupt:
            console.print("\n[yellow]⚠[/yellow]  [dim]Watching stopped by user[/dim]")
            raise typer.Exit(code=0) from None
        except Exception as e:
            console.print(f"[red]✗ {e}[/red]")
            raise typer.Exit(code=1) from e


@watch_app.command("resources")
def watch_resources(
    min_gpus: Annotated[
        int | None,
        typer.Option("--min-gpus", "-g", help="Minimum GPUs required"),
    ] = None,
    partition: Annotated[
        str | None,
        typer.Option("--partition", "-p", help="SLURM partition to watch"),
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
        typer.Option("--continuous", "-c", help="Watch continuously until Ctrl+C"),
    ] = False,
    profile: ProfileOpt = None,
    local: LocalOpt = False,
    quiet: QuietOpt = False,
) -> None:
    """Watch GPU resources until available or continuously.

    \b
    Examples:
        # Wait for 4 GPUs
        srunx watch resources --min-gpus 4

        # Continuous watching with notifications
        srunx watch resources --min-gpus 2 --continuous --notify $WEBHOOK

    Note:
        --profile / --local / --quiet are accepted for CLI-wide parity
        but ResourceMonitor currently queries local ``sinfo`` / ``squeue``
        only; SSH-backed resource polling is a follow-up phase. Passing
        ``--profile`` will emit the transport banner but not reroute the
        underlying queries.
    """
    console = Console()

    if min_gpus is None:
        console.print("[red]Error: --min-gpus is required[/red]")
        console.print("Usage: srunx watch resources --min-gpus N")
        sys.exit(1)

    # SF7: ResourceMonitor is local-only, so skip the full
    # resolve_transport() path (which would build an SSH handle + open
    # a pool) and call the pure helpers instead. ``peek_scheduler_key``
    # still raises on the ``--profile`` + ``--local`` conflict, and
    # ``emit_transport_banner`` reproduces the same stderr line
    # ``resolve_transport`` would emit so diffing scripts see no change.
    scheduler_key = peek_scheduler_key(profile=profile, local=local)
    source = resolve_transport_source(profile=profile, local=local)
    emit_transport_banner(label=scheduler_key, source=source, quiet=quiet)

    if profile:
        console.print(
            "[yellow]⚠️  ResourceMonitor ignores --profile in this release; "
            "resource queries still run against the local cluster.[/yellow]"
        )

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
                f"🔄 Continuously watching GPU resources "
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
        console.print("\n[yellow]Watching stopped by user[/yellow]")
        sys.exit(0)
    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        sys.exit(1)


@watch_app.command("cluster")
def watch_cluster(
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
        typer.Option("--partition", "-p", help="SLURM partition to watch"),
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
    profile: ProfileOpt = None,
    local: LocalOpt = False,
    quiet: QuietOpt = False,
) -> None:
    """Send periodic cluster status reports to Slack.

    \b
    Examples:
        # Hourly cluster reports
        srunx watch cluster --schedule 1h --notify $WEBHOOK

        # Daily report at 9am with specific sections
        srunx watch cluster --schedule "0 9 * * *" --notify $WEBHOOK \\
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

        with resolve_transport(profile=profile, local=local, quiet=quiet) as rt:
            # ScheduledReporter only exercises ``client.queue(...)``
            # (see _get_job_stats / _get_user_stats), which is part of
            # JobOperations. ``rt.job_ops`` is the CLI-facing
            # handle and is either a local ``Slurm`` or an
            # ``SlurmSSHClient``; the ``cast`` narrows the static type
            # to match ScheduledReporter's concrete-``Slurm`` signature
            # without changing that class (its refactor belongs to a
            # later transport phase).
            try:
                callback = SlackCallback(notify)
            except ValueError as e:
                console.print(f"[red]Invalid webhook URL: {e}[/red]")
                sys.exit(1)

            # Create and run reporter
            reporter = ScheduledReporter(cast(Slurm, rt.job_ops), callback, config)

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
