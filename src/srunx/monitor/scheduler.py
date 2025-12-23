"""Scheduled reporter for periodic SLURM status updates."""

import os
import re
import signal
import sys
from datetime import datetime, timedelta

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from loguru import logger

from srunx.callbacks import Callback
from srunx.client import Slurm
from srunx.models import JobStatus
from srunx.monitor.report_types import (
    JobStats,
    Report,
    ReportConfig,
    ResourceStats,
    RunningJob,
)
from srunx.monitor.resource_monitor import ResourceMonitor


class ScheduledReporter:
    """Scheduled reporter for periodic SLURM cluster status updates.

    Generates and sends periodic reports containing job queue statistics,
    resource availability, and user-specific job information to configured
    callbacks (e.g., Slack webhooks).

    Args:
        client: SLURM client for job operations
        callback: Callback for report delivery
        config: Report configuration

    Example:
        >>> from srunx import Slurm
        >>> from srunx.callbacks import SlackCallback
        >>> from srunx.monitor.scheduler import ScheduledReporter
        >>> from srunx.monitor.report_types import ReportConfig
        >>>
        >>> client = Slurm()
        >>> callback = SlackCallback(webhook_url)
        >>> config = ReportConfig(schedule="1h", include=["jobs", "resources"])
        >>>
        >>> reporter = ScheduledReporter(client, callback, config)
        >>> reporter.run()  # Blocking execution
    """

    def __init__(
        self,
        client: Slurm,
        callback: Callback,
        config: ReportConfig,
    ):
        """Initialize scheduled reporter."""
        self.client = client
        self.callback = callback
        self.config = config
        self.scheduler = BlockingScheduler()

        # Cache ResourceMonitor if needed
        self._resource_monitor: ResourceMonitor | None = None
        if "resources" in config.include:
            self._resource_monitor = ResourceMonitor(
                min_gpus=0,  # Not checking threshold, just querying
                partition=config.partition,
            )

        self._setup_scheduler()
        self._setup_signal_handlers()

    def _setup_scheduler(self) -> None:
        """Configure APScheduler with interval or cron trigger."""
        if self.config.is_cron_format():
            # Cron format: "0 * * * *"
            trigger = self._parse_cron_schedule()
        else:
            # Interval format: "1h", "30m", "1d"
            trigger = self._parse_interval_schedule()

        self.scheduler.add_job(
            self._generate_and_send_report,
            trigger=trigger,
            id="scheduled_report",
            name="SLURM Status Report",
            max_instances=1,
        )

    def _parse_cron_schedule(self) -> CronTrigger:
        """Parse cron format schedule.

        Returns:
            CronTrigger configured from schedule string

        Raises:
            ValueError: If cron format is invalid
        """
        parts = self.config.schedule.split()
        if len(parts) != 5:
            raise ValueError(
                f"Invalid cron format: {self.config.schedule}. "
                "Expected 5 fields: minute hour day month weekday"
            )

        minute, hour, day, month, day_of_week = parts
        return CronTrigger(
            minute=minute,
            hour=hour,
            day=day,
            month=month,
            day_of_week=day_of_week,
        )

    def _parse_interval_schedule(self) -> IntervalTrigger:
        """Parse interval format schedule.

        Returns:
            IntervalTrigger configured from schedule string

        Raises:
            ValueError: If interval format is invalid
        """
        # Pattern: <number><unit> where unit is s/m/h/d
        match = re.match(r"^(\d+)([smhd])$", self.config.schedule)
        if not match:
            raise ValueError(
                f"Invalid interval format: {self.config.schedule}. "
                "Expected format: <number><unit> (e.g., 1h, 30m, 1d)"
            )

        value = int(match.group(1))
        unit = match.group(2)

        # Convert to seconds for validation
        unit_to_seconds = {"s": 1, "m": 60, "h": 3600, "d": 86400}
        interval_seconds = value * unit_to_seconds[unit]

        # Enforce minimum interval of 60 seconds (1 minute)
        if interval_seconds == 0:
            raise ValueError(
                "Interval cannot be zero. Use a positive value (e.g., 1m, 1h, 1d)."
            )
        if interval_seconds < 60:
            raise ValueError(
                "Minimum interval is 60 seconds (1m). "
                "Use higher intervals to avoid SLURM overload."
            )

        unit_map = {
            "s": "seconds",
            "m": "minutes",
            "h": "hours",
            "d": "days",
        }

        kwargs = {unit_map[unit]: value}
        return IntervalTrigger(**kwargs)

    def _setup_signal_handlers(self) -> None:
        """Setup signal handlers for graceful shutdown."""

        def signal_handler(signum: int, frame: object) -> None:
            self.stop()
            sys.exit(0)

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

    def _generate_and_send_report(self) -> bool:
        """Generate report and send via callback.

        Returns:
            True if report was sent successfully, False otherwise
        """
        try:
            report = self._generate_report()
            self._send_report(report)
            logger.debug("Report sent successfully")
            return True
        except (ValueError, RuntimeError, ConnectionError, OSError) as e:
            logger.error(f"Failed to generate/send report: {e}")
            return False
        except Exception as e:
            logger.exception(f"Unexpected error in report generation: {e}")
            # Don't re-raise to allow scheduler to continue
            return False

    def _generate_report(self) -> Report:
        """Generate report based on configuration.

        Returns:
            Report containing requested statistics
        """
        report = Report(timestamp=datetime.now())

        if "jobs" in self.config.include:
            report.job_stats = self._get_job_stats()

        if "resources" in self.config.include:
            report.resource_stats = self._get_resource_stats()

        if "user" in self.config.include:
            report.user_stats = self._get_user_stats()

        if "running" in self.config.include:
            report.running_jobs = self._get_running_jobs()

        return report

    def _get_job_stats(self) -> JobStats:
        """Get overall job queue statistics.

        Returns:
            Job statistics for all users
        """
        try:
            # Get all jobs in queue
            all_jobs = self.client.queue()
        except (RuntimeError, ValueError, ConnectionError, OSError) as e:
            logger.warning(f"Failed to retrieve job queue: {e}")
            return JobStats(
                pending=0,
                running=0,
                completed=0,
                failed=0,
                cancelled=0,
            )
        except Exception as e:
            logger.exception(f"Unexpected error retrieving job queue: {e}")
            return JobStats(
                pending=0,
                running=0,
                completed=0,
                failed=0,
                cancelled=0,
            )

        # Count by status
        pending = sum(1 for j in all_jobs if j.status == JobStatus.PENDING)
        running = sum(1 for j in all_jobs if j.status == JobStatus.RUNNING)

        # Get completed/failed/cancelled jobs within timeframe
        # Note: This requires sacct which may not be available in all environments
        # For now, we'll return 0 for historical stats
        # TODO: Implement sacct-based historical job queries
        completed = 0
        failed = 0
        cancelled = 0

        return JobStats(
            pending=pending,
            running=running,
            completed=completed,
            failed=failed,
            cancelled=cancelled,
        )

    def _get_resource_stats(self) -> ResourceStats:
        """Get GPU and node resource statistics.

        Returns:
            Resource statistics for specified partition

        Raises:
            RuntimeError: If resource monitoring not configured
        """
        if self._resource_monitor is None:
            raise RuntimeError("Resource monitoring not configured")

        snapshot = self._resource_monitor.get_partition_resources()

        return ResourceStats(
            partition=snapshot.partition,
            total_gpus=snapshot.total_gpus,
            gpus_in_use=snapshot.gpus_in_use,
            gpus_available=snapshot.gpus_available,
            nodes_total=snapshot.nodes_total,
            nodes_idle=snapshot.nodes_idle,
            nodes_down=snapshot.nodes_down,
        )

    def _get_user_stats(self) -> JobStats:
        """Get user-specific job statistics.

        Returns:
            Job statistics filtered by user
        """
        # Determine target user
        target_user = self.config.user or os.getenv("USER")

        try:
            # Get user's jobs
            user_jobs = self.client.queue(user=target_user) if target_user else []
        except (RuntimeError, ValueError, ConnectionError, OSError) as e:
            logger.warning(f"Failed to retrieve user jobs for {target_user}: {e}")
            return JobStats(
                pending=0,
                running=0,
                completed=0,
                failed=0,
                cancelled=0,
            )
        except Exception as e:
            logger.exception(
                f"Unexpected error retrieving user jobs for {target_user}: {e}"
            )
            return JobStats(
                pending=0,
                running=0,
                completed=0,
                failed=0,
                cancelled=0,
            )

        # Count by status
        pending = sum(1 for j in user_jobs if j.status == JobStatus.PENDING)
        running = sum(1 for j in user_jobs if j.status == JobStatus.RUNNING)

        # Historical stats not yet implemented
        completed = 0
        failed = 0
        cancelled = 0

        return JobStats(
            pending=pending,
            running=running,
            completed=completed,
            failed=failed,
            cancelled=cancelled,
        )

    def _get_running_jobs(self) -> list[RunningJob]:
        """Get list of running and pending jobs.

        Returns:
            List of running jobs (limited by max_jobs config)
        """
        try:
            # Get all jobs in queue
            all_jobs = self.client.queue()
            logger.debug(f"Retrieved {len(all_jobs)} total jobs from queue")

            # Filter to running and pending only
            active_jobs = [
                j
                for j in all_jobs
                if j.status in (JobStatus.RUNNING, JobStatus.PENDING)
            ]
            logger.debug(
                f"Filtered to {len(active_jobs)} active jobs (RUNNING/PENDING)"
            )

            # Sort by status (running first) then by job_id
            active_jobs.sort(
                key=lambda j: (0 if j.status == JobStatus.RUNNING else 1, j.job_id or 0)
            )

            # Limit to max_jobs
            active_jobs = active_jobs[: self.config.max_jobs]

            # Convert to RunningJob format
            running_jobs = []
            logger.debug(f"Converting {len(active_jobs)} jobs to RunningJob format")
            for job in active_jobs:
                # Calculate runtime for running jobs
                runtime = None
                if job.status == JobStatus.RUNNING and job.elapsed_time:
                    # elapsed_time is a string like "1-02:03:04" or "02:03:04"
                    runtime = self._parse_elapsed_time(job.elapsed_time)

                running_jobs.append(
                    RunningJob(
                        job_id=job.job_id or 0,
                        name=job.name,
                        user=job.user or "unknown",
                        status=job.status.value,
                        partition=job.partition,
                        runtime=runtime,
                        nodes=job.nodes or 1,
                        gpus=job.gpus or 0,
                    )
                )

            logger.info(f"Returning {len(running_jobs)} running jobs for report")
            return running_jobs

        except (RuntimeError, ValueError, ConnectionError, OSError) as e:
            logger.warning(f"Failed to retrieve job list: {e}")
            return []
        except Exception as e:
            logger.exception(f"Unexpected error retrieving job list: {e}")
            return []

    def _parse_elapsed_time(self, elapsed: str) -> timedelta:
        """Parse SLURM elapsed time string.

        Args:
            elapsed: Time string like "1-02:03:04" or "02:03:04"

        Returns:
            Timedelta object
        """
        days = 0
        if "-" in elapsed:
            day_part, time_part = elapsed.split("-")
            days = int(day_part)
        else:
            time_part = elapsed

        parts = time_part.split(":")
        if len(parts) < 2:
            # Invalid format, return zero timedelta
            logger.warning(
                f"Invalid elapsed time format: '{elapsed}'. "
                "Expected format: 'HH:MM:SS' or 'D-HH:MM:SS'"
            )
            return timedelta()

        hours = int(parts[0])
        minutes = int(parts[1])
        seconds = int(parts[2]) if len(parts) > 2 else 0

        return timedelta(days=days, hours=hours, minutes=minutes, seconds=seconds)

    def _send_report(self, report: Report) -> None:
        """Send report via callback.

        Args:
            report: Generated report to send
        """
        # Call callback with report
        # The callback will format and send the report
        if hasattr(self.callback, "on_scheduled_report"):
            self.callback.on_scheduled_report(report)  # type: ignore
        else:
            logger.warning(
                f"Callback {type(self.callback).__name__} does not implement "
                "on_scheduled_report method"
            )

    def run(self) -> None:
        """Start scheduler in blocking mode.

        Runs until interrupted by SIGINT or SIGTERM.
        """
        from rich.console import Console
        from rich.live import Live
        from rich.panel import Panel
        from rich.table import Table

        console = Console()

        # Send initial report immediately
        console.print("[cyan]📤 Sending initial report...[/cyan]")
        if self._generate_and_send_report():
            console.print("[green]✓ Initial report sent[/green]\n")
        else:
            console.print("[red]✗ Failed to send initial report (see logs)[/red]\n")

        # Create status display
        def create_status() -> Panel:
            table = Table(show_header=False, box=None, padding=(0, 1))
            table.add_column("Status", style="dim")
            table.add_column("Value", style="cyan")

            table.add_row("🔄 Status", "Running")
            table.add_row("📅 Schedule", self.config.schedule)
            table.add_row("📊 Sections", ", ".join(self.config.include))

            return Panel(
                table,
                title="[bold blue]Scheduled Reporter[/bold blue]",
                subtitle="[dim]Press Ctrl+C to stop[/dim]",
                border_style="blue",
            )

        # Start scheduler with live display
        try:
            with Live(create_status(), console=console, refresh_per_second=1):
                self.scheduler.start()
        except (KeyboardInterrupt, SystemExit):
            console.print("\n[yellow]⏹ Stopping scheduler...[/yellow]")
            pass

    def stop(self) -> None:
        """Stop the scheduler gracefully."""
        if self.scheduler.running:
            self.scheduler.shutdown(wait=False)
