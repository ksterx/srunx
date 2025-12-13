"""Scheduled reporter for periodic SLURM status updates."""

import os
import re
import signal
import sys
from datetime import datetime

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from loguru import logger

from srunx.callbacks import Callback
from srunx.client import Slurm
from srunx.models import JobStatus
from srunx.monitor.report_types import JobStats, Report, ReportConfig, ResourceStats
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

        # Enforce minimum interval of 1 minute
        if unit == "s" and value < 60:
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
            logger.info(f"Received signal {signum}, shutting down...")
            self.stop()
            sys.exit(0)

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

    def _generate_and_send_report(self) -> None:
        """Generate report and send via callback."""
        try:
            report = self._generate_report()
            self._send_report(report)
        except (ValueError, RuntimeError, ConnectionError, OSError) as e:
            logger.error(f"Failed to generate/send report: {e}")
        except Exception as e:
            logger.exception(f"Unexpected error in report generation: {e}")
            # Don't re-raise to allow scheduler to continue

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
        logger.info(
            f"Starting scheduled reporter with schedule: {self.config.schedule}"
        )
        logger.info(f"Report includes: {', '.join(self.config.include)}")

        # Send initial report immediately
        logger.info("Sending initial report...")
        self._generate_and_send_report()

        # Start scheduler
        try:
            self.scheduler.start()
        except (KeyboardInterrupt, SystemExit):
            logger.info("Scheduler stopped")

    def stop(self) -> None:
        """Stop the scheduler gracefully."""
        if self.scheduler.running:
            self.scheduler.shutdown(wait=False)
