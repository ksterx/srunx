"""Callback system for job state notifications."""

import re
from typing import TYPE_CHECKING

from slack_sdk import WebhookClient

from srunx.models import JobType, Workflow
from srunx.utils import job_status_msg

if TYPE_CHECKING:
    from srunx.monitor.report_types import JobStats, Report, ResourceStats, RunningJob
    from srunx.monitor.types import ResourceSnapshot


class Callback:
    """Base callback class for job state notifications."""

    def on_job_submitted(self, job: JobType) -> None:
        """Called when a job is submitted to SLURM.

        Args:
            job: Job that was submitted.
        """
        pass

    def on_job_completed(self, job: JobType) -> None:
        """Called when a job completes successfully.

        Args:
            job: Job that completed.
        """
        pass

    def on_job_failed(self, job: JobType) -> None:
        """Called when a job fails.

        Args:
            job: Job that failed.
        """
        pass

    def on_job_running(self, job: JobType) -> None:
        """Called when a job starts running.

        Args:
            job: Job that started running.
        """
        pass

    def on_job_cancelled(self, job: JobType) -> None:
        """Called when a job is cancelled.

        Args:
            job: Job that was cancelled.
        """
        pass

    def on_workflow_started(self, workflow: Workflow) -> None:
        """Called when a workflow starts.

        Args:
            workflow: Workflow that started.
        """
        pass

    def on_workflow_completed(self, workflow: Workflow) -> None:
        """Called when a workflow completes.

        Args:
            workflow: Workflow that completed.
        """
        pass

    def on_resources_available(self, snapshot: "ResourceSnapshot") -> None:
        """Called when resources become available (threshold met).

        Args:
            snapshot: Resource snapshot at the time resources became available.
        """
        pass

    def on_resources_exhausted(self, snapshot: "ResourceSnapshot") -> None:
        """Called when resources are exhausted (below threshold).

        Args:
            snapshot: Resource snapshot at the time resources were exhausted.
        """
        pass

    def on_scheduled_report(self, report: "Report") -> None:
        """Called when a scheduled report is generated.

        Args:
            report: Generated report containing job and resource statistics.
        """
        pass


class SlackCallback(Callback):
    """Callback that sends notifications to Slack via webhook."""

    def __init__(self, webhook_url: str):
        """Initialize Slack callback.

        Args:
            webhook_url: Slack webhook URL for sending notifications.

        Raises:
            ValueError: If webhook_url is not a valid Slack webhook URL.
        """
        # Validate webhook URL format
        if not self._is_valid_slack_webhook(webhook_url):
            raise ValueError(
                "Invalid Slack webhook URL. Must be https://hooks.slack.com/services/..."
            )
        self.client = WebhookClient(webhook_url)

    @staticmethod
    def _is_valid_slack_webhook(url: str) -> bool:
        """Validate Slack webhook URL format.

        Args:
            url: URL to validate.

        Returns:
            True if URL is a valid Slack webhook URL, False otherwise.
        """
        # Slack webhook URLs must have exactly 3 path segments after /services/
        # Format: https://hooks.slack.com/services/WORKSPACE_ID/CHANNEL_ID/TOKEN
        pattern = r"^https://hooks\.slack\.com/services/[A-Za-z0-9_-]+/[A-Za-z0-9_-]+/[A-Za-z0-9_-]+$"
        return re.match(pattern, url) is not None

    @staticmethod
    def _sanitize_text(text: str) -> str:
        """Sanitize text for safe use in Slack messages.

        Prevents injection attacks by escaping special characters and
        removing control characters that could break message formatting.

        Args:
            text: Text to sanitize.

        Returns:
            Sanitized text with special characters escaped and control
            characters removed.
        """
        # Remove or replace control characters
        text = text.replace("\n", " ").replace("\r", " ").replace("\t", " ")

        # Escape special characters that could enable injection attacks
        # Note: & must be first to avoid double-escaping
        replacements = {
            "&": "&amp;",  # HTML entity escape (must be first)
            "<": "&lt;",  # Prevent HTML/script tag injection
            ">": "&gt;",  # Prevent HTML/script tag injection
            "`": "'",  # Prevent code block injection
            "*": "\\*",  # Escape markdown bold
            "_": "\\_",  # Escape markdown italic
            "~": "\\~",  # Escape markdown strikethrough
            "[": "\\[",  # Escape markdown link syntax
            "]": "\\]",  # Escape markdown link syntax
        }
        for char, replacement in replacements.items():
            text = text.replace(char, replacement)

        # Limit length to prevent message overflow
        max_length = 1000
        if len(text) > max_length:
            text = text[:max_length] + "..."

        return text

    def on_job_submitted(self, job: JobType) -> None:
        """Send a message to Slack.

        Args:
            job: Job that completed.
            message: Message to send.
        """
        safe_name = self._sanitize_text(job.name)
        self.client.send(
            text="Job submitted",
            blocks=[
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"`⚡ {'SUBMITTED':<12} Job {safe_name:<12} (ID: {job.job_id})`",
                    },
                }
            ],
        )

    def on_job_completed(self, job: JobType) -> None:
        """Send completion notification to Slack.

        Args:
            job: Job that completed.
        """
        safe_message = self._sanitize_text(job_status_msg(job))
        self.client.send(
            text="Job completed",
            blocks=[
                {
                    "type": "section",
                    "text": {"type": "mrkdwn", "text": f"`{safe_message}`"},
                }
            ],
        )

    def on_job_failed(self, job: JobType) -> None:
        """Send failure notification to Slack.

        Args:
            job: Job that failed.
        """
        safe_message = self._sanitize_text(job_status_msg(job))
        self.client.send(
            text="Job failed",
            blocks=[
                {
                    "type": "section",
                    "text": {"type": "mrkdwn", "text": f"`{safe_message}`"},
                }
            ],
        )

    def on_job_running(self, job: JobType) -> None:
        """Send running notification to Slack.

        Args:
            job: Job that started running.
        """
        safe_message = self._sanitize_text(job_status_msg(job))
        self.client.send(
            text="Job running",
            blocks=[
                {
                    "type": "section",
                    "text": {"type": "mrkdwn", "text": f"`{safe_message}`"},
                }
            ],
        )

    def on_job_cancelled(self, job: JobType) -> None:
        """Send cancellation notification to Slack.

        Args:
            job: Job that was cancelled.
        """
        safe_message = self._sanitize_text(job_status_msg(job))
        self.client.send(
            text="Job cancelled",
            blocks=[
                {
                    "type": "section",
                    "text": {"type": "mrkdwn", "text": f"`{safe_message}`"},
                }
            ],
        )

    def on_workflow_completed(self, workflow: Workflow) -> None:
        """Send completion notification to Slack.

        Args:
            workflow: Workflow that completed.
        """
        safe_name = self._sanitize_text(workflow.name)
        self.client.send(
            text="Workflow completed",
            blocks=[
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"🎉 Workflow {safe_name} completed🎉",
                    },
                }
            ],
        )

    def on_resources_available(self, snapshot: "ResourceSnapshot") -> None:
        """Send resource availability notification to Slack.

        Args:
            snapshot: Resource snapshot at the time resources became available.
        """
        if snapshot.partition:
            safe_partition = self._sanitize_text(snapshot.partition)
            partition_info = f" on {safe_partition}"
        else:
            partition_info = ""
        self.client.send(
            text="Resources available",
            blocks=[
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"✅ Resources available{partition_info}: {snapshot.gpus_available} GPU(s) free",
                    },
                }
            ],
        )

    def on_resources_exhausted(self, snapshot: "ResourceSnapshot") -> None:
        """Send resource exhaustion notification to Slack.

        Args:
            snapshot: Resource snapshot at the time resources were exhausted.
        """
        if snapshot.partition:
            safe_partition = self._sanitize_text(snapshot.partition)
            partition_info = f" on {safe_partition}"
        else:
            partition_info = ""
        self.client.send(
            text="Resources exhausted",
            blocks=[
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"⚠️ Resources exhausted{partition_info}: {snapshot.gpus_available} GPU(s) free (threshold not met)",
                    },
                }
            ],
        )

    def on_scheduled_report(self, report: "Report") -> None:
        """Send scheduled report to Slack.

        Args:
            report: Generated report containing job and resource statistics.
        """

        # Build report sections
        sections = []
        sections.append(self._build_header_section(report))

        # Add timestamp context
        timestamp = report.timestamp.strftime("%Y-%m-%d %H:%M:%S")
        sections.append(
            {
                "type": "context",
                "elements": [
                    {
                        "type": "mrkdwn",
                        "text": f"🕐 {timestamp}",
                    }
                ],
            }
        )

        # Add divider
        sections.append({"type": "divider"})

        if report.job_stats:
            sections.append(self._build_job_stats_section(report.job_stats))
            sections.append({"type": "divider"})

        if report.resource_stats:
            sections.append(self._build_resource_stats_section(report.resource_stats))
            sections.append({"type": "divider"})

        if report.user_stats:
            sections.append(self._build_user_stats_section(report.user_stats))
            sections.append({"type": "divider"})

        if report.running_jobs:
            from loguru import logger

            logger.info(
                f"Adding running jobs section with {len(report.running_jobs)} jobs"
            )
            sections.append(self._build_running_jobs_section(report.running_jobs))
        else:
            from loguru import logger

            logger.info("No running jobs to display in report")

        # Send to Slack
        self.client.send(
            text="SLURM Status Report",
            blocks=sections,
        )

    def _build_header_section(self, report: "Report") -> dict:
        """Build report header section.

        Args:
            report: Report to format

        Returns:
            Slack block for header
        """

        timestamp = report.timestamp.strftime("%Y-%m-%d %H:%M:%S")
        return {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": "📊 SLURM Cluster Status",
                "emoji": True,
            },
        }

    def _build_job_stats_section(self, stats: "JobStats") -> dict:
        """Build job statistics section.

        Args:
            stats: Job statistics to format

        Returns:
            Slack block for job stats
        """

        # Build status summary
        active_summary = (
            f"*{stats.total_active} active jobs*"
            if stats.total_active > 0
            else "No active jobs"
        )

        fields = [
            {
                "type": "mrkdwn",
                "text": f"*Queue Status*\n{active_summary}",
            },
            {
                "type": "mrkdwn",
                "text": f"⏳ Pending\n`{stats.pending:>3d}`",
            },
            {
                "type": "mrkdwn",
                "text": f"🔄 Running\n`{stats.running:>3d}`",
            },
        ]

        # Add completed jobs if any
        if stats.completed > 0 or stats.failed > 0 or stats.cancelled > 0:
            fields.extend(
                [
                    {
                        "type": "mrkdwn",
                        "text": f"✅ Completed\n`{stats.completed:>3d}`",
                    },
                    {
                        "type": "mrkdwn",
                        "text": f"❌ Failed\n`{stats.failed:>3d}`",
                    },
                    {
                        "type": "mrkdwn",
                        "text": f"🚫 Cancelled\n`{stats.cancelled:>3d}`",
                    },
                ]
            )

        return {
            "type": "section",
            "fields": fields,
        }

    def _build_resource_stats_section(self, stats: "ResourceStats") -> dict:
        """Build resource statistics section.

        Args:
            stats: Resource statistics to format

        Returns:
            Slack block for resource stats
        """

        partition_info = f" (`{stats.partition}`)" if stats.partition else ""

        # GPU utilization bar
        if stats.total_gpus > 0:
            bar_length = 10
            filled = int((stats.utilization / 100) * bar_length)
            bar = "█" * filled + "░" * (bar_length - filled)
            gpu_status = f"{bar} {stats.utilization:.0f}%"
            gpu_summary = (
                f"*GPU Resources{partition_info}*\n"
                f"{gpu_status}\n"
                f"• Total: {stats.total_gpus} | "
                f"In Use: {stats.gpus_in_use} | "
                f"Available: {stats.gpus_available}"
            )
        else:
            gpu_summary = f"*GPU Resources{partition_info}*\nNo GPUs available"

        # Node status
        node_status = f"*Nodes:* {stats.nodes_total} total"
        if stats.nodes_idle > 0:
            node_status += f" | {stats.nodes_idle} idle"
        if stats.nodes_down > 0:
            node_status += f" | ⚠️ {stats.nodes_down} down"

        return {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"{gpu_summary}\n\n{node_status}",
            },
        }

    def _build_user_stats_section(self, stats: "JobStats") -> dict:
        """Build user statistics section.

        Args:
            stats: User job statistics to format

        Returns:
            Slack block for user stats
        """

        if stats.total_active == 0:
            summary = "No active jobs"
        else:
            summary = f"{stats.total_active} active job{'s' if stats.total_active > 1 else ''}"

        fields = [
            {
                "type": "mrkdwn",
                "text": f"*Your Jobs*\n{summary}",
            },
            {
                "type": "mrkdwn",
                "text": f"⏳ Pending\n`{stats.pending:>3d}`",
            },
            {
                "type": "mrkdwn",
                "text": f"🔄 Running\n`{stats.running:>3d}`",
            },
        ]

        return {
            "type": "section",
            "fields": fields,
        }

    def _build_running_jobs_section(self, jobs: list["RunningJob"]) -> dict:
        """Build running jobs list section.

        Args:
            jobs: List of running jobs to display

        Returns:
            Slack block for running jobs
        """

        if not jobs:
            return {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "*Active Jobs*\nNo active jobs",
                },
            }

        # Build job list
        job_lines = [f"*Active Jobs* ({len(jobs)} shown)"]

        for job in jobs:
            # Format runtime
            if job.runtime:
                days = job.runtime.days
                hours, remainder = divmod(job.runtime.seconds, 3600)
                minutes, _ = divmod(remainder, 60)

                if days > 0:
                    runtime_str = f"{days}d {hours:02d}:{minutes:02d}"
                else:
                    runtime_str = f"{hours:02d}:{minutes:02d}"
            else:
                runtime_str = "-"

            # Status emoji
            status_emoji = "🔄" if job.status == "RUNNING" else "⏳"

            # Format job line
            job_line = (
                f"{status_emoji} `{job.job_id:>6}` "
                f"*{self._truncate(job.name, 20)}* "
                f"| {self._truncate(job.user, 12)} "
                f"| ⏱ {runtime_str:>9} "
            )

            # Add GPU info if present
            if job.gpus > 0:
                job_line += f"| 🎮 {job.gpus}"

            job_lines.append(job_line)

        return {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": "\n".join(job_lines),
            },
        }

    @staticmethod
    def _truncate(text: str, max_length: int) -> str:
        """Truncate text to maximum length.

        Args:
            text: Text to truncate
            max_length: Maximum length

        Returns:
            Truncated text with ellipsis if needed
        """
        if len(text) <= max_length:
            return text
        return text[: max_length - 1] + "…"
