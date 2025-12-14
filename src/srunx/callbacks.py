"""Callback system for job state notifications."""

import re
from dataclasses import asdict
from typing import TYPE_CHECKING

from slack_sdk import WebhookClient

from srunx.formatters import SlackNotificationFormatter
from srunx.models import JobType, Workflow
from srunx.utils import job_status_msg

if TYPE_CHECKING:
    from srunx.monitor.report_types import Report
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
        self.formatter = SlackNotificationFormatter()

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
        # Use new unified formatter
        message = self.formatter.resource_available(
            partition=snapshot.partition,
            available_gpus=snapshot.gpus_available,
            total_gpus=snapshot.total_gpus,
            idle_nodes=snapshot.nodes_idle,
            total_nodes=snapshot.nodes_total,
            utilization=snapshot.gpu_utilization,
        )

        self.client.send(
            text="Resources available",
            blocks=[
                {
                    "type": "section",
                    "text": {"type": "mrkdwn", "text": message},
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
        from loguru import logger

        # Log running jobs presence
        if report.running_jobs:
            logger.info(
                f"Adding running jobs section with {len(report.running_jobs)} jobs"
            )
        else:
            logger.info("No running jobs to display in report")

        # Convert dataclasses to dicts for formatter
        job_stats_dict = asdict(report.job_stats) if report.job_stats else None
        resource_stats_dict = (
            asdict(report.resource_stats) if report.resource_stats else None
        )
        running_jobs_list = (
            [asdict(job) for job in report.running_jobs]
            if report.running_jobs
            else None
        )

        # Use new unified formatter
        message = self.formatter.cluster_status(
            job_stats=job_stats_dict,
            resource_stats=resource_stats_dict,
            running_jobs=running_jobs_list,
            timestamp=report.timestamp,
        )

        # Send to Slack
        self.client.send(
            text="SLURM Status Report",
            blocks=[
                {
                    "type": "section",
                    "text": {"type": "mrkdwn", "text": message},
                }
            ],
        )
