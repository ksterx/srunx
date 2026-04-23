"""Legacy ``SlackCallback`` — in-process webhook sender.

Predates the outbox-based notification pipeline (poller → delivery queue).
Retained for backwards compatibility with CLI workflows that attach
``callbacks=[SlackCallback(...)]`` directly to :class:`~srunx.client.Slurm`.

New code should use the notification endpoint / watch / subscription
model (see :mod:`srunx.observability.notifications.service`) plus the
:class:`~srunx.observability.CallbackSink` wrapper so delivery goes
through the durable outbox.
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING

from slack_sdk import WebhookClient

from srunx.callbacks import Callback
from srunx.models import JobType, Workflow
from srunx.observability.notifications.formatting import SlackNotificationFormatter
from srunx.utils import job_status_msg

if TYPE_CHECKING:
    from srunx.monitor.report_types import Report
    from srunx.monitor.types import ResourceSnapshot


class SlackCallback(Callback):
    """Callback that sends notifications to Slack via webhook."""

    def __init__(self, webhook_url: str):
        """Initialize Slack callback.

        Args:
            webhook_url: Slack webhook URL for sending notifications.

        Raises:
            ValueError: If webhook_url is not a valid Slack webhook URL.
        """
        if not self._is_valid_slack_webhook(webhook_url):
            raise ValueError(
                "Invalid Slack webhook URL. Must be https://hooks.slack.com/services/..."
            )
        self.client = WebhookClient(webhook_url)
        self.formatter = SlackNotificationFormatter()

    @staticmethod
    def _is_valid_slack_webhook(url: str) -> bool:
        """Validate Slack webhook URL format.

        Format: ``https://hooks.slack.com/services/WORKSPACE_ID/CHANNEL_ID/TOKEN``
        — exactly 3 path segments after ``/services/``.
        """
        pattern = r"^https://hooks\.slack\.com/services/[A-Za-z0-9_-]+/[A-Za-z0-9_-]+/[A-Za-z0-9_-]+$"
        return re.match(pattern, url) is not None

    @staticmethod
    def _sanitize_text(text: str) -> str:
        """Sanitize text for safe use in Slack messages.

        Delegates to :func:`srunx.observability.notifications.sanitize.sanitize_slack_text`
        so the CLI callback and the delivery adapter share a single
        implementation. Kept as a ``@staticmethod`` wrapper for backward
        compatibility with existing call sites and tests.
        """
        from srunx.observability.notifications.sanitize import sanitize_slack_text

        return sanitize_slack_text(text)

    def on_job_submitted(self, job: JobType) -> None:
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

    def _send_job_status(self, job: JobType, label: str) -> None:
        safe_message = self._sanitize_text(job_status_msg(job))
        self.client.send(
            text=label,
            blocks=[
                {
                    "type": "section",
                    "text": {"type": "mrkdwn", "text": f"`{safe_message}`"},
                }
            ],
        )

    def on_job_completed(self, job: JobType) -> None:
        self._send_job_status(job, "Job completed")

    def on_job_failed(self, job: JobType) -> None:
        self._send_job_status(job, "Job failed")

    def on_job_running(self, job: JobType) -> None:
        self._send_job_status(job, "Job running")

    def on_job_cancelled(self, job: JobType) -> None:
        self._send_job_status(job, "Job cancelled")

    def on_workflow_completed(self, workflow: Workflow) -> None:
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

    def on_resources_available(self, snapshot: ResourceSnapshot) -> None:
        message = self.formatter.resource_available(
            partition=snapshot.partition,
            available_gpus=snapshot.gpus_available,
            total_gpus=snapshot.total_gpus,
            idle_nodes=snapshot.nodes_idle,
            total_nodes=snapshot.nodes_total,
            utilization=snapshot.gpu_utilization * 100,
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

    def on_resources_exhausted(self, snapshot: ResourceSnapshot) -> None:
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

    def on_scheduled_report(self, report: Report) -> None:
        from loguru import logger

        if report.running_jobs:
            logger.info(
                f"Adding running jobs section with {len(report.running_jobs)} jobs"
            )
        else:
            logger.info("No running jobs to display in report")

        job_stats_dict = report.job_stats.model_dump() if report.job_stats else None
        resource_stats_dict = (
            report.resource_stats.model_dump() if report.resource_stats else None
        )
        running_jobs_list = (
            [job.model_dump() for job in report.running_jobs]
            if report.running_jobs
            else None
        )

        message = self.formatter.cluster_status(
            job_stats=job_stats_dict,
            resource_stats=resource_stats_dict,
            running_jobs=running_jobs_list,
            timestamp=report.timestamp,
        )

        self.client.send(
            text="SLURM Status Report",
            blocks=[
                {
                    "type": "section",
                    "text": {"type": "mrkdwn", "text": message},
                }
            ],
        )
