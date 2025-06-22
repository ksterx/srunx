"""Callback system for job state notifications."""

from slack_sdk import WebhookClient

from srunx.models import JobType, Workflow
from srunx.utils import job_status_msg


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


class SlackCallback(Callback):
    """Callback that sends notifications to Slack via webhook."""

    def __init__(self, webhook_url: str):
        """Initialize Slack callback.

        Args:
            webhook_url: Slack webhook URL for sending notifications.
        """
        self.client = WebhookClient(webhook_url)

    def on_job_submitted(self, job: JobType) -> None:
        """Send a message to Slack.

        Args:
            job: Job that completed.
            message: Message to send.
        """
        self.client.send(
            text="Job submitted",
            blocks=[
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"`🌋 {'SUBMITTED':<12} Job {job.name:<12} (ID: {job.job_id})`",
                    },
                }
            ],
        )

    def on_job_completed(self, job: JobType) -> None:
        """Send completion notification to Slack.

        Args:
            job: Job that completed.
        """
        self.client.send(
            text="Job completed",
            blocks=[
                {
                    "type": "section",
                    "text": {"type": "mrkdwn", "text": f"`{job_status_msg(job)}`"},
                }
            ],
        )

    def on_job_failed(self, job: JobType) -> None:
        """Send failure notification to Slack.

        Args:
            job: Job that failed.
        """
        self.client.send(
            text="Job failed",
            blocks=[
                {
                    "type": "section",
                    "text": {"type": "mrkdwn", "text": f"`{job_status_msg(job)}`"},
                }
            ],
        )

    def on_workflow_completed(self, workflow: Workflow) -> None:
        """Send completion notification to Slack.

        Args:
            workflow: Workflow that completed.
        """
        self.client.send(
            text="Workflow completed",
            blocks=[
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"🎉 Workflow {workflow.name} completed🎉",
                    },
                }
            ],
        )
