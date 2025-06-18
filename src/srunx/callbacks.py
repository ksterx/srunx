from slack_sdk import WebhookClient

from srunx.models import JobType


class Callback:
    def on_job_submitted(self, job: JobType) -> None:
        pass

    def on_job_completed(self, job: JobType) -> None:
        pass

    def on_job_failed(self, job: JobType) -> None:
        pass

    def on_job_running(self, job: JobType) -> None:
        pass

    def on_job_cancelled(self, job: JobType) -> None:
        pass


class SlackCallback(Callback):
    def __init__(self, webhook_url: str):
        self.client = WebhookClient(webhook_url)

    def on_job_completed(self, job: JobType) -> None:
        self.client.send(
            text="🎉Job completed🎉",
            blocks=[
                {
                    "type": "section",
                    "text": {"type": "mrkdwn", "text": f"Job {job.name} completed"},
                }
            ],
        )

    def on_job_failed(self, job: JobType) -> None:
        self.client.send(
            text="☠️Job failed☠️",
            blocks=[
                {
                    "type": "section",
                    "text": {"type": "mrkdwn", "text": f"Job {job.name} failed"},
                }
            ],
        )
