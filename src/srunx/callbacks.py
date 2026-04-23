"""Callback system for job state notifications.

The ``SlackCallback`` concrete implementation lives in
:mod:`srunx.observability.notifications.legacy_slack` (#164); it is
re-exported here so legacy ``from srunx.callbacks import SlackCallback``
call-sites keep working.
"""

from typing import TYPE_CHECKING

from srunx.logging import get_logger
from srunx.models import JobType, Workflow

if TYPE_CHECKING:
    from srunx.monitor.report_types import Report
    from srunx.monitor.types import ResourceSnapshot
    from srunx.observability.notifications.legacy_slack import (
        SlackCallback as SlackCallback,  # re-exported via ``__getattr__``
    )

_logger = get_logger(__name__)


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


# SlackCallback moved to srunx.observability.notifications.legacy_slack (#164).
# Lazy-resolve via PEP 562 ``__getattr__`` so the cyclic import between this
# module (which defines ``Callback``) and the subclass module resolves cleanly
# without an E402-triggering late ``import``.
def __getattr__(name: str) -> object:
    if name == "SlackCallback":
        from srunx.observability.notifications.legacy_slack import SlackCallback

        return SlackCallback
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


class NotificationWatchCallback(Callback):
    """Attach a durable notification watch every time a job is submitted.

    This is the endpoint-watch bridge for CLI code paths that still
    drive submission through :class:`~srunx.client.Slurm` + the
    :class:`Callback` fan-out. On each ``on_job_submitted``, it calls
    :func:`srunx.cli._helpers.notification_setup.attach_notification_watch` so
    the poller pipeline takes over delivery — no in-process Slack send.

    Failures are swallowed with a warning: a missing/disabled endpoint
    must never break the submit (matches ``attach_notification_watch``'s
    own best-effort contract).

    ``scheduler_key`` (default ``"local"``) controls which transport
    axis the watch is created under. Callers driving SSH-backed
    transports must pass ``f"ssh:{profile_name}"`` so the poller can
    resolve the watch via the matching :class:`SlurmClientProtocol`
    implementation.
    """

    def __init__(
        self,
        endpoint_name: str,
        preset: str = "terminal",
        endpoint_kind: str = "slack_webhook",
        *,
        scheduler_key: str = "local",
    ) -> None:
        self.endpoint_name = endpoint_name
        self.preset = preset
        self.endpoint_kind = endpoint_kind
        self.scheduler_key = scheduler_key

    def on_job_submitted(self, job: JobType) -> None:
        if job.job_id is None:
            return
        from srunx.cli._helpers.notification_setup import attach_notification_watch

        try:
            attach_notification_watch(
                job_id=int(job.job_id),
                endpoint_name=self.endpoint_name,
                preset=self.preset,
                endpoint_kind=self.endpoint_kind,
                scheduler_key=self.scheduler_key,
            )
        except Exception as exc:
            _logger.warning(
                "NotificationWatchCallback: failed to attach watch for job %s: %s",
                job.job_id,
                exc,
            )
