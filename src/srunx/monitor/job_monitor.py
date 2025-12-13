"""Job monitoring implementation for SLURM."""

from typing import Any

from loguru import logger

from srunx.callbacks import Callback
from srunx.client import Slurm
from srunx.models import BaseJob, JobStatus
from srunx.monitor.base import BaseMonitor
from srunx.monitor.types import MonitorConfig


class JobMonitor(BaseMonitor):
    """Monitor SLURM jobs until they reach terminal states.

    Polls jobs at configured intervals and notifies callbacks on state transitions.
    Supports monitoring single or multiple jobs with target status detection.
    """

    def __init__(
        self,
        job_ids: list[int],
        target_statuses: list[JobStatus] | None = None,
        config: MonitorConfig | None = None,
        callbacks: list[Callback] | None = None,
        client: Slurm | None = None,
    ) -> None:
        """Initialize job monitor.

        Args:
            job_ids: List of SLURM job IDs to monitor.
            target_statuses: Terminal statuses to wait for. Defaults to
                [COMPLETED, FAILED, CANCELLED, TIMEOUT].
            config: Monitoring configuration. Defaults to MonitorConfig() if None.
            callbacks: List of notification callbacks. Defaults to empty list if None.
            client: SLURM client instance. Defaults to Slurm() if None.

        Raises:
            ValueError: If job_ids is empty.
        """
        super().__init__(config=config, callbacks=callbacks)

        if not job_ids:
            raise ValueError("job_ids cannot be empty")

        self.job_ids = job_ids
        self.target_statuses = target_statuses or [
            JobStatus.COMPLETED,
            JobStatus.FAILED,
            JobStatus.CANCELLED,
            JobStatus.TIMEOUT,
        ]
        self.client = client or Slurm()
        self._previous_states: dict[int, JobStatus] = {}

        logger.info(
            f"JobMonitor initialized for jobs {self.job_ids}, "
            f"target statuses: {[s.value for s in self.target_statuses]}"
        )

    def check_condition(self) -> bool:
        """Check if all monitored jobs have reached target statuses.

        Returns:
            True if all jobs have reached a target status, False otherwise.

        Raises:
            SlurmError: If SLURM command fails.
        """
        jobs = self._get_monitored_jobs()

        # Check if all jobs have reached terminal states
        for job in jobs:
            if job.status not in self.target_statuses:
                return False

        return True

    def get_current_state(self) -> dict[str, Any]:
        """Get current state of all monitored jobs.

        Returns:
            Dictionary mapping job IDs (as strings) to their current statuses.
            Format: {str(job_id): status_value, ...}

        Raises:
            SlurmError: If SLURM command fails.
        """
        jobs = self._get_monitored_jobs()
        return {
            str(job.job_id): job.status.value for job in jobs if job.job_id is not None
        }

    def _notify_callbacks(self, event: str) -> None:
        """Notify callbacks of job state transitions.

        Detects state changes and invokes appropriate callback methods based on
        new job status. Only notifies on actual transitions, not repeated states.

        Args:
            event: Event name (unused, state changes detected internally).

        Raises:
            SlurmError: If SLURM command fails.
        """
        jobs = self._get_monitored_jobs()

        for job in jobs:
            if job.job_id is None:
                continue

            current_status = job.status
            previous_status = self._previous_states.get(job.job_id)

            # Notify only on state transitions
            if current_status != previous_status:
                self._notify_transition(job, current_status)
                self._previous_states[job.job_id] = current_status

    def _get_monitored_jobs(self) -> list[BaseJob]:
        """Retrieve current status of all monitored jobs.

        Returns:
            List of BaseJob instances with current status information.

        Raises:
            SlurmError: If SLURM command fails.
        """
        from srunx.models import Job

        jobs = []
        for job_id in self.job_ids:
            try:
                job_info = self.client.retrieve(job_id)
                jobs.append(job_info)
            except Exception as e:
                logger.warning(f"Failed to retrieve job {job_id}: {e}")
                # Create placeholder job with unknown status
                placeholder = Job(
                    name=f"job_{job_id}",
                    job_id=job_id,
                    command=["unknown"],
                )
                placeholder._status = JobStatus.UNKNOWN
                jobs.append(placeholder)

        return jobs

    def _notify_transition(self, job: BaseJob, status: JobStatus) -> None:
        """Invoke appropriate callback methods based on job status transition.

        Args:
            job: Job that transitioned to new status.
            status: New status of the job.
        """
        logger.debug(f"Job {job.job_id} transitioned to {status.value}")

        for callback in self.callbacks:
            try:
                if status == JobStatus.RUNNING:
                    callback.on_job_running(job)
                elif status == JobStatus.COMPLETED:
                    callback.on_job_completed(job)
                elif status == JobStatus.FAILED:
                    callback.on_job_failed(job)
                elif status == JobStatus.CANCELLED:
                    callback.on_job_cancelled(job)
                elif status == JobStatus.TIMEOUT:
                    # Timeout is treated as a failure notification
                    callback.on_job_failed(job)
            except Exception as e:
                logger.error(f"Callback error for job {job.job_id}: {e}")
