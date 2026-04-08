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

    Uses per-cycle caching: jobs are fetched once per poll cycle and the result
    is reused by check_condition, get_current_state, and _notify_callbacks,
    reducing SLURM queries from 3*N to N per cycle.
    """

    def __init__(
        self,
        job_ids: list[int],
        target_statuses: list[JobStatus] | None = None,
        config: MonitorConfig | None = None,
        callbacks: list[Callback] | None = None,
        client: Slurm | None = None,
    ) -> None:
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
        self._cached_jobs: list[BaseJob] | None = None

        logger.info(
            f"JobMonitor initialized for jobs {self.job_ids}, "
            f"target statuses: {[s.value for s in self.target_statuses]}"
        )

    def _get_monitored_jobs(self) -> list[BaseJob]:
        """Get monitored jobs, using per-cycle cache.

        First call per cycle fetches from SLURM via client.retrieve();
        subsequent calls within the same cycle return the cached result.
        """
        if self._cached_jobs is not None:
            return self._cached_jobs

        from srunx.models import Job

        jobs: list[BaseJob] = []
        for job_id in self.job_ids:
            try:
                jobs.append(self.client.retrieve(job_id))
            except Exception as e:
                logger.warning(f"Failed to retrieve job {job_id}: {e}")
                placeholder = Job(
                    name=f"job_{job_id}",
                    job_id=job_id,
                    command=["unknown"],
                )
                placeholder._status = JobStatus.UNKNOWN
                jobs.append(placeholder)

        self._cached_jobs = jobs
        return jobs

    def check_condition(self) -> bool:
        """Check if all monitored jobs have reached target statuses.

        Invalidates the per-cycle cache so fresh data is fetched.
        """
        self._cached_jobs = None
        jobs = self._get_monitored_jobs()
        return all(job._status in self.target_statuses for job in jobs)

    def get_current_state(self) -> dict[str, Any]:
        """Get current state of all monitored jobs.

        Invalidates the per-cycle cache so fresh data is fetched.
        """
        self._cached_jobs = None
        jobs = self._get_monitored_jobs()
        return {
            str(job.job_id): job._status.value for job in jobs if job.job_id is not None
        }

    def _notify_callbacks(self, event: str) -> None:
        """Notify callbacks of job state transitions (uses cycle cache)."""
        jobs = self._get_monitored_jobs()

        for job in jobs:
            if job.job_id is None:
                continue

            current_status = job._status
            previous_status = self._previous_states.get(job.job_id)

            if current_status != previous_status:
                self._notify_transition(job, current_status)
                self._previous_states[job.job_id] = current_status

    def _notify_transition(self, job: BaseJob, status: JobStatus) -> None:
        """Invoke appropriate callback methods based on job status transition."""
        logger.debug(f"Job {job.job_id} transitioned to {status.value}")

        # Update history database for terminal states
        if status in {
            JobStatus.COMPLETED,
            JobStatus.FAILED,
            JobStatus.CANCELLED,
            JobStatus.TIMEOUT,
        }:
            try:
                from srunx.history import get_history

                history = get_history()
                if job.job_id:
                    history.update_job_completion(job.job_id, status)
            except Exception as e:
                logger.warning(f"Failed to update job history: {e}")

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
                    callback.on_job_failed(job)
            except Exception as e:
                logger.error(f"Callback error for job {job.job_id}: {e}")
