"""Job monitoring implementation for SLURM."""

from typing import TYPE_CHECKING, Any

from loguru import logger

from srunx.callbacks import Callback
from srunx.client import Slurm
from srunx.models import BaseJob, JobStatus
from srunx.observability.monitoring.base import BaseMonitor
from srunx.observability.monitoring.types import MonitorConfig

if TYPE_CHECKING:
    from srunx.client_protocol import JobOperationsProtocol
    from srunx.db.repositories.job_state_transitions import (
        JobStateTransitionRepository,
    )


class JobMonitor(BaseMonitor):
    """Monitor SLURM jobs until they reach terminal states.

    Polls jobs at configured intervals and notifies callbacks on state transitions.
    Supports monitoring single or multiple jobs with target status detection.

    Uses per-cycle caching: jobs are fetched once per poll cycle and the result
    is reused by check_condition, get_current_state, and _notify_callbacks,
    reducing SLURM queries from 3*N to N per cycle.

    When a :class:`JobStateTransitionRepository` is injected, observed
    state changes are additionally persisted to the SSOT
    ``job_state_transitions`` table with ``source='cli_monitor'``.
    DB failures are logged but never propagated — the CLI monitor must
    keep running even if the database is unavailable.

    ``scheduler_key`` pins every DB write to the right transport axis
    (``'local'`` or ``'ssh:<profile>'``). The CLI threads its resolved
    transport's ``scheduler_key`` in so SSH-backed monitors don't
    accidentally target the local-SLURM row (the regression the SF5
    hardening pass closes at the repository layer).
    """

    def __init__(
        self,
        job_ids: list[int],
        target_statuses: list[JobStatus] | None = None,
        config: MonitorConfig | None = None,
        callbacks: list[Callback] | None = None,
        client: "JobOperationsProtocol | None" = None,
        transition_repo: "JobStateTransitionRepository | None" = None,
        scheduler_key: str = "local",
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
        self.transition_repo = transition_repo
        # V5 axis: every DB write (transitions, completions) pins
        # ``(scheduler_key, job_id)`` together. Defaults to ``'local'`` so
        # existing local-SLURM callers stay source-compatible; the CLI
        # monitor command now threads the resolved transport's key in so
        # SSH-backed monitors write under the correct axis.
        self.scheduler_key = scheduler_key
        self._previous_states: dict[int, JobStatus] = {}
        self._cached_jobs: list[BaseJob] | None = None

        logger.debug(
            f"JobMonitor initialized for jobs {self.job_ids}, "
            f"target statuses: {[s.value for s in self.target_statuses]}"
        )

    def _get_monitored_jobs(self) -> list[BaseJob]:
        """Get monitored jobs, using per-cycle cache.

        First call per cycle fetches from SLURM via
        :meth:`JobOperationsProtocol.status`; subsequent calls within
        the same cycle return the cached result. Uses the Protocol
        method rather than :meth:`Slurm.retrieve` so SSH-backed
        monitors (``srunx monitor jobs --profile ...``) work too —
        ``retrieve`` is a local-Slurm-only legacy name.
        """
        if self._cached_jobs is not None:
            return self._cached_jobs

        from srunx.models import Job

        jobs: list[BaseJob] = []
        for job_id in self.job_ids:
            try:
                jobs.append(self.client.status(job_id))
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
                # Persist to SSOT (job_state_transitions) when a repo is
                # injected. Skip the first observation (previous_status
                # is None) per design: the CLI monitor records real
                # transitions only, mirroring the poller's behavior.
                # DB errors MUST NOT break the CLI monitor.
                if self.transition_repo is not None and previous_status is not None:
                    try:
                        self.transition_repo.insert(
                            job_id=job.job_id,
                            from_status=previous_status.value,
                            to_status=current_status.value,
                            source="cli_monitor",
                            scheduler_key=self.scheduler_key,
                        )
                    except Exception as e:
                        logger.warning(
                            f"Failed to record transition for job "
                            f"{job.job_id} ({previous_status.value} -> "
                            f"{current_status.value}): {e}"
                        )

                self._notify_transition(job, current_status)
                self._previous_states[job.job_id] = current_status

    def _notify_transition(self, job: BaseJob, status: JobStatus) -> None:
        """Invoke appropriate callback methods based on job status transition."""
        logger.debug(f"Job {job.job_id} transitioned to {status.value}")

        # Mirror terminal states into the state DB (best-effort; the
        # helper swallows + logs at debug if the DB isn't available).
        if (
            status
            in {
                JobStatus.COMPLETED,
                JobStatus.FAILED,
                JobStatus.CANCELLED,
                JobStatus.TIMEOUT,
            }
            and job.job_id is not None
        ):
            from srunx.db.cli_helpers import record_completion

            try:
                record_completion(
                    int(job.job_id), status, scheduler_key=self.scheduler_key
                )
            except Exception as exc:
                # ``record_completion`` is best-effort internally, but a
                # bug in it (or a monkeypatched test) must never prevent
                # callback notifications.
                logger.warning(f"record_completion failed: {exc}")

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
