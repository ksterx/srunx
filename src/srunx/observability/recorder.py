"""DB-recorder sink — persists job lifecycle events into the srunx state DB.

Previously this logic was inlined in ``srunx.slurm.local.Slurm.submit`` /
``._record_completion``; extracted here so ``slurm/local.py`` no longer
imports ``srunx.observability.storage`` (#161).
"""

from typing import Literal

from srunx.domain import BaseJob


class DBRecorderSink:
    """Writes submission and terminal-state rows to ``srunx.observability.storage``.

    DB writes are best-effort — exceptions are logged and swallowed
    inside the underlying ``record_submission_from_job`` /
    ``record_completion`` helpers, preserving the behaviour the old
    inlined code had.
    """

    def on_submit(
        self,
        job: BaseJob,
        *,
        workflow_name: str | None = None,
        workflow_run_id: int | None = None,
        transport_type: Literal["local", "ssh"] = "local",
        profile_name: str | None = None,
        scheduler_key: str = "local",
        record_history: bool = True,
    ) -> None:
        if not record_history:
            return
        from srunx.observability.storage.cli_helpers import record_submission_from_job

        record_submission_from_job(
            job,
            workflow_name=workflow_name,
            workflow_run_id=workflow_run_id,
            transport_type=transport_type,
            profile_name=profile_name,
            scheduler_key=scheduler_key,
        )

    def on_terminal(self, job: BaseJob) -> None:
        if job.job_id is None:
            return
        from srunx.observability.storage.cli_helpers import record_completion

        record_completion(int(job.job_id), job.status)
