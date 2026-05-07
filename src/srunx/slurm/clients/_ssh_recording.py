"""Best-effort DB recording helpers for SSH-submitted jobs.

Both helpers swallow exceptions and log at debug level — submission /
completion recording is observability metadata, never a hard requirement
for SLURM operations to succeed. The ``# noqa: BLE001`` on the broad
catches is the documented behaviour, not a deferral.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from srunx.common.logging import get_logger
from srunx.observability.storage.models import SubmissionSource, TransportType

if TYPE_CHECKING:
    from srunx.domain import JobStatus, RunnableJobType

logger = get_logger(__name__)


def record_job_submission(
    job: RunnableJobType,
    *,
    workflow_name: str | None,
    workflow_run_id: int | None,
    transport_type: TransportType = "ssh",
    profile_name: str | None = None,
    scheduler_key: str | None = None,
    submission_source: SubmissionSource | None = None,
) -> None:
    """Insert a ``jobs`` row for an SSH-submitted job.

    Thin wrapper around :func:`record_submission_from_job` — same
    best-effort contract as the local :class:`Slurm` executor. For
    backward compatibility with the :meth:`run` callsite that only
    passes ``workflow_name`` / ``workflow_run_id``, when
    ``profile_name`` / ``scheduler_key`` are not provided we record
    the row as local (the original pre-V5 behaviour); callsites that
    want the SSH triple must pass them explicitly.
    """
    try:
        from srunx.observability.storage.cli_helpers import (
            record_submission_from_job,
        )

        if profile_name is None:
            # Legacy callsite (pre-V5 style) — pass only the two
            # original kwargs so tests that mock
            # ``record_submission_from_job`` with the original
            # signature (``(job, *, workflow_name, workflow_run_id)``)
            # keep working. The DB default is local anyway.
            record_submission_from_job(
                job,
                workflow_name=workflow_name,
                workflow_run_id=workflow_run_id,
            )
        else:
            resolved_scheduler_key = (
                scheduler_key if scheduler_key is not None else f"ssh:{profile_name}"
            )
            record_submission_from_job(
                job,
                workflow_name=workflow_name,
                workflow_run_id=workflow_run_id,
                transport_type=transport_type,
                profile_name=profile_name,
                scheduler_key=resolved_scheduler_key,
                submission_source=submission_source,
            )
    except Exception as exc:  # noqa: BLE001
        # Best-effort by design: DB unavailability must never block
        # SLURM submission. Caller already has the sbatch result.
        logger.debug(f"record_job_submission failed: {exc}")


def record_completion_safe(
    job_id: int, status: JobStatus, *, scheduler_key: str
) -> None:
    """Record terminal status in ``jobs`` / ``job_state_transitions``.

    Targets the caller's ``scheduler_key`` so SSH-backed jobs update
    the remote-cluster row rather than a (possibly nonexistent) local
    row. Errors are best-effort — the caller has already determined
    the terminal status, and a DB write failure mustn't propagate up
    into the workflow runner's failure path.
    """
    try:
        from srunx.observability.storage.cli_helpers import record_completion

        record_completion(job_id, status, scheduler_key=scheduler_key)
    except Exception as exc:  # noqa: BLE001
        # Best-effort by design — see ``record_job_submission`` rationale.
        logger.debug(f"record_completion_safe failed: {exc}")
