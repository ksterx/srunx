"""Helper functions used by the CLI submit + monitor paths.

These wrap the per-call ``init_db`` + ``open_connection`` dance that
every CLI-side write to the state DB needs. Failures are best-effort —
they log at debug and never propagate, so a state-DB outage can never
break the caller's primary flow (submit / monitor).

This replaces the dual-write helpers that lived inside
``srunx.history`` before the history cutover (P2-4 #A).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Literal

from srunx.logging import get_logger

if TYPE_CHECKING:
    from srunx.models import JobStatus, JobType
    from srunx.observability.storage.models import SubmissionSource, TransportType

logger = get_logger(__name__)


def record_submission_from_job(
    job: JobType,
    *,
    workflow_name: str | None = None,
    workflow_run_id: int | None = None,
    transport_type: TransportType = "local",
    profile_name: str | None = None,
    scheduler_key: str = "local",
    submission_source: SubmissionSource | None = None,
) -> None:
    """Insert a ``jobs`` row + seed a ``PENDING`` transition for ``job``.

    ``workflow_name`` picks the default ``submission_source`` ("workflow"
    vs "cli") when the caller does not pass ``submission_source``
    explicitly. ``workflow_run_id`` — when provided — links the job back
    to its ``workflow_runs`` row.

    ``transport_type`` / ``profile_name`` / ``scheduler_key`` describe
    where the job was submitted to. Defaults are the local-SLURM triple
    so existing callers (CLI ``srunx submit``, local ``Slurm.submit``)
    record exactly what they did before. SSH callers (``SlurmSSHAdapter``
    on the Web / MCP / future CLI paths) must pass the matching triple;
    :func:`srunx.observability.storage.repositories.jobs._validate_transport_triple` rejects
    mismatches before they reach the DB.

    Fails closed — any exception is logged at debug and silently
    swallowed. The caller must NOT depend on a non-None return.
    """
    try:
        if job.job_id is None:
            return

        from srunx.models import Job
        from srunx.observability.storage.connection import initialized_connection
        from srunx.observability.storage.repositories.job_state_transitions import (
            JobStateTransitionRepository,
        )
        from srunx.observability.storage.repositories.jobs import JobRepository

        # ``submission_source`` falls back to the original workflow/cli
        # heuristic when the caller does not set it explicitly. The
        # Literal cast keeps mypy happy under ``from __future__ import
        # annotations``.
        effective_source: Literal["cli", "web", "workflow"]
        if submission_source is not None:
            effective_source = submission_source  # type: ignore[assignment]
        else:
            effective_source = "workflow" if workflow_name else "cli"

        with initialized_connection() as conn:
            resources = getattr(job, "resources", None)
            environment = getattr(job, "environment", None)
            command_val: list[str] | None = None
            if isinstance(job, Job) and job.command is not None:
                command_val = (
                    job.command if isinstance(job.command, list) else [str(job.command)]
                )

            job_repo = JobRepository(conn)
            row_id = job_repo.record_submission(
                job_id=int(job.job_id),
                name=job.name,
                status=(job._status.value if hasattr(job, "_status") else "PENDING"),
                submission_source=effective_source,
                transport_type=transport_type,
                profile_name=profile_name,
                scheduler_key=scheduler_key,
                command=command_val,
                nodes=getattr(resources, "nodes", None) if resources else None,
                gpus_per_node=(
                    getattr(resources, "gpus_per_node", None) if resources else None
                ),
                memory_per_node=(
                    getattr(resources, "memory_per_node", None) if resources else None
                ),
                time_limit=(
                    getattr(resources, "time_limit", None) if resources else None
                ),
                partition=(
                    getattr(resources, "partition", None) if resources else None
                ),
                nodelist=(getattr(resources, "nodelist", None) if resources else None),
                conda=(getattr(environment, "conda", None) if environment else None),
                venv=(getattr(environment, "venv", None) if environment else None),
                env_vars=(
                    getattr(environment, "env_vars", None) if environment else None
                ),
                workflow_run_id=workflow_run_id,
            )
            # Seed a baseline transition only when we actually inserted
            # the row. INSERT OR IGNORE returns 0 when the row already
            # exists; seeding again would be harmless but produces
            # noise in the transitions table.
            if row_id > 0:
                JobStateTransitionRepository(conn).insert(
                    job_id=int(job.job_id),
                    from_status=None,
                    to_status="PENDING",
                    source="webhook",
                    scheduler_key=scheduler_key,
                )
    except Exception as exc:  # noqa: BLE001 — best-effort
        logger.debug(f"record_submission_from_job failed: {exc}")


def create_cli_workflow_run(
    workflow_name: str,
    *,
    yaml_path: str | None = None,
    args: dict[str, Any] | None = None,
) -> int | None:
    """Insert a ``workflow_runs`` row for a CLI-launched workflow.

    Called at the start of :meth:`srunx.runner.WorkflowRunner.run` so
    CLI workflow submissions carry the same ``workflow_run_id`` that
    the Web UI path creates. Without this row, ``compute_workflow_stats``
    (``JOIN workflow_runs ON workflow_run_id``) reports zero CLI jobs
    per workflow even though they're in ``jobs``.

    Returns the new row id, or ``None`` on any error so the caller
    treats the link as best-effort and keeps submitting.
    """
    try:
        from srunx.observability.storage.connection import initialized_connection
        from srunx.observability.storage.repositories.workflow_runs import (
            WorkflowRunRepository,
        )

        with initialized_connection() as conn:
            return WorkflowRunRepository(conn).create(
                workflow_name=workflow_name,
                yaml_path=yaml_path,
                args=args,
                triggered_by="cli",
            )
    except Exception as exc:  # noqa: BLE001 — best-effort
        logger.debug(f"create_cli_workflow_run failed: {exc}")
        return None


def record_completion(
    job_id: int,
    status: JobStatus,
    *,
    scheduler_key: str = "local",
) -> None:
    """Mark ``job_id`` terminal in ``jobs`` + append a cli_monitor transition.

    No-ops if the job row doesn't exist in the state DB yet (e.g. the
    poller hasn't observed it, or it pre-dates the cutover). The
    read-modify-write of ``job_state_transitions`` runs inside a
    ``BEGIN IMMEDIATE`` so two racing observers (e.g. CLI monitor +
    poller) can't both append the same terminal state.

    ``scheduler_key`` defaults to ``'local'`` so the CLI monitor path
    (which only observes local SLURM) behaves identically to pre-V5.
    """
    try:
        from srunx.observability.storage.connection import (
            initialized_connection,
            transaction,
        )
        from srunx.observability.storage.repositories.job_state_transitions import (
            JobStateTransitionRepository,
        )
        from srunx.observability.storage.repositories.jobs import JobRepository

        with initialized_connection() as conn:
            repo = JobRepository(conn)
            if repo.get(job_id, scheduler_key=scheduler_key) is None:
                return
            transition_repo = JobStateTransitionRepository(conn)
            with transaction(conn, "IMMEDIATE"):
                latest = transition_repo.latest_for_job(
                    job_id, scheduler_key=scheduler_key
                )
                latest_status = latest.to_status if latest is not None else None
                if latest_status != status.value:
                    transition_repo.insert(
                        job_id=job_id,
                        from_status=latest_status,
                        to_status=status.value,
                        source="cli_monitor",
                        scheduler_key=scheduler_key,
                    )
                repo.update_completion(
                    job_id, status.value, scheduler_key=scheduler_key
                )
    except Exception as exc:  # noqa: BLE001 — best-effort
        logger.debug(f"record_completion failed: {exc}")


def list_recent_jobs(
    limit: int = 100,
    *,
    job_ids: list[int] | None = None,
    scheduler_key: str | None = None,
) -> list[dict[str, Any]]:
    """Return the N most recent jobs as legacy-shaped dicts.

    Thin wrapper around :meth:`JobRepository.list_recent_as_dict`.
    When ``job_ids`` is supplied, the SQL filter pushes the IDs down
    and bypasses the ``LIMIT`` so the CLI's ``srunx sacct -j <id>``
    finds rows that fell outside the most recent ``limit`` page.

    ``scheduler_key`` (``"local"`` / ``"ssh:<profile>"``) scopes to
    a single transport — passed through to the SQL ``WHERE`` so
    ``srunx sacct --profile X`` only sees that cluster's jobs.
    """
    from srunx.observability.storage.connection import initialized_connection
    from srunx.observability.storage.repositories.jobs import JobRepository

    with initialized_connection() as conn:
        return JobRepository(conn).list_recent_as_dict(
            limit=limit, job_ids=job_ids, scheduler_key=scheduler_key
        )


def compute_job_stats(
    from_date: str | None = None,
    to_date: str | None = None,
    *,
    scheduler_key: str | None = None,
) -> dict[str, Any]:
    """Return aggregate job stats in the legacy ``get_job_stats`` shape.

    Thin wrapper around :meth:`JobRepository.compute_stats` that owns
    the connection; used by the CLI ``report`` command.

    ``scheduler_key`` filters to a single transport for SSH parity
    on ``srunx sreport --profile X``.
    """
    from srunx.observability.storage.connection import initialized_connection
    from srunx.observability.storage.repositories.jobs import JobRepository

    with initialized_connection() as conn:
        return JobRepository(conn).compute_stats(
            from_date=from_date, to_date=to_date, scheduler_key=scheduler_key
        )


def compute_workflow_stats(
    workflow_name: str,
    *,
    scheduler_key: str | None = None,
) -> dict[str, Any]:
    """Return workflow-scoped stats in the legacy ``get_workflow_stats`` shape.

    Joins ``jobs`` with ``workflow_runs`` on ``workflow_name`` and
    aggregates. Fields: ``workflow_name``, ``total_jobs``,
    ``avg_duration_seconds``, ``first_submitted``, ``last_submitted``.
    Used by the CLI ``report --workflow`` command.

    ``scheduler_key`` scopes the join to jobs that ran on a given
    transport so ``--profile X`` reports only that cluster's runs
    of the workflow.
    """
    from srunx.observability.storage.connection import initialized_connection

    scheduler_clause = ""
    params: tuple[Any, ...] = (workflow_name,)
    if scheduler_key is not None:
        scheduler_clause = " AND j.scheduler_key = ?"
        params = (workflow_name, scheduler_key)

    with initialized_connection() as conn:
        row = conn.execute(
            f"""
            SELECT
                COUNT(*)                          AS total_jobs,
                AVG(j.duration_secs)              AS avg_duration_seconds,
                MIN(j.submitted_at)               AS first_submitted,
                MAX(j.submitted_at)               AS last_submitted
            FROM jobs j
            JOIN workflow_runs wr ON wr.id = j.workflow_run_id
            WHERE wr.workflow_name = ?{scheduler_clause}
            """,
            params,
        ).fetchone()

    return {
        "workflow_name": workflow_name,
        "total_jobs": int(row["total_jobs"] or 0),
        "avg_duration_seconds": (
            float(row["avg_duration_seconds"])
            if row["avg_duration_seconds"] is not None
            else None
        ),
        "first_submitted": row["first_submitted"],
        "last_submitted": row["last_submitted"],
    }
