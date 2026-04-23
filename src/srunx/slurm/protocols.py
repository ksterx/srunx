"""Unified SLURM client protocol.

Defines the interface that both the local :class:`LocalClient` and the
``SlurmSSHAdapter`` implement, so that notification pollers and other
downstream consumers can target either transparently.
"""

from __future__ import annotations

from collections.abc import Callable
from contextlib import AbstractContextManager
from datetime import datetime
from typing import TYPE_CHECKING, Protocol, runtime_checkable

from pydantic import BaseModel, Field

if TYPE_CHECKING:
    from srunx.domain import BaseJob, RunnableJobType
    from srunx.runtime.rendering import SubmissionRenderContext


class JobSnapshot(BaseModel):
    """Point-in-time snapshot of a SLURM job's status.

    Produced from ``squeue`` output for active jobs and ``sacct`` output
    for jobs that have already left the queue.
    """

    status: str
    started_at: datetime | None = None
    completed_at: datetime | None = None
    duration_secs: int | None = None
    nodelist: str | None = None


class LogChunk(BaseModel):
    """Incremental log tail chunk returned by ``tail_log_incremental``.

    Field names match the WebUI ``/api/jobs/{id}/logs`` wire shape so the
    Protocol, the WebUI router, and the CLI all speak the same vocabulary.
    """

    stdout: str
    stderr: str
    stdout_offset: int = Field(ge=0)
    stderr_offset: int = Field(ge=0)


@runtime_checkable
class JobOperations(Protocol):
    """CLI-facing job operations. Pure functions, no side-effects.

    Implementations must be thread-safe for the read paths (``status`` /
    ``queue`` / ``tail_log_incremental``) because CLI ``--follow`` loops
    and polling wrappers may call them concurrently.
    """

    def submit(
        self,
        job: RunnableJobType,
        *,
        submission_context: SubmissionRenderContext | None = None,
    ) -> RunnableJobType:
        """Submit *job* to SLURM and return the populated job object.

        The returned object MUST have ``job_id`` set. DB recording (via
        ``record_submission_from_job``) is the implementation's
        responsibility; callers must not record again.

        ``submission_context`` carries mount / default-path metadata that
        SSH-backed implementations apply via
        :func:`srunx.runtime.rendering.normalize_job_for_submission` before
        rendering the SLURM script, so CLI-supplied absolute ``work_dir``
        / ``log_dir`` paths get rewritten to the remote-mount equivalents.
        Local implementations accept the kwarg for Protocol conformance
        but ignore it (local submission never performs mount translation).
        """
        ...

    def submit_remote_sbatch(
        self,
        remote_path: str,
        *,
        submit_cwd: str | None = None,
        job_name: str | None = None,
        dependency: str | None = None,
        extra_sbatch_args: list[str] | None = None,
        callbacks_job: RunnableJobType | None = None,
    ) -> RunnableJobType:
        """Submit a script that already exists on the remote cluster.

        Distinct from :meth:`submit` because the bytes the cluster
        executes are *user-managed* (typically a synced mount file) —
        no template render, no SFTP-upload to ``$SRUNX_TEMP_DIR``,
        no ``-o $SLURM_LOG_DIR/%x_%j.log`` injection. The user's own
        ``#SBATCH`` directives win.

        ``submit_cwd`` is the remote directory ``sbatch`` runs from;
        SSH sessions otherwise default to ``$HOME`` and relative
        paths inside the script (``#SBATCH --output=./logs/%j.out``)
        would resolve there instead of the mount.

        ``extra_sbatch_args`` are appended to the sbatch command line
        verbatim, so callers can forward CLI-side flags like
        ``-N 4`` / ``--mem=32GB`` without modifying the on-disk
        script. SLURM treats command-line flags as overrides of
        ``#SBATCH`` directives, matching real ``sbatch``'s precedence.

        ``callbacks_job`` is the in-memory :class:`Job` /
        :class:`ShellJob` the caller wants the implementation to fire
        ``Callback.on_job_submitted`` against. Implementations MUST
        record the submission in the state DB and fire callbacks
        before returning, the same as :meth:`submit`. Callers must
        not double-record.
        """
        ...

    def cancel(self, job_id: int) -> None:
        """Cancel *job_id*. Raises :class:`JobNotFoundError` if unknown."""
        ...

    def status(self, job_id: int) -> BaseJob:
        """Return a snapshot of *job_id*'s current status.

        Raises :class:`JobNotFoundError` if unknown. The returned ``BaseJob``
        MUST NOT trigger lazy refresh on attribute access (callers may
        touch ``.status`` after the transport context is closed).
        """
        ...

    def queue(self, user: str | None = None) -> list[BaseJob]:
        """List jobs for *user* (defaults to the transport's current user).

        Local: ``$USER``. SSH: the profile's username. Empty list if
        none, never raises.
        """
        ...

    def tail_log_incremental(
        self,
        job_id: int,
        stdout_offset: int = 0,
        stderr_offset: int = 0,
    ) -> LogChunk:
        """Return new log content since *stdout_offset* / *stderr_offset*.

        For ``follow`` behaviour, callers poll this method with the
        returned offsets. The Protocol itself never blocks.
        """
        ...


@runtime_checkable
class Client(Protocol):
    """Abstract interface for batch-querying SLURM job state.

    Implementations must be safe to call from a background thread (the
    notification poller wraps invocations in ``anyio.to_thread.run_sync``).
    """

    def queue_by_ids(self, job_ids: list[int]) -> dict[int, JobSnapshot]:
        """Return a mapping of ``job_id`` to :class:`JobSnapshot`.

        Active jobs are looked up via ``squeue``; jobs that are no longer
        in the queue fall back to ``sacct``. Jobs that cannot be found in
        either source are omitted from the returned dict. An empty
        ``job_ids`` list yields an empty dict.
        """
        ...


def parse_slurm_datetime(value: str | None) -> datetime | None:
    """Parse a SLURM-formatted timestamp.

    SLURM emits timestamps like ``2026-04-18T10:00:00``. ``"N/A"``,
    ``"Unknown"``, empty strings, and parse failures all return ``None``.
    """
    if not value:
        return None
    cleaned = value.strip()
    if not cleaned or cleaned in {"N/A", "Unknown", "None"}:
        return None
    try:
        return datetime.fromisoformat(cleaned)
    except ValueError:
        return None


def parse_slurm_duration(value: str | None) -> int | None:
    """Parse a SLURM elapsed-time string into integer seconds.

    Accepts ``DD-HH:MM:SS``, ``HH:MM:SS``, and ``MM:SS`` formats. Returns
    ``None`` if the value is missing or unparseable.
    """
    if not value:
        return None
    cleaned = value.strip()
    if not cleaned or cleaned in {"N/A", "Unknown"}:
        return None

    days = 0
    if "-" in cleaned:
        days_str, cleaned = cleaned.split("-", 1)
        try:
            days = int(days_str)
        except ValueError:
            return None

    parts = cleaned.split(":")
    try:
        nums = [int(p) for p in parts]
    except ValueError:
        return None

    if len(nums) == 3:
        hours, minutes, seconds = nums
    elif len(nums) == 2:
        hours = 0
        minutes, seconds = nums
    else:
        return None

    return days * 86400 + hours * 3600 + minutes * 60 + seconds


@runtime_checkable
class WorkflowJobExecutor(Protocol):
    """SLURM job execution interface used by :class:`WorkflowRunner`.

    Narrower than :class:`Client`; focused on the single-job
    submit + monitor cycle that the runner needs. The poller-facing
    batch-query interface stays in :class:`Client` so the
    two concerns stay independently substitutable (ISP).
    """

    def run(
        self,
        job: RunnableJobType,
        *,
        workflow_name: str | None = None,
        workflow_run_id: int | None = None,
        submission_context: SubmissionRenderContext | None = None,
    ) -> RunnableJobType:
        """Submit *job* to SLURM and block until it reaches a terminal status.

        ``submission_context`` carries mount / default-path metadata that
        SSH-backed executors need to translate local ``work_dir`` / ``log_dir``
        values into their remote equivalents before rendering the SLURM
        script. Local executors (e.g. :class:`srunx.slurm.local.LocalClient`)
        accept the kwarg for protocol conformance but ignore it — local
        submission does not perform mount translation.
        """
        ...

    def get_job_output_detailed(
        self,
        job_id: int | str,
        job_name: str | None = None,
        skip_content: bool = False,
    ) -> dict[str, str | list[str] | None]:
        """Return log-file metadata (and optionally content) for *job_id*."""
        ...


WorkflowJobExecutorFactory = Callable[[], AbstractContextManager["WorkflowJobExecutor"]]
"""Context-manager factory for leasing a workflow executor.

Intended usage::

    with executor_factory() as executor:
        executor.run(job, workflow_run_id=...)

Implementations:

- Local: returns ``nullcontext(LocalClient())`` — singleton reuse, no teardown.
- SSH pool: returns a lease from a bounded pool of ``SlurmSSHAdapter``
  clones so concurrent sweep cells don't serialize through one SSH
  connection.
"""
