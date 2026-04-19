"""Unified SLURM client protocol.

Defines the interface that both the local ``Slurm`` client and the
``SlurmSSHAdapter`` implement, so that notification pollers and other
downstream consumers can target either transparently.
"""

from __future__ import annotations

from datetime import datetime
from typing import Protocol, runtime_checkable

from pydantic import BaseModel


class JobStatusInfo(BaseModel):
    """Point-in-time snapshot of a SLURM job's status.

    Produced from ``squeue`` output for active jobs and ``sacct`` output
    for jobs that have already left the queue.
    """

    status: str
    started_at: datetime | None = None
    completed_at: datetime | None = None
    duration_secs: int | None = None
    nodelist: str | None = None


@runtime_checkable
class SlurmClientProtocol(Protocol):
    """Abstract interface for batch-querying SLURM job state.

    Implementations must be safe to call from a background thread (the
    notification poller wraps invocations in ``anyio.to_thread.run_sync``).
    """

    def queue_by_ids(self, job_ids: list[int]) -> dict[int, JobStatusInfo]:
        """Return a mapping of ``job_id`` to :class:`JobStatusInfo`.

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
