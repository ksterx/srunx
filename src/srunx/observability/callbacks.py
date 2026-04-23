"""Legacy ``Callback`` → :class:`JobLifecycleSink` adapter.

The legacy ``srunx.callbacks.Callback`` class predates the lifecycle-sink
abstraction. This adapter lets existing ``Slurm(callbacks=[SlackCallback(),
...])`` invocations keep working while ``slurm/local.py`` only sees the
sink Protocol.
"""

from __future__ import annotations

from typing import Any

from srunx.callbacks import Callback
from srunx.domain import BaseJob
from srunx.domain.jobs import JobStatus


class CallbackSink:
    """Routes sink events to the equivalent ``Callback`` hook method."""

    def __init__(self, callback: Callback) -> None:
        self._cb = callback

    def on_submit(self, job: BaseJob, **_: Any) -> None:
        self._cb.on_job_submitted(job)

    def on_terminal(self, job: BaseJob) -> None:
        match job.status:
            case JobStatus.COMPLETED:
                self._cb.on_job_completed(job)
            case JobStatus.FAILED:
                self._cb.on_job_failed(job)
            case JobStatus.CANCELLED | JobStatus.TIMEOUT:
                self._cb.on_job_cancelled(job)
            case _:
                pass  # Non-terminal / unknown — nothing to dispatch.
