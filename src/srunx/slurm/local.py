"""Public ``Slurm`` wrapper + convenience functions over :class:`LocalClient`."""

from __future__ import annotations

from collections.abc import Sequence

from srunx.callbacks import Callback
from srunx.common.logging import get_logger
from srunx.domain import BaseJob, RunnableJobType
from srunx.observability import CallbackSink, DBRecorderSink
from srunx.runtime.lifecycle import CompositeSink, JobLifecycleSink
from srunx.slurm.clients.local import LocalClient

logger = get_logger(__name__)


class Slurm(LocalClient):
    """Legacy wrapper around :class:`LocalClient` honouring ``callbacks=``.

    The legacy constructor accepted a list of :class:`Callback`
    instances; this subclass translates them into a
    :class:`~srunx.observability.CallbackSink` chain, appends the
    default :class:`~srunx.observability.DBRecorderSink`, and forwards
    the composed sink to the canonical ``LocalClient`` constructor.
    """

    def __init__(
        self,
        default_template: str | None = None,
        callbacks: Sequence[Callback] | None = None,
        sink: JobLifecycleSink | None = None,
    ):
        if sink is None:
            # Order matches legacy behaviour: DB row is written first so
            # callbacks can safely query ``srunx.observability.storage``
            # for their own job (e.g. a future Slack message that links
            # back to the history table). Changing this order is a
            # breaking contract change.
            sinks: list[JobLifecycleSink] = [DBRecorderSink()]
            sinks.extend(CallbackSink(cb) for cb in (callbacks or []))
            sink = CompositeSink(sinks)
        super().__init__(default_template=default_template, sink=sink)
        # Preserve ``self.callbacks`` for callers that introspect it.
        self.callbacks = list(callbacks) if callbacks else []


def submit_job(
    job: RunnableJobType,
    template_path: str | None = None,
    callbacks: Sequence[Callback] | None = None,
    verbose: bool = False,
) -> RunnableJobType:
    """Submit a job to SLURM (convenience function).

    Routes through :class:`Slurm` so DB recording + legacy callbacks are
    wired by default.
    """
    client = Slurm(callbacks=callbacks)
    return client.submit(job, template_path=template_path, verbose=verbose)


def retrieve_job(job_id: int) -> BaseJob:
    """Get job status (convenience function)."""
    return Slurm().retrieve(job_id)


def cancel_job(job_id: int) -> None:
    """Cancel a job (convenience function)."""
    Slurm().cancel(job_id)
