"""Job lifecycle sinks — the interface slurm/ clients emit events through
without reaching into ``observability/`` directly.

Concrete sinks (``DBRecorderSink``, ``CallbackSink``) live under
``observability/``. The ``slurm/`` layer only sees this protocol, keeping
execution transport independent of persistence / notification concerns
(#156 Phase 5 / #161).
"""

from typing import Any, Literal, Protocol, runtime_checkable

from srunx.domain import BaseJob


@runtime_checkable
class JobLifecycleSink(Protocol):
    """Receives job lifecycle events from a SLURM client.

    Two events are emitted today:

    - ``on_submit`` — after a successful ``sbatch`` returns a job id.
    - ``on_terminal`` — when ``monitor()`` observes a terminal SLURM
      state (``COMPLETED`` / ``FAILED`` / ``CANCELLED`` / ``TIMEOUT``).

    ``on_transition`` (non-terminal state changes) is deliberately not
    part of the protocol yet — the local client has no place it would
    fire from. Added when Phase 8 pollers gain push-based state.
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
    ) -> None: ...

    def on_terminal(self, job: BaseJob) -> None: ...


class NoOpSink:
    """Default sink that records nothing — used when the caller opts out."""

    def on_submit(self, job: BaseJob, **kwargs: Any) -> None:
        pass

    def on_terminal(self, job: BaseJob) -> None:
        pass


class CompositeSink:
    """Fan-out to multiple sinks in declared order.

    Exceptions propagate to the caller — this preserves the legacy
    callback-invocation contract where a buggy ``SlackCallback`` surfaced
    its traceback instead of silently disappearing. Sinks that want
    best-effort semantics (the DB recorder) swallow internally.
    """

    def __init__(self, sinks: list[JobLifecycleSink]) -> None:
        self._sinks: list[JobLifecycleSink] = list(sinks)

    def on_submit(self, job: BaseJob, **kwargs: Any) -> None:
        for sink in self._sinks:
            sink.on_submit(job, **kwargs)

    def on_terminal(self, job: BaseJob) -> None:
        for sink in self._sinks:
            sink.on_terminal(job)
