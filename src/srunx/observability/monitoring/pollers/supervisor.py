"""Long-running poller supervisor with exception backoff and grace shutdown.

The supervisor owns an ``anyio`` task group that hosts one task per
registered :class:`Poller`. Each task loops over ``run_cycle`` and:

* sleeps ``poller.interval_seconds`` between successful cycles,
* applies 1 → 2 → 4 → ... capped at 60 second exponential backoff on
  exceptions,
* exits cleanly as soon as ``shutdown_event`` is set.

:meth:`PollerSupervisor.shutdown` signals the event and waits up to
``grace_seconds`` for pollers to wind down; if the grace window expires
the task group's cancel scope is triggered so lifespan shutdown does
not hang.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable

import anyio
from anyio.abc import TaskGroup

from srunx.common.logging import get_logger

logger = get_logger(__name__)

# Exponential backoff bounds (seconds) for poller cycle exceptions.
_BACKOFF_BASE_SECS: float = 1.0
_BACKOFF_CAP_SECS: float = 60.0


@runtime_checkable
class Poller(Protocol):
    """Contract implemented by supervised long-running tasks.

    Implementations provide a human-readable ``name`` (for logs), an
    ``interval_seconds`` between cycles, and an ``async run_cycle``
    coroutine executing one iteration of work.
    """

    name: str
    interval_seconds: float

    async def run_cycle(self) -> None: ...


class PollerSupervisor:
    """Supervise ``Poller`` instances with backoff and grace shutdown."""

    def __init__(
        self,
        pollers: list[Poller],
        shutdown_event: anyio.Event | None = None,
    ) -> None:
        """Initialize with a list of pollers and an optional shutdown event.

        Args:
            pollers: Pollers to supervise. Empty list is allowed.
            shutdown_event: Externally provided shutdown signal. If
                ``None``, the supervisor creates its own event so
                callers that only need ``shutdown()`` do not have to
                wire one up manually.
        """
        self._pollers: list[Poller] = list(pollers)
        self._shutdown_event: anyio.Event = (
            shutdown_event if shutdown_event is not None else anyio.Event()
        )
        self._task_group: TaskGroup | None = None

    @property
    def shutdown_event(self) -> anyio.Event:
        """Return the event used to signal graceful shutdown."""
        return self._shutdown_event

    async def start_all(self) -> None:
        """Run every registered poller until shutdown.

        This coroutine blocks for the lifetime of the task group; call
        it from a background task (typically inside a FastAPI lifespan
        or another task group) and use :meth:`shutdown` from a separate
        task to stop it.
        """
        async with anyio.create_task_group() as tg:
            self._task_group = tg
            for poller in self._pollers:
                tg.start_soon(
                    self._run_with_backoff,
                    poller,
                    name=f"poller:{poller.name}",
                )
        # Reset after the task group exits so a subsequent start_all()
        # call starts fresh.
        self._task_group = None

    async def shutdown(self, grace_seconds: float = 5.0) -> None:
        """Signal shutdown and cancel remaining work after the grace window.

        Sets the shutdown event (waking any poller currently sleeping),
        waits up to ``grace_seconds`` for the task group to drain, and
        cancels it if the grace window elapses first.

        Args:
            grace_seconds: Maximum time to wait for in-flight cycles to
                observe the shutdown event and exit cleanly.
        """
        if not self._shutdown_event.is_set():
            self._shutdown_event.set()

        task_group = self._task_group
        if task_group is None:
            return

        with anyio.move_on_after(grace_seconds) as scope:
            # Sleep in small slices so we return promptly once the
            # task group has fully drained (tg.cancel_scope.cancelled_caught
            # is only observable once the async-with exits, which happens
            # in the task running start_all(), not here).
            while self._task_group is not None:
                await anyio.sleep(0.05)

        if scope.cancelled_caught:
            logger.warning(
                "poller supervisor grace window expired; cancelling task group "
                f"(grace_seconds={grace_seconds})"
            )
            task_group.cancel_scope.cancel()

    async def _run_with_backoff(self, poller: Poller) -> None:
        """Run a single poller forever with exception backoff.

        Args:
            poller: The poller instance to drive.
        """
        backoff_secs: float = _BACKOFF_BASE_SECS

        while True:
            if self._shutdown_event.is_set():
                return

            try:
                await poller.run_cycle()
            except Exception as exc:
                # Supervise every exception so a crashing poller does not
                # take down the task group or its siblings.
                logger.bind(
                    poller=poller.name,
                    error_type=type(exc).__name__,
                    backoff_secs=backoff_secs,
                ).error(f"poller {poller.name} cycle failed: {exc}")

                if await self._sleep_or_shutdown(backoff_secs):
                    return
                backoff_secs = min(backoff_secs * 2, _BACKOFF_CAP_SECS)
                continue

            # Cycle succeeded: reset backoff and wait the configured
            # interval before the next cycle.
            backoff_secs = _BACKOFF_BASE_SECS
            if await self._sleep_or_shutdown(poller.interval_seconds):
                return

    async def _sleep_or_shutdown(self, seconds: float) -> bool:
        """Sleep for ``seconds`` or until shutdown, whichever fires first.

        Args:
            seconds: Time to sleep in seconds. Non-positive values skip
                the sleep entirely but still observe the shutdown event.

        Returns:
            True if the shutdown event fired during the wait, False
            otherwise.
        """
        if self._shutdown_event.is_set():
            return True
        if seconds <= 0:
            return self._shutdown_event.is_set()

        with anyio.move_on_after(seconds):
            await self._shutdown_event.wait()
        return self._shutdown_event.is_set()
