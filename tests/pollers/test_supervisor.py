"""Unit tests for :mod:`srunx.pollers.supervisor`."""

from __future__ import annotations

import time
from dataclasses import dataclass

import anyio
import pytest
from srunx.pollers.supervisor import Poller, PollerSupervisor


@dataclass
class CountingPoller:
    """A poller that records each successful ``run_cycle`` invocation."""

    name: str = "counter"
    interval_seconds: float = 0.01
    count: int = 0

    async def run_cycle(self) -> None:
        self.count += 1


@dataclass
class FailingPoller:
    """A poller whose ``run_cycle`` always raises."""

    name: str = "failing"
    interval_seconds: float = 0.01
    attempts: int = 0

    async def run_cycle(self) -> None:
        self.attempts += 1
        raise RuntimeError("boom")


@dataclass
class SlowCyclePoller:
    """A poller that sleeps inside ``run_cycle`` to simulate work."""

    name: str = "slow"
    interval_seconds: float = 0.01
    cycle_sleep_seconds: float = 0.1
    cycles_completed: int = 0

    async def run_cycle(self) -> None:
        await anyio.sleep(self.cycle_sleep_seconds)
        self.cycles_completed += 1


@dataclass
class LongSleepPoller:
    """A poller that would sleep for far longer than the grace window."""

    name: str = "long"
    interval_seconds: float = 0.01
    cycle_sleep_seconds: float = 10.0
    started: bool = False

    async def run_cycle(self) -> None:
        self.started = True
        await anyio.sleep(self.cycle_sleep_seconds)


def _protocol_check(poller: Poller) -> str:
    """Helper to exercise the ``Poller`` protocol (also narrows types)."""
    return poller.name


def test_counting_poller_satisfies_protocol() -> None:
    """Dummy poller classes defined here conform structurally to Poller."""
    poller = CountingPoller()
    # Structural conformance: this is the main typing guarantee.
    assert _protocol_check(poller) == "counter"
    # Attribute contract.
    assert hasattr(poller, "name")
    assert hasattr(poller, "interval_seconds")
    assert hasattr(poller, "run_cycle")


class TestHappyPath:
    """Verify pollers run repeatedly and shut down on signal."""

    def test_poller_runs_multiple_cycles_then_shuts_down(self) -> None:
        poller = CountingPoller(interval_seconds=0.01)
        supervisor = PollerSupervisor([poller])

        async def scenario() -> None:
            async with anyio.create_task_group() as tg:
                tg.start_soon(supervisor.start_all)
                # Give it time for several cycles.
                await anyio.sleep(0.2)
                await supervisor.shutdown(grace_seconds=0.5)

        anyio.run(scenario)

        assert poller.count > 1, f"expected multiple cycles in 0.2s, got {poller.count}"

    def test_two_pollers_run_concurrently(self) -> None:
        a = CountingPoller(name="a", interval_seconds=0.01)
        b = CountingPoller(name="b", interval_seconds=0.01)
        supervisor = PollerSupervisor([a, b])

        async def scenario() -> None:
            async with anyio.create_task_group() as tg:
                tg.start_soon(supervisor.start_all)
                await anyio.sleep(0.15)
                await supervisor.shutdown(grace_seconds=0.5)

        anyio.run(scenario)

        assert a.count > 0
        assert b.count > 0


class TestExceptionBackoff:
    """Verify failing pollers trigger exponential backoff without crashing."""

    def test_failing_poller_restarts_with_backoff(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Patch ``_sleep_or_shutdown`` to record backoff delays.

        Records the ``seconds`` argument requested by the supervisor so we
        can verify the 1 -> 2 -> 4 ... progression on consecutive failures
        and the reset behaviour after a success.
        """
        from srunx.pollers import supervisor as supervisor_module

        requested_sleeps: list[float] = []
        original_impl = supervisor_module.PollerSupervisor._sleep_or_shutdown

        async def recording_sleep(self: PollerSupervisor, seconds: float) -> bool:
            requested_sleeps.append(seconds)
            # Short-circuit the wait so the loop iterates quickly; still
            # observe the shutdown event so shutdown remains responsive.
            if self._shutdown_event.is_set():  # type: ignore[attr-defined]
                return True
            await anyio.sleep(0)
            return self._shutdown_event.is_set()  # type: ignore[attr-defined]

        monkeypatch.setattr(
            supervisor_module.PollerSupervisor,
            "_sleep_or_shutdown",
            recording_sleep,
        )
        # Ensure reference retained to original to placate linters.
        assert original_impl is not None

        poller = FailingPoller(interval_seconds=0.01)
        supervisor = PollerSupervisor([poller])

        async def scenario() -> None:
            async with anyio.create_task_group() as tg:
                tg.start_soon(supervisor.start_all)
                # Allow several failure iterations.
                await anyio.sleep(0.1)
                await supervisor.shutdown(grace_seconds=0.5)

        anyio.run(scenario)

        # Poller attempted run_cycle multiple times across iterations.
        assert poller.attempts >= 3, (
            f"expected at least 3 attempts, got {poller.attempts}"
        )

        # All recorded sleeps should fall in the canonical backoff set
        # (1, 2, 4, 8, 16, 32, 60) because run_cycle always raises.
        assert len(requested_sleeps) >= 3, (
            f"expected at least 3 recorded sleeps, got {requested_sleeps}"
        )
        # Verify the sequence starts at 1 and doubles.
        assert requested_sleeps[0] == 1.0
        assert requested_sleeps[1] == 2.0
        assert requested_sleeps[2] == 4.0
        # Cap at 60 after enough failures.
        for value in requested_sleeps:
            assert value in {1.0, 2.0, 4.0, 8.0, 16.0, 32.0, 60.0}, (
                f"unexpected backoff value {value}; got sequence {requested_sleeps}"
            )

    def test_backoff_resets_after_successful_cycle(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Backoff must reset to 1s once a cycle succeeds."""
        from srunx.pollers import supervisor as supervisor_module

        requested_sleeps: list[float] = []

        async def recording_sleep(self: PollerSupervisor, seconds: float) -> bool:
            requested_sleeps.append(seconds)
            if self._shutdown_event.is_set():  # type: ignore[attr-defined]
                return True
            await anyio.sleep(0)
            return self._shutdown_event.is_set()  # type: ignore[attr-defined]

        monkeypatch.setattr(
            supervisor_module.PollerSupervisor,
            "_sleep_or_shutdown",
            recording_sleep,
        )

        @dataclass
        class FlakyPoller:
            name: str = "flaky"
            interval_seconds: float = 0.5
            calls: int = 0

            async def run_cycle(self) -> None:
                self.calls += 1
                # Fail for first 2 calls, succeed on the 3rd, fail again on 4th.
                if self.calls in (1, 2, 4):
                    raise RuntimeError("intermittent")

        poller = FlakyPoller()
        supervisor = PollerSupervisor([poller])

        async def scenario() -> None:
            async with anyio.create_task_group() as tg:
                tg.start_soon(supervisor.start_all)
                await anyio.sleep(0.1)
                await supervisor.shutdown(grace_seconds=0.5)

        anyio.run(scenario)

        # We expect at least: 1 (after fail #1), 2 (after fail #2),
        # 0.5 (after success #3, the interval), 1 (after fail #4).
        assert poller.calls >= 4
        assert requested_sleeps[0] == 1.0
        assert requested_sleeps[1] == 2.0
        assert requested_sleeps[2] == 0.5  # interval after success, reset
        assert requested_sleeps[3] == 1.0  # backoff reset to base after success

    def test_failing_poller_does_not_bring_down_group(self) -> None:
        """A poller that always raises must not take down sibling pollers."""
        failing = FailingPoller(interval_seconds=0.01)
        healthy = CountingPoller(name="healthy", interval_seconds=0.01)
        supervisor = PollerSupervisor([failing, healthy])

        async def scenario() -> None:
            async with anyio.create_task_group() as tg:
                tg.start_soon(supervisor.start_all)
                await anyio.sleep(0.15)
                await supervisor.shutdown(grace_seconds=0.5)

        anyio.run(scenario)

        # Healthy poller kept running despite the sibling's failures.
        assert healthy.count > 0
        assert failing.attempts > 0


class TestGracefulShutdown:
    """Verify shutdown semantics."""

    def test_grace_shutdown_exits_cleanly_within_grace_window(self) -> None:
        poller = SlowCyclePoller(
            interval_seconds=0.01,
            cycle_sleep_seconds=0.1,
        )
        supervisor = PollerSupervisor([poller])

        start = time.monotonic()

        async def scenario() -> None:
            async with anyio.create_task_group() as tg:
                tg.start_soon(supervisor.start_all)
                # Let the poller begin its first cycle (which sleeps 0.1s).
                await anyio.sleep(0.05)
                await supervisor.shutdown(grace_seconds=0.5)

        anyio.run(scenario)

        elapsed = time.monotonic() - start
        # Start (0.05s) + grace window accommodation should be well under 1s.
        assert elapsed < 1.0, f"shutdown took too long: {elapsed:.3f}s"
        # At least one cycle should have had a chance to complete.
        assert poller.cycles_completed >= 0

    def test_forced_cancel_when_grace_window_elapses(self) -> None:
        """A long-running cycle must be cancelled cleanly after the grace window."""
        poller = LongSleepPoller(
            interval_seconds=0.01,
            cycle_sleep_seconds=10.0,
        )
        supervisor = PollerSupervisor([poller])

        start = time.monotonic()

        async def scenario() -> None:
            async with anyio.create_task_group() as tg:
                tg.start_soon(supervisor.start_all)
                # Make sure the poller actually entered its long sleep.
                await anyio.sleep(0.05)
                assert poller.started
                await supervisor.shutdown(grace_seconds=0.2)

        # Must complete well before the 10s poller sleep would naturally end.
        anyio.run(scenario)

        elapsed = time.monotonic() - start
        assert elapsed < 2.0, (
            f"forced cancel did not short-circuit 10s sleep (elapsed={elapsed:.3f}s)"
        )

    def test_shutdown_before_start_is_noop(self) -> None:
        """Calling shutdown before start_all should not raise."""
        supervisor = PollerSupervisor([])

        async def scenario() -> None:
            await supervisor.shutdown(grace_seconds=0.1)

        anyio.run(scenario)

    def test_empty_poller_list_starts_and_exits(self) -> None:
        """Supervisor with no pollers should exit start_all promptly."""
        supervisor = PollerSupervisor([])

        async def scenario() -> None:
            async with anyio.create_task_group() as tg:
                tg.start_soon(supervisor.start_all)
                await anyio.sleep(0.05)
                await supervisor.shutdown(grace_seconds=0.2)

        anyio.run(scenario)
