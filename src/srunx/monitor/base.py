"""Base monitor class for SLURM monitoring implementations."""

from abc import ABC, abstractmethod
from typing import Any
import signal
import time
from loguru import logger

from srunx.callbacks import Callback
from srunx.monitor.types import MonitorConfig


class BaseMonitor(ABC):
    """
    Abstract base class for SLURM monitoring implementations.

    Provides common functionality for polling, timeout handling, and signal management.
    Subclasses implement condition checking and state retrieval.
    """

    def __init__(
        self,
        config: MonitorConfig | None = None,
        callbacks: list[Callback] | None = None,
    ) -> None:
        """
        Initialize monitor with configuration and callbacks.

        Args:
            config: Monitoring configuration. Defaults to MonitorConfig() if None.
            callbacks: List of notification callbacks. Defaults to empty list if None.

        Raises:
            ValidationError: If config validation fails
        """
        self.config = config or MonitorConfig()
        self.callbacks = callbacks or []
        self._stop_requested = False
        self._setup_signal_handlers()

        if self.config.is_aggressive:
            logger.warning(
                f"Aggressive polling interval ({self.config.poll_interval}s) "
                "may impact SLURM performance on large clusters"
            )

    def _setup_signal_handlers(self) -> None:
        """
        Setup graceful shutdown handlers for SIGTERM and SIGINT.

        Sets _stop_requested flag to True when signal received.
        """
        signal.signal(signal.SIGTERM, self._handle_signal)
        signal.signal(signal.SIGINT, self._handle_signal)

    def _handle_signal(self, signum: int, frame: Any) -> None:
        """
        Handle shutdown signals gracefully.

        Args:
            signum: Signal number (SIGTERM=15, SIGINT=2)
            frame: Current stack frame (unused)
        """
        logger.info(f"Received signal {signum}, stopping monitor...")
        self._stop_requested = True

    @abstractmethod
    def check_condition(self) -> bool:
        """
        Check if monitoring condition is met.

        Subclasses implement specific condition logic:
        - JobMonitor: Check if job reached target state
        - ResourceMonitor: Check if resource threshold met

        Returns:
            True if condition met (monitoring should stop in until-mode)
            False if condition not yet met

        Raises:
            SlurmError: If SLURM command fails
        """
        pass

    @abstractmethod
    def get_current_state(self) -> dict[str, Any]:
        """
        Get current monitoring state for comparison and logging.

        Returns dictionary with state information for:
        - Duplicate notification prevention (continuous mode)
        - State change detection
        - Logging and debugging

        Returns:
            Dictionary with current state. Structure varies by subclass:
            - JobMonitor: {"job_id": int, "status": JobStatus}
            - ResourceMonitor: {"partition": str, "gpus_available": int}

        Raises:
            SlurmError: If SLURM command fails
        """
        pass

    def watch_until(self) -> None:
        """
        Monitor until condition is met (blocking).

        Polls at configured interval until:
        1. check_condition() returns True -> success
        2. Timeout reached -> TimeoutError
        3. Signal received (Ctrl+C) -> graceful exit

        Raises:
            TimeoutError: If timeout reached before condition met
            SlurmError: If SLURM command fails repeatedly
        """
        start_time = time.time()
        logger.info(
            f"Starting until-condition monitoring "
            f"(interval={self.config.poll_interval}s, timeout={self.config.timeout}s)"
        )

        while not self._stop_requested:
            if self.check_condition():
                logger.info("Condition met, stopping monitor")
                self._notify_callbacks("condition_met")
                return

            # Check timeout
            if self.config.timeout:
                elapsed = time.time() - start_time
                if elapsed >= self.config.timeout:
                    logger.warning(f"Timeout reached ({self.config.timeout}s)")
                    self._notify_callbacks("timeout")
                    raise TimeoutError(
                        f"Monitoring timeout after {elapsed:.0f} seconds"
                    )

            time.sleep(self.config.poll_interval)

        logger.info("Monitor stopped by user request")

    def watch_continuous(self) -> None:
        """
        Monitor continuously until signal received (blocking).

        Polls indefinitely and notifies on state changes:
        1. Get current state
        2. Compare with previous state
        3. If different and notify_on_change: call callbacks
        4. Sleep until next poll
        5. Repeat until Ctrl+C or SIGTERM

        Duplicate notifications prevented by state comparison.

        Raises:
            SlurmError: If SLURM command fails repeatedly
        """
        previous_state: dict[str, Any] | None = None
        logger.info(
            f"Starting continuous monitoring "
            f"(interval={self.config.poll_interval}s, Ctrl+C to stop)"
        )

        while not self._stop_requested:
            current_state = self.get_current_state()

            # Notify only on state change (prevent duplicates)
            if current_state != previous_state and previous_state is not None:
                logger.info(f"State changed: {previous_state} -> {current_state}")
                if self.config.notify_on_change:
                    self._notify_callbacks("state_changed")

            previous_state = current_state
            time.sleep(self.config.poll_interval)

        logger.info("Continuous monitor stopped")

    @abstractmethod
    def _notify_callbacks(self, event: str) -> None:
        """
        Notify all callbacks of event with appropriate data.

        Subclasses implement specific callback invocation:
        - JobMonitor: Call on_completed, on_failed, etc.
        - ResourceMonitor: Call on_resources_available, etc.

        Args:
            event: Event name ("condition_met", "state_changed", "timeout")

        Note:
            Callback errors are logged but don't stop monitoring.
        """
        pass
