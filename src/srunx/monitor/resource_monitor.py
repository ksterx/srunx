"""Resource monitoring implementation for SLURM."""

import re
import subprocess
from typing import Any

from loguru import logger

from srunx.callbacks import Callback
from srunx.monitor.base import BaseMonitor
from srunx.monitor.types import MonitorConfig, ResourceSnapshot


class ResourceMonitor(BaseMonitor):
    """Monitor SLURM GPU resources until availability threshold is met.

    Polls partition resources at configured intervals and notifies callbacks
    when resources become available or exhausted.
    """

    def __init__(
        self,
        min_gpus: int,
        partition: str | None = None,
        config: MonitorConfig | None = None,
        callbacks: list[Callback] | None = None,
    ) -> None:
        """Initialize resource monitor.

        Args:
            min_gpus: Minimum number of GPUs required for threshold.
            partition: SLURM partition to monitor. Defaults to all partitions if None.
            config: Monitoring configuration. Defaults to MonitorConfig() if None.
            callbacks: List of notification callbacks. Defaults to empty list if None.

        Raises:
            ValueError: If min_gpus < 0.
        """
        super().__init__(config=config, callbacks=callbacks)

        if min_gpus < 0:
            raise ValueError("min_gpus must be >= 0")

        self.min_gpus = min_gpus
        self.partition = partition
        self._was_available: bool | None = None  # None = uninitialized

        logger.debug(
            f"ResourceMonitor initialized for min_gpus={min_gpus}, "
            f"partition={partition or 'all'}"
        )

    def check_condition(self) -> bool:
        """Check if resource availability threshold is met.

        Returns:
            True if available GPUs >= min_gpus threshold, False otherwise.

        Raises:
            SlurmError: If SLURM command fails.
        """
        snapshot = self.get_partition_resources()
        return snapshot.meets_threshold(self.min_gpus)

    def get_current_state(self) -> dict[str, Any]:
        """Get current resource state for comparison and logging.

        Returns:
            Dictionary with current resource state.
            Format: {
                "partition": str | None,
                "gpus_available": int,
                "gpus_total": int,
                "meets_threshold": bool
            }

        Raises:
            SlurmError: If SLURM command fails.
        """
        snapshot = self.get_partition_resources()
        return {
            "partition": snapshot.partition,
            "gpus_available": snapshot.gpus_available,
            "gpus_total": snapshot.total_gpus,
            "meets_threshold": snapshot.meets_threshold(self.min_gpus),
        }

    def get_partition_resources(self) -> ResourceSnapshot:
        """Query SLURM for GPU resource availability.

        Uses sinfo to get total GPUs per partition and squeue to get GPUs in use.
        Filters out DOWN/DRAIN/DRAINING nodes from availability calculation.

        Returns:
            ResourceSnapshot with current resource state.

        Raises:
            SlurmError: If SLURM command fails.
        """
        # Get node and GPU statistics
        nodes_total, nodes_idle, nodes_down, total_gpus = self._get_node_stats()

        # Get GPUs in use and running jobs count
        gpus_in_use, jobs_running = self._get_gpu_usage()

        # Calculate available GPUs
        gpus_available = max(0, total_gpus - gpus_in_use)

        return ResourceSnapshot(
            partition=self.partition,
            total_gpus=total_gpus,
            gpus_in_use=gpus_in_use,
            gpus_available=gpus_available,
            jobs_running=jobs_running,
            nodes_total=nodes_total,
            nodes_idle=nodes_idle,
            nodes_down=nodes_down,
        )

    def _get_node_stats(self) -> tuple[int, int, int, int]:
        """Get node and GPU statistics from sinfo.

        Returns:
            Tuple of (nodes_total, nodes_idle, nodes_down, total_gpus).

        Raises:
            SlurmError: If SLURM command fails.
        """
        try:
            # Build sinfo command
            cmd = ["sinfo", "-o", "%n %G %T", "--noheader"]
            if self.partition:
                cmd.extend(["-p", self.partition])

            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=True,
                timeout=30,
            )

            nodes_total = 0
            nodes_idle = 0
            nodes_down = 0
            total_gpus = 0

            for line in result.stdout.strip().split("\n"):
                if not line.strip():
                    continue

                parts = line.split()
                if len(parts) < 3:
                    logger.debug(f"Skipping malformed sinfo line: {line}")
                    continue

                # parts: [nodename, gres, state]
                gres = parts[1]
                state = parts[2].lower()

                logger.debug(f"Node {parts[0]}: gres='{gres}', state='{state}'")

                nodes_total += 1

                # Normalize state for consistent matching
                state_lower = state.lower()

                # Check if node is down or draining (using consistent substring matching)
                is_unavailable = any(
                    keyword in state_lower
                    for keyword in ["down", "drain", "maint", "reserved"]
                )

                # Count node states
                if is_unavailable:
                    nodes_down += 1
                elif "idle" in state_lower:
                    nodes_idle += 1

                # Skip unavailable nodes for GPU count
                if is_unavailable:
                    logger.debug(f"Skipping {parts[0]} (state: {state})")
                    continue

                # Parse GPU count from gres using robust pattern
                # Matches: "gpu:8", "gres/gpu=8", "gpu:NVIDIA-A100:8", etc.
                match = re.search(r"gpu[:/=](?:[^:]+:)?(\d+)", gres, re.IGNORECASE)
                if match:
                    gpu_count = int(match.group(1))
                    logger.debug(f"Found {gpu_count} GPUs on {parts[0]}")
                    total_gpus += gpu_count
                elif "gpu" in gres.lower():
                    logger.warning(
                        f"Failed to parse GPU count from gres: '{gres}' on {parts[0]}"
                    )
                else:
                    logger.debug(f"No GPU on {parts[0]}")

            logger.info(
                f"Node stats: {nodes_total} total, {nodes_idle} idle, "
                f"{nodes_down} down, {total_gpus} total GPUs"
            )
            return nodes_total, nodes_idle, nodes_down, total_gpus

        except subprocess.TimeoutExpired:
            logger.warning("Timeout querying node stats with sinfo")
            return 0, 0, 0, 0
        except subprocess.CalledProcessError as e:
            logger.warning(f"Failed to query node stats with sinfo: {e}")
            return 0, 0, 0, 0

    def _get_gpu_usage(self) -> tuple[int, int]:
        """Get GPU usage and running jobs count from squeue.

        Returns:
            Tuple of (gpus_in_use, jobs_running).

        Raises:
            SlurmError: If SLURM command fails.
        """
        try:
            # Build squeue command
            cmd = ["squeue", "-o", "%i %T %b", "--noheader"]
            if self.partition:
                cmd.extend(["-p", self.partition])

            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=True,
                timeout=30,
            )

            gpus_in_use = 0
            jobs_running = 0

            for line in result.stdout.strip().split("\n"):
                if not line.strip():
                    continue

                parts = line.split()
                if len(parts) < 3:
                    logger.debug(f"Skipping malformed squeue line: {line}")
                    continue

                # parts: [job_id, state, tres]
                job_id = parts[0]
                state = parts[1]
                tres = parts[2]

                # Only count RUNNING jobs
                if state != "RUNNING":
                    continue

                jobs_running += 1

                # Parse GPU count from tres using robust pattern
                # Matches: "gpu:8", "gres:gpu=8", "gres:gpu:NVIDIA-A100:8", etc.
                match = re.search(r"gpu[:/=](?:[^:]+:)?(\d+)", tres, re.IGNORECASE)
                if match:
                    gpu_count = int(match.group(1))
                    logger.debug(
                        f"Job {job_id} using {gpu_count} GPUs (tres: '{tres}')"
                    )
                    gpus_in_use += gpu_count
                elif "gpu" in tres.lower():
                    logger.warning(
                        f"Failed to parse GPU count from tres: '{tres}' for job {job_id}"
                    )

            logger.info(f"GPU usage: {gpus_in_use} GPUs in use by {jobs_running} jobs")
            return gpus_in_use, jobs_running

        except subprocess.TimeoutExpired:
            logger.warning("Timeout querying GPU usage with squeue")
            return 0, 0
        except subprocess.CalledProcessError as e:
            logger.warning(f"Failed to query GPU usage with squeue: {e}")
            return 0, 0

    def _notify_callbacks(self, event: str) -> None:
        """Notify callbacks of resource availability changes.

        Detects transitions between available and exhausted states to prevent
        duplicate notifications.

        Args:
            event: Event name (unused, state changes detected internally).

        Raises:
            SlurmError: If SLURM command fails.
        """
        snapshot = self.get_partition_resources()
        is_available = snapshot.meets_threshold(self.min_gpus)

        # Initialize state on first check
        # Set to opposite of current state so first call detects transition and notifies
        # This handles both direct calls and calls from watch_continuous after BaseMonitor
        # detected a state change
        if self._was_available is None:
            self._was_available = not is_available
            logger.debug(
                f"Initializing availability tracking (current: {is_available})"
            )

        # Notify only on state transitions
        if is_available != self._was_available:
            for callback in self.callbacks:
                try:
                    if is_available:
                        callback.on_resources_available(snapshot)
                    else:
                        callback.on_resources_exhausted(snapshot)
                except Exception as e:
                    logger.error(f"Callback error for resource event: {e}")

            self._was_available = is_available
