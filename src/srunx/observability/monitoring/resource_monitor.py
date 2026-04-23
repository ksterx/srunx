"""Resource monitoring implementation for SLURM."""

import subprocess
from typing import Any

from loguru import logger

from srunx.callbacks import Callback
from srunx.observability.monitoring.base import BaseMonitor
from srunx.observability.monitoring.resource_source import ResourceSource
from srunx.observability.monitoring.types import MonitorConfig, ResourceSnapshot
from srunx.slurm.parsing import GPU_TRES_RE


class ResourceMonitor(BaseMonitor):
    """Monitor SLURM GPU resources until availability threshold is met.

    Polls partition resources at configured intervals and notifies callbacks
    when resources become available or exhausted.

    When a :class:`~srunx.observability.monitoring.resource_source.ResourceSource` is
    injected, partition queries delegate to it instead of shelling out
    to local ``sinfo`` / ``squeue``. That lets ``srunx ui`` talk to a
    remote cluster through the existing SSH adapter — the previous
    subprocess-only path only worked on a SLURM head/login node.
    """

    def __init__(
        self,
        min_gpus: int,
        partition: str | None = None,
        config: MonitorConfig | None = None,
        callbacks: list[Callback] | None = None,
        source: ResourceSource | None = None,
    ) -> None:
        """Initialize resource monitor.

        Args:
            min_gpus: Minimum number of GPUs required for threshold.
            partition: SLURM partition to monitor. Defaults to all partitions if None.
            config: Monitoring configuration. Defaults to MonitorConfig() if None.
            callbacks: List of notification callbacks. Defaults to empty list if None.
            source: Optional pluggable backend. When provided, partition
                queries delegate to ``source.get_snapshot(partition)``
                instead of the local-subprocess fallback. Required when
                ``sinfo`` / ``squeue`` aren't on the caller's PATH
                (e.g. a developer laptop driving a remote cluster).

        Raises:
            ValueError: If min_gpus < 0.
        """
        super().__init__(config=config, callbacks=callbacks)

        if min_gpus < 0:
            raise ValueError("min_gpus must be >= 0")

        self.min_gpus = min_gpus
        self.partition = partition
        self._source = source
        self._was_available: bool | None = None  # None = uninitialized
        self._cached_snapshot: ResourceSnapshot | None = None

        logger.debug(
            f"ResourceMonitor initialized for min_gpus={min_gpus}, "
            f"partition={partition or 'all'}, source={source.__class__.__name__ if source else 'subprocess'}"
        )

    def _get_snapshot(self) -> ResourceSnapshot:
        """Get partition resources, using per-cycle cache.

        First call per cycle fetches from SLURM; subsequent calls
        within the same cycle return the cached result.
        """
        if self._cached_snapshot is not None:
            return self._cached_snapshot
        self._cached_snapshot = self.get_partition_resources()
        return self._cached_snapshot

    def check_condition(self) -> bool:
        """Check if resource availability threshold is met.

        Invalidates the per-cycle cache so fresh data is fetched.
        """
        self._cached_snapshot = None
        return self._get_snapshot().meets_threshold(self.min_gpus)

    def get_current_state(self) -> dict[str, Any]:
        """Get current resource state for comparison and logging.

        Invalidates the per-cycle cache so fresh data is fetched.
        """
        self._cached_snapshot = None
        snapshot = self._get_snapshot()
        return {
            "partition": snapshot.partition,
            "gpus_available": snapshot.gpus_available,
            "gpus_total": snapshot.total_gpus,
            "meets_threshold": snapshot.meets_threshold(self.min_gpus),
        }

    def get_current_snapshot(self) -> ResourceSnapshot:
        """Alias of :meth:`get_partition_resources` used by ``ResourceSnapshotter``.

        Provides a stable, intent-revealing name that the poller stack can
        depend on regardless of how the partition query happens to be named
        internally.
        """
        return self.get_partition_resources()

    def get_partition_resources(self) -> ResourceSnapshot:
        """Query SLURM for GPU resource availability.

        Delegates to the injected :class:`ResourceSource` when one was
        provided (e.g. the SSH adapter for remote clusters). Otherwise
        falls back to the local ``sinfo`` / ``squeue`` subprocess path —
        unchanged behaviour for callers running on a SLURM head node.
        Filters out DOWN/DRAIN/DRAINING nodes from availability
        calculation.

        Returns:
            ResourceSnapshot with current resource state.

        Raises:
            SlurmError: If SLURM command fails.
        """
        if self._source is not None:
            return self._source.get_snapshot(self.partition)

        # Local-subprocess fallback: original implementation.
        nodes_total, nodes_idle, nodes_down, total_gpus = self._get_node_stats()
        gpus_in_use, jobs_running = self._get_gpu_usage()
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

                # Parse GPU count from gres using shared pattern
                match = GPU_TRES_RE.search(gres)
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

                # Parse GPU count from tres using shared pattern
                match = GPU_TRES_RE.search(tres)
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
        snapshot = self._get_snapshot()
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
