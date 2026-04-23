"""Tests for :mod:`srunx.monitor.resource_source`.

Covers the SSH-adapter-backed resource source. Regression surface:

- Stale-adapter guard: the source resolves the adapter per-call via
  a provider callable, so ``deps.swap_adapter`` at runtime is picked
  up on the next snapshotter cycle.
- Cluster-wide snapshots delegate to ``adapter.get_cluster_snapshot``
  (one cluster-wide sinfo+squeue pair with cross-partition dedup);
  summing ``adapter.get_resources(None)`` would double-count nodes
  that belong to multiple partitions AND could silently persist
  understated totals when per-partition errors are caught inside
  the adapter.
- Single-partition queries bubble up errors — transient failures
  must trigger supervisor backoff, not write zero snapshots.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest
from srunx.monitor.resource_source import (
    ResourceSource,
    SSHAdapterResourceSource,
)


def _row(**overrides: Any) -> dict[str, Any]:
    base = {
        "partition": "gpu",
        "total_gpus": 0,
        "gpus_in_use": 0,
        "gpus_available": 0,
        "jobs_running": 0,
        "nodes_total": 0,
        "nodes_idle": 0,
        "nodes_down": 0,
    }
    base.update(overrides)
    return base


def _snapshot_dict(**overrides: Any) -> dict[str, Any]:
    base = {
        "timestamp": "2026-04-20T00:00:00+00:00",
        "partition": None,
        "total_gpus": 0,
        "gpus_in_use": 0,
        "gpus_available": 0,
        "jobs_running": 0,
        "nodes_total": 0,
        "nodes_idle": 0,
        "nodes_down": 0,
        "gpu_utilization": 0.0,
        "has_available_gpus": False,
    }
    base.update(overrides)
    return base


class TestSSHAdapterResourceSource:
    def test_satisfies_protocol(self) -> None:
        source = SSHAdapterResourceSource(lambda: MagicMock())
        assert isinstance(source, ResourceSource)

    def test_single_partition_returns_that_row(self) -> None:
        adapter = MagicMock()
        adapter.get_resources.return_value = [
            _row(
                partition="gpu",
                total_gpus=16,
                gpus_in_use=10,
                gpus_available=6,
                jobs_running=3,
                nodes_total=4,
                nodes_idle=1,
                nodes_down=0,
            )
        ]

        snap = SSHAdapterResourceSource(lambda: adapter).get_snapshot("gpu")

        adapter.get_resources.assert_called_once_with("gpu")
        adapter.get_cluster_snapshot.assert_not_called()
        assert snap.partition == "gpu"
        assert snap.total_gpus == 16
        assert snap.gpus_in_use == 10
        assert snap.gpus_available == 6
        assert snap.jobs_running == 3
        assert snap.nodes_total == 4
        assert snap.nodes_idle == 1
        assert snap.nodes_down == 0

    def test_cluster_wide_uses_adapter_cluster_snapshot(self) -> None:
        """``partition=None`` delegates to ``adapter.get_cluster_snapshot``.

        Summing ``get_resources(None)`` per-partition dicts would
        double-count shared nodes AND silently swallow per-partition
        errors; the dedicated cluster-wide adapter call does neither.
        """
        adapter = MagicMock()
        adapter.get_cluster_snapshot.return_value = _snapshot_dict(
            total_gpus=18,
            gpus_in_use=10,
            gpus_available=8,
            nodes_total=13,
            nodes_idle=6,
        )

        snap = SSHAdapterResourceSource(lambda: adapter).get_snapshot(None)

        adapter.get_cluster_snapshot.assert_called_once_with()
        adapter.get_resources.assert_not_called()
        assert snap.partition is None
        assert snap.total_gpus == 18
        assert snap.gpus_in_use == 10
        assert snap.gpus_available == 8
        assert snap.nodes_total == 13
        assert snap.nodes_idle == 6

    def test_single_partition_empty_result_yields_zero_snapshot(self) -> None:
        """An empty list for a named partition is a zero snapshot, not IndexError."""
        adapter = MagicMock()
        adapter.get_resources.return_value = []

        snap = SSHAdapterResourceSource(lambda: adapter).get_snapshot("gpu")

        assert snap.partition == "gpu"
        assert snap.total_gpus == 0
        assert snap.nodes_total == 0

    def test_handles_none_field_values(self) -> None:
        """Adapter rows with ``None`` for numeric fields coerce to 0."""
        adapter = MagicMock()
        adapter.get_resources.return_value = [
            {
                "partition": "gpu",
                "total_gpus": None,
                "gpus_in_use": None,
                "gpus_available": None,
                "jobs_running": None,
                "nodes_total": 2,
                "nodes_idle": None,
                "nodes_down": None,
            }
        ]

        snap = SSHAdapterResourceSource(lambda: adapter).get_snapshot("gpu")

        assert snap.total_gpus == 0
        assert snap.gpus_in_use == 0
        assert snap.nodes_total == 2

    def test_single_partition_error_propagates(self) -> None:
        """Per-partition SLURM failure must surface — supervisor backs off.

        Silently returning a zero snapshot here would persist bogus
        rows every cycle until the cluster recovered, with no signal
        in the logs beyond one info line.
        """
        adapter = MagicMock()
        adapter.get_resources.side_effect = RuntimeError("ssh dropped")

        with pytest.raises(RuntimeError, match="ssh dropped"):
            SSHAdapterResourceSource(lambda: adapter).get_snapshot("gpu")

    def test_cluster_wide_error_propagates(self) -> None:
        """Cluster-wide SLURM failure must surface — see above rationale."""
        adapter = MagicMock()
        adapter.get_cluster_snapshot.side_effect = RuntimeError("sinfo timeout")

        with pytest.raises(RuntimeError, match="sinfo timeout"):
            SSHAdapterResourceSource(lambda: adapter).get_snapshot(None)

    def test_no_adapter_raises(self) -> None:
        """Provider returning None — raise, don't persist a zero row."""
        source = SSHAdapterResourceSource(lambda: None)

        with pytest.raises(RuntimeError, match="No SLURM adapter"):
            source.get_snapshot(None)

    def test_provider_is_resolved_per_call(self) -> None:
        """Swapping the adapter (profile switch) is reflected next cycle.

        Before this fix, the source captured the startup adapter
        reference. After ``deps.swap_adapter`` the snapshotter kept
        talking to the now-disconnected old adapter while the Web UI
        routes used the new one — two inconsistent views of the
        cluster persisted in ``resource_snapshots``.
        """
        first = MagicMock()
        first.get_cluster_snapshot.return_value = _snapshot_dict(total_gpus=8)
        second = MagicMock()
        second.get_cluster_snapshot.return_value = _snapshot_dict(total_gpus=16)

        current = {"adapter": first}
        source = SSHAdapterResourceSource(lambda: current["adapter"])

        snap1 = source.get_snapshot(None)
        assert snap1.total_gpus == 8

        # Simulate a profile switch.
        current["adapter"] = second

        snap2 = source.get_snapshot(None)
        assert snap2.total_gpus == 16
        first.get_cluster_snapshot.assert_called_once()
        second.get_cluster_snapshot.assert_called_once()
