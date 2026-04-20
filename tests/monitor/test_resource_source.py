"""Tests for :mod:`srunx.monitor.resource_source`.

Covers the SSH-adapter-backed resource source that lets
``ResourceMonitor`` talk to a remote cluster instead of requiring local
``sinfo`` / ``squeue``. Guards:

- Single-partition query returns the exact adapter row, coerced to a
  ``ResourceSnapshot``.
- Cluster-wide query (``partition=None``) sums the per-partition dicts
  the adapter produces, so the resulting snapshot matches what the
  local subprocess path would produce on the same cluster.
- Empty adapter return values don't blow up — we produce a zero
  snapshot instead of an IndexError.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

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


class TestSSHAdapterResourceSource:
    def test_satisfies_protocol(self) -> None:
        """``SSHAdapterResourceSource`` is a structural match for ``ResourceSource``."""
        adapter = MagicMock()
        source = SSHAdapterResourceSource(adapter)
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

        snap = SSHAdapterResourceSource(adapter).get_snapshot("gpu")

        adapter.get_resources.assert_called_once_with("gpu")
        assert snap.partition == "gpu"
        assert snap.total_gpus == 16
        assert snap.gpus_in_use == 10
        assert snap.gpus_available == 6
        assert snap.jobs_running == 3
        assert snap.nodes_total == 4
        assert snap.nodes_idle == 1
        assert snap.nodes_down == 0

    def test_cluster_wide_sums_all_partitions(self) -> None:
        """``partition=None`` aggregates every partition dict the adapter returns.

        Matches the local-subprocess semantic: ``sinfo`` without ``-p``
        returns cluster-wide totals. Per-partition dicts from the
        adapter must be summed so the downstream snapshotter produces
        the same numbers regardless of transport.
        """
        adapter = MagicMock()
        adapter.get_resources.return_value = [
            _row(
                partition="gpu",
                total_gpus=16,
                gpus_in_use=10,
                gpus_available=6,
                nodes_total=4,
                nodes_idle=1,
            ),
            _row(
                partition="cpu",
                total_gpus=0,
                nodes_total=8,
                nodes_idle=4,
            ),
            _row(
                partition="debug",
                total_gpus=2,
                gpus_available=2,
                nodes_total=1,
                nodes_idle=1,
            ),
        ]

        snap = SSHAdapterResourceSource(adapter).get_snapshot(None)

        adapter.get_resources.assert_called_once_with(None)
        assert snap.partition is None
        assert snap.total_gpus == 18  # 16 + 0 + 2
        assert snap.gpus_in_use == 10
        assert snap.gpus_available == 8  # 6 + 0 + 2
        assert snap.nodes_total == 13  # 4 + 8 + 1
        assert snap.nodes_idle == 6  # 1 + 4 + 1

    def test_empty_adapter_response_yields_zero_snapshot(self) -> None:
        """No partitions → zero snapshot, not IndexError."""
        adapter = MagicMock()
        adapter.get_resources.return_value = []

        snap = SSHAdapterResourceSource(adapter).get_snapshot(None)

        assert snap.partition is None
        assert snap.total_gpus == 0
        assert snap.gpus_available == 0
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

        snap = SSHAdapterResourceSource(adapter).get_snapshot("gpu")

        assert snap.total_gpus == 0
        assert snap.gpus_in_use == 0
        assert snap.nodes_total == 2
