"""Per-commit read-only git snapshots for SSH SLURM submissions."""

from srunx.snapshot.errors import (
    SnapshotError,
    SnapshotLockTimeoutError,
    SnapshotRemoteError,
    SnapshotValidationError,
)
from srunx.snapshot.lease import Lease
from srunx.snapshot.local_git import SnapshotPlan, plan_snapshot
from srunx.snapshot.service import LeaseHandle, PreparedSnapshot, SnapshotService

__all__ = [
    "Lease",
    "LeaseHandle",
    "PreparedSnapshot",
    "SnapshotError",
    "SnapshotLockTimeoutError",
    "SnapshotPlan",
    "SnapshotRemoteError",
    "SnapshotService",
    "SnapshotValidationError",
    "plan_snapshot",
]
