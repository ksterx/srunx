from srunx.snapshot import (
    SnapshotError,
    SnapshotLockTimeoutError,
    SnapshotRemoteError,
    SnapshotValidationError,
)


def test_error_hierarchy():
    assert issubclass(SnapshotError, RuntimeError)
    assert issubclass(SnapshotValidationError, SnapshotError)
    assert issubclass(SnapshotRemoteError, SnapshotError)
    assert issubclass(SnapshotLockTimeoutError, SnapshotRemoteError)
