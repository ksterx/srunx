"""Exceptions raised by the snapshot subsystem."""


class SnapshotError(RuntimeError):
    """Base class for all snapshot failures."""


class SnapshotValidationError(SnapshotError):
    """Local validation failed (bad repo state, option, or path). CLI exit 2."""


class SnapshotRemoteError(SnapshotError):
    """A remote step (probe, push, commit, bind, lease) failed. CLI exit 1."""


class SnapshotLockTimeoutError(SnapshotRemoteError):
    """The per-repo remote lock could not be acquired within the wait window."""
