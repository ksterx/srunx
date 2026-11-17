"""Exceptions raised by the snapshot subsystem."""


class SnapshotError(RuntimeError):
    """Base class for all snapshot failures."""


class SnapshotValidationError(SnapshotError):
    """Local validation failed (bad repo state, option, or path). CLI exit 2."""


class SnapshotRemoteError(SnapshotError):
    """A remote step (probe, push, commit, bind, lease) failed. CLI exit 1."""


class SnapshotLockTimeoutError(SnapshotRemoteError):
    """The per-repo remote lock could not be acquired within the wait window."""


class SnapshotRemoteTimeoutError(SnapshotRemoteError):
    """A remote script did not finish in time; ``partial_output`` is what it printed."""

    def __init__(self, message: str, partial_output: str = "") -> None:
        super().__init__(message)
        self.partial_output = partial_output
