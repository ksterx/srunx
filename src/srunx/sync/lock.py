"""Cross-process advisory lock for per-mount sync serialization.

Two concurrent ``srunx sbatch --sync`` invocations targeting the same
mount must not race: one would see a half-transferred tree the other is
writing to. A file-based ``fcntl.flock`` provides cheap serialization
across processes on the same machine, which is the only scope we care
about (rsync writes from a single workstation).

The lock file lives under ``$XDG_CONFIG_HOME/srunx/locks/`` with a
sanitised name encoding ``profile`` + ``mount``. It is created on first
use and left in place afterwards — acquisition is cooperative via
``flock`` so leaving an empty file around is harmless and avoids
cleanup races.
"""

from __future__ import annotations

import contextlib
import errno
import fcntl
import os
import re
import time
from collections.abc import Iterator
from pathlib import Path

from srunx.config import _user_config_dir
from srunx.logging import get_logger

logger = get_logger(__name__)


class SyncLockTimeoutError(RuntimeError):
    """Raised when the sync lock could not be acquired within the timeout.

    Carries the lock path so callers can surface it to the user; the
    path itself is the easiest recovery hint ("another process is
    syncing <mount>; either wait, or inspect/delete the lock file if
    it's stale").
    """

    def __init__(self, lock_path: Path, timeout: float) -> None:
        super().__init__(
            f"Could not acquire sync lock {lock_path} within {timeout:.1f}s. "
            f"Another srunx process is syncing the same mount; wait for it "
            f"to finish, or override with --no-sync."
        )
        self.lock_path = lock_path
        self.timeout = timeout


_SANITISE_RE = re.compile(r"[^A-Za-z0-9._-]+")


def _sanitise(value: str) -> str:
    """Return *value* with unsafe filesystem characters replaced.

    Keeps the lock filename readable (``profile-mount.lock``) while
    defending against path-traversal or shell metacharacters leaking
    from user-provided names.
    """
    cleaned = _SANITISE_RE.sub("_", value).strip("._-")
    return cleaned or "unnamed"


def lock_path_for(profile: str, mount: str) -> Path:
    """Return the lock file path for a (profile, mount) pair."""
    return (
        _user_config_dir() / "locks" / f"{_sanitise(profile)}-{_sanitise(mount)}.lock"
    )


@contextlib.contextmanager
def acquire_sync_lock(
    profile: str,
    mount: str,
    timeout: float,
    *,
    poll_interval: float = 0.25,
) -> Iterator[Path]:
    """Block until the per-(profile,mount) lock is held, then yield.

    Implementation: open a lock file and call ``fcntl.flock(LOCK_EX |
    LOCK_NB)`` in a polling loop until success or ``timeout`` elapses.
    We prefer polling over blocking ``flock`` because it lets us honour
    a deadline; the process will spend ``timeout`` mostly idle anyway.

    The returned path is the actual lock file, useful for diagnostics
    and tests.
    """
    if timeout <= 0:
        raise ValueError("timeout must be positive")

    path = lock_path_for(profile, mount)
    path.parent.mkdir(parents=True, exist_ok=True)

    # ``open(..., "a+")`` creates on demand and keeps the fd usable for
    # flock. We close it explicitly on exit to release the lock — even
    # on POSIX where GC would close it, explicit close keeps the
    # semantics obvious.
    fd = os.open(str(path), os.O_RDWR | os.O_CREAT, 0o644)

    deadline = time.monotonic() + timeout
    try:
        while True:
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except OSError as exc:
                if exc.errno not in (errno.EAGAIN, errno.EWOULDBLOCK):
                    raise
                if time.monotonic() >= deadline:
                    os.close(fd)
                    raise SyncLockTimeoutError(path, timeout) from None
                time.sleep(poll_interval)

        try:
            yield path
        finally:
            try:
                fcntl.flock(fd, fcntl.LOCK_UN)
            except OSError:
                logger.debug("flock(LOCK_UN) failed; fd may already be closed")
    finally:
        try:
            os.close(fd)
        except OSError:
            pass
