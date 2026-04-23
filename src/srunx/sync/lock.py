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
import hashlib
import os
import re
import time
from collections.abc import Iterator
from pathlib import Path

from srunx.common.config import _user_config_dir
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
        # Note: ``--no-sync`` does NOT bypass this lock — we still take
        # it briefly to make the sbatch handoff race-free against a
        # concurrent rsync. Tell the user the actual recovery options:
        # wait for the holder, raise the timeout via config, or remove
        # the lock file if a previous run crashed without releasing it.
        super().__init__(
            f"Could not acquire sync lock {lock_path} within {timeout:.1f}s. "
            f"Another srunx process is syncing the same mount; wait for it "
            f"to finish, raise sync.lock_timeout_seconds in config, or "
            f"delete the lock file if it's stale."
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


def _disambiguator(profile: str, mount: str) -> str:
    """Short hash that disambiguates colliding ``(profile, mount)`` pairs.

    The pre-#137 filename ``f"{sanitise(profile)}-{sanitise(mount)}.lock"``
    collapsed pairs whose split fell at a ``-``: ``("foo-bar", "baz")`` and
    ``("foo", "bar-baz")`` both produced ``foo-bar-baz.lock``. The
    consequence was *over-serialisation* (two unrelated mounts queueing
    on one lock) — safe but wrong. Two unrelated workflows on
    overlapping name shapes would block each other for no reason.

    Mixing in a short hash of the raw ``(profile, mount)`` pair (a
    ``\\x00`` separator avoids the same collapse the readable name
    suffers) restores per-pair uniqueness while keeping the filename
    diagnosable. 8 hex chars ≈ 32 bits — collision probability is
    negligible for the handful of mounts a single user will ever
    register.
    """
    payload = f"{profile}\x00{mount}".encode()
    return hashlib.sha256(payload).hexdigest()[:8]


def lock_path_for(profile: str, mount: str) -> Path:
    """Return the lock file path for a (profile, mount) pair.

    Filename format: ``{sanitised_profile}-{sanitised_mount}-{hash8}.lock``.
    The ``hash8`` suffix disambiguates pairs whose readable parts
    collide after sanitisation (``foo-bar / baz`` vs ``foo / bar-baz``).
    See :func:`_disambiguator`.
    """
    return (
        _user_config_dir()
        / "locks"
        / (
            f"{_sanitise(profile)}-{_sanitise(mount)}"
            f"-{_disambiguator(profile, mount)}.lock"
        )
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
