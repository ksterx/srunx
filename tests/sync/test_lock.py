"""Tests for the per-mount sync lock.

The lock is the only thing standing between two concurrent
``srunx sbatch --sync`` invocations and a half-transferred mount.
We need to verify three behaviours:

* Single-process acquire/release works.
* A second acquirer respects the timeout and raises
  :class:`SyncLockTimeoutError` (with the lock path embedded so the
  user can locate / inspect it).
* Names with shell metacharacters / path separators are sanitised
  before they hit the filesystem.
"""

from __future__ import annotations

import fcntl
import multiprocessing
import os
import time
from pathlib import Path

import pytest

from srunx.sync.lock import (
    SyncLockTimeoutError,
    _sanitise,
    acquire_sync_lock,
    lock_path_for,
)


@pytest.fixture(autouse=True)
def isolated_config_home(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Force ``$XDG_CONFIG_HOME`` into tmp so locks land in a sandbox."""
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path))


def test_acquire_release_basic() -> None:
    """A clean acquire creates the lock file and releases on exit."""
    with acquire_sync_lock("alice", "ml", timeout=1.0) as path:
        assert path.exists()
        assert path.parent.name == "locks"

    # After the with-block, a second acquirer should grab it
    # immediately — no leftover OS-level lock.
    with acquire_sync_lock("alice", "ml", timeout=0.5) as path2:
        assert path2 == path


def test_lock_path_sanitises_special_chars() -> None:
    """Profile / mount names with slashes or shell chars are flattened.

    Otherwise, ``profile="../../etc/passwd"`` would let a malicious
    name escape the locks directory or collide with a sibling lock.
    """
    p = lock_path_for("../etc/passwd", "&|;")
    assert ".." not in p.name
    assert "/" not in p.name
    assert ";" not in p.name
    # Sanitised parts still produce a non-empty filename.
    assert p.name.endswith(".lock")


def test_lock_path_disambiguates_dash_collision() -> None:
    """``foo-bar / baz`` and ``foo / bar-baz`` get distinct lock files.

    Pre-#137 the filename was ``f"{sanitise(p)}-{sanitise(m)}.lock"``
    which collapsed both pairs into ``foo-bar-baz.lock`` — distinct
    mounts queued on the same lock for no semantic reason. The hash
    suffix added in :func:`_disambiguator` restores per-pair
    uniqueness.
    """
    a = lock_path_for("foo-bar", "baz")
    b = lock_path_for("foo", "bar-baz")
    assert a != b
    # Both still live in the locks dir and end in .lock — readability
    # of the diagnostic prefix is preserved.
    assert a.parent == b.parent
    assert a.name.endswith(".lock") and b.name.endswith(".lock")


def test_lock_path_is_deterministic() -> None:
    """Same inputs → same path. The hash suffix is content-derived."""
    assert lock_path_for("alice", "ml") == lock_path_for("alice", "ml")


def test_lock_path_separates_unrelated_pairs_after_sanitise() -> None:
    """Names that sanitise to the same string still get distinct locks.

    ``"a/b"`` and ``"a_b"`` both flatten to ``a_b`` under sanitisation;
    without the hash they'd map to the same lock. The disambiguator
    runs against the *raw* names so each unique input gets its own
    lock file.
    """
    a = lock_path_for("a/b", "ml")
    b = lock_path_for("a_b", "ml")
    assert a != b


def test_sanitise_empty_value_falls_back() -> None:
    assert _sanitise("") == "unnamed"
    assert _sanitise("___") == "unnamed"  # Strip-able chars only.


def test_zero_timeout_rejected() -> None:
    with pytest.raises(ValueError, match="positive"):
        with acquire_sync_lock("alice", "ml", timeout=0):
            pass


def _hold_lock(profile: str, mount: str, hold_for: float) -> None:
    """Worker process: hold the lock for *hold_for* seconds."""
    with acquire_sync_lock(profile, mount, timeout=5.0):
        time.sleep(hold_for)


def test_contended_acquire_times_out(tmp_path: Path) -> None:
    """A second acquirer waits up to ``timeout`` then raises with the path.

    Hold the same lock file through a separate descriptor in this
    process so the contention is established before the assertion begins.
    This preserves the contract without relying on subprocess startup
    timing or a sleep-based handoff.
    """
    path = lock_path_for("alice", "ml")
    path.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(str(path), os.O_RDWR | os.O_CREAT, 0o644)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)

        start = time.monotonic()
        with pytest.raises(SyncLockTimeoutError) as exc_info:
            with acquire_sync_lock(
                "alice", "ml", timeout=0.5, poll_interval=0.05
            ):
                pytest.fail("should not have acquired contended lock")
        elapsed = time.monotonic() - start

        # We honoured the timeout (within a generous slack for CI
        # scheduling jitter).
        assert 0.4 < elapsed < 1.5

        err = exc_info.value
        assert err.timeout == pytest.approx(0.5)
        assert err.lock_path == path
    finally:
        try:
            fcntl.flock(fd, fcntl.LOCK_UN)
        finally:
            os.close(fd)


def test_acquire_after_holder_exits(tmp_path: Path) -> None:
    """Once a holder exits, a fresh acquirer succeeds promptly."""
    ctx = multiprocessing.get_context("spawn")
    holder = ctx.Process(target=_hold_lock, args=("alice", "ml", 0.4))
    holder.start()
    try:
        time.sleep(0.1)

        # Block long enough that the holder definitely exits during
        # our wait. The acquire should succeed without raising.
        with acquire_sync_lock("alice", "ml", timeout=2.0) as path:
            assert path.exists()
    finally:
        holder.join(timeout=5)
        if holder.is_alive():
            holder.terminate()
            holder.join()
