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

import multiprocessing
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

    Spawn a child that holds the lock for longer than the parent's
    timeout, then assert the parent gets a timely
    :class:`SyncLockTimeoutError` carrying the exact lock file.
    """
    # multiprocessing on macOS defaults to spawn; ensure children pick
    # up the same XDG_CONFIG_HOME we set in the autouse fixture.
    ctx = multiprocessing.get_context("spawn")
    holder = ctx.Process(target=_hold_lock, args=("alice", "ml", 2.0))
    holder.start()
    try:
        # Give the holder a beat to actually take the lock.
        time.sleep(0.3)

        start = time.monotonic()
        with pytest.raises(SyncLockTimeoutError) as exc_info:
            with acquire_sync_lock("alice", "ml", timeout=0.5):
                pytest.fail("should not have acquired contended lock")
        elapsed = time.monotonic() - start

        # We honoured the timeout (within a generous slack for CI
        # scheduling jitter).
        assert 0.4 < elapsed < 1.5

        err = exc_info.value
        assert err.timeout == pytest.approx(0.5)
        assert err.lock_path == lock_path_for("alice", "ml")
    finally:
        holder.join(timeout=5)
        if holder.is_alive():
            holder.terminate()
            holder.join()


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
