"""High-level workspace sync orchestration for CLI submissions.

Ties together the lower-level pieces that each handle one concern:

* :func:`srunx.web.sync_utils.sync_mount_by_name` wraps rsync.
* :func:`srunx.sync.lock.acquire_sync_lock` serialises concurrent
  syncs on the same mount.
* :func:`is_dirty_git_worktree` surfaces uncommitted changes before we
  push them.

This service layer exists so the CLI command handler stays simple —
``ensure_mount_synced`` is the only function it needs to call — and so
Phase 2 (Workflow CLI) / Phase 3 (Web ``/api/jobs`` with
``script_path`` mode) can reuse the exact same orchestration later.

The service intentionally does not own the rsync client itself; that
stays in :mod:`srunx.sync.rsync` / :mod:`srunx.web.sync_utils`.
"""

from __future__ import annotations

import subprocess
from dataclasses import dataclass
from pathlib import Path

from srunx.config import SyncDefaults
from srunx.logging import get_logger
from srunx.ssh.core.config import MountConfig, ServerProfile
from srunx.sync.lock import SyncLockTimeoutError, acquire_sync_lock

logger = get_logger(__name__)


class SyncAbortedError(RuntimeError):
    """Raised when sync is refused — the CLI should abort submission.

    Subclassing :class:`RuntimeError` keeps it distinct from the
    underlying ``subprocess.CalledProcessError`` / rsync failures, so
    the CLI layer can pick an appropriate exit code and message.
    """


@dataclass(frozen=True)
class SyncOutcome:
    """Result of a single sync attempt surfaced to the CLI."""

    mount_name: str
    performed: bool
    """True when rsync actually ran; False when disabled/skipped."""

    warnings: tuple[str, ...] = ()


def is_dirty_git_worktree(path: Path) -> tuple[bool, str]:
    """Return ``(is_dirty, short_summary)`` for the git worktree at *path*.

    Best-effort. Any subprocess failure (``git`` not installed, path
    not inside a repo, timeout) yields ``(False, "")`` so callers can
    treat "unknown" as "clean" rather than blocking submission. The
    ``warn_dirty`` / ``require_clean`` guards only fire on a
    definitive "dirty" answer.

    ``git status --porcelain`` output is truncated to the first few
    lines so we can show it in a one-shot warning.
    """
    try:
        result = subprocess.run(
            ["git", "-C", str(path), "status", "--porcelain"],
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return False, ""

    if result.returncode != 0:
        return False, ""

    lines = [ln for ln in result.stdout.splitlines() if ln.strip()]
    if not lines:
        return False, ""

    summary = ", ".join(ln.strip() for ln in lines[:3])
    if len(lines) > 3:
        summary += f", … (+{len(lines) - 3} more)"
    return True, summary


def ensure_mount_synced(
    *,
    profile_name: str,
    profile: ServerProfile,
    mount: MountConfig,
    config: SyncDefaults,
) -> SyncOutcome:
    """Acquire the lock and rsync *mount* to the remote.

    Called by the CLI on the ``IN_PLACE`` submission path when sync
    is enabled. Handles the four failure/warning modes we care about:

    * Dirty worktree + ``require_clean=true`` → :class:`SyncAbortedError`
    * Dirty worktree + ``warn_dirty=true`` → log warning, still sync
    * Lock contention → :class:`SyncLockTimeoutError` (bubbles up)
    * rsync non-zero exit → :class:`RuntimeError` (from inner helper)

    We import ``sync_mount_by_name`` lazily to avoid pulling the web
    sync utilities into non-web CLI invocations on import.
    """
    from srunx.web.sync_utils import sync_mount_by_name

    warnings: list[str] = []
    local_root = Path(mount.local).expanduser()
    if config.warn_dirty or config.require_clean:
        dirty, summary = is_dirty_git_worktree(local_root)
        if dirty:
            msg = f"Mount '{mount.name}' has uncommitted changes: {summary}"
            if config.require_clean:
                raise SyncAbortedError(
                    f"{msg}. Commit or stash before syncing, or disable "
                    f"``sync.require_clean``."
                )
            if config.warn_dirty:
                logger.warning(msg)
                warnings.append(msg)

    logger.info("Syncing mount '%s' before submission", mount.name)
    try:
        with acquire_sync_lock(
            profile_name, mount.name, timeout=config.lock_timeout_seconds
        ):
            sync_mount_by_name(profile, mount.name)
    except SyncLockTimeoutError:
        raise
    except RuntimeError:
        # Raised by sync_mount_by_name with the rsync stderr embedded.
        # Re-raise as-is; the CLI translates it into a BadParameter
        # with exit code 2.
        raise

    return SyncOutcome(mount_name=mount.name, performed=True, warnings=tuple(warnings))
