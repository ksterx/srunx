"""High-level workspace sync orchestration for CLI submissions.

Ties together the lower-level pieces that each handle one concern:

* :func:`srunx.sync.mount_helpers.sync_mount_by_name` wraps rsync.
* :func:`srunx.sync.lock.acquire_sync_lock` serialises concurrent
  syncs on the same mount.
* :func:`is_dirty_git_worktree` surfaces uncommitted changes before we
  push them.

The CLI consumes this module via :func:`mount_sync_session`, which is
a context manager wrapping the full lock + sync lifetime. Holding the
lock until ``sbatch`` returns prevents a window where another process
could rsync the mount between our sync and our submission, which would
let the cluster execute bytes the user did not approve. This was Codex
blocker #3 on PR #134.
"""

from __future__ import annotations

import contextlib
import subprocess
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

from srunx.config import SyncDefaults
from srunx.logging import get_logger
from srunx.ssh.core.config import MountConfig, ServerProfile
from srunx.sync.lock import acquire_sync_lock
from srunx.sync.mount_helpers import sync_mount_by_name

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
    treat "unknown" as "clean" rather than blocking submission.

    Skips the ``git status`` call entirely when there is no ``.git``
    directory or file (worktree marker) under *path* — keeps the
    common ""mount is not a git repo"" case free of subprocess
    overhead. Codex follow-up on #134.
    """
    if not (path / ".git").exists():
        return False, ""

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


@contextlib.contextmanager
def mount_sync_session(
    *,
    profile_name: str,
    profile: ServerProfile,
    mount: MountConfig,
    config: SyncDefaults,
    sync_required: bool,
) -> Iterator[SyncOutcome]:
    """Acquire the per-mount lock, optionally rsync, hold lock until exit.

    The lock is held for the entire ``with`` block so callers can
    submit ``sbatch`` against the just-synced bytes without racing a
    second sync from another process. Closes Codex blocker #3 on
    PR #134 (""sync lock released before remote validation and
    sbatch"").

    When ``sync_required`` is ``False`` (e.g. ``--no-sync``) we still
    take the lock — that lets concurrent ``--no-sync`` callers safely
    observe a stable mount root for the brief sbatch handoff — but
    we skip the rsync invocation itself and return a ``performed=False``
    outcome.

    Failure modes:

    * Dirty worktree + ``require_clean=true`` → :class:`SyncAbortedError`
      (rsync does **not** run; lock is released before raising).
    * Dirty worktree + ``warn_dirty=true`` → log warning, sync proceeds.
    * Lock contention → :class:`~srunx.sync.lock.SyncLockTimeoutError`
      bubbles up.
    * rsync non-zero exit → :class:`RuntimeError` (from inner helper).
    """
    warnings: list[str] = []
    if sync_required:
        local_root = Path(mount.local).expanduser()
        if config.warn_dirty or config.require_clean:
            dirty, summary = is_dirty_git_worktree(local_root)
            if dirty:
                msg = f"Mount '{mount.name}' has uncommitted changes: {summary}"
                if config.require_clean:
                    raise SyncAbortedError(
                        f"{msg}. Commit or stash before syncing, or "
                        f"disable ``sync.require_clean``."
                    )
                if config.warn_dirty:
                    logger.warning(msg)
                    warnings.append(msg)

    with acquire_sync_lock(
        profile_name, mount.name, timeout=config.lock_timeout_seconds
    ):
        if sync_required:
            logger.info("Syncing mount '%s' before submission", mount.name)
            # delete=False (Codex blocker #4): auto-sync must not wipe
            # remote-only outputs (training checkpoints, run logs).
            # ``srunx ssh sync`` keeps the historical mirror behaviour.
            sync_mount_by_name(profile, mount.name, delete=False)

        yield SyncOutcome(
            mount_name=mount.name,
            performed=sync_required,
            warnings=tuple(warnings),
        )


def ensure_mount_synced(
    *,
    profile_name: str,
    profile: ServerProfile,
    mount: MountConfig,
    config: SyncDefaults,
) -> SyncOutcome:
    """Backwards-compatible entry point that performs a one-shot sync.

    Prefer :func:`mount_sync_session` for new code that needs to hold
    the lock across sync + submission. This wrapper exists so the
    one-call ""just sync this mount"" surface stays available for
    contexts (workflow precheck, ad-hoc CLI hooks) that don't need the
    held-lock semantics.
    """
    with mount_sync_session(
        profile_name=profile_name,
        profile=profile,
        mount=mount,
        config=config,
        sync_required=True,
    ) as outcome:
        return outcome
