"""Mount-aware rsync helpers, used by both CLI and Web sync paths.

Originally these lived in :mod:`srunx.web.sync_utils` because the Web
router was the only caller. Phase 1 of auto-sync (PR #134) made the
CLI ``srunx sbatch`` path depend on the same helpers, which left
``srunx.sync.service`` reaching into ``srunx.web``. That dependency
direction is wrong: ``srunx.sync`` is shared infrastructure, ``srunx.web``
is one of its consumers. Moved here so the layering reads correctly,
and the old ``srunx.web.sync_utils`` re-exports for backward
compatibility.

Two functions live here:

* :func:`build_rsync_client` — translates a :class:`ServerProfile` into
  an :class:`RsyncClient`, honouring ``ssh_host``-based ``~/.ssh/config``
  delegation when present.
* :func:`sync_mount_by_name` — runs ``rsync push`` for the named mount,
  with a configurable ``delete`` flag (default ``False`` for safety:
  Phase 1 auto-sync ran with ``delete=True`` and silently ate
  remote-side outputs/checkpoints inside mounts).

The ``delete`` default change is the user-visible behavioural fix for
Codex's blocker #4 on PR #134. Callers that explicitly want the old
mirror-style behaviour pass ``delete=True`` (e.g. the manual
``srunx ssh sync`` command).
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from srunx.ssh.core.config import ServerProfile

from srunx.sync.rsync import RsyncClient


def build_rsync_client(profile: ServerProfile) -> RsyncClient:
    """Create RsyncClient from SSH profile, handling ssh_host vs hostname.

    When *ssh_host* is set, the client delegates all connection
    parameters (user, key, proxy, port) to ``~/.ssh/config``.
    Otherwise the explicit profile fields are used directly.
    """
    if profile.ssh_host:
        return RsyncClient(
            hostname=profile.ssh_host,
            username="",
            ssh_config_path=str(Path.home() / ".ssh" / "config"),
        )
    return RsyncClient(
        hostname=profile.hostname,
        username=profile.username,
        key_filename=profile.key_filename,
        port=profile.port,
        proxy_jump=profile.proxy_jump,
    )


def sync_mount_by_name(
    profile: ServerProfile,
    mount_name: str,
    *,
    delete: bool = False,
    dry_run: bool = False,
    verbose: bool = False,
) -> str:
    """Sync a named mount's local directory to remote via rsync.

    ``delete`` is **False by default** so callers that don't opt in
    can't accidentally wipe remote-only outputs. The manual
    ``srunx ssh sync`` command and any explicit ""mirror this exactly""
    callers should pass ``delete=True``. Auto-sync paths (PR #134
    Phase 1) leave the default.

    ``dry_run=True`` runs rsync with ``-n -i`` (no transfer + itemize)
    and returns rsync's stdout — the human-readable list of files
    that *would* be touched. The remote is not modified. Used by the
    CLI ``srunx sbatch --dry-run`` preview path (#137 part 2).

    ``verbose=True`` switches the underlying rsync invocation to
    streaming mode so per-file progress reaches the user's terminal
    live (#137 part 3). Mutually compatible with ``dry_run`` — the
    preview output streams the same way.

    Returns:
        rsync stdout — empty for a non-dry-run sync, the itemize lines
        for a dry-run preview.

    Raises:
        ValueError: If *mount_name* does not exist in the profile.
        RuntimeError: If the rsync process exits with a non-zero code.
            The error message includes rsync's stderr so the CLI / API
            layer can surface the underlying cause unchanged.
    """
    mount = next((m for m in profile.mounts if m.name == mount_name), None)
    if mount is None:
        raise ValueError(f"Mount '{mount_name}' not found in profile")
    rsync = build_rsync_client(profile)
    result = rsync.push(
        mount.local,
        mount.remote,
        delete=delete,
        dry_run=dry_run,
        # ``itemize`` tracks ``dry_run`` — for an actual push we don't
        # want the per-file output spam in the success path, but for
        # a preview the itemize lines ARE the value.
        itemize=dry_run,
        verbose=verbose,
        exclude_patterns=mount.exclude_patterns or None,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"rsync sync failed for mount '{mount_name}': {result.stderr}"
        )
    return result.stdout
