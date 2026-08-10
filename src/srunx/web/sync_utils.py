"""Shared rsync sync utilities for web routers.

The mount-rsync helpers (:func:`build_rsync_client`,
:func:`sync_mount_by_name`) moved to :mod:`srunx.sync.mount_helpers`
in PR #134's Codex-driven cleanup so the layering reads correctly
(``srunx.sync`` is shared infra, ``srunx.web`` is one consumer). They
remain re-exported from here for backward compatibility — every
existing import site keeps working unchanged.

Web-specific helpers — ``get_current_profile`` (resolves the active
SSH profile from web config or :class:`ConfigManager`),
``find_mount`` (404-on-missing lookup), and
``resolve_mounts_for_workflow`` (longest-prefix mount inference for
workflow jobs) — stay here.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from srunx.ssh.core.config import ServerProfile

# Re-exports: keep the historical import paths working.
from srunx.sync.mount_helpers import (
    build_rsync_client,  # noqa: F401  (re-exported)
    sync_mount_by_name,  # noqa: F401  (re-exported)
)


def get_current_profile_name() -> str | None:
    """Resolve the active SSH profile *name*.

    Checks ``SRUNX_SSH_PROFILE`` (via :func:`get_web_config`) first, then
    falls back to :meth:`ConfigManager.get_current_profile_name`.

    Exposed separately from :func:`get_current_profile` because
    :class:`ServerProfile` does not carry its own name, yet the name is the
    key :func:`~srunx.sync.lock.acquire_sync_lock` serialises on — a caller
    that needs the lock needs the name, not just the profile.
    """
    from srunx.ssh.core.config import ConfigManager

    from .config import get_web_config

    name = get_web_config().ssh_profile
    if not name:
        name = ConfigManager().get_current_profile_name()
    return name or None


def get_current_profile() -> ServerProfile | None:
    """Get the current SSH profile from web config or ConfigManager.

    Returns ``None`` if no profile is configured.
    """
    from srunx.ssh.core.config import ConfigManager

    profile_name = get_current_profile_name()
    if not profile_name:
        return None

    return ConfigManager().get_profile(profile_name)


def get_current_profile_with_name() -> tuple[str, ServerProfile] | None:
    """Resolve the active profile **and** its name in one shot.

    A caller that needs the sync lock must not look these up separately: the
    active profile can change between the two lookups, and syncing profile A's
    paths while holding profile B's lock serialises against nothing at all.

    Returns ``None`` when no profile is configured or the resolved name has no
    stored profile.
    """
    from srunx.ssh.core.config import ConfigManager

    name = get_current_profile_name()
    if not name:
        return None
    profile = ConfigManager().get_profile(name)
    if profile is None:
        return None
    return name, profile


def locked_sync_mount(
    profile: ServerProfile,
    mount_name: str,
    *,
    profile_name: str,
    delete: bool = False,
) -> str:
    """Sync a mount while holding its per-(profile, mount) sync lock.

    Web routes that sync a mount on their own — the file browser, template
    submission, job submission — must queue on the same lock as every other
    srunx writer (submission auto-sync, workflow runs, ``srunx ssh sync``, the
    MCP sync tool). Otherwise two syncs interleave and leave a mixed tree, and
    the MCP mirror preflight's "nothing changed between the check and the push"
    guarantee stops holding.

    **Do not call this while already holding the lock.** ``flock`` is per-fd,
    so re-acquiring inside the same process blocks until the timeout expires.
    The workflow submission path already holds locks for every mount it
    touches and calls :func:`sync_mount_by_name` directly for that reason.

    Args:
        profile: The profile whose mount is being synced.
        mount_name: Mount to sync, as named on *profile*.
        profile_name: The name *profile* was resolved under — required, and
            passed in rather than looked up here on purpose. Re-resolving the
            active profile inside this call could return a different one than
            the caller fetched, which would sync one profile's paths while
            holding another's lock. Use
            :func:`get_current_profile_with_name` to obtain the pair
            atomically.
        delete: Mirror instead of syncing additively.

    Raises:
        ValueError: If *mount_name* is not defined on the profile.
        RuntimeError: On rsync failure, or
            :class:`~srunx.sync.lock.SyncLockTimeoutError` (a RuntimeError
            subclass) when another sync holds the lock.
    """
    from srunx.common.config import get_config
    from srunx.sync.lock import acquire_sync_lock

    timeout = get_config().sync.lock_timeout_seconds
    with acquire_sync_lock(profile_name, mount_name, timeout=timeout):
        return sync_mount_by_name(profile, mount_name, delete=delete)


def find_mount(profile: ServerProfile, mount_name: str):
    """Find a mount by name within a profile's mounts.

    Raises ValueError if not found.
    """
    for m in profile.mounts:
        if m.name == mount_name:
            return m
    raise ValueError(f"Mount '{mount_name}' not found")


def resolve_mounts_for_workflow(
    profile: ServerProfile,
    jobs_data: list[dict],
    default_project: str | None = None,
) -> list[str]:
    """Identify mount names to sync for a workflow's jobs.

    Matches each job's ``work_dir`` against mount remote paths using
    longest-prefix matching.  Also includes *default_project* if it
    corresponds to a valid mount.

    Returns:
        Deduplicated list of mount names.
    """
    mount_names: set[str] = set()

    if default_project:
        if any(m.name == default_project for m in profile.mounts):
            mount_names.add(default_project)

    for jd in jobs_data:
        work_dir = jd.get("work_dir", "")
        if not work_dir:
            continue
        # Find longest prefix match among mounts
        best_mount: str | None = None
        best_len = 0
        for m in profile.mounts:
            remote = m.remote.rstrip("/")
            if work_dir == remote or work_dir.startswith(remote + "/"):
                if len(remote) > best_len:
                    best_mount = m.name
                    best_len = len(remote)
        if best_mount:
            mount_names.add(best_mount)

    return list(mount_names)
