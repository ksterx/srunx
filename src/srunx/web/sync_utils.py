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


def get_current_profile() -> ServerProfile | None:
    """Get the current SSH profile from web config or ConfigManager.

    Checks ``SRUNX_SSH_PROFILE`` (via :func:`get_web_config`) first,
    then falls back to :meth:`ConfigManager.get_current_profile_name`.

    Returns ``None`` if no profile is configured.
    """
    from srunx.ssh.core.config import ConfigManager

    from .config import get_web_config

    config = get_web_config()
    cm = ConfigManager()

    profile_name = config.ssh_profile
    if not profile_name:
        profile_name = cm.get_current_profile_name()

    if not profile_name:
        return None

    return cm.get_profile(profile_name)


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
