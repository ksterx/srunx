"""Shared rsync sync utilities for web routers."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from srunx.ssh.core.config import ServerProfile

from srunx.sync.rsync import RsyncClient


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


def build_rsync_client(profile: ServerProfile) -> RsyncClient:
    """Create RsyncClient from SSH profile, handling ssh_host vs hostname.

    When *ssh_host* is set the client delegates all connection parameters
    (user, key, proxy, port) to ``~/.ssh/config``.
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


def sync_mount_by_name(profile: ServerProfile, mount_name: str) -> None:
    """Sync a named mount's local directory to remote via rsync.

    Raises:
        ValueError: If *mount_name* does not exist in the profile.
        RuntimeError: If the rsync process exits with a non-zero code.
    """
    mount = next((m for m in profile.mounts if m.name == mount_name), None)
    if mount is None:
        raise ValueError(f"Mount '{mount_name}' not found in profile")
    rsync = build_rsync_client(profile)
    result = rsync.push(
        mount.local, mount.remote, exclude_patterns=mount.exclude_patterns or None
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"rsync sync failed for mount '{mount_name}': {result.stderr}"
        )


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
