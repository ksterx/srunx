"""MCP tool: rsync-based file sync between local + remote SLURM cluster."""

from __future__ import annotations

from typing import Any

from srunx.mcp.app import mcp
from srunx.mcp.helpers import err, ok


@mcp.tool()
def sync_files(
    transport: str,
    mount_name: str | None = None,
    local_path: str | None = None,
    remote_path: str | None = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Sync files between local machine and a remote SLURM cluster using rsync.

    Can sync using a configured mount point (transport + mount_name), or using
    explicit paths (local_path + remote_path).

    Args:
        transport: SSH profile name to sync against. Required and must name an
            SSH profile — there is no local-to-local sync, and (unlike the CLI)
            no implicit current-profile fallback. ``"local"`` is rejected.
        mount_name: Mount point name from the SSH profile to sync
        local_path: Local directory path (alternative to mount_name)
        remote_path: Remote directory path (alternative to mount_name)
        dry_run: If true, show what would be transferred without actually syncing
    """
    try:
        from srunx.ssh.core.config import ConfigManager
        from srunx.web.sync_utils import build_rsync_client

        pname = transport.strip() if transport else ""
        if not pname or pname == "local":
            return err(
                "sync_files requires an SSH profile name (transport='<profile>'); "
                "there is no local sync."
            )

        cm = ConfigManager()
        profile = cm.get_profile(pname)
        if not profile:
            return err(f"SSH profile '{pname}' not found")

        if mount_name:
            mount = next((m for m in profile.mounts if m.name == mount_name), None)
            if not mount:
                available = [m.name for m in profile.mounts]
                return err(
                    f"Mount '{mount_name}' not found in profile '{pname}'. "
                    f"Available: {available}"
                )

            rsync = build_rsync_client(profile)
            result = rsync.push(
                mount.local,
                mount.remote,
                dry_run=dry_run,
                exclude_patterns=mount.exclude_patterns,
            )
            if not result.success:
                return err(
                    f"rsync failed (exit {result.returncode}): "
                    f"{result.stderr[:500] if result.stderr else 'unknown error'}"
                )
            return ok(
                profile=pname,
                mount=mount_name,
                local=mount.local,
                remote=mount.remote,
                dry_run=dry_run,
                output=result.stdout[:2000] if result.stdout else "",
            )

        if local_path:
            rsync = build_rsync_client(profile)
            result = rsync.push(local_path, remote_path, dry_run=dry_run)
            if not result.success:
                return err(
                    f"rsync failed (exit {result.returncode}): "
                    f"{result.stderr[:500] if result.stderr else 'unknown error'}"
                )
            return ok(
                profile=pname,
                local=local_path,
                remote=remote_path or rsync.get_default_remote_path(local_path),
                dry_run=dry_run,
                output=result.stdout[:2000] if result.stdout else "",
            )

        return err("Specify either mount_name or local_path for sync")

    except Exception as e:
        return err(str(e))
