"""MCP tools: srunx config + SSH profile listing."""

from __future__ import annotations

from typing import Any

from srunx.mcp.app import mcp
from srunx.mcp.helpers import err, ok


@mcp.tool()
def get_config() -> dict[str, Any]:
    """Get the current srunx configuration including resource defaults and environment settings."""
    try:
        from srunx.common.config import get_config as _get_config

        config = _get_config()
        return ok(
            resources=config.resources.model_dump(),
            environment={
                "conda": config.environment.conda,
                "venv": config.environment.venv,
                "env_vars": config.environment.env_vars,
            },
            log_dir=config.log_dir,
            work_dir=config.work_dir,
        )
    except Exception as e:
        return err(str(e))


@mcp.tool()
def list_ssh_profiles() -> dict[str, Any]:
    """List all configured SSH connection profiles for remote SLURM clusters.

    Shows profile names, hostnames, and configured mount points.
    """
    try:
        from srunx.ssh.core.config import ConfigManager

        cm = ConfigManager()
        profiles = cm.list_profiles()
        current = cm.get_current_profile_name()

        result = []
        for name, profile in profiles.items():
            mounts = [
                {"name": m.name, "local": m.local, "remote": m.remote}
                for m in profile.mounts
            ]
            result.append(
                {
                    "name": name,
                    "hostname": profile.hostname,
                    "username": profile.username,
                    "port": profile.port,
                    "description": profile.description,
                    "is_current": name == current,
                    "mounts": mounts,
                }
            )

        return ok(profiles=result, current=current, count=len(result))
    except Exception as e:
        return err(str(e))
