"""Shared helpers for MCP tool modules.

Response shape (``ok`` / ``err``), input validation (``validate_job_id`` /
``validate_partition``), output conversion (``job_to_dict``), SSH client
resolution (``get_ssh_client``), and the ``python:`` prefix guard
(``reject_python_prefix``) live here. Tool modules under
:mod:`srunx.mcp.tools` import these by name; tests patch the lookup site
inside the calling tool module rather than this module.
"""

from __future__ import annotations

import re
from typing import Any

_SAFE_JOB_ID = re.compile(r"^\d+(_\d+)?$")
_SAFE_PARTITION = re.compile(r"^[a-zA-Z0-9_\-]+$")


def ok(data: Any = None, **kwargs: Any) -> dict[str, Any]:
    """Build a success response payload."""
    result: dict[str, Any] = {"success": True}
    if data is not None:
        result["data"] = data
    result.update(kwargs)
    return result


def err(message: str) -> dict[str, Any]:
    """Build an error response payload."""
    return {"success": False, "error": message}


def validate_job_id(job_id: str) -> str:
    """Validate that ``job_id`` is a numeric SLURM job ID (e.g. '12345' or '12345_1')."""
    if not _SAFE_JOB_ID.match(job_id):
        raise ValueError(f"Invalid job ID: {job_id!r}. Must be numeric (e.g. '12345').")
    return job_id


def validate_partition(partition: str) -> str:
    """Validate that ``partition`` contains only safe characters."""
    if not _SAFE_PARTITION.match(partition):
        raise ValueError(
            f"Invalid partition name: {partition!r}. "
            "Must contain only alphanumeric, underscore, or hyphen."
        )
    return partition


def job_to_dict(job: Any) -> dict[str, Any]:
    """Convert a BaseJob / Job / ShellJob to a JSON-serializable dict."""
    d: dict[str, Any] = {
        "name": job.name,
        "job_id": job.job_id,
        "status": job._status.value if hasattr(job, "_status") else "UNKNOWN",
    }
    for field in (
        "partition",
        "user",
        "elapsed_time",
        "nodes",
        "nodelist",
        "cpus",
        "gpus",
    ):
        val = getattr(job, field, None)
        if val is not None:
            d[field] = val
    if hasattr(job, "command"):
        cmd = job.command
        d["command"] = cmd if isinstance(cmd, str) else " ".join(cmd or [])
    if hasattr(job, "script_path"):
        d["script_path"] = job.script_path
    return d


def get_ssh_client() -> Any:
    """Return an :class:`SSHSlurmClient` for the current SSH profile.

    Resolves connection params from the active profile, falling back to
    ``~/.ssh/config`` when the profile references an SSH alias. Raises
    :class:`RuntimeError` if no current profile is set or the profile is
    missing.
    """
    from srunx.ssh.core.client import SSHSlurmClient
    from srunx.ssh.core.config import ConfigManager
    from srunx.ssh.core.ssh_config import SSHConfigParser

    cm = ConfigManager()
    profile_name = cm.get_current_profile_name()
    if not profile_name:
        raise RuntimeError(
            "No active SSH profile. Set one with: srunx ssh profile use <name>"
        )
    profile = cm.get_profile(profile_name)
    if not profile:
        raise RuntimeError(f"SSH profile '{profile_name}' not found")

    if profile.ssh_host:
        parser = SSHConfigParser()
        ssh_host = parser.get_host(profile.ssh_host)
        if not ssh_host:
            raise RuntimeError(
                f"SSH host '{profile.ssh_host}' not found in ~/.ssh/config"
            )
        return SSHSlurmClient(
            hostname=ssh_host.hostname or profile.ssh_host,
            username=ssh_host.user or "",
            key_filename=ssh_host.identity_file,
            port=ssh_host.port or 22,
            proxy_jump=ssh_host.proxy_jump,
            env_vars=dict(profile.env_vars) if profile.env_vars else None,
        )

    resolved_hostname = profile.hostname
    resolved_key = profile.key_filename
    resolved_port = profile.port
    resolved_proxy = profile.proxy_jump

    parser = SSHConfigParser()
    ssh_host = parser.get_host(profile.hostname)
    if ssh_host and ssh_host.hostname:
        resolved_hostname = ssh_host.hostname
        if ssh_host.identity_file and not resolved_key:
            resolved_key = ssh_host.identity_file
        if ssh_host.port:
            resolved_port = ssh_host.port
        if ssh_host.proxy_jump and not resolved_proxy:
            resolved_proxy = ssh_host.proxy_jump

    return SSHSlurmClient(
        hostname=resolved_hostname,
        username=profile.username,
        key_filename=resolved_key,
        port=resolved_port,
        proxy_jump=resolved_proxy,
        env_vars=dict(profile.env_vars) if profile.env_vars else None,
    )


def reject_python_prefix(payload: Any, *, source: str) -> None:
    """Reject ``python:``-prefixed strings in MCP tool payloads.

    MCP is reachable by any Claude Code session so we apply the same
    security rule as the Web path. Raises :class:`ValueError` on the
    first violation; callers convert that to the tool's
    ``{"success": False}`` response shape.
    """
    from srunx.runtime.security import find_python_prefix

    violation = find_python_prefix(payload, source=source)
    if violation is not None:
        raise ValueError(
            f"{violation.source} at '{violation.path}' contains 'python:' "
            f"prefix which is not allowed: {violation.value!r}"
        )
