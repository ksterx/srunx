"""Shared helpers for MCP tool modules.

Response shape (``ok`` / ``err``), input validation (``validate_job_id`` /
``validate_partition``), output conversion (``job_to_dict``), and the
``python:`` prefix guard (``reject_python_prefix``) live here. Tool modules
under :mod:`srunx.mcp.tools` import these by name; tests patch the lookup
site inside the calling tool module rather than this module.

Transport selection (local vs SSH profile) is *not* here — it lives in
:mod:`srunx.mcp.transport`, which routes every cluster-acting tool through
the shared ``resolve_transport`` handle. There is no MCP-specific SSH client
factory; the former current-profile-forcing resolver was removed in favour
of the explicit ``transport`` argument.
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


def reject_python_prefix_in_yaml_file(yaml_path: str) -> None:
    """Apply the ``python:`` guard to a workflow file's own ``args`` section.

    ``WorkflowRunner.from_yaml`` merges and evaluates the YAML file's ``args``
    at load time, so guarding only the caller-supplied ``args`` override
    (as :func:`reject_python_prefix` does) leaves the file's own args
    unguarded. This mirrors the Web path's ``reject_python_prefix_in_yaml_args``
    so MCP validate/run/get share the same boundary. Missing/malformed files
    are left to downstream validation to report.
    """
    import yaml

    from pathlib import Path

    try:
        text = Path(yaml_path).read_text()
        data = yaml.safe_load(text)
    except Exception:
        return

    if not isinstance(data, dict):
        return
    args = data.get("args")
    if isinstance(args, dict):
        reject_python_prefix(args, source="args")
