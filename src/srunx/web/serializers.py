"""Serialize data for frontend-compatible responses.

In the SSH adapter architecture, most data is already dicts from
the adapter's parsing. These helpers normalize edge cases.
"""

from __future__ import annotations

from typing import Any


def serialize_history_entry(entry: dict[str, Any]) -> dict[str, Any]:
    """Map history dict keys to frontend JobHistoryEntry schema."""
    return {
        "job_id": entry.get("job_id"),
        "job_name": entry.get("job_name", entry.get("name", "")),
        "command": entry.get("command"),
        "status": entry.get("status", "UNKNOWN"),
        "submitted_at": str(entry.get("submitted_at", "")),
        "completed_at": str(entry["completed_at"])
        if entry.get("completed_at")
        else None,
        "workflow_name": entry.get("workflow_name"),
        "partition": entry.get("partition"),
        "nodes": entry.get("nodes"),
        "gpus": entry.get("gpus"),
    }


def serialize_job_stats(stats: dict[str, Any]) -> dict[str, Any]:
    """Map history stats dict to frontend JobStats schema."""
    by_status = stats.get("jobs_by_status", {})
    return {
        "total": stats.get("total_jobs", 0),
        "completed": by_status.get("COMPLETED", 0),
        "failed": by_status.get("FAILED", 0),
        "cancelled": by_status.get("CANCELLED", 0),
        "avg_runtime_seconds": stats.get("avg_duration_seconds"),
    }
