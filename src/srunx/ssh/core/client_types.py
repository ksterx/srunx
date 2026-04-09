"""Shared data types for the SSH core package."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class SlurmJob:
    job_id: str
    name: str
    status: str = "UNKNOWN"
    output_file: str | None = None
    error_file: str | None = None
    script_path: str | None = None  # Path to script on server
    is_local_script: bool = False  # Whether script was uploaded from local
    _cleanup: bool = False  # Whether to cleanup temporary files
