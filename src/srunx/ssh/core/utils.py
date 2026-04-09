"""Shared utility functions for the SSH core package.

These were originally static methods on SSHSlurmClient and carry no
instance state, so they live here as plain module-level functions.
"""

from __future__ import annotations

import re
import shlex
import subprocess
from pathlib import Path


def quote_shell_path(path: str) -> str:
    """Quote a path for remote shell, handling ~ expansion.

    shlex.quote prevents ~ expansion, so paths starting with ~/
    are converted to use $HOME with double quotes instead.
    """
    if path.startswith("~/"):
        # Double quotes allow $HOME expansion while preventing word splitting
        suffix = path[2:]
        return '"$HOME/' + suffix + '"'
    return shlex.quote(path)


def sanitize_job_id(job_id: str | int) -> str:
    """Sanitize a SLURM job ID, supporting array and step IDs.

    Valid formats: 12345, 12345_4, 12345_[1-10], 12345.0
    """
    job_id_str = str(job_id)
    if not re.fullmatch(r"[0-9][0-9_.\[\]\-]*", job_id_str):
        raise ValueError(f"Invalid SLURM job ID: {job_id_str!r}")
    return job_id_str


def detect_project_root() -> str:
    """Detect the project root directory via git or fallback to cwd."""
    try:
        result = subprocess.run(  # noqa: S603, S607
            ["git", "rev-parse", "--show-toplevel"],
            capture_output=True,
            text=True,
        )
        if result.returncode == 0:
            return result.stdout.strip()
    except FileNotFoundError:
        pass
    return str(Path.cwd())
