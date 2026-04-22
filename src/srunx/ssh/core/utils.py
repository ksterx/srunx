"""Shared utility functions for the SSH core package.

These were originally static methods on SSHSlurmClient and carry no
instance state, so they live here as plain module-level functions.
"""

from __future__ import annotations

import re
import shlex
import subprocess
from collections.abc import Callable
from pathlib import Path
from typing import Any

# scontrol prints whitespace-separated ``Key=Value`` tokens; ``JobState=`` is
# the observed state (e.g. RUNNING, COMPLETED) and ``ExitCode=N:M`` carries
# the process exit code (N) and terminating signal (M). Match until the next
# whitespace so multi-word values stop at the token boundary.
_SCONTROL_JOBSTATE_RE = re.compile(r"JobState=(\S+)")
_SCONTROL_EXITCODE_RE = re.compile(r"ExitCode=(\d+):(\d+)")

# Matches a SLURM job id (optionally with array / step suffix). Shared by
# the 3-stage status-query helper so both the facade and the component
# class reject shell-unsafe input identically.
_JOB_ID_RE = re.compile(r"^[0-9]+([._][A-Za-z0-9_-]+)?$")


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


def parse_scontrol_job_state(output: str) -> str | None:
    """Extract a SLURM state string from ``scontrol show job`` output.

    Returns ``None`` when the output is empty or lacks a ``JobState=`` token
    — callers treat that as "scontrol didn't know either" and fall through
    to the NOT_FOUND sentinel. When ``JobState=COMPLETED`` appears we also
    consult ``ExitCode`` to disambiguate: a non-zero exit code or signal
    means the job actually failed (SLURM reports COMPLETED for any clean
    exit regardless of the process's own status), so we downgrade to
    FAILED. Other states pass through unchanged so RUNNING / PENDING /
    CANCELLED / TIMEOUT remain distinguishable.
    """
    if not output or not output.strip():
        return None

    state_match = _SCONTROL_JOBSTATE_RE.search(output)
    if not state_match:
        return None

    state = state_match.group(1).strip().upper()
    if state == "COMPLETED":
        exit_match = _SCONTROL_EXITCODE_RE.search(output)
        if exit_match:
            exit_code = int(exit_match.group(1))
            signal = int(exit_match.group(2))
            if exit_code != 0 or signal != 0:
                return "FAILED"
    return state


def query_slurm_job_state(
    job_id: str,
    execute: Callable[[str], tuple[str, str, int]],
    logger: Any,
) -> str:
    """Three-stage SLURM job state query: sacct -> squeue -> scontrol.

    Returns the raw SLURM state string (e.g. ``"RUNNING"``) or one of the
    sentinels ``"NOT_FOUND"`` / ``"ERROR"``. The ``execute`` callable must
    run a SLURM command with a fully prepared environment and return the
    ``(stdout, stderr, exit_code)`` tuple — typically
    ``SSHSlurmClient._execute_slurm_command`` or
    ``SlurmRemoteClient.execute_slurm_command``.

    The scontrol tier is load-bearing on pyxis / slurmdbd-unreachable
    clusters: sacct returns empty for finished jobs and once a job
    leaves the queue squeue returns empty too. scontrol keeps the
    record in memory for MinJobAge (~5 min default) and does not depend
    on slurmdbd, so it disambiguates COMPLETED vs FAILED via ExitCode
    for the critical post-completion window.
    """
    try:
        if not _JOB_ID_RE.match(job_id):
            logger.error(f"Invalid job_id format: {job_id!r}")
            return "ERROR"

        sacct_cmd = (
            f"sacct -j {job_id} --format=JobID,State --noheader | "
            "grep -E '^[0-9]+' | head -1"
        )
        stdout, _, exit_code = execute(sacct_cmd)

        if exit_code == 0 and stdout.strip():
            status = stdout.strip().split()[1].split("+")[0]
            return status

        squeue_cmd = f"squeue -j {job_id} -h -o %T | head -1"
        stdout, _, exit_code = execute(squeue_cmd)
        if exit_code == 0 and stdout.strip():
            return stdout.strip().split("\n")[0].strip()

        quoted_id = shlex.quote(job_id)
        scontrol_out, _, scontrol_rc = execute(
            f"scontrol show job {quoted_id} 2>/dev/null"
        )
        if scontrol_rc == 0 and scontrol_out.strip():
            parsed = parse_scontrol_job_state(scontrol_out)
            if parsed is not None:
                return parsed

        return "NOT_FOUND"

    except Exception as e:
        logger.error(f"Failed to get job status for job {job_id}: {e}")
        return "ERROR"
