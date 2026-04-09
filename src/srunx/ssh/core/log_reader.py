"""Remote SLURM log retrieval over SSH.

Provides methods to locate, read, and stream job output/error logs
from a remote SLURM cluster.
"""

from __future__ import annotations

import os
import re
import shlex
from typing import TYPE_CHECKING

from srunx.logging import get_logger

from .utils import quote_shell_path, sanitize_job_id

if TYPE_CHECKING:
    from .connection import SSHConnection
    from .slurm import SlurmRemoteClient

_logger = get_logger(__name__)


class RemoteLogReader:
    """Read and stream SLURM job logs from a remote host."""

    def __init__(
        self,
        connection: SSHConnection,
        slurm: SlurmRemoteClient,
    ) -> None:
        self._conn = connection
        self._slurm = slurm
        self.logger = _logger

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def get_job_output(
        self,
        job_id: str,
        job_name: str | None = None,
        stdout_offset: int = 0,
        stderr_offset: int = 0,
    ) -> tuple[str, str, int, int]:
        """Get job output from SLURM log files.

        First tries ``scontrol show job`` to discover the actual StdOut/StdErr
        paths configured for the job.  Falls back to pattern-based search if
        scontrol doesn't return usable paths.

        When *stdout_offset* / *stderr_offset* are non-zero, only the bytes
        **after** that position are returned (tail-like incremental reads).

        Returns:
            ``(stdout, stderr, new_stdout_offset, new_stderr_offset)``
        """
        try:
            safe_job_id = sanitize_job_id(job_id)
            safe_job_name = None
            if job_name and re.fullmatch(r"[\w\-\.]+", job_name):
                safe_job_name = job_name

            output_content = ""
            error_content = ""
            new_stdout_offset = stdout_offset
            new_stderr_offset = stderr_offset

            # 1. Try scontrol to get actual log paths
            stdout_path, stderr_path = self._get_log_paths_from_scontrol(safe_job_id)

            if stdout_path:
                output_content, new_stdout_offset = self._read_file_from_offset(
                    stdout_path, stdout_offset
                )

            if stderr_path and stderr_path != stdout_path:
                error_content, new_stderr_offset = self._read_file_from_offset(
                    stderr_path, stderr_offset
                )

            if output_content or error_content:
                return (
                    output_content,
                    error_content,
                    new_stdout_offset,
                    new_stderr_offset,
                )

            # 2. Fallback: pattern-based file search (full read)
            out, err = self._get_job_output_by_pattern(safe_job_id, safe_job_name)
            return out, err, len(out.encode()), len(err.encode())

        except Exception as e:
            self.logger.error(f"Failed to get job output for {job_id}: {e}")
            return "", "", stdout_offset, stderr_offset

    def get_job_output_detailed(
        self, job_id: str, job_name: str | None = None
    ) -> dict[str, str | list[str] | None]:
        """Get detailed job output information including found log files."""
        try:
            safe_job_id = sanitize_job_id(job_id)
            safe_job_name = None
            if job_name:
                if re.fullmatch(r"[\w\-\.]+", job_name):
                    safe_job_name = job_name
                else:
                    self.logger.warning(
                        f"Rejecting unsafe job_name for log search: {job_name!r}"
                    )

            potential_log_patterns = [
                f"{safe_job_name}_{safe_job_id}.log" if safe_job_name else None,
                f"*_{safe_job_id}.log",
                f"{safe_job_name}_{safe_job_id}.log" if safe_job_name else None,
                f"{safe_job_id}.log",
                f"job_{safe_job_id}.log",
                f"*_{safe_job_id}.out",
            ]

            patterns = [p for p in potential_log_patterns if p is not None]

            log_dirs = [
                os.environ.get("SLURM_LOG_DIR", "~/logs/slurm"),
                "./",
                "/tmp",
                "/var/log/slurm",
            ]

            found_files: list[str] = []
            primary_log: str | None = None
            output_content = ""
            error_content = ""

            for log_dir in log_dirs:
                quoted_dir = quote_shell_path(log_dir)
                for pattern in patterns:
                    quoted_pattern = shlex.quote(pattern)
                    find_cmd = f"find {quoted_dir} -name {quoted_pattern} -type f 2>/dev/null | head -5"
                    stdout, stderr, exit_code = self._conn.execute_command(find_cmd)

                    if exit_code == 0 and stdout.strip():
                        log_files = stdout.strip().split("\n")
                        for log_file in log_files:
                            if log_file.strip():
                                found_files.append(log_file.strip())

            found_files = list(set(found_files))
            if found_files:
                primary_log = found_files[0]
                quoted_log = shlex.quote(primary_log)
                stdout_output, _, _ = self._conn.execute_command(
                    f"cat {quoted_log} 2>/dev/null || echo 'Could not read log file'"
                )
                output_content = stdout_output

                if len(found_files) > 1:
                    for log_file in found_files[1:]:
                        if "err" in log_file.lower() or "error" in log_file.lower():
                            quoted_err_log = shlex.quote(log_file)
                            stderr_output, _, _ = self._conn.execute_command(
                                f"cat {quoted_err_log} 2>/dev/null || echo ''"
                            )
                            error_content += stderr_output

            return {
                "found_files": found_files,
                "primary_log": primary_log,
                "output": output_content,
                "error": error_content,
                "slurm_log_dir": os.environ.get("SLURM_LOG_DIR"),
                "searched_dirs": log_dirs,
            }

        except Exception as e:
            self.logger.error(f"Failed to get detailed output for {job_id}: {e}")
            return {
                "found_files": [],
                "primary_log": None,
                "output": "",
                "error": "",
                "slurm_log_dir": os.environ.get("SLURM_LOG_DIR"),
                "searched_dirs": [],
            }

    def tail_log(
        self,
        job_id: str,
        job_name: str | None = None,
        follow: bool = False,
        last_n: int | None = None,
        poll_interval: float = 1.0,
    ) -> dict[str, str | bool | None]:
        """Display job logs with optional real-time streaming via SSH.

        Args:
            job_id: SLURM job ID
            job_name: Job name for better log file detection
            follow: If True, continuously stream new log lines (like tail -f)
            last_n: Show only the last N lines
            poll_interval: Polling interval in seconds for follow mode

        Returns:
            Dictionary with log information:
            - success: Whether log retrieval was successful
            - log_content: Log content (empty in follow mode)
            - tail_command: Command to execute for follow mode (None in static mode)
            - status_message: Status or error message
            - log_file: Path to the primary log file
        """
        log_info = self.get_job_output_detailed(job_id, job_name)
        primary_log = log_info.get("primary_log")
        found_files = log_info.get("found_files", [])

        if not found_files:
            searched_dirs = log_info.get("searched_dirs", [])
            searched_dirs_list = (
                searched_dirs if isinstance(searched_dirs, list) else []
            )
            msg = f"No log files found for job {job_id}\n"
            msg += f"Searched in: {', '.join(searched_dirs_list)}\n"
            slurm_log_dir = log_info.get("slurm_log_dir")
            if slurm_log_dir:
                msg += f"SLURM_LOG_DIR: {slurm_log_dir}\n"
            return {
                "success": False,
                "log_content": "",
                "tail_command": None,
                "status_message": msg,
                "log_file": None,
            }

        if not primary_log:
            return {
                "success": False,
                "log_content": "",
                "tail_command": None,
                "status_message": "Could not find primary log file",
                "log_file": None,
            }

        primary_log_str = str(primary_log) if primary_log else ""
        quoted_log_path = shlex.quote(primary_log_str)

        if follow:
            tail_cmd = "tail -f"
            if last_n:
                tail_cmd = f"tail -n {last_n} -f"

            tail_cmd += f" {quoted_log_path}"

            return {
                "success": True,
                "log_content": "",
                "tail_command": tail_cmd,
                "status_message": f"Streaming logs from {primary_log_str} (Ctrl+C to stop)...",
                "log_file": primary_log_str,
            }

        else:
            output_raw = log_info.get("output", "")
            output = str(output_raw) if output_raw else ""

            if last_n and isinstance(output, str):
                lines = output.split("\n")
                output = "\n".join(lines[-last_n:])

            if output:
                return {
                    "success": True,
                    "log_content": output,
                    "tail_command": None,
                    "status_message": f"Log file: {primary_log_str}",
                    "log_file": primary_log_str,
                }
            else:
                return {
                    "success": True,
                    "log_content": "",
                    "tail_command": None,
                    "status_message": "Log file is empty",
                    "log_file": primary_log_str,
                }

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _read_file_from_offset(self, path: str, offset: int) -> tuple[str, int]:
        """Read a remote file from a byte offset. Returns (content, new_offset)."""
        quoted = shlex.quote(path)
        if offset > 0:
            out, _, rc = self._conn.execute_command(
                f"tail -c +{offset + 1} {quoted} 2>/dev/null"
            )
        else:
            out, _, rc = self._conn.execute_command(f"cat {quoted} 2>/dev/null")
        if rc != 0:
            return "", offset

        size_out, _, src = self._conn.execute_command(f"wc -c < {quoted} 2>/dev/null")
        if src == 0 and size_out.strip().isdigit():
            new_offset = int(size_out.strip())
        else:
            new_offset = offset + len(out.encode())
        return out, new_offset

    def _get_log_paths_from_scontrol(
        self, job_id: str
    ) -> tuple[str | None, str | None]:
        """Query ``scontrol show job`` for StdOut / StdErr paths."""
        quoted_id = shlex.quote(job_id)
        stdout, _, rc = self._slurm.execute_slurm_command(
            f"scontrol show job {quoted_id} 2>/dev/null"
        )
        if rc != 0 or not stdout.strip():
            return None, None

        stdout_path: str | None = None
        stderr_path: str | None = None
        for line in stdout.splitlines():
            for token in line.split():
                if token.startswith("StdOut="):
                    stdout_path = token.split("=", 1)[1]
                elif token.startswith("StdErr="):
                    stderr_path = token.split("=", 1)[1]
        return stdout_path, stderr_path

    def _get_job_output_by_pattern(
        self, safe_job_id: str, safe_job_name: str | None
    ) -> tuple[str, str]:
        """Search common directories for SLURM log files by naming patterns."""
        potential_log_patterns = [
            f"{safe_job_name}_{safe_job_id}.log" if safe_job_name else None,
            f"*_{safe_job_id}.log",
            f"slurm-{safe_job_id}.out",
            f"slurm-{safe_job_id}.err",
            f"job_{safe_job_id}.log",
            f"{safe_job_id}.log",
            f"*_{safe_job_id}.out",
        ]

        patterns = [p for p in potential_log_patterns if p is not None]

        log_dirs = [
            os.environ.get("SLURM_LOG_DIR", "~/logs/slurm"),
            "./",
            "/tmp",
            "/var/log/slurm",
        ]

        output_content = ""
        error_content = ""
        found_files: list[str] = []

        for log_dir in log_dirs:
            quoted_dir = quote_shell_path(log_dir)
            for pattern in patterns:
                quoted_pattern = shlex.quote(pattern)
                find_cmd = f"find {quoted_dir} -name {quoted_pattern} -type f 2>/dev/null | head -5"
                stdout, stderr, exit_code = self._conn.execute_command(find_cmd)

                if exit_code == 0 and stdout.strip():
                    log_files = stdout.strip().split("\n")
                    for log_file in log_files:
                        if log_file.strip():
                            found_files.append(log_file.strip())
                            self.logger.debug(
                                f"Found potential log file: {log_file.strip()}"
                            )

        if found_files:
            primary_log = found_files[0]
            quoted_log = shlex.quote(primary_log)
            stdout_output, _, _ = self._conn.execute_command(
                f"cat {quoted_log} 2>/dev/null || echo 'Could not read log file'"
            )
            output_content = stdout_output

            if len(found_files) > 1:
                for log_file in found_files[1:]:
                    if "err" in log_file.lower() or "error" in log_file.lower():
                        quoted_err_log = shlex.quote(log_file)
                        stderr_output, _, _ = self._conn.execute_command(
                            f"cat {quoted_err_log} 2>/dev/null || echo ''"
                        )
                        error_content += stderr_output

            if self._conn.verbose:
                self.logger.info(
                    f"Found {len(found_files)} log file(s) for job {safe_job_id}"
                )
                self.logger.debug(f"Primary log file: {primary_log}")
        else:
            self.logger.warning(
                f"No log files found for job {safe_job_id} using common patterns"
            )

            default_patterns: list[str] = []
            if safe_job_name:
                default_patterns.append(f"{safe_job_name}_{safe_job_id}.log")
            default_patterns.append(f"{safe_job_id}.log")
            for pattern in default_patterns:
                quoted_pattern = shlex.quote(pattern)
                stdout_output, _, _ = self._conn.execute_command(
                    f"cat {quoted_pattern} 2>/dev/null || echo ''"
                )
                output_content += stdout_output

        return output_content, error_content
