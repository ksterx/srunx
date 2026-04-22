"""SLURM operations over an SSH connection.

Handles path discovery, environment setup, job submission, monitoring,
and error handling for remote SLURM clusters.
"""

from __future__ import annotations

import re
import shlex
import time
import uuid
from typing import TYPE_CHECKING

from srunx.logging import get_logger

if TYPE_CHECKING:
    from .connection import SSHConnection
    from .file_manager import RemoteFileManager

# Imported at module level so the facade can reference it
from .client_types import SlurmJob  # noqa: F401  re-exported for convenience
from .utils import query_slurm_job_state

_logger = get_logger(__name__)


class SlurmRemoteClient:
    """SLURM operations executed over an SSH connection."""

    def __init__(
        self,
        connection: SSHConnection,
        file_manager: RemoteFileManager,
    ) -> None:
        self._conn = connection
        self._files = file_manager
        self.logger = _logger
        self._slurm_path: str | None = None

    # ------------------------------------------------------------------
    # Initialization (called after connect())
    # ------------------------------------------------------------------

    def initialize(self) -> None:
        """Discover SLURM paths and verify the installation."""
        self._initialize_slurm_paths()
        self._verify_slurm_setup()

    def _initialize_slurm_paths(self) -> None:
        """Initialize SLURM command paths by checking common locations and environment."""
        try:
            stdout, stderr, exit_code = self._conn.execute_command(
                "bash -l -c 'echo $PATH' 2>/dev/null || echo ''"
            )

            if stdout.strip():
                login_path = stdout.strip()
                self.logger.debug(f"Login shell PATH: {login_path}")

                stdout, stderr, exit_code = self._conn.execute_command(
                    "bash -l -c 'which sbatch' 2>/dev/null || echo 'NOT_FOUND'"
                )

                self.logger.debug(
                    f"SLURM which command result: stdout='{stdout}', stderr='{stderr}', exit_code={exit_code}"
                )

                if exit_code == 0 and "NOT_FOUND" not in stdout:
                    sbatch_path = stdout.strip()
                    self._slurm_path = sbatch_path.rsplit("/", 1)[0]
                    if self._conn.verbose:
                        self.logger.info(f"Found SLURM at: {self._slurm_path}")
                        self.logger.debug(f"Full sbatch path: {sbatch_path}")
                    return

            common_paths = [
                "/cm/shared/apps/slurm/current/bin",
                "/usr/bin",
                "/usr/local/bin",
                "/opt/slurm/bin",
                "/cluster/slurm/bin",
            ]

            for path in common_paths:
                stdout, stderr, exit_code = self._conn.execute_command(
                    f"test -f {path}/sbatch && echo 'FOUND' || echo 'NOT_FOUND'"
                )
                self.logger.debug(f"Checking {path}: {stdout.strip()}")
                if "FOUND" in stdout:
                    self._slurm_path = path
                    if self._conn.verbose:
                        self.logger.info(f"Found SLURM at: {self._slurm_path}")
                    stdout, stderr, exit_code = self._conn.execute_command(
                        f"test -x {path}/sbatch && echo 'EXECUTABLE' || echo 'NOT_EXECUTABLE'"
                    )
                    self.logger.debug(f"SLURM executable check: {stdout.strip()}")
                    return

            self.logger.warning("SLURM commands not found in standard locations")

        except Exception as e:
            self.logger.warning(f"Failed to initialize SLURM paths: {e}")

    def _get_slurm_command(self, command: str) -> str:
        """Get the full path for a SLURM command, or use login shell if path not found."""
        if self._slurm_path:
            return f"{self._slurm_path}/{command}"
        else:
            return command

    def _get_slurm_env_setup(self) -> str:
        """Get environment setup commands for SLURM execution."""
        env_commands = [
            "cd ~",
            "source /etc/profile 2>/dev/null || true",
            "source ~/.bash_profile 2>/dev/null || true",
            "source ~/.bashrc 2>/dev/null || true",
            "source ~/.profile 2>/dev/null || true",
            "module load slurm 2>/dev/null || true",
            "module load slurm/current 2>/dev/null || true",
            'which sbatch >/dev/null 2>&1 || export PATH="$PATH:/cm/shared/apps/slurm/current/bin" 2>/dev/null || true',
            'export PATH="$PATH:/usr/local/bin:/opt/slurm/bin:/cluster/slurm/bin" 2>/dev/null || true',
        ]

        for key, value in self._conn.custom_env_vars.items():
            escaped_value = value.replace("'", "'\\''")
            env_commands.append(f"export {key}='{escaped_value}'")
            self.logger.debug(f"Adding custom environment variable: {key}={value}")

        return " && ".join(env_commands)

    def _verify_slurm_setup(self) -> None:
        """Verify that SLURM commands are working properly."""
        try:
            test_cmd = (
                "sbatch --version 2>/dev/null | head -1 || echo 'SLURM_NOT_AVAILABLE'"
            )
            stdout, stderr, exit_code = self.execute_slurm_command(test_cmd)

            if exit_code == 0 and "SLURM_NOT_AVAILABLE" not in stdout:
                if self._conn.verbose:
                    self.logger.info(f"SLURM verification successful: {stdout.strip()}")
            else:
                self.logger.warning(f"SLURM verification failed: {stdout} / {stderr}")
        except Exception as e:
            self.logger.warning(f"SLURM verification error: {e}")

    # ------------------------------------------------------------------
    # Command execution
    # ------------------------------------------------------------------

    def execute_slurm_command(self, command: str) -> tuple[str, str, int]:
        """Execute a SLURM command with proper environment."""
        env_setup = self._get_slurm_env_setup()

        if self._slurm_path:
            slurm_commands = [
                "sbatch",
                "squeue",
                "sacct",
                "scancel",
                "scontrol",
                "sinfo",
            ]
            modified_command = command
            for cmd in slurm_commands:
                if modified_command.startswith(cmd + " ") or modified_command == cmd:
                    modified_command = modified_command.replace(
                        cmd, f"{self._slurm_path}/{cmd}", 1
                    )
                    break
            final_command = f"{env_setup} && {modified_command}"
        else:
            final_command = f"{env_setup} && {command}"

        full_cmd = f"bash -l -c {shlex.quote(final_command)}"
        self.logger.debug(f"Executing SLURM command: {full_cmd}")
        stdout, stderr, exit_code = self._conn.execute_command(full_cmd)
        self.logger.debug(
            f"SLURM command result: exit_code={exit_code}, stdout_len={len(stdout)}, stderr_len={len(stderr)}"
        )
        if stderr and exit_code != 0:
            self.logger.debug(f"SLURM command stderr: {stderr[:500]}...")
        return stdout, stderr, exit_code

    # ------------------------------------------------------------------
    # Job submission
    # ------------------------------------------------------------------

    def submit_sbatch_job(
        self,
        script_content: str,
        job_name: str | None = None,
        dependency: str | None = None,
    ) -> SlurmJob | None:
        """Submit an sbatch job with script content."""
        try:
            unique_id = str(uuid.uuid4())[:8]
            remote_script_path = f"{self._conn.temp_dir}/job_{unique_id}.sh"
            self._files.write_remote_file(remote_script_path, script_content)

            self._conn.execute_command(f"chmod +x {shlex.quote(remote_script_path)}")

            valid, validation_msg = self._files.validate_remote_script(
                remote_script_path
            )
            if not valid:
                self.logger.error(f"Script validation failed: {validation_msg}")
                return None

            cmd = f"{self._get_slurm_command('sbatch')}"
            if job_name:
                safe_name = re.sub(r"[^a-zA-Z0-9_.-]", "_", job_name)
                cmd += f" --job-name={safe_name}"
            if dependency:
                if not re.fullmatch(r"[a-z]+:\d+(,[a-z]+:\d+)*", dependency):
                    raise ValueError(f"Invalid dependency format: {dependency!r}")
                cmd += f" --dependency={dependency}"
            cmd += f" {remote_script_path}"

            stdout, stderr, exit_code = self.execute_slurm_command(cmd)
            if exit_code == 0:
                match = re.search(r"Submitted batch job (\d+)", stdout)
                if match:
                    job_id = match.group(1)
                    return SlurmJob(job_id=job_id, name=job_name or f"job_{job_id}")

            self.logger.error(
                f"Failed to submit job. stdout: {stdout}, stderr: {stderr}, exit_code: {exit_code}"
            )
            return None

        except Exception as e:
            self.logger.error(f"Job submission failed: {e}")
            return None

    def submit_sbatch_file(
        self, script_path: str, job_name: str | None = None, cleanup: bool = True
    ) -> SlurmJob | None:
        """Submit an sbatch job from a local or remote file."""
        try:
            from pathlib import Path as _Path

            path_obj = _Path(script_path)
            if path_obj.exists():
                remote_path = self._files.upload_file(script_path)

                if not job_name:
                    job_name = path_obj.stem

                safe_name = re.sub(r"[^a-zA-Z0-9_.-]", "_", job_name)
                cmd = f"{self._get_slurm_command('sbatch')}"
                cmd += f" -J {safe_name}"
                cmd += " -o $SLURM_LOG_DIR/%x_%j.log"
                cmd += f" {remote_path}"

                stdout, stderr, exit_code = self.execute_slurm_command(cmd)
                if exit_code == 0:
                    match = re.search(r"Submitted batch job (\d+)", stdout)
                    if match:
                        job_id = match.group(1)
                        job = SlurmJob(job_id=job_id, name=safe_name)
                        job.script_path = remote_path
                        job.is_local_script = True
                        job._cleanup = cleanup
                        return job

                self.logger.error(
                    f"Failed to submit job. stdout: {stdout}, stderr: {stderr}, exit_code: {exit_code}"
                )
                return None
            else:
                valid, validation_msg = self._files.validate_remote_script(script_path)
                if not valid:
                    self.logger.error(
                        f"Remote script validation failed: {validation_msg}"
                    )
                    return None

                if not job_name:
                    job_name = _Path(script_path).stem

                safe_name = re.sub(r"[^a-zA-Z0-9_.-]", "_", job_name)
                cmd = f"{self._get_slurm_command('sbatch')}"
                cmd += f" -J {safe_name}"
                cmd += " -o $SLURM_LOG_DIR/%x_%j.log"
                cmd += f" {shlex.quote(script_path)}"

                stdout, stderr, exit_code = self.execute_slurm_command(cmd)
                if exit_code == 0:
                    match = re.search(r"Submitted batch job (\d+)", stdout)
                    if match:
                        job_id = match.group(1)
                        job = SlurmJob(job_id=job_id, name=safe_name)
                        job.script_path = script_path
                        return job

                self.logger.error(
                    f"Failed to submit job. stdout: {stdout}, stderr: {stderr}, exit_code: {exit_code}"
                )
                return None

        except Exception as e:
            self.logger.error(f"Job submission failed: {e}")
            return None

    def submit_remote_sbatch_file(
        self,
        remote_path: str,
        *,
        submit_cwd: str | None = None,
        job_name: str | None = None,
        dependency: str | None = None,
    ) -> SlurmJob | None:
        """Submit a script that already exists on the remote cluster.

        Distinct from :meth:`submit_sbatch_file` so the in-place
        execution path stays free of that method's two side-effects
        that would break user expectations here:

        1. ``submit_sbatch_file`` inspects *local* path existence and
           SFTP-uploads when the file is present locally. For in-place
           execution we *want* sbatch to target the remote-resident
           path verbatim, even if a like-named file happens to exist
           locally.
        2. ``submit_sbatch_file`` injects ``-o $SLURM_LOG_DIR/%x_%j.log``
           unconditionally, overriding any ``#SBATCH --output=`` the
           user wrote in their own script. For a user-authored script
           on a synced mount, their directives must win.

        The ``submit_cwd`` argument determines where the sbatch
        invocation runs from. SSH sessions default to the user's
        remote ``$HOME``; relative paths in the user's ``#SBATCH``
        directives (e.g. ``--output=./logs/%j.out``) only resolve the
        way the user expects when we ``cd`` somewhere meaningful
        first. Pass ``None`` to keep the default.
        """
        try:
            valid, validation_msg = self._files.validate_remote_script(remote_path)
            if not valid:
                self.logger.error(f"Remote script validation failed: {validation_msg}")
                return None

            cmd_parts: list[str] = [self._get_slurm_command("sbatch")]
            if job_name:
                safe_name = re.sub(r"[^a-zA-Z0-9_.-]", "_", job_name)
                cmd_parts.append(f"--job-name={shlex.quote(safe_name)}")
            if dependency:
                if not re.fullmatch(r"[a-z]+:\d+(,[a-z]+:\d+)*", dependency):
                    raise ValueError(f"Invalid dependency format: {dependency!r}")
                cmd_parts.append(f"--dependency={dependency}")
            cmd_parts.append(shlex.quote(remote_path))

            cmd = " ".join(cmd_parts)
            if submit_cwd:
                cmd = f"cd {shlex.quote(submit_cwd)} && {cmd}"

            stdout, stderr, exit_code = self.execute_slurm_command(cmd)
            if exit_code == 0:
                match = re.search(r"Submitted batch job (\d+)", stdout)
                if match:
                    job_id = match.group(1)
                    resolved_name = (
                        re.sub(r"[^a-zA-Z0-9_.-]", "_", job_name)
                        if job_name
                        else f"job_{job_id}"
                    )
                    job = SlurmJob(job_id=job_id, name=resolved_name)
                    job.script_path = remote_path
                    return job

            self.logger.error(
                f"Failed to submit remote sbatch. stdout: {stdout}, "
                f"stderr: {stderr}, exit_code: {exit_code}"
            )
            return None
        except Exception as e:
            self.logger.error(f"Remote sbatch submission failed: {e}")
            return None

    # ------------------------------------------------------------------
    # Job status / monitoring
    # ------------------------------------------------------------------

    def get_job_status(self, job_id: str) -> str:
        """Get job status using SLURM commands (sacct -> squeue -> scontrol)."""
        return query_slurm_job_state(job_id, self.execute_slurm_command, self.logger)

    def monitor_job(
        self, job: SlurmJob, poll_interval: int = 10, timeout: int | None = None
    ) -> SlurmJob:
        """Monitor a job until completion."""
        start_time = time.time()
        while True:
            current_time = time.time()
            elapsed_time = current_time - start_time
            job.status = self.get_job_status(job.job_id)
            if job.status in [
                "COMPLETED",
                "FAILED",
                "CANCELLED",
                "TIMEOUT",
                "NOT_FOUND",
            ]:
                break
            if timeout and elapsed_time > timeout:
                job.status = "TIMEOUT"
                break
            time.sleep(poll_interval)
        return job

    def cleanup_job_files(self, job: SlurmJob) -> None:
        """Cleanup temporary files for a job if it was a local script."""
        if job.is_local_script and job.script_path and job._cleanup:
            self._files.cleanup_file(job.script_path)

    # ------------------------------------------------------------------
    # Error handling
    # ------------------------------------------------------------------

    def _handle_slurm_error(
        self, command: str, error_output: str, exit_code: int
    ) -> None:
        """Handle SLURM command errors with helpful suggestions."""
        self.logger.error(
            f"{command} failed with exit code {exit_code}: {error_output}"
        )
        error_lower = error_output.lower()
        if "command not found" in error_lower or "sbatch: not found" in error_lower:
            self.logger.error("SLURM commands not found in PATH.")
            self.logger.error("Ensure SLURM is installed and configured on the server.")
        elif "permission denied" in error_lower:
            self.logger.error(
                "Permission denied. Check file permissions and user access."
            )
        elif "invalid partition" in error_lower:
            self.logger.error(
                "Invalid partition specified. Check available partitions with 'sinfo'."
            )
        elif "invalid qos" in error_lower:
            self.logger.error(
                "Invalid QoS specified. Check available QoS with 'sacctmgr show qos'."
            )
