"""Backward-compatible facade for SSH/SLURM operations.

:class:`SSHSlurmClient` composes four focused classes —
:class:`SSHConnection`, :class:`RemoteFileManager`,
:class:`SlurmRemoteClient`, and :class:`RemoteLogReader` — and
re-exposes every original method so that existing consumers and
tests continue to work without modification.

Methods that involve cross-method calls (e.g. ``submit_sbatch_job``
calling ``_write_remote_file``, ``validate_remote_script``, and
``_execute_slurm_command``) are implemented directly on the facade
so that instance-level method replacements (such as test mocks)
propagate correctly through the entire call chain.  The component
classes can also be used independently for finer-grained control.
"""

from __future__ import annotations

import os
import re
import shlex
import subprocess
import time
import uuid
from pathlib import Path
from typing import TYPE_CHECKING

from srunx.logging import get_logger
from srunx.sync import RsyncClient

if TYPE_CHECKING:
    import paramiko

    from .proxy_client import ProxySSHClient

from .client_types import SlurmJob
from .connection import SSHConnection
from .file_manager import RemoteFileManager
from .log_reader import RemoteLogReader
from .slurm import SlurmRemoteClient
from .utils import (
    quote_shell_path,
    sanitize_job_id,
)

# Re-export so ``from srunx.ssh.core.client import SlurmJob`` keeps working
__all__ = ["SSHSlurmClient", "SlurmJob"]

_logger = get_logger(__name__)


class SSHSlurmClient:
    """Backward-compatible facade composing all SSH/SLURM components.

    Internally the work is organized into four component classes:

    * ``self.connection`` -- :class:`SSHConnection`
    * ``self.files``      -- :class:`RemoteFileManager`
    * ``self.slurm``      -- :class:`SlurmRemoteClient`
    * ``self.logs``       -- :class:`RemoteLogReader`

    Most facade methods carry their own implementation (identical to
    the original monolithic class) and call ``self.xxx()`` so that
    instance-level method replacements (e.g. test mocks) propagate
    correctly through the call chain.
    """

    def __init__(
        self,
        hostname: str,
        username: str,
        password: str | None = None,
        key_filename: str | None = None,
        port: int = 22,
        proxy_jump: str | None = None,
        ssh_config_path: str | None = None,
        env_vars: dict[str, str] | None = None,
        verbose: bool = False,
    ):
        self.hostname = hostname
        self.username = username
        self.password = password
        self.key_filename = key_filename
        self.port = port
        self.proxy_jump = proxy_jump
        self.ssh_config_path = ssh_config_path
        self.ssh_client: paramiko.SSHClient | None = None
        self.sftp_client: paramiko.SFTPClient | None = None
        self.proxy_client: ProxySSHClient | None = None
        self.logger = _logger
        self.temp_dir = os.getenv("SRUNX_TEMP_DIR", "/tmp/srunx")
        self._slurm_path: str | None = None
        self.custom_env_vars: dict[str, str] = env_vars or {}
        self.verbose = verbose

        # ── Composed components (for standalone / advanced usage) ───
        self.connection = SSHConnection(
            hostname=hostname,
            username=username,
            password=password,
            key_filename=key_filename,
            port=port,
            proxy_jump=proxy_jump,
            ssh_config_path=ssh_config_path,
            verbose=verbose,
            temp_dir=self.temp_dir,
            env_vars=env_vars,
        )
        self.files = RemoteFileManager(self.connection)
        self.slurm = SlurmRemoteClient(self.connection, self.files)
        self.logs = RemoteLogReader(self.connection, self.slurm)

        # RsyncClient for project sync (key-based auth only)
        self._rsync_client: RsyncClient | None = None
        if self.key_filename:
            try:
                self._rsync_client = RsyncClient(
                    hostname=self.hostname,
                    username=self.username,
                    port=self.port,
                    key_filename=self.key_filename,
                    proxy_jump=self.proxy_jump,
                    ssh_config_path=self.ssh_config_path,
                )
            except RuntimeError:
                self.logger.warning("rsync not available; sync_project() disabled")

    # ==================================================================
    # Connection lifecycle
    # ==================================================================

    def connect(self) -> bool:
        try:
            if self.proxy_jump:
                if not self.key_filename:
                    raise ValueError("ProxyJump requires key-based authentication")

                from .proxy_client import create_proxy_aware_connection

                self.ssh_client, self.proxy_client = create_proxy_aware_connection(
                    hostname=self.hostname,
                    username=self.username,
                    key_filename=self.key_filename,
                    port=self.port,
                    proxy_jump=self.proxy_jump,
                    ssh_config_path=self.ssh_config_path,
                    logger=self.logger,
                )
            else:
                import paramiko  # type: ignore

                self.ssh_client = paramiko.SSHClient()
                self.ssh_client.load_system_host_keys()
                self.ssh_client.set_missing_host_key_policy(paramiko.WarningPolicy())

                if self.key_filename:
                    self.ssh_client.connect(
                        hostname=self.hostname,
                        username=self.username,
                        key_filename=self.key_filename,
                        port=self.port,
                    )
                else:
                    self.ssh_client.connect(
                        hostname=self.hostname,
                        username=self.username,
                        password=self.password,
                        port=self.port,
                    )

            self.sftp_client = self.ssh_client.open_sftp()
            self.execute_command(f"mkdir -p {shlex.quote(self.temp_dir)}")

            # Initialize SLURM paths and environment
            self._initialize_slurm_paths()

            # Final verification of SLURM setup
            self._verify_slurm_setup()

            connection_info = f"{self.hostname}"
            if self.proxy_jump:
                connection_info += f" (via {self.proxy_jump})"
            if self.verbose:
                self.logger.info(f"Successfully connected to {connection_info}")
            return True

        except Exception as e:
            self.logger.error(f"Failed to connect to {self.hostname}: {e}")
            return False

    def disconnect(self):
        if self.sftp_client:
            self.sftp_client.close()
            self.sftp_client = None
        if self.ssh_client:
            self.ssh_client.close()
            self.ssh_client = None
        if self.proxy_client:
            self.proxy_client.close_proxy()
            self.proxy_client = None

        info = self.hostname + (f" (via {self.proxy_jump})" if self.proxy_jump else "")
        self.logger.info(f"Disconnected from {info}")

    def test_connection(self) -> dict[str, str | bool]:
        result: dict[str, str | bool] = {
            "ssh_connected": False,
            "slurm_available": False,
            "hostname": "",
            "user": "",
            "slurm_version": "",
        }

        try:
            if not self.connect():
                result["error"] = "Failed to establish SSH connection"
                return result

            result["ssh_connected"] = True

            stdout_data, stderr_data, exit_code = self.execute_command("hostname")
            result["hostname"] = stdout_data.strip()

            stdout_data, stderr_data, exit_code = self.execute_command("whoami")
            result["user"] = stdout_data.strip()

            return result

        except Exception as e:
            result["error"] = str(e)
            return result
        finally:
            self.disconnect()

    def execute_command(self, command: str) -> tuple[str, str, int]:
        if not self.ssh_client:
            raise ConnectionError("SSH client is not connected")

        stdin, stdout, stderr = self.ssh_client.exec_command(command)
        stdout_data = stdout.read().decode("utf-8", errors="replace")
        stderr_data = stderr.read().decode("utf-8", errors="replace")
        exit_code = stdout.channel.recv_exit_status()

        return stdout_data, stderr_data, exit_code

    def _execute_with_environment(self, command: str) -> tuple[str, str, int]:
        """Execute command with full login environment."""
        return self.execute_command(f"bash -l -c {shlex.quote(command)}")

    def __enter__(self):
        if self.connect():
            return self
        else:
            raise ConnectionError("Failed to establish SSH connection")

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()

    # ==================================================================
    # File operations
    # ==================================================================

    def upload_file(self, local_path: str, remote_path: str | None = None) -> str:
        if not self.sftp_client:
            raise ConnectionError("SFTP client is not connected")

        local_path_obj = Path(local_path)
        if not local_path_obj.exists():
            raise FileNotFoundError(f"Local file not found: {local_path}")

        if remote_path is None:
            unique_id = str(uuid.uuid4())[:8]
            remote_filename = (
                f"{local_path_obj.stem}_{unique_id}{local_path_obj.suffix}"
            )
            remote_path = f"{self.temp_dir}/{remote_filename}"

        try:
            self.sftp_client.put(str(local_path_obj), remote_path)
            if local_path_obj.suffix in [".sh", ".py", ".pl", ".r"]:
                self.execute_command(f"chmod +x {shlex.quote(remote_path)}")
            if self.verbose:
                self.logger.info(f"Uploaded {local_path} to {remote_path}")
            return remote_path
        except Exception as e:
            self.logger.error(f"Failed to upload file: {e}")
            raise

    def cleanup_file(self, remote_path: str) -> None:
        try:
            self.execute_command(f"rm -f {shlex.quote(remote_path)}")
            if self.verbose:
                self.logger.info(f"Cleaned up remote file: {remote_path}")
        except Exception as e:
            self.logger.warning(f"Failed to cleanup file {remote_path}: {e}")

    def file_exists(self, remote_path: str) -> bool:
        stdout, stderr, exit_code = self.execute_command(
            f"test -f {shlex.quote(remote_path)} && echo 'exists' || echo 'not_found'"
        )
        exists = stdout.strip() == "exists"
        self.logger.debug(f"File existence check for {remote_path}: {exists}")
        return exists

    def validate_remote_script(self, remote_path: str) -> tuple[bool, str]:
        if not self.file_exists(remote_path):
            return False, f"Remote script file not found: {remote_path}"

        quoted_path = shlex.quote(remote_path)
        stdout, stderr, exit_code = self.execute_command(
            f"test -r {quoted_path} && echo 'readable' || echo 'not_readable'"
        )
        if stdout.strip() != "readable":
            return False, f"Remote script file is not readable: {remote_path}"

        stdout, stderr, exit_code = self.execute_command(
            f"test -x {quoted_path} && echo 'executable' || echo 'not_executable'"
        )
        if stdout.strip() != "executable":
            self.logger.warning(
                f"Remote script file is not executable: {remote_path}. SLURM may fail to run it."
            )

        stdout, stderr, exit_code = self.execute_command(
            f"wc -c < {quoted_path} 2>/dev/null || echo '0'"
        )
        try:
            file_size = int(stdout.strip())
            if file_size == 0:
                self.logger.warning(f"Remote script file is empty: {remote_path}")
            elif file_size > 1024 * 1024:
                self.logger.warning(
                    f"Remote script file is very large ({file_size} bytes): {remote_path}"
                )
            self.logger.debug(f"Remote script file size: {file_size} bytes")
        except ValueError:
            self.logger.warning(f"Could not determine file size for: {remote_path}")

        if remote_path.endswith(".sh"):
            stdout, stderr, exit_code = self.execute_command(
                f"bash -n {quoted_path} 2>&1 || echo 'SYNTAX_ERROR'"
            )
            if "SYNTAX_ERROR" in stdout or exit_code != 0:
                return (
                    False,
                    f"Shell script syntax error in {remote_path}: {stdout.strip()}",
                )
            self.logger.debug(f"Shell script syntax check passed for {remote_path}")

        return True, "Script validation successful"

    def _write_remote_file(self, remote_path: str, content: str) -> None:
        if not self.sftp_client:
            raise ConnectionError("SFTP client is not connected")
        with self.sftp_client.open(remote_path, "w") as f:
            f.write(content)

    def sync_project(
        self,
        local_path: str | None = None,
        remote_path: str | None = None,
        *,
        delete: bool = True,
        dry_run: bool = False,
        exclude_patterns: list[str] | None = None,
    ) -> str:
        if self._rsync_client is None:
            raise RuntimeError(
                "sync_project() requires key-based SSH auth and rsync installed locally"
            )

        if local_path is None:
            local_path = self._detect_project_root()

        if remote_path is None:
            remote_path = RsyncClient.get_default_remote_path(local_path)

        result = self._rsync_client.push(
            local_path,
            remote_path,
            delete=delete,
            dry_run=dry_run,
            exclude_patterns=exclude_patterns,
        )

        if not result.success:
            raise RuntimeError(
                f"rsync failed (exit {result.returncode}): {result.stderr}"
            )

        if self.verbose:
            self.logger.info(f"Project synced to {self.hostname}:{remote_path}")

        return remote_path

    @staticmethod
    def _detect_project_root() -> str:
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

    # ==================================================================
    # SLURM operations
    # ==================================================================

    def _initialize_slurm_paths(self) -> None:
        try:
            stdout, stderr, exit_code = self.execute_command(
                "bash -l -c 'echo $PATH' 2>/dev/null || echo ''"
            )

            if stdout.strip():
                login_path = stdout.strip()
                self.logger.debug(f"Login shell PATH: {login_path}")

                stdout, stderr, exit_code = self.execute_command(
                    "bash -l -c 'which sbatch' 2>/dev/null || echo 'NOT_FOUND'"
                )

                self.logger.debug(
                    f"SLURM which command result: stdout='{stdout}', stderr='{stderr}', exit_code={exit_code}"
                )

                if exit_code == 0 and "NOT_FOUND" not in stdout:
                    sbatch_path = stdout.strip()
                    self._slurm_path = sbatch_path.rsplit("/", 1)[0]
                    if self.verbose:
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
                stdout, stderr, exit_code = self.execute_command(
                    f"test -f {path}/sbatch && echo 'FOUND' || echo 'NOT_FOUND'"
                )
                self.logger.debug(f"Checking {path}: {stdout.strip()}")
                if "FOUND" in stdout:
                    self._slurm_path = path
                    if self.verbose:
                        self.logger.info(f"Found SLURM at: {self._slurm_path}")
                    stdout, stderr, exit_code = self.execute_command(
                        f"test -x {path}/sbatch && echo 'EXECUTABLE' || echo 'NOT_EXECUTABLE'"
                    )
                    self.logger.debug(f"SLURM executable check: {stdout.strip()}")
                    return

            self.logger.warning("SLURM commands not found in standard locations")

        except Exception as e:
            self.logger.warning(f"Failed to initialize SLURM paths: {e}")

    def _get_slurm_command(self, command: str) -> str:
        if self._slurm_path:
            return f"{self._slurm_path}/{command}"
        else:
            return command

    def _execute_slurm_command(self, command: str) -> tuple[str, str, int]:
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
        stdout, stderr, exit_code = self.execute_command(full_cmd)
        self.logger.debug(
            f"SLURM command result: exit_code={exit_code}, stdout_len={len(stdout)}, stderr_len={len(stderr)}"
        )
        if stderr and exit_code != 0:
            self.logger.debug(f"SLURM command stderr: {stderr[:500]}...")
        return stdout, stderr, exit_code

    # Public alias
    execute_slurm_command = _execute_slurm_command

    def _get_slurm_env_setup(self) -> str:
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

        for key, value in self.custom_env_vars.items():
            escaped_value = value.replace("'", "'\\''")
            env_commands.append(f"export {key}='{escaped_value}'")
            self.logger.debug(f"Adding custom environment variable: {key}={value}")

        return " && ".join(env_commands)

    def _verify_slurm_setup(self) -> None:
        try:
            test_cmd = (
                "sbatch --version 2>/dev/null | head -1 || echo 'SLURM_NOT_AVAILABLE'"
            )
            stdout, stderr, exit_code = self._execute_slurm_command(test_cmd)

            if exit_code == 0 and "SLURM_NOT_AVAILABLE" not in stdout:
                if self.verbose:
                    self.logger.info(f"SLURM verification successful: {stdout.strip()}")
            else:
                self.logger.warning(f"SLURM verification failed: {stdout} / {stderr}")
        except Exception as e:
            self.logger.warning(f"SLURM verification error: {e}")

    def submit_sbatch_job(
        self,
        script_content: str,
        job_name: str | None = None,
        dependency: str | None = None,
    ) -> SlurmJob | None:
        try:
            unique_id = str(uuid.uuid4())[:8]
            remote_script_path = f"{self.temp_dir}/job_{unique_id}.sh"
            self._write_remote_file(remote_script_path, script_content)

            self.execute_command(f"chmod +x {shlex.quote(remote_script_path)}")

            valid, validation_msg = self.validate_remote_script(remote_script_path)
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

            stdout, stderr, exit_code = self._execute_slurm_command(cmd)
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
        try:
            path_obj = Path(script_path)
            if path_obj.exists():
                remote_path = self.upload_file(script_path)

                if not job_name:
                    job_name = path_obj.stem

                safe_name = re.sub(r"[^a-zA-Z0-9_.-]", "_", job_name)
                cmd = f"{self._get_slurm_command('sbatch')}"
                cmd += f" -J {safe_name}"
                cmd += " -o $SLURM_LOG_DIR/%x_%j.log"
                cmd += f" {remote_path}"

                stdout, stderr, exit_code = self._execute_slurm_command(cmd)
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
                valid, validation_msg = self.validate_remote_script(script_path)
                if not valid:
                    self.logger.error(
                        f"Remote script validation failed: {validation_msg}"
                    )
                    return None

                if not job_name:
                    job_name = Path(script_path).stem

                safe_name = re.sub(r"[^a-zA-Z0-9_.-]", "_", job_name)
                cmd = f"{self._get_slurm_command('sbatch')}"
                cmd += f" -J {safe_name}"
                cmd += " -o $SLURM_LOG_DIR/%x_%j.log"
                cmd += f" {shlex.quote(script_path)}"

                stdout, stderr, exit_code = self._execute_slurm_command(cmd)
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

    def cleanup_job_files(self, job: SlurmJob) -> None:
        if job.is_local_script and job.script_path and job._cleanup:
            self.cleanup_file(job.script_path)

    def get_job_status(self, job_id: str) -> str:
        try:
            if not re.match(r"^[0-9]+([._][A-Za-z0-9_-]+)?$", job_id):
                self.logger.error(f"Invalid job_id format: {job_id!r}")
                return "ERROR"

            sacct_cmd = f"sacct -j {job_id} --format=JobID,State --noheader | grep -E '^[0-9]+' | head -1"
            stdout, stderr, exit_code = self._execute_slurm_command(sacct_cmd)

            if exit_code == 0 and stdout.strip():
                status = stdout.strip().split()[1].split("+")[0]
                return status

            squeue_cmd = f"squeue -j {job_id} -h -o %T | head -1"
            stdout, stderr, exit_code = self._execute_slurm_command(squeue_cmd)
            if exit_code == 0 and stdout.strip():
                return stdout.strip().split("\n")[0].strip()

            return "NOT_FOUND"

        except Exception as e:
            self.logger.error(f"Failed to get job status for job {job_id}: {e}")
            return "ERROR"

    def monitor_job(
        self, job: SlurmJob, poll_interval: int = 10, timeout: int | None = None
    ) -> SlurmJob:
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

    def _handle_slurm_error(
        self, command: str, error_output: str, exit_code: int
    ) -> None:
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

    # ==================================================================
    # Log retrieval
    # ==================================================================

    def get_job_output(
        self,
        job_id: str,
        job_name: str | None = None,
        stdout_offset: int = 0,
        stderr_offset: int = 0,
    ) -> tuple[str, str, int, int]:
        try:
            safe_job_id = self._sanitize_job_id(job_id)
            safe_job_name = None
            if job_name and re.fullmatch(r"[\w\-\.]+", job_name):
                safe_job_name = job_name

            output_content = ""
            error_content = ""
            new_stdout_offset = stdout_offset
            new_stderr_offset = stderr_offset

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

            out, err = self._get_job_output_by_pattern(safe_job_id, safe_job_name)
            return out, err, len(out.encode()), len(err.encode())

        except Exception as e:
            self.logger.error(f"Failed to get job output for {job_id}: {e}")
            return "", "", stdout_offset, stderr_offset

    def _read_file_from_offset(self, path: str, offset: int) -> tuple[str, int]:
        quoted = shlex.quote(path)
        if offset > 0:
            out, _, rc = self.execute_command(
                f"tail -c +{offset + 1} {quoted} 2>/dev/null"
            )
        else:
            out, _, rc = self.execute_command(f"cat {quoted} 2>/dev/null")
        if rc != 0:
            return "", offset

        size_out, _, src = self.execute_command(f"wc -c < {quoted} 2>/dev/null")
        if src == 0 and size_out.strip().isdigit():
            new_offset = int(size_out.strip())
        else:
            new_offset = offset + len(out.encode())
        return out, new_offset

    def _get_log_paths_from_scontrol(
        self, job_id: str
    ) -> tuple[str | None, str | None]:
        quoted_id = shlex.quote(job_id)
        stdout, _, rc = self._execute_slurm_command(
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
            quoted_dir = self._quote_shell_path(log_dir)
            for pattern in patterns:
                quoted_pattern = shlex.quote(pattern)
                find_cmd = f"find {quoted_dir} -name {quoted_pattern} -type f 2>/dev/null | head -5"
                stdout, stderr, exit_code = self.execute_command(find_cmd)

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
            stdout_output, _, _ = self.execute_command(
                f"cat {quoted_log} 2>/dev/null || echo 'Could not read log file'"
            )
            output_content = stdout_output

            if len(found_files) > 1:
                for log_file in found_files[1:]:
                    if "err" in log_file.lower() or "error" in log_file.lower():
                        quoted_err_log = shlex.quote(log_file)
                        stderr_output, _, _ = self.execute_command(
                            f"cat {quoted_err_log} 2>/dev/null || echo ''"
                        )
                        error_content += stderr_output

            if self.verbose:
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
                stdout_output, _, _ = self.execute_command(
                    f"cat {quoted_pattern} 2>/dev/null || echo ''"
                )
                output_content += stdout_output

        return output_content, error_content

    def get_job_output_detailed(
        self, job_id: str, job_name: str | None = None
    ) -> dict[str, str | list[str] | None]:
        try:
            safe_job_id = self._sanitize_job_id(job_id)
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
                quoted_dir = self._quote_shell_path(log_dir)
                for pattern in patterns:
                    quoted_pattern = shlex.quote(pattern)
                    find_cmd = f"find {quoted_dir} -name {quoted_pattern} -type f 2>/dev/null | head -5"
                    stdout, stderr, exit_code = self.execute_command(find_cmd)

                    if exit_code == 0 and stdout.strip():
                        log_files = stdout.strip().split("\n")
                        for log_file in log_files:
                            if log_file.strip():
                                found_files.append(log_file.strip())

            found_files = list(set(found_files))
            if found_files:
                primary_log = found_files[0]
                quoted_log = shlex.quote(primary_log)
                stdout_output, _, _ = self.execute_command(
                    f"cat {quoted_log} 2>/dev/null || echo 'Could not read log file'"
                )
                output_content = stdout_output

                if len(found_files) > 1:
                    for log_file in found_files[1:]:
                        if "err" in log_file.lower() or "error" in log_file.lower():
                            quoted_err_log = shlex.quote(log_file)
                            stderr_output, _, _ = self.execute_command(
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

    # ==================================================================
    # Static utilities (backward compatibility)
    # ==================================================================

    @staticmethod
    def _quote_shell_path(path: str) -> str:
        return quote_shell_path(path)

    @staticmethod
    def _sanitize_job_id(job_id: str) -> str:
        return sanitize_job_id(job_id)
