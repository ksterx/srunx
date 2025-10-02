import logging
import os
import re
import time
import uuid
from dataclasses import dataclass
from pathlib import Path

import paramiko  # type: ignore

from .proxy_client import ProxySSHClient, create_proxy_aware_connection


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


class SSHSlurmClient:
    def __init__(
        self,
        hostname: str,
        username: str,
        password: str | None = None,
        key_filename: str | None = None,
        port: int = 22,
        proxy_jump: str | None = None,
        ssh_config_path: str | None = None,
        env_vars: dict | None = None,
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
        self.logger = logging.getLogger(__name__)
        self.temp_dir = os.getenv("SRUNX_TEMP_DIR", "/tmp/srunx")
        self._slurm_path: str | None = None  # Cache for SLURM command paths
        self._slurm_env: dict[str, str] | None = (
            None  # Cache for SLURM environment variables
        )
        self.custom_env_vars = env_vars or {}  # Custom environment variables to pass
        self.verbose = verbose  # Control detailed logging  # Control detailed logging

    def connect(self) -> bool:
        try:
            if self.proxy_jump:
                # Use ProxyJump connection
                if not self.key_filename:
                    raise ValueError("ProxyJump requires key-based authentication")

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
                # Direct connection
                self.ssh_client = paramiko.SSHClient()
                self.ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

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

            # Create SFTP client for file transfers
            self.sftp_client = self.ssh_client.open_sftp()

            # Create temp directory on server
            self.execute_command(f"mkdir -p {self.temp_dir}")

            # Initialize SLURM paths and environment
            self._initialize_slurm_paths()
            self._initialize_slurm_environment()

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
        """Test SSH connection and SLURM availability.

        Returns:
            Dictionary with test results including:
            - ssh_connected: Whether SSH connection succeeded
            - slurm_available: Whether SLURM commands are available
            - hostname: Remote hostname
            - user: Remote username
            - slurm_version: SLURM version if available
            - error: Error message if connection failed
        """
        result: dict[str, str | bool] = {
            "ssh_connected": False,
            "slurm_available": False,
            "hostname": "",
            "user": "",
            "slurm_version": "",
        }

        try:
            # Test SSH connection
            if not self.connect():
                result["error"] = "Failed to establish SSH connection"
                return result

            result["ssh_connected"] = True

            # Get hostname
            stdout_data, stderr_data, exit_code = self.execute_command("hostname")
            result["hostname"] = stdout_data.strip()

            # Get username
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
        stdout_data = stdout.read().decode("utf-8")
        stderr_data = stderr.read().decode("utf-8")
        exit_code = stdout.channel.recv_exit_status()

        return stdout_data, stderr_data, exit_code

    def upload_file(self, local_path: str, remote_path: str | None = None) -> str:
        """Upload a local file to the server and return the remote path"""
        if not self.sftp_client:
            raise ConnectionError("SFTP client is not connected")

        local_path_obj = Path(local_path)
        if not local_path_obj.exists():
            raise FileNotFoundError(f"Local file not found: {local_path}")

        if remote_path is None:
            # Generate unique remote path in temp directory
            unique_id = str(uuid.uuid4())[:8]
            remote_filename = (
                f"{local_path_obj.stem}_{unique_id}{local_path_obj.suffix}"
            )
            remote_path = f"{self.temp_dir}/{remote_filename}"

        try:
            self.sftp_client.put(str(local_path_obj), remote_path)
            # Make the uploaded script executable if it's a script
            if local_path_obj.suffix in [".sh", ".py", ".pl", ".r"]:
                self.execute_command(f"chmod +x {remote_path}")
            if self.verbose:
                self.logger.info(f"Uploaded {local_path} to {remote_path}")
            return remote_path
        except Exception as e:
            self.logger.error(f"Failed to upload file: {e}")
            raise

    def cleanup_file(self, remote_path: str) -> None:
        """Remove a file from the server"""
        try:
            self.execute_command(f"rm -f {remote_path}")
            if self.verbose:
                self.logger.info(f"Cleaned up remote file: {remote_path}")
        except Exception as e:
            self.logger.warning(f"Failed to cleanup file {remote_path}: {e}")

    def file_exists(self, remote_path: str) -> bool:
        """Check if a file exists on the server"""
        stdout, stderr, exit_code = self.execute_command(
            f"test -f {remote_path} && echo 'exists' || echo 'not_found'"
        )
        exists = stdout.strip() == "exists"
        self.logger.debug(f"File existence check for {remote_path}: {exists}")
        return exists

    def validate_remote_script(self, remote_path: str) -> tuple[bool, str]:
        """Validate a remote script file and return (is_valid, error_message)"""
        # Check if file exists
        if not self.file_exists(remote_path):
            return False, f"Remote script file not found: {remote_path}"

        # Check if file is readable
        stdout, stderr, exit_code = self.execute_command(
            f"test -r {remote_path} && echo 'readable' || echo 'not_readable'"
        )
        if stdout.strip() != "readable":
            return False, f"Remote script file is not readable: {remote_path}"

        # Check if file is executable (warn if not)
        stdout, stderr, exit_code = self.execute_command(
            f"test -x {remote_path} && echo 'executable' || echo 'not_executable'"
        )
        if stdout.strip() != "executable":
            self.logger.warning(
                f"Remote script file is not executable: {remote_path}. SLURM may fail to run it."
            )

        # Check file size (warn if empty or too large)
        stdout, stderr, exit_code = self.execute_command(
            f"wc -c < {remote_path} 2>/dev/null || echo '0'"
        )
        try:
            file_size = int(stdout.strip())
            if file_size == 0:
                self.logger.warning(f"Remote script file is empty: {remote_path}")
            elif file_size > 1024 * 1024:  # 1MB
                self.logger.warning(
                    f"Remote script file is very large ({file_size} bytes): {remote_path}"
                )

            self.logger.debug(f"Remote script file size: {file_size} bytes")
        except ValueError:
            self.logger.warning(f"Could not determine file size for: {remote_path}")

        # Basic syntax check for shell scripts
        if remote_path.endswith(".sh"):
            stdout, stderr, exit_code = self.execute_command(
                f"bash -n {remote_path} 2>&1 || echo 'SYNTAX_ERROR'"
            )
            if "SYNTAX_ERROR" in stdout or exit_code != 0:
                return (
                    False,
                    f"Shell script syntax error in {remote_path}: {stdout.strip()}",
                )
            self.logger.debug(f"Shell script syntax check passed for {remote_path}")

        return True, "Script validation successful"

    def _initialize_slurm_paths(self) -> None:
        """Initialize SLURM command paths by checking common locations and environment"""
        try:
            # Try with login shell environment
            stdout, stderr, exit_code = self.execute_command(
                "bash -l -c 'echo $PATH' 2>/dev/null || echo ''"
            )

            if stdout.strip():
                login_path = stdout.strip()
                self.logger.debug(f"Login shell PATH: {login_path}")

                # Test if SLURM commands are available with login shell
                stdout, stderr, exit_code = self.execute_command(
                    "bash -l -c 'which sbatch' 2>/dev/null || echo 'NOT_FOUND'"
                )

                self.logger.debug(
                    f"SLURM which command result: stdout='{stdout}', stderr='{stderr}', exit_code={exit_code}"
                )

                if exit_code == 0 and "NOT_FOUND" not in stdout:
                    sbatch_path = stdout.strip()
                    self._slurm_path = sbatch_path.rsplit("/", 1)[0]  # Get directory
                    if self.verbose:
                        self.logger.info(f"Found SLURM at: {self._slurm_path}")
                        self.logger.debug(f"Full sbatch path: {sbatch_path}")
                    return

            # Fallback: Check common SLURM installation paths
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
                    # Verify permissions
                    stdout, stderr, exit_code = self.execute_command(
                        f"test -x {path}/sbatch && echo 'EXECUTABLE' || echo 'NOT_EXECUTABLE'"
                    )
                    self.logger.debug(f"SLURM executable check: {stdout.strip()}")
                    return

            self.logger.warning("SLURM commands not found in standard locations")

        except Exception as e:
            self.logger.warning(f"Failed to initialize SLURM paths: {e}")

    def _initialize_slurm_environment(self) -> None:
        """Capture the actual working SLURM environment"""
        try:
            # Test if sbatch works with full environment setup
            env_setup = self._get_slurm_env_setup()
            test_cmd = f"{env_setup} && which sbatch 2>/dev/null || echo 'NOT_FOUND'"

            stdout, stderr, exit_code = self.execute_command(f"bash -l -c '{test_cmd}'")

            self.logger.debug(
                f"SLURM verification test: stdout='{stdout}', stderr='{stderr}', exit_code={exit_code}"
            )

            if exit_code == 0 and "NOT_FOUND" not in stdout and stdout.strip():
                sbatch_location = stdout.strip()
                if self.verbose:
                    self.logger.info(f"SLURM sbatch verified at: {sbatch_location}")

                # Try to get SLURM environment variables after setup
                env_cmd = (
                    f"{env_setup} && env | grep -E '^(SLURM|CLUSTER|PATH)' | head -20"
                )
                stdout, stderr, exit_code = self.execute_command(
                    f"bash -l -c '{env_cmd}'"
                )

                if exit_code == 0 and stdout.strip():
                    # Parse environment variables
                    env_vars = {}
                    for line in stdout.strip().split("\n"):
                        if "=" in line:
                            key, value = line.split("=", 1)
                            env_vars[key] = value
                    self._slurm_env = env_vars
                    if self.verbose:
                        self.logger.info(
                            f"Captured SLURM environment with {len(env_vars)} variables"
                        )
                        self.logger.debug(
                            f"Environment variables: {list(env_vars.keys())}"
                        )
            else:
                self.logger.warning(
                    f"SLURM sbatch test failed. stdout: {stdout}, stderr: {stderr}"
                )

        except Exception as e:
            self.logger.warning(f"Failed to initialize SLURM environment: {e}")

    def _get_slurm_command(self, command: str) -> str:
        """Get the full path for a SLURM command, or use login shell if path not found"""
        if self._slurm_path:
            return f"{self._slurm_path}/{command}"
        else:
            # Fallback to bash login shell - command will be handled in _execute_slurm_command
            return command

    def _execute_slurm_command(self, command: str) -> tuple[str, str, int]:
        """Execute a SLURM command with proper environment"""
        # Always use full login shell environment with proper SLURM command path
        env_setup = self._get_slurm_env_setup()

        # Build the command with full path if available
        if self._slurm_path:
            # Replace SLURM commands with full paths
            slurm_commands = ["sbatch", "squeue", "sacct", "scancel", "sinfo"]
            modified_command = command
            for cmd in slurm_commands:
                if modified_command.startswith(cmd + " ") or modified_command == cmd:
                    modified_command = modified_command.replace(
                        cmd, f"{self._slurm_path}/{cmd}", 1
                    )
                    break
            final_command = f"{env_setup} && {modified_command}"
        else:
            # Use login shell to find commands in PATH
            final_command = f"{env_setup} && {command}"

        # Execute with bash login shell
        full_cmd = f"bash -l -c '{final_command}'"
        self.logger.debug(f"Executing SLURM command: {full_cmd}")
        stdout, stderr, exit_code = self.execute_command(full_cmd)
        self.logger.debug(
            f"SLURM command result: exit_code={exit_code}, stdout_len={len(stdout)}, stderr_len={len(stderr)}"
        )
        if stderr and exit_code != 0:
            self.logger.debug(
                f"SLURM command stderr: {stderr[:500]}..."
            )  # First 500 chars
        return stdout, stderr, exit_code

    def _get_slurm_env_setup(self) -> str:
        """Get environment setup commands for SLURM execution"""
        # Use the exact same environment setup as an interactive login shell
        env_commands = [
            "cd ~",
            # Fully simulate login shell environment
            "source /etc/profile 2>/dev/null || true",
            "source ~/.bash_profile 2>/dev/null || true",
            "source ~/.bashrc 2>/dev/null || true",
            "source ~/.profile 2>/dev/null || true",
            # Try multiple methods to load SLURM environment
            "module load slurm 2>/dev/null || true",
            "module load slurm/current 2>/dev/null || true",
            # Check if sbatch is in PATH after module loads
            'which sbatch >/dev/null 2>&1 || export PATH="$PATH:/cm/shared/apps/slurm/current/bin" 2>/dev/null || true',
            # Final fallback - add known SLURM paths
            'export PATH="$PATH:/usr/local/bin:/opt/slurm/bin:/cluster/slurm/bin" 2>/dev/null || true',
        ]

        # Add custom environment variables
        for key, value in self.custom_env_vars.items():
            # Use single quotes to avoid shell interpretation
            escaped_value = value.replace("'", "'\\''")
            env_commands.append(f"export {key}='{escaped_value}'")
            self.logger.debug(f"Adding custom environment variable: {key}={value}")

        return " && ".join(env_commands)

    def _verify_slurm_setup(self) -> None:
        """Verify that SLURM commands are working properly"""
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
        self, script_content: str, job_name: str | None = None
    ) -> SlurmJob | None:
        """Submit an sbatch job with script content"""
        try:
            # Create temporary script file on server
            unique_id = str(uuid.uuid4())[:8]
            remote_script_path = f"{self.temp_dir}/job_{unique_id}.sh"
            self._write_remote_file(remote_script_path, script_content)

            # Make script executable
            self.execute_command(f"chmod +x {remote_script_path}")

            # Validate the uploaded script
            valid, validation_msg = self.validate_remote_script(remote_script_path)
            if not valid:
                self.logger.error(f"Script validation failed: {validation_msg}")
                return None

            # Submit the job
            cmd = f"{self._get_slurm_command('sbatch')}"
            if job_name:
                safe_name = re.sub(r"[^a-zA-Z0-9_.-]", "_", job_name)
                cmd += f" --job-name={safe_name}"
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
        """Submit an sbatch job from a local or remote file"""
        try:
            path_obj = Path(script_path)
            if path_obj.exists():
                # Local file: upload and submit
                remote_path = self.upload_file(script_path)
                job = self.submit_sbatch_job(
                    f"#!/bin/bash\n{remote_path}", job_name=job_name
                )
                if job:
                    job.script_path = remote_path
                    job.is_local_script = True
                    job._cleanup = cleanup
                return job
            else:
                # Remote file: submit directly on server
                # Validate remote file
                valid, validation_msg = self.validate_remote_script(script_path)
                if not valid:
                    self.logger.error(
                        f"Remote script validation failed: {validation_msg}"
                    )
                    return None

                # Submit the job
                cmd = f"{self._get_slurm_command('sbatch')}"
                if job_name:
                    safe_name = re.sub(r"[^a-zA-Z0-9_.-]", "_", job_name)
                    cmd += f" --job-name={safe_name}"
                cmd += f" {script_path}"

                stdout, stderr, exit_code = self._execute_slurm_command(cmd)
                if exit_code == 0:
                    match = re.search(r"Submitted batch job (\d+)", stdout)
                    if match:
                        job_id = match.group(1)
                        job = SlurmJob(job_id=job_id, name=job_name or f"job_{job_id}")
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
        """Cleanup temporary files for a job if it was a local script"""
        if job.is_local_script and job.script_path and job._cleanup:
            self.cleanup_file(job.script_path)

    def get_job_status(self, job_id: str) -> str:
        """Get job status using SLURM commands"""
        try:
            # Validate job_id to prevent command injection
            # Allow numeric job IDs with optional step/array components (e.g., 12345, 12345_1, 12345.batch)
            if not re.match(r"^[0-9]+([._][A-Za-z0-9_-]+)?$", job_id):
                self.logger.error(f"Invalid job_id format: {job_id!r}")
                return "ERROR"

            # Try sacct for completed jobs, squeue for running/pending
            # Do NOT use _get_slurm_command here; _execute_slurm_command will prepend the path safely
            sacct_cmd = f"sacct -j {job_id} --format=JobID,State --noheader | grep -E '^[0-9]+' | head -1"
            stdout, stderr, exit_code = self._execute_slurm_command(sacct_cmd)

            if exit_code == 0 and stdout.strip():
                status = stdout.strip().split()[1].split("+")[0]
                return status

            # Fallback to squeue
            squeue_cmd = f"squeue -j {job_id} -h -o %T | head -1"
            stdout, stderr, exit_code = self._execute_slurm_command(squeue_cmd)
            if exit_code == 0 and stdout.strip():
                return stdout.strip().split("\n")[0].strip()

            return "NOT_FOUND"

        except Exception as e:
            self.logger.error(f"Failed to get job status for job {job_id}: {e}")
            return "ERROR"

    def get_job_output(
        self, job_id: str, job_name: str | None = None
    ) -> tuple[str, str]:
        """Get job output from SLURM log files, supporting various log file naming patterns"""
        try:
            # Try multiple common SLURM log file patterns
            potential_log_patterns = [
                # Pattern from SBATCH directives: %x_%j.log (job_name_job_id.log)
                f"{job_name}_{job_id}.log" if job_name else None,
                # Common SLURM_LOG_DIR patterns
                f"*_{job_id}.log",
                # Default SLURM patterns
                f"slurm-{job_id}.out",
                f"slurm-{job_id}.err",
                # Alternative patterns
                f"job_{job_id}.log",
                f"{job_id}.log",
                f"*_{job_id}.out",
            ]

            # Remove None values
            patterns = [p for p in potential_log_patterns if p is not None]

            # Common SLURM log directories to search
            log_dirs = [
                os.environ.get("SLURM_LOG_DIR", "~/logs/slurm"),
                "./",  # Current directory
                "/tmp",
                "/var/log/slurm",
            ]

            output_content = ""
            error_content = ""
            found_files = []

            for log_dir in log_dirs:
                for pattern in patterns:
                    # Try to find log files with this pattern
                    find_cmd = f"find {log_dir} -name '{pattern}' -type f 2>/dev/null | head -5"
                    stdout, stderr, exit_code = self.execute_command(find_cmd)

                    if exit_code == 0 and stdout.strip():
                        log_files = stdout.strip().split("\n")
                        for log_file in log_files:
                            if log_file.strip():
                                found_files.append(log_file.strip())
                                self.logger.debug(
                                    f"Found potential log file: {log_file.strip()}"
                                )

            # Read content from found log files
            if found_files:
                # Use the first found file as primary output
                primary_log = found_files[0]
                stdout_output, _, _ = self.execute_command(
                    f"cat '{primary_log}' 2>/dev/null || echo 'Could not read log file'"
                )
                output_content = stdout_output

                # If there are separate error files or multiple files, try to distinguish
                if len(found_files) > 1:
                    for log_file in found_files[1:]:
                        if "err" in log_file.lower() or "error" in log_file.lower():
                            stderr_output, _, _ = self.execute_command(
                                f"cat '{log_file}' 2>/dev/null || echo ''"
                            )
                            error_content += stderr_output

                if self.verbose:
                    self.logger.info(
                        f"Found {len(found_files)} log file(s) for job {job_id}"
                    )
                    self.logger.debug(f"Primary log file: {primary_log}")
            else:
                # Fallback to default SLURM patterns in current directory
                self.logger.warning(
                    f"No log files found for job {job_id} using common patterns"
                )

                # Try default patterns as fallback
                default_patterns = [f"slurm-{job_id}.out", f"slurm-{job_id}.err"]
                for pattern in default_patterns:
                    stdout_output, _, _ = self.execute_command(
                        f"cat {pattern} 2>/dev/null || echo ''"
                    )
                    if "out" in pattern:
                        output_content += stdout_output
                    else:
                        error_content += stdout_output

            return output_content, error_content

        except Exception as e:
            self.logger.error(f"Failed to get job output for {job_id}: {e}")
            return "", ""

    def get_job_output_detailed(
        self, job_id: str, job_name: str | None = None
    ) -> dict[str, str | list[str] | None]:
        """Get detailed job output information including found log files"""
        try:
            # Try multiple common SLURM log file patterns
            potential_log_patterns = [
                # Pattern from SBATCH directives: %x_%j.log (job_name_job_id.log)
                f"{job_name}_{job_id}.log" if job_name else None,
                # Common SLURM_LOG_DIR patterns
                f"*_{job_id}.log",
                # Default SLURM patterns
                f"slurm-{job_id}.out",
                f"slurm-{job_id}.err",
                # Alternative patterns
                f"job_{job_id}.log",
                f"{job_id}.log",
                f"*_{job_id}.out",
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
                for pattern in patterns:
                    find_cmd = f"find {log_dir} -name '{pattern}' -type f 2>/dev/null | head -5"
                    stdout, stderr, exit_code = self.execute_command(find_cmd)

                    if exit_code == 0 and stdout.strip():
                        log_files = stdout.strip().split("\n")
                        for log_file in log_files:
                            if log_file.strip():
                                found_files.append(log_file.strip())

            if found_files:
                primary_log = found_files[0]
                stdout_output, _, _ = self.execute_command(
                    f"cat '{primary_log}' 2>/dev/null || echo 'Could not read log file'"
                )
                output_content = stdout_output

                if len(found_files) > 1:
                    for log_file in found_files[1:]:
                        if "err" in log_file.lower() or "error" in log_file.lower():
                            stderr_output, _, _ = self.execute_command(
                                f"cat '{log_file}' 2>/dev/null || echo ''"
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

    def monitor_job(
        self, job: SlurmJob, poll_interval: int = 10, timeout: int | None = None
    ) -> SlurmJob:
        """Monitor a job until completion"""
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

    def _write_remote_file(self, remote_path: str, content: str) -> None:
        if not self.sftp_client:
            raise ConnectionError("SFTP client is not connected")
        with self.sftp_client.open(remote_path, "w") as f:
            f.write(content)

    def _handle_slurm_error(
        self, command: str, error_output: str, exit_code: int
    ) -> None:
        """Handle SLURM command errors with helpful suggestions"""
        self.logger.error(
            f"{command} failed with exit code {exit_code}: {error_output}"
        )
        # Common error patterns and suggestions
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

    def _execute_with_environment(self, command: str) -> tuple[str, str, int]:
        """Execute command with full login environment"""
        return self.execute_command(f'bash -l -c "{command}"')

    def __enter__(self):
        if self.connect():
            return self
        else:
            raise ConnectionError("Failed to establish SSH connection")

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()
