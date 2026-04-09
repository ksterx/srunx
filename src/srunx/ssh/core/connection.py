"""SSH connection lifecycle management.

Handles connect, disconnect, command execution, and context manager
protocol.  SLURM-specific initialization is deliberately excluded --
that responsibility belongs to :class:`SlurmRemoteClient`.
"""

from __future__ import annotations

import os
import shlex

import paramiko  # type: ignore

from srunx.logging import get_logger

from .proxy_client import ProxySSHClient, create_proxy_aware_connection

_logger = get_logger(__name__)


class SSHConnection:
    """Manages an SSH transport to a remote host."""

    def __init__(
        self,
        hostname: str,
        username: str,
        password: str | None = None,
        key_filename: str | None = None,
        port: int = 22,
        proxy_jump: str | None = None,
        ssh_config_path: str | None = None,
        verbose: bool = False,
        temp_dir: str | None = None,
        env_vars: dict[str, str] | None = None,
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
        self.temp_dir: str = temp_dir or os.getenv("SRUNX_TEMP_DIR") or "/tmp/srunx"
        self.custom_env_vars: dict[str, str] = env_vars or {}
        self.verbose = verbose

    # ------------------------------------------------------------------
    # Connection lifecycle
    # ------------------------------------------------------------------

    def connect(self) -> bool:
        """Establish SSH + SFTP transport (no SLURM init)."""
        try:
            if self.proxy_jump:
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

            # Create SFTP client for file transfers
            self.sftp_client = self.ssh_client.open_sftp()

            # Create temp directory on server
            self.execute_command(f"mkdir -p {shlex.quote(self.temp_dir)}")

            connection_info = f"{self.hostname}"
            if self.proxy_jump:
                connection_info += f" (via {self.proxy_jump})"
            if self.verbose:
                self.logger.info(f"Successfully connected to {connection_info}")
            return True

        except Exception as e:
            self.logger.error(f"Failed to connect to {self.hostname}: {e}")
            return False

    def disconnect(self) -> None:
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

    # ------------------------------------------------------------------
    # Command execution
    # ------------------------------------------------------------------

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

    # ------------------------------------------------------------------
    # Connection test
    # ------------------------------------------------------------------

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

    # ------------------------------------------------------------------
    # Context manager
    # ------------------------------------------------------------------

    def __enter__(self) -> SSHConnection:
        if self.connect():
            return self
        else:
            raise ConnectionError("Failed to establish SSH connection")

    def __exit__(self, exc_type: object, exc_val: object, exc_tb: object) -> None:
        self.disconnect()
