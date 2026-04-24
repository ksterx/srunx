"""Remote file operations over SSH.

Provides upload, download, cleanup, validation, and project sync via
rsync.  All operations delegate command execution to an
:class:`SSHConnection` instance.
"""

from __future__ import annotations

import shlex
import uuid
from pathlib import Path
from typing import TYPE_CHECKING

from srunx.common.logging import get_logger
from srunx.sync import RsyncClient

from .utils import detect_project_root

if TYPE_CHECKING:
    from .connection import SSHConnection

_logger = get_logger(__name__)


class RemoteFileManager:
    """File operations on a remote host via an SSH connection."""

    def __init__(self, connection: SSHConnection) -> None:
        self._conn = connection
        self.logger = _logger

        # RsyncClient for project sync (key-based auth only)
        self._rsync_client: RsyncClient | None = None
        if self._conn.key_filename:
            try:
                self._rsync_client = RsyncClient(
                    hostname=self._conn.hostname,
                    username=self._conn.username,
                    port=self._conn.port,
                    key_filename=self._conn.key_filename,
                    proxy_jump=self._conn.proxy_jump,
                    ssh_config_path=self._conn.ssh_config_path,
                )
            except RuntimeError:
                self.logger.warning("rsync not available; sync_project() disabled")

    # ------------------------------------------------------------------
    # File CRUD
    # ------------------------------------------------------------------

    def upload_file(self, local_path: str, remote_path: str | None = None) -> str:
        """Upload a local file to the server and return the remote path."""
        if not self._conn.sftp_client:
            raise ConnectionError("SFTP client is not connected")

        local_path_obj = Path(local_path)
        if not local_path_obj.exists():
            raise FileNotFoundError(f"Local file not found: {local_path}")

        if remote_path is None:
            unique_id = str(uuid.uuid4())[:8]
            remote_filename = (
                f"{local_path_obj.stem}_{unique_id}{local_path_obj.suffix}"
            )
            remote_path = f"{self._conn.temp_dir}/{remote_filename}"

        try:
            self._conn.sftp_client.put(str(local_path_obj), remote_path)
            if local_path_obj.suffix in [".sh", ".py", ".pl", ".r"]:
                self._conn.execute_command(f"chmod +x {shlex.quote(remote_path)}")
            if self._conn.verbose:
                self.logger.info(f"Uploaded {local_path} to {remote_path}")
            return remote_path
        except Exception as e:
            self.logger.error(f"Failed to upload file: {e}")
            raise

    def cleanup_file(self, remote_path: str) -> None:
        """Remove a file from the server."""
        try:
            self._conn.execute_command(f"rm -f {shlex.quote(remote_path)}")
            if self._conn.verbose:
                self.logger.info(f"Cleaned up remote file: {remote_path}")
        except Exception as e:
            self.logger.warning(f"Failed to cleanup file {remote_path}: {e}")

    def file_exists(self, remote_path: str) -> bool:
        """Check if a file exists on the server."""
        stdout, stderr, exit_code = self._conn.execute_command(
            f"test -f {shlex.quote(remote_path)} && echo 'exists' || echo 'not_found'"
        )
        exists = stdout.strip() == "exists"
        self.logger.debug(f"File existence check for {remote_path}: {exists}")
        return exists

    def write_remote_file(self, remote_path: str, content: str) -> None:
        """Write content to a remote file via SFTP."""
        if not self._conn.sftp_client:
            raise ConnectionError("SFTP client is not connected")
        with self._conn.sftp_client.open(remote_path, "w") as f:
            f.write(content)

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------

    def validate_remote_script(self, remote_path: str) -> tuple[bool, str]:
        """Validate a remote script file and return (is_valid, error_message)."""
        if not self.file_exists(remote_path):
            return False, f"Remote script file not found: {remote_path}"

        quoted_path = shlex.quote(remote_path)
        stdout, stderr, exit_code = self._conn.execute_command(
            f"test -r {quoted_path} && echo 'readable' || echo 'not_readable'"
        )
        if stdout.strip() != "readable":
            return False, f"Remote script file is not readable: {remote_path}"

        stdout, stderr, exit_code = self._conn.execute_command(
            f"test -x {quoted_path} && echo 'executable' || echo 'not_executable'"
        )
        if stdout.strip() != "executable":
            self.logger.warning(
                f"Remote script file is not executable: {remote_path}. SLURM may fail to run it."
            )

        stdout, stderr, exit_code = self._conn.execute_command(
            f"wc -c < {quoted_path} 2>/dev/null || echo '0'"
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

        if remote_path.endswith(".sh"):
            stdout, stderr, exit_code = self._conn.execute_command(
                f"bash -n {quoted_path} 2>&1 || echo 'SYNTAX_ERROR'"
            )
            if "SYNTAX_ERROR" in stdout or exit_code != 0:
                return (
                    False,
                    f"Shell script syntax error in {remote_path}: {stdout.strip()}",
                )
            self.logger.debug(f"Shell script syntax check passed for {remote_path}")

        return True, "Script validation successful"

    # ------------------------------------------------------------------
    # Project sync
    # ------------------------------------------------------------------

    def sync_project(
        self,
        local_path: str | None = None,
        remote_path: str | None = None,
        *,
        delete: bool = True,
        dry_run: bool = False,
        exclude_patterns: list[str] | None = None,
    ) -> str:
        """Sync the local project directory to the remote workspace via rsync.

        Args:
            local_path: Local project root to sync. If None, uses git toplevel
                or cwd.
            remote_path: Remote destination. If None, uses the default
                ``~/.config/srunx/workspace/{repo_name}/``.
            delete: Remove remote files not present locally (default True).
            dry_run: Preview what would be transferred without syncing.
            exclude_patterns: Additional exclude patterns for this sync.

        Returns:
            The remote project path (for use with ``sbatch --chdir``).

        Raises:
            RuntimeError: If rsync is not available or key-based auth is not
                configured.
        """
        if self._rsync_client is None:
            raise RuntimeError(
                "sync_project() requires key-based SSH auth and rsync installed locally"
            )

        if local_path is None:
            local_path = detect_project_root()

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

        if self._conn.verbose:
            self.logger.info(f"Project synced to {self._conn.hostname}:{remote_path}")

        return remote_path
