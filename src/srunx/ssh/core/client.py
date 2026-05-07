"""Thin facade composing the four SSH/SLURM components.

:class:`SSHSlurmClient` constructs and owns four focused subobjects —
:class:`SSHConnection` (``self.connection``),
:class:`RemoteFileManager` (``self.files``),
:class:`SlurmRemoteClient` (``self.slurm``), and
:class:`RemoteLogReader` (``self.logs``) — and exposes:

* lifecycle methods (``connect`` / ``disconnect`` / ``__enter__`` / ``__exit__``
  / ``test_connection``) that compose across components,
* :meth:`sync_project` because it requires a separate ``RsyncClient`` that
  doesn't fit any single component,
* the components themselves as attributes — callers reach for the
  appropriate component rather than going through facade-level wrappers.

For example, instead of ``client._execute_slurm_command(cmd)``, callers
use ``client.slurm.execute_slurm_command(cmd)``; instead of
``client.upload_file(path)``, ``client.files.upload_file(path)``.

A previous version of this facade re-exposed every component method
inline (~1000 lines of duplication) so test mocks at the facade level
would propagate. Tests now mock at the component level
(``client.slurm.execute_slurm_command = Mock(...)`` etc.) — see
``tests/ssh/core/test_client.py``.
"""

from __future__ import annotations

import subprocess
from pathlib import Path

from srunx.common.logging import get_logger
from srunx.sync import RsyncClient

from .client_types import SlurmJob
from .connection import SSHConnection
from .file_manager import RemoteFileManager
from .log_reader import RemoteLogReader
from .slurm import SlurmRemoteClient

# Re-export so ``from srunx.ssh.core.client import SlurmJob`` keeps working.
__all__ = ["SSHSlurmClient", "SlurmJob"]

_logger = get_logger(__name__)


class SSHSlurmClient:
    """Composes :class:`SSHConnection`, :class:`RemoteFileManager`,
    :class:`SlurmRemoteClient`, and :class:`RemoteLogReader` into a
    single context-manageable handle.

    The facade adds:

    * :meth:`connect` — SSH transport + SLURM path discovery in one call.
    * :meth:`sync_project` — local→remote rsync via a separate
      :class:`RsyncClient` (key-auth profiles only).
    * Context manager protocol so ``with SSHSlurmClient(...) as c`` raises
      ``ConnectionError`` instead of returning ``False`` like
      :meth:`SSHConnection.connect`.

    Direct method-level access (``client.submit_sbatch_job(...)``,
    ``client._execute_slurm_command(...)``) was removed — call through the
    component (``client.slurm.submit_sbatch_job(...)``,
    ``client.slurm.execute_slurm_command(...)``).
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
        self.logger = _logger
        self.verbose = verbose

        self.connection = SSHConnection(
            hostname=hostname,
            username=username,
            password=password,
            key_filename=key_filename,
            port=port,
            proxy_jump=proxy_jump,
            ssh_config_path=ssh_config_path,
            verbose=verbose,
            env_vars=env_vars,
        )
        self.files = RemoteFileManager(self.connection)
        self.slurm = SlurmRemoteClient(self.connection, self.files)
        self.logs = RemoteLogReader(self.connection, self.slurm)

        # RsyncClient for project sync (key-based auth only).
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
    # Lifecycle (composes SSH + SLURM init)
    # ==================================================================

    def connect(self) -> bool:
        """Open the SSH transport and initialise SLURM paths.

        Returns ``True`` on success, ``False`` on any failure. Inspect
        ``self.connection._last_error`` to see what went wrong —
        :meth:`__enter__` uses that pattern to surface a usable error
        message.
        """
        if not self.connection.connect():
            return False
        try:
            self.slurm.initialize()
        except Exception as exc:  # noqa: BLE001
            # SLURM init failures shouldn't kill the connection — log and
            # let callers proceed (squeue / scancel may still work via
            # PATH-resolved binaries even if path discovery failed).
            self.logger.warning(f"SLURM initialization failed: {exc}")
        return True

    def disconnect(self) -> None:
        self.connection.disconnect()

    def test_connection(self) -> dict[str, str | bool]:
        """Connect → run hostname/whoami → disconnect.

        Wraps :meth:`SSHConnection.test_connection` so callers that work
        through the facade keep a single import. Returns the same
        dict shape as the underlying method.
        """
        return self.connection.test_connection()

    def __enter__(self) -> SSHSlurmClient:
        if self.connect():
            return self
        cause = self.connection._last_error
        target = self.hostname
        if self.proxy_jump:
            target += f" (via {self.proxy_jump})"
        reason = f"{type(cause).__name__}: {cause}" if cause else "unknown error"
        raise ConnectionError(
            f"Failed to establish SSH connection to {target}: {reason}"
        ) from cause

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()

    # ==================================================================
    # Project sync (separate from the four core components)
    # ==================================================================

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
