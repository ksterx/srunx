"""Rsync-based file synchronization for remote SLURM servers."""

from __future__ import annotations

import os
import shlex
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import ClassVar

from srunx.logging import get_logger

logger = get_logger(__name__)


@dataclass
class RsyncResult:
    """Result of an rsync operation."""

    returncode: int
    stdout: str
    stderr: str

    @property
    def success(self) -> bool:
        return self.returncode == 0


class RsyncClient:
    """Rsync wrapper for syncing files to/from remote SLURM servers.

    Handles SSH connection options (port, key, ProxyJump, ssh_config)
    and builds rsync commands with sensible defaults for development
    workflow synchronization.
    """

    DEFAULT_EXCLUDES: ClassVar[list[str]] = [
        ".git/",
        "__pycache__/",
        ".venv/",
        "*.pyc",
        ".mypy_cache/",
        ".pytest_cache/",
        ".ruff_cache/",
        "*.egg-info/",
        ".tox/",
        "node_modules/",
        ".DS_Store",
    ]

    def __init__(
        self,
        hostname: str,
        username: str,
        port: int = 22,
        key_filename: str | None = None,
        proxy_jump: str | None = None,
        ssh_config_path: str | None = None,
        exclude_patterns: list[str] | None = None,
    ) -> None:
        rsync_path = shutil.which("rsync")
        if rsync_path is None:
            raise RuntimeError(
                "rsync is not installed or not found in PATH. "
                "Please install rsync before using RsyncClient."
            )

        self.hostname = hostname
        self.username = username
        self.port = port
        self.key_filename = key_filename
        self.proxy_jump = proxy_jump
        self.ssh_config_path = ssh_config_path

        # Detect rsync capabilities
        self._supports_protect_args = False
        self._supports_mkpath = False
        self._detect_rsync_capabilities(rsync_path)

        # Merge caller-supplied excludes with defaults (no duplicates)
        self.exclude_patterns = list(self.DEFAULT_EXCLUDES)
        if exclude_patterns:
            seen = set(self.exclude_patterns)
            for pattern in exclude_patterns:
                if pattern not in seen:
                    self.exclude_patterns.append(pattern)
                    seen.add(pattern)

    def _detect_rsync_capabilities(self, rsync_path: str) -> None:
        """Detect which flags the installed rsync binary supports."""
        try:
            result = subprocess.run(
                [rsync_path, "--help"],
                capture_output=True,
                text=True,
            )
            help_text = result.stdout + result.stderr
            self._supports_protect_args = "--protect-args" in help_text
            self._supports_mkpath = "--mkpath" in help_text
        except (OSError, subprocess.SubprocessError):
            pass

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def push(
        self,
        local_path: str | Path,
        remote_path: str | None = None,
        *,
        delete: bool = True,
        dry_run: bool = False,
        exclude_patterns: list[str] | None = None,
    ) -> RsyncResult:
        """Sync a local directory/file to the remote server.

        Args:
            local_path: Local file or directory to push.
            remote_path: Destination path on the remote server.
                If None, uses ``get_default_remote_path()``.
            delete: Remove remote files not present locally (default True).
            dry_run: Perform a trial run with no changes made.
            exclude_patterns: Additional exclude patterns for this call only.

        Returns:
            RsyncResult with returncode, stdout, and stderr.
        """
        if remote_path is None:
            remote_path = self.get_default_remote_path(local_path)

        local = Path(local_path)
        src = str(local)
        # Trailing slash ensures rsync copies directory *contents*, not the
        # directory itself.
        if local.is_dir() and not src.endswith("/"):
            src += "/"

        dst = self._format_remote(remote_path)

        # Ensure remote directory exists when --mkpath is unavailable
        if not self._supports_mkpath and not dry_run:
            self._ensure_remote_dir(remote_path)

        excludes = self._merge_excludes(exclude_patterns)
        cmd = self._build_rsync_cmd(
            src, dst, delete=delete, dry_run=dry_run, excludes=excludes
        )
        return self._run_rsync(cmd)

    def pull(
        self,
        remote_path: str,
        local_path: str | Path,
        *,
        delete: bool = False,
        dry_run: bool = False,
        exclude_patterns: list[str] | None = None,
    ) -> RsyncResult:
        """Sync a remote directory/file to the local machine.

        Args:
            remote_path: Source path on the remote server.
            local_path: Local destination path.
            delete: Remove local files not present on the remote (default False).
            dry_run: Perform a trial run with no changes made.
            exclude_patterns: Additional exclude patterns for this call only.

        Returns:
            RsyncResult with returncode, stdout, and stderr.
        """
        src = self._format_remote(remote_path)
        dst = str(local_path)

        excludes = self._merge_excludes(exclude_patterns)
        cmd = self._build_rsync_cmd(
            src, dst, delete=delete, dry_run=dry_run, excludes=excludes
        )
        return self._run_rsync(cmd)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _build_ssh_cmd(self) -> list[str]:
        """Build the SSH command list used by rsync's ``-e`` flag."""
        parts: list[str] = ["ssh"]

        if self.port != 22:
            parts.extend(["-p", str(self.port)])
        if self.key_filename:
            parts.extend(["-i", os.path.expanduser(self.key_filename)])
        if self.proxy_jump:
            parts.extend(["-J", self.proxy_jump])
        if self.ssh_config_path:
            parts.extend(["-F", self.ssh_config_path])

        parts.extend(["-o", "StrictHostKeyChecking=accept-new"])
        parts.extend(["-o", "BatchMode=yes"])

        return parts

    def _build_rsync_cmd(
        self,
        src: str,
        dst: str,
        *,
        delete: bool,
        dry_run: bool,
        excludes: list[str],
    ) -> list[str]:
        """Build the full rsync command."""
        cmd: list[str] = ["rsync", "-az"]

        if self._supports_protect_args:
            cmd.append("--protect-args")
        if self._supports_mkpath:
            cmd.append("--mkpath")

        ssh_cmd = self._build_ssh_cmd()
        cmd.extend(["-e", shlex.join(ssh_cmd)])

        if delete:
            cmd.append("--delete")
        if dry_run:
            cmd.append("-n")

        for pattern in excludes:
            cmd.extend(["--exclude", pattern])

        cmd.extend(["--", src, dst])
        return cmd

    def _run_rsync(self, cmd: list[str]) -> RsyncResult:
        """Execute an rsync command and return the result."""
        logger.debug("Running rsync: {}", shlex.join(cmd))

        proc = subprocess.run(cmd, capture_output=True, text=True)  # noqa: S603

        if proc.returncode != 0:
            logger.warning(
                "rsync exited with code {}: {}", proc.returncode, proc.stderr.strip()
            )

        return RsyncResult(
            returncode=proc.returncode,
            stdout=proc.stdout,
            stderr=proc.stderr,
        )

    def _ensure_remote_dir(self, remote_path: str) -> None:
        """Create the remote directory via ssh mkdir -p (fallback for rsync without --mkpath)."""
        ssh_cmd = self._build_ssh_cmd()
        mkdir_cmd = [
            *ssh_cmd,
            f"{self.username}@{self.hostname}",
            f"mkdir -p {remote_path}",
        ]
        logger.debug("Ensuring remote dir: {}", shlex.join(mkdir_cmd))
        subprocess.run(mkdir_cmd, capture_output=True, text=True)  # noqa: S603

    def _merge_excludes(self, extra: list[str] | None) -> list[str]:
        """Merge per-call exclude patterns with instance patterns."""
        if not extra:
            return self.exclude_patterns
        seen = set(self.exclude_patterns)
        merged = list(self.exclude_patterns)
        for pattern in extra:
            if pattern not in seen:
                merged.append(pattern)
                seen.add(pattern)
        return merged

    @staticmethod
    def get_default_remote_path(local_path: str | Path | None = None) -> str:
        """Derive a default remote workspace path from the git repo or cwd.

        Args:
            local_path: Optional local directory to derive the project name
                from. If None, uses the current working directory.

        Returns:
            A path like ``~/.config/srunx/workspace/<project_name>/``.
        """
        cwd = str(Path(local_path)) if local_path else None
        try:
            result = subprocess.run(  # noqa: S603, S607
                ["git", "rev-parse", "--show-toplevel"],
                capture_output=True,
                text=True,
                cwd=cwd,
            )
            if result.returncode == 0:
                basename = Path(result.stdout.strip()).name
            else:
                basename = Path(cwd).name if cwd else Path.cwd().name
        except FileNotFoundError:
            # git not installed
            basename = Path(cwd).name if cwd else Path.cwd().name

        return f"~/.config/srunx/workspace/{basename}/"

    def _format_remote(self, path: str) -> str:
        """Format a remote path as ``user@host:path`` or ``host:path``.

        When *username* is empty (e.g. SSH config host alias), the
        ``user@`` prefix is omitted so that rsync delegates to the
        SSH config for user resolution.

        Tilde (``~``) is left unquoted so the remote shell can expand it.
        ``--protect-args`` handles any special characters in the path.
        """
        if self.username:
            return f"{self.username}@{self.hostname}:{path}"
        return f"{self.hostname}:{path}"
