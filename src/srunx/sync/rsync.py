"""Rsync-based file synchronization for remote SLURM servers."""

from __future__ import annotations

import os
import shlex
import shutil
import subprocess
import sys
import threading
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import IO, ClassVar

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
        exclude_patterns: Sequence[str] | None = None,
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
        itemize: bool = False,
        verbose: bool = False,
        exclude_patterns: Sequence[str] | None = None,
    ) -> RsyncResult:
        """Sync a local directory/file to the remote server.

        Args:
            local_path: Local file or directory to push.
            remote_path: Destination path on the remote server.
                If None, uses ``get_default_remote_path()``.
            delete: Remove remote files not present locally (default True).
            dry_run: Perform a trial run with no changes made.
            itemize: Add ``--itemize-changes`` so the result lists every
                file rsync *would* (or did) touch, with the standard
                ``YXcstpoguax`` flag prefix. Required for ``dry_run``
                callers that want a human-readable preview.
            verbose: Stream rsync's per-file progress to stderr live
                instead of capturing it silently. Adds
                ``--info=progress2`` so users with large mounts see
                progress instead of a frozen terminal.
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
            src,
            dst,
            delete=delete,
            dry_run=dry_run,
            itemize=itemize,
            verbose=verbose,
            excludes=excludes,
        )
        if verbose:
            return self._run_rsync_streaming(cmd)
        return self._run_rsync(cmd)

    def pull(
        self,
        remote_path: str,
        local_path: str | Path,
        *,
        delete: bool = False,
        dry_run: bool = False,
        itemize: bool = False,
        exclude_patterns: Sequence[str] | None = None,
    ) -> RsyncResult:
        """Sync a remote directory/file to the local machine.

        Args:
            remote_path: Source path on the remote server.
            local_path: Local destination path.
            delete: Remove local files not present on the remote (default False).
            dry_run: Perform a trial run with no changes made.
            itemize: Add ``--itemize-changes`` so the result enumerates
                every file rsync *would* (or did) touch.
            exclude_patterns: Additional exclude patterns for this call only.

        Returns:
            RsyncResult with returncode, stdout, and stderr.
        """
        src = self._format_remote(remote_path)
        dst = str(local_path)

        excludes = self._merge_excludes(exclude_patterns)
        cmd = self._build_rsync_cmd(
            src,
            dst,
            delete=delete,
            dry_run=dry_run,
            itemize=itemize,
            excludes=excludes,
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
        itemize: bool = False,
        verbose: bool = False,
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
        if itemize:
            # ``-i`` (``--itemize-changes``) makes rsync emit one line
            # per file with a ``YXcstpoguax``-style flag prefix so the
            # CLI dry-run preview can render exactly what would change.
            cmd.append("-i")
        if verbose:
            # ``--info=progress2`` is the single-line aggregate progress
            # form that updates in place (via ``\r``). It's the only
            # progress mode that doesn't drown the terminal in per-file
            # output for thousand-file syncs.
            cmd.append("--info=progress2")

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

    def _run_rsync_streaming(self, cmd: list[str]) -> RsyncResult:
        """Execute rsync, streaming stdout to stderr live as it arrives.

        Drains both pipes from dedicated threads. A single-thread
        ``readline()`` loop on Popen.stdout would deadlock once
        rsync's stderr pipe buffer fills (typically 64 KiB), and
        ``select`` doesn't work for text-mode pipes on every platform
        — two blocking threads are the simplest correct shape.

        The streamed lines are also accumulated into the returned
        :class:`RsyncResult` so callers that read ``result.stdout`` /
        ``result.stderr`` (e.g. error-message construction) keep
        working unchanged.
        """
        logger.debug("Running rsync (streaming): {}", shlex.join(cmd))

        proc = subprocess.Popen(  # noqa: S603
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
        )

        stdout_buf: list[str] = []
        stderr_buf: list[str] = []

        def _pump(src: IO[str], sink: IO[str], buf: list[str]) -> None:
            for line in src:
                buf.append(line)
                sink.write(line)
                sink.flush()

        assert proc.stdout is not None and proc.stderr is not None
        t_out = threading.Thread(
            target=_pump, args=(proc.stdout, sys.stderr, stdout_buf)
        )
        t_err = threading.Thread(
            target=_pump, args=(proc.stderr, sys.stderr, stderr_buf)
        )
        t_out.start()
        t_err.start()

        returncode = proc.wait()
        t_out.join()
        t_err.join()

        stdout_text = "".join(stdout_buf)
        stderr_text = "".join(stderr_buf)

        if returncode != 0:
            logger.warning(
                "rsync exited with code {}: {}", returncode, stderr_text.strip()
            )

        return RsyncResult(
            returncode=returncode,
            stdout=stdout_text,
            stderr=stderr_text,
        )

    def _ensure_remote_dir(self, remote_path: str) -> None:
        """Create the remote directory via ssh mkdir -p (fallback for rsync without --mkpath)."""
        self._ssh_run(f"mkdir -p {shlex.quote(remote_path.rstrip('/'))}")

    def _ssh_dest(self) -> str:
        """Return the ``user@host`` (or just ``host``) string for ssh."""
        if self.username:
            return f"{self.username}@{self.hostname}"
        return self.hostname

    def _ssh_run(
        self,
        remote_cmd: str,
        *,
        stdin: str | None = None,
    ) -> subprocess.CompletedProcess[str]:
        """Run *remote_cmd* on the configured host via ssh.

        Reuses the same SSH options rsync uses (key, port, ProxyJump,
        ssh_config) so that anything rsync can reach, this can reach
        too. ``stdin`` is piped through to the remote process — used
        by the owner-marker writer to send JSON via ``tee``.
        """
        ssh_cmd = [*self._build_ssh_cmd(), self._ssh_dest(), remote_cmd]
        logger.debug("ssh: {}", shlex.join(ssh_cmd))
        return subprocess.run(  # noqa: S603
            ssh_cmd,
            input=stdin,
            capture_output=True,
            text=True,
        )

    def read_remote_file(self, remote_path: str) -> str | None:
        """Return the remote file's contents, or ``None`` if it doesn't exist.

        Used by the per-machine ownership marker (#137 part 4) to read
        ``.srunx-owner.json`` before each sync. The check needs to
        distinguish "file missing" (legitimate first sync, returns
        ``None``) from "ssh / network failed" (raise so the caller
        knows the marker can't be trusted).

        Implementation: ``ssh ... cat -- <path>`` with a per-file
        existence test wrapped in a single shell command — keeps the
        round-trip count to one per check.
        """
        # ``test -f X && cat X`` returns:
        #   * 0 + stdout: file exists, content returned
        #   * 1 + empty stdout: file does not exist
        #   * 2+ : actual error (permission denied, ssh failure, …)
        # We disambiguate via the exit code so transient failures
        # don't get silently treated as "no marker".
        quoted = shlex.quote(remote_path)
        result = self._ssh_run(f"test -f {quoted} && cat -- {quoted}")
        if result.returncode == 0:
            return result.stdout
        if result.returncode == 1:
            # ``test -f`` returned false — file does not exist.
            return None
        raise RuntimeError(
            f"ssh read of {remote_path!r} failed (exit {result.returncode}): "
            f"{result.stderr.strip()}"
        )

    def write_remote_file(self, remote_path: str, content: str) -> None:
        """Atomically write *content* to *remote_path* via ssh + tee.

        ``tee`` (without ``-a``) truncates and rewrites the destination
        in one shell op so a concurrent reader either sees the old
        content or the new content — never a half-written file. The
        parent directory is assumed to exist (the rsync that just ran
        guarantees it for the owner-marker case).

        Raises :class:`RuntimeError` if ssh / tee exits non-zero so the
        caller can surface the failure rather than silently leaving a
        stale marker on disk.
        """
        quoted = shlex.quote(remote_path)
        # Use ``> /dev/null`` so tee's stdout doesn't echo the JSON
        # back through ssh (waste of bytes + stdout pollution).
        result = self._ssh_run(f"tee -- {quoted} > /dev/null", stdin=content)
        if result.returncode != 0:
            raise RuntimeError(
                f"ssh write to {remote_path!r} failed "
                f"(exit {result.returncode}): {result.stderr.strip()}"
            )

    def _merge_excludes(self, extra: Sequence[str] | None) -> list[str]:
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
