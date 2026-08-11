"""Rsync-based file synchronization for remote SLURM servers."""

from __future__ import annotations

import os
import re
import shlex
import shutil
import subprocess
import sys
import threading
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import IO, ClassVar

from srunx.common.logging import get_logger

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
        # srunx's own control file on the remote side. It has no local
        # counterpart, so without this a mirror (``--delete``) deletes it on
        # every sync — taking with it the ownership marker that stops one
        # machine from syncing over another's mount. Excluding a path also
        # protects it from deletion, which is the point here.
        #
        # Anchored with a leading slash so it matches only the transfer root.
        # A bare ``.srunx-owner.json`` matches that basename at *every* depth,
        # which would silently skip a user's own ``subdir/.srunx-owner.json``;
        # only the file at the mount root is srunx's.
        #
        # Known consequence: mirror paths that don't go through
        # ``mount_sync_session`` (``srunx ssh sync --delete``, the MCP and Web
        # mirror callers) don't rewrite the marker, so after one workstation
        # mirrors over a mount another one owned, the marker still names the old
        # host and the next ordinary auto-sync is refused. That is the safe
        # direction to fail — nothing is lost, and ``--force-sync`` (or
        # ``[sync] owner_check = false``) clears it — but it is only truly fixed
        # by giving every configured-mount push one shared entry point that owns
        # the marker's lifecycle.
        "/.srunx-owner.json",
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
        delete: bool = False,
        dry_run: bool = False,
        itemize: bool = False,
        verbose: bool = False,
        max_delete: int | None = None,
        exclude_patterns: Sequence[str] | None = None,
    ) -> RsyncResult:
        """Sync a local directory/file to the remote server.

        Args:
            local_path: Local file or directory to push.
            remote_path: Destination path on the remote server.
                If None, uses ``get_default_remote_path()``.
            delete: Remove remote files not present locally. **Defaults to
                False**: a push that silently prunes remote-only files
                (training checkpoints, run logs) is a data-loss footgun, and
                every historical incident here came from a caller inheriting
                a mirror-by-default. Mirror semantics are opt-in — callers
                that genuinely want them pass ``delete=True`` explicitly.
            max_delete: Blast-radius cap for a mirror — **not** an atomic
                refusal. rsync deletes up to this many entries, skips the
                remaining deletions, finishes transferring, and only then exits
                25, so on a cap hit the destination *has already changed*
                (verified against openrsync). A caller that needs "refuse
                without touching anything" must count deletions in a separate
                dry run and decide before calling; the MCP
                :func:`~srunx.mcp.tools.sync.sync_files` tool does exactly
                that. Only meaningful with ``delete=True``. Must be >= 1 —
                see :class:`ValueError` below.
            dry_run: Perform a trial run that changes nothing on the remote —
                no transfers, no deletions, and no directory creation. Note
                that rsync without ``--mkpath`` cannot evaluate a transfer
                against a missing destination parent, so a preview of a
                not-yet-created destination fails rather than reporting an
                empty diff.
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

        Raises:
            ValueError: If ``max_delete`` is 0 or negative. Zero cannot be
                forwarded safely (rsync 2.6.x / openrsync read
                ``--max-delete=0`` as *unlimited*, inverting the strictest cap
                into no cap at all), and silently downgrading the call to
                ``delete=False`` would be worse: the caller asked for a mirror
                that refuses rather than one that quietly leaves extra
                destination files behind. To forbid deletion, pass
                ``delete=False`` explicitly.
        """
        if max_delete is not None and max_delete < 1:
            raise ValueError(
                f"max_delete must be >= 1, got {max_delete}. To forbid "
                "deletion entirely, pass delete=False — 0 cannot be forwarded "
                "to rsync safely (2.6.x / openrsync read --max-delete=0 as "
                "unlimited), and quietly dropping --delete instead would turn "
                "a mirror that should refuse into one that silently leaves "
                "extra destination files in place."
            )

        if remote_path is None:
            remote_path = self.get_default_remote_path(local_path)

        local = Path(local_path)
        src = str(local)
        # Trailing slash ensures rsync copies directory *contents*, not the
        # directory itself.
        if local.is_dir() and not src.endswith("/"):
            src += "/"

        dst = self._format_remote(remote_path)

        # Ensure remote directory exists when --mkpath is unavailable.
        #
        # Deliberately restricted to real runs. A dry run must not touch the
        # remote at all: otherwise "nothing was changed" stops being true, and
        # a preview whose destination is a *file* path would create a
        # directory exactly where that file belongs. The cost is that a first
        # mirror against a not-yet-existing destination fails its preflight —
        # a safe, explicit failure that ``sync_files`` explains how to work
        # around, which is a better trade than a preview with side effects.
        if not self._supports_mkpath and not dry_run:
            # A file destination needs its *parent* created. ``mkdir -p`` on
            # the file path itself would put a directory where the file goes,
            # after which rsync can never write it.
            if local.is_dir() or remote_path.endswith("/"):
                self._ensure_remote_dir(remote_path)
            else:
                parent = str(PurePosixPath(remote_path).parent)
                if parent not in (".", "/", ""):
                    self._ensure_remote_dir(parent)

        excludes = self._merge_excludes(exclude_patterns)
        cmd = self._build_rsync_cmd(
            src,
            dst,
            delete=delete,
            dry_run=dry_run,
            itemize=itemize,
            verbose=verbose,
            max_delete=max_delete,
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

        # Host-key strictness mirrors the paramiko path (same env var). Default
        # is strict: a host already in known_hosts is required, blocking
        # first-contact MITM. Opt into TOFU with SRUNX_SSH_HOST_KEY_POLICY=accept-new.
        policy = os.environ.get("SRUNX_SSH_HOST_KEY_POLICY", "reject").strip().lower()
        strict_value = {
            "accept-new": "accept-new",
            "warn": "no",
        }.get(policy, "yes")
        parts.extend(["-o", f"StrictHostKeyChecking={strict_value}"])
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
        max_delete: int | None = None,
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

        # A zero cap never reaches here — ``push`` rejects it outright, because
        # ``--max-delete=0`` means *unlimited* on rsync 2.6.x / openrsync
        # (stock on macOS) rather than "delete nothing". Verified: a
        # ``--delete --max-delete=0`` run against openrsync deleted every
        # destination-only file and exited 0, turning the strictest possible
        # cap into no cap at all.
        if delete:
            cmd.append("--delete")
            if max_delete is not None:
                # Bounds a mirror's blast radius: rsync stops deleting and
                # exits 25 once the cap is exceeded. Only emitted alongside
                # --delete, since without it rsync deletes nothing and the
                # flag would be inert noise.
                cmd.append(f"--max-delete={max_delete}")
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
        """Run *remote_cmd* on the configured host via ssh, under ``sh``.

        Reuses the same SSH options rsync uses (key, port, ProxyJump,
        ssh_config) so that anything rsync can reach, this can reach
        too. ``stdin`` is piped through to the remote process — used
        by the owner-marker writer to send its JSON.

        The command is wrapped in ``sh -c`` instead of being handed to the
        account's login shell. OpenSSH evaluates a remote command *with that
        login shell*, and the ``if ...; then ...; fi`` scripts here are not
        valid csh/tcsh — still the default shell on some HPC accounts. Without
        the wrapper those commands fail on every invocation, and because the
        callers downgrade failures (a missing marker is a warning, an
        unavailable hash skips verification) the breakage would be silent:
        the cross-workstation overwrite guard and the post-sync hash check
        would both quietly stop working.

        The wrapper only holds for a single-line command. csh cannot carry a
        newline through single quotes, so a multi-line script gets split and the
        fragments parsed by csh itself — which not only fails but can run pieces
        of the script as separate commands. Rather than leave that as a trap for
        the next caller, a newline is rejected outright; join steps with ``;``.

        Raises:
            ValueError: If *remote_cmd* spans multiple lines.
        """
        if "\n" in remote_cmd:
            raise ValueError(
                "remote_cmd must be a single line — a csh/tcsh login shell "
                "splits newlines out of the quoted argument and parses the "
                "fragments itself, running part of the script as separate "
                "commands. Join the steps with ';' instead."
            )
        wrapped = f"sh -c {shlex.quote(remote_cmd)}"
        ssh_cmd = [*self._build_ssh_cmd(), self._ssh_dest(), wrapped]
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
        """Write *content* to *remote_path* atomically (temp file + rename).

        ``mv`` within one directory is a ``rename(2)``, so a concurrent reader
        sees either the previous file or the complete new one. Writing with
        ``tee`` straight at the target does **not** give that: ``tee``
        truncates first, so a reader in that window sees an empty file. This
        function used to do exactly that while documenting the opposite.

        The target is rejected when it is a symlink (``tee`` / ``mv`` would act
        on whatever it points at, letting a planted link redirect the write
        outside the mount) or a directory (``mv`` would move the new file
        *inside* it and exit 0, reporting success while the control file stays
        unreadable). A probe that cannot be completed is also an error, since
        its silence proves nothing.

        Those checks are made twice — once up front and once immediately before
        the rename — and the result is verified afterwards. GNU coreutils can
        refuse a directory destination outright (``mv -T``) and that path is
        taken when available; elsewhere a swap in the instant before the rename
        is detected rather than prevented, and never reported as success.

        Known limit: a peer could point our temp directory's name at a *different
        directory we own*, which passes the ownership check. That costs nothing
        (they cannot reach inside a directory of ours) beyond the publish
        possibly crossing filesystems. Closing it would need fd-relative
        operations, which a shell cannot express — it would mean driving this
        over SFTP instead of ssh.

        The parent directory is assumed to exist (for the owner-marker case
        the rsync that just ran guarantees it).

        Raises:
            ValueError: If *remote_path* is login-relative. The write anchors
                its working directory inside a private temp directory, which
                would change how such a path resolves.
            RuntimeError: If the target is a symlink or a directory, if the
                target could not be probed, or if any write / publish step
                exits non-zero — so the caller surfaces the failure instead of
                silently leaving a stale file behind.
        """
        if not remote_path.startswith(("/", "~")):
            # The write anchors its working directory inside a private temp
            # directory (so the directory entry cannot be swapped out from under
            # it), which changes how a login-relative path would resolve: the
            # rename would land inside the temp directory and the verification
            # would look somewhere else entirely. Rejecting is better than
            # writing the file somewhere the caller did not ask for.
            raise ValueError(
                f"remote_path must be absolute or ~-relative, got "
                f"{remote_path!r} — a login-relative path cannot be resolved "
                "safely by this writer"
            )

        quoted = shlex.quote(remote_path)
        prefix = shlex.quote(f"{PurePosixPath(remote_path).parent}/.srunx-write.")

        # The whole guarded sequence runs in ONE ssh invocation. Split across
        # several it cost that many connections — that many key exchanges, and
        # that many hardware-key touches — on *every* synced submission, since
        # the owner marker is rewritten each time. Sharing one shell also shrinks
        # the window between checking the target and renaming over it to near
        # zero, and lets the checks repeat immediately before the rename.
        #
        # The temp lives inside a directory we create with ``mkdir -m 700``, and
        # that is what makes writing it safe. Two earlier attempts were not:
        #
        # 1. ``mktemp`` then ``chmod`` then ``cat`` — mktemp closes the file, so
        #    the later steps reopened it **by name**. Another account with write
        #    access to the directory could unlink it and leave a symlink in that
        #    gap. Verified locally: the sequence truncated an unrelated file
        #    through the planted link — arbitrary overwrite, not just a broken
        #    marker.
        # 2. A single ``set -C`` redirection — noclobber only refuses an existing
        #    *regular* file (POSIX XCU 2.7.2). Verified locally: with a FIFO
        #    planted at the predictable name, the redirection did not fail, it
        #    **hung** in open(). An attacker could stall every sync, or read the
        #    marker content, and then have ``mv`` publish their object.
        #
        # ``mkdir`` is the exclusive-create primitive that covers every kind of
        # inode: verified to fail with "File exists" against both a planted FIFO
        # and a planted symlink. Mode 700 means nobody else can enter the
        # directory afterwards, so the file created inside it cannot be swapped —
        # no name is reopened in an untrusted directory at any point.
        #
        # ``umask 022`` fixes the marker's mode at creation, since it must stay
        # readable to other accounts on a shared mount: an unreadable marker
        # reads as "no owner" and disables the guard for them.
        #
        # The directory sits beside the target so publishing is a rename within
        # one filesystem — across filesystems ``mv`` becomes copy+unlink and
        # stops being atomic.
        #
        # Cleanup removes the known filename and then ``rmdir``s, rather than
        # ``rm -rf``, so an unexpected entry is never deleted recursively.
        #
        # Exit codes are distinct so the failure can be reported precisely
        # rather than as one opaque "ssh failed".
        script = (
            f"set -u; "
            f"if [ -h {quoted} ]; then exit 3; fi; "
            f"if [ -d {quoted} ]; then exit 4; fi; "
            f"d={prefix}$$; "
            f'mkdir -m 700 -- "$d" || exit 5; '
            # ``cd`` into the directory we just created and work in relative
            # paths from here on. Mode 700 stops others entering it, but it does
            # NOT protect the directory *entry*: write permission on the parent
            # lets a watcher rename our directory away and recreate one under the
            # same name, after which a path like "$d/file" would resolve into
            # theirs. A shell's working directory follows the inode, not the
            # name, so nothing below reopens a path an attacker controls.
            f'cd -- "$d" || {{ rmdir -- "$d"; exit 5; }}; '
            # ``mkdir`` then ``cd`` is itself a TOCTOU: write permission on the
            # parent lets a peer rename our entry away and put their own
            # directory (or a symlink) under the same name before we open it. We
            # would then create, chmod and write inside *their* directory, where
            # they can swap the file for a symlink — back to the arbitrary
            # overwrite this whole sequence exists to prevent. So verify what we
            # actually entered: a peer can only create directories owned by
            # themselves, so a uid match rules that out. Compared via
            # ``ls -ldn`` + ``id -u`` because ``test -O`` is a bash/ksh
            # extension, absent from POSIX test and from dash — the /bin/sh on
            # most Linux clusters. No cleanup here: if this fails we are standing
            # in someone else's directory and must not delete anything in it.
            f'[ "$(ls -ldn . | awk \'NR==1{{print $3}}\')" = "$(id -u)" ] '
            f"|| exit 5; "
            # Random basename via mktemp — safe to use here precisely because
            # this directory is ours and unreachable by others, which was not
            # true of the mount root. It matters because on a non-GNU ``mv``
            # (BSD, BusyBox) a target swapped for a symlink-to-directory makes
            # the publish land at ``<referent>/<basename>``: a predictable name
            # like the PID could be guessed and made to collide with a file the
            # attacker wants destroyed. ``$$`` was no good — it is already
            # exposed in the directory name.
            f'f=$(mktemp -- ./w.XXXXXX) || {{ cd /; rmdir -- "$d"; exit 5; }}; '
            # mktemp creates 0600; the published marker must stay readable to
            # other accounts on a shared mount, since an unreadable marker reads
            # as "no owner" and disables the guard for them. Reopening by name is
            # fine *inside* this directory — the risk it carried at the mount
            # root does not exist where no one else can reach.
            # No ``--`` here: BSD chmod does not accept it and treats it as a
            # filename ("chmod: --: No such file or directory"), which failed
            # every write on macOS-family remotes. Safe to omit because mktemp's
            # template starts with "./", so the name can never look like a flag.
            f'chmod 644 "$f" || {{ rm -f -- "$f"; cd /; rmdir -- "$d"; exit 9; }}; '
            f'cat > "$f" || {{ rm -f -- "$f"; cd /; rmdir -- "$d"; exit 6; }}; '
            # Re-check right before the rename: a target swapped in after the
            # first check would otherwise be followed by ``mv``.
            f"if [ -h {quoted} ] || [ -d {quoted} ]; then "
            f'rm -f -- "$f"; cd /; rmdir -- "$d"; exit 3; fi; '
            # GNU coreutils can refuse a directory destination outright with
            # ``-T``, which closes the swap race instead of only detecting it.
            # ``mv --version`` is the reliable probe: it succeeds on GNU and
            # fails on BSD, where ``-T`` does not exist (verified — BSD reports
            # "illegal option -- T"). Blindly retrying without ``-T`` on any
            # error would be wrong, since "no such option" and "target is a
            # directory" would be indistinguishable.
            f"if mv --version >/dev/null 2>&1; then "
            f'mv -fT -- "$f" {quoted} '
            f'|| {{ rm -f -- "$f"; cd /; rmdir -- "$d"; exit 7; }}; '
            f"else "
            f'mv -f -- "$f" {quoted} '
            f'|| {{ rm -f -- "$f"; cd /; rmdir -- "$d"; exit 7; }}; '
            f"fi; "
            # Leave the directory before removing it, or the rmdir can fail with
            # the working directory still inside it.
            f'cd /; rmdir -- "$d" 2>/dev/null; '
            # Verify what the rename actually produced. If the target was
            # swapped for a directory (or a symlink to one) in the instant
            # between the recheck above and this rename, ``mv`` moved the temp
            # *inside* it and exited 0. Preventing that portably is not
            # possible — ``mv -T`` is a GNU extension, and its failure cannot
            # be told apart from "target is a directory", so falling back on
            # error would silently drop the guard on BSD. Detecting it is
            # portable, and refusing to call that success is what matters:
            # otherwise the caller believes the marker was updated when the
            # content landed somewhere else entirely.
            # Nothing is deleted on this path, on purpose. An earlier version
            # removed ``<target>/marker`` to avoid leaving the relocated file
            # behind, but if the target was swapped for a symlink to some other
            # directory, that path resolves to an unrelated pre-existing file and
            # the cleanup deletes it — under the syncing user's credentials, and
            # possibly outside the mount. A leftover file costs disk space; a
            # wrong ``rm`` costs data. The error tells the operator what to
            # inspect instead.
            f"if [ ! -f {quoted} ] || [ -h {quoted} ]; then exit 8; fi"
        )
        result = self._ssh_run(script, stdin=content)
        if result.returncode == 0:
            return

        detail = result.stderr.strip()
        reasons = {
            3: (
                f"refusing to write {remote_path!r}: it is a symlink or a "
                "directory. Following a symlink could write outside the mount, "
                "and renaming onto a directory would move the file inside it "
                "while reporting success"
            ),
            4: (
                f"refusing to write {remote_path!r}: it exists as a directory, "
                "so publishing would move the new file inside it instead of "
                "replacing it"
            ),
            5: (
                f"could not set up the private temp directory beside "
                f"{remote_path!r} — anything already occupying that name "
                "(file, symlink, FIFO) lands here, since the directory is "
                "created exclusively, and so does a directory that turned out "
                "not to be owned by this account (which means another user "
                "swapped it in between creating and entering it)"
            ),
            6: f"could not write the temp file beside {remote_path!r}",
            9: (
                f"could not make the replacement for {remote_path!r} readable "
                "(chmod failed); publishing it owner-only would hide the marker "
                "from other accounts sharing the mount, which reads as "
                '"no owner" and disables the guard for them'
            ),
            7: f"could not publish {remote_path!r} (rename failed)",
            8: (
                f"published {remote_path!r} but it is not a regular file "
                "afterwards — something replaced the target mid-write, so the "
                "content may have landed elsewhere and this file was NOT "
                "updated. Nothing was deleted in response, since the swapped-in "
                "path could point anywhere; inspect it on the cluster and remove "
                "any stray 'marker' file yourself before retrying"
            ),
        }
        reason = reasons.get(
            result.returncode,
            f"ssh write to {remote_path!r} failed",
        )
        raise RuntimeError(
            f"{reason} (exit {result.returncode})" + (f": {detail}" if detail else "")
        )

    # Custom exit codes wired into the remote shell snippet for
    # remote_sha256. Chosen above the common shell-defined range
    # (1=generic error, 2=misuse, 126/127=can't exec / not found,
    # 255=ssh) so they can't be confused with a genuine failure mode
    # we'd want to surface as RuntimeError.
    _SHA256_REMOTE_MISSING_EXIT = 10
    _SHA256_REMOTE_NO_TOOL_EXIT = 11

    # sha256sum / shasum -a 256 prefix every output line with the 64-hex
    # digest followed by whitespace. Anchored at start of line so we
    # don't accidentally match a hex-looking substring inside a path.
    _SHA256_HEX_RE = re.compile(r"^([0-9a-f]{64})\b", re.IGNORECASE)

    def remote_sha256(self, remote_path: str) -> str | None:
        """Return the SHA-256 hex digest of *remote_path*, or ``None``.

        Used by :func:`srunx.sync.hash_verify.verify_paths_match`
        (#137 part 5) to detect the silent-rsync-failure case where
        rsync exits 0 but the specific file we're about to ``sbatch``
        never reached the cluster (excluded by a stray rule, lost to
        a path-translation bug, …). A None return defers to the
        caller, which decides whether "missing" or "no tool" should
        block submission.

        Returns:
            The 64-char lowercase hex digest on success.
            ``None`` when the file does not exist on the remote.
            ``None`` when neither ``sha256sum`` nor ``shasum -a 256``
            is available on the remote PATH (logged at debug — the
            rsync that just succeeded is the user's main signal).

        Raises:
            RuntimeError: For any other ssh / network failure (connection
                refused, host key mismatch, host unreachable, …). Callers
                that want "best effort" can catch and downgrade; the
                marker-read code in :func:`check_owner` is the prior art
                for that pattern.
        """
        quoted = shlex.quote(remote_path)
        # Single round-trip: existence check, then prefer sha256sum
        # (Linux), fall back to shasum -a 256 (macOS / BSD). Custom
        # exit codes disambiguate "file missing" and "no tool" from
        # genuine failures so the Python side doesn't have to grep
        # stderr to make that distinction.
        # Written on ONE line. A multi-line script breaks on csh/tcsh login
        # shells even inside ``sh -c '...'``: csh cannot carry a newline through
        # single quotes, so it splits the text and parses the fragments itself.
        # Verified against tcsh — the newline form produced ``Unmatched '``,
        # ``Ambiguous output redirect`` and ``else: endif not found``, and ran
        # part of the script as separate commands.
        script = (
            f"test -f {quoted} || exit {self._SHA256_REMOTE_MISSING_EXIT}; "
            f"if command -v sha256sum >/dev/null 2>&1; then "
            f"sha256sum -- {quoted}; "
            f"elif command -v shasum >/dev/null 2>&1; then "
            f"shasum -a 256 -- {quoted}; "
            f"else exit {self._SHA256_REMOTE_NO_TOOL_EXIT}; fi"
        )
        result = self._ssh_run(script)
        if result.returncode == 0:
            match = self._SHA256_HEX_RE.match(result.stdout.strip())
            if match is None:
                # Unparseable output is a genuine failure — sha256sum
                # / shasum surfaced something we don't understand,
                # better to fail loud than silently fall through to
                # "no hash".
                raise RuntimeError(
                    f"could not parse sha256 output for {remote_path!r}: "
                    f"{result.stdout.strip()!r}"
                )
            return match.group(1).lower()
        if result.returncode == self._SHA256_REMOTE_MISSING_EXIT:
            return None
        if result.returncode == self._SHA256_REMOTE_NO_TOOL_EXIT:
            logger.debug(
                "Remote sha256 verification skipped for {}: "
                "neither sha256sum nor shasum available on remote PATH",
                remote_path,
            )
            return None
        raise RuntimeError(
            f"ssh sha256 of {remote_path!r} failed "
            f"(exit {result.returncode}): {result.stderr.strip()}"
        )

    def effective_excludes(
        self, exclude_patterns: Sequence[str] | None = None
    ) -> list[str]:
        """Return the patterns a call passing *exclude_patterns* would filter on.

        ``push`` / ``pull`` merge per-call patterns on top of the instance's for
        that invocation only, without storing them, so
        :attr:`exclude_patterns` alone under-reports what a given call actually
        filtered on — it omits exactly the mount-level patterns a user
        configured.

        That matters wherever the filter is reported back to a user: an excluded
        path is invisible to an inspection *and* protected from a mirror's
        deletions, so a missing pattern makes the report read as "in sync" when
        it really means "never looked at".
        """
        return list(self._merge_excludes(exclude_patterns))

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
