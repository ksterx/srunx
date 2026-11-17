"""Remote side of snapshots: POSIX ``sh`` script builders and the store that runs them.

Every step that needs the per-repo lock is one script that acquires the lock,
does its work and releases it through a ``trap``. The client never holds the
lock across round trips, so nothing ever has to break a stale lock. Without a
pty, sshd does not send SIGHUP when the client disconnects; the script instead
dies of SIGPIPE on its next write to stdout. The EXIT trap releases the lock
for HUP / INT / TERM / PIPE (each trap converts the signal to an ``exit``), and
each script also calls ``release`` before it writes its result lines so the
lock never depends on a write succeeding. A script killed with SIGKILL, or a
node crash, leaves ``.lock`` behind; that is cleared by hand (the lock-timeout
message names the path and the owner).
"""

from __future__ import annotations

import os
import posixpath
import re
import secrets
import shlex
import socket
import subprocess
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, Protocol, TypedDict

from loguru import logger

from srunx.snapshot._shell import (
    HEX32_RE as _HEX32_RE,
)
from srunx.snapshot._shell import (
    MAKE_DIRS_WRITABLE as _MAKE_DIRS_WRITABLE,
)
from srunx.snapshot._shell import (
    MTIME_OF as _MTIME_OF,
)
from srunx.snapshot._shell import (
    REPO_DIR_RE as _REPO_DIR_RE,
)
from srunx.snapshot._shell import (
    RESOLVE_PATH as _RESOLVE_PATH,
)
from srunx.snapshot._shell import (
    TREE_INTACT as _TREE_INTACT,
)
from srunx.snapshot._shell import (
    RemoteExit,
)
from srunx.snapshot._shell import (
    check as _check,
)
from srunx.snapshot._shell import (
    fill as _fill,
)
from srunx.snapshot._shell import (
    lock_preamble as _lock_preamble,
)
from srunx.snapshot._shell import (
    repo_vars as _repo_vars,
)
from srunx.snapshot.errors import (
    SnapshotLockTimeoutError,
    SnapshotRemoteError,
    SnapshotRemoteTimeoutError,
    SnapshotValidationError,
)
from srunx.snapshot.lease import Lease
from srunx.snapshot.local_git import REPO_ID_RE, SHA_RE
from srunx.ssh.core.config import validate_snapshot_root

DEFAULT_SNAPSHOT_ROOT = "~/.local/share/srunx/snapshots"
MIN_REMOTE_GIT = (2, 17)
LOCK_WAIT_SECONDS = 120
PROBE_TIMEOUT = LOCK_WAIT_SECONDS + 60
COMMIT_TIMEOUT = LOCK_WAIT_SECONDS + 600
LEASE_TIMEOUT = LOCK_WAIT_SECONDS + 60
PUSH_TIMEOUT = 900
INVENTORY_TIMEOUT = 300  # no lock; one git rev-parse per tree
PRUNE_TIMEOUT = LOCK_WAIT_SECONDS + 900  # lock wait + trash removal after release
# How far behind the login node's clock a refreshed marker may be before the
# touch is judged not to have happened (file servers can lag a little).
MARKER_SKEW_SECONDS = 300


def _check_snapshot_root(value: str) -> str:
    # Charset and ``..`` rules are shared with ServerProfile validation.
    validate_snapshot_root(value)
    return value


def root_expr(snapshot_root: str | None) -> str:
    """Return a shell word for the root: ``"$HOME"/'x'`` or ``'/abs'``."""
    root = _check_snapshot_root(snapshot_root or DEFAULT_SNAPSHOT_ROOT)
    if root == "~" or root.startswith("~/"):
        tail = posixpath.normpath("/" + root[2:]) if root != "~" else "/"
        return '"$HOME"' + ("" if tail == "/" else shlex.quote(tail))
    norm = posixpath.normpath(root)
    if norm in ("/", "//"):
        raise ValueError("snapshot_root must not be the filesystem root")
    return shlex.quote(norm)


def _literal(path: str) -> tuple[str, str] | None:
    """Split a path into (anchor, normalised-absolute-tail) for literal comparison."""
    if path == "~" or path.startswith("~/"):
        return "~", posixpath.normpath("/" + path[2:])
    if path.startswith("/"):
        return "/", posixpath.normpath(path)
    return None


def _is_within(child: str, parent: str) -> bool:
    return child == parent or child.startswith(parent.rstrip("/") + "/")


def literal_root_conflict(
    snapshot_root: str | None, mounts: Sequence[str]
) -> str | None:
    """Return a message when root and a mount overlap *as written*, else None.

    Only paths with the same anchor (``/`` or ``~``) can be compared without
    asking the remote; the probe repeats the check on physical paths.
    """
    root = _literal(snapshot_root or DEFAULT_SNAPSHOT_ROOT)
    if root is None:
        return None
    for mount in mounts:
        m = _literal(mount)
        if m is None or m[0] != root[0]:
            continue
        if _is_within(root[1], m[1]):
            return f"snapshot_root {snapshot_root!r} is inside the mount {mount!r}"
        if _is_within(m[1], root[1]):
            return f"the mount {mount!r} is inside snapshot_root {snapshot_root!r}"
    return None


def _mount_expr(mount: str) -> str:
    if mount == "~" or mount.startswith("~/"):
        tail = mount[1:]
        return '"$HOME"' + (shlex.quote(tail) if tail else "")
    return shlex.quote(mount)


class LockOwner(TypedDict):
    client_host: str
    client_pid: int
    token: str


# Lease write, run inside a locked script. Needs $REPO and $LEASE_ID/$LEASE_JSON.
_WRITE_LEASE = r"""write_lease() {
  LEASES=$REPO/leases
  TARGET=$LEASES/$LEASE_ID.json
  if [ -L "$LEASES" ] || [ ! -d "$LEASES" ] || [ ! -O "$LEASES" ]; then
    echo "leases directory is missing, a symlink, or not owned by us: $LEASES" >&2
    exit @EXIT_LEASE_WRITE@
  fi
  if [ -L "$TARGET" ] || [ -d "$TARGET" ]; then
    echo "refusing to overwrite lease target: $TARGET" >&2
    exit @EXIT_LEASE_WRITE@
  fi
  umask 077
  TMP=$LEASES/.tmp.$LEASE_ID.$$
  if ! printf '%s\n' "$LEASE_JSON" > "$TMP"; then
    rm -f "$TMP"
    echo "cannot write lease" >&2
    exit @EXIT_LEASE_WRITE@
  fi
  if ! mv -f "$TMP" "$TARGET"; then
    rm -f "$TMP"
    echo "cannot move lease into place" >&2
    exit @EXIT_LEASE_WRITE@
  fi
  if [ ! -f "$TARGET" ] || [ -L "$TARGET" ]; then
    echo "lease is not a regular file after write: $TARGET" >&2
    exit @EXIT_LEASE_WRITE@
  fi
}
"""

_PROBE_BODY = r"""check_overlap() {
  co_root=${1%/}
  co_mount=${2%/}
  case "$co_root/" in
    "$co_mount"/*) echo "snapshot root $1 is inside the mount $2" >&2; return 1 ;;
  esac
  case "$co_mount/" in
    "$co_root"/*) echo "the mount $2 is inside the snapshot root $1" >&2; return 1 ;;
  esac
  return 0
}
check_mounts() {
@MOUNT_CHECKS@  return 0
}
GIT=$(command -v git 2>/dev/null || true)
case $GIT in
  /*) ;;
  *) echo "git was not found on the remote PATH" >&2; exit @EXIT_GIT_MISSING@ ;;
esac
gv_line=$("$GIT" --version 2>/dev/null || true)
GITVER=${gv_line#git version }
GITVER=${GITVER%%[!0-9.]*}
gv_major=${GITVER%%.*}
gv_rest=${GITVER#*.}
gv_minor=${gv_rest%%.*}
case "$gv_major$gv_minor" in
  ''|*[!0-9]*) echo "cannot parse remote git version: $gv_line" >&2; exit @EXIT_GIT_TOO_OLD@ ;;
esac
if [ "$gv_major" -lt @MIN_MAJOR@ ] || { [ "$gv_major" -eq @MIN_MAJOR@ ] && [ "$gv_minor" -lt @MIN_MINOR@ ]; }; then
  echo "remote git $GITVER is older than @MIN_MAJOR@.@MIN_MINOR@" >&2
  exit @EXIT_GIT_TOO_OLD@
fi
ROOT_IN=@ROOT_EXPR@
if [ -L "${ROOT_IN%/}" ]; then
  echo "snapshot root is a symlink: $ROOT_IN (it must be a real directory; remove the symlink or choose a private snapshot_root). Fix: choose a private snapshot_root" >&2
  exit @EXIT_DIR_UNSAFE@
fi
resolve_path "$ROOT_IN" || { echo "snapshot root cannot be resolved: $ROOT_IN" >&2; exit @EXIT_ROOT_INVALID@; }
ROOT=$RESOLVED
check_mounts || exit @EXIT_ROOT_OVERLAP@
case $ROOT in
  *[!A-Za-z0-9._/+-]*) echo "snapshot root has characters outside [A-Za-z0-9._/+-]: $ROOT" >&2; exit @EXIT_ROOT_INVALID@ ;;
esac
check_ancestors "$ROOT"
(umask 077; mkdir -p "$ROOT") || { echo "cannot create snapshot root: $ROOT" >&2; exit @EXIT_ROOT_INVALID@; }
resolve_path "$ROOT" || { echo "snapshot root cannot be resolved: $ROOT" >&2; exit @EXIT_ROOT_INVALID@; }
ROOT=$RESOLVED
case $ROOT in
  *[!A-Za-z0-9._/+-]*) echo "snapshot root has characters outside [A-Za-z0-9._/+-]: $ROOT" >&2; exit @EXIT_ROOT_INVALID@ ;;
esac
check_mounts || exit @EXIT_ROOT_OVERLAP@
check_private_dir "$ROOT"
REPO=$ROOT/@REPO_ID@
for sd in "$REPO" "$REPO/trees" "$REPO/leases"; do
  if [ ! -e "$sd" ] && [ ! -L "$sd" ]; then
    mkdir -m 700 "$sd" || { echo "cannot create $sd" >&2; exit @EXIT_ROOT_INVALID@; }
  fi
done
check_store_dirs
LOCKDIR=$REPO/.lock
if [ ! -d "$REPO/repo.git" ]; then
  acquire
  if [ ! -d "$REPO/repo.git" ]; then
    INITTMP=$REPO/.repo.git.tmp.$$
    rm -rf "$INITTMP"
    if ! "$GIT" init --bare -q "$INITTMP" || ! mv "$INITTMP" "$REPO/repo.git"; then
      rm -rf "$INITTMP"
      echo "git init failed in $REPO" >&2
      exit @EXIT_REPO_INIT@
    fi
  fi
  release
fi
"$GIT" --git-dir="$REPO/repo.git" rev-parse --git-dir >/dev/null 2>&1 || { echo "$REPO/repo.git is not a valid git repository" >&2; exit @EXIT_REPO_INIT@; }
HAVE=0
if "$GIT" --git-dir="$REPO/repo.git" cat-file -e '@SHA@^{commit}' 2>/dev/null; then HAVE=1; fi
release
printf 'ROOT=%s\n' "$ROOT"
printf 'REPO=%s\n' "$REPO"
printf 'GIT=%s\n' "$GIT"
printf 'GITVER=%s\n' "$GITVER"
printf 'HAVE=%s\n' "$HAVE"
"""


def build_probe_script(
    *,
    root_expr: str,
    repo_id: str,
    sha: str,
    mounts: Sequence[str],
    client_host: str,
    client_pid: int,
    token: str,
    lock_wait: int = LOCK_WAIT_SECONDS,
) -> str:
    """Probe git, validate the root against the mounts, init the bare repo, test for the sha."""
    _check(REPO_ID_RE, repo_id, "repo_id")
    _check(SHA_RE, sha, "sha")
    checks = "".join(
        f"  resolve_path {_mount_expr(m)} || {{ echo 'mount path cannot be resolved: '{shlex.quote(m)} >&2; return 1; }}\n"
        '  check_overlap "$ROOT" "$RESOLVED" || return 1\n'
        for m in mounts
    )
    body = _fill(
        _PROBE_BODY,
        MOUNT_CHECKS=checks,
        MIN_MAJOR=str(MIN_REMOTE_GIT[0]),
        MIN_MINOR=str(MIN_REMOTE_GIT[1]),
        ROOT_EXPR=root_expr,
        REPO_ID=repo_id,
        SHA=sha,
    )
    return (
        _lock_preamble(
            client_host=client_host,
            client_pid=client_pid,
            token=token,
            lock_wait=lock_wait,
        )
        + _RESOLVE_PATH
        + body
    )


def _lease_vars(lease_json: str | None, lease_id: str | None) -> str:
    if lease_id is None:
        return ""
    _check(_HEX32_RE, lease_id, "lease_id")
    out = f"LEASE_ID={lease_id}\n"
    if lease_json is not None:
        out += f"LEASE_JSON={shlex.quote(lease_json)}\n"
    return out


_COMMIT_BODY = r"""acquire
if ! "$GIT" --git-dir="$GITDIR" cat-file -e "$SHA^{commit}" 2>/dev/null; then
  echo "commit $SHA is not in $GITDIR" >&2
  exit @EXIT_COMMIT_MISSING@
fi
if tree_intact; then
  STATUS=reused
  # The marker mtime means "last requested": refresh it so prune sees the reuse,
  # and refuse to hand out a snapshot whose refresh did not take.
  if ! touch "$MARK" 2>/dev/null; then
    echo "cannot refresh the snapshot marker $MARK" >&2
    exit @EXIT_MARKER_TOUCH@
  fi
  mk_new=$(mtime_of "$MARK")
  mk_now=$(date +%s)
  case $mk_new in
    ''|*[!0-9]*) echo "cannot read the marker mtime after touch: $MARK" >&2; exit @EXIT_MARKER_TOUCH@ ;;
  esac
  if [ "$mk_new" -lt $((mk_now - @SKEW@)) ]; then
    echo "marker mtime did not advance after touch: $MARK" >&2
    exit @EXIT_MARKER_TOUCH@
  fi
else
  STATUS=created
  if [ -e "$TREE" ] || [ -L "$TREE" ] || [ -e "$MARK" ] || "$GIT" --git-dir="$GITDIR" worktree list --porcelain 2>/dev/null | grep -qxF "worktree $TREE"; then
    STATUS=recovered
    # Drop links first so neither chmod nor worktree remove can follow them.
    if [ -L "$TREE" ]; then rm -f "$TREE"; fi
    if [ -d "$TREE" ]; then make_dirs_writable "$TREE"; fi
    "$GIT" --git-dir="$GITDIR" worktree remove --force "$TREE" >/dev/null 2>&1 || rm -rf "$TREE"
    rm -rf "$MARK"
    "$GIT" --git-dir="$GITDIR" worktree prune --expire=now >/dev/null 2>&1
  fi
  if ! "$GIT" --git-dir="$GITDIR" worktree add --detach -q "$TREE" "$SHA" >&2; then
    echo "git worktree add failed for $TREE" >&2
    exit @EXIT_WORKTREE_ADD@
  fi
  chmod -R a-w "$TREE" || { echo "chmod -R a-w failed for $TREE" >&2; exit @EXIT_CHMOD@; }
  : > "$MARK" || { echo "cannot create marker $MARK" >&2; exit @EXIT_MARKER@; }
fi
LEASE=
@WRITE_LEASE_CALL@release
printf 'STATUS=%s\n' "$STATUS"
printf 'PATH=%s\n' "$TREE"
printf 'LEASE=%s\n' "$LEASE"
"""


def build_commit_script(
    *,
    repo_dir: str,
    git: str,
    sha: str,
    lease_json: str | None,
    lease_id: str | None,
    client_host: str,
    client_pid: int,
    token: str,
    lock_wait: int = LOCK_WAIT_SECONDS,
) -> str:
    """Ensure the read-only worktree for *sha* exists; optionally write a provisional lease."""
    if (lease_json is None) != (lease_id is None):
        raise ValueError("lease_json and lease_id must be given together")
    call = "write_lease\nLEASE=$REPO/leases/$LEASE_ID.json\n" if lease_id else ""
    return (
        _lock_preamble(
            client_host=client_host,
            client_pid=client_pid,
            token=token,
            lock_wait=lock_wait,
        )
        + _repo_vars(repo_dir, git, sha)
        + _lease_vars(lease_json, lease_id)
        + _fill(_WRITE_LEASE)
        + _MTIME_OF
        + _MAKE_DIRS_WRITABLE
        + _TREE_INTACT
        + _fill(_COMMIT_BODY, WRITE_LEASE_CALL=call, SKEW=str(MARKER_SKEW_SECONDS))
    )


_BIND_BODY = r"""acquire
if tree_intact; then SNAPSHOT=present; else SNAPSHOT=missing; fi
write_lease
release
printf 'SNAPSHOT=%s\n' "$SNAPSHOT"
"""


def build_bind_script(
    *,
    repo_dir: str,
    git: str | None = None,
    sha: str,
    lease_json: str,
    lease_id: str,
    client_host: str,
    client_pid: int,
    token: str,
    lock_wait: int = LOCK_WAIT_SECONDS,
) -> str:
    """Rewrite the lease as bound; report whether the snapshot is still intact.

    *git* is the probed absolute path (as in the commit script); without it the
    remote ``PATH`` lookup is used.
    """
    fallback = "" if git else "GIT=$(command -v git 2>/dev/null || true)\n"
    return (
        _lock_preamble(
            client_host=client_host,
            client_pid=client_pid,
            token=token,
            lock_wait=lock_wait,
        )
        + _repo_vars(repo_dir, git, sha)
        + fallback
        + _lease_vars(lease_json, lease_id)
        + _fill(_WRITE_LEASE)
        + _TREE_INTACT
        + _fill(_BIND_BODY)
    )


_DELETE_BODY = r"""acquire
rm -f "$REPO/leases/$LEASE_ID.json"
release
"""


def build_delete_lease_script(
    *,
    repo_dir: str,
    lease_id: str,
    client_host: str,
    client_pid: int,
    token: str,
    lock_wait: int = LOCK_WAIT_SECONDS,
) -> str:
    """Remove one lease file under the lock."""
    return (
        _lock_preamble(
            client_host=client_host,
            client_pid=client_pid,
            token=token,
            lock_wait=lock_wait,
        )
        + _repo_vars(repo_dir, None, None)
        + _lease_vars(None, lease_id)
        + _fill(_DELETE_BODY)
    )


class RemoteShell(Protocol):
    """What the store needs from the transport (``RsyncClient`` in production)."""

    def run_script(
        self, script: str, *, timeout: float | None = None
    ) -> subprocess.CompletedProcess[str]: ...

    def ssh_command(self) -> list[str]: ...

    def remote_spec(self, path: str) -> str: ...


@dataclass(frozen=True)
class ProbeResult:
    root: str
    repo_dir: str
    git: str
    git_version: str
    has_commit: bool


@dataclass(frozen=True)
class CommitResult:
    path: str
    status: Literal["reused", "created", "recovered"]
    lease_path: str | None


_KNOWN_EXITS = frozenset(int(e) for e in RemoteExit)
_EXIT_MESSAGES: dict[RemoteExit, str] = {
    RemoteExit.GIT_MISSING: "git was not found on the remote PATH (git >= {min} is required)",
    RemoteExit.GIT_TOO_OLD: "remote git is too old (git >= {min} is required)",
    RemoteExit.ROOT_INVALID: "the snapshot root is invalid or unusable",
    RemoteExit.ROOT_OVERLAP: "the snapshot root overlaps a mount, or their relation could not be determined",
    RemoteExit.REPO_INIT: "could not initialise the bare repository on the remote",
    RemoteExit.COMMIT_MISSING: "the commit is missing from the remote repository",
    RemoteExit.WORKTREE_ADD: "git worktree add failed on the remote",
    RemoteExit.CHMOD: "chmod -R a-w failed on the remote snapshot",
    RemoteExit.MARKER: "could not create the snapshot completion marker",
    RemoteExit.DIR_UNSAFE: "a snapshot directory is not private (must be a real directory owned by you and not group/other-writable)",
    RemoteExit.LEASE_WRITE: "could not write the lease (refused or failed)",
    RemoteExit.MARKER_TOUCH: "could not refresh the snapshot's last-requested time; the snapshot was not reused",
}


def _parse_kv(stdout: str) -> dict[str, str]:
    out: dict[str, str] = {}
    for line in stdout.splitlines():
        key, sep, value = line.partition("=")
        if sep and re.fullmatch(r"[A-Z]+", key):
            out[key] = value
    return out


class RemoteSnapshotStore:
    """Runs the remote scripts and the push; maps every failure to a precise error."""

    def __init__(
        self,
        shell: RemoteShell,
        *,
        snapshot_root: str | None,
        mounts: Sequence[str],
        lock_wait: int = LOCK_WAIT_SECONDS,
    ) -> None:
        self._shell = shell
        self._mounts = list(mounts)
        self._lock_wait = lock_wait
        self._git: str | None = None  # probed path, remembered for bind
        try:
            self._root_expr = root_expr(snapshot_root)
        except ValueError as exc:
            raise SnapshotValidationError(str(exc)) from exc

    @staticmethod
    def lease_path(repo_dir: str, lease_id: str) -> str:
        return f"{repo_dir}/leases/{lease_id}.json"

    @property
    def root_expr(self) -> str:
        return self._root_expr

    @property
    def lock_wait(self) -> int:
        return self._lock_wait

    def owner(self) -> LockOwner:
        return {
            "client_host": socket.gethostname(),
            "client_pid": os.getpid(),
            "token": secrets.token_hex(16),
        }

    def _run(self, script: str, *, timeout: float, what: str) -> dict[str, str]:
        return _parse_kv(self.run(script, timeout=timeout, what=what))

    def run(self, script: str, *, timeout: float, what: str) -> str:
        """Run *script* remotely and return its raw stdout; failures map to errors."""
        try:
            proc = self._shell.run_script(script, timeout=timeout)
        except subprocess.TimeoutExpired as exc:
            partial = exc.stdout or ""
            if isinstance(partial, bytes):
                partial = partial.decode(errors="replace")
            raise SnapshotRemoteTimeoutError(
                f"remote {what} timed out after {timeout:g}s", partial
            ) from exc
        except OSError as exc:
            raise SnapshotRemoteError(f"remote {what} could not run: {exc}") from exc
        if proc.returncode != 0:
            self._raise_for(proc.returncode, proc.stderr or "", what)
        return proc.stdout or ""

    def _raise_for(self, code: int, stderr: str, what: str) -> None:
        stderr = stderr.strip()
        if code == RemoteExit.LOCK_TIMEOUT:
            lines = stderr.splitlines()
            lock = next(
                (ln[len("LOCK=") :] for ln in lines if ln.startswith("LOCK=")),
                "<unknown>",
            )
            owner = "\n".join(ln for ln in lines if not ln.startswith("LOCK="))
            raise SnapshotLockTimeoutError(
                f"could not acquire the snapshot lock {lock} within "
                f"{self._lock_wait}s during {what}.\n"
                f"Lock owner:\n{owner or 'unknown'}\n"
                "If that owner is no longer running, remove the lock by hand "
                f"(only after confirming): rm -rf {shlex.quote(lock)}"
            )
        template = (
            _EXIT_MESSAGES.get(RemoteExit(code)) if code in _KNOWN_EXITS else None
        )
        if template is not None:
            minimum = ".".join(str(n) for n in MIN_REMOTE_GIT)
            msg = f"{template.format(min=minimum)} ({what}, exit {code})"
        else:
            msg = f"remote {what} failed (exit {code})"
        if stderr:
            msg += f": {stderr}"
        raise SnapshotRemoteError(msg)

    @staticmethod
    def _need(values: dict[str, str], *keys: str, what: str) -> None:
        missing = [k for k in keys if k not in values]
        if missing:
            raise SnapshotRemoteError(
                f"remote {what} returned no {', '.join(missing)} (unexpected output)"
            )

    def probe(self, *, repo_id: str, sha: str) -> ProbeResult:
        script = build_probe_script(
            root_expr=self._root_expr,
            repo_id=repo_id,
            sha=sha,
            mounts=self._mounts,
            lock_wait=self._lock_wait,
            **self.owner(),
        )
        out = self._run(script, timeout=PROBE_TIMEOUT, what="probe")
        self._need(out, "ROOT", "REPO", "GIT", "GITVER", "HAVE", what="probe")
        for key in ("ROOT", "REPO"):
            if not _REPO_DIR_RE.fullmatch(out[key]):
                raise SnapshotRemoteError(
                    f"remote probe reported an unsafe {key} path: {out[key]!r}"
                )
        if not out["GIT"].startswith("/"):
            raise SnapshotRemoteError(
                f"remote git path is not absolute: {out['GIT']!r}"
            )
        if out["HAVE"] not in ("0", "1"):
            raise SnapshotRemoteError(f"remote probe returned HAVE={out['HAVE']!r}")
        return ProbeResult(
            root=out["ROOT"],
            repo_dir=out["REPO"],
            git=out["GIT"],
            git_version=out["GITVER"],
            has_commit=out["HAVE"] == "1",
        )

    def push(self, *, repo_top: Path, probe: ProbeResult, sha: str) -> None:
        _check(SHA_RE, sha, "sha")
        if not probe.git.startswith("/"):
            raise SnapshotRemoteError(f"remote git path is not absolute: {probe.git!r}")
        env = {
            **os.environ,
            "GIT_SSH_COMMAND": shlex.join(self._shell.ssh_command()),
            "GIT_TERMINAL_PROMPT": "0",
        }
        cmd = [
            "git",
            "-C",
            str(repo_top),
            "push",
            "--no-verify",
            "--quiet",
            f"--receive-pack={shlex.quote(probe.git)} receive-pack",
            self._shell.remote_spec(f"{probe.repo_dir}/repo.git"),
            f"{sha}:refs/srunx/snapshots/{sha}",
        ]
        try:
            proc = subprocess.run(  # noqa: S603
                cmd,
                capture_output=True,
                text=True,
                errors="replace",
                timeout=PUSH_TIMEOUT,
                env=env,
                check=False,
            )
        except subprocess.TimeoutExpired as exc:
            raise SnapshotRemoteError(
                f"git push timed out after {PUSH_TIMEOUT}s"
            ) from exc
        except OSError as exc:
            raise SnapshotRemoteError(f"git push could not run: {exc}") from exc
        if proc.returncode != 0:
            detail = (proc.stderr or proc.stdout or "").strip()
            raise SnapshotRemoteError(
                f"git push failed (exit {proc.returncode}): {detail}"
            )
        logger.debug("pushed {} to {}", sha, probe.repo_dir)

    def commit(
        self, *, probe: ProbeResult, sha: str, lease: Lease | None
    ) -> CommitResult:
        script = build_commit_script(
            repo_dir=probe.repo_dir,
            git=probe.git,
            sha=sha,
            lease_json=lease.to_json() if lease else None,
            lease_id=lease.lease_id if lease else None,
            lock_wait=self._lock_wait,
            **self.owner(),
        )
        self._git = probe.git
        out = self._run(script, timeout=COMMIT_TIMEOUT, what="snapshot commit")
        self._need(out, "STATUS", "PATH", what="snapshot commit")
        status = out["STATUS"]
        if status not in ("reused", "created", "recovered"):
            raise SnapshotRemoteError(f"remote commit returned STATUS={status!r}")
        return CommitResult(
            path=out["PATH"],
            status=status,  # type: ignore[arg-type]
            lease_path=out.get("LEASE") or None,
        )

    def bind(self, *, repo_dir: str, lease: Lease) -> bool:
        """Rewrite the lease; False when the snapshot's marker is gone."""
        script = build_bind_script(
            repo_dir=repo_dir,
            git=self._git,
            sha=lease.sha,
            lease_json=lease.to_json(),
            lease_id=lease.lease_id,
            lock_wait=self._lock_wait,
            **self.owner(),
        )
        out = self._run(script, timeout=LEASE_TIMEOUT, what="lease bind")
        self._need(out, "SNAPSHOT", what="lease bind")
        return out["SNAPSHOT"] == "present"

    def delete_lease(self, *, repo_dir: str, lease_id: str) -> None:
        script = build_delete_lease_script(
            repo_dir=repo_dir,
            lease_id=lease_id,
            lock_wait=self._lock_wait,
            **self.owner(),
        )
        self._run(script, timeout=LEASE_TIMEOUT, what="lease delete")
