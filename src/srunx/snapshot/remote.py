"""Remote side of snapshots: POSIX ``sh`` script builders and the store that runs them.

Every step that needs the per-repo lock is one script that acquires the lock,
does its work and releases it through a ``trap``. The client never holds the
lock across round trips, so nothing ever has to break a stale lock: a client
that dies takes its ssh session (and so the script's shell) with it, and the
shell's HUP trap releases the lock. A script killed with SIGKILL, or a node
crash, leaves ``.lock`` behind; that is cleared by hand (the lock-timeout
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
from typing import Literal, Protocol

from loguru import logger

from srunx.snapshot.errors import (
    SnapshotLockTimeoutError,
    SnapshotRemoteError,
    SnapshotValidationError,
)
from srunx.snapshot.lease import Lease

DEFAULT_SNAPSHOT_ROOT = "~/.srunx/snapshots"
# Shared charset with ``srunx.ssh.core.config.validate_snapshot_root``.
SNAPSHOT_ROOT_RE = r"(~|~/[A-Za-z0-9._/+-]*|/[A-Za-z0-9._/+-]*)"
REPO_ID_RE = r"[A-Za-z0-9._-]+"
MIN_REMOTE_GIT = (2, 17)
LOCK_WAIT_SECONDS = 120
PROBE_TIMEOUT = LOCK_WAIT_SECONDS + 60
COMMIT_TIMEOUT = LOCK_WAIT_SECONDS + 600
LEASE_TIMEOUT = LOCK_WAIT_SECONDS + 60
PUSH_TIMEOUT = 900

_SHA_RE = re.compile(r"[0-9a-f]{40}")
_HEX32_RE = re.compile(r"[0-9a-f]{32}")
_REPO_DIR_RE = re.compile(r"/[A-Za-z0-9._/+-]+")


def _check(pattern: str | re.Pattern[str], value: str, what: str) -> str:
    if not re.fullmatch(pattern, value):
        raise ValueError(f"invalid {what}: {value!r}")
    return value


def _check_snapshot_root(value: str) -> str:
    _check(SNAPSHOT_ROOT_RE, value, "snapshot_root")
    if ".." in value.split("/"):
        raise ValueError(f"invalid snapshot_root (contains '..'): {value!r}")
    return value


def _fill(template: str, **values: str) -> str:
    for key, val in values.items():
        template = template.replace(f"@{key}@", val)
    return template


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


def _owner_args(client_host: str, client_pid: int, token: str) -> tuple[str, str, str]:
    _check(_HEX32_RE, token, "lock token")
    host = "".join(c for c in client_host if c.isprintable()) or "unknown"
    return shlex.quote(host), str(int(client_pid)), token


_LOCK_PREAMBLE = r"""set -u
LOCKDIR=
held=0
release() {
  if [ "$held" = 1 ]; then
    if [ ! -f "$LOCKDIR/owner" ] || grep -q '^token=@TOKEN@$' "$LOCKDIR/owner" 2>/dev/null; then
      rm -f "$LOCKDIR/owner"
      rmdir "$LOCKDIR" 2>/dev/null
    fi
  fi
  held=0
}
trap release EXIT
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM
acquire() {
  if [ ! -d "${LOCKDIR%/*}" ]; then
    echo "directory for lock does not exist: ${LOCKDIR%/*}" >&2
    exit 22
  fi
  waited=0
  until mkdir "$LOCKDIR" 2>/dev/null; do
    if [ "$waited" -ge @WAIT@ ]; then
      echo "LOCK=$LOCKDIR" >&2
      if [ -f "$LOCKDIR/owner" ]; then cat "$LOCKDIR/owner" >&2; else echo "owner file missing" >&2; fi
      exit 30
    fi
    sleep 1
    waited=$((waited + 1))
  done
  held=1
  {
    printf 'host=%s\n' @HOST@
    printf 'pid=%s\n' @PID@
    printf 'ts=%s\n' "$(date +%s)"
    printf 'time=%s\n' "$(date -u)"
    printf 'token=%s\n' '@TOKEN@'
  } > "$LOCKDIR/owner"
}
"""


def _lock_preamble(
    *, client_host: str, client_pid: int, token: str, lock_wait: int
) -> str:
    host, pid, tok = _owner_args(client_host, client_pid, token)
    return _fill(
        _LOCK_PREAMBLE, TOKEN=tok, WAIT=str(int(lock_wait)), HOST=host, PID=pid
    )


# Lease write, run inside a locked script. Needs $REPO and $LEASE_ID/$LEASE_JSON.
_WRITE_LEASE = r"""write_lease() {
  LEASES=$REPO/leases
  TARGET=$LEASES/$LEASE_ID.json
  if [ -L "$LEASES" ] || [ ! -d "$LEASES" ] || [ ! -O "$LEASES" ]; then
    echo "leases directory is missing, a symlink, or not owned by us: $LEASES" >&2
    exit 31
  fi
  if [ -L "$TARGET" ] || [ -d "$TARGET" ]; then
    echo "refusing to overwrite lease target: $TARGET" >&2
    exit 31
  fi
  umask 077
  TMP=$LEASES/.tmp.$LEASE_ID.$$
  if ! printf '%s\n' "$LEASE_JSON" > "$TMP"; then
    rm -f "$TMP"
    echo "cannot write lease" >&2
    exit 31
  fi
  if ! mv -f "$TMP" "$TARGET"; then
    rm -f "$TMP"
    echo "cannot move lease into place" >&2
    exit 31
  fi
  if [ ! -f "$TARGET" ] || [ -L "$TARGET" ]; then
    echo "lease is not a regular file after write: $TARGET" >&2
    exit 31
  fi
}
"""

_PROBE_BODY = r"""resolve_path() {
  rp_in=$(printf '%s' "$1" | sed 's:/*$::')
  [ -n "$rp_in" ] || rp_in=/
  RESOLVED=
  case $rp_in in /*) ;; *) return 1 ;; esac
  rp_rest=
  while :; do
    if [ -d "$rp_in" ]; then break; fi
    if [ -e "$rp_in" ] || [ -L "$rp_in" ]; then return 1; fi
    rp_base=${rp_in##*/}
    case $rp_base in ''|.|..) return 1 ;; esac
    rp_rest=/$rp_base$rp_rest
    rp_in=${rp_in%/*}
    [ -n "$rp_in" ] || rp_in=/
  done
  rp_phys=$(cd "$rp_in" 2>/dev/null && pwd -P) || return 1
  [ -n "$rp_phys" ] || return 1
  if [ "$rp_phys" = / ]; then
    RESOLVED=$rp_rest
    [ -n "$RESOLVED" ] || RESOLVED=/
  else
    RESOLVED=$rp_phys$rp_rest
  fi
}
check_overlap() {
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
  *) echo "git was not found on the remote PATH" >&2; exit 20 ;;
esac
GITVER=$("$GIT" --version 2>/dev/null | sed -n 's/^git version \([0-9][0-9.]*\).*/\1/p')
gv_major=${GITVER%%.*}
gv_rest=${GITVER#*.}
gv_minor=${gv_rest%%.*}
case "$gv_major$gv_minor" in
  ''|*[!0-9]*) echo "cannot parse remote git version: $GITVER" >&2; exit 21 ;;
esac
if [ "$gv_major" -lt @MIN_MAJOR@ ] || { [ "$gv_major" -eq @MIN_MAJOR@ ] && [ "$gv_minor" -lt @MIN_MINOR@ ]; }; then
  echo "remote git $GITVER is older than @MIN_MAJOR@.@MIN_MINOR@" >&2
  exit 21
fi
ROOT_IN=@ROOT_EXPR@
resolve_path "$ROOT_IN" || { echo "snapshot root cannot be resolved: $ROOT_IN" >&2; exit 22; }
ROOT=$RESOLVED
check_mounts || exit 23
mkdir -p "$ROOT" || { echo "cannot create snapshot root: $ROOT" >&2; exit 22; }
resolve_path "$ROOT" || { echo "snapshot root cannot be resolved: $ROOT" >&2; exit 22; }
ROOT=$RESOLVED
case $ROOT in
  *[!A-Za-z0-9._/+-]*) echo "snapshot root has characters outside [A-Za-z0-9._/+-]: $ROOT" >&2; exit 22 ;;
esac
check_mounts || exit 23
REPO=$ROOT/@REPO_ID@
mkdir -p "$REPO/trees" || { echo "cannot create $REPO/trees" >&2; exit 22; }
mkdir -p -m 700 "$REPO/leases" || { echo "cannot create $REPO/leases" >&2; exit 22; }
LOCKDIR=$REPO/.lock
if [ ! -d "$REPO/repo.git" ]; then
  acquire
  if [ ! -d "$REPO/repo.git" ]; then
    INITTMP=$REPO/.repo.git.tmp.$$
    rm -rf "$INITTMP"
    if ! "$GIT" init --bare -q "$INITTMP" || ! mv "$INITTMP" "$REPO/repo.git"; then
      rm -rf "$INITTMP"
      echo "git init failed in $REPO" >&2
      exit 24
    fi
  fi
  release
fi
"$GIT" --git-dir="$REPO/repo.git" rev-parse --git-dir >/dev/null 2>&1 || { echo "$REPO/repo.git is not a valid git repository" >&2; exit 24; }
HAVE=0
if "$GIT" --git-dir="$REPO/repo.git" cat-file -e '@SHA@^{commit}' 2>/dev/null; then HAVE=1; fi
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
    _check(_SHA_RE, sha, "sha")
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
        + body
    )


def _repo_vars(repo_dir: str, git: str | None, sha: str | None) -> str:
    _check(_REPO_DIR_RE, repo_dir, "repo_dir")
    out = f"REPO={shlex.quote(repo_dir)}\nLOCKDIR=$REPO/.lock\nGITDIR=$REPO/repo.git\n"
    if git is not None:
        if not git.startswith("/") or "\n" in git:
            raise ValueError(f"git must be an absolute path: {git!r}")
        out += f"GIT={shlex.quote(git)}\n"
    if sha is not None:
        _check(_SHA_RE, sha, "sha")
        out += f"SHA={sha}\nTREE=$REPO/trees/$SHA\nMARK=$REPO/trees/$SHA.complete\n"
    return out


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
  exit 26
fi
if [ -f "$MARK" ] && [ -d "$TREE" ]; then
  STATUS=reused
else
  STATUS=created
  if [ -e "$TREE" ] || [ -L "$TREE" ] || [ -e "$MARK" ] || "$GIT" --git-dir="$GITDIR" worktree list --porcelain 2>/dev/null | grep -qxF "worktree $TREE"; then
    STATUS=recovered
    if [ -d "$TREE" ] && [ ! -L "$TREE" ]; then chmod -R u+w "$TREE" 2>/dev/null; fi
    "$GIT" --git-dir="$GITDIR" worktree remove --force "$TREE" >/dev/null 2>&1 || rm -rf "$TREE"
    rm -f "$MARK"
    "$GIT" --git-dir="$GITDIR" worktree prune --expire=now >/dev/null 2>&1
  fi
  if ! "$GIT" --git-dir="$GITDIR" worktree add --detach -q "$TREE" "$SHA" >&2; then
    echo "git worktree add failed for $TREE" >&2
    exit 27
  fi
  chmod -R a-w "$TREE" || { echo "chmod -R a-w failed for $TREE" >&2; exit 28; }
  : > "$MARK" || { echo "cannot create marker $MARK" >&2; exit 29; }
fi
LEASE=
@WRITE_LEASE_CALL@printf 'STATUS=%s\n' "$STATUS"
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
        + _WRITE_LEASE
        + _fill(_COMMIT_BODY, WRITE_LEASE_CALL=call)
    )


_BIND_BODY = r"""acquire
if [ -f "$MARK" ]; then SNAPSHOT=present; else SNAPSHOT=missing; fi
write_lease
printf 'SNAPSHOT=%s\n' "$SNAPSHOT"
"""


def build_bind_script(
    *,
    repo_dir: str,
    sha: str,
    lease_json: str,
    lease_id: str,
    client_host: str,
    client_pid: int,
    token: str,
    lock_wait: int = LOCK_WAIT_SECONDS,
) -> str:
    """Rewrite the lease as bound; report whether the snapshot marker still exists."""
    return (
        _lock_preamble(
            client_host=client_host,
            client_pid=client_pid,
            token=token,
            lock_wait=lock_wait,
        )
        + _repo_vars(repo_dir, None, sha)
        + _lease_vars(lease_json, lease_id)
        + _WRITE_LEASE
        + _BIND_BODY
    )


_DELETE_BODY = r"""acquire
rm -f "$REPO/leases/$LEASE_ID.json"
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
        + _DELETE_BODY
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


_EXIT_MESSAGES = {
    20: "git was not found on the remote PATH (git >= {min} is required)",
    21: "remote git is too old (git >= {min} is required)",
    22: "the snapshot root is invalid or unusable",
    23: "the snapshot root overlaps a mount, or their relation could not be determined",
    24: "could not initialise the bare repository on the remote",
    26: "the commit is missing from the remote repository",
    27: "git worktree add failed on the remote",
    28: "chmod -R a-w failed on the remote snapshot",
    29: "could not create the snapshot completion marker",
    31: "could not write the lease (refused or failed)",
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
        try:
            self._root_expr = root_expr(snapshot_root)
        except ValueError as exc:
            raise SnapshotValidationError(str(exc)) from exc

    @staticmethod
    def lease_path(repo_dir: str, lease_id: str) -> str:
        return f"{repo_dir}/leases/{lease_id}.json"

    def _owner(self) -> dict[str, object]:
        return {
            "client_host": socket.gethostname(),
            "client_pid": os.getpid(),
            "token": secrets.token_hex(16),
        }

    def _run(self, script: str, *, timeout: float, what: str) -> dict[str, str]:
        try:
            proc = self._shell.run_script(script, timeout=timeout)
        except subprocess.TimeoutExpired as exc:
            raise SnapshotRemoteError(
                f"remote {what} timed out after {timeout:g}s"
            ) from exc
        except OSError as exc:
            raise SnapshotRemoteError(f"remote {what} could not run: {exc}") from exc
        if proc.returncode != 0:
            self._raise_for(proc.returncode, proc.stderr or "", what)
        return _parse_kv(proc.stdout or "")

    def _raise_for(self, code: int, stderr: str, what: str) -> None:
        stderr = stderr.strip()
        if code == 30:
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
        template = _EXIT_MESSAGES.get(code)
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
            **self._owner(),  # type: ignore[arg-type]
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
        _check(_SHA_RE, sha, "sha")
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
            **self._owner(),  # type: ignore[arg-type]
        )
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
            sha=lease.sha,
            lease_json=lease.to_json(),
            lease_id=lease.lease_id,
            lock_wait=self._lock_wait,
            **self._owner(),  # type: ignore[arg-type]
        )
        out = self._run(script, timeout=LEASE_TIMEOUT, what="lease bind")
        self._need(out, "SNAPSHOT", what="lease bind")
        return out["SNAPSHOT"] == "present"

    def delete_lease(self, *, repo_dir: str, lease_id: str) -> None:
        script = build_delete_lease_script(
            repo_dir=repo_dir,
            lease_id=lease_id,
            lock_wait=self._lock_wait,
            **self._owner(),  # type: ignore[arg-type]
        )
        self._run(script, timeout=LEASE_TIMEOUT, what="lease delete")
