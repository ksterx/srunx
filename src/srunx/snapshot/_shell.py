"""Shell fragments and helpers shared by every generated snapshot script.

The Phase 1 scripts (``remote.py``) and the prune scripts (``inventory.py``)
build on the same lock preamble, path resolution, mtime helper and tree check,
so they live here once instead of being imported across modules by private
name.
"""

from __future__ import annotations

import re
import shlex
from enum import IntEnum

from srunx.snapshot.local_git import SHA_RE

HEX32_RE = re.compile(r"[0-9a-f]{32}")
REPO_DIR_RE = re.compile(r"/[A-Za-z0-9._/+-]+")


class RemoteExit(IntEnum):
    """Exit codes of the generated remote scripts (substituted into the templates)."""

    GIT_MISSING = 20
    GIT_TOO_OLD = 21
    ROOT_INVALID = 22
    ROOT_OVERLAP = 23
    REPO_INIT = 24
    COMMIT_MISSING = 26
    WORKTREE_ADD = 27
    CHMOD = 28
    MARKER = 29
    LOCK_TIMEOUT = 30
    LEASE_WRITE = 31
    DIR_UNSAFE = 32
    MARKER_TOUCH = 33


PLACEHOLDER_RE = re.compile(r"@[A-Z][A-Z_]*@")


def check(pattern: str | re.Pattern[str], value: str, what: str) -> str:
    if not re.fullmatch(pattern, value):
        raise ValueError(f"invalid {what}: {value!r}")
    return value


def fill(template: str, **values: str) -> str:
    """Substitute ``@KEY@`` placeholders; ``@EXIT_*@`` come from :class:`RemoteExit`.

    Raises ``ValueError`` when the template has a placeholder nobody supplied,
    so a typo can never reach a remote shell as a literal ``@NAME@``.
    """
    all_values = {f"EXIT_{e.name}": str(int(e)) for e in RemoteExit} | values
    unknown = sorted(
        set(PLACEHOLDER_RE.findall(template)) - {f"@{k}@" for k in all_values}
    )
    if unknown:
        raise ValueError(f"unfilled template placeholders: {', '.join(unknown)}")
    for key, val in all_values.items():
        template = template.replace(f"@{key}@", val)
    return template


def owner_args(client_host: str, client_pid: int, token: str) -> tuple[str, str, str]:
    check(HEX32_RE, token, "lock token")
    host = "".join(c for c in client_host if c.isprintable()) or "unknown"
    return shlex.quote(host), str(int(client_pid)), token


LOCK_PREAMBLE = r"""set -u
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
trap 'exit 141' PIPE
trap 'exit 130' INT
trap 'exit 143' TERM
# A directory shields everything below it when other users cannot traverse it:
# no group/other execute (chars 7 and 10 are - or S/T) and no ACL ('+'; a
# trailing '.' SELinux or '@' xattr marker is fine). Owner is checked apart.
mode_shields() {
  case $1 in
    *+*) return 1 ;;
    ??????[-S]??[-T]*) return 0 ;;
  esac
  return 1
}
below_shield() {
  for bs_s in $cd_shields; do
    case "$1/" in
      "$bs_s"/?*) return 0 ;;
    esac
  done
  return 1
}
check_dirs() {
  cd_kind=$1
  shift
  for cd_p in "$@"; do
    if [ -L "$cd_p" ] || [ ! -d "$cd_p" ]; then
      echo "snapshot directory is missing or a symlink: $cd_p (it must be a real directory; remove the symlink or choose a private snapshot_root)" >&2
      exit @EXIT_DIR_UNSAFE@
    fi
  done
  [ -n "${SNAP_UID:-}" ] || SNAP_UID=$(id -u)
  # One ls + awk for all paths. ls sorts its operands by name, so every output
  # line carries its own path (last field; the allowed charset has no spaces)
  # instead of being matched to an argument by position.
  cd_all=$(ls -ldn "$@" 2>/dev/null | awk '{print $NF, $1, $3}') || cd_all=
  cd_n=0
  cd_shields=
  if [ "$cd_kind" = anc ]; then
    # ls order is not trusted, so find the shielding directories first and then
    # judge every ancestor strictly below one as covered.
    while read -r cd_p cd_mode cd_owner; do
      if [ "$cd_owner" = "$SNAP_UID" ] && mode_shields "$cd_mode"; then
        cd_shields="$cd_shields $cd_p"
      fi
    done <<CDEOF
$cd_all
CDEOF
  fi
  while read -r cd_p cd_mode cd_owner; do
    cd_n=$((cd_n + 1))
    if [ -n "$cd_shields" ] && below_shield "$cd_p"; then
      continue
    fi
    case $cd_kind in
      own)
        if [ "$cd_owner" != "$SNAP_UID" ]; then
          echo "snapshot directory $cd_p is owned by uid $cd_owner (mode $cd_mode), not by you (uid $SNAP_UID); choose a private snapshot_root or have the owner hand it over" >&2
          exit @EXIT_DIR_UNSAFE@
        fi
        case $cd_mode in
          ?????w*|????????w*)
            echo "snapshot directory $cd_p is group- or other-writable (mode $cd_mode); another user could swap its contents. Fix: chmod go-w $cd_p, or choose a private snapshot_root" >&2
            exit @EXIT_DIR_UNSAFE@
            ;;
        esac
        ;;
      *)
        if [ "$cd_owner" != "$SNAP_UID" ] && [ "$cd_owner" != 0 ]; then
          echo "snapshot root ancestor $cd_p is owned by uid $cd_owner (mode $cd_mode), neither you (uid $SNAP_UID) nor root; another user could replace the snapshot root. Fix: choose a snapshot_root under a private directory" >&2
          exit @EXIT_DIR_UNSAFE@
        fi
        case $cd_mode in
          ?????????[tT]*) ;;
          ?????w*|????????w*)
            echo "snapshot root ancestor $cd_p is group- or other-writable without the sticky bit (owner uid $cd_owner, mode $cd_mode); another user could replace the snapshot root. Fix: choose a snapshot_root under a private directory, or chmod go-w $cd_p" >&2
            exit @EXIT_DIR_UNSAFE@
            ;;
        esac
        ;;
    esac
  done <<CDEOF
$cd_all
CDEOF
  if [ "$cd_n" != "$#" ]; then
    echo "cannot inspect snapshot directories: $*" >&2
    exit @EXIT_DIR_UNSAFE@
  fi
}
check_private_dir() {
  check_dirs own "$@"
}
check_ancestors() {
  # Every existing ancestor up to /, StrictModes style. Missing ones are created
  # by us under umask 077, so only the nearest existing one matters.
  ca_list=
  ca_p=${1%/*}
  while [ -n "$ca_p" ]; do
    if [ -d "$ca_p" ]; then ca_list="$ca_list $ca_p"; fi
    ca_p=${ca_p%/*}
  done
  ca_list="$ca_list /"
  # shellcheck disable=SC2086
  check_dirs anc $ca_list
}
check_store_dirs() {
  [ -n "${REPO:-}" ] || return 0
  check_ancestors "${REPO%/*}"
  check_private_dir "${REPO%/*}" "$REPO" "$REPO/trees" "$REPO/leases"
}
acquire() {
  if [ ! -d "${LOCKDIR%/*}" ]; then
    echo "directory for lock does not exist: ${LOCKDIR%/*}" >&2
    exit @EXIT_ROOT_INVALID@
  fi
  check_store_dirs
  waited=0
  until mkdir "$LOCKDIR" 2>/dev/null; do
    if [ "$waited" -ge @WAIT@ ]; then
      echo "LOCK=$LOCKDIR" >&2
      if [ -f "$LOCKDIR/owner" ]; then cat "$LOCKDIR/owner" >&2; else echo "owner file missing" >&2; fi
      exit @EXIT_LOCK_TIMEOUT@
    fi
    sleep 1
    waited=$((waited + 1))
  done
  held=1
  check_store_dirs
  {
    printf 'host=%s\n' @HOST@
    printf 'pid=%s\n' @PID@
    printf 'ts=%s\n' "$(date +%s)"
    printf 'time=%s\n' "$(date -u)"
    printf 'token=%s\n' '@TOKEN@'
  } > "$LOCKDIR/owner"
}
"""


def lock_preamble(
    *, client_host: str, client_pid: int, token: str, lock_wait: int
) -> str:
    host, pid, tok = owner_args(client_host, client_pid, token)
    return fill(LOCK_PREAMBLE, TOKEN=tok, WAIT=str(int(lock_wait)), HOST=host, PID=pid)


RESOLVE_PATH = r"""resolve_path() {
  rp_in=$1
  while [ "${rp_in%/}" != "$rp_in" ]; do rp_in=${rp_in%/}; done
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
"""


def repo_vars(repo_dir: str, git: str | None, sha: str | None) -> str:
    check(REPO_DIR_RE, repo_dir, "repo_dir")
    out = f"REPO={shlex.quote(repo_dir)}\nLOCKDIR=$REPO/.lock\nGITDIR=$REPO/repo.git\n"
    if git is not None:
        if not git.startswith("/") or "\n" in git:
            raise ValueError(f"git must be an absolute path: {git!r}")
        out += f"GIT={shlex.quote(git)}\n"
    if sha is not None:
        check(SHA_RE, sha, "sha")
        out += f"SHA={sha}\nTREE=$REPO/trees/$SHA\nMARK=$REPO/trees/$SHA.complete\n"
    return out


# ``mtime_of <path>`` prints the mtime as epoch seconds, or ``-`` when it cannot
# be read; ``mtime_set`` stores it in ``$MTIME_RESULT`` without a subshell.
# GNU and BSD ``stat`` differ, so the flavour is probed once on ``/``.
MTIME_OF = r"""if stat -c %Y / >/dev/null 2>&1; then
  MTIME_FLAVOUR=gnu
elif stat -f %m / >/dev/null 2>&1; then
  MTIME_FLAVOUR=bsd
else
  MTIME_FLAVOUR=
fi
mtime_set() {
  mo_v=
  case $MTIME_FLAVOUR in
    gnu) mo_v=$(stat -c %Y "$1" 2>/dev/null) ;;
    bsd) mo_v=$(stat -f %m "$1" 2>/dev/null) ;;
  esac
  case $mo_v in
    ''|*[!0-9]*) MTIME_RESULT=- ;;
    *) MTIME_RESULT=$mo_v ;;
  esac
}
mtime_of() {
  mtime_set "$1"
  echo "$MTIME_RESULT"
}
"""

# A snapshot is intact only when the marker is a regular file, the tree a real
# directory (-f / -d follow symlinks) and the tree really is at the commit.
TREE_INTACT = r"""tree_intact() {
  [ -f "$MARK" ] && [ ! -L "$MARK" ] && [ -d "$TREE" ] && [ ! -L "$TREE" ] \
    && [ "$("$GIT" -C "$TREE" rev-parse HEAD 2>/dev/null)" = "$SHA" ]
}
"""
