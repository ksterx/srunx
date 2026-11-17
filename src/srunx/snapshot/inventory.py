"""Read-only inventory of a snapshot root, and the locked deletion of snapshots.

Two remote scripts, both POSIX ``sh`` built from validated inputs:

* the *inventory* takes no lock and reports every repo, snapshot, lease and
  leftover under the root, with one opaque fingerprint per sha;
* the *prune* script takes the per-repo lock, recomputes each fingerprint with
  the very same shell function and removes a sha only when it is unchanged.

This module also holds the record types and the parsers of both outputs. A
listing without its ``END`` line is rejected as truncated.
"""

from __future__ import annotations

import re
import shlex
from collections.abc import Sequence
from dataclasses import dataclass

from srunx.snapshot.errors import SnapshotRemoteError
from srunx.snapshot.lease import Lease
from srunx.snapshot.local_git import REPO_ID_RE, SHA_RE
from srunx.snapshot.remote import (
    _MTIME_OF,
    _REPO_DIR_RE,
    _RESOLVE_PATH,
    _TREE_INTACT,
    INVENTORY_TIMEOUT,
    LOCK_WAIT_SECONDS,
    PRUNE_TIMEOUT,
    RemoteSnapshotStore,
    _check,
    _fill,
    _lock_preamble,
    _repo_vars,
)

_LEASE_ID = r"[0-9a-f]{32}"
FINGERPRINT_RE = re.compile(
    r"m(-|\d+)\.t(-|\d+)\.i([01])\.l"
    rf"(!|(?:{_LEASE_ID}:(?:-|\d+)(?:,{_LEASE_ID}:(?:-|\d+))*)?)"
)
TRASH_NAME_RE = re.compile(r"\.trash\.[0-9a-f]{40}\.[0-9a-f]{32}")
_LEASE_FILE_RE = re.compile(rf"({_LEASE_ID})\.json")
_DIGITS_RE = re.compile(r"\d+")


@dataclass(frozen=True)
class LeaseRecord:
    filename: str  # "<name>.json" as found in leases/
    mtime: int | None  # remote epoch
    lease: Lease | None  # None when unreadable
    error: str | None  # why it is unreadable


@dataclass(frozen=True)
class SnapshotRecord:
    sha: str
    fingerprint: str  # opaque; passed back to the delete script
    has_tree: bool  # an entry named trees/<sha> exists (even a symlink)
    has_marker: bool  # an entry named trees/<sha>.complete exists
    intact: bool
    marker_mtime: int | None
    tree_mtime: int | None
    # None when the remote could not attribute leases ("l!"); empty when no
    # lease mentions the sha.
    fp_lease_ids: frozenset[str] | None
    size_kb: int | None


@dataclass(frozen=True)
class RepoInventory:
    repo_id: str
    repo_dir: str
    skipped: str | None  # reason; snapshots/leases are empty when set
    locked_since: int | None
    snapshots: tuple[SnapshotRecord, ...]
    leases: tuple[LeaseRecord, ...]
    trash: tuple[str, ...]  # TRASH_NAME_RE names that are owned real directories
    stray: tuple[str, ...]  # everything unrecognised, incl. malformed .trash* names


@dataclass(frozen=True)
class Inventory:
    root: str
    exists: bool
    now: int | None
    git: str | None
    repos: tuple[RepoInventory, ...]
    stray: tuple[str, ...] = ()  # entries of the root that are not repos


@dataclass(frozen=True)
class PruneTarget:
    sha: str
    fingerprint: str
    lease_ids: tuple[str, ...]


@dataclass(frozen=True)
class PruneRepoResult:
    removed: tuple[str, ...]
    changed: tuple[str, ...]
    failed: tuple[tuple[str, str], ...]  # (sha, reason)
    trash_left: int  # valid trash entries that could not be deleted
    trash_skipped: tuple[str, ...]  # .trash* entries left alone


def _int_or_none(text: str) -> int | None:
    return int(text) if _DIGITS_RE.fullmatch(text) else None


def _bad(line: str, why: str) -> SnapshotRemoteError:
    return SnapshotRemoteError(f"malformed inventory line ({why}): {line[:200]!r}")


def _parse_lease(
    repo_id: str, filename: str, mtime: int | None, text: str
) -> LeaseRecord:
    m = _LEASE_FILE_RE.fullmatch(filename)
    if m is None:
        return LeaseRecord(filename, mtime, None, "unexpected lease file name")
    try:
        lease = Lease.from_json(text)
    except ValueError as exc:  # pydantic ValidationError is a ValueError
        return LeaseRecord(filename, mtime, None, f"not valid lease JSON: {exc}")
    if lease.lease_id != m.group(1):
        return LeaseRecord(filename, mtime, None, "lease_id differs from the file name")
    if lease.repo_id != repo_id:
        return LeaseRecord(filename, mtime, None, "repo_id differs from the repo")
    if not SHA_RE.fullmatch(lease.sha):
        return LeaseRecord(filename, mtime, None, "lease sha is not a commit id")
    return LeaseRecord(filename, mtime, lease, None)


def _snapshot_from_line(parts: list[str], line: str) -> SnapshotRecord:
    if len(parts) != 6:
        raise _bad(line, "SNAP needs 6 fields")
    _, _repo, sha, fp, size, flags = parts
    m = FINGERPRINT_RE.fullmatch(fp)
    if not SHA_RE.fullmatch(sha) or m is None:
        raise _bad(line, "bad sha or fingerprint")
    if not (size == "-" or _DIGITS_RE.fullmatch(size)):
        raise _bad(line, "bad size")
    if not re.fullmatch(r"[01]{2}", flags):
        raise _bad(line, "bad flags")
    ids_text = m.group(4)
    fp_ids: frozenset[str] | None
    if ids_text == "!":
        fp_ids = None
    else:
        fp_ids = frozenset(
            item.split(":", 1)[0] for item in ids_text.split(",") if item
        )
    return SnapshotRecord(
        sha=sha,
        fingerprint=fp,
        has_tree=flags[0] == "1",
        has_marker=flags[1] == "1",
        intact=m.group(3) == "1",
        marker_mtime=_int_or_none(m.group(1)),
        tree_mtime=_int_or_none(m.group(2)),
        fp_lease_ids=fp_ids,
        size_kb=_int_or_none(size),
    )


def parse_inventory(stdout: str) -> Inventory:
    """Parse the inventory script output.

    Raises ``SnapshotRemoteError`` when ``BEGIN`` / ``END`` is missing (the
    listing was truncated) or a record is malformed. Lines outside
    ``BEGIN`` .. ``END`` (login banners) are ignored.
    """
    lines = stdout.splitlines()
    if "BEGIN" not in lines:
        raise SnapshotRemoteError(
            "remote inventory has no BEGIN line (unexpected output)"
        )
    body = lines[lines.index("BEGIN") + 1 :]
    if "END" not in body:
        raise SnapshotRemoteError(
            "remote inventory is truncated (no END line); nothing was judged"
        )
    body = body[: body.index("END")]

    now: int | None = None
    root: str | None = None
    exists: bool | None = None
    git: str | None = None
    root_stray: list[str] = []
    order: list[str] = []
    repo_dirs: dict[str, str] = {}
    skipped: dict[str, str] = {}
    locked: dict[str, int | None] = {}
    snaps: dict[str, list[SnapshotRecord]] = {}
    leases: dict[str, list[LeaseRecord]] = {}
    trash: dict[str, list[str]] = {}
    stray: dict[str, list[str]] = {}
    seen_now = False

    def repo_of(rid: str, line: str) -> str:
        if rid not in repo_dirs:
            raise _bad(line, "record for an unannounced repo")
        return rid

    for line in body:
        kind, _, rest = line.partition(" ")
        if kind == "NOW":
            seen_now = True
            now = _int_or_none(rest) if rest != "-" else None
            if rest != "-" and now is None:
                raise _bad(line, "bad clock")
        elif kind == "ROOT":
            if not _REPO_DIR_RE.fullmatch(rest):
                raise _bad(line, "unsafe root path")
            root = rest
        elif kind == "EXISTS":
            if rest not in ("0", "1"):
                raise _bad(line, "bad EXISTS")
            exists = rest == "1"
        elif kind == "GIT":
            if not rest.startswith("/") or "\n" in rest:
                raise _bad(line, "git path is not absolute")
            git = rest
        elif kind in ("REPO", "SKIP"):
            rid, _, reason = rest.partition(" ")
            if not REPO_ID_RE.fullmatch(rid) or rid.startswith(".") or root is None:
                raise _bad(line, "bad repo id")
            if kind == "REPO" and reason:
                raise _bad(line, "trailing text after the repo id")
            if rid in repo_dirs:
                raise _bad(line, "repo announced twice")
            order.append(rid)
            repo_dirs[rid] = f"{root.rstrip('/')}/{rid}"
            if kind == "SKIP":
                skipped[rid] = reason or "skipped"
        elif kind == "LOCK":
            parts = rest.split(" ")
            if len(parts) != 2:
                raise _bad(line, "LOCK needs 2 fields")
            locked[repo_of(parts[0], line)] = _int_or_none(parts[1])
        elif kind == "SNAP":
            parts = line.split(" ")
            rid = repo_of(parts[1] if len(parts) > 1 else "", line)
            snaps.setdefault(rid, []).append(_snapshot_from_line(parts, line))
        elif kind in ("LEASE", "LEASEBAD"):
            parts = rest.split(" ", 3)
            if len(parts) != 4:
                raise _bad(line, f"{kind} needs 4 fields")
            rid = repo_of(parts[0], line)
            mtime = _int_or_none(parts[2])
            if kind == "LEASE":
                rec = _parse_lease(rid, parts[1], mtime, parts[3])
            else:
                rec = LeaseRecord(parts[1], mtime, None, parts[3])
            leases.setdefault(rid, []).append(rec)
        elif kind == "TRASH":
            parts = rest.split(" ")
            if len(parts) != 2 or not TRASH_NAME_RE.fullmatch(parts[1]):
                raise _bad(line, "bad TRASH")
            trash.setdefault(repo_of(parts[0], line), []).append(parts[1])
        elif kind == "STRAY":
            rid, _, name = rest.partition(" ")
            if rid == "-":
                root_stray.append(name)
            else:
                stray.setdefault(repo_of(rid, line), []).append(name)

    if root is None or exists is None or not seen_now:
        raise SnapshotRemoteError(
            "remote inventory is missing ROOT, EXISTS or NOW (unexpected output)"
        )
    repos = tuple(
        RepoInventory(
            repo_id=rid,
            repo_dir=repo_dirs[rid],
            skipped=skipped.get(rid),
            locked_since=locked.get(rid),
            snapshots=tuple(snaps.get(rid, ())),
            leases=tuple(leases.get(rid, ())),
            trash=tuple(trash.get(rid, ())),
            stray=tuple(stray.get(rid, ())),
        )
        for rid in order
    )
    return Inventory(
        root=root,
        exists=exists,
        now=now,
        git=git,
        repos=repos,
        stray=tuple(root_stray),
    )


def parse_prune_output(stdout: str, targets: Sequence[PruneTarget]) -> PruneRepoResult:
    """Parse the prune script output; a target without a result line failed."""
    lines = stdout.splitlines()
    if "END" not in lines:
        raise SnapshotRemoteError(
            "remote prune output is truncated (no END line); the outcome is unknown"
        )
    wanted = {t.sha for t in targets}
    removed: list[str] = []
    changed: list[str] = []
    failed: dict[str, str] = {}
    skipped: list[str] = []
    trash_left = 0
    seen: set[str] = set()
    for line in lines[: lines.index("END")]:
        kind, _, rest = line.partition(" ")
        if kind in ("REMOVED", "CHANGED", "FAILED"):
            sha, _, reason = rest.partition(" ")
            if sha not in wanted or sha in seen:
                continue
            seen.add(sha)
            if kind == "REMOVED":
                removed.append(sha)
            elif kind == "CHANGED":
                changed.append(sha)
            else:
                failed[sha] = reason or "failed"
        elif kind == "TRASHSKIP" and rest:
            skipped.append(rest)
        elif kind == "TRASHLEFT" and _DIGITS_RE.fullmatch(rest):
            trash_left = int(rest)
    for t in targets:
        if t.sha not in seen:
            failed[t.sha] = "no result"
    return PruneRepoResult(
        removed=tuple(removed),
        changed=tuple(changed),
        failed=tuple(failed.items()),
        trash_left=trash_left,
        trash_skipped=tuple(skipped),
    )


# --------------------------------------------------------------------- scripts

# Shared by the inventory and the prune script so the fingerprint cannot drift.
# ``snap_fp <sha>`` prints ``m<marker mtime|->.t<tree mtime|->.i<0|1>.l<list>``;
# the lease list is ``id:mtime`` of every lease file that mentions the sha, or
# ``!`` when it cannot be attributed. It never hands an unexpanded glob to a
# command: an empty ``leases/`` leaves the pattern, which fails ``[ -f ]`` and
# is dropped.
_GC_HELPERS = r"""is_plain() {
  case $1 in
    ''|*[!A-Za-z0-9._+-]*) return 1 ;;
  esac
  return 0
}
is_sha_name() {
  [ "${#1}" = 40 ] || return 1
  case $1 in
    *[!0-9a-f]*) return 1 ;;
  esac
  return 0
}
is_trash_name() {
  is_plain "$1" || return 1
  printf '%s\n' "$1" | grep -Eqx '\.trash\.[0-9a-f]{40}\.[0-9a-f]{32}'
}
is_owned_trash() {
  is_trash_name "${1##*/}" && [ ! -L "$1" ] && [ -d "$1" ] && [ -O "$1" ]
}
shown_name() {
  if is_plain "$1"; then printf '%s' "$1"; else printf '%s' '(unprintable name)'; fi
}
snap_fp() {
  SHA=$1
  TREE=$REPO/trees/$SHA
  MARK=$TREE.complete
  sf_m=-
  sf_t=-
  sf_i=0
  if [ -f "$MARK" ] && [ ! -L "$MARK" ]; then mtime_set "$MARK"; sf_m=$MTIME_RESULT; fi
  if [ -d "$TREE" ] && [ ! -L "$TREE" ]; then mtime_set "$TREE"; sf_t=$MTIME_RESULT; fi
  if tree_intact; then sf_i=1; fi
  set --
  for sf_f in "$REPO"/leases/*.json; do
    if [ -f "$sf_f" ] && [ ! -L "$sf_f" ]; then set -- "$@" "$sf_f"; fi
  done
  sf_list=
  if [ $# -gt 0 ]; then
    sf_hits=$(grep -lF -- "$SHA" "$@" 2>/dev/null </dev/null)
    sf_rc=$?
    if [ "$sf_rc" -ge 2 ]; then
      sf_list='!'
    elif [ "$sf_rc" -eq 0 ]; then
      sf_acc=
      while IFS= read -r sf_h; do
        [ -n "$sf_h" ] || continue
        sf_b=${sf_h##*/}
        sf_id=${sf_b%.json}
        if [ "${#sf_id}" != 32 ] || [ "$sf_id.json" != "$sf_b" ]; then sf_list='!'; break; fi
        case $sf_id in
          *[!0-9a-f]*) sf_list='!'; break ;;
        esac
        mtime_set "$sf_h"
        sf_acc="$sf_acc$sf_id:$MTIME_RESULT
"
      done <<SFEOF
$sf_hits
SFEOF
      if [ "$sf_list" != '!' ]; then
        sf_list=$(printf '%s' "$sf_acc" | LC_ALL=C sort | tr '\n' ',')
        sf_list=${sf_list%,}
      fi
    fi
  fi
  printf 'm%s.t%s.i%s.l%s\n' "$sf_m" "$sf_t" "$sf_i" "$sf_list"
}
"""

_INVENTORY_BODY = r"""GIT=$(command -v git 2>/dev/null || true)
case $GIT in
  /*) ;;
  *) echo "git was not found on the remote PATH" >&2; exit @EXIT_GIT_MISSING@ ;;
esac
ROOT_IN=@ROOT_EXPR@
if [ -L "${ROOT_IN%/}" ]; then
  echo "snapshot root is a symlink: $ROOT_IN (it must be a real directory; remove the symlink or choose a private snapshot_root)" >&2
  exit @EXIT_DIR_UNSAFE@
fi
resolve_path "$ROOT_IN" || { echo "snapshot root cannot be resolved: $ROOT_IN" >&2; exit @EXIT_ROOT_INVALID@; }
ROOT=$RESOLVED
case $ROOT in
  *[!A-Za-z0-9._/+-]*) echo "snapshot root has characters outside [A-Za-z0-9._/+-]: $ROOT" >&2; exit @EXIT_ROOT_INVALID@ ;;
esac
if [ -d "$ROOT" ]; then
  check_ancestors "$ROOT"
  check_private_dir "$ROOT"
fi
NOW=$(date +%s)
case $NOW in
  ''|*[!0-9]*) NOW=- ;;
esac
echo BEGIN
printf 'NOW %s\n' "$NOW"
printf 'ROOT %s\n' "$ROOT"
if [ ! -d "$ROOT" ]; then
  echo 'EXISTS 0'
  echo END
  exit 0
fi
echo 'EXISTS 1'
printf 'GIT %s\n' "$GIT"
emit_stray() {
  printf 'STRAY %s %s%s\n' "$1" "$2" "$(shown_name "$3")"
}
inventory_repo() {
  ir_n=$1
  REPO=$ROOT/$ir_n
  if ir_msg=$(check_private_dir "$REPO" "$REPO/trees" "$REPO/leases" 2>&1); then
    :
  else
    ir_msg=$(printf '%s' "$ir_msg" | tr '\n' ' ')
    printf 'SKIP %s %s\n' "$ir_n" "${ir_msg:-directory is not private}"
    return 0
  fi
  printf 'REPO %s\n' "$ir_n"
  if [ -e "$REPO/.lock" ] || [ -L "$REPO/.lock" ]; then
    printf 'LOCK %s %s\n' "$ir_n" "$(mtime_of "$REPO/.lock")"
  fi
  ir_shas=$(
    for ir_t in "$REPO"/trees/*; do
      if [ -e "$ir_t" ] || [ -L "$ir_t" ]; then
        ir_h=${ir_t##*/}
        ir_h=${ir_h%.complete}
        if is_sha_name "$ir_h"; then echo "$ir_h"; fi
      fi
    done
    set --
    for ir_l in "$REPO"/leases/*.json; do
      if [ -f "$ir_l" ] && [ ! -L "$ir_l" ]; then set -- "$@" "$ir_l"; fi
    done
    if [ $# -gt 0 ]; then
      sed -n 's/.*"sha":"\([0-9a-f]\{40\}\)".*/\1/p' "$@" 2>/dev/null </dev/null
    fi
  )
  ir_shas=$(printf '%s\n' "$ir_shas" | LC_ALL=C sort -u)
  for ir_sha in $ir_shas; do
    is_sha_name "$ir_sha" || continue
    ir_fp=$(snap_fp "$ir_sha")
    ir_tf=0
    ir_mf=0
    if [ -e "$REPO/trees/$ir_sha" ] || [ -L "$REPO/trees/$ir_sha" ]; then ir_tf=1; fi
    if [ -e "$REPO/trees/$ir_sha.complete" ] || [ -L "$REPO/trees/$ir_sha.complete" ]; then ir_mf=1; fi
    ir_size=-
    if [ "$WITH_SIZE" = 1 ] && [ -d "$REPO/trees/$ir_sha" ] && [ ! -L "$REPO/trees/$ir_sha" ]; then
      ir_size=$(du -sk "$REPO/trees/$ir_sha" 2>/dev/null </dev/null | cut -f1)
      case $ir_size in
        ''|*[!0-9]*) ir_size=- ;;
      esac
    fi
    printf 'SNAP %s %s %s %s %s%s\n' "$ir_n" "$ir_sha" "$ir_fp" "$ir_size" "$ir_tf" "$ir_mf"
  done
  for ir_l in "$REPO"/leases/*.json; do
    if [ ! -e "$ir_l" ] && [ ! -L "$ir_l" ]; then continue; fi
    ir_b=${ir_l##*/}
    ir_lm=$(mtime_of "$ir_l")
    if ! is_plain "$ir_b"; then
      printf 'LEASEBAD %s %s %s %s\n' "$ir_n" '?' "$ir_lm" 'unusual file name'
      continue
    fi
    if [ -L "$ir_l" ] || [ ! -f "$ir_l" ]; then
      printf 'LEASEBAD %s %s %s %s\n' "$ir_n" "$ir_b" "$ir_lm" 'not a regular file'
      continue
    fi
    ir_sz=$(wc -c < "$ir_l" 2>/dev/null | tr -d ' ')
    case $ir_sz in
      ''|*[!0-9]*) printf 'LEASEBAD %s %s %s %s\n' "$ir_n" "$ir_b" "$ir_lm" 'cannot be read'; continue ;;
    esac
    if [ "$ir_sz" -eq 0 ]; then
      printf 'LEASEBAD %s %s %s %s\n' "$ir_n" "$ir_b" "$ir_lm" 'empty'
    elif [ "$ir_sz" -gt 4096 ]; then
      printf 'LEASEBAD %s %s %s %s\n' "$ir_n" "$ir_b" "$ir_lm" 'larger than 4096 bytes'
    else
      ir_json=$(head -n 1 "$ir_l" 2>/dev/null)
      printf 'LEASE %s %s %s %s\n' "$ir_n" "$ir_b" "$ir_lm" "$ir_json"
    fi
  done
  for ir_t in "$REPO"/trees/* "$REPO"/trees/.[!.]*; do
    if [ ! -e "$ir_t" ] && [ ! -L "$ir_t" ]; then continue; fi
    ir_b=${ir_t##*/}
    ir_h=${ir_b%.complete}
    if is_sha_name "$ir_h"; then continue; fi
    if is_owned_trash "$ir_t"; then
      printf 'TRASH %s %s\n' "$ir_n" "$ir_b"
    else
      emit_stray "$ir_n" trees/ "$ir_b"
    fi
  done
  for ir_t in "$REPO"/* "$REPO"/.[!.]*; do
    if [ ! -e "$ir_t" ] && [ ! -L "$ir_t" ]; then continue; fi
    ir_b=${ir_t##*/}
    case $ir_b in
      repo.git|trees|leases|.lock) continue ;;
    esac
    emit_stray "$ir_n" '' "$ir_b"
  done
}
for ent in "$ROOT"/* "$ROOT"/.[!.]*; do
  if [ ! -e "$ent" ] && [ ! -L "$ent" ]; then continue; fi
  ent_n=${ent##*/}
  case $ent_n in
    .*|*[!A-Za-z0-9._-]*) emit_stray - '' "$ent_n"; continue ;;
  esac
  if [ -L "$ent" ] || [ ! -d "$ent" ] || [ ! -d "$ent/repo.git" ]; then
    emit_stray - '' "$ent_n"
    continue
  fi
  inventory_repo "$ent_n"
done
echo END
"""


def build_inventory_script(*, root_expr: str, with_size: bool = False) -> str:
    """Read-only inventory of the whole root; takes no lock."""
    # The preamble's ``acquire`` is never called; the owner is a placeholder.
    preamble = _lock_preamble(
        client_host="inventory", client_pid=0, token="0" * 32, lock_wait=0
    )
    body = _fill(_INVENTORY_BODY, ROOT_EXPR=root_expr)
    return (
        preamble
        + _RESOLVE_PATH
        + _MTIME_OF
        + _TREE_INTACT
        + _GC_HELPERS
        + f"WITH_SIZE={1 if with_size else 0}\n"
        + body
    )


_PRUNE_BODY = r"""RESULTS=
prune_one() {
  po_sha=$1
  po_fp=$2
  po_ids=$3
  SHA=$po_sha
  TREE=$REPO/trees/$SHA
  MARK=$TREE.complete
  po_cur=$(snap_fp "$po_sha")
  if [ "$po_cur" != "$po_fp" ]; then
    RESULTS="${RESULTS}CHANGED $po_sha
"
    return 0
  fi
  if [ ! -L "$TREE" ] && [ -e "$TREE" ] && [ ! -O "$TREE" ]; then
    RESULTS="${RESULTS}FAILED $po_sha not-owned
"
    return 0
  fi
  rm -rf "$MARK"
  if [ -L "$TREE" ]; then
    rm -f "$TREE"
  elif [ -d "$TREE" ]; then
    chmod u+w "$TREE" 2>/dev/null
    if ! mv "$TREE" "$REPO/trees/.trash.$po_sha.@TOKEN@" 2>/dev/null; then
      RESULTS="${RESULTS}FAILED $po_sha rename
"
      return 0
    fi
  elif [ -e "$TREE" ]; then
    rm -f "$TREE"
  fi
  "$GIT" --git-dir="$GITDIR" update-ref -d "refs/srunx/snapshots/$po_sha" >/dev/null 2>&1 </dev/null
  for po_id in $(printf '%s' "$po_ids" | tr ',' ' '); do
    rm -f "$REPO/leases/$po_id.json"
  done
  RESULTS="${RESULTS}REMOVED $po_sha
"
}
acquire
@PRUNE_CALLS@"$GIT" --git-dir="$GITDIR" worktree prune --expire=now >/dev/null 2>&1 </dev/null
release
printf '%s' "$RESULTS"
TRASH_LEFT=0
for tr_e in "$REPO"/trees/.trash*; do
  if [ ! -e "$tr_e" ] && [ ! -L "$tr_e" ]; then continue; fi
  if is_owned_trash "$tr_e"; then
    chmod -R u+w "$tr_e" 2>/dev/null
    rm -rf "$tr_e" 2>/dev/null
    if [ -e "$tr_e" ] || [ -L "$tr_e" ]; then TRASH_LEFT=$((TRASH_LEFT + 1)); fi
  else
    printf 'TRASHSKIP %s\n' "$(shown_name "${tr_e##*/}")"
  fi
done
printf 'TRASHLEFT %s\n' "$TRASH_LEFT"
echo END
"""


def build_prune_script(
    *,
    repo_dir: str,
    git: str,
    targets: Sequence[PruneTarget],
    client_host: str,
    client_pid: int,
    token: str,
    lock_wait: int = LOCK_WAIT_SECONDS,
) -> str:
    """Remove the unchanged *targets* of one repo under its lock, then sweep trash.

    The token doubles as the suffix of the trash directory names; it is unique
    per call. *targets* may be empty (a trash-only run).
    """
    calls = ""
    for t in targets:
        _check(SHA_RE, t.sha, "sha")
        _check(FINGERPRINT_RE, t.fingerprint, "fingerprint")
        listed = FINGERPRINT_RE.fullmatch(t.fingerprint)
        assert listed is not None
        if listed.group(4) == "!":
            raise ValueError(
                f"fingerprint cannot be attributed to leases: {t.fingerprint!r}"
            )
        ids = [_check(_LEASE_ID, i, "lease id") for i in t.lease_ids]
        fp_ids = {item.split(":", 1)[0] for item in listed.group(4).split(",") if item}
        if set(ids) != fp_ids:
            raise ValueError("lease ids differ from the ones in the fingerprint")
        calls += (
            f"prune_one {t.sha} {shlex.quote(t.fingerprint)} "
            f"{shlex.quote(','.join(ids))}\n"
        )
    return (
        _lock_preamble(
            client_host=client_host,
            client_pid=client_pid,
            token=token,
            lock_wait=lock_wait,
        )
        + _repo_vars(repo_dir, git, None)
        + _MTIME_OF
        + _TREE_INTACT
        + _GC_HELPERS
        + _fill(_PRUNE_BODY, PRUNE_CALLS=calls, TOKEN=token)
    )


class InventoryStore:
    """Runs the inventory and prune scripts through a :class:`RemoteSnapshotStore`."""

    def __init__(self, store: RemoteSnapshotStore) -> None:
        self._store = store

    def read(self, *, with_size: bool = False) -> Inventory:
        script = build_inventory_script(
            root_expr=self._store.root_expr, with_size=with_size
        )
        out = self._store.run(
            script, timeout=INVENTORY_TIMEOUT, what="snapshot inventory"
        )
        return parse_inventory(out)

    def prune_repo(
        self, *, repo_dir: str, git: str, targets: Sequence[PruneTarget]
    ) -> PruneRepoResult:
        script = build_prune_script(
            repo_dir=repo_dir,
            git=git,
            targets=targets,
            lock_wait=self._store.lock_wait,
            **self._store.owner(),
        )
        out = self._store.run(script, timeout=PRUNE_TIMEOUT, what="snapshot prune")
        return parse_prune_output(out, targets)
