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
from collections.abc import Sequence
from dataclasses import dataclass

from srunx.snapshot.errors import SnapshotRemoteError
from srunx.snapshot.lease import Lease
from srunx.snapshot.local_git import REPO_ID_RE, SHA_RE
from srunx.snapshot.remote import _REPO_DIR_RE

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
