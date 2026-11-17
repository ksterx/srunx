"""Fail-closed local git inspection for snapshot submissions.

Every git call has a timeout and any failure (non-zero exit, timeout, git
missing) raises :class:`SnapshotValidationError`, so a snapshot is never built
from a state that was not positively verified.
"""

from __future__ import annotations

import os
import re
import subprocess
from dataclasses import dataclass
from pathlib import Path, PurePosixPath

from srunx.snapshot.errors import SnapshotValidationError

GIT_TIMEOUT = 30.0

REPO_ID_RE = re.compile(r"[A-Za-z0-9._-]+")
SHA_RE = re.compile(r"[0-9a-f]{40}")
_REGULAR_MODES = frozenset({"100644", "100755"})


def run_git(
    args: list[str],
    *,
    cwd: Path,
    timeout: float = GIT_TIMEOUT,
    env: dict[str, str] | None = None,
) -> str:
    """Run ``git <args>`` in *cwd* and return stdout; fail closed on any error."""
    full_env = {**os.environ, "GIT_TERMINAL_PROMPT": "0", "LC_ALL": "C"}
    if env:
        full_env.update(env)
    cmd = ["git", *args]
    try:
        proc = subprocess.run(  # noqa: S603
            cmd,
            cwd=cwd,
            capture_output=True,
            text=True,
            errors="replace",
            timeout=timeout,
            env=full_env,
            check=False,
        )
    except FileNotFoundError as exc:
        raise SnapshotValidationError("git is not installed or not on PATH") from exc
    except subprocess.TimeoutExpired as exc:
        raise SnapshotValidationError(
            f"git {args[0] if args else ''} timed out after {timeout:g}s"
        ) from exc
    except OSError as exc:
        raise SnapshotValidationError(f"cannot run git in {cwd}: {exc}") from exc
    if proc.returncode != 0:
        detail = proc.stderr.strip() or proc.stdout.strip() or "no output"
        raise SnapshotValidationError(
            f"git {' '.join(args[:2])} failed (exit {proc.returncode}): {detail}"
        )
    return proc.stdout


def physical_dir(path: Path) -> Path:
    """Resolve symlinks of the nearest existing ancestor and re-append the rest."""
    path = Path(os.path.abspath(path))
    rest: list[str] = []
    cur = path
    while not cur.exists():
        if cur.parent == cur:
            break
        rest.append(cur.name)
        cur = cur.parent
    resolved = cur.resolve()
    for part in reversed(rest):
        resolved = resolved / part
    return resolved


def repo_toplevel(start: Path) -> Path:
    """Return the physical top-level directory of the work tree containing *start*."""
    start = physical_dir(start)
    if not start.is_dir():
        raise SnapshotValidationError(f"not a directory: {start}")
    out = run_git(["rev-parse", "--show-toplevel"], cwd=start).strip()
    if not out:
        raise SnapshotValidationError(f"not inside a git work tree: {start}")
    return physical_dir(Path(out))


def resolve_commit(top: Path, rev: str) -> str:
    """Resolve *rev* to a full 40-hex commit sha."""
    if not rev or rev.startswith("-"):
        raise SnapshotValidationError(f"invalid revision: {rev!r}")
    try:
        out = run_git(["rev-parse", "--verify", f"{rev}^{{commit}}"], cwd=top)
    except SnapshotValidationError as exc:
        raise SnapshotValidationError(
            f"revision {rev!r} does not resolve to a commit: {exc}"
        ) from exc
    sha = out.strip()
    if not SHA_RE.fullmatch(sha):
        raise SnapshotValidationError(f"revision {rev!r} resolved to {sha!r}")
    return sha


def head_commit(top: Path) -> str:
    return resolve_commit(top, "HEAD")


def ensure_clean_tracked(top: Path) -> list[str]:
    """Reject tracked changes; return untracked (non-ignored) paths."""
    dirty = run_git(["status", "--porcelain", "--untracked-files=no"], cwd=top)
    if dirty.strip():
        lines = dirty.rstrip("\n").splitlines()
        shown = "\n".join(f"  {ln}" for ln in lines[:10])
        more = f"\n  ... and {len(lines) - 10} more" if len(lines) > 10 else ""
        raise SnapshotValidationError(
            "tracked files have uncommitted changes; commit or stash them, or "
            f"snapshot another commit with --snapshot-rev:\n{shown}{more}"
        )
    out = run_git(["ls-files", "--others", "--exclude-standard", "-z"], cwd=top)
    return [p for p in out.split("\0") if p]


def _list_tree(top: Path, sha: str, *, names_only: bool = False) -> list[str]:
    args = ["ls-tree", "-r", "-z"]
    if names_only:
        args.append("--name-only")
    out = run_git([*args, sha], cwd=top)
    return [e for e in out.split("\0") if e]


def _git_bytes(args: list[str], *, top: Path, stdin: bytes | None = None) -> bytes:
    """Run git and return raw stdout; paths and link targets may not be UTF-8."""
    try:
        proc = subprocess.run(  # noqa: S603
            ["git", *args],  # noqa: S607
            cwd=top,
            input=stdin,
            capture_output=True,
            timeout=GIT_TIMEOUT,
            env={**os.environ, "GIT_TERMINAL_PROMPT": "0", "LC_ALL": "C"},
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise SnapshotValidationError(f"cannot run git {args[0]}: {exc}") from exc
    if proc.returncode != 0:
        detail = proc.stderr.decode(errors="replace").strip() or "no output"
        raise SnapshotValidationError(
            f"git {args[0]} failed (exit {proc.returncode}): {detail}"
        )
    return proc.stdout


def _symlink_map(top: Path, sha: str) -> dict[bytes, bytes]:
    """Map every committed symlink path to its target, all as raw bytes."""
    listing = _git_bytes(["ls-tree", "-r", "-z", sha], top=top)
    links: list[tuple[bytes, bytes]] = []
    for entry in listing.split(b"\0"):
        meta, _, name = entry.partition(b"\t")
        parts = meta.split(b" ")
        if len(parts) == 3 and parts[0] == b"120000":
            links.append((name, parts[2]))
    if not links:
        return {}
    out = _git_bytes(
        ["cat-file", "--batch"],
        top=top,
        stdin=b"".join(oid + b"\n" for _, oid in links),
    )
    pos, result = 0, {}
    for name, oid in links:
        nl = out.find(b"\n", pos)
        header = out[pos:nl].split(b" ") if nl >= 0 else []
        if len(header) != 3 or header[0] != oid or not header[2].isdigit():
            raise SnapshotValidationError(
                f"cannot read symlink target of {_show(name)}"
            )
        size = int(header[2])
        result[name] = out[nl + 1 : nl + 1 + size]
        pos = nl + 1 + size + 1
    return result


def _show(raw: bytes) -> str:
    return raw.decode("utf-8", "backslashreplace")


MAX_SYMLINK_EXPANSIONS = 40


def _symlink_problem(link: bytes, links: dict[bytes, bytes]) -> str | None:
    """Resolve *link* component by component inside the commit's symlink map.

    Returns why it is unsafe (absolute target, leaves the repo root, loop) or
    None. A component that is itself a committed symlink is expanded in place,
    so chains such as ``dir -> .`` plus ``x -> dir/../outside`` are followed the
    way the kernel would follow them.
    """
    cur = link.split(b"/")[:-1]
    pending = links[link].split(b"/")
    if not links[link] or links[link].startswith(b"/"):
        return "absolute or empty target"
    expansions = 0
    while pending:
        comp = pending.pop(0)
        if comp in (b"", b"."):
            continue
        if comp == b"..":
            if not cur:
                return "escapes the repository root"
            cur.pop()
            continue
        cur.append(comp)
        target = links.get(b"/".join(cur))
        if target is None:
            continue
        expansions += 1
        if expansions > MAX_SYMLINK_EXPANSIONS:
            return f"symlink loop (more than {MAX_SYMLINK_EXPANSIONS} expansions)"
        if not target or target.startswith(b"/"):
            return f"leads through an absolute link ({_show(b'/'.join(cur))} -> {_show(target)})"
        cur.pop()
        pending = target.split(b"/") + pending
    return None


def ensure_supported_tree(top: Path, sha: str) -> None:
    """Reject shallow clones, submodules, Git LFS paths and escaping symlinks."""
    shallow = run_git(["rev-parse", "--is-shallow-repository"], cwd=top).strip()
    if shallow != "false":
        raise SnapshotValidationError(
            "shallow clones are not supported for snapshots; run `git fetch --unshallow`"
        )
    entries = _list_tree(top, sha)
    for entry in entries:
        meta, _, name = entry.partition("\t")
        if meta.split(" ", 1)[0] == "160000":
            raise SnapshotValidationError(
                f"submodules are not supported for snapshots (found {name!r})"
            )
    links = _symlink_map(top, sha)
    bad = []
    for link in sorted(links):
        problem = _symlink_problem(link, links)
        if problem:
            bad.append(f"{_show(link)} -> {_show(links[link])} ({problem})")
    if bad:
        shown = "\n".join(f"  {b}" for b in bad[:10])
        more = f"\n  ... and {len(bad) - 10} more" if len(bad) > 10 else ""
        raise SnapshotValidationError(
            "symlinks pointing outside the snapshot (absolute, or escaping the "
            "repo root) would let the job read mutable code; remove them or "
            f"make them in-repo relative links:\n{shown}{more}"
        )
    for name in _list_tree(top, sha, names_only=True):
        if PurePosixPath(name).name == ".gitattributes":
            if "filter=lfs" in read_blob(top, sha, name):
                raise SnapshotValidationError(
                    f"Git LFS is not supported for snapshots (see {name!r})"
                )


def compute_repo_id(top: Path, sha: str) -> str:
    """``<sanitized repo name>-<12 hex of the smallest root commit>``.

    The name comes from the main repository directory (parent of the git common
    dir) so every linked worktree of one repository maps to the same id.
    """
    common = run_git(["rev-parse", "--git-common-dir"], cwd=top).strip()
    common_path = physical_dir(top / common)
    name = common_path.parent.name if common_path.name == ".git" else top.name
    safe = re.sub(r"[^A-Za-z0-9._-]", "-", name).lstrip(".-") or "repo"
    roots = sorted(
        r for r in run_git(["rev-list", "--max-parents=0", sha], cwd=top).split() if r
    )
    if not roots or not SHA_RE.fullmatch(roots[0]):
        raise SnapshotValidationError(f"cannot determine the root commit of {sha}")
    repo_id = f"{safe}-{roots[0][:12]}"
    if not REPO_ID_RE.fullmatch(repo_id):
        raise SnapshotValidationError(f"computed repo id {repo_id!r} is invalid")
    return repo_id


def _tree_entry(top: Path, sha: str, rel: str) -> tuple[str, str] | None:
    """Return ``(mode, type)`` of *rel* in the commit, or None when absent."""
    out = run_git(["ls-tree", "-z", sha, "--", rel], cwd=top)
    for entry in out.split("\0"):
        meta, _, name = entry.partition("\t")
        if name == rel:
            parts = meta.split(" ")
            return parts[0], parts[1]
    return None


def ensure_regular_blob(top: Path, sha: str, rel: str) -> None:
    entry = _tree_entry(top, sha, rel)
    if entry is None:
        raise SnapshotValidationError(f"{rel!r} does not exist in commit {sha[:12]}")
    mode, kind = entry
    if kind != "blob" or mode not in _REGULAR_MODES:
        raise SnapshotValidationError(
            f"{rel!r} in commit {sha[:12]} is not a regular file (mode {mode}, {kind})"
        )


def read_blob(top: Path, sha: str, rel: str) -> str:
    return run_git(["cat-file", "blob", f"{sha}:{rel}"], cwd=top)


def tree_has_dir(top: Path, sha: str, rel: str) -> bool:
    if rel in ("", "."):
        return True
    entry = _tree_entry(top, sha, rel)
    return entry is not None and entry[1] == "tree"


@dataclass(frozen=True)
class SnapshotPlan:
    repo_top: Path
    repo_id: str
    sha: str
    script_rel: str | None
    script_text: str | None
    cwd_rel: str | None
    warnings: tuple[str, ...]


def _relative_to_top(path: Path, top: Path) -> str | None:
    try:
        return path.relative_to(top).as_posix()
    except ValueError:
        return None


def plan_snapshot(
    *, start: Path, rev: str | None, script: Path | None, cwd: Path
) -> SnapshotPlan:
    """Validate the local repo and describe what to snapshot (no remote access)."""
    top = repo_toplevel(start)
    sha = resolve_commit(top, rev or "HEAD")
    warnings: list[str] = []
    if sha == head_commit(top):
        untracked = ensure_clean_tracked(top)
        if untracked:
            warnings.append(
                f"{len(untracked)} untracked file(s) are not part of the snapshot "
                f"(e.g. {untracked[0]!r})"
            )
    ensure_supported_tree(top, sha)
    repo_id = compute_repo_id(top, sha)

    script_rel: str | None = None
    script_text: str | None = None
    if script is not None:
        abs_script = script if script.is_absolute() else cwd / script
        phys = physical_dir(abs_script.parent) / abs_script.name
        script_rel = _relative_to_top(phys, top)
        if script_rel is None or script_rel in ("", "."):
            raise SnapshotValidationError(
                f"script {script} is outside the repository {top}"
            )
        ensure_regular_blob(top, sha, script_rel)
        script_text = read_blob(top, sha, script_rel)

    cwd_rel: str | None = None
    rel = _relative_to_top(physical_dir(cwd), top)
    if rel is not None and rel != ".":
        if tree_has_dir(top, sha, rel):
            cwd_rel = rel
        else:
            warnings.append(
                f"working directory {rel!r} is not a directory in commit "
                f"{sha[:12]}; using the snapshot top"
            )
    return SnapshotPlan(
        repo_top=top,
        repo_id=repo_id,
        sha=sha,
        script_rel=script_rel,
        script_text=script_text,
        cwd_rel=cwd_rel,
        warnings=tuple(warnings),
    )
