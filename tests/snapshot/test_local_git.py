"""Tests for srunx.snapshot.local_git using real temporary git repos."""

from __future__ import annotations

import os
import re
import subprocess
from pathlib import Path

import pytest

from srunx.snapshot import local_git
from srunx.snapshot.errors import SnapshotValidationError
from srunx.snapshot.local_git import (
    compute_repo_id,
    ensure_clean_tracked,
    ensure_regular_blob,
    ensure_supported_tree,
    head_commit,
    physical_dir,
    plan_snapshot,
    read_blob,
    repo_toplevel,
    resolve_commit,
    run_git,
    tree_has_dir,
)

# tests/conftest.py patches ``subprocess.run`` for every test; these tests need
# the real one, so grab it at import time and restore it in the fixture below.
_REAL_RUN = subprocess.run

_ENV = {
    "GIT_AUTHOR_NAME": "t",
    "GIT_AUTHOR_EMAIL": "t@example.com",
    "GIT_COMMITTER_NAME": "t",
    "GIT_COMMITTER_EMAIL": "t@example.com",
    "GIT_CONFIG_GLOBAL": os.devnull,
    "GIT_CONFIG_SYSTEM": os.devnull,
}


@pytest.fixture(autouse=True)
def _git_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(subprocess, "run", _REAL_RUN)
    for k, v in _ENV.items():
        monkeypatch.setenv(k, v)
    monkeypatch.setenv("GIT_ALLOW_PROTOCOL", "file")


def git(cwd: Path, *args: str) -> str:
    return _REAL_RUN(
        ["git", *args], cwd=cwd, check=True, capture_output=True, text=True
    ).stdout.strip()


def commit_all(repo: Path, msg: str = "c") -> str:
    git(repo, "add", "-A")
    git(repo, "commit", "-q", "-m", msg)
    return git(repo, "rev-parse", "HEAD")


@pytest.fixture
def repo(tmp_path: Path) -> Path:
    r = tmp_path / "my repo"
    r.mkdir()
    git(r, "init", "-q", "-b", "main")
    (r / "job.sbatch").write_text("#!/bin/bash\n#SBATCH --job-name=jj\necho hi\n")
    (r / "sub").mkdir()
    (r / "sub" / "run.sh").write_text("#!/bin/sh\necho sub\n")
    (r / "README").write_text("x")
    commit_all(r)
    return r.resolve()


def test_clean_head_plan(repo: Path) -> None:
    plan = plan_snapshot(start=repo, rev=None, script=repo / "job.sbatch", cwd=repo)
    assert plan.sha == git(repo, "rev-parse", "HEAD")
    assert plan.script_rel == "job.sbatch"
    assert plan.script_text is not None and "jj" in plan.script_text
    assert plan.cwd_rel is None
    assert plan.warnings == ()
    assert re.fullmatch(r"my-repo-[0-9a-f]{12}", plan.repo_id)


def test_relative_script_and_cwd_subdir(repo: Path) -> None:
    plan = plan_snapshot(
        start=repo / "sub", rev=None, script=Path("run.sh"), cwd=repo / "sub"
    )
    assert plan.script_rel == "sub/run.sh"
    assert plan.cwd_rel == "sub"


@pytest.mark.parametrize("how", ["none", "HEAD", "sha"])
def test_tracked_dirty_rejected_when_commit_is_head(repo: Path, how: str) -> None:
    (repo / "README").write_text("changed")
    rev = {"none": None, "HEAD": "HEAD", "sha": git(repo, "rev-parse", "HEAD")}[how]
    with pytest.raises(SnapshotValidationError, match="uncommitted"):
        plan_snapshot(start=repo, rev=rev, script=None, cwd=repo)


def test_dirty_accepted_for_other_commit(repo: Path) -> None:
    first = git(repo, "rev-parse", "HEAD")
    (repo / "README").write_text("two")
    commit_all(repo)
    (repo / "README").write_text("dirty")
    plan = plan_snapshot(start=repo, rev=first, script=None, cwd=repo)
    assert plan.sha == first


def test_untracked_warns(repo: Path) -> None:
    (repo / "scratch.txt").write_text("x")
    plan = plan_snapshot(start=repo, rev=None, script=None, cwd=repo)
    assert any("untracked" in w for w in plan.warnings)
    assert ensure_clean_tracked(repo) == ["scratch.txt"]


def test_bad_rev(repo: Path) -> None:
    with pytest.raises(SnapshotValidationError, match="does not resolve"):
        resolve_commit(repo, "nope")
    with pytest.raises(SnapshotValidationError, match="invalid revision"):
        resolve_commit(repo, "--output=x")


def test_not_a_repo(tmp_path: Path) -> None:
    d = tmp_path / "plain"
    d.mkdir()
    with pytest.raises(SnapshotValidationError):
        repo_toplevel(d)


def test_git_timeout_fails_closed(repo: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    def boom(*a: object, **k: object) -> None:
        raise subprocess.TimeoutExpired("git", 1)

    monkeypatch.setattr(local_git.subprocess, "run", boom)
    with pytest.raises(SnapshotValidationError, match="timed out"):
        run_git(["status"], cwd=repo)


def test_shallow_rejected(repo: Path, tmp_path: Path) -> None:
    (repo / "a").write_text("1")
    commit_all(repo)
    clone = tmp_path / "shallow"
    git(tmp_path, "clone", "-q", "--depth", "1", f"file://{repo}", str(clone))
    with pytest.raises(SnapshotValidationError, match="shallow"):
        plan_snapshot(start=clone, rev=None, script=None, cwd=clone)


def test_submodule_gitlink_rejected(repo: Path) -> None:
    git(
        repo,
        "update-index",
        "--add",
        "--cacheinfo",
        "160000,1111111111111111111111111111111111111111,vendor/lib",
    )
    git(repo, "commit", "-q", "-m", "gitlink")
    (repo / "vendor" / "lib").mkdir(parents=True)  # empty dir keeps status clean
    with pytest.raises(SnapshotValidationError, match="submodule"):
        plan_snapshot(start=repo, rev=None, script=None, cwd=repo)


def test_lfs_attribute_rejected(repo: Path) -> None:
    (repo / "sub" / ".gitattributes").write_text("*.bin filter=lfs diff=lfs\n")
    commit_all(repo)
    with pytest.raises(SnapshotValidationError, match="LFS"):
        ensure_supported_tree(repo, head_commit(repo))


def test_script_missing_and_directory(repo: Path) -> None:
    with pytest.raises(SnapshotValidationError, match="does not exist"):
        plan_snapshot(start=repo, rev=None, script=repo / "nope.sh", cwd=repo)
    with pytest.raises(SnapshotValidationError, match="not a regular file"):
        plan_snapshot(start=repo, rev=None, script=repo / "sub", cwd=repo)


def test_script_symlink_in_commit_rejected(repo: Path) -> None:
    (repo / "link.sh").symlink_to("job.sbatch")
    commit_all(repo)
    with pytest.raises(SnapshotValidationError, match="not a regular file"):
        plan_snapshot(start=repo, rev=None, script=repo / "link.sh", cwd=repo)


def test_script_under_symlinked_parent(repo: Path, tmp_path: Path) -> None:
    alias = tmp_path / "alias"
    alias.symlink_to(repo, target_is_directory=True)
    plan = plan_snapshot(
        start=repo, rev=None, script=alias / "sub" / "run.sh", cwd=repo
    )
    assert plan.script_rel == "sub/run.sh"


def test_script_absent_locally_but_in_commit(repo: Path) -> None:
    (repo / "sub" / "run.sh").unlink()
    git(repo, "update-index", "--assume-unchanged", "sub/run.sh")
    plan = plan_snapshot(start=repo, rev=None, script=repo / "sub" / "run.sh", cwd=repo)
    assert plan.script_text is not None and "sub" in plan.script_text


def test_script_outside_repo(repo: Path, tmp_path: Path) -> None:
    other = tmp_path / "x.sh"
    other.write_text("x")
    with pytest.raises(SnapshotValidationError, match="outside"):
        plan_snapshot(start=repo, rev=None, script=other, cwd=repo)


def test_cwd_rel_tree_file_absent(repo: Path) -> None:
    assert tree_has_dir(repo, head_commit(repo), "sub")
    assert not tree_has_dir(repo, head_commit(repo), "README")
    assert not tree_has_dir(repo, head_commit(repo), "missing")
    # cwd is a local-only directory absent from the commit -> snapshot top + warning
    (repo / "localonly").mkdir()
    plan = plan_snapshot(start=repo, rev=None, script=None, cwd=repo / "localonly")
    assert plan.cwd_rel is None
    assert any("localonly" in w for w in plan.warnings)


def test_ensure_regular_blob_executable_ok(repo: Path) -> None:
    git(repo, "update-index", "--chmod=+x", "job.sbatch")
    git(repo, "commit", "-q", "-m", "x")
    ensure_regular_blob(repo, head_commit(repo), "job.sbatch")
    assert "echo hi" in read_blob(repo, head_commit(repo), "job.sbatch")


def test_linked_worktree_shares_repo_id(repo: Path, tmp_path: Path) -> None:
    wt = tmp_path / "linked-wt"
    git(repo, "worktree", "add", "-q", "--detach", str(wt))
    a = plan_snapshot(start=repo, rev="HEAD", script=None, cwd=repo)
    b = plan_snapshot(start=wt.resolve(), rev="HEAD", script=None, cwd=wt)
    assert a.repo_id == b.repo_id


def test_repo_id_charset_and_smallest_root(repo: Path) -> None:
    git(repo, "checkout", "-q", "--orphan", "other")
    (repo / "o").write_text("o")
    commit_all(repo, "orphan root")
    git(repo, "merge", "-q", "--allow-unrelated-histories", "-m", "m", "main")
    merged = head_commit(repo)
    roots = sorted(git(repo, "rev-list", "--max-parents=0", merged).split())
    rid = compute_repo_id(repo, merged)
    assert rid == f"my-repo-{roots[0][:12]}"
    assert re.fullmatch(r"[A-Za-z0-9._-]+", rid)


def test_repo_id_sanitises_name(tmp_path: Path) -> None:
    r = tmp_path / "we ird$na;me"
    r.mkdir()
    git(r, "init", "-q")
    (r / "f").write_text("1")
    sha = commit_all(r)
    rid = compute_repo_id(r.resolve(), sha)
    assert re.fullmatch(r"we-ird-na-me-[0-9a-f]{12}", rid)


def test_physical_dir_nonexistent_tail(tmp_path: Path) -> None:
    real = tmp_path / "real"
    real.mkdir()
    (tmp_path / "ln").symlink_to(real, target_is_directory=True)
    assert physical_dir(tmp_path / "ln" / "a" / "b") == real.resolve() / "a" / "b"


def test_symlink_absolute_target_rejected(repo: Path) -> None:
    (repo / "lib").symlink_to("/home/u/live-lib")
    commit_all(repo)
    with pytest.raises(SnapshotValidationError, match="symlink") as ei:
        ensure_supported_tree(repo, head_commit(repo))
    assert "lib" in str(ei.value) and "/home/u/live-lib" in str(ei.value)


def test_symlink_escaping_repo_rejected(repo: Path) -> None:
    (repo / "sub" / "up").symlink_to("../../x")
    commit_all(repo)
    with pytest.raises(SnapshotValidationError, match="symlink") as ei:
        ensure_supported_tree(repo, head_commit(repo))
    assert "sub/up" in str(ei.value) and "../../x" in str(ei.value)


def test_symlink_in_repo_relative_accepted(repo: Path) -> None:
    (repo / "sub" / "to_readme").symlink_to("../README")
    (repo / "alias.sbatch").symlink_to("sub/../job.sbatch")
    commit_all(repo)
    ensure_supported_tree(repo, head_commit(repo))
