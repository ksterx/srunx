"""Run the snapshot inventory / prune scripts and the scheduler commands for real.

The "remote" is a temp directory driven by a local ``sh -s``; the scheduler
commands run through ``bash -c`` against shell shims on a private ``PATH``. One
module-scoped template holds a bare repo with three pushed commits and their
three snapshots (built by the real Phase 1 commit script); each test copies it.
A missing ``git`` fails the fixtures with an assertion; nothing is skipped.
"""

from __future__ import annotations

import os
import shutil
import stat
import subprocess
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

import pytest

from srunx.snapshot.errors import SnapshotLockTimeoutError, SnapshotRemoteError
from srunx.snapshot.inventory import (
    Inventory,
    InventoryStore,
    PruneRepoResult,
    PruneTarget,
    RepoInventory,
)
from srunx.snapshot.job_states import (
    SlurmExecute,
    query_job_cluster,
    query_job_states,
)
from srunx.snapshot.lease import Lease
from srunx.snapshot.prune import referencing_jobs
from srunx.snapshot.remote import ProbeResult, RemoteSnapshotStore
from tests.integration._local_shell import REAL_RUN, LocalShell

REPO_ID = "proj-0123456789ab"
TRASH_TOKEN = "f" * 32
GIT_ENV = {
    "GIT_AUTHOR_NAME": "t",
    "GIT_AUTHOR_EMAIL": "t@example.com",
    "GIT_COMMITTER_NAME": "t",
    "GIT_COMMITTER_EMAIL": "t@example.com",
    "GIT_CONFIG_GLOBAL": os.devnull,
    "GIT_CONFIG_SYSTEM": os.devnull,
}


@pytest.fixture(autouse=True)
def _real_subprocess(monkeypatch: pytest.MonkeyPatch) -> None:
    # tests/conftest.py replaces subprocess.run for every test; ``git push``
    # inside RemoteSnapshotStore and the shims need the real one.
    monkeypatch.setattr(subprocess, "run", REAL_RUN)


def _git(cwd: Path, *args: str) -> str:
    return REAL_RUN(
        ["git", *args],
        cwd=cwd,
        check=True,
        capture_output=True,
        text=True,
        env={**os.environ, **GIT_ENV},
    ).stdout.strip()


def _make_writable(path: Path) -> None:
    REAL_RUN(["chmod", "-R", "u+w", str(path)], check=False, capture_output=True)


@dataclass(frozen=True)
class Template:
    root: Path
    shas: tuple[str, str, str]
    git: str


@pytest.fixture(scope="module")
def template(tmp_path_factory: pytest.TempPathFactory) -> Iterator[Template]:
    git = shutil.which("git")
    assert git, "git must be installed to run the snapshot prune script tests"
    base = tmp_path_factory.mktemp("prune-template").resolve()
    src = base / "src"
    src.mkdir()
    _git(src, "init", "-q", "-b", "main")
    shas: list[str] = []
    for i in range(3):
        (src / f"file{i}").write_text(f"{i}\n")
        _git(src, "add", "-A")
        _git(src, "commit", "-q", "-m", f"c{i}")
        shas.append(_git(src, "rev-parse", "HEAD"))
    root = base / "root"
    shell = LocalShell(os.environ["PATH"])
    store = RemoteSnapshotStore(shell, snapshot_root=str(root), mounts=[], lock_wait=10)
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(subprocess, "run", REAL_RUN)
        probe = store.probe(repo_id=REPO_ID, sha=shas[0])
        # One git push for all three snapshot refs (store.push sends one each).
        _git(
            src,
            "push",
            "--quiet",
            f"--receive-pack={git} receive-pack",
            f"{probe.repo_dir}/repo.git",
            *(f"{sha}:refs/srunx/snapshots/{sha}" for sha in shas),
        )
        for sha in shas:
            store.commit(probe=probe, sha=sha, lease=None)
    yield Template(Path(probe.root), (shas[0], shas[1], shas[2]), git)
    _make_writable(base)


class Remote:
    """A private copy of the template with helpers to poke and read it."""

    def __init__(self, tmp: Path, tpl: Template) -> None:
        self.tpl = tpl
        self.shas = list(tpl.shas)
        self.root = (tmp / "root").resolve()
        REAL_RUN(["cp", "-R", str(tpl.root), str(self.root)], check=True)
        self.repo = self.root / REPO_ID
        self._retarget(str(tpl.root), str(self.root))
        self.git = tpl.git
        self.shell = LocalShell(os.environ["PATH"])

    def _retarget(self, old: str, new: str) -> None:
        """Git records absolute paths for worktrees; point the copy at its new home."""
        files = [
            *(self.repo / "trees" / s / ".git" for s in self.shas),
            *(self.repo / "repo.git" / "worktrees").glob("*/gitdir"),
        ]
        for f in files:
            parent = f.parent
            pmode = parent.stat().st_mode
            fmode = f.stat().st_mode
            parent.chmod(pmode | stat.S_IWUSR)
            f.chmod(fmode | stat.S_IWUSR)
            f.write_text(f.read_text().replace(old, new))
            f.chmod(fmode)
            parent.chmod(pmode)

    # -- stores
    def store(
        self, *, lock_wait: int = 10, shell: LocalShell | None = None
    ) -> RemoteSnapshotStore:
        return RemoteSnapshotStore(
            shell or self.shell,
            snapshot_root=str(self.root),
            mounts=[],
            lock_wait=lock_wait,
        )

    def inv_store(
        self, *, lock_wait: int = 10, shell: LocalShell | None = None
    ) -> InventoryStore:
        return InventoryStore(self.store(lock_wait=lock_wait, shell=shell))

    def probe(self) -> ProbeResult:
        return ProbeResult(
            root=str(self.root),
            repo_dir=str(self.repo),
            git=self.git,
            git_version="2",
            has_commit=True,
        )

    # -- reading (each call is one remote script run; keep them few)
    def inventory(self) -> Inventory:
        return self.inv_store().read()

    def repo_inv(self) -> RepoInventory:
        (repo,) = self.inventory().repos
        return repo

    @staticmethod
    def targets_of(repo: RepoInventory, *shas: str) -> list[PruneTarget]:
        by = {s.sha: s for s in repo.snapshots}
        out = []
        for sha in shas:
            fp_ids = by[sha].fp_lease_ids
            assert fp_ids is not None
            out.append(PruneTarget(sha, by[sha].fingerprint, tuple(sorted(fp_ids))))
        return out

    def targets(self, *shas: str) -> list[PruneTarget]:
        return self.targets_of(self.repo_inv(), *shas)

    def prune(
        self, targets: list[PruneTarget], *, store: InventoryStore | None = None
    ) -> PruneRepoResult:
        return (store or self.inv_store()).prune_repo(
            repo_dir=str(self.repo), git=self.git, targets=targets
        )

    # -- poking
    def tree(self, sha: str) -> Path:
        return self.repo / "trees" / sha

    def marker(self, sha: str) -> Path:
        return self.repo / "trees" / f"{sha}.complete"

    def lease_file(self, lease: Lease) -> Path:
        return self.repo / "leases" / f"{lease.lease_id}.json"

    def add_lease(
        self,
        sha: str,
        *,
        bound: bool = False,
        job_id: int = 5,
        cluster: str | None = "clusterA",
    ) -> Lease:
        lease = Lease.new_provisional(
            repo_id=REPO_ID,
            sha=sha,
            snapshot_path=f"{self.repo}/trees/{sha}",
            profile="p",
        )
        if bound:
            lease = lease.bound(job_id, cluster=cluster)
        self.lease_file(lease).write_text(lease.to_json() + "\n")
        return lease

    def remove_tree(self, sha: str) -> None:
        _make_writable(self.tree(sha))
        shutil.rmtree(self.tree(sha))

    def registered(self) -> str:
        return _git(self.repo / "repo.git", "worktree", "list", "--porcelain")

    def has_ref(self, sha: str) -> bool:
        proc = REAL_RUN(
            [
                "git",
                "--git-dir",
                str(self.repo / "repo.git"),
                "rev-parse",
                "--verify",
                "-q",
                f"refs/srunx/snapshots/{sha}",
            ],
            capture_output=True,
            text=True,
            check=False,
        )
        return proc.returncode == 0


@pytest.fixture
def rem(tmp_path: Path, template: Template) -> Iterator[Remote]:
    yield Remote(tmp_path, template)
    _make_writable(tmp_path)


def _shim(directory: Path, name: str, body: str) -> None:
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / name
    path.write_text("#!/bin/sh\n" + body)
    path.chmod(0o755)


def _shim_shell(directory: Path) -> LocalShell:
    return LocalShell(f"{directory}{os.pathsep}{os.environ['PATH']}")


# ---------------------------------------------------------------- inventory


class TestInventoryScript:
    def test_missing_and_empty_root(self, tmp_path: Path) -> None:
        shell = LocalShell(os.environ["PATH"])
        missing = RemoteSnapshotStore(
            shell, snapshot_root=str(tmp_path / "nope"), mounts=[]
        )
        inv = InventoryStore(missing).read()
        assert inv.exists is False and inv.repos == () and inv.now is not None
        (tmp_path / "empty").mkdir(mode=0o700)
        empty = RemoteSnapshotStore(
            shell, snapshot_root=str(tmp_path / "empty"), mounts=[]
        )
        inv = InventoryStore(empty).read()
        assert inv.exists is True and inv.repos == ()

    def test_snapshots_leases_lock_trash_and_stray(self, rem: Remote) -> None:
        sha0, sha1, _ = rem.shas
        lease = rem.add_lease(sha0, bound=True, job_id=77)
        # a lease for a commit that has no snapshot at all
        ghost = "9" * 40
        ghost_lease = rem.add_lease(ghost)
        (rem.repo / ".lock").mkdir()
        trash = rem.repo / "trees" / f".trash.{sha1}.{TRASH_TOKEN}"
        trash.mkdir()
        (rem.repo / "trees" / "stray.txt").write_text("x")
        (rem.repo / "notes").write_text("x")
        (rem.root / "loose").write_text("x")
        os.utime(rem.marker(sha0), (1_000_000, 1_000_000))

        inv = rem.inventory()
        assert inv.git and inv.git.startswith("/")
        assert inv.stray == ("loose",)
        (repo,) = inv.repos
        assert repo.skipped is None and repo.locked_since is not None
        assert repo.trash == (trash.name,)
        assert set(repo.stray) == {"trees/stray.txt", "notes"}
        by = {s.sha: s for s in repo.snapshots}
        assert set(by) == {*rem.shas, ghost}
        a = by[sha0]
        assert (a.has_tree, a.has_marker, a.intact) == (True, True, True)
        assert a.marker_mtime == 1_000_000
        assert a.fp_lease_ids == frozenset({lease.lease_id})
        g = by[ghost]
        assert (g.has_tree, g.has_marker, g.intact) == (False, False, False)
        assert g.fp_lease_ids == frozenset({ghost_lease.lease_id})
        assert by[sha1].fp_lease_ids == frozenset()
        rec = {r.filename: r for r in repo.leases}[f"{lease.lease_id}.json"]
        assert rec.lease is not None and rec.lease.job_id == 77
        assert rec.lease.cluster == "clusterA"

    def test_incomplete_marker_only_symlinked_and_bad_leases(self, rem: Remote) -> None:
        sha0, sha1, sha2 = rem.shas
        rem.marker(sha0).unlink()  # tree without marker
        rem.remove_tree(sha1)  # marker without tree
        outside = rem.root.parent / "elsewhere"
        outside.mkdir()
        rem.remove_tree(sha2)
        rem.tree(sha2).symlink_to(outside)  # a symlinked tree is never intact
        leases = rem.repo / "leases"
        good = rem.add_lease(sha0)
        target = rem.root.parent / "real-lease.json"
        target.write_text(good.to_json() + "\n")
        (leases / f"{'1' * 32}.json").symlink_to(target)
        (leases / f"{'2' * 32}.json").write_text("garbage {\n")
        (leases / f"{'3' * 32}.json").write_text("")
        (leases / f"{'4' * 32}.json").write_text("x" * 5000 + "\n")
        (leases / f"{'5' * 32}.json").mkdir()

        repo = rem.repo_inv()
        by = {s.sha: s for s in repo.snapshots}
        assert (by[sha0].has_tree, by[sha0].has_marker, by[sha0].intact) == (
            True,
            False,
            False,
        )
        assert (by[sha1].has_tree, by[sha1].has_marker) == (False, True)
        assert by[sha1].tree_mtime is None and by[sha1].marker_mtime is not None
        assert (by[sha2].has_tree, by[sha2].intact) == (True, False)
        assert by[sha2].tree_mtime is None
        records = {r.filename: r for r in repo.leases}
        assert records[f"{good.lease_id}.json"].lease is not None
        for ident in "12345":
            rec = records[f"{ident * 32}.json"]
            assert rec.lease is None and rec.error, ident

    def test_skipped_repos_other_repos_and_no_side_effects(self, rem: Remote) -> None:
        (rem.repo / "leases").chmod(0o770)  # writable by group: skipped
        symlinked = rem.root / "linked-0123456789ab"
        (symlinked / "repo.git").mkdir(parents=True, mode=0o700)
        (symlinked / "leases").mkdir(mode=0o700)
        moved = rem.root.parent / "moved-trees"
        moved.mkdir()
        (symlinked / "trees").symlink_to(moved)
        fine = rem.root / "other-0123456789ab"
        for sub in ("repo.git", "trees", "leases"):
            (fine / sub).mkdir(parents=True, mode=0o700)
        before = sorted(str(p) for p in rem.root.rglob("*"))

        inv = rem.inventory()

        assert sorted(str(p) for p in rem.root.rglob("*")) == before
        by = {r.repo_id: r for r in inv.repos}
        assert "writable" in (by[REPO_ID].skipped or "")
        assert by[REPO_ID].snapshots == () and by[REPO_ID].leases == ()
        assert by["linked-0123456789ab"].skipped
        assert "symlink" in (by["linked-0123456789ab"].skipped or "")
        assert by["other-0123456789ab"].skipped is None

    def test_group_writable_root_fails_the_inventory(self, rem: Remote) -> None:
        rem.root.chmod(0o770)
        with pytest.raises(SnapshotRemoteError, match="not private"):
            rem.inventory()

    def test_empty_leases_dir_fingerprint_and_size_on_request(
        self, rem: Remote
    ) -> None:
        assert list((rem.repo / "leases").iterdir()) == []
        repo = rem.inv_store().read(with_size=True).repos[0]
        assert all(s.size_kb is not None for s in repo.snapshots)
        for s in repo.snapshots:
            assert s.fp_lease_ids == frozenset()
            assert s.fingerprint.endswith(".l")
        # the prune script recomputes the very same fingerprint: nothing CHANGED
        res = rem.prune(rem.targets_of(repo, rem.shas[0]))
        assert res.removed == (rem.shas[0],) and res.changed == () and res.failed == ()


# --------------------------------------------------------------------- reuse


class TestReuseMarker:
    def test_reuse_touches_the_marker(self, rem: Remote) -> None:
        sha = rem.shas[0]
        os.utime(rem.marker(sha), (1_000_000, 1_000_000))
        result = rem.store().commit(probe=rem.probe(), sha=sha, lease=None)
        assert result.status == "reused"
        assert rem.marker(sha).stat().st_mtime > 1_000_000 + 10_000

    def test_reuse_fails_with_exit_33_when_touch_fails_or_has_no_effect(
        self, rem: Remote, tmp_path: Path
    ) -> None:
        sha = rem.shas[0]
        os.utime(rem.marker(sha), (1_000_000, 1_000_000))
        for name, body in (("failing", "exit 1\n"), ("noop", "exit 0\n")):
            shim = tmp_path / f"shim-{name}"
            _shim(shim, "touch", body)
            store = rem.store(shell=_shim_shell(shim))
            with pytest.raises(SnapshotRemoteError, match="exit 33") as ei:
                store.commit(probe=rem.probe(), sha=sha, lease=None)
            assert "last-requested" in str(ei.value)
        assert rem.marker(sha).stat().st_mtime == 1_000_000


# --------------------------------------------------------------------- prune


class TestPruneScript:
    def test_removes_exactly_the_targets_and_nothing_protected(
        self, rem: Remote
    ) -> None:
        sha0, sha1, sha2 = rem.shas
        mine = rem.add_lease(sha0, bound=True)
        other = rem.add_lease(sha1, bound=True, job_id=6)
        stray_repo = rem.repo / "notes"
        stray_repo.write_text("keep")
        stray_trees = rem.repo / "trees" / "stray.txt"
        stray_trees.write_text("keep")
        loose = rem.root / "loose"
        loose.write_text("keep")
        leftover = rem.repo / "trees" / f".trash.{sha1}.{'1' * 32}"
        leftover.mkdir()
        assert rem.has_ref(sha0) and str(rem.tree(sha0)) in rem.registered()

        res = rem.prune(rem.targets(sha0, sha2))

        assert sorted(res.removed) == sorted([sha0, sha2])
        assert res.failed == () and res.changed == () and res.trash_left == 0
        for sha in (sha0, sha2):
            assert not rem.tree(sha).exists() and not rem.marker(sha).exists()
            assert not rem.has_ref(sha)
            assert str(rem.tree(sha)) not in rem.registered()
        assert not rem.lease_file(mine).exists()
        assert rem.lease_file(other).exists()
        assert (
            rem.tree(sha1).is_dir() and rem.marker(sha1).exists() and rem.has_ref(sha1)
        )
        assert str(rem.tree(sha1)) in rem.registered()
        # leftover trash from an earlier run is swept too; nothing else is
        assert not leftover.exists()
        assert sorted(p.name for p in (rem.repo / "trees").iterdir()) == sorted(
            [sha1, f"{sha1}.complete", "stray.txt"]
        )
        assert not (rem.repo / ".lock").exists()
        for keep in (rem.repo / "repo.git", rem.repo / "trees", rem.repo / "leases"):
            assert keep.is_dir()
        assert stray_repo.read_text() == "keep" and loose.read_text() == "keep"
        assert stray_trees.read_text() == "keep"
        # a pruned sha can be recreated by the commit script
        again = rem.store().commit(probe=rem.probe(), sha=sha0, lease=None)
        assert again.status == "created"

    def test_changed_shas_are_left_alone_while_others_are_removed(
        self, rem: Remote
    ) -> None:
        sha0, sha1, sha2 = rem.shas
        ghost = "9" * 40  # a sha that only has a lease
        existing = rem.add_lease(sha0)
        rem.add_lease(ghost)
        targets = rem.targets(sha0, sha1, sha2, ghost)
        # sha0: lease rewritten (provisional -> bound); sha1: marker touched;
        # ghost: a second lease appears after the inventory
        path = rem.lease_file(existing)
        path.write_text(existing.bound(9, cluster="clusterA").to_json() + "\n")
        os.utime(path, (2_000_000, 2_000_000))
        os.utime(rem.marker(sha1), (3_000_000, 3_000_000))
        late = rem.add_lease(ghost)

        res = rem.prune(targets)

        assert sorted(res.changed) == sorted([sha0, sha1, ghost])
        assert res.removed == (sha2,) and res.failed == ()
        for sha in (sha0, sha1):
            assert (
                rem.tree(sha).is_dir() and rem.marker(sha).exists() and rem.has_ref(sha)
            )
        assert path.exists() and rem.lease_file(late).exists()
        assert not rem.tree(sha2).exists()
        assert len(list((rem.repo / "leases").iterdir())) == 3

    def test_failed_when_the_rename_fails_leaves_the_tree_in_place(
        self, rem: Remote, tmp_path: Path
    ) -> None:
        sha = rem.shas[0]
        shim = tmp_path / "shim"
        _shim(shim, "mv", "exit 1\n")
        targets = rem.targets(sha)
        res = rem.prune(targets, store=rem.inv_store(shell=_shim_shell(shim)))
        assert res.failed == ((sha, "rename"),)
        assert res.removed == () and rem.tree(sha).is_dir()
        assert not (rem.repo / ".lock").exists()

    def test_held_lock_times_out_and_the_lock_survives(self, rem: Remote) -> None:
        sha = rem.shas[0]
        lock = rem.repo / ".lock"
        lock.mkdir()
        (lock / "owner").write_text("host=other\npid=1\ntoken=zz\n")
        target = PruneTarget(sha, "m-.t-.i0.l", ())  # never compared: no lock
        with pytest.raises(SnapshotLockTimeoutError, match="host=other"):
            rem.prune([target], store=rem.inv_store(lock_wait=0))
        assert (lock / "owner").read_text().startswith("host=other")
        assert rem.tree(sha).is_dir() and rem.marker(sha).exists()

    def test_trash_sweep_removes_only_owned_real_trash_directories(
        self, rem: Remote
    ) -> None:
        sha = rem.shas[0]
        trees = rem.repo / "trees"
        real = trees / f".trash.{sha}.{'1' * 32}"
        (real / "sub").mkdir(parents=True)
        (real / "sub" / "f").write_text("x")
        (real / "sub").chmod(0o500)  # read-only inside, as a pruned snapshot is
        bad_name = trees / ".trash.foo"
        bad_name.mkdir()
        outside = rem.root.parent / "outside-dir"
        outside.mkdir()
        (outside / "keep").write_text("keep")
        link = trees / f".trash.{sha}.{'2' * 32}"
        link.symlink_to(outside)
        regular = trees / f".trash.{sha}.{'3' * 32}"
        regular.write_text("x")
        plain_file = trees / ".trash"
        plain_file.write_text("x")

        inv_store = rem.inv_store()
        repo = inv_store.read().repos[0]
        assert repo.trash == (real.name,)
        assert {
            "trees/.trash.foo",
            f"trees/{link.name}",
            f"trees/{regular.name}",
            "trees/.trash",
        } <= set(repo.stray)
        assert real.is_dir()  # the inventory never sweeps

        res = rem.prune([], store=inv_store)

        assert res.removed == () and res.trash_left == 0
        assert not real.exists()
        assert bad_name.is_dir() and link.is_symlink() and regular.is_file()
        assert plain_file.is_file() and (outside / "keep").read_text() == "keep"
        assert set(res.trash_skipped) == {
            ".trash.foo",
            link.name,
            regular.name,
            ".trash",
        }
        assert not (rem.repo / ".lock").exists()

    def test_incomplete_marker_only_leases_only_and_symlinked_shas_are_removed(
        self, rem: Remote
    ) -> None:
        sha0, sha1, sha2 = rem.shas
        ghost = "9" * 40
        lease = rem.add_lease(ghost, bound=True)
        rem.marker(sha0).unlink()  # tree without marker
        rem.remove_tree(sha1)  # marker without tree
        outside = rem.root.parent / "elsewhere"
        outside.mkdir()
        (outside / "keep").write_text("keep")
        rem.remove_tree(sha2)
        rem.tree(sha2).symlink_to(outside)  # unlinked, never followed
        res = rem.prune(rem.targets(sha0, sha1, sha2, ghost))
        assert sorted(res.removed) == sorted([sha0, sha1, sha2, ghost])
        assert not rem.tree(sha0).exists() and not rem.marker(sha1).exists()
        assert not rem.tree(sha2).is_symlink() and not rem.lease_file(lease).exists()
        assert (outside / "keep").read_text() == "keep"


# --------------------------------------------------------- scheduler commands

_SQUEUE_SHIM = r"""echo "squeue: some noise" >&2
fmt=
all=0
tall=0
while [ $# -gt 0 ]; do
  case $1 in
    -o) fmt=$2; shift ;;
    -a) all=1 ;;
    -t) [ "$2" = all ] && tall=1; shift ;;
  esac
  shift
done
US=$(printf '\037')
[ "$all" = 1 ] && [ "$tall" = 1 ] || { echo "missing -a / -t all" >&2; exit 4; }
[ "$fmt" = "%F${US}%A${US}%T${US}%Z${US}%o" ] || { echo "bad format" >&2; exit 3; }
printf '7%s7%sRUNNING%s/work dir/a|b%sexperiments/x/train.sbatch --opt "a | b"\n' "$US" "$US" "$US" "$US"
printf '8%s9%sCOMPLETED%s/w2%s/abs/run.sh\n' "$US" "$US" "$US" "$US"
"""

_SACCT_SHIM = r"""[ "${SLURM_TIME_FORMAT:-}" = "%s" ] || { echo "no epoch format" >&2; exit 5; }
case " $* " in
  *" -j "*) ;;
  *) echo "unexpected args: $*" >&2; exit 6 ;;
esac
echo "10|COMPLETED|1000|1500|clusterA"
echo "11|FAILED|1000|1600|clusterA"
"""

_SCONTROL_SHIM = r"""case "$*" in
  "show config") echo "MinJobAge              = 300 sec" ;;
  "show job 12") echo "JobId=12 JobName=x JobState=COMPLETED" ;;
  *) echo "slurm_load_jobs error: Invalid job id specified" >&2; exit 1 ;;
esac
"""


def _min_path(directory: Path, *tools: str) -> Path:
    """A PATH directory holding symlinks to just *tools* (so others are absent)."""
    directory.mkdir(parents=True, exist_ok=True)
    for tool in tools:
        found = shutil.which(tool)
        assert found, tool
        (directory / tool).symlink_to(found)
    return directory


def _executor(*dirs: Path) -> SlurmExecute:
    env = {"PATH": os.pathsep.join(str(d) for d in dirs), "HOME": str(dirs[0])}

    def run(cmd: str) -> tuple[str, str, int]:
        proc = REAL_RUN(
            ["/bin/bash", "-c", cmd],
            capture_output=True,
            text=True,
            env=env,
            check=False,
            timeout=30,
        )
        return proc.stdout, proc.stderr, proc.returncode

    return run


@pytest.fixture
def tools(tmp_path: Path) -> Path:
    return _min_path(tmp_path / "base-bin", "id", "date")


class TestSchedulerCommands:
    def test_queue_command_runs_against_shims_and_parses_every_shape(
        self, tmp_path: Path, tools: Path
    ) -> None:
        bin_ = tmp_path / "bin"
        _shim(bin_, "squeue", _SQUEUE_SHIM)
        _shim(bin_, "sacct", _SACCT_SHIM)
        _shim(bin_, "scontrol", _SCONTROL_SHIM)
        report = query_job_states([10, 11, 12, 13], _executor(bin_, tools))
        assert report.error is None
        assert report.now is not None and report.min_job_age == 300
        assert (
            report.states[10].kind == "terminal" and report.states[10].ended_at == 1500
        )
        assert report.states[10].cluster == "clusterA"
        assert report.states[11].state == "FAILED"
        assert (
            report.states[12].kind == "active"
            and report.states[12].source == "scontrol"
        )
        assert report.states[13].kind == "unknown"
        workdirs = {r.job_id: (r.workdir, r.command) for r in report.queue}
        assert workdirs[7] == (
            "/work dir/a|b",
            'experiments/x/train.sbatch --opt "a | b"',
        )
        assert workdirs[9] == ("/w2", "/abs/run.sh")
        # job 8 is listed through the array base id of the COMPLETED row
        assert query_job_states([8], _executor(bin_, tools)).states[8].kind == "active"

    def test_stderr_noise_does_not_matter_and_quiet_commands_work_too(
        self, tmp_path: Path, tools: Path
    ) -> None:
        bin_ = tmp_path / "bin"
        quiet = _SQUEUE_SHIM.replace('echo "squeue: some noise" >&2\n', "")
        _shim(bin_, "squeue", quiet)
        report = query_job_states([], _executor(bin_, tools))
        assert report.error is None and len(report.queue) == 2

    def test_relative_command_with_absolute_workdir_references_by_workdir(
        self, tmp_path: Path, tools: Path
    ) -> None:
        bin_ = tmp_path / "bin"
        sha = "a" * 40
        script = (
            _SQUEUE_SHIM.split("printf '7")[0]
            + f'printf \'7%s7%sRUNNING%s/r/snap/{REPO_ID}/trees/{sha}/sub%srelative/run.sbatch\\n\' "$US" "$US" "$US" "$US"\n'
        )
        _shim(bin_, "squeue", script)
        report = query_job_states([], _executor(bin_, tools))
        assert report.error is None
        assert referencing_jobs(REPO_ID, sha, report.queue) == (7,)

    def test_scontrol_absent_means_no_min_job_age_and_unknown_jobs(
        self, tmp_path: Path, tools: Path
    ) -> None:
        bin_ = tmp_path / "bin"
        _shim(bin_, "squeue", _SQUEUE_SHIM)
        _shim(bin_, "sacct", "exit 1\n")
        report = query_job_states([12], _executor(bin_, tools))
        assert report.error is None
        assert report.min_job_age is None
        assert report.states[12].kind == "unknown"

    def test_scontrol_denied_is_the_same_as_absent(
        self, tmp_path: Path, tools: Path
    ) -> None:
        bin_ = tmp_path / "bin"
        _shim(bin_, "squeue", _SQUEUE_SHIM)
        _shim(bin_, "sacct", "exit 1\n")
        _shim(bin_, "scontrol", "echo denied >&2; exit 126\n")
        report = query_job_states([12], _executor(bin_, tools))
        assert report.error is None and report.min_job_age is None
        assert report.states[12].kind == "unknown"

    def test_sacct_absent_never_makes_a_job_terminal(
        self, tmp_path: Path, tools: Path
    ) -> None:
        bin_ = tmp_path / "bin"
        _shim(bin_, "squeue", _SQUEUE_SHIM)
        report = query_job_states([10, 11], _executor(bin_, tools))
        assert report.error is None
        assert {report.states[10].kind, report.states[11].kind} == {"unknown"}

    def test_squeue_absent_fails_closed(self, tmp_path: Path, tools: Path) -> None:
        bin_ = tmp_path / "bin"
        _shim(bin_, "sacct", _SACCT_SHIM)
        report = query_job_states([10, 11], _executor(bin_, tools))
        assert report.error is not None
        assert {s.kind for s in report.states.values()} == {"unknown"}

    def test_squeue_failing_with_exit_zero_wrapper_still_fails_closed(
        self, tmp_path: Path, tools: Path
    ) -> None:
        bin_ = tmp_path / "bin"
        _shim(bin_, "squeue", "echo down >&2; exit 1\n")
        report = query_job_states([10], _executor(bin_, tools))
        assert report.error is not None

    def test_cluster_lookup_with_timeout_present(
        self, tmp_path: Path, tools: Path
    ) -> None:
        bin_ = tmp_path / "bin"
        log = tmp_path / "timeout.log"
        _shim(bin_, "timeout", f'echo "$*" >> {log}\nshift\nexec "$@"\n')
        _shim(bin_, "sacct", 'echo "clusterA"\n')
        assert query_job_cluster(5, _executor(bin_, tools)) == "clusterA"
        assert log.read_text().startswith("10 sacct ")

    def test_cluster_lookup_with_timeout_absent(
        self, tmp_path: Path, tools: Path
    ) -> None:
        bin_ = tmp_path / "bin"
        _shim(bin_, "sacct", 'echo "clusterA"\n')
        assert not (tools / "timeout").exists()
        assert query_job_cluster(5, _executor(bin_, tools)) == "clusterA"

    def test_cluster_lookup_timeout_expiring_gives_none(
        self, tmp_path: Path, tools: Path
    ) -> None:
        bin_ = tmp_path / "bin"
        _shim(bin_, "timeout", "exit 124\n")
        _shim(bin_, "sacct", 'echo "clusterA"\n')
        assert query_job_cluster(5, _executor(bin_, tools)) is None

    def test_cluster_lookup_with_sacct_absent_gives_none(
        self, tmp_path: Path, tools: Path
    ) -> None:
        assert query_job_cluster(5, _executor(tools)) is None

    def test_cluster_lookup_runs_sacct_exactly_once(
        self, tmp_path: Path, tools: Path
    ) -> None:
        bin_ = tmp_path / "bin"
        log = tmp_path / "sacct.log"
        _shim(bin_, "sacct", f"echo run >> {log}\necho clusterA\n")
        assert query_job_cluster(5, _executor(bin_, tools)) == "clusterA"
        assert log.read_text().count("run") == 1
