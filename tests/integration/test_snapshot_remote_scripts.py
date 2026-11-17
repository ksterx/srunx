"""Run the generated snapshot scripts for real, with a local ``sh -s`` as the "remote".

The remote is a temp directory, the bare repo is local, and git is wrapped by a
logging shim so tests can count ``worktree add`` calls. A missing ``git`` fails
the fixture with an assertion.
"""

from __future__ import annotations

import json
import os
import shutil
import signal
import stat
import subprocess
import threading
import time
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

import pytest

from srunx.snapshot import remote
from srunx.snapshot.errors import SnapshotLockTimeoutError, SnapshotRemoteError
from srunx.snapshot.lease import Lease
from srunx.snapshot.remote import (
    LockOwner,
    RemoteSnapshotStore,
    build_commit_script,
    build_probe_script,
    root_expr,
)
from tests.integration._local_shell import REAL_RUN, LocalShell

_REAL_RUN = REAL_RUN
_REAL_POPEN = subprocess.Popen

TOKEN = "a1" * 16


OWNER = LockOwner(client_host="testhost", client_pid=4242, token=TOKEN)


@dataclass
class Env:
    tmp: Path
    shell: LocalShell
    repo: Path
    sha: str
    shim_dir: Path
    shim_log: Path
    root: Path

    def store(
        self,
        *,
        lock_wait: int = 10,
        mounts: list[str] | None = None,
        root: Path | None = None,
    ) -> RemoteSnapshotStore:
        return RemoteSnapshotStore(
            self.shell,
            snapshot_root=str(root or self.root),
            mounts=mounts or [],
            lock_wait=lock_wait,
        )

    def probe_script(
        self,
        *,
        root: Path | None = None,
        mounts: list[str] | None = None,
        lock_wait: int = 10,
    ) -> str:
        return build_probe_script(
            root_expr=root_expr(str(root or self.root)),
            repo_id="proj-0123456789ab",
            sha=self.sha,
            mounts=mounts or [],
            lock_wait=lock_wait,
            **OWNER,
        )

    def worktree_adds(self) -> int:
        if not self.shim_log.exists():
            return 0
        return sum(
            1
            for ln in self.shim_log.read_text().splitlines()
            if " worktree add " in f" {ln} "
        )

    def prepare(
        self, store: RemoteSnapshotStore | None = None
    ) -> tuple[RemoteSnapshotStore, remote.ProbeResult]:
        store = store or self.store()
        probe = store.probe(repo_id="proj-0123456789ab", sha=self.sha)
        if not probe.has_commit:
            store.push(repo_top=self.repo, probe=probe, sha=self.sha)
        return store, probe

    def new_lease(self, probe: remote.ProbeResult) -> Lease:
        return Lease.new_provisional(
            repo_id="proj-0123456789ab",
            sha=self.sha,
            snapshot_path=f"{probe.repo_dir}/trees/{self.sha}",
            profile="prof",
        )


def _git(cwd: Path, *args: str) -> str:
    return _REAL_RUN(
        ["git", *args], cwd=cwd, check=True, capture_output=True, text=True
    ).stdout.strip()


def _make_shim(directory: Path, real_git: str, log: Path) -> None:
    directory.mkdir(parents=True, exist_ok=True)
    shim = directory / "git"
    shim.write_text(
        "#!/bin/sh\n"
        f'echo "$*" >> {log}\n'
        'case " $* " in\n'
        "  *' worktree add '*)\n"
        f"    if [ -e {directory}/slow ]; then touch {directory}/started; sleep 1; exit 1; fi\n"
        "    ;;\n"
        "esac\n"
        f'exec {real_git} "$@"\n'
    )
    shim.chmod(0o755)


@pytest.fixture(scope="module")
def source_repo(tmp_path_factory: pytest.TempPathFactory) -> tuple[Path, str]:
    """One local repo shared by every test (building it costs several git runs)."""
    assert shutil.which("git"), "git must be installed to run the snapshot script tests"
    repo = tmp_path_factory.mktemp("local")
    git_env = {
        **os.environ,
        "GIT_AUTHOR_NAME": "t",
        "GIT_AUTHOR_EMAIL": "t@example.com",
        "GIT_COMMITTER_NAME": "t",
        "GIT_COMMITTER_EMAIL": "t@example.com",
        "GIT_CONFIG_GLOBAL": os.devnull,
        "GIT_CONFIG_SYSTEM": os.devnull,
    }

    def run(*args: str) -> str:
        return _REAL_RUN(
            ["git", *args],
            cwd=repo,
            check=True,
            capture_output=True,
            text=True,
            env=git_env,
        ).stdout.strip()

    run("init", "-q", "-b", "main")
    (repo / "README").write_text("hello\n")
    (repo / "run.sh").write_text("#!/bin/sh\necho run\n")
    run("add", "-A")
    run("commit", "-q", "-m", "c")
    return repo, run("rev-parse", "HEAD")


@pytest.fixture
def env(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, source_repo: tuple[Path, str]
) -> Iterator[Env]:
    monkeypatch.setattr(subprocess, "run", _REAL_RUN)
    real_git = shutil.which("git")
    assert real_git, "git must be installed to run the snapshot script tests"
    repo, sha = source_repo

    shim_dir = tmp_path / "shim"
    shim_log = tmp_path / "shim.log"
    _make_shim(shim_dir, real_git, shim_log)
    shell = LocalShell(f"{shim_dir}{os.pathsep}{os.environ['PATH']}")

    root = tmp_path / "remote-root"
    yield Env(tmp_path, shell, repo, sha, shim_dir, shim_log, root)

    # Snapshots are read-only; restore write permission so pytest can clean up.
    for dirpath, dirnames, filenames in os.walk(tmp_path):
        for name in [*dirnames, *filenames]:
            p = Path(dirpath) / name
            if not p.is_symlink():
                p.chmod(p.stat().st_mode | stat.S_IWUSR)
        Path(dirpath).chmod(Path(dirpath).stat().st_mode | stat.S_IWUSR)


def _u_w(path: Path) -> None:
    subprocess.run(["chmod", "-R", "u+w", str(path)], check=True)


# --------------------------------------------------------------- create / reuse


def test_create_snapshot_is_readonly_with_marker_and_lease(env: Env) -> None:
    store, probe = env.prepare()
    assert probe.git == str(env.shim_dir / "git")
    lease = env.new_lease(probe)
    res = store.commit(probe=probe, sha=env.sha, lease=lease)

    tree = Path(res.path)
    assert res.status == "created"
    assert tree == Path(probe.repo_dir) / "trees" / env.sha
    assert (tree / "README").read_text() == "hello\n"
    assert not os.access(tree / "README", os.W_OK)
    assert not os.access(tree, os.W_OK)
    assert (Path(probe.repo_dir) / "trees" / f"{env.sha}.complete").is_file()

    lease_file = Path(res.lease_path or "")
    assert lease_file == Path(probe.repo_dir) / "leases" / f"{lease.lease_id}.json"
    assert stat.S_IMODE(lease_file.stat().st_mode) == 0o600
    data = json.loads(lease_file.read_text())
    assert data["state"] == "provisional" and data["schema"] == 1
    assert Lease.from_json(lease_file.read_text()) == lease
    assert stat.S_IMODE((Path(probe.repo_dir) / "leases").stat().st_mode) == 0o700
    assert not (Path(probe.repo_dir) / ".lock").exists()


def test_reuse_does_not_add_a_second_worktree(env: Env) -> None:
    store = env.store()
    assert not store.probe(repo_id="proj-0123456789ab", sha=env.sha).has_commit
    store, probe = env.prepare(store)
    ref = _git(
        Path(probe.repo_dir) / "repo.git",
        "rev-parse",
        f"refs/srunx/snapshots/{env.sha}",
    )
    assert ref == env.sha
    first = store.commit(probe=probe, sha=env.sha, lease=None)
    assert first.status == "created" and first.lease_path is None
    second = store.commit(probe=probe, sha=env.sha, lease=None)
    assert second.status == "reused"
    assert env.worktree_adds() == 1
    # A second probe reports HAVE=1, so the caller needs no push.
    assert store.probe(repo_id="proj-0123456789ab", sha=env.sha).has_commit


def test_directory_without_marker_is_recovered(env: Env) -> None:
    store, probe = env.prepare()
    res = store.commit(probe=probe, sha=env.sha, lease=None)
    tree = Path(res.path)
    marker = Path(probe.repo_dir) / "trees" / f"{env.sha}.complete"
    _u_w(tree)
    marker.unlink()
    # Simulate a half-written, unregistered directory left by a crash.
    shutil.rmtree(tree)
    tree.mkdir()
    (tree / "junk").write_text("partial")
    again = store.commit(probe=probe, sha=env.sha, lease=None)
    assert again.status == "recovered"
    assert not (tree / "junk").exists()
    assert (tree / "README").is_file() and marker.is_file()
    assert not os.access(tree, os.W_OK)


def test_registration_without_directory_is_recovered(env: Env) -> None:
    store, probe = env.prepare()
    res = store.commit(probe=probe, sha=env.sha, lease=None)
    tree = Path(res.path)
    _u_w(tree)
    shutil.rmtree(tree)
    (Path(probe.repo_dir) / "trees" / f"{env.sha}.complete").unlink()
    listing = _git(Path(probe.repo_dir) / "repo.git", "worktree", "list", "--porcelain")
    assert str(tree) in listing  # still registered
    again = store.commit(probe=probe, sha=env.sha, lease=None)
    assert again.status == "recovered"
    assert (tree / "README").is_file()
    assert env.worktree_adds() == 2


def test_marker_without_tree_is_recovered(env: Env) -> None:
    store, probe = env.prepare()
    res = store.commit(probe=probe, sha=env.sha, lease=None)
    tree = Path(res.path)
    _u_w(tree)
    shutil.rmtree(tree)
    assert (Path(probe.repo_dir) / "trees" / f"{env.sha}.complete").exists()
    again = store.commit(probe=probe, sha=env.sha, lease=None)
    assert again.status == "recovered" and (tree / "run.sh").is_file()


def test_unwritable_tree_without_marker_is_recovered(env: Env) -> None:
    store, probe = env.prepare()
    res = store.commit(probe=probe, sha=env.sha, lease=None)
    (Path(probe.repo_dir) / "trees" / f"{env.sha}.complete").unlink()
    assert not os.access(res.path, os.W_OK)  # read-only, registered, no marker
    again = store.commit(probe=probe, sha=env.sha, lease=None)
    assert again.status == "recovered"
    assert not os.access(res.path, os.W_OK)


def test_missing_commit_exits_26(env: Env) -> None:
    store = env.store()
    probe = store.probe(repo_id="proj-0123456789ab", sha=env.sha)
    assert not probe.has_commit  # never pushed
    with pytest.raises(SnapshotRemoteError, match="exit 26"):
        store.commit(probe=probe, sha=env.sha, lease=None)
    assert not (Path(probe.repo_dir) / ".lock").exists()


# ---------------------------------------------------------------------- leases


def _commit_script(env: Env, probe: remote.ProbeResult, lease: Lease | None) -> str:
    return build_commit_script(
        repo_dir=probe.repo_dir,
        git=probe.git,
        sha=env.sha,
        lease_json=lease.to_json() if lease else None,
        lease_id=lease.lease_id if lease else None,
        lock_wait=5,
        **OWNER,
    )


def test_lease_target_symlink_exits_31(env: Env) -> None:
    store, probe = env.prepare()
    lease = env.new_lease(probe)
    victim = env.tmp / "victim"
    victim.write_text("keep")
    (Path(probe.repo_dir) / "leases" / f"{lease.lease_id}.json").symlink_to(victim)
    proc = env.shell.run_script(_commit_script(env, probe, lease))
    assert proc.returncode == 31, proc.stderr
    assert victim.read_text() == "keep"
    assert not (Path(probe.repo_dir) / ".lock").exists()


def test_leases_dir_symlink_exits_31(env: Env) -> None:
    store, probe = env.prepare()
    leases = Path(probe.repo_dir) / "leases"
    elsewhere = env.tmp / "elsewhere"
    elsewhere.mkdir()
    leases.rmdir()
    leases.symlink_to(elsewhere)
    proc = env.shell.run_script(_commit_script(env, probe, env.new_lease(probe)))
    assert proc.returncode in (31, 32), proc.stderr
    assert list(elsewhere.iterdir()) == []


def test_bind_marker_present_then_removed_then_delete_lease(env: Env) -> None:
    store, probe = env.prepare()
    lease = env.new_lease(probe)
    res = store.commit(probe=probe, sha=env.sha, lease=lease)
    path = Path(store.lease_path(probe.repo_dir, lease.lease_id))

    # marker present: bound, mode 0600, no temp files left behind
    assert store.bind(repo_dir=probe.repo_dir, lease=lease.bound(77)) is True
    data = json.loads(path.read_text())
    assert data["state"] == "bound" and data["job_id"] == 77
    assert stat.S_IMODE(path.stat().st_mode) == 0o600
    assert not [p for p in path.parent.iterdir() if p.name.startswith(".tmp")]

    # marker removed: reported missing, the lease still records the job
    (Path(probe.repo_dir) / "trees" / f"{env.sha}.complete").unlink()
    assert store.bind(repo_dir=probe.repo_dir, lease=lease.bound(78)) is False
    data = json.loads(path.read_text())
    assert data["state"] == "bound" and data["job_id"] == 78

    # delete is idempotent and leaves the snapshot alone
    store.delete_lease(repo_dir=probe.repo_dir, lease_id=lease.lease_id)
    assert not Path(res.lease_path or "").exists()
    store.delete_lease(repo_dir=probe.repo_dir, lease_id=lease.lease_id)
    assert (Path(probe.repo_dir) / "trees" / env.sha).is_dir()


# ----------------------------------------------------------------------- locks


def _foreign_lock(probe: remote.ProbeResult) -> Path:
    lock = Path(probe.repo_dir) / ".lock"
    lock.mkdir()
    (lock / "owner").write_text(
        "host=other\npid=999\nts=1700000000\ntime=then\ntoken=" + "f" * 32 + "\n"
    )
    return lock


def test_waiter_blocks_until_holder_releases_then_proceeds(env: Env) -> None:
    store, probe = env.prepare(env.store(lock_wait=15))
    lock = _foreign_lock(probe)

    def release() -> None:
        time.sleep(0.3)
        (lock / "owner").unlink()
        lock.rmdir()

    t = threading.Thread(target=release)
    started = time.monotonic()
    t.start()
    res = store.commit(probe=probe, sha=env.sha, lease=None)
    elapsed = time.monotonic() - started
    t.join()
    assert res.status == "created"
    assert elapsed >= 0.2
    assert not lock.exists()


def test_waiter_times_out_and_never_breaks_the_lock(env: Env) -> None:
    store, probe = env.prepare(env.store(lock_wait=1))
    lock = _foreign_lock(probe)
    started = time.monotonic()
    with pytest.raises(SnapshotLockTimeoutError) as ei:
        store.commit(probe=probe, sha=env.sha, lease=None)
    assert time.monotonic() - started < 10
    msg = str(ei.value)
    assert str(lock) in msg
    assert "host=other" in msg and "pid=999" in msg and "ts=1700000000" in msg
    assert "token=" + "f" * 32 in msg
    assert f"rm -rf {lock}" in msg
    assert (lock / "owner").is_file()  # foreign lock untouched
    assert not (Path(probe.repo_dir) / "trees" / env.sha).exists()


def test_sigterm_to_lock_holder_leaves_no_lock(env: Env) -> None:
    store, probe = env.prepare()
    (env.shim_dir / "slow").touch()  # shim makes `worktree add` linger, lock held
    script = _commit_script(env, probe, None)
    lock = Path(probe.repo_dir) / ".lock"
    proc = _REAL_POPEN(
        ["/bin/sh", "-s"],
        stdin=subprocess.PIPE,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        text=True,
        env=env.shell.env,
    )
    assert proc.stdin is not None
    proc.stdin.write(script)
    proc.stdin.close()
    try:
        deadline = time.monotonic() + 10
        while not (env.shim_dir / "started").exists():
            assert time.monotonic() < deadline, "script never reached worktree add"
            time.sleep(0.05)
        assert lock.is_dir()
        os.kill(proc.pid, signal.SIGTERM)
        proc.wait(timeout=15)
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait()
    assert proc.returncode == 143
    assert not lock.exists()


def test_closed_stdout_pipe_leaves_no_lock(env: Env) -> None:
    """sshd without a pty sends no SIGHUP; the script dies of SIGPIPE on its write."""
    store, probe = env.prepare()
    script = _commit_script(env, probe, None)
    lock = Path(probe.repo_dir) / ".lock"
    read_fd, write_fd = os.pipe()
    os.close(read_fd)  # the reader is already gone when the script writes
    # dash (the usual remote /bin/sh) skips the EXIT trap on SIGPIPE; bash does not.
    sh = shutil.which("dash") or "/bin/sh"
    try:
        proc = _REAL_POPEN(
            [sh, "-s"],
            stdin=subprocess.PIPE,
            stdout=write_fd,
            stderr=subprocess.DEVNULL,
            text=True,
            env=env.shell.env,
        )
    finally:
        os.close(write_fd)
    proc.communicate(script, timeout=30)
    assert proc.returncode != 0
    assert not lock.exists()


# --------------------------------------------------------- root vs mount checks


def _assert_exit(env: Env, script: str, code: int) -> subprocess.CompletedProcess[str]:
    proc = env.shell.run_script(script)
    assert proc.returncode == code, (proc.returncode, proc.stderr)
    return proc


def test_root_inside_mount_literally_exits_23(env: Env) -> None:
    mount = env.tmp / "mount"
    mount.mkdir()
    root = mount / "snaps"
    _assert_exit(env, env.probe_script(root=root, mounts=[str(mount)]), 23)
    assert not root.exists()


def test_root_inside_mount_via_symlink_exits_23(env: Env) -> None:
    mount = env.tmp / "mount"
    mount.mkdir()
    link = env.tmp / "alias"
    link.symlink_to(mount, target_is_directory=True)
    root = link / "snaps"
    _assert_exit(env, env.probe_script(root=root, mounts=[str(mount)]), 23)
    assert not (mount / "snaps").exists()


def test_mount_reached_via_symlink_while_root_is_literal_exits_23(env: Env) -> None:
    real = env.tmp / "real"
    real.mkdir()
    link = env.tmp / "alias"
    link.symlink_to(real, target_is_directory=True)
    _assert_exit(env, env.probe_script(root=real / "snaps", mounts=[str(link)]), 23)
    assert not (real / "snaps").exists()


def test_mount_inside_root_exits_23(env: Env) -> None:
    root = env.tmp / "snaproot"
    mount = root / "work"
    _assert_exit(env, env.probe_script(root=root, mounts=[str(mount)]), 23)
    assert not root.exists()


def test_unresolvable_mount_exits_23(env: Env) -> None:
    afile = env.tmp / "afile"
    afile.write_text("x")
    dangling = env.tmp / "dangling"
    dangling.symlink_to(env.tmp / "nowhere")
    for bad in (str(afile / "sub"), str(dangling), str(env.tmp / "a" / ".." / "b")):
        proc = _assert_exit(env, env.probe_script(mounts=[bad]), 23)
        assert "cannot be resolved" in proc.stderr
    assert not env.root.exists()


def test_unrelated_mount_is_accepted_and_sibling_prefix_is_not_overlap(
    env: Env,
) -> None:
    mount = env.tmp / "remote-root2"  # shares a name prefix with the root
    mount.mkdir()
    _assert_exit(env, env.probe_script(mounts=[str(mount)]), 0)
    assert env.root.is_dir()


def test_unusable_root_exits_22(env: Env) -> None:
    afile = env.tmp / "afile"
    afile.write_text("x")
    _assert_exit(env, env.probe_script(root=afile / "snaps"), 22)


def test_root_through_symlink_with_odd_target_name_exits_22(env: Env) -> None:
    odd = env.tmp / "odd dir"
    odd.mkdir()
    link = env.tmp / "to-odd"
    link.symlink_to(odd, target_is_directory=True)
    proc = _assert_exit(env, env.probe_script(root=link / "snaps"), 22)
    assert "characters outside" in proc.stderr


# ------------------------------------------------------------------ git gating


def test_old_git_exits_21_and_creates_nothing(env: Env) -> None:
    old = env.tmp / "oldgit"
    old.mkdir()
    fake = old / "git"
    fake.write_text('#!/bin/sh\necho "git version 2.16.0"\n')
    fake.chmod(0o755)
    shell = LocalShell(f"{old}{os.pathsep}{os.environ['PATH']}")
    proc = shell.run_script(env.probe_script())
    assert proc.returncode == 21
    assert "2.17" in proc.stderr
    assert not env.root.exists()


def test_git_217_is_accepted_and_version_is_reported(env: Env) -> None:
    ok = env.tmp / "okgit"
    ok.mkdir()
    real = shutil.which("git")
    fake = ok / "git"
    fake.write_text(
        '#!/bin/sh\nif [ "$1" = --version ]; then echo "git version 2.17.0 (x)"; exit 0; fi\n'
        f'exec {real} "$@"\n'
    )
    fake.chmod(0o755)
    shell = LocalShell(f"{ok}{os.pathsep}{os.environ['PATH']}")
    proc = shell.run_script(env.probe_script())
    assert proc.returncode == 0, proc.stderr
    assert "GITVER=2.17.0" in proc.stdout


def test_no_git_exits_20_and_creates_nothing(env: Env) -> None:
    empty = env.tmp / "emptybin"
    empty.mkdir()
    shell = LocalShell(str(empty))
    proc = shell.run_script(env.probe_script())
    assert proc.returncode == 20
    assert "git" in proc.stderr
    assert not env.root.exists()


# ------------------------------------------------- private store directories


def test_new_store_dirs_are_mode_700(env: Env) -> None:
    probe = env.store().probe(repo_id="proj-0123456789ab", sha=env.sha)
    repo = Path(probe.repo_dir)
    for d in (env.root, repo, repo / "trees", repo / "leases"):
        assert stat.S_IMODE(d.stat().st_mode) == 0o700, d


def test_group_writable_repo_dir_rejected_by_locked_scripts(env: Env) -> None:
    probe = env.store().probe(repo_id="proj-0123456789ab", sha=env.sha)
    repo = Path(probe.repo_dir)
    repo.chmod(0o770)
    lease = env.new_lease(probe)
    proc = env.shell.run_script(_commit_script(env, probe, lease))
    assert proc.returncode == 32 and str(repo) in proc.stderr, proc.stderr
    assert "chmod go-w" in proc.stderr
    assert not (repo / ".lock").exists()
    assert not (repo / "trees" / env.sha).exists()


def test_group_writable_repo_dir_rejected_by_probe(env: Env) -> None:
    probe = env.store().probe(repo_id="proj-0123456789ab", sha=env.sha)
    repo = Path(probe.repo_dir)
    repo.chmod(0o770)
    proc = env.shell.run_script(env.probe_script())
    assert proc.returncode == 32, proc.stderr
    assert str(repo) in proc.stderr


def test_other_writable_root_rejected_by_probe(env: Env) -> None:
    env.store().probe(repo_id="proj-0123456789ab", sha=env.sha)
    env.root.chmod(0o757)
    proc = env.shell.run_script(env.probe_script())
    assert proc.returncode == 32 and str(env.root) in proc.stderr, proc.stderr


def test_symlinked_trees_dir_rejected(env: Env) -> None:
    probe = env.store().probe(repo_id="proj-0123456789ab", sha=env.sha)
    trees = Path(probe.repo_dir) / "trees"
    elsewhere = env.tmp / "elsewhere"
    elsewhere.mkdir()
    trees.rmdir()
    trees.symlink_to(elsewhere)
    proc = env.shell.run_script(_commit_script(env, probe, None))
    assert proc.returncode == 32 and str(trees) in proc.stderr, proc.stderr
    assert list(elsewhere.iterdir()) == []


def test_symlinked_repo_dir_rejected_by_bind_and_delete(env: Env) -> None:
    probe = env.store().probe(repo_id="proj-0123456789ab", sha=env.sha)
    repo = Path(probe.repo_dir)
    moved = env.tmp / "moved"
    repo.rename(moved)
    repo.symlink_to(moved)
    lease = env.new_lease(probe)
    bind = remote.build_bind_script(
        repo_dir=probe.repo_dir,
        sha=env.sha,
        lease_json=lease.to_json(),
        lease_id=lease.lease_id,
        lock_wait=5,
        **OWNER,
    )
    assert env.shell.run_script(bind).returncode == 32
    delete = remote.build_delete_lease_script(
        repo_dir=probe.repo_dir, lease_id=lease.lease_id, lock_wait=5, **OWNER
    )
    assert env.shell.run_script(delete).returncode == 32
    assert not list((moved / "leases").iterdir())


def test_symlinked_root_final_component_rejected(env: Env) -> None:
    real = env.tmp / "real-root"
    real.mkdir()
    env.root.symlink_to(real, target_is_directory=True)
    proc = env.shell.run_script(env.probe_script())
    assert proc.returncode == 32 and str(env.root) in proc.stderr, proc.stderr
    assert list(real.iterdir()) == []


def test_foreign_owner_is_rejected_by_the_uid_comparison() -> None:
    """``/usr`` is root-owned and 0755: only the owner check can reject it."""
    preamble = remote._lock_preamble(
        client_host="h", client_pid=1, token=TOKEN, lock_wait=1
    )
    proc = REAL_RUN(
        ["/bin/sh", "-s"],
        input=preamble + "check_private_dir /usr\necho ok\n",
        capture_output=True,
        text=True,
        check=False,
    )
    if os.getuid() == 0:
        assert proc.returncode == 0, proc.stderr
    else:
        assert proc.returncode == 32, (proc.returncode, proc.stderr)
        assert "owned by uid 0" in proc.stderr and "/usr" in proc.stderr
