"""Unit tests for srunx.snapshot.remote (script builders; store tests appended below)."""

from __future__ import annotations

import pytest

from srunx.snapshot import remote
from srunx.snapshot._shell import PLACEHOLDER_RE
from srunx.snapshot.remote import (
    build_bind_script,
    build_commit_script,
    build_delete_lease_script,
    build_probe_script,
    literal_root_conflict,
    root_expr,
)

SHA = "b" * 40
TOKEN = "c" * 32
LEASE_ID = "d" * 32
HOST = "host1"
PID = 123


def probe_script(
    *,
    repo_id: str = "repo-0123456789ab",
    sha: str = SHA,
    mounts: tuple[str, ...] = (),
    token: str = TOKEN,
    lock_wait: int = 120,
) -> str:
    return build_probe_script(
        root_expr=root_expr("/r/snap"),
        repo_id=repo_id,
        sha=sha,
        mounts=mounts,
        client_host=HOST,
        client_pid=PID,
        token=token,
        lock_wait=lock_wait,
    )


def commit_script(
    *,
    repo_dir: str = "/r/snap/x",
    git: str = "/usr/bin/git",
    sha: str = SHA,
    lease_json: str | None = None,
    lease_id: str | None = None,
) -> str:
    return build_commit_script(
        repo_dir=repo_dir,
        git=git,
        sha=sha,
        lease_json=lease_json,
        lease_id=lease_id,
        client_host=HOST,
        client_pid=PID,
        token=TOKEN,
    )


def bind_script(
    *,
    repo_dir: str = "/r/x",
    sha: str = SHA,
    lease_json: str = "{}",
    lease_id: str = LEASE_ID,
) -> str:
    return build_bind_script(
        repo_dir=repo_dir,
        sha=sha,
        lease_json=lease_json,
        lease_id=lease_id,
        client_host=HOST,
        client_pid=PID,
        token=TOKEN,
    )


def delete_script(*, repo_dir: str = "/r/x", lease_id: str = LEASE_ID) -> str:
    return build_delete_lease_script(
        repo_dir=repo_dir,
        lease_id=lease_id,
        client_host=HOST,
        client_pid=PID,
        token=TOKEN,
    )


class TestRootExpr:
    def test_default_is_home_relative(self) -> None:
        assert root_expr(None) == '"$HOME"/.local/share/srunx/snapshots'

    def test_home_only(self) -> None:
        assert root_expr("~") == '"$HOME"'

    def test_home_subdir(self) -> None:
        assert root_expr("~/snap/x") == '"$HOME"/snap/x'

    def test_absolute(self) -> None:
        assert root_expr("/scratch/me/snap") == "/scratch/me/snap"

    def test_normalises_slashes(self) -> None:
        assert root_expr("/a//b/./c/") == "/a/b/c"

    @pytest.mark.parametrize(
        "bad",
        [
            "relative/path",
            "/a b",
            "/a'b",
            "/a$HOME",
            "/a;b",
            "/a/../b",
            "~/../x",
            "~user/x",
            "/",
            "/a\nb",
        ],
    )
    def test_rejects(self, bad: str) -> None:
        with pytest.raises(ValueError):
            root_expr(bad)


class TestLiteralConflict:
    def test_root_inside_mount(self) -> None:
        assert literal_root_conflict("/data/proj/snap", ["/data/proj"]) is not None

    def test_mount_inside_root(self) -> None:
        assert literal_root_conflict("/data/snap", ["/data/snap/work"]) is not None

    def test_equal(self) -> None:
        assert literal_root_conflict("/data/snap", ["/data/snap/"]) is not None

    def test_sibling_prefix_is_not_a_conflict(self) -> None:
        assert literal_root_conflict("/data/snap", ["/data/snap2"]) is None

    def test_home_root_vs_absolute_mount_unknown(self) -> None:
        assert literal_root_conflict(None, ["/home/me/proj"]) is None

    def test_home_vs_home(self) -> None:
        assert literal_root_conflict("~/work/snap", ["~/work"]) is not None


class TestBuilders:
    def test_probe_rejects_bad_repo_id_and_sha(self) -> None:
        for bad in ("a b", "a/b", "a;b", "", "../x"):
            with pytest.raises(ValueError):
                probe_script(repo_id=bad)
        with pytest.raises(ValueError):
            probe_script(sha="xyz")
        with pytest.raises(ValueError):
            probe_script(sha="B" * 40)

    def test_probe_rejects_bad_token(self) -> None:
        with pytest.raises(ValueError):
            probe_script(token="not-hex")

    def test_hostile_mount_path_is_quoted(self) -> None:
        script = probe_script(mounts=("/m/$(touch pwned);`id`'x",))
        assert "'/m/$(touch pwned);`id`'\"'\"'x'" in script

    def test_home_mount_uses_home_expr(self) -> None:
        assert '"$HOME"/work' in probe_script(mounts=("~/work",))

    def test_lock_wait_and_traps_embedded(self) -> None:
        script = probe_script(lock_wait=7)
        assert "-ge 7 ]" in script
        for trap in ("trap release EXIT", "exit 129", "exit 130", "exit 143"):
            assert trap in script

    def test_no_stale_lock_breaking(self) -> None:
        for script in (probe_script(), delete_script()):
            assert "stale" not in script.lower()
            assert 'rm -rf "$LOCKDIR"' not in script

    def test_commit_rejects_bad_inputs(self) -> None:
        commit_script()
        with pytest.raises(ValueError):
            commit_script(repo_dir="/r/a b")
        with pytest.raises(ValueError):
            commit_script(repo_dir="relative")
        with pytest.raises(ValueError):
            commit_script(git="git")
        with pytest.raises(ValueError):
            commit_script(sha="abc")
        with pytest.raises(ValueError):
            commit_script(lease_id="zz", lease_json="{}")
        with pytest.raises(ValueError):
            commit_script(lease_json="{}")  # id and json go together

    def test_lease_json_is_quoted(self) -> None:
        script = bind_script(repo_dir="/r/snap/x", lease_json='{"a":"it\'s"}')
        assert 'LEASE_JSON=\'{"a":"it\'"\'"\'s"}\'' in script

    def test_bind_and_delete_reject_bad_ids(self) -> None:
        with pytest.raises(ValueError):
            bind_script(sha="1")
        with pytest.raises(ValueError):
            delete_script(lease_id="../x")

    def test_constants(self) -> None:
        assert remote.MIN_REMOTE_GIT == (2, 17)


# ---------------------------------------------------------------- store tests

import subprocess  # noqa: E402
from pathlib import Path  # noqa: E402
from typing import Any  # noqa: E402

from srunx.snapshot.errors import (  # noqa: E402
    SnapshotLockTimeoutError,
    SnapshotRemoteError,
    SnapshotValidationError,
)
from srunx.snapshot.lease import Lease  # noqa: E402
from srunx.snapshot.remote import (  # noqa: E402
    COMMIT_TIMEOUT,
    INVENTORY_TIMEOUT,
    LEASE_TIMEOUT,
    LOCK_WAIT_SECONDS,
    PROBE_TIMEOUT,
    PRUNE_TIMEOUT,
    PUSH_TIMEOUT,
    ProbeResult,
    RemoteSnapshotStore,
)

REPO_DIR = "/r/snap/repo-0123456789ab"


class FakeShell:
    def __init__(self, *results: subprocess.CompletedProcess[str] | Exception) -> None:
        self.results = list(results)
        self.calls: list[tuple[str, float | None]] = []

    def run_script(
        self, script: str, *, timeout: float | None = None
    ) -> subprocess.CompletedProcess[str]:
        self.calls.append((script, timeout))
        res = self.results.pop(0)
        if isinstance(res, Exception):
            raise res
        return res

    def ssh_command(self) -> list[str]:
        return ["ssh", "-p", "2222", "-o", "ProxyJump=a b"]

    def remote_spec(self, path: str) -> str:
        return f"me@host:{path}"


def done(
    stdout: str = "", rc: int = 0, stderr: str = ""
) -> subprocess.CompletedProcess[str]:
    return subprocess.CompletedProcess([], rc, stdout, stderr)


def make_store(shell: FakeShell, **kw: Any) -> RemoteSnapshotStore:
    return RemoteSnapshotStore(shell, snapshot_root="/r/snap", mounts=["/m"], **kw)


PROBE_OUT = (
    "ROOT=/r/snap\nREPO=/r/snap/repo-0123456789ab\nGIT=/usr/bin/git\n"
    "GITVER=2.39.0\nHAVE=1\n"
)


def probe_result(git: str = "/usr/bin/git") -> ProbeResult:
    return ProbeResult(
        root="/r/snap",
        repo_dir=REPO_DIR,
        git=git,
        git_version="2.39.0",
        has_commit=False,
    )


def lease() -> Lease:
    return Lease.new_provisional(
        repo_id="repo-0123456789ab",
        sha=SHA,
        snapshot_path=f"{REPO_DIR}/trees/{SHA}",
        profile="p",
    )


class TestStoreTimeouts:
    def test_every_timeout_exceeds_lock_wait(self) -> None:
        for t in (PROBE_TIMEOUT, COMMIT_TIMEOUT, LEASE_TIMEOUT):
            assert t > LOCK_WAIT_SECONDS

    def test_timeouts_passed_per_script(self) -> None:
        shell = FakeShell(
            done(PROBE_OUT),
            done(f"STATUS=created\nPATH={REPO_DIR}/trees/{SHA}\nLEASE=\n"),
            done("SNAPSHOT=present\n"),
            done(),
        )
        store = make_store(shell)
        store.probe(repo_id="repo-0123456789ab", sha=SHA)
        store.commit(probe=probe_result(), sha=SHA, lease=None)
        store.bind(repo_dir=REPO_DIR, lease=lease().bound(5))
        store.delete_lease(repo_dir=REPO_DIR, lease_id="e" * 32)
        assert [t for _, t in shell.calls] == [
            PROBE_TIMEOUT,
            COMMIT_TIMEOUT,
            LEASE_TIMEOUT,
            LEASE_TIMEOUT,
        ]

    def test_client_timeout_becomes_remote_error(self) -> None:
        shell = FakeShell(subprocess.TimeoutExpired("ssh", 1))
        with pytest.raises(SnapshotRemoteError, match="timed out"):
            make_store(shell).probe(repo_id="r", sha=SHA)

    def test_bad_root_is_validation_error(self) -> None:
        with pytest.raises(SnapshotValidationError):
            RemoteSnapshotStore(FakeShell(), snapshot_root="/a b", mounts=[])


class TestRunRaw:
    def test_run_raw_returns_stdout_untouched(self) -> None:
        shell = FakeShell(done("A=1\nnoise\nEND\n"))
        out = make_store(shell).run("true", timeout=7, what="x")
        assert out == "A=1\nnoise\nEND\n"
        assert shell.calls == [("true", 7)]

    def test_run_raw_maps_exit_codes_like_run(self) -> None:
        with pytest.raises(SnapshotRemoteError, match="invalid or unusable"):
            make_store(FakeShell(done(rc=22))).run("x", timeout=1, what="inv")
        stderr = "LOCK=/r/.lock\nhost=h\n"
        with pytest.raises(SnapshotLockTimeoutError):
            make_store(FakeShell(done(rc=30, stderr=stderr))).run(
                "x", timeout=1, what="prune"
            )

    def test_run_raw_timeout_becomes_remote_error(self) -> None:
        shell = FakeShell(subprocess.TimeoutExpired("ssh", 1))
        with pytest.raises(SnapshotRemoteError, match="timed out"):
            make_store(shell).run("x", timeout=1, what="inv")

    def test_public_accessors(self) -> None:
        store = make_store(FakeShell(), lock_wait=7)
        assert store.root_expr == "/r/snap"
        assert store.lock_wait == 7
        owner = store.owner()
        assert set(owner) == {"client_host", "client_pid", "token"}
        assert len(owner["token"]) == 32


class TestTimeoutConstants:
    def test_prune_timeout_exceeds_lock_wait(self) -> None:
        assert PRUNE_TIMEOUT > LOCK_WAIT_SECONDS
        assert INVENTORY_TIMEOUT > 0


class TestMarkerTouch:
    def test_marker_touch_exit_33_message(self) -> None:
        shell = FakeShell(done(rc=33, stderr="cannot refresh"))
        with pytest.raises(SnapshotRemoteError, match="last-requested time") as ei:
            make_store(shell).commit(probe=probe_result(), sha=SHA, lease=None)
        assert "exit 33" in str(ei.value)

    def test_marker_touch_in_reuse_branch_only(self) -> None:
        script = commit_script()
        assert 'touch "$MARK"' in script
        assert "exit 33" in script
        assert f"{remote.MARKER_SKEW_SECONDS}" in script
        assert script.index("mtime_of()") < script.index('touch "$MARK"')


class TestStoreResults:
    def test_probe_parses(self) -> None:
        res = make_store(FakeShell(done(PROBE_OUT))).probe(repo_id="r", sha=SHA)
        assert res == ProbeResult(
            root="/r/snap",
            repo_dir=REPO_DIR,
            git="/usr/bin/git",
            git_version="2.39.0",
            has_commit=True,
        )

    def test_probe_ignores_banner_noise_and_requires_keys(self) -> None:
        noisy = "Welcome to host\n" + PROBE_OUT
        assert make_store(FakeShell(done(noisy))).probe(repo_id="r", sha=SHA).has_commit
        with pytest.raises(SnapshotRemoteError, match="unexpected output"):
            make_store(FakeShell(done("ROOT=/r/snap\n"))).probe(repo_id="r", sha=SHA)

    def test_probe_rejects_unsafe_paths(self) -> None:
        bad = PROBE_OUT.replace("REPO=/r/snap/repo-0123456789ab", "REPO=/r/a b")
        with pytest.raises(SnapshotRemoteError, match="unsafe"):
            make_store(FakeShell(done(bad))).probe(repo_id="r", sha=SHA)

    def test_commit_with_lease_returns_lease_path(self) -> None:
        lp = f"{REPO_DIR}/leases/x.json"
        shell = FakeShell(
            done(f"STATUS=recovered\nPATH={REPO_DIR}/trees/{SHA}\nLEASE={lp}\n")
        )
        res = make_store(shell).commit(probe=probe_result(), sha=SHA, lease=lease())
        assert res.status == "recovered" and res.lease_path == lp
        assert "LEASE_JSON=" in shell.calls[0][0]

    def test_commit_without_lease_has_no_lease_code(self) -> None:
        shell = FakeShell(
            done(f"STATUS=created\nPATH={REPO_DIR}/trees/{SHA}\nLEASE=\n")
        )
        res = make_store(shell).commit(probe=probe_result(), sha=SHA, lease=None)
        assert res.lease_path is None
        assert "LEASE_JSON=" not in shell.calls[0][0]
        assert "\nwrite_lease\n" not in shell.calls[0][0]

    def test_commit_rejects_unknown_status(self) -> None:
        shell = FakeShell(done("STATUS=weird\nPATH=/x\n"))
        with pytest.raises(SnapshotRemoteError, match="STATUS"):
            make_store(shell).commit(probe=probe_result(), sha=SHA, lease=None)

    def test_non_absolute_git_rejected_by_commit(self) -> None:
        with pytest.raises(ValueError):
            make_store(FakeShell()).commit(
                probe=probe_result("git"), sha=SHA, lease=None
            )

    def test_bind_reports_marker_state(self) -> None:
        store = make_store(
            FakeShell(done("SNAPSHOT=present\n"), done("SNAPSHOT=missing\n"))
        )
        assert store.bind(repo_dir=REPO_DIR, lease=lease().bound(1)) is True
        assert store.bind(repo_dir=REPO_DIR, lease=lease().bound(1)) is False

    def test_lease_path(self) -> None:
        assert RemoteSnapshotStore.lease_path("/a/b", "x") == "/a/b/leases/x.json"


class TestExitCodes:
    @pytest.mark.parametrize(
        ("code", "fragment"),
        [
            (20, "not found"),
            (21, "2.17"),
            (22, "invalid or unusable"),
            (23, "overlaps a mount"),
            (24, "bare repository"),
            (26, "missing"),
            (27, "worktree add"),
            (28, "chmod"),
            (29, "marker"),
            (31, "lease"),
            (255, "exit 255"),
        ],
    )
    def test_exit_codes(self, code: int, fragment: str) -> None:
        shell = FakeShell(done(rc=code, stderr="detail here"))
        with pytest.raises(SnapshotRemoteError, match=fragment) as ei:
            make_store(shell).probe(repo_id="r", sha=SHA)
        assert "detail here" in str(ei.value)
        assert not isinstance(ei.value, SnapshotLockTimeoutError)

    def test_lock_timeout_message(self) -> None:
        stderr = "LOCK=/r/snap/repo/.lock\nhost=h1\npid=42\nts=1700000000\ntoken=abc\n"
        shell = FakeShell(done(rc=30, stderr=stderr))
        with pytest.raises(SnapshotLockTimeoutError) as ei:
            make_store(shell).commit(probe=probe_result(), sha=SHA, lease=None)
        msg = str(ei.value)
        assert "/r/snap/repo/.lock" in msg
        assert "host=h1" in msg and "pid=42" in msg and "token=abc" in msg
        assert "rm -rf /r/snap/repo/.lock" in msg


class TestPush:
    def _push(self, monkeypatch: pytest.MonkeyPatch, **result: Any) -> dict[str, Any]:
        seen: dict[str, Any] = {}

        def fake_run(cmd: list[str], **kw: Any) -> subprocess.CompletedProcess[str]:
            seen["cmd"] = cmd
            seen["kw"] = kw
            if "exc" in result:
                raise result["exc"]
            return subprocess.CompletedProcess(
                cmd, result.get("rc", 0), "", result.get("err", "")
            )

        monkeypatch.setattr(remote.subprocess, "run", fake_run)
        make_store(FakeShell()).push(
            repo_top=Path("/local/top"),
            probe=probe_result("/opt/my git/bin/git"),
            sha=SHA,
        )
        return seen

    def test_push_argv_and_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        seen = self._push(monkeypatch)
        cmd = seen["cmd"]
        assert cmd[:4] == ["git", "-C", "/local/top", "push"]
        assert "--no-verify" in cmd
        assert "--receive-pack='/opt/my git/bin/git' receive-pack" in cmd
        assert cmd[-2:] == [
            f"me@host:{REPO_DIR}/repo.git",
            f"{SHA}:refs/srunx/snapshots/{SHA}",
        ]
        env = seen["kw"]["env"]
        assert env["GIT_SSH_COMMAND"] == "ssh -p 2222 -o 'ProxyJump=a b'"
        assert env["GIT_TERMINAL_PROMPT"] == "0"
        assert seen["kw"]["timeout"] == PUSH_TIMEOUT

    def test_push_failure_and_timeout(self, monkeypatch: pytest.MonkeyPatch) -> None:
        with pytest.raises(SnapshotRemoteError, match="exit 1.*denied"):
            self._push(monkeypatch, rc=1, err="denied")
        with pytest.raises(SnapshotRemoteError, match="timed out"):
            self._push(monkeypatch, exc=subprocess.TimeoutExpired("git", 1))

    def test_push_requires_absolute_git(self) -> None:
        with pytest.raises(SnapshotRemoteError, match="absolute"):
            make_store(FakeShell()).push(
                repo_top=Path("/t"), probe=probe_result("git"), sha=SHA
            )


def test_fill_rejects_unfilled_placeholder() -> None:
    with pytest.raises(ValueError, match="@MISSING@"):
        remote._fill("echo @GIVEN@ @MISSING@", GIVEN="x")


def test_fill_substitutes_exit_codes_from_the_enum() -> None:
    out = remote._fill("exit @EXIT_LOCK_TIMEOUT@")
    assert out == f"exit {int(remote.RemoteExit.LOCK_TIMEOUT)}"


def test_generated_scripts_have_no_placeholders_left() -> None:
    for script in (
        probe_script(),
        build_commit_script(
            repo_dir="/r/x",
            git="/usr/bin/git",
            sha=SHA,
            lease_json=None,
            lease_id=None,
            client_host=HOST,
            client_pid=PID,
            token=TOKEN,
        ),
    ):
        assert not PLACEHOLDER_RE.search(script)
