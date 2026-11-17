"""Tests for srunx.snapshot.inventory (parsers first; builders and store below)."""

from __future__ import annotations

import json
import subprocess

import pytest

from srunx.snapshot.errors import SnapshotLockTimeoutError, SnapshotRemoteError
from srunx.snapshot.inventory import (
    FINGERPRINT_RE,
    TRASH_NAME_RE,
    InventoryStore,
    LeaseRecord,
    PruneTarget,
    build_inventory_script,
    build_prune_script,
    parse_inventory,
    parse_prune_output,
)
from srunx.snapshot.lease import Lease
from srunx.snapshot.remote import (
    INVENTORY_TIMEOUT,
    PRUNE_TIMEOUT,
    RemoteSnapshotStore,
)

REPO = "proj-0123456789ab"
SHA = "a" * 40
SHA2 = "b" * 40
LID = "c" * 32
LID2 = "d" * 32
ROOT = "/r/snap"


def lease_json(
    *, lid: str = LID, repo: str = REPO, sha: str = SHA, **extra: object
) -> str:
    base = Lease.new_provisional(
        repo_id=repo, sha=sha, snapshot_path=f"{ROOT}/{repo}/trees/{sha}", profile="p"
    ).model_copy(update={"lease_id": lid})
    data = json.loads(base.to_json()) | extra
    return json.dumps(data, separators=(",", ":"))


def fp(m: str = "100", t: str = "90", i: str = "1", leases: str = "") -> str:
    return f"m{m}.t{t}.i{i}.l{leases}"


def listing(
    *records: str, root: str = ROOT, exists: str = "1", now: str = "1000"
) -> str:
    head = [
        "BEGIN",
        f"NOW {now}",
        f"ROOT {root}",
        f"EXISTS {exists}",
        "GIT /usr/bin/git",
    ]
    return "\n".join([*head, *records, "END"]) + "\n"


class TestFingerprint:
    @pytest.mark.parametrize(
        "text",
        [
            "m-.t-.i0.l",
            "m1.t2.i1.l",
            "m1.t2.i1.l!",
            f"m1.t2.i1.l{LID}:5",
            f"m1.t2.i1.l{LID}:-,{LID2}:7",
        ],
    )
    def test_accepts(self, text: str) -> None:
        assert FINGERPRINT_RE.fullmatch(text)

    @pytest.mark.parametrize(
        "text",
        ["m1.t2.i2.l", "m1.t2.i1", "m1.t2.i1.lxyz", f"m1.t2.i1.l{LID}", "m1.t2.i1.l,"],
    )
    def test_rejects(self, text: str) -> None:
        assert not FINGERPRINT_RE.fullmatch(text)

    def test_trash_name(self) -> None:
        assert TRASH_NAME_RE.fullmatch(f".trash.{SHA}.{LID}")
        assert not TRASH_NAME_RE.fullmatch(".trash.foo")
        assert not TRASH_NAME_RE.fullmatch(f".trash.{SHA}.{LID}x")


class TestParseInventory:
    def test_full_listing(self) -> None:
        out = listing(
            f"REPO {REPO}",
            f"LOCK {REPO} 55",
            f"SNAP {REPO} {SHA} {fp(leases=f'{LID}:60')} 12 11",
            f"SNAP {REPO} {SHA2} {fp('-', '-', '0')} - 00",
            f"LEASE {REPO} {LID}.json 60 {lease_json()}",
            f"TRASH {REPO} .trash.{SHA}.{LID}",
            f"STRAY {REPO} odd thing",
            "STRAY - loose",
        )
        inv = parse_inventory("banner\n" + out + "trailing noise\n")
        assert (inv.root, inv.exists, inv.now, inv.git) == (
            ROOT,
            True,
            1000,
            "/usr/bin/git",
        )
        assert inv.stray == ("loose",)
        (repo,) = inv.repos
        assert repo.repo_id == REPO and repo.repo_dir == f"{ROOT}/{REPO}"
        assert repo.skipped is None and repo.locked_since == 55
        a, b = repo.snapshots
        assert (a.sha, a.has_tree, a.has_marker, a.intact) == (SHA, True, True, True)
        assert (a.marker_mtime, a.tree_mtime, a.size_kb) == (100, 90, 12)
        assert a.fp_lease_ids == frozenset({LID})
        assert (b.has_tree, b.has_marker, b.intact) == (False, False, False)
        assert b.marker_mtime is None and b.size_kb is None
        assert b.fp_lease_ids == frozenset()
        (lease,) = repo.leases
        assert lease.error is None and lease.lease is not None
        assert lease.mtime == 60 and lease.filename == f"{LID}.json"
        assert repo.trash == (f".trash.{SHA}.{LID}",)
        assert repo.stray == ("odd thing",)

    def test_empty_lease_list_in_fingerprint_is_an_empty_set(self) -> None:
        out = listing(f"REPO {REPO}", f"SNAP {REPO} {SHA} {fp()} - 11")
        (snap,) = parse_inventory(out).repos[0].snapshots
        assert snap.fp_lease_ids == frozenset()

    def test_bang_means_unattributable(self) -> None:
        out = listing(f"REPO {REPO}", f"SNAP {REPO} {SHA} {fp(leases='!')} - 11")
        (snap,) = parse_inventory(out).repos[0].snapshots
        assert snap.fp_lease_ids is None

    def test_missing_root_listing(self) -> None:
        inv = parse_inventory(f"BEGIN\nNOW 5\nROOT {ROOT}\nEXISTS 0\nEND\n")
        assert inv.exists is False and inv.repos == () and inv.git is None

    def test_clock_dash_is_none(self) -> None:
        assert parse_inventory(listing(now="-")).now is None

    def test_skipped_repo(self) -> None:
        inv = parse_inventory(listing(f"SKIP {REPO} leases is group-writable"))
        (repo,) = inv.repos
        assert repo.skipped == "leases is group-writable"
        assert repo.snapshots == () and repo.leases == ()

    def test_missing_end_is_truncated(self) -> None:
        out = listing(f"REPO {REPO}").replace("END\n", "")
        with pytest.raises(SnapshotRemoteError, match="truncated"):
            parse_inventory(out)

    def test_missing_begin(self) -> None:
        with pytest.raises(SnapshotRemoteError, match="BEGIN"):
            parse_inventory("END\n")

    def test_missing_root_record(self) -> None:
        with pytest.raises(SnapshotRemoteError, match="ROOT"):
            parse_inventory("BEGIN\nNOW 1\nEND\n")

    @pytest.mark.parametrize(
        "record",
        [
            f"SNAP {REPO} {SHA} {fp()} -",  # missing flags
            f"SNAP {REPO} {SHA} m1.t2.i1.lzz - 11",  # bad fingerprint
            f"SNAP {REPO} short {fp()} - 11",
            f"SNAP {REPO} {SHA} {fp()} x 11",
            f"SNAP {REPO} {SHA} {fp()} - 12",
            f"SNAP other-0123456789ab {SHA} {fp()} - 11",  # unannounced repo
            f"LEASE {REPO} {LID}.json 5",
            f"LOCK {REPO}",
            f"TRASH {REPO} .trash.foo",
        ],
    )
    def test_malformed_record_is_an_error(self, record: str) -> None:
        with pytest.raises(SnapshotRemoteError):
            parse_inventory(listing(f"REPO {REPO}", record))

    @pytest.mark.parametrize("bad", ["..", ".", ".hidden", "a b"])
    def test_bad_repo_id_is_an_error(self, bad: str) -> None:
        with pytest.raises(SnapshotRemoteError):
            parse_inventory(listing(f"REPO {bad}"))

    def test_relative_root_is_an_error(self) -> None:
        with pytest.raises(SnapshotRemoteError):
            parse_inventory(listing(root="relative"))

    def test_repo_announced_twice(self) -> None:
        with pytest.raises(SnapshotRemoteError):
            parse_inventory(listing(f"REPO {REPO}", f"REPO {REPO}"))


class TestLeaseRecords:
    def _one(self, record: str) -> LeaseRecord:
        inv = parse_inventory(listing(f"REPO {REPO}", record))
        (rec,) = inv.repos[0].leases
        return rec

    def test_valid_bound_lease(self) -> None:
        text = lease_json(state="bound", job_id=7, cluster="clusterA")
        rec = self._one(f"LEASE {REPO} {LID}.json 60 {text}")
        assert rec.lease is not None and rec.lease.job_id == 7
        assert rec.lease.cluster == "clusterA"

    def test_garbage_is_unreadable(self) -> None:
        rec = self._one(f"LEASE {REPO} {LID}.json 60 not json {{")
        assert rec.lease is None and rec.error

    def test_lease_id_must_match_file_name(self) -> None:
        rec = self._one(f"LEASE {REPO} {LID2}.json 60 {lease_json()}")
        assert rec.lease is None and "lease_id" in (rec.error or "")

    def test_repo_id_must_match_repo(self) -> None:
        text = lease_json(repo="other-0123456789ab")
        rec = self._one(f"LEASE {REPO} {LID}.json 60 {text}")
        assert rec.lease is None and "repo_id" in (rec.error or "")

    def test_unexpected_file_name(self) -> None:
        rec = self._one(f"LEASE {REPO} weird.json 60 {lease_json()}")
        assert rec.lease is None

    def test_leasebad_keeps_the_reason(self) -> None:
        rec = self._one(f"LEASEBAD {REPO} {LID}.json 60 is a symlink")
        assert rec.lease is None and rec.error == "is a symlink"

    def test_unknown_mtime(self) -> None:
        rec = self._one(f"LEASE {REPO} {LID}.json - {lease_json()}")
        assert rec.mtime is None and rec.lease is not None

    def test_sha_that_is_not_a_commit_id_is_unreadable(self) -> None:
        rec = self._one(f"LEASE {REPO} {LID}.json 60 {lease_json(sha='zz')}")
        assert rec.lease is None


class TestParsePruneOutput:
    TARGETS = [
        PruneTarget(SHA, fp(), ()),
        PruneTarget(SHA2, fp(), (LID,)),
    ]

    def test_results(self) -> None:
        out = f"REMOVED {SHA}\nCHANGED {SHA2}\nTRASHLEFT 2\nTRASHSKIP .trash.foo\nEND\n"
        res = parse_prune_output(out, self.TARGETS)
        assert res.removed == (SHA,) and res.changed == (SHA2,)
        assert res.failed == () and res.trash_left == 2
        assert res.trash_skipped == (".trash.foo",)

    def test_failed_with_reason(self) -> None:
        out = f"REMOVED {SHA}\nFAILED {SHA2} not-owned\nEND\n"
        res = parse_prune_output(out, self.TARGETS)
        assert res.failed == ((SHA2, "not-owned"),)

    def test_target_without_result_line_counts_as_failed(self) -> None:
        res = parse_prune_output(f"REMOVED {SHA}\nEND\n", self.TARGETS)
        assert res.failed == ((SHA2, "no result"),)

    def test_missing_end_is_an_error(self) -> None:
        with pytest.raises(SnapshotRemoteError, match="END"):
            parse_prune_output(f"REMOVED {SHA}\n", self.TARGETS)

    def test_complete_run_has_no_sweep_warning(self) -> None:
        out = f"REMOVED {SHA}\nRESULTS_DONE\nTRASHLEFT 0\nEND\n"
        assert parse_prune_output(out, self.TARGETS[:1]).sweep_incomplete is False

    def test_results_marker_without_end_is_accepted_as_an_unfinished_sweep(
        self,
    ) -> None:
        out = f"REMOVED {SHA}\nCHANGED {SHA2}\nRESULTS_DONE\nTRASHSKIP .trash.foo\n"
        res = parse_prune_output(out, self.TARGETS)
        assert res.removed == (SHA,) and res.changed == (SHA2,) and res.failed == ()
        assert res.sweep_incomplete is True
        assert res.trash_skipped == (".trash.foo",)

    def test_truncation_before_the_results_marker_is_still_an_error(self) -> None:
        with pytest.raises(SnapshotRemoteError, match="END"):
            parse_prune_output(f"REMOVED {SHA}\n", self.TARGETS)

    def test_targets_without_result_are_failed_even_when_the_sweep_was_cut(
        self,
    ) -> None:
        res = parse_prune_output(f"REMOVED {SHA}\nRESULTS_DONE\n", self.TARGETS)
        assert res.failed == ((SHA2, "no result"),) and res.sweep_incomplete

    def test_unknown_sha_is_ignored_and_empty_targets_work(self) -> None:
        res = parse_prune_output(f"REMOVED {SHA}\nTRASHLEFT 0\nEND\n", [])
        assert res.removed == () and res.failed == ()


TOKEN = "e" * 32
REPO_DIR = f"{ROOT}/{REPO}"


def prune_script(targets: list[PruneTarget] | None = None) -> str:
    return build_prune_script(
        repo_dir=REPO_DIR,
        git="/usr/bin/git",
        targets=targets or [],
        client_host="h",
        client_pid=1,
        token=TOKEN,
    )


class TestBuilders:
    def test_prune_script_embeds_targets_and_token(self) -> None:
        t = PruneTarget(SHA, fp(leases=f"{LID}:5"), (LID,))
        script = prune_script([t])
        assert f"prune_one {SHA} " in script
        assert f".trash.$po_sha.{TOKEN}" in script

    def test_prune_script_with_no_targets_still_locks_and_sweeps(self) -> None:
        script = prune_script([])
        assert "\nacquire\n" in script and "worktree prune" in script
        assert "TRASHSKIP" in script
        calls = script.split("\nacquire\n", 1)[1].split('"$GIT"')[0]
        assert "prune_one" not in calls

    def test_prune_script_never_runs_gc(self) -> None:
        script = prune_script([PruneTarget(SHA, fp(), ())])
        assert " gc" not in script

    @pytest.mark.parametrize(
        "target",
        [
            PruneTarget("short", fp(), ()),
            PruneTarget(SHA, "m1.t2", ()),
            PruneTarget(SHA, fp(leases="!"), ()),  # cannot be attributed
            PruneTarget(SHA, fp(leases=f"{LID}:5"), ("zz",)),
            PruneTarget(SHA, fp(leases=f"{LID}:5"), ()),  # ids differ from the fp
            PruneTarget(SHA, fp(), (LID,)),
            PruneTarget(SHA + "; true", fp(), ()),
        ],
    )
    def test_prune_script_rejects_bad_targets(self, target: PruneTarget) -> None:
        with pytest.raises(ValueError):
            prune_script([target])

    def test_prune_script_rejects_bad_repo_dir_token_and_git(self) -> None:
        with pytest.raises(ValueError):
            build_prune_script(
                repo_dir="/a b",
                git="/usr/bin/git",
                targets=[],
                client_host="h",
                client_pid=1,
                token=TOKEN,
            )
        with pytest.raises(ValueError):
            build_prune_script(
                repo_dir=REPO_DIR,
                git="git",
                targets=[],
                client_host="h",
                client_pid=1,
                token=TOKEN,
            )
        with pytest.raises(ValueError):
            build_prune_script(
                repo_dir=REPO_DIR,
                git="/usr/bin/git",
                targets=[],
                client_host="h",
                client_pid=1,
                token="nothex",
            )

    def test_inventory_script_takes_no_lock(self) -> None:
        script = build_inventory_script(root_expr="/r/snap")
        assert "ROOT_IN=/r/snap" in script
        assert "\nacquire\n" not in script
        assert "WITH_SIZE=0" in script
        assert "WITH_SIZE=1" in build_inventory_script(root_expr="/r", with_size=True)

    def test_scripts_share_the_fingerprint_function(self) -> None:
        inv = build_inventory_script(root_expr="/r/snap")
        pr = prune_script([])
        start = inv.index("snap_fp() {")
        end = inv.index("\n}\n", start)
        assert inv[start:end] in pr


class StubShell:
    def __init__(self, result: subprocess.CompletedProcess[str]) -> None:
        self.result = result
        self.error: Exception | None = None
        self.calls: list[tuple[str, float | None]] = []

    def run_script(
        self, script: str, *, timeout: float | None = None
    ) -> subprocess.CompletedProcess[str]:
        self.calls.append((script, timeout))
        if self.error is not None:
            raise self.error
        return self.result

    def ssh_command(self) -> list[str]:
        return ["ssh"]

    def remote_spec(self, path: str) -> str:
        return path


def store_for(
    result: subprocess.CompletedProcess[str],
) -> tuple[InventoryStore, StubShell]:
    shell = StubShell(result)
    raw = RemoteSnapshotStore(shell, snapshot_root=ROOT, mounts=[], lock_wait=7)
    return InventoryStore(raw), shell


def done(
    stdout: str = "", rc: int = 0, stderr: str = ""
) -> subprocess.CompletedProcess[str]:
    return subprocess.CompletedProcess([], rc, stdout, stderr)


class TestInventoryStore:
    def test_read_uses_inventory_timeout_and_parses(self) -> None:
        store, shell = store_for(done(listing(f"REPO {REPO}")))
        inv = store.read()
        assert [r.repo_id for r in inv.repos] == [REPO]
        assert shell.calls[0][1] == INVENTORY_TIMEOUT

    def test_read_rejects_truncated_output(self) -> None:
        store, _ = store_for(done("BEGIN\nNOW 1\nROOT /r\nEXISTS 1\n"))
        with pytest.raises(SnapshotRemoteError, match="truncated"):
            store.read()

    def test_read_maps_unsafe_root_exit_code(self) -> None:
        store, _ = store_for(done(rc=32, stderr="group-writable"))
        with pytest.raises(SnapshotRemoteError, match="not private"):
            store.read()

    def test_prune_repo_uses_prune_timeout_and_lock_wait(self) -> None:
        store, shell = store_for(done(f"REMOVED {SHA}\nTRASHLEFT 0\nEND\n"))
        res = store.prune_repo(
            repo_dir=REPO_DIR, git="/usr/bin/git", targets=[PruneTarget(SHA, fp(), ())]
        )
        assert res.removed == (SHA,)
        script, timeout = shell.calls[0]
        assert timeout == PRUNE_TIMEOUT
        assert '"$waited" -ge 7 ]' in script  # lock_wait of the underlying store

    def test_prune_repo_lock_timeout_maps_to_lock_error(self) -> None:
        store, _ = store_for(done(rc=30, stderr=f"LOCK={REPO_DIR}/.lock\nhost=h\n"))
        with pytest.raises(SnapshotLockTimeoutError):
            store.prune_repo(repo_dir=REPO_DIR, git="/usr/bin/git", targets=[])

    def test_prune_timeout_after_the_results_marker_reports_the_removals(self) -> None:
        exc = subprocess.TimeoutExpired(
            "ssh", 5, output=f"REMOVED {SHA}\nRESULTS_DONE\n".encode()
        )
        shell = StubShell(done())
        shell.error = exc
        raw = RemoteSnapshotStore(shell, snapshot_root=ROOT, mounts=[])
        res = InventoryStore(raw).prune_repo(
            repo_dir=REPO_DIR, git="/usr/bin/git", targets=[PruneTarget(SHA, fp(), ())]
        )
        assert res.removed == (SHA,) and res.sweep_incomplete is True

    def test_prune_timeout_before_the_results_marker_is_an_error(self) -> None:
        shell = StubShell(done())
        shell.error = subprocess.TimeoutExpired("ssh", 5, output=b"partial")
        raw = RemoteSnapshotStore(shell, snapshot_root=ROOT, mounts=[])
        with pytest.raises(SnapshotRemoteError, match="timed out"):
            InventoryStore(raw).prune_repo(
                repo_dir=REPO_DIR,
                git="/usr/bin/git",
                targets=[PruneTarget(SHA, fp(), ())],
            )

    def test_prune_repo_without_end_is_an_error(self) -> None:
        store, _ = store_for(done(f"REMOVED {SHA}\n"))
        with pytest.raises(SnapshotRemoteError):
            store.prune_repo(
                repo_dir=REPO_DIR,
                git="/usr/bin/git",
                targets=[PruneTarget(SHA, fp(), ())],
            )

    def test_prune_repo_target_without_result_counts_as_failed(self) -> None:
        store, _ = store_for(done("TRASHLEFT 0\nEND\n"))
        res = store.prune_repo(
            repo_dir=REPO_DIR, git="/usr/bin/git", targets=[PruneTarget(SHA, fp(), ())]
        )
        assert res.failed == ((SHA, "no result"),)
