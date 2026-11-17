"""Tests for srunx.snapshot.service using a recording fake store."""

from __future__ import annotations

import re
from collections.abc import Iterator
from pathlib import Path

import pytest
from loguru import logger

from srunx.common.exceptions import SbatchIndeterminateError, SbatchRejectedError
from srunx.snapshot.errors import SnapshotRemoteError, SnapshotValidationError
from srunx.snapshot.job_states import SlurmExecute
from srunx.snapshot.lease import Lease
from srunx.snapshot.local_git import SnapshotPlan
from srunx.snapshot.remote import CommitResult, ProbeResult
from srunx.snapshot.service import PreparedSnapshot, SnapshotService
from srunx.ssh.core.config import MountConfig, ServerProfile

SHA = "a" * 40
REPO_DIR = "/r/snap/proj-0123456789ab"
TREE = f"{REPO_DIR}/trees/{SHA}"


class FakeStore:
    def __init__(self, *, has_commit: bool = False) -> None:
        self.calls: list[str] = []
        self.has_commit = has_commit
        self.commit_lease: Lease | None = None
        self.bind_result: bool | Exception = True
        self.delete_error: Exception | None = None
        self.bound_lease: Lease | None = None
        self.push_error: Exception | None = None
        self.commit_error: Exception | None = None

    def probe(self, *, repo_id: str, sha: str) -> ProbeResult:
        self.calls.append("probe")
        return ProbeResult(
            root="/r/snap",
            repo_dir=REPO_DIR,
            git="/usr/bin/git",
            git_version="2.40.0",
            has_commit=self.has_commit,
        )

    def push(self, *, repo_top: Path, probe: ProbeResult, sha: str) -> None:
        self.calls.append("push")
        if self.push_error:
            raise self.push_error

    def commit(
        self, *, probe: ProbeResult, sha: str, lease: Lease | None
    ) -> CommitResult:
        self.calls.append("commit")
        if self.commit_error:
            raise self.commit_error
        self.commit_lease = lease
        lease_path = f"{REPO_DIR}/leases/{lease.lease_id}.json" if lease else None
        return CommitResult(path=TREE, status="created", lease_path=lease_path)

    def bind(self, *, repo_dir: str, lease: Lease) -> bool:
        self.calls.append("bind")
        self.bound_lease = lease
        if isinstance(self.bind_result, Exception):
            raise self.bind_result
        return self.bind_result

    def delete_lease(self, *, repo_dir: str, lease_id: str) -> None:
        self.calls.append("delete_lease")
        if self.delete_error:
            raise self.delete_error


def make_plan(
    *, script_rel: str | None = "jobs/train.sbatch", cwd_rel: str | None = None
) -> SnapshotPlan:
    return SnapshotPlan(
        repo_top=Path("/local/top"),
        repo_id="proj-0123456789ab",
        sha=SHA,
        script_rel=script_rel,
        script_text="#!/bin/bash\n",
        cwd_rel=cwd_rel,
        warnings=(),
    )


def make_service(
    store: FakeStore,
    *,
    slurm_execute: SlurmExecute | None = None,
    snapshot_root: str | None = None,
    mounts: list[MountConfig] | None = None,
) -> SnapshotService:
    profile = ServerProfile(
        hostname="h",
        username="u",
        key_filename="/k",
        snapshot_root=snapshot_root,
        mounts=mounts or [],
    )
    return SnapshotService(
        profile_name="prof",
        profile=profile,
        store=store,
        slurm_execute=slurm_execute,
    )


@pytest.fixture
def logs() -> Iterator[list[tuple[str, str]]]:
    records: list[tuple[str, str]] = []
    hid = logger.add(
        lambda m: records.append((m.record["level"].name, m.record["message"])),
        level="DEBUG",
    )
    yield records
    logger.remove(hid)


def messages(logs: list[tuple[str, str]], level: str) -> list[str]:
    return [m for lv, m in logs if lv == level]


class TestCreate:
    def test_create_passes_no_lease_and_pushes_when_missing(self) -> None:
        store = FakeStore()
        prepared = make_service(store).create(make_plan())
        assert store.calls == ["probe", "push", "commit"]
        assert store.commit_lease is None
        assert prepared.path == TREE and prepared.status == "created"
        assert prepared.root == "/r/snap" and prepared.repo_dir == REPO_DIR

    def test_push_skipped_when_remote_has_commit(self) -> None:
        store = FakeStore(has_commit=True)
        make_service(store).create(make_plan())
        assert store.calls == ["probe", "commit"]

    def test_literal_conflict_raises_before_any_store_call(self) -> None:
        store = FakeStore()
        svc = make_service(
            store,
            snapshot_root="/data/work/snap",
            mounts=[MountConfig(name="m", local="/l", remote="/data/work")],
        )
        with pytest.raises(SnapshotValidationError, match="inside the mount"):
            svc.create(make_plan())
        with pytest.raises(SnapshotValidationError):
            with svc.leased_snapshot(make_plan()):
                pass  # pragma: no cover
        assert store.calls == []


class TestPrepared:
    def test_paths_and_env(self) -> None:
        prep = PreparedSnapshot(
            plan=make_plan(cwd_rel="jobs"),
            root="/r",
            repo_dir=REPO_DIR,
            path=TREE,
            status="reused",
        )
        assert prep.script_path == f"{TREE}/jobs/train.sbatch"
        assert prep.submit_cwd == f"{TREE}/jobs"
        assert prep.env_vars() == {"SRUNX_CODE_DIR": TREE, "SRUNX_SNAPSHOT_SHA": SHA}

    def test_top_cwd_and_no_script(self) -> None:
        prep = PreparedSnapshot(
            plan=make_plan(script_rel=None, cwd_rel=None),
            root="/r",
            repo_dir=REPO_DIR,
            path=TREE,
            status="reused",
        )
        assert prep.script_path is None
        assert prep.submit_cwd == TREE


class TestLeasedSnapshot:
    def test_order_probe_push_commit_with_provisional_lease_then_bind(self) -> None:
        store = FakeStore()
        with make_service(store).leased_snapshot(make_plan()) as handle:
            assert store.calls == ["probe", "push", "commit"]
            assert store.commit_lease is not None
            assert store.commit_lease.state == "provisional"
            assert store.commit_lease.snapshot_path == TREE
            assert handle.lease_path == (
                f"{REPO_DIR}/leases/{store.commit_lease.lease_id}.json"
            )
            handle.bind(4242)
        assert store.calls == ["probe", "push", "commit", "bind"]
        assert store.bound_lease is not None
        assert store.bound_lease.state == "bound" and store.bound_lease.job_id == 4242
        assert store.bound_lease.lease_id == store.commit_lease.lease_id
        assert "delete_lease" not in store.calls

    def test_metadata(self) -> None:
        store = FakeStore()
        with make_service(store).leased_snapshot(make_plan()) as handle:
            meta = handle.metadata()
            handle.bind(1)
        assert meta == {
            "snapshot": {
                "repo_id": "proj-0123456789ab",
                "sha": SHA,
                "path": TREE,
                "lease_id": handle.lease.lease_id,
            }
        }

    def test_rejected_deletes_lease_and_reraises(
        self, logs: list[tuple[str, str]]
    ) -> None:
        store = FakeStore()
        with pytest.raises(SbatchRejectedError):
            with make_service(store).leased_snapshot(make_plan()):
                raise SbatchRejectedError("bad partition")
        assert store.calls[-1] == "delete_lease"
        assert not messages(logs, "WARNING")

    def test_rejected_with_failed_delete_warns_and_still_raises_rejection(
        self, logs: list[tuple[str, str]]
    ) -> None:
        store = FakeStore()
        store.delete_error = SnapshotRemoteError("lock timeout")
        with pytest.raises(SbatchRejectedError):
            with make_service(store).leased_snapshot(make_plan()):
                raise SbatchRejectedError("bad partition")
        warned = messages(logs, "WARNING")
        assert len(warned) == 1 and "/leases/" in warned[0]

    @pytest.mark.parametrize(
        "exc",
        [
            SbatchIndeterminateError("connection lost"),
            RuntimeError("boom"),
            KeyboardInterrupt(),
        ],
        ids=["indeterminate", "generic", "keyboard-interrupt"],
    )
    def test_other_exceptions_keep_lease_and_warn_with_path(
        self, exc: BaseException, logs: list[tuple[str, str]]
    ) -> None:
        store = FakeStore()
        with pytest.raises(type(exc)):
            with make_service(store).leased_snapshot(make_plan()) as handle:
                raise exc
        assert "delete_lease" not in store.calls
        warned = messages(logs, "WARNING")
        assert len(warned) == 1 and handle.lease_path in warned[0]

    def test_exit_without_bind_keeps_lease_and_warns(
        self, logs: list[tuple[str, str]]
    ) -> None:
        store = FakeStore()
        with make_service(store).leased_snapshot(make_plan()) as handle:
            pass
        assert store.calls == ["probe", "push", "commit"]
        warned = messages(logs, "WARNING")
        assert len(warned) == 1 and handle.lease_path in warned[0]

    def test_bind_failure_keeps_provisional_and_does_not_raise(
        self, logs: list[tuple[str, str]]
    ) -> None:
        store = FakeStore()
        store.bind_result = SnapshotRemoteError("lock timeout")
        with make_service(store).leased_snapshot(make_plan()) as handle:
            handle.bind(9)  # must not raise
        assert handle.lease.state == "provisional"
        assert not handle.is_bound
        assert "delete_lease" not in store.calls
        warned = messages(logs, "WARNING")
        assert any(handle.lease_path in m and "9" in m for m in warned)

    def test_bind_reporting_missing_marker_logs_error(
        self, logs: list[tuple[str, str]]
    ) -> None:
        store = FakeStore()
        store.bind_result = False
        with make_service(store).leased_snapshot(make_plan()) as handle:
            handle.bind(11)
        assert handle.lease.state == "bound" and handle.lease.job_id == 11
        errors = messages(logs, "ERROR")
        assert len(errors) == 1 and TREE in errors[0] and "11" in errors[0]
        assert not messages(logs, "WARNING")

    def test_body_exception_after_successful_bind_does_not_warn_or_delete(
        self, logs: list[tuple[str, str]]
    ) -> None:
        store = FakeStore()
        with pytest.raises(RuntimeError):
            with make_service(store).leased_snapshot(make_plan()) as handle:
                handle.bind(5)
                raise RuntimeError("later failure")
        assert "delete_lease" not in store.calls
        assert not messages(logs, "WARNING")

    def test_remote_failure_before_yield_propagates_without_lease_calls(self) -> None:
        class Failing(FakeStore):
            def commit(
                self, *, probe: ProbeResult, sha: str, lease: Lease | None
            ) -> CommitResult:
                raise SnapshotRemoteError("worktree add failed")

        store = Failing()
        with pytest.raises(SnapshotRemoteError):
            with make_service(store).leased_snapshot(make_plan()):
                pass  # pragma: no cover
        assert "delete_lease" not in store.calls and "bind" not in store.calls


def _sacct_runner(
    rows: str, rc: int = 0, calls: list[str] | None = None
) -> SlurmExecute:
    def run(cmd: str) -> tuple[str, str, int]:
        if calls is not None:
            calls.append(cmd)
        marker = re.search(r"__SRUNX_([0-9a-f]+)_CLUSTERQ__", cmd)
        assert marker
        n = marker.group(1)
        # the controller (squeue) answers; accounting would answer the same
        return (
            f"__SRUNX_{n}_CLUSTERQ__\n{rows}\nRC {rc}\n"
            f"__SRUNX_{n}_CLUSTERA__\n{rows}\nRC {rc}\n",
            "",
            0,
        )

    return run


class TestBindCluster:
    def _bind(self, store: FakeStore, execute: SlurmExecute | None) -> None:
        with make_service(store, slurm_execute=execute).leased_snapshot(
            make_plan()
        ) as handle:
            handle.bind(9)

    def test_cluster_is_stored_when_found(self) -> None:
        store = FakeStore()
        calls: list[str] = []
        self._bind(store, _sacct_runner("clusterA\n", calls=calls))
        assert store.bound_lease is not None
        assert store.bound_lease.cluster == "clusterA"
        assert len(calls) == 1

    def test_cluster_none_when_lookup_returns_nothing(self) -> None:
        store = FakeStore()
        self._bind(store, _sacct_runner(""))
        assert store.bound_lease is not None
        assert store.bound_lease.state == "bound"
        assert store.bound_lease.cluster is None

    def test_cluster_none_when_lookup_raises_and_bind_still_happens(self) -> None:
        def boom(cmd: str) -> tuple[str, str, int]:
            raise RuntimeError("ssh down")

        store = FakeStore()
        self._bind(store, boom)
        assert store.bound_lease is not None
        assert store.bound_lease.job_id == 9 and store.bound_lease.cluster is None

    def test_cluster_none_without_a_runner(self) -> None:
        store = FakeStore()
        self._bind(store, None)
        assert store.bound_lease is not None
        assert store.bound_lease.cluster is None

    def test_cluster_none_on_timeout_exit(self) -> None:
        store = FakeStore()
        self._bind(store, _sacct_runner("clusterA\n", rc=124))
        assert store.bound_lease is not None
        assert store.bound_lease.cluster is None


def test_push_failure_is_tolerated_when_commit_step_finds_the_commit() -> None:
    store = FakeStore()
    store.push_error = SnapshotRemoteError("git push failed: reference already exists")
    prepared = make_service(store).create(make_plan())
    assert store.calls == ["probe", "push", "commit"]
    assert prepared.path == TREE


def test_push_failure_is_raised_when_the_commit_is_still_missing() -> None:
    store = FakeStore()
    store.push_error = SnapshotRemoteError("git push failed (exit 1): no route")
    store.commit_error = SnapshotRemoteError("the commit is missing (exit 26)")
    with pytest.raises(SnapshotRemoteError, match="git push failed .*no route"):
        make_service(store).create(make_plan())
    assert store.calls == ["probe", "push", "commit"]


def test_commit_failure_without_push_failure_is_unchanged() -> None:
    store = FakeStore(has_commit=True)
    store.commit_error = SnapshotRemoteError("lock timeout")
    with pytest.raises(SnapshotRemoteError, match="lock timeout"):
        make_service(store).create(make_plan())
