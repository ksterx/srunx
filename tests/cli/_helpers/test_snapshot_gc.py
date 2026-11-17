"""Tests for srunx.cli._helpers.snapshot_gc."""

from __future__ import annotations

import io
from collections.abc import Sequence
from typing import Any
from unittest.mock import MagicMock

from rich.console import Console

from srunx.cli._helpers.snapshot_gc import (
    execute_plan,
    list_as_json,
    render_list_table,
    render_plan,
    slurm_runner,
)
from srunx.snapshot.errors import SnapshotLockTimeoutError
from srunx.snapshot.inventory import PruneRepoResult, PruneTarget
from srunx.snapshot.prune import decide
from tests.snapshot.test_prune import (
    NOW,
    POLICY,
    REPO,
    REPO_DIR,
    SHA,
    SHA2,
    active,
    inv,
    lrec,
    make_lease,
    repo,
    report,
    row,
    snap,
    terminal,
)


def console() -> tuple[Console, io.StringIO]:
    buf = io.StringIO()
    return Console(file=buf, width=200, force_terminal=False), buf


def sample():
    lease = make_lease(job_id=100)
    s = snap(SHA, lease_ids=(lease.lease_id,))
    return inv(repo([s], [lrec(lease)])), lease


def test_slurm_runner_returns_the_clients_runner_or_none() -> None:
    rt = MagicMock()
    assert slurm_runner(rt) is rt.job_ops.run_slurm_command
    rt.job_ops = MagicMock(spec=[])
    assert slurm_runner(rt) is None


def test_list_table_shows_state_leases_job_state_and_referencing_jobs() -> None:
    inventory, _ = sample()
    q = (row(55, workdir=f"{REPO_DIR}/trees/{SHA}"),)
    con, buf = console()
    render_list_table(con, inventory, report(active(100, "RUNNING"), queue=q))
    text = buf.getvalue()
    assert SHA[:12] in text and "complete" in text
    assert "job 100 RUNNING" in text and "clusterA" in text
    assert "55" in text and "10d ago" in text


def test_list_table_unknown_job_and_unreadable_lease_and_skipped_repo() -> None:
    lease = make_lease(job_id=7)
    r1 = repo([snap(SHA, lease_ids=(lease.lease_id,))], [lrec(lease), lrec(None, n=9)])
    r2 = repo([], skipped="leases is group-writable", repo_id="other-0123456789ab")
    con, buf = console()
    render_list_table(con, inv(r1, r2), report())
    text = buf.getvalue()
    assert "job 7 unknown" in text
    assert "unreadable lease" in text
    assert "skipped: leases is group-writable" in text


def test_list_table_missing_root_and_empty_root() -> None:
    from srunx.snapshot.inventory import Inventory

    con, buf = console()
    render_list_table(con, Inventory("/r", False, 1, None, ()), report())
    assert "No snapshot root yet" in buf.getvalue()
    con, buf = console()
    render_list_table(con, Inventory("/r", True, 1, None, ()), report())
    assert "No snapshots" in buf.getvalue()


def test_list_table_shows_lock_and_size_column() -> None:
    from dataclasses import replace

    inventory, _ = sample()
    r = replace(inventory.repos[0], locked_since=NOW - 120)
    s = replace(r.snapshots[0], size_kb=42)
    inventory = replace(inventory, repos=(replace(r, snapshots=(s,)),))
    con, buf = console()
    render_list_table(con, inventory, report(), show_size=True)
    text = buf.getvalue()
    assert "locked since 2m ago" in text and "42" in text


def test_list_json_has_every_field() -> None:
    inventory, lease = sample()
    q = (row(55, command=f"{REPO_DIR}/trees/{SHA}/x"),)
    data = list_as_json(inventory, report(terminal(100), queue=q))
    assert data["root"] == "/r/snap" and data["exists"] is True
    assert data["scheduler_error"] is None
    (r,) = data["repos"]
    assert r["repo_id"] == REPO and r["skipped"] is None
    (s,) = r["snapshots"]
    assert s["sha"] == SHA and s["state"] == "complete"
    assert s["referenced_by"] == [55]
    (le,) = s["leases"]
    assert le["lease_id"] == lease.lease_id and le["cluster"] == "clusterA"
    assert le["job"]["kind"] == "terminal" and le["job"]["state"] == "COMPLETED"
    assert {"size_kb", "last_requested", "marker_mtime", "tree_mtime"} <= set(s)


def test_list_json_reports_scheduler_error_and_unreadable_leases() -> None:
    r = repo([snap(SHA)], [lrec(None, n=3)])
    data = list_as_json(inv(r), report(error="squeue failed"))
    assert data["scheduler_error"] == "squeue failed"
    assert data["repos"][0]["unreadable_leases"][0]["error"]


def test_render_plan_lists_keep_remove_skip_and_trash() -> None:
    kept = snap(SHA2, marker_age=3600)
    removed = snap(SHA)
    skipped = repo([], skipped="leases is a symlink", repo_id="skip-0123456789ab")
    trashy = repo([], trash=(f".trash.{SHA}.{'1' * 32}",), repo_id="trash-0123456789ab")
    plan = decide(inv(repo([removed, kept]), skipped, trashy), report(), POLICY)
    con, buf = console()
    render_plan(con, plan, POLICY)
    text = buf.getvalue()
    assert f"remove {REPO}/{SHA[:12]}" in text
    assert f"keep   {REPO}/{SHA2[:12]}" in text
    assert "no leases, last requested" in text
    assert "skip" in text and "leases is a symlink" in text
    assert "leftover trash" in text and "trash-0123456789ab" in text
    assert "1 snapshot(s) to remove, 1 kept" in text


class FakeStore:
    def __init__(self, results: dict[str, Any]) -> None:
        self.results = results
        self.calls: list[tuple[str, str, list[PruneTarget]]] = []

    def prune_repo(
        self, *, repo_dir: str, git: str, targets: Sequence[PruneTarget]
    ) -> PruneRepoResult:
        self.calls.append((repo_dir, git, list(targets)))
        res = self.results[repo_dir]
        if isinstance(res, Exception):
            raise res
        return res


def two_repo_plan():
    other = "other-0123456789ab"
    inventory = inv(repo([snap(SHA)]), repo([snap(SHA)], repo_id=other))
    return inventory, decide(inventory, report(), POLICY), f"/r/snap/{other}"


def result(**kw: Any) -> PruneRepoResult:
    base: dict[str, Any] = {
        "removed": (),
        "changed": (),
        "failed": (),
        "trash_left": 0,
        "trash_skipped": (),
    }
    return PruneRepoResult(**(base | kw))


def test_execute_plan_continues_past_a_failing_repo_and_returns_1() -> None:
    inventory, plan, other_dir = two_repo_plan()
    store = FakeStore(
        {
            REPO_DIR: SnapshotLockTimeoutError("lock busy"),
            other_dir: result(removed=(SHA,)),
        }
    )
    err, err_buf = console()
    out, out_buf = console()
    rc = execute_plan(store, inventory, plan, err, out)
    assert rc == 1
    assert [c[0] for c in store.calls] == sorted([REPO_DIR, other_dir])
    assert "lock busy" in err_buf.getvalue()
    assert f"removed other-0123456789ab/{SHA[:12]}" in out_buf.getvalue()


def test_execute_plan_changed_is_kept_and_not_an_error() -> None:
    inventory, plan, other_dir = two_repo_plan()
    store = FakeStore(
        {REPO_DIR: result(changed=(SHA,)), other_dir: result(removed=(SHA,))}
    )
    err, _ = console()
    out, out_buf = console()
    assert execute_plan(store, inventory, plan, err, out) == 0
    assert "changed since the inventory" in out_buf.getvalue()


def test_execute_plan_failed_sha_returns_1() -> None:
    inventory, plan, other_dir = two_repo_plan()
    store = FakeStore(
        {
            REPO_DIR: result(failed=((SHA, "not-owned"),)),
            other_dir: result(removed=(SHA,)),
        }
    )
    err, err_buf = console()
    out, _ = console()
    assert execute_plan(store, inventory, plan, err, out) == 1
    assert "not-owned" in err_buf.getvalue()


def test_execute_plan_trash_skip_is_a_warning_only() -> None:
    inventory, plan, other_dir = two_repo_plan()
    store = FakeStore(
        {
            REPO_DIR: result(
                removed=(SHA,), trash_skipped=(".trash.foo",), trash_left=1
            ),
            other_dir: result(removed=(SHA,)),
        }
    )
    err, err_buf = console()
    out, _ = console()
    assert execute_plan(store, inventory, plan, err, out) == 0
    text = err_buf.getvalue()
    assert ".trash.foo" in text and "could not be deleted" in text


def test_execute_plan_without_a_git_path_fails_closed() -> None:
    from dataclasses import replace

    inventory, plan, _ = two_repo_plan()
    store = FakeStore({})
    err, err_buf = console()
    out, _ = console()
    rc = execute_plan(store, replace(inventory, git=None), plan, err, out)
    assert rc == 1 and store.calls == []
    assert "git path" in err_buf.getvalue()


def test_execute_plan_trash_only_repo_is_run_with_no_targets() -> None:
    trashy = repo([], trash=(f".trash.{SHA}.{'1' * 32}",))
    inventory = inv(trashy)
    plan = decide(inventory, report(), POLICY)
    store = FakeStore({REPO_DIR: result()})
    err, _ = console()
    out, _ = console()
    assert execute_plan(store, inventory, plan, err, out) == 0
    assert store.calls == [(REPO_DIR, "/usr/bin/git", [])]


def test_execute_plan_unfinished_trash_sweep_warns_and_returns_1_but_lists_removals() -> (
    None
):
    inventory, plan, other_dir = two_repo_plan()
    store = FakeStore(
        {
            REPO_DIR: result(removed=(SHA,), sweep_incomplete=True),
            other_dir: result(removed=(SHA,)),
        }
    )
    err, err_buf = console()
    out, out_buf = console()
    assert execute_plan(store, inventory, plan, err, out) == 1
    assert out_buf.getvalue().count("removed ") == 2
    assert "trash cleanup did not finish" in err_buf.getvalue()
