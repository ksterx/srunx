"""Tests for ``srunx snapshot create``."""

from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest
from typer.testing import CliRunner

from srunx.cli.main import app
from srunx.snapshot import PreparedSnapshot, SnapshotPlan, SnapshotRemoteError
from srunx.snapshot.errors import SnapshotValidationError

SHA = "c" * 40
PATH = f"/r/repo-123456789abc/trees/{SHA}"


def _plan() -> SnapshotPlan:
    return SnapshotPlan(
        repo_top=Path("/repo"),
        repo_id="repo-123456789abc",
        sha=SHA,
        script_rel=None,
        script_text=None,
        cwd_rel=None,
        warnings=("1 untracked file(s) are not part of the snapshot",),
    )


class _Service:
    created: list[Any] = []
    error: Exception | None = None
    leased_called = False

    def __init__(self, **kw: Any) -> None:
        self.kw = kw

    def create(self, plan: SnapshotPlan) -> PreparedSnapshot:
        if _Service.error is not None:
            raise _Service.error
        _Service.created.append(plan)
        return PreparedSnapshot(
            plan=plan,
            root="/r",
            repo_dir="/r/repo-123456789abc",
            path=PATH,
            status="created",
        )

    def leased_snapshot(self, plan: SnapshotPlan) -> None:  # pragma: no cover
        _Service.leased_called = True
        raise AssertionError("snapshot create must not take a lease")


@pytest.fixture
def env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path / "xdg"))
    monkeypatch.delenv("SRUNX_CLUSTER", raising=False)
    _Service.created = []
    _Service.error = None
    _Service.leased_called = False
    state: dict[str, Any] = {"plan_kw": None, "plan_error": None, "transport": "ssh"}

    def fake_plan(**kw: Any) -> SnapshotPlan:
        state["plan_kw"] = kw
        if state["plan_error"] is not None:
            raise state["plan_error"]
        return _plan()

    @contextmanager
    def fake_rt(**_kw: Any):
        rt = MagicMock()
        rt.transport_type = state["transport"]
        rt.profile_name = "p" if state["transport"] == "ssh" else None
        yield rt

    monkeypatch.setattr("srunx.cli.commands.snapshot.plan_snapshot", fake_plan)
    monkeypatch.setattr("srunx.cli.commands.snapshot.resolve_transport", fake_rt)
    monkeypatch.setattr("srunx.cli.commands.snapshot.SnapshotService", _Service)
    cfg = MagicMock()
    cfg.return_value.get_profile.return_value = MagicMock()
    monkeypatch.setattr("srunx.ssh.core.config.ConfigManager", cfg)
    return state


def test_snapshot_create_stdout_is_exactly_the_path(env: dict[str, Any]) -> None:
    result = CliRunner().invoke(app, ["snapshot", "create", "v1", "--cluster", "p"])
    assert result.exit_code == 0, result.output
    assert result.stdout == PATH + "\n"
    assert result.stderr.count("Snapshot ") == 1  # status line once, not also logged
    assert "untracked" in result.stderr
    assert env["plan_kw"]["rev"] == "v1"
    assert env["plan_kw"]["script"] is None
    assert len(_Service.created) == 1


def test_snapshot_create_writes_no_lease(env: dict[str, Any]) -> None:
    result = CliRunner().invoke(app, ["snapshot", "create", "--cluster", "p"])
    assert result.exit_code == 0
    assert _Service.leased_called is False
    assert env["plan_kw"]["rev"] is None


def test_snapshot_create_rejects_local_transport(env: dict[str, Any]) -> None:
    env["transport"] = "local"
    result = CliRunner().invoke(app, ["snapshot", "create", "--cluster", "local"])
    assert result.exit_code == 2
    assert result.stdout == ""
    assert _Service.created == []


def test_snapshot_create_validation_error_exits_2(env: dict[str, Any]) -> None:
    env["plan_error"] = SnapshotValidationError("tracked changes")
    result = CliRunner().invoke(app, ["snapshot", "create", "--cluster", "p"])
    assert result.exit_code == 2
    assert result.stdout == ""
    assert _Service.created == []


def test_snapshot_create_service_validation_error_exits_2(
    env: dict[str, Any],
) -> None:
    _Service.error = SnapshotValidationError("root inside mount")
    result = CliRunner().invoke(app, ["snapshot", "create", "--cluster", "p"])
    assert result.exit_code == 2
    assert result.stdout == ""


def test_snapshot_create_remote_error_exits_1(env: dict[str, Any]) -> None:
    _Service.error = SnapshotRemoteError("push failed")
    result = CliRunner().invoke(app, ["snapshot", "create", "--cluster", "p"])
    assert result.exit_code == 1
    assert result.stdout == ""
    assert "push failed" in result.stderr


# --------------------------------------------------------- snapshot list / prune

import json  # noqa: E402
from dataclasses import replace  # noqa: E402

import typer  # noqa: E402

from srunx.snapshot.errors import SnapshotLockTimeoutError  # noqa: E402
from srunx.snapshot.inventory import Inventory, PruneRepoResult  # noqa: E402
from srunx.snapshot.remote import literal_root_conflict  # noqa: E402
from tests.snapshot.test_prune import (  # noqa: E402
    DAY,
    REPO,
    REPO_DIR,
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

OSHA = "d" * 40


class _InvStore:
    def __init__(self) -> None:
        self.inventory = inv(repo([snap(OSHA)]))
        self.read_error: Exception | None = None
        self.prune_results: dict[str, Any] = {}
        self.prune_calls: list[tuple[str, list[Any]]] = []
        self.read_kwargs: list[dict[str, Any]] = []

    def read(self, **kw: Any) -> Any:
        self.read_kwargs.append(kw)
        if self.read_error is not None:
            raise self.read_error
        return self.inventory

    def prune_repo(self, *, repo_dir: str, git: str, targets: Any) -> PruneRepoResult:
        self.prune_calls.append((repo_dir, list(targets)))
        res = self.prune_results.get(repo_dir)
        if isinstance(res, Exception):
            raise res
        return res or PruneRepoResult(
            removed=tuple(t.sha for t in targets),
            changed=(),
            failed=(),
            trash_left=0,
            trash_skipped=(),
        )


@pytest.fixture
def gc(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> dict[str, Any]:
    monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path / "xdg"))
    monkeypatch.delenv("SRUNX_CLUSTER", raising=False)
    state: dict[str, Any] = {
        "store": _InvStore(),
        "transport": "ssh",
        "report": report(),
        "query_calls": [],
        "profile": MagicMock(snapshot_root=None, mounts=[]),
        "runner": object(),
    }

    @contextmanager
    def fake_rt(**_kw: Any):
        rt = MagicMock()
        rt.transport_type = state["transport"]
        rt.profile_name = "p" if state["transport"] == "ssh" else None
        rt.job_ops.run_slurm_command = state["runner"]
        yield rt

    def fake_query(job_ids: Any, execute: Any) -> Any:
        state["query_calls"].append((list(job_ids), execute))
        return state["report"]

    def fake_build(profile: Any) -> Any:
        conflict = literal_root_conflict(
            profile.snapshot_root, [m.remote for m in profile.mounts]
        )
        if conflict:
            raise typer.BadParameter(conflict)
        return state["store"]

    monkeypatch.setattr("srunx.cli.commands.snapshot.resolve_transport", fake_rt)
    monkeypatch.setattr("srunx.cli.commands.snapshot.query_job_states", fake_query)
    monkeypatch.setattr(
        "srunx.cli.commands.snapshot._build_inventory_store", fake_build
    )
    cfg = MagicMock()
    cfg.return_value.get_profile.side_effect = lambda name: state["profile"]
    monkeypatch.setattr("srunx.ssh.core.config.ConfigManager", cfg)
    return state


def _run(*args: str, input: str | None = None) -> Any:
    return CliRunner().invoke(app, ["snapshot", *args], input=input)


def _dead_inventory(store: _InvStore) -> None:
    """One lease-less snapshot, 10 days old: removable with the defaults."""
    store.inventory = inv(repo([snap(OSHA, marker_age=10 * DAY)]))


def test_snapshot_list_json_output(gc: dict[str, Any]) -> None:
    lease = make_lease(job_id=100, sha=OSHA)
    gc["store"].inventory = inv(
        repo([snap(OSHA, lease_ids=(lease.lease_id,))], [lrec(lease)])
    )
    gc["report"] = report(
        active(100, "RUNNING"), queue=(row(5, workdir=f"{REPO_DIR}/trees/{OSHA}"),)
    )
    result = _run("list", "--format", "json", "--cluster", "p")
    assert result.exit_code == 0, result.output
    data = json.loads(result.stdout)
    (s,) = data["repos"][0]["snapshots"]
    assert s["sha"] == OSHA and s["referenced_by"] == [5]
    assert s["leases"][0]["job"]["state"] == "RUNNING"
    assert data["repos"][0]["repo_id"] == REPO
    assert gc["query_calls"][0][0] == [100]
    assert gc["query_calls"][0][1] is gc["runner"]


def test_snapshot_list_short_format_flag_and_table_default(gc: dict[str, Any]) -> None:
    short = _run("list", "-f", "json")
    assert short.exit_code == 0 and json.loads(short.stdout)["exists"] is True
    table = _run("list")
    assert table.exit_code == 0
    assert OSHA[:12] in table.stdout and "complete" in table.stdout


def test_snapshot_list_bad_format_exits_2_before_any_remote_call(
    gc: dict[str, Any],
) -> None:
    result = _run("list", "--format", "yaml")
    assert result.exit_code == 2
    assert gc["store"].read_kwargs == []


def test_snapshot_list_and_prune_reject_local_transport(gc: dict[str, Any]) -> None:
    gc["transport"] = "local"
    for cmd in ("list", "prune"):
        result = _run(cmd, "--cluster", "local")
        assert result.exit_code == 2, cmd
        assert result.stdout == ""
    assert gc["store"].read_kwargs == []


def test_snapshot_list_scheduler_failure_warns_and_exits_0(gc: dict[str, Any]) -> None:
    gc["report"] = report(error="squeue failed (exit 1)")
    result = _run("list", "--format", "json")
    assert result.exit_code == 0
    assert "scheduler state unavailable" in result.stderr
    assert json.loads(result.stdout)["scheduler_error"] == "squeue failed (exit 1)"


def test_snapshot_list_inventory_failure_exits_1(gc: dict[str, Any]) -> None:
    gc["store"].read_error = SnapshotRemoteError("inventory is truncated")
    result = _run("list")
    assert result.exit_code == 1 and "truncated" in result.stderr


def test_snapshot_list_missing_root_prints_empty_listing_and_exits_0(
    gc: dict[str, Any],
) -> None:
    gc["store"].inventory = Inventory("/r/snap", False, 1, None, ())
    result = _run("list")
    assert result.exit_code == 0 and "No snapshot root yet" in result.stdout
    assert gc["query_calls"] == []  # nothing to judge, scheduler not asked


def test_snapshot_list_size_flag_is_passed_through(gc: dict[str, Any]) -> None:
    _run("list", "--size", "-f", "json")
    assert gc["store"].read_kwargs == [{"with_size": True}]


def test_snapshot_list_literal_root_conflict_exits_2(gc: dict[str, Any]) -> None:
    gc["profile"] = MagicMock(
        snapshot_root="/data/snaps", mounts=[MagicMock(remote="/data")]
    )
    result = _run("list")
    assert result.exit_code == 2
    assert gc["store"].read_kwargs == []


def test_snapshot_prune_dry_run_prints_plan_and_deletes_nothing(
    gc: dict[str, Any],
) -> None:
    _dead_inventory(gc["store"])
    result = _run("prune", "-n")
    assert result.exit_code == 0, result.output
    assert f"remove {REPO}/{OSHA[:12]}" in result.stdout
    assert "no leases, last requested" in result.stdout
    assert gc["store"].prune_calls == []
    result = _run("prune", "--dry-run")
    assert result.exit_code == 0 and gc["store"].prune_calls == []


def test_snapshot_prune_declined_prompt_deletes_nothing_and_exits_0(
    gc: dict[str, Any],
) -> None:
    _dead_inventory(gc["store"])
    result = _run("prune", input="n\n")
    assert result.exit_code == 0
    assert "Remove 1 snapshot(s)?" in result.stderr
    assert gc["store"].prune_calls == []


def test_snapshot_prune_confirmed_prompt_deletes(gc: dict[str, Any]) -> None:
    _dead_inventory(gc["store"])
    result = _run("prune", input="y\n")
    assert result.exit_code == 0, result.output
    assert len(gc["store"].prune_calls) == 1
    assert f"removed {REPO}/{OSHA[:12]}" in result.stdout


def test_snapshot_prune_closed_stdin_aborts_with_exit_1(gc: dict[str, Any]) -> None:
    _dead_inventory(gc["store"])
    result = _run("prune", input="")
    assert result.exit_code == 1
    assert gc["store"].prune_calls == []


def test_snapshot_prune_force_skips_the_prompt(gc: dict[str, Any]) -> None:
    _dead_inventory(gc["store"])
    result = _run("prune", "-f")
    assert result.exit_code == 0
    assert "Remove" not in result.stderr
    assert len(gc["store"].prune_calls) == 1


def test_snapshot_prune_nothing_to_remove(gc: dict[str, Any]) -> None:
    gc["store"].inventory = inv(repo([snap(OSHA, marker_age=3600)]))
    result = _run("prune", "-f")
    assert result.exit_code == 0 and "Nothing to remove" in result.stdout
    assert gc["store"].prune_calls == []


def test_snapshot_prune_scheduler_failure_deletes_nothing_even_for_lease_less(
    gc: dict[str, Any],
) -> None:
    _dead_inventory(gc["store"])  # only a lease-less snapshot
    gc["report"] = report(error="squeue failed (exit 1)")
    result = _run("prune", "-f")
    assert result.exit_code == 1
    assert "scheduler state unavailable" in result.stdout
    assert "nothing was deleted" in result.stderr
    assert gc["store"].prune_calls == []
    assert gc["query_calls"], "the scheduler is asked even with no leases"


def test_snapshot_prune_scheduler_failure_also_exits_1_with_dry_run(
    gc: dict[str, Any],
) -> None:
    _dead_inventory(gc["store"])
    gc["report"] = report(error="boom")
    assert _run("prune", "-n").exit_code == 1


def test_snapshot_prune_older_than_below_min_job_age_exits_2_and_deletes_nothing(
    gc: dict[str, Any],
) -> None:
    _dead_inventory(gc["store"])
    gc["report"] = replace(report(), min_job_age=7200)
    result = _run("prune", "-f", "--older-than", "2h")
    assert result.exit_code == 2
    assert "MinJobAge" in result.output
    assert gc["store"].prune_calls == []
    ok = _run("prune", "-f", "--older-than", "3h")
    assert ok.exit_code == 0 and len(gc["store"].prune_calls) == 1


def test_snapshot_prune_one_hour_accepted_when_min_job_age_unreadable(
    gc: dict[str, Any],
) -> None:
    _dead_inventory(gc["store"])
    gc["report"] = report()  # min_job_age None
    assert _run("prune", "-n", "--older-than", "1h").exit_code == 0


def test_snapshot_prune_bad_older_than_exits_2_before_any_remote_call(
    gc: dict[str, Any],
) -> None:
    for bad in ("59m", "5m", "0d", "7", "1w", "-1d", "abc"):
        result = _run("prune", "--older-than", bad)
        assert result.exit_code == 2, bad
    assert gc["store"].read_kwargs == [] and gc["query_calls"] == []
    for good in ("7d", "12h", "1h", "90m"):
        assert _run("prune", "-n", "--older-than", good).exit_code == 0, good


def test_snapshot_prune_include_unbound_warns_per_lease_on_stderr(
    gc: dict[str, Any],
) -> None:
    lease = make_lease(state="provisional", sha=OSHA)
    gc["store"].inventory = inv(
        repo(
            [snap(OSHA, lease_ids=(lease.lease_id,), marker_age=10 * DAY)],
            [lrec(lease)],
        )
    )
    without = _run("prune", "-n", "--older-than", "2h")
    assert "provisional lease" in without.stdout and "remove " not in without.stdout
    assert "warning" not in without.stderr
    result = _run("prune", "-n", "--include-unbound", "--older-than", "2h")
    assert result.exit_code == 0
    assert result.stderr.count("warning: removing provisional lease") == 1
    assert f"{lease.lease_id}.json" in result.stderr
    assert f"remove {REPO}/{OSHA[:12]}" in result.stdout


def test_snapshot_prune_one_repo_failing_does_not_stop_the_others_and_exits_1(
    gc: dict[str, Any],
) -> None:
    other = "other-0123456789ab"
    gc["store"].inventory = inv(
        repo([snap(OSHA, marker_age=10 * DAY)]),
        repo([snap(OSHA, marker_age=10 * DAY)], repo_id=other),
    )
    gc["store"].prune_results[REPO_DIR] = SnapshotLockTimeoutError("lock busy")
    result = _run("prune", "-f")
    assert result.exit_code == 1
    assert len(gc["store"].prune_calls) == 2
    assert "lock busy" in result.stderr
    assert f"removed {other}/{OSHA[:12]}" in result.stdout


def test_snapshot_prune_changed_sha_is_kept_and_exit_is_0(gc: dict[str, Any]) -> None:
    _dead_inventory(gc["store"])
    gc["store"].prune_results[REPO_DIR] = PruneRepoResult(
        removed=(), changed=(OSHA,), failed=(), trash_left=0, trash_skipped=()
    )
    result = _run("prune", "-f")
    assert result.exit_code == 0 and "changed since the inventory" in result.stdout


def test_snapshot_prune_failed_sha_exits_1(gc: dict[str, Any]) -> None:
    _dead_inventory(gc["store"])
    gc["store"].prune_results[REPO_DIR] = PruneRepoResult(
        removed=(),
        changed=(),
        failed=((OSHA, "rename"),),
        trash_left=0,
        trash_skipped=(),
    )
    assert _run("prune", "-f").exit_code == 1


def test_snapshot_prune_inventory_failure_exits_1(gc: dict[str, Any]) -> None:
    gc["store"].read_error = SnapshotRemoteError("root is not private")
    result = _run("prune", "-f")
    assert result.exit_code == 1 and "not private" in result.stderr
    assert gc["store"].prune_calls == []


def test_snapshot_prune_trash_only_repo_is_cleaned_but_not_by_dry_run(
    gc: dict[str, Any],
) -> None:
    gc["store"].inventory = inv(repo([], trash=(f".trash.{OSHA}.{'1' * 32}",)))
    assert "leftover trash" in _run("prune", "-n").stdout
    assert gc["store"].prune_calls == []
    result = _run("prune", "-f")
    assert result.exit_code == 0
    assert gc["store"].prune_calls == [(REPO_DIR, [])]


def test_snapshot_prune_referenced_snapshot_is_kept(gc: dict[str, Any]) -> None:
    _dead_inventory(gc["store"])
    gc["report"] = report(
        queue=(row(9, command=f"{REPO_DIR}/trees/{OSHA}/run.sh", state="COMPLETED"),)
    )
    result = _run("prune", "-f")
    assert "referenced by job 9" in result.stdout
    assert gc["store"].prune_calls == []


def test_snapshot_prune_finished_job_removes_snapshot_and_its_lease(
    gc: dict[str, Any],
) -> None:
    lease = make_lease(job_id=100, sha=OSHA)
    gc["store"].inventory = inv(
        repo([snap(OSHA, lease_ids=(lease.lease_id,))], [lrec(lease)])
    )
    gc["report"] = report(terminal(100, ended_ago=5 * DAY))
    result = _run("prune", "-f", "--older-than", "2h")
    assert result.exit_code == 0, result.output
    ((_, targets),) = gc["store"].prune_calls
    assert targets[0].lease_ids == (lease.lease_id,)
    assert gc["query_calls"][0][0] == [100]
