"""Tests for srunx.snapshot.prune (pure decision logic)."""

from __future__ import annotations

from dataclasses import replace
from typing import Literal

import pytest

from srunx.snapshot.errors import SnapshotValidationError
from srunx.snapshot.inventory import (
    Inventory,
    LeaseRecord,
    RepoInventory,
    SnapshotRecord,
)
from srunx.snapshot.job_states import JobState, JobStateReport, QueueRow
from srunx.snapshot.lease import Lease
from srunx.snapshot.prune import (
    PrunePolicy,
    check_min_job_age,
    decide,
    job_ids_to_query,
    lease_reference_time,
    lease_verifiable,
    needs_scheduler,
    parse_duration,
    referencing_jobs,
    targets_for,
)

REPO = "proj-0123456789ab"
REPO_DIR = f"/r/snap/{REPO}"
SHA = "a" * 40
SHA2 = "b" * 40
NOW = 10_000_000
DAY = 86400
HOUR = 3600
POLICY = PrunePolicy(older_than=2 * HOUR, include_unbound=False)


def lid(n: int) -> str:
    return f"{n:032x}"


def make_lease(
    n: int = 1,
    *,
    sha: str = SHA,
    state: Literal["provisional", "bound"] = "bound",
    job_id: int | None = 100,
    cluster: str | None = "clusterA",
    profile: str = "p",
    created_at: str = "2026-01-01T00:00:00Z",
    bound_at: str | None = "2026-01-01T00:00:05Z",
) -> Lease:
    return Lease(
        lease_id=lid(n),
        repo_id=REPO,
        sha=sha,
        snapshot_path=f"{REPO_DIR}/trees/{sha}",
        state=state,
        job_id=job_id if state == "bound" else None,
        profile=profile,
        scheduler_key=f"ssh:{profile}",
        client_host="h",
        created_at=created_at,
        bound_at=bound_at if state == "bound" else None,
        cluster=cluster,
    )


def lrec(
    lease: Lease | None, *, n: int = 1, mtime: int | None = NOW - 5 * DAY
) -> LeaseRecord:
    if lease is None:
        return LeaseRecord(f"{lid(n)}.json", mtime, None, "not valid lease JSON")
    return LeaseRecord(f"{lease.lease_id}.json", mtime, lease, None)


def snap(
    sha: str = SHA,
    *,
    lease_ids: tuple[str, ...] | None = (),
    marker_age: int | None = 10 * DAY,
    tree: bool = True,
    marker: bool = True,
    intact: bool = True,
) -> SnapshotRecord:
    m = None if marker_age is None or not marker else NOW - marker_age
    return SnapshotRecord(
        sha=sha,
        fingerprint=f"m{m if m is not None else '-'}.t{NOW - 11 * DAY}.i{int(intact)}.l"
        + ",".join(f"{i}:1" for i in sorted(lease_ids or ())),
        has_tree=tree,
        has_marker=marker,
        intact=intact,
        marker_mtime=m,
        tree_mtime=NOW - 11 * DAY if tree else None,
        fp_lease_ids=None if lease_ids is None else frozenset(lease_ids),
        size_kb=None,
    )


def repo(
    snaps: list[SnapshotRecord],
    leases: list[LeaseRecord] | None = None,
    *,
    skipped: str | None = None,
    trash: tuple[str, ...] = (),
    repo_id: str = REPO,
) -> RepoInventory:
    return RepoInventory(
        repo_id=repo_id,
        repo_dir=f"/r/snap/{repo_id}",
        skipped=skipped,
        locked_since=None,
        snapshots=tuple(snaps),
        leases=tuple(leases or ()),
        trash=trash,
        stray=(),
    )


def inv(*repos: RepoInventory, now: int | None = NOW) -> Inventory:
    return Inventory(
        root="/r/snap", exists=True, now=now, git="/usr/bin/git", repos=repos
    )


def terminal(
    job_id: int = 100,
    *,
    ended_ago: int | None = 5 * DAY,
    cluster: str | None = "clusterA",
    submitted_at: int | None = None,
) -> JobState:
    return JobState(
        job_id,
        "terminal",
        "COMPLETED",
        None if ended_ago is None else NOW - ended_ago,
        submitted_at,
        cluster,
        "sacct",
        "",
    )


def active(job_id: int = 100, state: str = "RUNNING") -> JobState:
    return JobState(job_id, "active", state, None, None, None, "squeue", "")


def unknown(job_id: int = 100) -> JobState:
    return JobState(job_id, "unknown", None, None, None, None, None, "")


def report(
    *states: JobState,
    queue: tuple[QueueRow, ...] = (),
    now: int | None = NOW,
    error: str | None = None,
) -> JobStateReport:
    return JobStateReport(
        now=now,
        min_job_age=None,
        queue=queue,
        states={s.job_id: s for s in states},
        error=error,
    )


def row(
    job_id: int, *, workdir: str = "/w", command: str = "run.sh", state: str = "RUNNING"
) -> QueueRow:
    return QueueRow(job_id, job_id, state, workdir, command)


def bound_case(
    *,
    lease: Lease | None = None,
    job: JobState | None = None,
    mtime: int | None = NOW - 5 * DAY,
    policy: PrunePolicy = POLICY,
    jobs: JobStateReport | None = None,
):
    lease = lease or make_lease()
    ids = (lease.lease_id,)
    plan = decide(
        inv(repo([snap(lease_ids=ids)], [lrec(lease, mtime=mtime)])),
        jobs or report(*([job] if job else [])),
        policy,
    )
    (verdict,) = plan.verdicts
    return verdict


# --------------------------------------------------------------- parse_duration


class TestDuration:
    @pytest.mark.parametrize(
        ("text", "seconds"),
        [
            ("7d", 7 * DAY),
            ("12h", 12 * HOUR),
            ("1h", HOUR),
            ("90m", 5400),
            ("60m", HOUR),
        ],
    )
    def test_accepts(self, text: str, seconds: int) -> None:
        assert parse_duration(text) == seconds

    @pytest.mark.parametrize(
        "text", ["59m", "30m", "0d", "7", "1w", "-1d", "abc", "", "1.5h", "01h", " 1h"]
    )
    def test_rejects(self, text: str) -> None:
        with pytest.raises(SnapshotValidationError):
            parse_duration(text)

    def test_one_hour_floor_message(self) -> None:
        with pytest.raises(SnapshotValidationError, match="1h"):
            parse_duration("59m")


class TestMinJobAge:
    def test_below_is_rejected_and_names_both_values(self) -> None:
        with pytest.raises(SnapshotValidationError, match="MinJobAge") as ei:
            check_min_job_age(2 * HOUR, 7200)
        assert "7200" in str(ei.value) and "2h" in str(ei.value)

    def test_exactly_min_plus_margin_is_accepted(self) -> None:
        check_min_job_age(7200 + 600, 7200)

    def test_just_below_the_margin_is_rejected(self) -> None:
        with pytest.raises(SnapshotValidationError):
            check_min_job_age(7200 + 599, 7200)

    def test_above_is_accepted(self) -> None:
        check_min_job_age(3 * HOUR, 7200)

    def test_none_is_a_no_op(self) -> None:
        check_min_job_age(HOUR, None)


# ----------------------------------------------------------- lease reasons


class TestLeaseReasons:
    def test_job_finished_is_dead_and_removes_the_snapshot(self) -> None:
        v = bound_case(job=terminal())
        assert v.remove and v.reason == "jobs-finished"
        assert v.leases[0].reason == "job-finished" and v.leases[0].dead
        assert "all 1 job(s) finished more than 2h ago" in v.message

    def test_job_active_keeps_with_job_id_and_state(self) -> None:
        v = bound_case(job=active(state="PENDING"))
        assert not v.remove and v.reason == "live-lease"
        assert v.leases[0].reason == "job-active"
        assert "100" in v.message and "PENDING" in v.message

    def test_job_in_grace_keeps(self) -> None:
        v = bound_case(job=terminal(ended_ago=HOUR))
        assert not v.remove and v.leases[0].reason == "job-in-grace"
        assert "resume grace" in v.message

    def test_exactly_older_than_is_still_in_grace(self) -> None:
        v = bound_case(job=terminal(ended_ago=2 * HOUR))
        assert v.leases[0].reason == "job-in-grace"

    def test_job_state_unknown_keeps(self) -> None:
        assert bound_case(job=unknown()).leases[0].reason == "job-unknown"

    def test_job_missing_from_report_keeps(self) -> None:
        assert bound_case(job=None).leases[0].reason == "job-unknown"

    def test_job_end_unknown_keeps(self) -> None:
        v = bound_case(job=terminal(ended_ago=None))
        assert v.leases[0].reason == "job-end-unknown"

    def test_clock_unknown_for_jobs_keeps(self) -> None:
        v = bound_case(job=terminal(), jobs=report(terminal(), now=None))
        assert v.leases[0].reason == "clock-unknown"

    def test_provisional_keeps_by_default(self) -> None:
        lease = make_lease(state="provisional")
        v = bound_case(lease=lease)
        assert not v.remove and v.leases[0].reason == "provisional"

    def test_unbound_expired_is_dead_with_flag_whatever_the_profile(self) -> None:
        policy = PrunePolicy(2 * HOUR, include_unbound=True)
        for profile in ("p", "someone-else"):
            v = bound_case(
                lease=make_lease(state="provisional", profile=profile), policy=policy
            )
            assert v.remove and v.leases[0].reason == "unbound-expired"

    def test_unbound_inside_window_keeps_even_with_flag(self) -> None:
        policy = PrunePolicy(2 * HOUR, include_unbound=True)
        v = bound_case(
            lease=make_lease(state="provisional"), mtime=NOW - HOUR, policy=policy
        )
        assert not v.remove and v.leases[0].reason == "provisional"

    def test_unbound_mtime_unknown_keeps(self) -> None:
        policy = PrunePolicy(2 * HOUR, include_unbound=True)
        v = bound_case(lease=make_lease(state="provisional"), mtime=None, policy=policy)
        assert v.leases[0].reason == "mtime-unknown"

    def test_unbound_clock_unknown_keeps(self) -> None:
        policy = PrunePolicy(2 * HOUR, include_unbound=True)
        lease = make_lease(state="provisional")
        plan = decide(
            inv(repo([snap(lease_ids=(lease.lease_id,))], [lrec(lease)]), now=None),
            report(),
            policy,
        )
        assert plan.verdicts[0].leases[0].reason == "clock-unknown"

    def test_unreadable_lease_reason_is_invalid(self) -> None:
        plan = decide(inv(repo([snap()], [lrec(None)])), report(), POLICY)
        assert plan.verdicts[0].reason == "unreadable-lease-in-repo"

    def test_invalid_lease_verdict_reason(self) -> None:
        # a lease that is unreadable is reported with reason "invalid"
        from srunx.snapshot.prune import _lease_verdict

        v = _lease_verdict(lrec(None), report(), POLICY, NOW)
        assert v.reason == "invalid" and not v.dead

    def test_other_cluster_and_unverifiable_reasons_are_live(self) -> None:
        other = bound_case(job=terminal(cluster="clusterB"))
        assert other.leases[0].reason == "other-cluster"
        unver = bound_case(job=terminal(cluster=None))
        assert unver.leases[0].reason == "unverifiable"

    def test_lease_with_two_jobs_one_live_keeps(self) -> None:
        a, b = make_lease(1, job_id=100), make_lease(2, job_id=101)
        plan = decide(
            inv(
                repo(
                    [snap(lease_ids=(a.lease_id, b.lease_id))],
                    [lrec(a, n=1), lrec(b, n=2)],
                )
            ),
            report(terminal(100), active(101)),
            POLICY,
        )
        assert not plan.verdicts[0].remove
        assert plan.verdicts[0].reason == "live-lease"

    def test_all_leases_dead_removes_and_counts_jobs(self) -> None:
        a, b = make_lease(1, job_id=100), make_lease(2, job_id=101)
        plan = decide(
            inv(
                repo(
                    [snap(lease_ids=(a.lease_id, b.lease_id))],
                    [lrec(a, n=1), lrec(b, n=2)],
                )
            ),
            report(terminal(100), terminal(101)),
            POLICY,
        )
        assert plan.verdicts[0].remove
        assert "all 2 job(s)" in plan.verdicts[0].message


# ------------------------------------------------------------------ cluster


class TestClusterAttribution:
    def test_cluster_equal_through_another_profile_is_verifiable_and_removable(
        self,
    ) -> None:
        lease = make_lease(profile="other-login-node", cluster="clusterA")
        v = bound_case(lease=lease, job=terminal(cluster="clusterA"))
        assert v.remove

    def test_cluster_different_keeps_even_with_old_terminal_rows(self) -> None:
        v = bound_case(job=terminal(ended_ago=30 * DAY, cluster="clusterB"))
        assert not v.remove
        assert v.leases[0].reason == "other-cluster"
        assert "clusterA" in v.message and "clusterB" in v.message

    def test_cluster_set_but_job_cluster_unknown_keeps(self) -> None:
        v = bound_case(job=terminal(cluster=None))
        assert not v.remove and v.leases[0].reason == "unverifiable"
        assert "cannot be verified" in v.message

    def test_cluster_table_rows_through_lease_verifiable(self) -> None:
        rec = lrec(make_lease(cluster="clusterA"))
        assert lease_verifiable(rec, terminal(cluster="clusterA"))[0] is True
        ok, reason, _ = lease_verifiable(rec, terminal(cluster="clusterB"))
        assert (ok, reason) == (False, "other-cluster")
        ok, reason, _ = lease_verifiable(rec, terminal(cluster=None))
        assert (ok, reason) == (False, "unverifiable")

    def test_cluster_ignores_submit_time_when_set(self) -> None:
        rec = lrec(make_lease(cluster="clusterA"))
        job = terminal(cluster="clusterB", submitted_at=NOW - 5 * DAY)
        assert lease_verifiable(rec, job)[0] is False


# -------------------------------------------------------------- correlation


class TestCorrelation:
    def _verify(
        self,
        submitted_at: int | None,
        *,
        mtime: int | None = NOW - 5 * DAY,
        lease: Lease | None = None,
    ):
        lease = lease or make_lease(cluster=None)
        return lease_verifiable(
            lrec(lease, mtime=mtime),
            terminal(cluster="clusterA", submitted_at=submitted_at),
        )

    def test_correlation_inside_window_is_verifiable(self) -> None:
        assert self._verify(NOW - 5 * DAY + 30)[0] is True

    def test_correlation_exactly_600_seconds_is_inside_either_side(self) -> None:
        ref = NOW - 5 * DAY
        assert self._verify(ref + 600)[0] is True
        assert self._verify(ref - 600)[0] is True

    def test_correlation_outside_window_keeps(self) -> None:
        ref = NOW - 5 * DAY
        for s in (ref + 601, ref - 601):
            ok, reason, _ = self._verify(s)
            assert (ok, reason) == (False, "unverifiable")

    def test_correlation_missing_submit_keeps(self) -> None:
        assert self._verify(None)[0] is False

    def test_correlation_never_falls_back_to_the_profile(self) -> None:
        lease = make_lease(cluster=None, profile="current")
        assert lease.scheduler_key == "ssh:current"
        v = bound_case(
            lease=lease, job=terminal(cluster="clusterA", submitted_at=NOW - 20 * DAY)
        )
        assert not v.remove and v.leases[0].reason == "unverifiable"

    def test_correlation_removes_when_inside_window_through_decide(self) -> None:
        lease = make_lease(cluster=None)
        v = bound_case(
            lease=lease, job=terminal(cluster=None, submitted_at=NOW - 5 * DAY - 10)
        )
        assert v.remove

    def test_correlation_reference_time_prefers_mtime(self) -> None:
        rec = lrec(make_lease(), mtime=1234)
        assert lease_reference_time(rec) == 1234

    def test_correlation_reference_time_falls_back_to_bound_at_then_created_at(
        self,
    ) -> None:
        lease = make_lease(bound_at="2026-01-01T00:00:10Z")
        assert lease_reference_time(lrec(lease, mtime=None)) == 1767225610
        lease = make_lease(bound_at=None, created_at="2026-01-01T00:00:00Z")
        assert lease_reference_time(lrec(lease, mtime=None)) == 1767225600
        lease = make_lease(bound_at="garbage", created_at="2026-01-01T00:00:00Z")
        assert lease_reference_time(lrec(lease, mtime=None)) == 1767225600

    def test_correlation_reference_time_none_when_unusable(self) -> None:
        lease = make_lease(bound_at=None, created_at="garbage")
        assert lease_reference_time(lrec(lease, mtime=None)) is None
        assert lease_reference_time(lrec(None, mtime=None)) is None
        ok, reason, _ = lease_verifiable(
            lrec(lease, mtime=None), terminal(cluster=None, submitted_at=5)
        )
        assert (ok, reason) == (False, "unverifiable")


# --------------------------------------------------------------- referenced


class TestReferenced:
    def test_referenced_through_the_working_directory(self) -> None:
        q = (row(7, workdir=f"{REPO_DIR}/trees/{SHA}/sub"),)
        assert referencing_jobs(REPO, SHA, q) == (7,)

    def test_referenced_through_the_command(self) -> None:
        q = (row(7, command=f"{REPO_DIR}/trees/{SHA}/run.sbatch"),)
        assert referencing_jobs(REPO, SHA, q) == (7,)

    def test_referenced_by_a_completed_row_still_counts(self) -> None:
        q = (row(7, workdir=f"{REPO_DIR}/trees/{SHA}", state="COMPLETED"),)
        plan = decide(inv(repo([snap()])), report(queue=q), POLICY)
        assert not plan.verdicts[0].remove
        assert plan.verdicts[0].reason == "referenced-by-job"
        assert plan.verdicts[0].message == "referenced by job 7"

    def test_referenced_lease_less_snapshot_older_than_7_days_is_kept(self) -> None:
        q = (row(7, command=f"{REPO_DIR}/trees/{SHA}/x"),)
        plan = decide(inv(repo([snap(marker_age=30 * DAY)])), report(queue=q), POLICY)
        assert plan.verdicts[0].reason == "referenced-by-job"

    def test_referenced_row_naming_another_sha_does_not_keep_it(self) -> None:
        q = (row(7, workdir=f"{REPO_DIR}/trees/{SHA2}"),)
        plan = decide(inv(repo([snap(SHA)])), report(queue=q), POLICY)
        assert plan.verdicts[0].remove

    def test_referenced_row_naming_another_repo_does_not_keep_it(self) -> None:
        q = (row(7, workdir=f"/r/snap/other-0123456789ab/trees/{SHA}"),)
        plan = decide(inv(repo([snap(SHA)])), report(queue=q), POLICY)
        assert plan.verdicts[0].remove

    def test_referenced_old_provisional_with_flag_removed_when_unreferenced(
        self,
    ) -> None:
        policy = PrunePolicy(2 * HOUR, include_unbound=True)
        lease = make_lease(state="provisional")
        i = inv(repo([snap(lease_ids=(lease.lease_id,))], [lrec(lease)]))
        plan = decide(i, report(), policy)
        assert plan.verdicts[0].remove
        assert len(plan.warnings) == 1
        assert f"{REPO_DIR}/leases/{lease.lease_id}.json" in plan.warnings[0]
        assert SHA in plan.warnings[0]

    def test_referenced_old_provisional_with_flag_kept_when_referenced(self) -> None:
        policy = PrunePolicy(2 * HOUR, include_unbound=True)
        lease = make_lease(state="provisional")
        i = inv(repo([snap(lease_ids=(lease.lease_id,))], [lrec(lease)]))
        q = (row(9, workdir=f"{REPO_DIR}/trees/{SHA}"),)
        plan = decide(i, report(queue=q), policy)
        assert not plan.verdicts[0].remove
        assert plan.verdicts[0].reason == "referenced-by-job"
        assert plan.warnings == ()


# ----------------------------------------------------------------- unleased


class TestUnleased:
    def test_unleased_marker_3_days_old_with_1h_keeps(self) -> None:
        policy = PrunePolicy(HOUR, include_unbound=False)
        plan = decide(inv(repo([snap(marker_age=3 * DAY)])), report(), policy)
        assert not plan.verdicts[0].remove and plan.verdicts[0].reason == "recent"
        assert "last requested 3d ago" in plan.verdicts[0].message

    def test_unleased_8_days_old_removes(self) -> None:
        policy = PrunePolicy(HOUR, include_unbound=False)
        plan = decide(inv(repo([snap(marker_age=8 * DAY)])), report(), policy)
        v = plan.verdicts[0]
        assert v.remove and v.reason == "unleased-old"
        assert v.message == "no leases, last requested 8d ago"

    def test_unleased_30d_threshold_with_10_day_old_marker_keeps(self) -> None:
        policy = PrunePolicy(30 * DAY, include_unbound=False)
        plan = decide(inv(repo([snap(marker_age=10 * DAY)])), report(), policy)
        assert not plan.verdicts[0].remove

    def test_unleased_exactly_at_the_threshold_keeps(self) -> None:
        policy = PrunePolicy(HOUR, include_unbound=False)
        plan = decide(inv(repo([snap(marker_age=7 * DAY)])), report(), policy)
        assert not plan.verdicts[0].remove

    def test_leased_snapshot_uses_older_than_not_the_floor(self) -> None:
        lease = make_lease()
        plan = decide(
            inv(
                repo(
                    [snap(lease_ids=(lease.lease_id,), marker_age=3 * HOUR)],
                    [lrec(lease)],
                )
            ),
            report(terminal()),
            POLICY,
        )
        assert plan.verdicts[0].remove

    def test_leased_snapshot_requested_recently_is_kept_even_if_job_is_dead(
        self,
    ) -> None:
        lease = make_lease()
        plan = decide(
            inv(
                repo(
                    [snap(lease_ids=(lease.lease_id,), marker_age=HOUR)], [lrec(lease)]
                )
            ),
            report(terminal()),
            POLICY,
        )
        assert plan.verdicts[0].reason == "recent"


# ---------------------------------------------------------------- scheduler


class TestScheduler:
    def test_scheduler_error_keeps_every_snapshot_including_lease_less(self) -> None:
        lease = make_lease()
        i = inv(
            repo(
                [
                    snap(SHA, lease_ids=(lease.lease_id,)),
                    snap(SHA2, marker_age=30 * DAY),
                ],
                [lrec(lease)],
            )
        )
        plan = decide(i, report(terminal(), error="squeue failed"), POLICY)
        assert plan.removals == ()
        assert {v.reason for v in plan.verdicts} == {"scheduler-unavailable"}
        assert plan.verdicts[0].message == "scheduler state unavailable"

    def test_scheduler_needs_scheduler_for_any_snapshot_or_lease(self) -> None:
        assert not needs_scheduler(inv())
        assert not needs_scheduler(inv(repo([])))
        assert needs_scheduler(inv(repo([snap()])))
        assert needs_scheduler(inv(repo([], [lrec(make_lease())])))
        assert not needs_scheduler(inv(repo([snap()], skipped="unsafe")))


# ------------------------------------------------------- remaining snapshot rules


class TestSnapshotRules:
    def test_both_clocks_missing(self) -> None:
        plan = decide(inv(repo([snap()]), now=None), report(now=None), POLICY)
        assert plan.verdicts[0].reason == "clock-unknown"

    def test_marker_mtime_unknown_is_age_unknown_even_with_leases(self) -> None:
        lease = make_lease()
        plan = decide(
            inv(
                repo(
                    [snap(lease_ids=(lease.lease_id,), marker_age=None)], [lrec(lease)]
                )
            ),
            report(terminal()),
            POLICY,
        )
        assert plan.verdicts[0].reason == "age-unknown"

    def test_attribution_mismatch_when_remote_saw_other_leases(self) -> None:
        lease = make_lease()
        plan = decide(
            inv(repo([snap(lease_ids=(lid(9),))], [lrec(lease)])),
            report(terminal()),
            POLICY,
        )
        assert plan.verdicts[0].reason == "attribution-mismatch"

    def test_attribution_mismatch_when_remote_could_not_attribute(self) -> None:
        plan = decide(inv(repo([snap(lease_ids=None)])), report(), POLICY)
        assert plan.verdicts[0].reason == "attribution-mismatch"

    def test_unreadable_lease_protects_its_whole_repo_but_not_others(self) -> None:
        other = "other-0123456789ab"
        a = repo([snap(SHA)], [lrec(None, n=5)])
        b = repo([snap(SHA)], repo_id=other)
        plan = decide(inv(a, b), report(), POLICY)
        assert [v.reason for v in plan.verdicts] == [
            "unreadable-lease-in-repo",
            "unleased-old",
        ]

    def test_skipped_repo_is_never_pruned(self) -> None:
        plan = decide(
            inv(repo([snap()], skipped="leases is writable")), report(), POLICY
        )
        assert plan.verdicts == () and len(plan.skipped_repos) == 1
        assert targets_for(plan) == {}

    def test_incomplete_tree_without_marker_removed_after_floor(self) -> None:
        s = snap(marker=False, intact=False)
        s = replace(s, tree_mtime=NOW - 9 * DAY)
        v = decide(inv(repo([s])), report(), POLICY).verdicts[0]
        assert v.remove and v.reason == "incomplete"
        assert v.message == "incomplete snapshot (tree without marker)"

    def test_incomplete_tree_younger_than_floor_is_kept(self) -> None:
        s = replace(snap(marker=False, intact=False), tree_mtime=NOW - 3 * DAY)
        assert not decide(inv(repo([s])), report(), POLICY).verdicts[0].remove

    def test_tree_with_marker_but_not_intact_is_incomplete(self) -> None:
        v = decide(inv(repo([snap(intact=False)])), report(), POLICY).verdicts[0]
        assert v.remove and v.reason == "incomplete"

    def test_marker_without_tree_removed_when_old(self) -> None:
        v = decide(
            inv(repo([snap(tree=False, intact=False, marker_age=9 * DAY)])),
            report(),
            POLICY,
        ).verdicts[0]
        assert v.remove and v.reason == "orphan-marker"
        assert v.message == "marker without tree"

    def test_marker_without_tree_kept_when_young(self) -> None:
        v = decide(
            inv(repo([snap(tree=False, intact=False, marker_age=DAY)])),
            report(),
            POLICY,
        ).verdicts[0]
        assert not v.remove

    def test_leases_without_snapshot_removed_when_all_dead_with_no_age_condition(
        self,
    ) -> None:
        lease = make_lease()
        s = snap(lease_ids=(lease.lease_id,), tree=False, marker=False, intact=False)
        plan = decide(inv(repo([s], [lrec(lease)])), report(terminal()), POLICY)
        v = plan.verdicts[0]
        assert v.remove and v.reason == "dangling-leases"
        assert v.message == "leases without snapshot"

    def test_leases_without_snapshot_kept_when_job_active(self) -> None:
        lease = make_lease()
        s = snap(lease_ids=(lease.lease_id,), tree=False, marker=False, intact=False)
        plan = decide(inv(repo([s], [lrec(lease)])), report(active()), POLICY)
        assert not plan.verdicts[0].remove

    def test_trash_only_repo_is_listed_and_targeted(self) -> None:
        r = repo([], trash=(f".trash.{SHA}.{lid(1)}",))
        plan = decide(inv(r), report(), POLICY)
        assert plan.trash_repos == (r,)
        assert targets_for(plan) == {REPO_DIR: []}


class TestHelpers:
    def test_job_ids_to_query_sorted_unique_bound_only(self) -> None:
        leases = [
            lrec(make_lease(1, job_id=30), n=1),
            lrec(make_lease(2, job_id=10), n=2),
            lrec(make_lease(3, job_id=30), n=3),
            lrec(make_lease(4, state="provisional"), n=4),
            lrec(None, n=5),
        ]
        skipped = repo(
            [],
            [lrec(make_lease(6, job_id=99), n=6)],
            skipped="x",
            repo_id="s-0123456789ab",
        )
        assert job_ids_to_query(inv(repo([], leases), skipped)) == [10, 30]

    def test_targets_for_content(self) -> None:
        lease = make_lease()
        s = snap(lease_ids=(lease.lease_id,))
        plan = decide(
            inv(repo([s, snap(SHA2, marker_age=DAY)], [lrec(lease)])),
            report(terminal()),
            POLICY,
        )
        targets = targets_for(plan)
        assert list(targets) == [REPO_DIR]
        (t,) = targets[REPO_DIR]
        assert t.sha == SHA and t.fingerprint == s.fingerprint
        assert t.lease_ids == (lease.lease_id,)
