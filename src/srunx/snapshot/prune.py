"""Decide which snapshots are dead. Pure: no I/O and no clock read.

A snapshot is removed only when every fact needed to call it dead was
positively confirmed; anything that cannot be judged is kept. The rules are in
``docs/specs/snapshot-prune/spec.md`` (REQ-6b .. REQ-8); ``decide`` applies them
in order and the first failing rule becomes the reason shown to the user.

No profile or ``scheduler_key`` is consulted anywhere: a profile is not a
cluster, so leases are attributed by cluster name or by submit-time
correlation only.
"""

from __future__ import annotations

import re
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Literal

from srunx.snapshot.errors import SnapshotValidationError
from srunx.snapshot.inventory import (
    Inventory,
    LeaseRecord,
    PruneTarget,
    RepoInventory,
    SnapshotRecord,
)
from srunx.snapshot.job_states import JobState, JobStateReport, QueueRow

MIN_OLDER_THAN = 3600
MIN_JOB_AGE_MARGIN = 600
UNLEASED_MIN_AGE = 7 * 86400
CORRELATION_WINDOW = 600  # seconds, inclusive, either side
DEFAULT_OLDER_THAN = "7d"

_DURATION_RE = re.compile(r"([1-9][0-9]*)([dhm])")
_UNIT_SECONDS = {"d": 86400, "h": 3600, "m": 60}


def format_age(seconds: int) -> str:
    """Compact human duration: ``3d``, ``5h``, ``12m``, ``40s``."""
    seconds = max(0, int(seconds))
    for unit, size in (("d", 86400), ("h", 3600), ("m", 60)):
        if seconds >= size:
            return f"{seconds // size}{unit}"
    return f"{seconds}s"


def parse_duration(text: str) -> int:
    """``<N>d`` / ``<N>h`` / ``<N>m`` in seconds; below one hour is rejected."""
    m = _DURATION_RE.fullmatch(text)
    if m is None:
        raise SnapshotValidationError(
            f"invalid duration {text!r}: use <N>d, <N>h or <N>m with a positive integer"
        )
    seconds = int(m.group(1)) * _UNIT_SECONDS[m.group(2)]
    if seconds < MIN_OLDER_THAN:
        raise SnapshotValidationError(
            f"--older-than {text} is below the minimum of 1h; a job that finished "
            "recently can still be requeued and needs its snapshot"
        )
    return seconds


def check_min_job_age(older_than: int, min_job_age: int | None) -> None:
    """Reject ``older_than`` below ``MinJobAge`` plus a margin; no-op when unknown."""
    if min_job_age is None:
        return
    needed = min_job_age + MIN_JOB_AGE_MARGIN
    if older_than < needed:
        raise SnapshotValidationError(
            f"--older-than ({format_age(older_than)}) must be at least MinJobAge "
            f"({min_job_age}s) + {MIN_JOB_AGE_MARGIN // 60} minutes "
            f"({format_age(needed)}): the scheduler keeps finished jobs in memory "
            "for MinJobAge and they can be requeued meanwhile"
        )


@dataclass(frozen=True)
class PrunePolicy:
    older_than: int
    include_unbound: bool


LeaseReason = Literal[
    "job-finished",
    "unbound-expired",  # dead
    "job-active",
    "job-in-grace",
    "job-unknown",
    "job-end-unknown",
    "provisional",
    "other-cluster",
    "unverifiable",
    "mtime-unknown",
    "clock-unknown",
    "invalid",
]


@dataclass(frozen=True)
class LeaseVerdict:
    record: LeaseRecord
    dead: bool
    reason: LeaseReason
    message: str
    job: JobState | None


SnapshotReason = Literal[
    "unleased-old",
    "jobs-finished",
    "incomplete",
    "orphan-marker",
    "dangling-leases",  # remove
    "scheduler-unavailable",
    "referenced-by-job",
    "recent",
    "live-lease",
    "unreadable-lease-in-repo",
    "attribution-mismatch",
    "age-unknown",
    "clock-unknown",
]


@dataclass(frozen=True)
class SnapshotVerdict:
    repo: RepoInventory
    snapshot: SnapshotRecord
    remove: bool
    reason: SnapshotReason
    message: str
    leases: tuple[LeaseVerdict, ...]
    referenced_by: tuple[int, ...]  # job ids from the path-reference guard


@dataclass(frozen=True)
class PrunePlan:
    verdicts: tuple[SnapshotVerdict, ...]
    skipped_repos: tuple[RepoInventory, ...]
    trash_repos: tuple[RepoInventory, ...]
    warnings: tuple[str, ...]  # one per expired provisional lease being removed

    @property
    def removals(self) -> tuple[SnapshotVerdict, ...]:
        return tuple(v for v in self.verdicts if v.remove)


def needs_scheduler(inventory: Inventory) -> bool:
    """True when any snapshot or lease exists in a repo that is not skipped."""
    return any(r.skipped is None and (r.snapshots or r.leases) for r in inventory.repos)


def job_ids_to_query(inventory: Inventory) -> list[int]:
    ids: set[int] = set()
    for repo in inventory.repos:
        if repo.skipped is not None:
            continue
        for rec in repo.leases:
            if rec.lease is not None and rec.lease.job_id is not None:
                ids.add(rec.lease.job_id)
    return sorted(ids)


def referencing_jobs(
    repo_id: str, sha: str, queue: Sequence[QueueRow]
) -> tuple[int, ...]:
    needle = f"{repo_id}/trees/{sha}"
    return tuple(
        sorted({r.job_id for r in queue if needle in r.workdir or needle in r.command})
    )


def _parse_utc(text: str | None) -> int | None:
    if not text:
        return None
    try:
        return int(
            datetime.strptime(text, "%Y-%m-%dT%H:%M:%SZ")
            .replace(tzinfo=UTC)
            .timestamp()
        )
    except ValueError:
        return None


def lease_reference_time(record: LeaseRecord) -> int | None:
    """Lease file mtime (the cluster's clock), else ``bound_at``, else ``created_at``."""
    if record.mtime is not None:
        return record.mtime
    if record.lease is None:
        return None
    for text in (record.lease.bound_at, record.lease.created_at):
        parsed = _parse_utc(text)
        if parsed is not None:
            return parsed
    return None


def lease_verifiable(
    record: LeaseRecord, job: JobState
) -> tuple[bool, LeaseReason | None, str]:
    """May a *terminal* job state be used for this bound lease? (REQ-6b)"""
    lease = record.lease
    assert lease is not None
    if lease.cluster is not None:
        if job.cluster == lease.cluster:
            return True, None, ""
        if job.cluster is not None:
            return (
                False,
                "other-cluster",
                f"lease from another cluster ({job.cluster}); "
                f"the lease was written for {lease.cluster}",
            )
        return (
            False,
            "unverifiable",
            "lease cannot be verified against this cluster",
        )
    reference = lease_reference_time(record)
    if (
        job.submitted_at is not None
        and reference is not None
        and abs(job.submitted_at - reference) <= CORRELATION_WINDOW
    ):
        return True, None, ""
    return False, "unverifiable", "lease cannot be verified against this cluster"


def _lease_verdict(
    record: LeaseRecord,
    jobs: JobStateReport,
    policy: PrunePolicy,
    inventory_now: int | None,
) -> LeaseVerdict:
    lease = record.lease
    if lease is None:
        return LeaseVerdict(
            record,
            False,
            "invalid",
            f"unreadable lease {record.filename}: {record.error}",
            None,
        )
    if lease.state == "provisional" or lease.job_id is None:
        text = "provisional lease (no job id)"
        if not policy.include_unbound:
            return LeaseVerdict(record, False, "provisional", text, None)
        if record.mtime is None:
            return LeaseVerdict(
                record, False, "mtime-unknown", "lease file mtime unknown", None
            )
        if inventory_now is None:
            return LeaseVerdict(record, False, "clock-unknown", "clock unknown", None)
        age = inventory_now - record.mtime
        if age > policy.older_than:
            return LeaseVerdict(
                record,
                True,
                "unbound-expired",
                f"provisional lease expired ({format_age(age)} old)",
                None,
            )
        return LeaseVerdict(record, False, "provisional", text, None)

    jid = lease.job_id
    job = jobs.states.get(jid)
    if job is None or job.kind == "unknown":
        return LeaseVerdict(
            record, False, "job-unknown", f"job {jid} state unknown", job
        )
    if job.kind == "active":
        return LeaseVerdict(
            record, False, "job-active", f"job {jid} active ({job.state})", job
        )
    ok, reason, message = lease_verifiable(record, job)
    if not ok:
        assert reason is not None
        return LeaseVerdict(record, False, reason, message, job)
    if job.ended_at is None:
        return LeaseVerdict(
            record, False, "job-end-unknown", f"job {jid} end time unknown", job
        )
    if jobs.now is None:
        return LeaseVerdict(record, False, "clock-unknown", "clock unknown", job)
    age = jobs.now - job.ended_at
    if age > policy.older_than:
        return LeaseVerdict(
            record,
            True,
            "job-finished",
            f"job {jid} finished {format_age(age)} ago",
            job,
        )
    return LeaseVerdict(
        record,
        False,
        "job-in-grace",
        f"job {jid} finished {format_age(age)} ago (inside the resume grace)",
        job,
    )


def _kept(
    repo: RepoInventory,
    snap: SnapshotRecord,
    reason: SnapshotReason,
    message: str,
    leases: tuple[LeaseVerdict, ...],
    referenced: tuple[int, ...],
) -> SnapshotVerdict:
    return SnapshotVerdict(repo, snap, False, reason, message, leases, referenced)


def _decide_snapshot(
    inventory: Inventory,
    repo: RepoInventory,
    snap: SnapshotRecord,
    lease_verdicts: dict[str, LeaseVerdict],
    jobs: JobStateReport,
    policy: PrunePolicy,
) -> SnapshotVerdict:
    mine = tuple(
        v
        for v in lease_verdicts.values()
        if v.record.lease is not None and v.record.lease.sha == snap.sha
    )
    referenced = referencing_jobs(repo.repo_id, snap.sha, jobs.queue)

    def keep(reason: SnapshotReason, message: str) -> SnapshotVerdict:
        return _kept(repo, snap, reason, message, mine, referenced)

    if jobs.error is not None:
        return keep("scheduler-unavailable", "scheduler state unavailable")
    if referenced:
        ids = ", ".join(str(i) for i in referenced)
        return keep("referenced-by-job", f"referenced by job {ids}")
    bad = next((r for r in repo.leases if r.lease is None), None)
    if bad is not None:
        return keep(
            "unreadable-lease-in-repo",
            f"repo has an unreadable lease ({bad.filename}: {bad.error})",
        )
    mine_ids = {v.record.lease.lease_id for v in mine if v.record.lease is not None}
    if snap.fp_lease_ids is None or snap.fp_lease_ids != mine_ids:
        return keep(
            "attribution-mismatch",
            "lease files attributed to this snapshot do not match their contents",
        )
    live = [v for v in mine if not v.dead]
    if live:
        return keep("live-lease", "; ".join(v.message for v in live))

    has_snapshot = snap.has_tree or snap.has_marker
    if has_snapshot:
        if inventory.now is None:
            return keep("clock-unknown", "clock unknown")
        mtime = snap.marker_mtime if snap.has_marker else snap.tree_mtime
        if mtime is None:
            return keep("age-unknown", "last requested time unknown")
        threshold = (
            policy.older_than if mine else max(policy.older_than, UNLEASED_MIN_AGE)
        )
        age = inventory.now - mtime
        if age <= threshold:
            return keep(
                "recent",
                f"last requested {format_age(age)} ago "
                f"(removed after {format_age(threshold)})",
            )

    reason: SnapshotReason
    if not has_snapshot:
        reason, message = "dangling-leases", "leases without snapshot"
    elif snap.has_tree and not snap.has_marker:
        reason, message = "incomplete", "incomplete snapshot (tree without marker)"
    elif snap.has_tree and not snap.intact:
        reason, message = "incomplete", "incomplete snapshot (tree is not intact)"
    elif not snap.has_tree:
        reason, message = "orphan-marker", "marker without tree"
    elif mine:
        reason = "jobs-finished"
        message = (
            f"all {len(mine)} job(s) finished more than "
            f"{format_age(policy.older_than)} ago"
        )
    else:
        assert inventory.now is not None
        mtime = snap.marker_mtime if snap.has_marker else snap.tree_mtime
        assert mtime is not None
        reason = "unleased-old"
        message = f"no leases, last requested {format_age(inventory.now - mtime)} ago"
    return SnapshotVerdict(repo, snap, True, reason, message, mine, referenced)


def decide(
    inventory: Inventory, jobs: JobStateReport, policy: PrunePolicy
) -> PrunePlan:
    verdicts: list[SnapshotVerdict] = []
    skipped: list[RepoInventory] = []
    trash_repos: list[RepoInventory] = []
    warnings: list[str] = []
    for repo in inventory.repos:
        if repo.skipped is not None:
            skipped.append(repo)
            continue
        if repo.trash:
            trash_repos.append(repo)
        lease_verdicts = {
            rec.filename: _lease_verdict(rec, jobs, policy, inventory.now)
            for rec in repo.leases
        }
        for snap in repo.snapshots:
            verdict = _decide_snapshot(
                inventory, repo, snap, lease_verdicts, jobs, policy
            )
            verdicts.append(verdict)
            if verdict.remove:
                for lv in verdict.leases:
                    if lv.reason == "unbound-expired":
                        lease = lv.record.lease
                        assert lease is not None
                        warnings.append(
                            f"removing provisional lease "
                            f"{repo.repo_dir}/leases/{lv.record.filename} "
                            f"(sha {lease.sha}, {lv.message}): a job submitted with an "
                            "unknown sbatch outcome may still exist; it is only "
                            "detected when its working directory or command names "
                            "the snapshot"
                        )
    return PrunePlan(
        verdicts=tuple(verdicts),
        skipped_repos=tuple(skipped),
        trash_repos=tuple(trash_repos),
        warnings=tuple(warnings),
    )


def targets_for(plan: PrunePlan) -> dict[str, list[PruneTarget]]:
    """``repo_dir -> targets``; repos with only leftover trash map to ``[]``."""
    out: dict[str, list[PruneTarget]] = {}
    for v in plan.removals:
        ids = tuple(
            sorted(
                lv.record.lease.lease_id
                for lv in v.leases
                if lv.record.lease is not None
            )
        )
        out.setdefault(v.repo.repo_dir, []).append(
            PruneTarget(v.snapshot.sha, v.snapshot.fingerprint, ids)
        )
    for repo in plan.trash_repos:
        out.setdefault(repo.repo_dir, [])
    return out
