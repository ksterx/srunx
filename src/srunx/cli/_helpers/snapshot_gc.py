"""Rendering and execution helpers for ``srunx snapshot list`` / ``prune``.

Pure presentation plus the per-repo deletion loop; all decisions come from
:mod:`srunx.snapshot.prune` and all remote work from
:mod:`srunx.snapshot.inventory`.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any, Protocol

from rich.console import Console
from rich.markup import escape
from rich.table import Table

from srunx.snapshot.errors import SnapshotError, SnapshotRemoteError
from srunx.snapshot.inventory import (
    Inventory,
    LeaseRecord,
    PruneRepoResult,
    PruneTarget,
    RepoInventory,
    SnapshotRecord,
)
from srunx.snapshot.job_states import JobState, JobStateReport, SlurmExecute
from srunx.snapshot.prune import (
    PrunePlan,
    PrunePolicy,
    format_age,
    referencing_jobs,
    targets_for,
)


class Pruner(Protocol):
    """The slice of ``InventoryStore`` the deletion loop uses."""

    def prune_repo(
        self, *, repo_dir: str, git: str, targets: Sequence[PruneTarget]
    ) -> PruneRepoResult: ...


def slurm_runner(rt: Any) -> SlurmExecute | None:
    """The SSH client's SLURM command runner, or None when the transport has none."""
    return getattr(rt.job_ops, "run_slurm_command", None)


def _state_of(snap: SnapshotRecord) -> str:
    if snap.has_tree and snap.has_marker and snap.intact:
        return "complete"
    if snap.has_tree:
        return "incomplete"
    return "missing"


def _last_requested(snap: SnapshotRecord) -> int | None:
    return snap.marker_mtime if snap.has_marker else snap.tree_mtime


def _age_text(now: int | None, then: int | None) -> str:
    if now is None or then is None:
        return "-"
    return f"{format_age(now - then)} ago"


def _job_text(job: JobState | None) -> str:
    if job is None or job.kind == "unknown":
        return "unknown"
    if job.kind == "terminal":
        return f"{job.state} (finished)"
    return str(job.state)


def _leases_for(repo: RepoInventory, sha: str) -> list[LeaseRecord]:
    return [r for r in repo.leases if r.lease is not None and r.lease.sha == sha]


def _lease_text(rec: LeaseRecord, jobs: JobStateReport) -> str:
    lease = rec.lease
    assert lease is not None
    where = f"{lease.profile}, {lease.cluster or 'cluster unknown'}"
    if lease.state == "provisional" or lease.job_id is None:
        return f"provisional (no job id) [{lease.profile}]"
    state = _job_text(jobs.states.get(lease.job_id))
    return f"job {lease.job_id} {state} [{where}]"


def render_list_table(
    console: Console,
    inventory: Inventory,
    jobs: JobStateReport,
    *,
    show_size: bool = False,
) -> None:
    """One table per repo: snapshots, their leases with live job states, referencing jobs."""
    if not inventory.exists:
        console.print(f"No snapshot root yet: {escape(inventory.root)}")
        return
    if not inventory.repos:
        console.print(f"No snapshots under {escape(inventory.root)}")
        return
    for repo in inventory.repos:
        title = escape(f"{repo.repo_id}  ({repo.repo_dir})")
        if repo.locked_since is not None:
            since = _age_text(inventory.now, repo.locked_since)
            title += f"  locked since {escape(since)}"
        console.print(f"[bold]{title}[/bold]")
        if repo.skipped is not None:
            console.print(f"  skipped: {escape(repo.skipped)}")
            continue
        table = Table(show_header=True, header_style="bold")
        table.add_column("SHA")
        table.add_column("State")
        table.add_column("Last requested")
        if show_size:
            table.add_column("Size (KiB)", justify="right")
        table.add_column("Leases")
        table.add_column("Referenced by job")
        for snap in repo.snapshots:
            leases = "\n".join(
                escape(_lease_text(r, jobs)) for r in _leases_for(repo, snap.sha)
            )
            refs = referencing_jobs(repo.repo_id, snap.sha, jobs.queue)
            row = [
                snap.sha[:12],
                _state_of(snap),
                _age_text(inventory.now, _last_requested(snap)),
            ]
            if show_size:
                row.append("-" if snap.size_kb is None else str(snap.size_kb))
            row += [leases or "-", ", ".join(str(i) for i in refs) or "-"]
            table.add_row(*row)
        console.print(table)
        bad = [r for r in repo.leases if r.lease is None]
        for rec in bad:
            console.print(
                escape(f"  unreadable lease {rec.filename}: {rec.error}"),
                style="yellow",
            )
        if repo.trash:
            console.print(f"  {len(repo.trash)} leftover trash director(y/ies)")
        for name in repo.stray:
            console.print(escape(f"  unrecognised entry: {name}"), style="dim")
    for name in inventory.stray:
        console.print(escape(f"unrecognised entry in the root: {name}"), style="dim")


def _job_json(job: JobState | None) -> dict[str, Any] | None:
    if job is None:
        return None
    return {
        "kind": job.kind,
        "state": job.state,
        "ended_at": job.ended_at,
        "cluster": job.cluster,
        "source": job.source,
    }


def list_as_json(inventory: Inventory, jobs: JobStateReport) -> dict[str, Any]:
    """Every field, whatever the table shows."""
    repos: list[dict[str, Any]] = []
    for repo in inventory.repos:
        snaps: list[dict[str, Any]] = []
        for snap in repo.snapshots:
            leases = []
            for rec in _leases_for(repo, snap.sha):
                lease = rec.lease
                assert lease is not None
                leases.append(
                    {
                        "lease_id": lease.lease_id,
                        "state": lease.state,
                        "job_id": lease.job_id,
                        "profile": lease.profile,
                        "cluster": lease.cluster,
                        "created_at": lease.created_at,
                        "bound_at": lease.bound_at,
                        "mtime": rec.mtime,
                        "job": _job_json(
                            jobs.states.get(lease.job_id)
                            if lease.job_id is not None
                            else None
                        ),
                    }
                )
            snaps.append(
                {
                    "sha": snap.sha,
                    "state": _state_of(snap),
                    "last_requested": _last_requested(snap),
                    "marker_mtime": snap.marker_mtime,
                    "tree_mtime": snap.tree_mtime,
                    "size_kb": snap.size_kb,
                    "leases": leases,
                    "referenced_by": list(
                        referencing_jobs(repo.repo_id, snap.sha, jobs.queue)
                    ),
                }
            )
        repos.append(
            {
                "repo_id": repo.repo_id,
                "repo_dir": repo.repo_dir,
                "skipped": repo.skipped,
                "locked_since": repo.locked_since,
                "snapshots": snaps,
                "unreadable_leases": [
                    {"filename": r.filename, "error": r.error, "mtime": r.mtime}
                    for r in repo.leases
                    if r.lease is None
                ],
                "trash": list(repo.trash),
                "stray": list(repo.stray),
            }
        )
    return {
        "root": inventory.root,
        "exists": inventory.exists,
        "now": inventory.now,
        "scheduler_error": jobs.error,
        "min_job_age": jobs.min_job_age,
        "repos": repos,
        "stray": list(inventory.stray),
    }


def render_plan(console: Console, plan: PrunePlan, policy: PrunePolicy) -> None:
    """One line per snapshot with the action and the reason."""
    for v in plan.verdicts:
        action = "remove" if v.remove else "keep"
        style = "red" if v.remove else "green"
        console.print(
            f"[{style}]{action:<6}[/{style}] "
            f"{escape(v.repo.repo_id)}/{v.snapshot.sha[:12]}  {escape(v.message)}"
        )
    for repo in plan.skipped_repos:
        console.print(
            f"[yellow]skip  [/yellow] {escape(repo.repo_id)}  {escape(repo.skipped or '')}"
        )
    for repo in plan.trash_repos:
        console.print(
            f"[red]clean [/red] {escape(repo.repo_id)}  "
            f"{len(repo.trash)} leftover trash director(y/ies) from an interrupted prune"
        )
    n = len(plan.removals)
    console.print(
        f"{n} snapshot(s) to remove, {len(plan.verdicts) - n} kept "
        f"(older than {format_age(policy.older_than)})"
    )


def execute_plan(
    store: Pruner,
    inventory: Inventory,
    plan: PrunePlan,
    err: Console,
    out: Console | None = None,
) -> int:
    """Run the locked deletion repo by repo; 1 when any repo raised or any sha failed.

    A failing repo never stops the others. ``CHANGED`` shas are reported as
    kept and are not errors; left-alone trash entries are warnings only.
    """
    out = out or Console()
    rc = 0
    for repo_dir, targets in sorted(targets_for(plan).items()):
        name = repo_dir.rsplit("/", 1)[-1]
        try:
            if inventory.git is None:
                raise SnapshotRemoteError("the inventory did not report a git path")
            result = store.prune_repo(
                repo_dir=repo_dir, git=inventory.git, targets=targets
            )
        except SnapshotError as exc:
            err.print(
                f"Error: {name}: {exc}", style="red", markup=False, highlight=False
            )
            rc = 1
            continue
        for sha in result.removed:
            out.print(f"removed {escape(name)}/{sha[:12]}")
        for sha in result.changed:
            out.print(
                f"kept    {escape(name)}/{sha[:12]}  changed since the inventory (left untouched)"
            )
        for sha, reason in result.failed:
            err.print(
                f"FAILED  {name}/{sha[:12]}  {reason}",
                style="red",
                markup=False,
                highlight=False,
            )
            rc = 1
        for entry in result.trash_skipped:
            err.print(
                f"warning: {name}: left {entry!r} in trees/ (not a prune trash directory)",
                style="yellow",
                markup=False,
                highlight=False,
            )
        if result.sweep_incomplete:
            err.print(
                f"warning: {name}: the removals above are complete but the trash "
                "cleanup did not finish; the next prune finishes it",
                style="yellow",
                markup=False,
                highlight=False,
            )
            rc = 1
        if result.trash_left:
            err.print(
                f"warning: {name}: {result.trash_left} trash director(y/ies) could "
                "not be deleted; the next prune retries",
                style="yellow",
                markup=False,
                highlight=False,
            )
    return rc
