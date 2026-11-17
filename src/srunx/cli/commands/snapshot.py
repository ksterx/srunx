"""``srunx snapshot`` sub-application: create, list, prune."""

import json
from pathlib import Path
from typing import TYPE_CHECKING, Annotated, Any

import typer
from rich.console import Console

from srunx.cli._helpers.snapshot_gc import (
    execute_plan,
    list_as_json,
    render_list_table,
    render_plan,
    slurm_runner,
)
from srunx.cli._helpers.transport import resolve_transport
from srunx.cli._helpers.transport_options import LocalOpt, ProfileOpt, QuietOpt
from srunx.common.exceptions import TransportError
from srunx.common.logging import get_logger
from srunx.snapshot import (
    InventoryStore,
    SnapshotError,
    SnapshotService,
    SnapshotValidationError,
    check_min_job_age,
    decide,
    parse_duration,
    plan_snapshot,
)
from srunx.snapshot.inventory import Inventory
from srunx.snapshot.job_states import JobStateReport, query_job_states
from srunx.snapshot.prune import (
    DEFAULT_OLDER_THAN,
    PrunePolicy,
    job_ids_to_query,
    needs_scheduler,
    targets_for,
)
from srunx.snapshot.remote import RemoteSnapshotStore, literal_root_conflict

if TYPE_CHECKING:
    from srunx.ssh.core.config import ServerProfile

logger = get_logger(__name__)

snapshot_app = typer.Typer(help="Per-commit read-only code snapshots on a cluster")


def _require_ssh_profile(rt: Any, command: str) -> tuple[str, "ServerProfile"]:
    """Return ``(profile name, profile)``; a transport that is not SSH exits 2."""
    if rt.transport_type != "ssh" or rt.profile_name is None:
        raise typer.BadParameter(
            f"snapshot {command} requires an SSH profile (use --profile <name>).",
            param_hint="--profile",
        )
    from srunx.ssh.core.config import ConfigManager

    server_profile = ConfigManager().get_profile(rt.profile_name)
    if server_profile is None:
        raise typer.BadParameter(
            f"SSH profile {rt.profile_name!r} not found.", param_hint="--profile"
        )
    return rt.profile_name, server_profile


def _build_inventory_store(profile: "ServerProfile") -> InventoryStore:
    """Literal root/mount conflicts exit 2 before any remote call."""
    mounts = [m.remote for m in profile.mounts]
    conflict = literal_root_conflict(profile.snapshot_root, mounts)
    if conflict:
        raise typer.BadParameter(conflict)
    from srunx.sync.mount_helpers import build_rsync_client

    return InventoryStore(
        RemoteSnapshotStore(
            build_rsync_client(profile),
            snapshot_root=profile.snapshot_root,
            mounts=mounts,
        )
    )


def _empty_report() -> JobStateReport:
    return JobStateReport(now=None, min_job_age=None, queue=(), states={}, error=None)


def _job_report(rt: Any, inventory: Inventory) -> JobStateReport:
    """Ask the scheduler whenever there is a snapshot or lease to judge."""
    if not needs_scheduler(inventory):
        return _empty_report()
    return query_job_states(job_ids_to_query(inventory), slurm_runner(rt))


@snapshot_app.command("create")
def snapshot_create(
    rev: Annotated[
        str | None,
        typer.Argument(help="Git revision to snapshot (default: HEAD)."),
    ] = None,
    profile: ProfileOpt = None,
    local: LocalOpt = False,
    quiet: QuietOpt = False,
) -> None:
    """Ensure a read-only snapshot of REV exists on the cluster.

    Prints only the absolute snapshot path on stdout (status goes to stderr),
    so ``$(srunx snapshot create)`` is safe to use in scripts. SSH profiles
    only. Writes no lease.
    """
    err = Console(stderr=True)
    cwd = Path.cwd()
    try:
        plan = plan_snapshot(start=cwd, rev=rev, script=None, cwd=cwd)
    except SnapshotValidationError as exc:
        raise typer.BadParameter(str(exc)) from exc
    for w in plan.warnings:
        err.print(f"warning: {w}", style="yellow", markup=False, highlight=False)

    with resolve_transport(profile=profile, local=local, quiet=quiet) as rt:
        profile_name, server_profile = _require_ssh_profile(rt, "create")
        service = SnapshotService(profile_name=profile_name, profile=server_profile)
        try:
            prepared = service.create(plan)
        except SnapshotValidationError as exc:
            raise typer.BadParameter(str(exc)) from exc
        except (SnapshotError, TransportError) as exc:
            typer.echo(f"Error: {exc}", err=True)
            raise typer.Exit(code=1) from exc

    err.print(
        f"Snapshot {plan.sha[:12]} {prepared.status}", markup=False, highlight=False
    )
    typer.echo(prepared.path)


@snapshot_app.command("list")
def snapshot_list(
    format: Annotated[
        str,
        typer.Option("--format", "-f", help="Output format: table or json"),
    ] = "table",
    size: Annotated[
        bool,
        typer.Option(
            "--size",
            help="Also report each tree's size (walks every tree; slow on network filesystems).",
        ),
    ] = False,
    profile: ProfileOpt = None,
    local: LocalOpt = False,
    quiet: QuietOpt = False,
) -> None:
    """List every repo and snapshot under the profile's snapshot root.

    Shows each snapshot's state, last-requested time, its leases with the live
    state of their jobs, and queued jobs that reference its path. A scheduler
    failure only shows job states as unknown (with a warning); it does not
    change the exit code. SSH profiles only.
    """
    if format not in ("table", "json"):
        raise typer.BadParameter(
            f"unknown format {format!r} (use table or json).", param_hint="--format"
        )
    err = Console(stderr=True)
    with resolve_transport(profile=profile, local=local, quiet=quiet) as rt:
        _, server_profile = _require_ssh_profile(rt, "list")
        store = _build_inventory_store(server_profile)
        try:
            inventory = store.read(with_size=size)
            jobs = _job_report(rt, inventory)
        except SnapshotValidationError as exc:
            raise typer.BadParameter(str(exc)) from exc
        except (SnapshotError, TransportError) as exc:
            typer.echo(f"Error: {exc}", err=True)
            raise typer.Exit(code=1) from exc
    if jobs.error is not None:
        err.print(
            f"warning: scheduler state unavailable ({jobs.error}); "
            "job states are shown as unknown",
            style="yellow",
            markup=False,
            highlight=False,
        )
    if format == "json":
        typer.echo(json.dumps(list_as_json(inventory, jobs), indent=2))
        return
    render_list_table(Console(), inventory, jobs, show_size=size)


@snapshot_app.command("prune")
def snapshot_prune(
    older_than: Annotated[
        str,
        typer.Option(
            "--older-than",
            help="Remove only snapshots whose jobs ended, or that were last "
            "requested, longer ago than this: <N>d, <N>h or <N>m (minimum 1h).",
        ),
    ] = DEFAULT_OLDER_THAN,
    dry_run: Annotated[
        bool,
        typer.Option("--dry-run", "-n", help="Print the plan and delete nothing."),
    ] = False,
    force: Annotated[
        bool,
        typer.Option("--force", "-f", help="Do not ask for confirmation."),
    ] = False,
    include_unbound: Annotated[
        bool,
        typer.Option(
            "--include-unbound",
            help="Also treat old provisional leases (an sbatch whose outcome was "
            "unknown) as dead. A job may still exist; it is then only detected "
            "when its working directory or command names the snapshot.",
        ),
    ] = False,
    profile: ProfileOpt = None,
    local: LocalOpt = False,
    quiet: QuietOpt = False,
) -> None:
    """Remove snapshots that no pending, running or requeue-able job can use.

    Everything that cannot be positively judged dead is kept: if the scheduler
    cannot be queried, nothing is deleted and the exit code is 1. Prints one
    line per snapshot (keep or remove, and why), then asks for confirmation
    unless --force. SSH profiles only.
    """
    try:
        older_seconds = parse_duration(older_than)
    except SnapshotValidationError as exc:
        raise typer.BadParameter(str(exc), param_hint="--older-than") from exc
    policy = PrunePolicy(older_than=older_seconds, include_unbound=include_unbound)
    err = Console(stderr=True)
    out = Console()
    with resolve_transport(profile=profile, local=local, quiet=quiet) as rt:
        _, server_profile = _require_ssh_profile(rt, "prune")
        store = _build_inventory_store(server_profile)
        try:
            inventory = store.read()
            jobs = _job_report(rt, inventory)
            check_min_job_age(older_seconds, jobs.min_job_age)
        except SnapshotValidationError as exc:
            raise typer.BadParameter(str(exc), param_hint="--older-than") from exc
        except (SnapshotError, TransportError) as exc:
            typer.echo(f"Error: {exc}", err=True)
            raise typer.Exit(code=1) from exc

        plan = decide(inventory, jobs, policy)
        render_plan(out, plan, policy)
        for warning in plan.warnings:
            err.print(
                f"warning: {warning}", style="yellow", markup=False, highlight=False
            )
        if jobs.error is not None:
            typer.echo(
                f"Error: scheduler state unavailable ({jobs.error}); nothing was deleted.",
                err=True,
            )
            raise typer.Exit(code=1)
        if dry_run:
            return
        targets = targets_for(plan)
        if not targets:
            out.print("Nothing to remove.")
            return
        count = len(plan.removals)
        question = (
            f"Remove {count} snapshot(s)?"
            if count
            else "Clean up leftover trash from an interrupted prune?"
        )
        if not force and not typer.confirm(question, default=False, err=True):
            err.print("Aborted; nothing was deleted.", markup=False, highlight=False)
            return
        code = execute_plan(store, inventory, plan, err, out)
    if code != 0:
        raise typer.Exit(code=code)
