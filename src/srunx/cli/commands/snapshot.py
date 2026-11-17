"""``srunx snapshot`` sub-application: create."""

from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console

from srunx.cli._helpers.transport import resolve_transport
from srunx.cli._helpers.transport_options import LocalOpt, ProfileOpt, QuietOpt
from srunx.common.exceptions import TransportError
from srunx.common.logging import get_logger
from srunx.snapshot import (
    SnapshotError,
    SnapshotService,
    SnapshotValidationError,
    plan_snapshot,
)

logger = get_logger(__name__)

snapshot_app = typer.Typer(help="Per-commit read-only code snapshots on a cluster")


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
        if rt.transport_type != "ssh" or rt.profile_name is None:
            raise typer.BadParameter(
                "snapshot create requires an SSH profile (use --profile <name>).",
                param_hint="--profile",
            )
        from srunx.ssh.core.config import ConfigManager

        server_profile = ConfigManager().get_profile(rt.profile_name)
        if server_profile is None:
            raise typer.BadParameter(
                f"SSH profile {rt.profile_name!r} not found.", param_hint="--profile"
            )
        service = SnapshotService(profile_name=rt.profile_name, profile=server_profile)
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
