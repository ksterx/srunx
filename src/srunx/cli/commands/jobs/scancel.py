"""``srunx scancel`` — cancel a running job."""

from typing import Annotated

import typer
from rich.console import Console

import srunx.slurm.local as _slurm_local  # noqa: E402,I001 — kept so ``patch("srunx.slurm.local.Slurm")`` intercepts all call sites
from srunx.cli._helpers.transport_options import LocalOpt, ProfileOpt, QuietOpt
from srunx.common.exceptions import JobNotFoundError, TransportError
from srunx.transport import resolve_transport


def scancel(
    job_id: Annotated[int, typer.Argument(help="Job ID to cancel")],
    profile: ProfileOpt = None,
    local: LocalOpt = False,
    quiet: QuietOpt = False,
) -> None:
    """Cancel a running job."""
    try:
        with resolve_transport(profile=profile, local=local, quiet=quiet) as rt:
            if rt.transport_type == "local":
                client = _slurm_local.Slurm()
                client.cancel(job_id)
            else:
                rt.job_ops.cancel(job_id)

        console = Console()
        console.print(f"✅ Job {job_id} cancelled successfully")

    except JobNotFoundError:
        typer.secho(
            f"Job {job_id} not found",
            err=True,
            fg=typer.colors.RED,
        )
        raise typer.Exit(code=1) from None
    except TransportError as exc:
        typer.secho(f"Transport error: {exc}", err=True, fg=typer.colors.RED)
        raise typer.Exit(code=1) from None
    except Exception as e:
        typer.secho(
            f"Error cancelling job {job_id}: {e}", err=True, fg=typer.colors.RED
        )
        raise typer.Exit(code=1) from e
