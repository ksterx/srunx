"""``srunx squeue`` — list active jobs on the cluster."""

from dataclasses import dataclass
from typing import TYPE_CHECKING, Annotated, Any

if TYPE_CHECKING:
    from collections.abc import Callable

import typer
from rich.console import Console
from rich.table import Table

import srunx.slurm.local as _slurm_local  # noqa: E402,I001 — kept so ``patch("srunx.slurm.local.Slurm")`` intercepts all call sites
from srunx.cli._helpers.state_colors import colorize_state
from srunx.cli._helpers.transport_options import LocalOpt, ProfileOpt, QuietOpt
from srunx.common.exceptions import TransportError
from srunx.transport import resolve_transport


def squeue(
    job_filter: Annotated[
        list[int] | None,
        typer.Option(
            "-j",
            "--jobs",
            help=(
                "Filter to one or more specific job IDs. Replaces the old "
                "``srunx status <id>`` command — equivalent to ``squeue -j ID``."
            ),
        ),
    ] = None,
    user: Annotated[
        str | None,
        typer.Option(
            "-u",
            "--user",
            help=(
                "Filter to a single username (like ``squeue --user <name>``). "
                "Default is all users."
            ),
        ),
    ] = None,
    iterate: Annotated[
        float | None,
        typer.Option(
            "-i",
            "--iterate",
            help=(
                "Re-query the queue every N seconds and redraw in place "
                "(matches native ``squeue -i``). Exit with Ctrl+C."
            ),
        ),
    ] = None,
    show_partition: Annotated[
        bool,
        typer.Option("--show-partition", help="Add the Partition column."),
    ] = False,
    show_cpus: Annotated[
        bool,
        typer.Option("--show-cpus", help="Add the CPUs column."),
    ] = False,
    show_limit: Annotated[
        bool,
        typer.Option("--show-limit", help="Add the time-limit column."),
    ] = False,
    show_nodes: Annotated[
        bool,
        typer.Option("--show-nodes", help="Add the Nodes count column."),
    ] = False,
    show_all: Annotated[
        bool,
        typer.Option(
            "--all",
            "-a",
            help="Shortcut for --show-partition --show-cpus --show-limit --show-nodes.",
        ),
    ] = False,
    format: Annotated[
        str,
        typer.Option("--format", "-f", help="Output format: table or json"),
    ] = "table",
    profile: ProfileOpt = None,
    local: LocalOpt = False,
    quiet: QuietOpt = False,
) -> None:
    """List active jobs on the cluster.

    Shows all users' jobs by default (matching native ``squeue``).

    Default columns: Job ID, User, Name, Status, GPUs, Elapsed,
    NodeList. Use ``--show-partition`` / ``--show-cpus`` /
    ``--show-limit`` / ``--show-nodes`` (or ``-a`` / ``--all``) to
    surface the remaining SLURM fields. ``--format json`` always
    emits every field regardless of these flags — scripts can pick
    what they need.

    ``-i N`` / ``--iterate N`` re-queries the queue every ``N``
    seconds and redraws the table in place (like native
    ``squeue -i``). Incompatible with ``--format json`` (live mode is
    human-facing only). Ctrl+C exits and leaves the final frame on
    screen.

    For finished jobs, see ``srunx history``.

    Examples:
        srunx squeue
        srunx squeue -j 12345
        srunx squeue --user alice
        srunx squeue -a
        srunx squeue -i 5                # refresh every 5 seconds
        srunx squeue --format json
    """
    import json

    if iterate is not None:
        if iterate <= 0:
            typer.secho(
                "--iterate must be a positive number of seconds.",
                err=True,
                fg=typer.colors.RED,
            )
            raise typer.Exit(code=2)
        if format == "json":
            typer.secho(
                "--iterate is incompatible with --format json.",
                err=True,
                fg=typer.colors.RED,
            )
            raise typer.Exit(code=2)

    # Column-visibility flags — the four SLURM fields flagged as
    # "useful but not always needed" are hidden by default; ``--show-X``
    # (or ``-a``) surfaces them.
    visibility = _SqueueColumnVisibility(
        partition=show_partition or show_all,
        cpus=show_cpus or show_all,
        limit=show_limit or show_all,
        nodes=show_nodes or show_all,
    )

    try:
        with resolve_transport(profile=profile, local=local, quiet=quiet) as rt:

            def fetch() -> list[Any]:
                if rt.transport_type == "local":
                    client = _slurm_local.Slurm()
                    jobs = client.queue(user=user)
                else:
                    jobs = rt.job_ops.queue(user=user)
                if job_filter:
                    wanted = {int(j) for j in job_filter}
                    jobs = [j for j in jobs if j.job_id in wanted]
                return jobs

            if iterate is not None:
                _run_squeue_live(fetch, visibility=visibility, interval=iterate)
                return

            jobs = fetch()

            if format == "json":
                Console().print(json.dumps(_squeue_json(jobs), indent=2))
                return

            if not jobs:
                Console().print("No jobs in queue")
                return

            Console().print(_render_squeue_table(jobs, visibility))

    except TransportError as exc:
        typer.secho(f"Transport error: {exc}", err=True, fg=typer.colors.RED)
        raise typer.Exit(code=1) from None
    except Exception as e:
        typer.secho(f"Error retrieving job queue: {e}", err=True, fg=typer.colors.RED)
        raise typer.Exit(code=1) from e


@dataclass(frozen=True)
class _SqueueColumnVisibility:
    """Which opt-in columns the squeue table should render."""

    partition: bool
    cpus: bool
    limit: bool
    nodes: bool


def _render_squeue_table(jobs: list[Any], v: _SqueueColumnVisibility) -> Table:
    """Build the Rich ``Table`` for one squeue snapshot.

    Split out of :func:`squeue` so the live-refresh path and the
    one-shot path render through the exact same code — no drift
    possible between "first frame" and "refreshed frame" layouts.
    """
    table = Table()
    table.add_column("Job ID", style="cyan")
    table.add_column("User")
    table.add_column("Name", style="magenta", overflow="fold")
    if v.partition:
        table.add_column("Partition")
    table.add_column("Status")
    if v.nodes:
        table.add_column("Nodes", justify="right")
    if v.cpus:
        table.add_column("CPUs", justify="right")
    table.add_column("GPUs", justify="right", style="yellow")
    table.add_column("Elapsed", justify="right")
    if v.limit:
        table.add_column("Limit", justify="right")
    table.add_column("NodeList", overflow="fold")

    for job in jobs:
        status_name = job.status.name if hasattr(job, "status") else "UNKNOWN"
        row: list[str] = [
            str(job.job_id) if job.job_id else "N/A",
            getattr(job, "user", None) or "N/A",
            job.name,
        ]
        if v.partition:
            row.append(getattr(job, "partition", None) or "N/A")
        row.append(colorize_state(status_name))
        if v.nodes:
            row.append(str(getattr(job, "nodes", None) or "N/A"))
        if v.cpus:
            row.append(str(getattr(job, "cpus", None) or 0))
        row.append(str(getattr(job, "gpus", None) or 0))
        row.append(getattr(job, "elapsed_time", None) or "N/A")
        if v.limit:
            row.append(getattr(job, "time_limit", None) or "N/A")
        row.append(getattr(job, "nodelist", None) or "N/A")
        table.add_row(*row)

    return table


def _squeue_json(jobs: list[Any]) -> list[dict[str, Any]]:
    """Serialise a squeue result set to JSON-ready dicts.

    Fields match what the Pydantic BaseJob surfaces via
    ``local.Slurm.queue`` / ``SlurmSSHClient.queue`` after the S1
    refactor — kept separate from the Table builder so ``--format
    json`` isn't affected by column-visibility flags.
    """
    return [
        {
            "job_id": job.job_id,
            "user": getattr(job, "user", None),
            "name": job.name,
            "partition": getattr(job, "partition", None),
            "status": (job.status.name if hasattr(job, "status") else "UNKNOWN"),
            "nodes": getattr(job, "nodes", None),
            "cpus": getattr(job, "cpus", None),
            "gpus": getattr(job, "gpus", None),
            "nodelist": getattr(job, "nodelist", None),
            "elapsed_time": getattr(job, "elapsed_time", None),
            "time_limit": getattr(job, "time_limit", None),
        }
        for job in jobs
    ]


def _run_squeue_live(
    fetch: "Callable[[], list[Any]]",
    *,
    visibility: _SqueueColumnVisibility,
    interval: float,
) -> None:
    """Drive the live-refresh loop for ``srunx squeue -i``.

    Uses :class:`rich.live.Live` in overlay mode so the transport
    banner (emitted once before we enter) stays visible above the
    refreshing region, and so Ctrl+C leaves the last frame on screen
    instead of wiping it (which alt-screen mode would do).

    The transport context is owned by the caller — we only redraw.
    A transient ``queue()`` failure (SLURM flapping, SSH hiccup) is
    rendered as a dim notice in place of the table for that tick
    rather than bubbling out; otherwise one sinfo timeout would kill
    an hours-long watch.
    """
    import time as _time

    from rich.live import Live
    from rich.text import Text

    def _snapshot() -> Any:
        try:
            jobs = fetch()
        except Exception as exc:  # noqa: BLE001 — best-effort refresh
            return Text(f"(refresh failed: {exc})", style="bright_black italic")
        if not jobs:
            return Text("No jobs in queue", style="dim")
        return _render_squeue_table(jobs, visibility)

    # ``transient=False`` keeps the final frame on screen after Ctrl+C
    # so the user can scroll back over what they just saw.
    # ``refresh_per_second=4`` is Rich's default and is fine even for
    # our slow data-refresh cadence — Live only re-renders when we
    # call ``live.update()``.
    with Live(_snapshot(), console=Console(), transient=False) as live:
        try:
            while True:
                _time.sleep(interval)
                live.update(_snapshot())
        except KeyboardInterrupt:
            return
