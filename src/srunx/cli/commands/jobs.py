"""Job-oriented CLI commands: sbatch, squeue, scancel, sinfo, gpus, tail."""

import os
import sys
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Annotated, Any

import typer
from rich.console import Console
from rich.table import Table

import srunx.slurm.local as _slurm_local  # noqa: E402,I001 — kept so ``patch("srunx.slurm.local.Slurm")`` intercepts all call sites

# Module-level import kept so tests can ``patch("srunx.slurm.local.Slurm")``
# and intercept the single canonical class (jobs.py + sbatch_helpers.py
# both dereference it via this module reference at call time).
from srunx.callbacks import Callback
from srunx.cli._helpers.sbatch_helpers import (
    _build_extra_sbatch_args,
    _parse_container_args,
    _parse_env_vars,
    _parse_gres_gpu,
    _print_in_place_sync_preview,
    _submit_via_transport,
)
from srunx.cli._helpers.state_colors import colorize_state
from srunx.cli._helpers.transport_options import LocalOpt, ProfileOpt, QuietOpt
from srunx.common.config import get_config
from srunx.common.exceptions import JobNotFoundError, TransportError
from srunx.common.logging import get_logger
from srunx.domain import (
    Job,
    JobEnvironment,
    JobResource,
    ShellJob,
)
from srunx.observability.notifications.legacy_slack import SlackCallback
from srunx.transport import resolve_transport

logger = get_logger(__name__)


def sbatch(
    ctx: typer.Context,
    script: Annotated[
        Path | None,
        typer.Argument(
            help=(
                "Sbatch script file. Mutually exclusive with --wrap. "
                "Matches the SLURM ``sbatch <script>`` convention."
            ),
        ),
    ] = None,
    wrap: Annotated[
        str | None,
        typer.Option(
            "--wrap",
            help=(
                "Run the supplied command line in the SLURM job. "
                "Equivalent to SLURM's ``sbatch --wrap=...``; mutually "
                "exclusive with the positional script argument."
            ),
        ),
    ] = None,
    profile: ProfileOpt = None,
    local: LocalOpt = False,
    quiet: QuietOpt = False,
    name: Annotated[
        str,
        typer.Option("-J", "--name", "--job-name", help="Job name (sbatch -J)"),
    ] = "job",
    log_dir: Annotated[
        str | None, typer.Option("--log-dir", help="Log directory")
    ] = None,
    work_dir: Annotated[
        str | None,
        typer.Option(
            "-D", "--work-dir", "--chdir", help="Working directory for the job"
        ),
    ] = None,
    # Resource options
    nodes: Annotated[int, typer.Option("-N", "--nodes", help="Number of nodes")] = 1,
    gpus_per_node: Annotated[
        int, typer.Option("--gpus-per-node", help="Number of GPUs per node")
    ] = 0,
    gres: Annotated[
        str | None,
        typer.Option(
            "--gres",
            help=(
                "Generic SLURM resource (sbatch --gres). Currently parses "
                "the ``gpu:N`` form into --gpus-per-node; richer gres "
                "expressions (``gpu:tesla:2`` etc.) are not yet supported."
            ),
        ),
    ] = None,
    ntasks_per_node: Annotated[
        int, typer.Option("--ntasks-per-node", help="Number of tasks per node")
    ] = 1,
    cpus_per_task: Annotated[
        int, typer.Option("-c", "--cpus-per-task", help="Number of CPUs per task")
    ] = 1,
    memory: Annotated[
        str | None,
        typer.Option("--mem", "--memory", help="Memory per node (e.g., '32GB', '1TB')"),
    ] = None,
    time: Annotated[
        str | None,
        typer.Option(
            "-t",
            "--time",
            "--time-limit",
            help="Time limit (e.g., '1:00:00', '30:00', '1-12:00:00')",
        ),
    ] = None,
    nodelist: Annotated[
        str | None,
        typer.Option(
            "-w",
            "--nodelist",
            help="Specific nodes to use (e.g., 'node001,node002')",
        ),
    ] = None,
    partition: Annotated[
        str | None,
        typer.Option(
            "-p", "--partition", help="SLURM partition to use (e.g., 'gpu', 'cpu')"
        ),
    ] = None,
    # Environment options
    conda: Annotated[
        str | None, typer.Option("--conda", help="Conda environment name")
    ] = None,
    venv: Annotated[
        str | None, typer.Option("--venv", help="Virtual environment path")
    ] = None,
    container: Annotated[
        str | None, typer.Option("--container", help="Container image or config")
    ] = None,
    container_runtime: Annotated[
        str | None,
        typer.Option(
            "--container-runtime",
            help="Container runtime: pyxis, apptainer, or singularity",
        ),
    ] = None,
    no_container: Annotated[
        bool,
        typer.Option(
            "--no-container",
            help="Suppress config-default container injection",
        ),
    ] = False,
    env: Annotated[
        list[str] | None,
        typer.Option("--env", help="Environment variables (KEY=VALUE)"),
    ] = None,
    # Job options
    dry_run: Annotated[
        bool,
        typer.Option("--dry-run", help="Show what would be submitted without running"),
    ] = False,
    wait: Annotated[
        bool, typer.Option("--wait", help="Wait for job completion")
    ] = False,
    slack: Annotated[
        bool, typer.Option("--slack", help="Send notifications to Slack")
    ] = False,
    endpoint: Annotated[
        str | None,
        typer.Option(
            "--endpoint",
            help=(
                "Name of a configured notification endpoint (see "
                "`/api/endpoints` / Settings UI). Takes precedence over "
                "--slack when both are set."
            ),
        ),
    ] = None,
    preset: Annotated[
        str | None,
        typer.Option(
            "--preset",
            help=(
                "Subscription preset for --endpoint: terminal (default), "
                "running_and_terminal, all, or digest."
            ),
        ),
    ] = None,
    template: Annotated[
        str | None, typer.Option("--template", help="Custom SLURM script template")
    ] = None,
    sync: Annotated[
        bool | None,
        typer.Option(
            "--sync/--no-sync",
            help=(
                "Rsync the script's enclosing mount before sbatch. Default "
                "comes from ``config.sync.auto`` (true unless explicitly "
                "disabled). ``--no-sync`` submits against the remote's "
                "current state — useful when you manage sync yourself."
            ),
        ),
    ] = None,
    force_sync: Annotated[
        bool,
        typer.Option(
            "--force-sync",
            help=(
                "Bypass the per-machine ownership check and sync this "
                "mount even if another workstation last touched it. Use "
                "after confirming the other machine isn't mid-edit. "
                "Disable the check globally via ``[sync] owner_check = "
                "false`` if your setup is solo-machine."
            ),
        ),
    ] = False,
    verbose: Annotated[
        bool, typer.Option("--verbose", "-v", help="Show verbose output")
    ] = False,
) -> None:
    """Submit a SLURM job (matches the SLURM ``sbatch`` invocation shape).

    Two input modes:

    * ``srunx sbatch script.sh``        — submit a sbatch script (positional)
    * ``srunx sbatch --wrap "cmd ..."`` — wrap a command line into a job

    The two are mutually exclusive, mirroring SLURM's own ``sbatch``
    behaviour. srunx-specific extensions (``--profile`` / ``--conda`` /
    ``--container`` / ``--template`` etc.) layer on top of the standard
    SLURM flags and are surfaced in this command's --help.
    """
    # Positional script vs --wrap are mutually exclusive (matches
    # ``sbatch <script>`` vs ``sbatch --wrap=...`` semantics).
    if script is not None and wrap is not None:
        raise typer.BadParameter(
            "Positional script and --wrap are mutually exclusive.",
            param_hint="<script> / --wrap",
        )
    if script is None and wrap is None:
        raise typer.BadParameter(
            "Missing job source. Provide a script path or use --wrap <command>.",
            param_hint="<script> / --wrap",
        )

    # SLURM ``--gres=gpu:N`` overrides ``--gpus-per-node`` so callers
    # can paste sbatch lines verbatim. Explicit ``--gpus-per-node`` wins
    # only when ``--gres`` is absent (the "no override" case).
    gres_gpus = _parse_gres_gpu(gres)
    if gres_gpus is not None:
        gpus_per_node = gres_gpus

    config = get_config()

    # Use defaults from config if not specified
    if log_dir is None:
        log_dir = config.log_dir
    if work_dir is None:
        work_dir = config.work_dir

    # Parse environment variables
    env_vars = _parse_env_vars(env)

    # Create resources
    resources = JobResource(
        nodes=nodes,
        gpus_per_node=gpus_per_node,
        ntasks_per_node=ntasks_per_node,
        cpus_per_task=cpus_per_task,
        memory_per_node=memory,
        time_limit=time,
        nodelist=nodelist,
        partition=partition,
    )

    # Create environment with explicit handling of defaults
    env_config: dict[str, Any] = {"env_vars": env_vars}
    if conda is not None:
        env_config["conda"] = conda
    if venv is not None:
        env_config["venv"] = venv

    # Resolve container: --no-container suppresses config defaults
    if no_container:
        env_config["container"] = None
    elif container is not None:
        parsed = _parse_container_args(container)
        if parsed is not None:
            if container_runtime is not None:
                # Explicit --container-runtime overrides
                parsed = parsed.model_copy(update={"runtime": container_runtime})
            elif parsed.runtime == "pyxis":
                # No explicit runtime in --container or --container-runtime:
                # apply config default runtime if available (REQ-9 resolution order)
                default_container = config.environment.container
                if (
                    default_container is not None
                    and default_container.runtime != "pyxis"
                ):
                    parsed = parsed.model_copy(
                        update={"runtime": default_container.runtime}
                    )
        env_config["container"] = parsed
    elif container_runtime is not None:
        # No explicit --container, but --container-runtime was given.
        # Override runtime on config-default container if one exists (must have image).
        default_container = config.environment.container
        if default_container is not None and default_container.image:
            container_dict = default_container.model_dump()
            container_dict["runtime"] = container_runtime
            env_config["container"] = container_dict
        # If no default container with image, --container-runtime alone is a no-op
        # (runtime without image is not actionable)

    environment = JobEnvironment.model_validate(env_config)

    job: Job | ShellJob
    if script is not None:
        # ShellJob's schema is intentionally thin: it only records the
        # script path + script_vars. Resource / environment configuration
        # travels with the script itself rather than the model, so we do
        # not forward ``resources`` / ``environment`` / ``log_dir`` /
        # ``work_dir`` here. Those CLI flags are accepted for UX symmetry
        # with --wrap submits but are no-ops under positional script mode.
        shell_data: dict[str, Any] = {
            "name": name,
            "script_path": str(script),
        }
        job = ShellJob.model_validate(shell_data)
    else:
        # ``--wrap`` should match real ``sbatch --wrap``: SLURM wraps
        # the supplied string into ``/bin/sh -c "<cmd>"`` so shell
        # operators (``&&`` / ``|`` / ``>`` / ``;``) are evaluated on
        # the compute node, not on the submitting host. Pass the wrap
        # string as a three-token list ``["bash", "-c", "<cmd>"]``;
        # ``render_job_script`` uses ``shlex.join`` so the rendered
        # template emits ``srun bash -c '<cmd>'`` with the payload
        # safely single-quoted. Closes #138.
        assert wrap is not None  # type narrowing: enforced by mutex above
        job_data: dict[str, Any] = {
            "name": name,
            "command": ["bash", "-c", wrap],
            "resources": resources,
            "environment": environment,
            "log_dir": log_dir,
        }
        if work_dir is not None:
            job_data["work_dir"] = work_dir
        job = Job.model_validate(job_data)

    # Resolve the endpoint for the new watch+subscription pipeline.
    #
    # IMPORTANT: the CLI honours ``--endpoint`` ONLY. We deliberately
    # do NOT consult ``config.notifications.default_endpoint_name`` —
    # that field is a Web UI submit-dialog pre-selection and adopting
    # it here would silently opt users into CLI notifications. (R10)
    effective_endpoint: str | None = endpoint
    effective_preset: str = preset or config.notifications.default_preset

    callbacks: list[Callback]
    if slack:
        # Legacy in-process callback path — kept as a fallback even when
        # --endpoint is also set so users who ask for notifications
        # always get *some* notification pipe. Without this fallback,
        # an attach failure (endpoint missing/disabled/DB error) would
        # silently drop every notification for a run the user explicitly
        # opted into. (R11)
        logger.warning(
            "`--slack` is deprecated; configure an endpoint via "
            "Settings → Notifications and pass `--endpoint <name>`."
        )
        webhook_url = os.getenv("SLACK_WEBHOOK_URL")
        if not webhook_url:
            raise ValueError("SLACK_WEBHOOK_URL is not set")
        callbacks = [SlackCallback(webhook_url=webhook_url)]
    else:
        callbacks = []

    if dry_run:
        console = Console()
        console.print("🔍 Dry run mode - would submit job:")
        console.print(f"  Name: {job.name}")
        if isinstance(job, Job):
            command_str = (
                job.command
                if isinstance(job.command, str)
                else " ".join(job.command or [])
            )
            console.print(f"  Command: {command_str}")
            console.print(f"  Nodes: {job.resources.nodes}")
            console.print(f"  GPUs: {job.resources.gpus_per_node}")
        elif isinstance(job, ShellJob):
            console.print(f"  Script: {job.script_path}")
        # #137 part 2: when the dry run targets an SSH in-place
        # candidate (positional script under a profile mount), also
        # show rsync's preview of what *would* transfer. Lets the user
        # spot a stray ``build/`` or ``.cache/`` they forgot to
        # gitignore before they trigger an actual sync.
        _print_in_place_sync_preview(
            console=console,
            script=script,
            profile_name=profile,
            local=local,
            sync_flag=sync,
            config=config,
        )
        return

    # Submit job through the resolved transport.
    #
    # Local path keeps the richer ``Slurm.submit`` signature (accepts
    # callbacks + template_path + verbose) which the Protocol does not
    # yet expose; SSH path uses the Protocol method, and the adapter
    # owns its own DB recording + callbacks lifecycle.
    # CLI resource flags need to reach ``sbatch`` in IN_PLACE
    # mode — they get baked into the rendered Job.resources for the
    # tmp-upload path, but ShellJob (positional script) on the
    # in-place path has no such render step. Forward only flags the
    # user actually typed on the command line (via Click's
    # ParameterSource); never forward defaults nor config-injected
    # values, so the on-disk ``#SBATCH`` directives stay authoritative
    # for anything the user did not explicitly override.
    from click.core import ParameterSource

    log_dir_user = (
        log_dir
        if ctx.get_parameter_source("log_dir") == ParameterSource.COMMANDLINE
        else None
    )
    extra_sbatch_args = _build_extra_sbatch_args(
        ctx,
        values={
            "nodes": nodes,
            "gpus_per_node": gpus_per_node,
            "ntasks_per_node": ntasks_per_node,
            "cpus_per_task": cpus_per_task,
            "memory": memory,
            "time": time,
            "nodelist": nodelist,
            "partition": partition,
            "work_dir": work_dir,
        },
        log_dir_user=log_dir_user,
    )

    # ``--gres=gpu:N`` was parsed earlier into ``gpus_per_node``; if
    # the user typed ``--gres`` (not ``--gpus-per-node``) we still
    # need to forward the resulting value as ``--gpus-per-node=N``,
    # because ParameterSource for ``gpus_per_node`` shows DEFAULT in
    # that path. Avoid duplication by stripping any earlier entry.
    if (
        ctx.get_parameter_source("gres") == ParameterSource.COMMANDLINE
        and gres is not None
    ):
        extra_sbatch_args = [
            a for a in extra_sbatch_args if not a.startswith("--gpus-per-node")
        ]
        extra_sbatch_args.append(f"--gpus-per-node={gpus_per_node}")

    with resolve_transport(
        profile=profile,
        local=local,
        quiet=quiet,
        callbacks=callbacks,
        submission_source="cli",
    ) as rt:
        client: Any | None
        submitted_job = _submit_via_transport(
            rt=rt,
            job=job,
            script_path=script,
            profile_name=rt.profile_name,
            sync_flag=sync,
            template=template,
            verbose=verbose,
            callbacks=callbacks,
            config=config,
            extra_sbatch_args=extra_sbatch_args,
            force_sync=force_sync,
        )
        client = (
            _slurm_local.Slurm(callbacks=callbacks)
            if rt.transport_type == "local"
            else None
        )

        # Attach a durable notification watch if the user asked for one.
        if effective_endpoint and submitted_job.job_id is not None:
            from srunx.cli._helpers.notification_setup import attach_notification_watch

            attach_notification_watch(
                job_id=int(submitted_job.job_id),
                endpoint_name=effective_endpoint,
                preset=effective_preset,
                scheduler_key=rt.scheduler_key,
            )

        console = Console()
        console.print(
            f"✅ Job submitted successfully: [bold green]{submitted_job.job_id}[/bold green]"
        )
        console.print(f"   Job name: {submitted_job.name}")
        if isinstance(submitted_job, Job) and submitted_job.command:
            command_str = (
                submitted_job.command
                if isinstance(submitted_job.command, str)
                else " ".join(submitted_job.command)
            )
            console.print(f"   Command: {command_str}")
        elif isinstance(submitted_job, ShellJob):
            console.print(f"   Script: {submitted_job.script_path}")

        if wait:
            if client is None:
                # JobOperations does not define a blocking
                # monitor method, so --wait on SSH transports is a
                # no-op until the Protocol grows one (tracked by the
                # SSH monitor wiring follow-up).
                console.print(
                    "⚠️  --wait is not yet supported for SSH transports; "
                    "submitted job continues to run."
                )
            else:
                try:
                    final_job = client.monitor(submitted_job)
                    if final_job.status.name == "COMPLETED":
                        console.print("✅ Job completed successfully")
                    else:
                        console.print(
                            f"❌ Job failed with status: {final_job.status.name}"
                        )
                        raise typer.Exit(code=1)
                except KeyboardInterrupt:
                    console.print("\n⚠️  Monitoring interrupted by user")
                    console.print(
                        f"Job {submitted_job.job_id} is still running in the background"
                    )


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
    ``local.Slurm.queue`` / ``SlurmSSHAdapter.queue`` after the S1
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


def _run_tail_follow_ssh(
    job_ops: Any,
    *,
    job_id: int,
    last: int | None,
    interval: float,
) -> None:
    """Stream incremental log output from an SSH-hosted SLURM job.

    Mirrors the structural pattern of :func:`_run_squeue_live` — ``fetch``
    runs inside a ``try`` that renders transient failures (log file not
    yet created, SSH hiccup) without killing the loop, and
    :class:`KeyboardInterrupt` exits cleanly. Follows ``tail -f``
    convention of polling forever; the user is expected to stop via
    Ctrl+C.

    Log writes go straight to ``sys.stdout`` / ``sys.stderr`` (not
    through Rich) because this is an append-oriented stream, not a
    rewritten snapshot — using :class:`~rich.live.Live` would wipe and
    redraw every tick and destroy the scrollback the user is there to
    read.

    ``last`` applies to the **initial** chunk only; subsequent ticks
    print every new byte. Matches how ``tail -fn N`` works at the OS
    level.
    """
    import time as _time

    offset_out = 0
    offset_err = 0

    # First poll uses ``last_n=last`` so the remote runs ``tail -n N``
    # and only the tail ships over SSH — we never bring a multi-GB log
    # across the wire just to show its last 50 lines. The chunk
    # returns an offset at current EOF; subsequent ticks resume from
    # there.
    try:
        chunk = job_ops.tail_log_incremental(
            job_id, offset_out, offset_err, last_n=last
        )
    except Exception as exc:  # noqa: BLE001 — first poll can race the log file's creation
        typer.secho(
            f"(log not yet available: {exc})",
            err=True,
            fg=typer.colors.BRIGHT_BLACK,
        )
    else:
        if chunk.stdout:
            sys.stdout.write(chunk.stdout)
            sys.stdout.flush()
        if chunk.stderr and chunk.stderr != chunk.stdout:
            sys.stderr.write(chunk.stderr)
            sys.stderr.flush()
        offset_out = chunk.stdout_offset
        offset_err = chunk.stderr_offset

    try:
        while True:
            _time.sleep(interval)
            try:
                chunk = job_ops.tail_log_incremental(job_id, offset_out, offset_err)
            except Exception as exc:  # noqa: BLE001 — keep the tail alive through transient errors
                typer.secho(
                    f"(poll failed, retrying: {exc})",
                    err=True,
                    fg=typer.colors.BRIGHT_BLACK,
                )
                continue
            if chunk.stdout:
                sys.stdout.write(chunk.stdout)
                sys.stdout.flush()
            if chunk.stderr:
                sys.stderr.write(chunk.stderr)
                sys.stderr.flush()
            offset_out = chunk.stdout_offset
            offset_err = chunk.stderr_offset
    except KeyboardInterrupt:
        return


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


# Node state colours for ``srunx sinfo`` — disjoint from the job-state
# colour map in :mod:`srunx.cli._helpers.state_colors`. SLURM uses
# lowercase node states (idle/mixed/allocated/...) that have different
# semantics from the uppercase job states (RUNNING/COMPLETED/...), so
# the two maps intentionally live apart.
_NODE_STATE_COLORS = {
    "idle": "green",
    "mixed": "yellow",
    "mix": "yellow",
    "allocated": "red",
    "alloc": "red",
    "completing": "cyan",
    "drained": "magenta",
    "drain": "magenta",
    "draining": "magenta",
    "down": "bright_red",
    "fail": "bright_red",
    "failing": "bright_red",
    "maint": "bright_black",
    "reserved": "blue",
    "future": "bright_black",
    "unknown": "bright_black",
}


def sinfo(
    partition: Annotated[
        str | None,
        typer.Option("--partition", "-p", help="SLURM partition to query"),
    ] = None,
    format: Annotated[
        str,
        typer.Option("--format", "-f", help="Output format: table or json"),
    ] = "table",
    profile: ProfileOpt = None,
    local: LocalOpt = False,
    quiet: QuietOpt = False,
) -> None:
    """Display partition / node state — same information as native ``sinfo``.

    Columns mirror the default ``sinfo`` layout: ``PARTITION`` (with
    ``*`` on the default partition), ``AVAIL`` (up/down), ``TIMELIMIT``,
    ``NODES``, ``STATE``, ``NODELIST``. For the GPU-aggregate summary
    that used to live here, see ``srunx gpus``.

    With ``--profile <name>`` (or ``$SRUNX_SSH_PROFILE`` / current
    profile) the query runs against the remote cluster via the SSH
    adapter. Local mode shells out to the head-node ``sinfo`` binary.

    Examples:
        srunx sinfo
        srunx sinfo --partition gpu
        srunx sinfo --format json
        srunx sinfo --profile dgx-server --partition gpu
    """
    import json
    from typing import cast

    from srunx.slurm.partitions import (
        PartitionRow,
        fetch_sinfo_rows_local,
        fetch_sinfo_rows_ssh,
    )
    from srunx.transport import resolve_transport

    try:
        with resolve_transport(profile=profile, local=local, quiet=quiet) as rt:
            rows: list[PartitionRow]
            if rt.transport_type == "ssh":
                # Cast Protocol → concrete ``SlurmSSHAdapter`` so we
                # can reuse the adapter-scoped ``_run_slurm_cmd`` path
                # (login-shell env, SLURM PATH, I/O lock). The
                # Protocol deliberately doesn't expose SSH primitives.
                from srunx.slurm.ssh import SlurmSSHAdapter

                adapter = cast(SlurmSSHAdapter, rt.job_ops)
                rows = fetch_sinfo_rows_ssh(adapter, partition)
            else:
                rows = fetch_sinfo_rows_local(partition)

        if format == "json":
            Console().print(json.dumps([row.to_dict() for row in rows], indent=2))
            return

        _render_sinfo_table(rows)

    except Exception as e:
        logger.error(f"Error querying partition info: {e}")
        Console().print(f"[red]Error: {e}[/red]")
        sys.exit(1)


def _render_sinfo_table(rows: list[Any]) -> None:
    """Render :class:`PartitionRow` list as a Rich table.

    The shape matches native ``sinfo`` (same columns, same order) so a
    SLURM user sees familiar output. Styling uses colour on ``STATE``
    to make node health scan-able; no column is dropped or re-ordered.
    """
    table = Table()
    table.add_column("PARTITION", style="cyan")
    table.add_column("AVAIL")
    table.add_column("TIMELIMIT")
    table.add_column("NODES", justify="right")
    table.add_column("STATE")
    table.add_column("NODELIST", overflow="fold")

    for row in rows:
        partition_display = f"{row.partition}*" if row.is_default else row.partition
        avail_color = "green" if row.avail == "up" else "red"
        state_color = _NODE_STATE_COLORS.get(row.state.lower(), "white")
        table.add_row(
            partition_display,
            f"[{avail_color}]{row.avail}[/{avail_color}]",
            row.timelimit,
            str(row.nodes),
            f"[{state_color}]{row.state}[/{state_color}]",
            row.nodelist,
        )

    Console().print(table)


def gpus(
    partition: Annotated[
        str | None,
        typer.Option("--partition", "-p", help="SLURM partition to query"),
    ] = None,
    format: Annotated[
        str,
        typer.Option("--format", "-f", help="Output format: table or json"),
    ] = "table",
    profile: ProfileOpt = None,
    local: LocalOpt = False,
    quiet: QuietOpt = False,
) -> None:
    """Display current GPU resource availability (aggregate snapshot).

    Produces the GPU-focused summary that used to live under
    ``srunx sinfo``. For the native-``sinfo`` partition / state /
    nodelist listing, see ``srunx sinfo``.

    With ``--profile <name>`` (or ``$SRUNX_SSH_PROFILE`` / current
    profile) the query runs against the remote cluster via the SSH
    adapter. Local mode keeps the subprocess ``sinfo`` / ``squeue``
    path.

    Examples:
        srunx gpus
        srunx gpus --partition gpu
        srunx gpus --format json
        srunx gpus --profile dgx-server --partition gpu
    """
    import json
    from typing import cast

    from srunx.observability.monitoring.resource_monitor import ResourceMonitor
    from srunx.observability.monitoring.resource_source import (
        ResourceSource,
        SSHAdapterResourceSource,
    )
    from srunx.transport import resolve_transport

    try:
        with resolve_transport(profile=profile, local=local, quiet=quiet) as rt:
            source: ResourceSource | None = None
            if rt.transport_type == "ssh":
                from srunx.slurm.ssh import SlurmSSHAdapter

                adapter = cast(SlurmSSHAdapter, rt.job_ops)
                source = SSHAdapterResourceSource(lambda: adapter)

            monitor = ResourceMonitor(min_gpus=0, partition=partition, source=source)
            snapshot = monitor.get_partition_resources()

        if format == "json":
            data = {
                "partition": snapshot.partition,
                "gpus_total": snapshot.total_gpus,
                "gpus_in_use": snapshot.gpus_in_use,
                "gpus_available": snapshot.gpus_available,
                "jobs_running": snapshot.jobs_running,
                "nodes_total": snapshot.nodes_total,
                "nodes_idle": snapshot.nodes_idle,
                "nodes_down": snapshot.nodes_down,
            }
            Console().print(json.dumps(data, indent=2))
            return

        table = Table()
        table.add_column("Metric", style="cyan")
        table.add_column("Value", justify="right", style="green")

        table.add_row("Total GPUs", str(snapshot.total_gpus))
        table.add_row("GPUs in Use", str(snapshot.gpus_in_use))
        table.add_row("GPUs Available", str(snapshot.gpus_available))
        table.add_row("", "")
        table.add_row("Running Jobs", str(snapshot.jobs_running))
        table.add_row("", "")
        table.add_row("Total Nodes", str(snapshot.nodes_total))
        table.add_row("Idle Nodes", str(snapshot.nodes_idle))
        table.add_row("Down Nodes", str(snapshot.nodes_down))

        Console().print(table)

    except Exception as e:
        logger.error(f"Error querying GPU resources: {e}")
        Console().print(f"[red]Error: {e}[/red]")
        sys.exit(1)


def tail(
    job_id: Annotated[int, typer.Argument(help="Job ID to show logs for")],
    follow: Annotated[
        bool,
        typer.Option("--follow", "-f", help="Stream logs in real-time (like tail -f)"),
    ] = False,
    interval: Annotated[
        float,
        typer.Option(
            "--interval",
            help=(
                "Seconds between polls when --follow is set over SSH "
                "(local follow inherits Slurm.tail_log's existing cadence). "
                "Default 2s."
            ),
        ),
    ] = 2.0,
    last: Annotated[
        int,
        typer.Option(
            "--last",
            "-n",
            help=(
                "Show only the last N lines (default 10, matches native "
                "``tail``). Use ``--all`` to dump the entire log."
            ),
        ),
    ] = 10,
    show_all: Annotated[
        bool,
        typer.Option(
            "--all",
            help=(
                "Dump the entire log instead of the last ``--last`` lines. "
                "Skips the ``tail -n`` optimization and ``cat``s the whole "
                "file — a multi-GB log will transfer across SSH as-is."
            ),
        ),
    ] = False,
    job_name: Annotated[
        str | None,
        typer.Option("--name", help="Job name for better log file detection"),
    ] = None,
    profile: ProfileOpt = None,
    local: LocalOpt = False,
    quiet: QuietOpt = False,
) -> None:
    """Display job logs with optional real-time streaming.

    Defaults to the last 10 lines of the log (native ``tail``
    convention). Pass ``-n N`` for a different cap, or ``--all`` to
    dump everything. With ``--follow`` / ``-f``, the default also
    applies to the initial frame — subsequent ticks stream only the
    delta regardless.
    """
    if follow and interval <= 0:
        typer.secho(
            "--interval must be a positive number of seconds.",
            err=True,
            fg=typer.colors.RED,
        )
        raise typer.Exit(code=2)
    if last <= 0 and not show_all:
        typer.secho(
            "--last must be a positive integer (or use --all to dump everything).",
            err=True,
            fg=typer.colors.RED,
        )
        raise typer.Exit(code=2)

    # ``--all`` wins over ``--last``: passing ``last_n=None`` to the
    # adapter triggers the legacy ``cat`` full-read path.
    effective_last: int | None = None if show_all else last

    try:
        with resolve_transport(profile=profile, local=local, quiet=quiet) as rt:
            if rt.transport_type == "local":
                # Local keeps the interactive ``Slurm.tail_log`` path for
                # follow + last_n + file-discovery behaviour the Protocol
                # does not yet expose.
                client = _slurm_local.Slurm()
                client.tail_log(
                    job_id=job_id,
                    job_name=job_name,
                    follow=follow,
                    last_n=effective_last,
                )
            elif follow:
                _run_tail_follow_ssh(
                    rt.job_ops,
                    job_id=job_id,
                    last=effective_last,
                    interval=interval,
                )
            else:
                # One-shot SSH read. ``last_n`` flows through so the
                # remote runs ``tail -n N`` — only the tail bytes cross
                # the SSH link. ``--all`` passes ``last_n=None`` which
                # falls back to ``cat`` for a full dump.
                chunk = rt.job_ops.tail_log_incremental(
                    job_id, 0, 0, last_n=effective_last
                )
                if chunk.stdout:
                    sys.stdout.write(chunk.stdout)
                if chunk.stderr and chunk.stderr != chunk.stdout:
                    sys.stderr.write(chunk.stderr)

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
            f"Error retrieving logs for job {job_id}: {e}",
            err=True,
            fg=typer.colors.RED,
        )
        raise typer.Exit(code=1) from e
