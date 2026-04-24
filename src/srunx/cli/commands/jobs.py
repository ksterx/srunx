"""Job-oriented CLI commands: sbatch, squeue, scancel, sinfo, gpus, tail."""

import os
import sys
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

    For finished jobs, see ``srunx history``.

    Examples:
        srunx squeue
        srunx squeue -j 12345
        srunx squeue --user alice
        srunx squeue -a
        srunx squeue --show-partition --show-cpus
        srunx squeue --format json
    """
    import json

    try:
        with resolve_transport(profile=profile, local=local, quiet=quiet) as rt:
            if rt.transport_type == "local":
                client = _slurm_local.Slurm()
                jobs = client.queue(user=user)
            else:
                jobs = rt.job_ops.queue(user=user)

        # Filter to user-specified job IDs after the queue() call so
        # the dispatch path stays simple; SLURM's own ``squeue -j`` does
        # the same in-memory filtering at the client side.
        if job_filter:
            wanted = {int(j) for j in job_filter}
            jobs = [j for j in jobs if j.job_id in wanted]

        # JSON format always emits every field (scripts pick what
        # they consume — column-hiding would just force callers to
        # reconstruct the full shape from multiple calls).
        if format == "json":
            job_data = [
                {
                    "job_id": job.job_id,
                    "user": getattr(job, "user", None),
                    "name": job.name,
                    "partition": getattr(job, "partition", None),
                    "status": (
                        job.status.name if hasattr(job, "status") else "UNKNOWN"
                    ),
                    "nodes": getattr(job, "nodes", None),
                    "cpus": getattr(job, "cpus", None),
                    "gpus": getattr(job, "gpus", None),
                    "nodelist": getattr(job, "nodelist", None),
                    "elapsed_time": getattr(job, "elapsed_time", None),
                    "time_limit": getattr(job, "time_limit", None),
                }
                for job in jobs
            ]
            Console().print(json.dumps(job_data, indent=2))
            return

        # Empty-queue sentinel only for human-facing table format.
        if not jobs:
            Console().print("No jobs in queue")
            return

        # Column visibility. The four SLURM fields the user flagged as
        # "information density, not always needed" are hidden by
        # default; flags (or ``-a``) surface them. Keeping GPUs /
        # NodeList / User in the default set because those are what
        # disambiguate jobs in a multi-user queue — the original
        # complaint that triggered this redesign.
        show_partition_col = show_partition or show_all
        show_cpus_col = show_cpus or show_all
        show_limit_col = show_limit or show_all
        show_nodes_col = show_nodes or show_all

        table = Table()
        table.add_column("Job ID", style="cyan")
        table.add_column("User")
        table.add_column("Name", style="magenta", overflow="fold")
        if show_partition_col:
            table.add_column("Partition")
        table.add_column("Status")
        if show_nodes_col:
            table.add_column("Nodes", justify="right")
        if show_cpus_col:
            table.add_column("CPUs", justify="right")
        table.add_column("GPUs", justify="right", style="yellow")
        table.add_column("Elapsed", justify="right")
        if show_limit_col:
            table.add_column("Limit", justify="right")
        table.add_column("NodeList", overflow="fold")

        for job in jobs:
            status_name = job.status.name if hasattr(job, "status") else "UNKNOWN"
            row: list[str] = [
                str(job.job_id) if job.job_id else "N/A",
                getattr(job, "user", None) or "N/A",
                job.name,
            ]
            if show_partition_col:
                row.append(getattr(job, "partition", None) or "N/A")
            row.append(colorize_state(status_name))
            if show_nodes_col:
                row.append(str(getattr(job, "nodes", None) or "N/A"))
            if show_cpus_col:
                row.append(str(getattr(job, "cpus", None) or 0))
            row.append(str(getattr(job, "gpus", None) or 0))
            row.append(getattr(job, "elapsed_time", None) or "N/A")
            if show_limit_col:
                row.append(getattr(job, "time_limit", None) or "N/A")
            row.append(getattr(job, "nodelist", None) or "N/A")
            table.add_row(*row)

        Console().print(table)

    except TransportError as exc:
        typer.secho(f"Transport error: {exc}", err=True, fg=typer.colors.RED)
        raise typer.Exit(code=1) from None
    except Exception as e:
        typer.secho(f"Error retrieving job queue: {e}", err=True, fg=typer.colors.RED)
        raise typer.Exit(code=1) from e


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


_STATE_COLORS = {
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
        state_color = _STATE_COLORS.get(row.state.lower(), "white")
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
    last: Annotated[
        int | None, typer.Option("--last", "-n", help="Show only the last N lines")
    ] = None,
    job_name: Annotated[
        str | None,
        typer.Option("--name", help="Job name for better log file detection"),
    ] = None,
    profile: ProfileOpt = None,
    local: LocalOpt = False,
    quiet: QuietOpt = False,
) -> None:
    """Display job logs with optional real-time streaming."""
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
                    last_n=last,
                )
            else:
                # SSH path: use the pure Protocol tail_log_incremental
                # once for non-follow retrieval. --follow over SSH is a
                # Phase 5b+ concern (loop belongs in the CLI layer, but
                # the SSH adapter does not yet stream logs).
                chunk = rt.job_ops.tail_log_incremental(job_id, 0, 0)
                # ``--last N`` is applied client-side on the SSH path:
                # the adapter returns the full log content and we slice
                # here so the flag isn't silently dropped (SF6). The
                # local path above already honours ``last_n`` via
                # ``Slurm.tail_log``.
                stdout_text = chunk.stdout or ""
                stderr_text = chunk.stderr or ""
                if last is not None:
                    if stdout_text:
                        stdout_text = "\n".join(stdout_text.splitlines()[-last:])
                        # Preserve the trailing newline if the original
                        # chunk ended with one so terminal output stays
                        # unambiguous.
                        if chunk.stdout and chunk.stdout.endswith("\n"):
                            stdout_text += "\n"
                    if stderr_text and stderr_text != chunk.stdout:
                        stderr_text = "\n".join(stderr_text.splitlines()[-last:])
                        if chunk.stderr and chunk.stderr.endswith("\n"):
                            stderr_text += "\n"
                if stdout_text:
                    sys.stdout.write(stdout_text)
                if stderr_text and stderr_text != (chunk.stdout or ""):
                    sys.stderr.write(stderr_text)
                if follow:
                    typer.secho(
                        "--follow is not yet supported for SSH transports.",
                        err=True,
                        fg=typer.colors.YELLOW,
                    )

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
