"""``srunx sbatch`` — submit a SLURM job (script or --wrap)."""

import os
from pathlib import Path
from typing import Annotated, Any

import typer
from rich.console import Console
from rich.markup import escape

import srunx.slurm.local as _slurm_local  # noqa: E402,I001 — kept so ``patch("srunx.slurm.local.Slurm")`` intercepts all call sites
from srunx.callbacks import Callback
from srunx.cli._helpers.click_compat import is_commandline
from srunx.cli._helpers.sbatch_helpers import (
    _build_extra_sbatch_args,
    _parse_container_args,
    _parse_env_vars,
    _print_in_place_sync_preview,
    _resolve_job_name,
    _submit_via_transport,
)
from srunx.cli._helpers.sbatch_passthrough import (
    SBATCH_ARG_OPT,
    own_option_specs,
    validate_passthrough_args,
)
from srunx.cli._helpers.sbatch_resolve import (
    has_clusters_option,
    merge_script_defaults,
    parse_directives,
)
from srunx.cli._helpers.snapshot_submit import (
    build_snapshot_plan,
    print_snapshot_dry_run,
    reject_clusters_in_script,
    require_ssh_transport,
    validate_snapshot_options,
)
from srunx.cli._helpers.transport import resolve_cluster_name, resolve_transport
from srunx.cli._helpers.transport_options import ClusterOpt, QuietOpt
from srunx.common.config import (
    ConfigError,
    activate_config,
    apply_unset,
    load_settings,
    use_project,
)
from srunx.common.logging import get_logger
from srunx.domain import (
    Job,
    JobEnvironment,
    JobResource,
    ShellJob,
)
from srunx.observability.notifications.legacy_slack import SlackCallback

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
    cluster: ClusterOpt = None,
    project: Annotated[
        Path | None,
        typer.Option(
            "--project",
            help="Project root (directory holding .srunx/config.toml). "
            "Default: discovered from the current directory.",
        ),
    ] = None,
    unset: Annotated[
        list[str] | None,
        typer.Option(
            "--unset",
            help="Drop an inherited config default (e.g. --unset partition). "
            "Repeatable.",
        ),
    ] = None,
    yes: Annotated[
        bool,
        typer.Option("--yes", "-y", help="Approve a first sync to a new remote path"),
    ] = False,
    quiet: QuietOpt = False,
    name: Annotated[
        str,
        typer.Option("-J", "--job-name", help="Job name (sbatch -J)"),
    ] = "job",
    log_dir: Annotated[
        str | None, typer.Option("--log-dir", help="Log directory")
    ] = None,
    work_dir: Annotated[
        str | None,
        typer.Option("-D", "--chdir", help="Working directory for the job"),
    ] = None,
    # Resource options
    nodes: Annotated[
        int | None, typer.Option("-N", "--nodes", help="Number of nodes")
    ] = None,
    gpus_per_node: Annotated[
        int | None, typer.Option("--gpus-per-node", help="Number of GPUs per node")
    ] = None,
    gres: Annotated[
        str | None,
        typer.Option(
            "--gres",
            help="Generic SLURM resource (sbatch --gres), passed through verbatim.",
        ),
    ] = None,
    ntasks_per_node: Annotated[
        int | None,
        typer.Option("--ntasks-per-node", help="Number of tasks per node"),
    ] = None,
    cpus_per_task: Annotated[
        int | None,
        typer.Option("-c", "--cpus-per-task", help="Number of CPUs per task"),
    ] = None,
    memory: Annotated[
        str | None,
        typer.Option("--mem", help="Memory per node (e.g., '32GB', '1TB')"),
    ] = None,
    time: Annotated[
        str | None,
        typer.Option(
            "-t",
            "--time",
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
    snapshot: Annotated[
        bool,
        typer.Option(
            "--snapshot",
            help=(
                "Submit from a read-only per-commit snapshot of the current git "
                "repo on the cluster (SSH clusters only; HEAD unless "
                "--snapshot-rev). The job's default cwd is the snapshot; "
                "SRUNX_CODE_DIR / SRUNX_SNAPSHOT_SHA are set. No auto-sync."
            ),
        ),
    ] = False,
    snapshot_rev: Annotated[
        str | None,
        typer.Option(
            "--snapshot-rev",
            metavar="REV",
            help="Snapshot this git revision instead of HEAD (implies --snapshot).",
        ),
    ] = None,
    verbose: Annotated[
        bool, typer.Option("--verbose", "-v", help="Show verbose output")
    ] = False,
    sbatch_arg: Annotated[
        list[str] | None,
        typer.Option(
            SBATCH_ARG_OPT,
            help=(
                "Pass one raw sbatch option token through verbatim "
                "(repeatable). Escape hatch for sbatch options srunx does "
                "not model directly; most native SLURM spellings "
                "(--array, --qos, -d ...) are recognized and routed here "
                "automatically without needing this flag."
            ),
        ),
    ] = None,
) -> None:
    """Submit a SLURM job (matches the SLURM ``sbatch`` invocation shape).

    Two input modes:

    * ``srunx sbatch script.sh``        — submit a sbatch script (positional)
    * ``srunx sbatch --wrap "cmd ..."`` — wrap a command line into a job

    The two are mutually exclusive, mirroring SLURM's own ``sbatch``
    behaviour. srunx-specific extensions (``--cluster`` / ``--conda`` /
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

    try:
        with use_project(project):
            _sbatch_in_project(**locals())
    except ConfigError as exc:
        raise typer.BadParameter(str(exc), param_hint="--cluster / config") from exc


def _sbatch_in_project(
    *,
    ctx: typer.Context,
    script: Path | None,
    wrap: str | None,
    cluster: str | None,
    project: Path | None,
    unset: list[str] | None,
    yes: bool,
    quiet: bool,
    name: str,
    log_dir: str | None,
    work_dir: str | None,
    nodes: int | None,
    gpus_per_node: int | None,
    gres: str | None,
    ntasks_per_node: int | None,
    cpus_per_task: int | None,
    memory: str | None,
    time: str | None,
    nodelist: str | None,
    partition: str | None,
    conda: str | None,
    venv: str | None,
    container: str | None,
    container_runtime: str | None,
    no_container: bool,
    env: list[str] | None,
    dry_run: bool,
    wait: bool,
    slack: bool,
    endpoint: str | None,
    preset: str | None,
    template: str | None,
    sync: bool | None,
    force_sync: bool,
    snapshot: bool,
    snapshot_rev: str | None,
    verbose: bool,
    sbatch_arg: list[str] | None,
) -> None:
    # Cluster + effective defaults for this (project, cluster) context.
    settings = load_settings(reload=True)
    cluster_name, _transport = resolve_cluster_name(cluster=cluster)
    _, cluster_origin = settings.cluster_source(cluster)
    config = apply_unset(settings.effective(cluster_name), unset or [])

    with activate_config(config):
        _sbatch_with_config(
            ctx=ctx,
            config=config,
            cluster_name=cluster_name,
            cluster_origin=cluster_origin,
            explicit_cluster=cluster,
            script=script,
            wrap=wrap,
            yes=yes,
            quiet=quiet,
            name=name,
            log_dir=log_dir,
            work_dir=work_dir,
            nodes=nodes,
            gpus_per_node=gpus_per_node,
            gres=gres,
            ntasks_per_node=ntasks_per_node,
            cpus_per_task=cpus_per_task,
            memory=memory,
            time=time,
            nodelist=nodelist,
            partition=partition,
            conda=conda,
            venv=venv,
            container=container,
            container_runtime=container_runtime,
            no_container=no_container,
            env=env,
            dry_run=dry_run,
            wait=wait,
            slack=slack,
            endpoint=endpoint,
            preset=preset,
            template=template,
            sync=sync,
            force_sync=force_sync,
            snapshot=snapshot,
            snapshot_rev=snapshot_rev,
            verbose=verbose,
            sbatch_arg=sbatch_arg,
        )


def _sbatch_with_config(
    *,
    ctx: typer.Context,
    config: Any,
    cluster_name: str,
    cluster_origin: str,
    explicit_cluster: str | None,
    script: Path | None,
    wrap: str | None,
    yes: bool,
    quiet: bool,
    name: str,
    log_dir: str | None,
    work_dir: str | None,
    nodes: int | None,
    gpus_per_node: int | None,
    gres: str | None,
    ntasks_per_node: int | None,
    cpus_per_task: int | None,
    memory: str | None,
    time: str | None,
    nodelist: str | None,
    partition: str | None,
    conda: str | None,
    venv: str | None,
    container: str | None,
    container_runtime: str | None,
    no_container: bool,
    env: list[str] | None,
    dry_run: bool,
    wait: bool,
    slack: bool,
    endpoint: str | None,
    preset: str | None,
    template: str | None,
    sync: bool | None,
    force_sync: bool,
    snapshot: bool,
    snapshot_rev: str | None,
    verbose: bool,
    sbatch_arg: list[str] | None,
) -> None:
    cluster = cluster_name
    # --snapshot / --snapshot-rev: reject conflicting options (exit 2), then
    # validate the local repo before any remote call.
    snapshot_mode = validate_snapshot_options(
        snapshot=snapshot,
        snapshot_rev=snapshot_rev,
        script=script,
        wrap=wrap,
        template=template,
        sync=sync,
        force_sync=force_sync,
        sbatch_args=sbatch_arg or [],
    )
    snapshot_plan = None
    snapshot_profile_name: str | None = None
    if snapshot_mode:
        snapshot_profile_name = require_ssh_transport(cluster=cluster)
        snapshot_plan = build_snapshot_plan(
            script=script, rev=snapshot_rev, cwd=Path.cwd()
        )
        reject_clusters_in_script(snapshot_plan.script_text)

    # Validate the final --sbatch-arg list (format R2.6 + reject list
    # R2.5) once, here, before any I/O. Tokens may have arrived either
    # hand-typed or normalized by SbatchCommand.parse_args's native-option
    # rewrite (main.py registers ``sbatch`` with ``cls=SbatchCommand``).
    passthrough = validate_passthrough_args(
        sbatch_arg or [], own_option_specs(ctx.command.get_params(ctx))
    )
    # Slurm's --clusters/-M would send the job somewhere other than the
    # cluster srunx records it under (design §6).
    if has_clusters_option(passthrough):
        raise typer.BadParameter(
            "--clusters/-M is not supported: choose the target with --cluster.",
            param_hint="--clusters",
        )

    script_text: str | None = None
    directives: dict[str, str] = {}
    if script is not None:
        if snapshot_plan is not None:
            script_text = snapshot_plan.script_text
        else:
            try:
                script_text = script.read_text(encoding="utf-8", errors="replace")
            except OSError:
                script_text = None
        directives = parse_directives(script_text or "")
        from srunx.cli._helpers.sbatch_helpers import script_sbatch_tokens

        raw_tokens = [
            t for line in script_sbatch_tokens(script_text or "") for t in line
        ]
        if "--clusters" in directives or has_clusters_option(raw_tokens):
            raise typer.BadParameter(
                "The script's #SBATCH --clusters/-M is not supported: choose "
                "the target with --cluster.",
                param_hint="--clusters",
            )

    # Parse environment variables
    env_vars = _parse_env_vars(env)

    def typed(param: str) -> bool:
        return is_commandline(ctx.get_parameter_source(param))

    # Only values the user typed go into the models; everything else is
    # filled from the activated config by the domain models.
    resource_values = {
        "nodes": nodes,
        "gpus_per_node": gpus_per_node,
        "ntasks_per_node": ntasks_per_node,
        "cpus_per_task": cpus_per_task,
        "memory_per_node": memory,
        "time_limit": time,
        "nodelist": nodelist,
        "partition": partition,
    }
    param_names = {"memory_per_node": "memory", "time_limit": "time"}
    resource_data = {
        k: v
        for k, v in resource_values.items()
        if v is not None and typed(param_names.get(k, k))
    }
    # ``--gres`` decides GPUs itself: drop an inherited GPU default rather
    # than requesting both (the GRES string is passed through untouched).
    if gres is not None and "gpus_per_node" not in resource_data:
        resource_data["gpus_per_node"] = 0
    resources = JobResource.model_validate(resource_data)

    # Create environment with explicit handling of defaults
    env_config: dict[str, Any] = {}
    # $SRUNX_CODE_DIR: the project's code location on the target, so a script
    # can use the same paths from a mount or a snapshot (snapshot submission
    # overrides it with the snapshot path). srunx's value wins over --env.
    from srunx.cli._helpers.sbatch_resolve import CODE_DIR_ENV, code_dir_for

    code_dir = None if snapshot_plan is not None else code_dir_for(cluster_name, script)
    if env_vars or code_dir:
        env_config["env_vars"] = {
            **config.environment.env_vars,
            **env_vars,
            **({CODE_DIR_ENV: code_dir} if code_dir else {}),
        }
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

    # Did the user actually type ``-J`` / ``--name``? The option defaults
    # to "job"; only an explicit value may override a script's own
    # ``#SBATCH --job-name``. Same ParameterSource pattern as the resource
    # flags below.
    name_explicit = is_commandline(ctx.get_parameter_source("name"))

    # ``inject_job_name`` = should ``--job-name`` go on the sbatch command
    # line? Default True; only a positional script that declares its own
    # ``#SBATCH --job-name`` (and no explicit ``-J``) suppresses it, so the
    # script's directive wins. ``--wrap`` always injects — it has no
    # user-authored directive to protect, and suppressing would let a stray
    # ``SBATCH_JOB_NAME`` in the environment rename the job out from under
    # the name srunx records.
    inject_job_name = True

    job: Job | ShellJob
    if script is not None:
        # ShellJob's schema is intentionally thin: it records the script
        # path + script_vars. Resource configuration (``resources`` /
        # ``log_dir`` / ``work_dir``) still travels with the script itself
        # rather than the model, so those CLI flags remain no-ops under
        # positional script mode. ``environment`` IS forwarded: env_vars
        # propagate via the submission environment (sbatch process env +
        # --export=ALL locally / remote export prefix + --export=ALL over
        # SSH), so --env is effective for positional scripts identically to
        # --wrap.
        #
        # ``name`` is resolved the way SLURM will name the job (explicit
        # -J > script's #SBATCH --job-name > script basename) so srunx's
        # display / history match the scheduler without a post-submit
        # query; ``inject_job_name`` rides along from the same resolution.
        effective_name, inject_job_name = _resolve_job_name(
            script,
            name,
            cli_name_explicit=name_explicit,
            script_text=(snapshot_plan.script_text if snapshot_plan else None),
        )
        shell_data: dict[str, Any] = {
            "name": effective_name,
            "script_path": str(script),
            "environment": environment,
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
        }
        if log_dir is not None:
            job_data["log_dir"] = log_dir
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
    log_dir_user = log_dir if typed("log_dir") else None

    # * Positional script: forward what the user typed, then config
    #   defaults the script itself does not set (design §6), then the
    #   passthrough tokens last (so passthrough wins ties per R5).
    # * --wrap: the rendered template already carries every resource
    #   from ``job.resources`` (config defaults included); only the
    #   passthrough tokens belong on the command line.
    merge = None
    if script is not None:
        typed_args = _build_extra_sbatch_args(
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
        if gres is not None:
            typed_args.append(f"--gres={gres}")
        explicit_options = {a.split("=", 1)[0] for a in typed_args}
        explicit_options |= {
            t.split("=", 1)[0] for t in passthrough if t.startswith("--")
        }
        # Short passthrough spellings (``-p x``) count too.
        from srunx.cli._helpers.sbatch_resolve import DIRECTIVES

        short_to_long = {v: k for k, v in DIRECTIVES.items() if v}
        explicit_options |= {
            short_to_long[t[:2]] for t in passthrough if t[:2] in short_to_long
        }
        merge = merge_script_defaults(
            config, directives, explicit_options=explicit_options
        )
        for w in merge.warnings:
            logger.warning(w)
        inherited = merge.args
        if load_settings().cluster(cluster_name).transport == "ssh":
            from srunx.cli._helpers.sbatch_resolve import translate_local_paths
            from srunx.ssh.core.config import ConfigManager

            profile = ConfigManager().get_profile(cluster_name)
            if profile is not None and profile.mounts:
                inherited = translate_local_paths(inherited, profile.mounts)
        extra_sbatch_args = inherited + typed_args + list(passthrough)
    else:
        extra_sbatch_args = list(passthrough)
        if gres is not None:
            extra_sbatch_args.insert(0, f"--gres={gres}")

    if dry_run:
        console = Console()
        console.print("🔍 Dry run mode - would submit job:")
        console.print(
            f"  Cluster: [cyan]{cluster_name}[/cyan]"
            + (f"  [dim]({escape(cluster_origin)})[/dim]" if cluster_origin else "")
        )
        if config.project_root:
            console.print(f"  Project: {config.project_root}")
        if code_dir:
            console.print(f"  SRUNX_CODE_DIR: {code_dir}")
        console.print(f"  Name: {job.name}")
        if isinstance(job, Job):
            command_str = (
                job.command
                if isinstance(job.command, str)
                else " ".join(job.command or [])
            )
            console.print(f"  Command: {command_str}")
            for field_name, value in job.resources.model_dump().items():
                source = config.sources.get(f"resources.{field_name}")
                if typed(param_names.get(field_name, field_name)):
                    source = "command line"
                if value is None:
                    continue
                console.print(
                    f"  {field_name}: {value}"
                    + (f"  [dim]({escape(source)})[/dim]" if source else "")
                )
        elif isinstance(job, ShellJob):
            console.print(f"  Script: {job.script_path}")
            for option, value in directives.items():
                console.print(f"  {option}={value}  [dim](#SBATCH)[/dim]")
            if merge is not None:
                for option, (value, source) in merge.forwarded.items():
                    console.print(f"  {option}={value}  [dim]({escape(source)})[/dim]")
                for w in merge.warnings:
                    console.print(f"  [yellow]warning: {escape(w)}[/yellow]")
        if extra_sbatch_args:
            console.print(f"  sbatch arguments: {' '.join(extra_sbatch_args)}")
        # #137 part 2: when the dry run targets an SSH in-place
        # candidate (positional script under a mount), also show rsync's
        # preview of what *would* transfer.
        if snapshot_plan is not None:
            print_snapshot_dry_run(
                console,
                snapshot_plan,
                profile_name=snapshot_profile_name,
                extra_sbatch_args=extra_sbatch_args,
            )
        else:
            _print_in_place_sync_preview(
                console=console,
                script=script,
                cluster=cluster_name,
                sync_flag=sync,
                config=config,
            )
        return

    # The explicit flag (not the resolved name) so the banner reports
    # where the selection came from.
    with resolve_transport(
        cluster=explicit_cluster,
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
            inject_job_name=inject_job_name,
            snapshot=snapshot_plan,
            assume_yes=yes,
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
