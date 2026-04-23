"""Main CLI interface for srunx."""

import os
import sys
import tempfile
from pathlib import Path
from typing import Annotated, Any

import typer
from rich.console import Console
from rich.panel import Panel
from rich.syntax import Syntax
from rich.table import Table

from srunx.callbacks import Callback, SlackCallback
from srunx.cli.transport_options import LocalOpt, ProfileOpt, QuietOpt
from srunx.cli.watch import watch_app
from srunx.client import Slurm
from srunx.config import (
    create_example_config,
    get_config,
    get_config_paths,
)
from srunx.exceptions import JobNotFound, TransportError
from srunx.logging import (
    configure_cli_logging,
    get_logger,
)
from srunx.models import (
    ContainerResource,
    Job,
    JobEnvironment,
    JobResource,
    JobType,
    ShellJob,
    render_job_script,
    render_shell_job_script,
)
from srunx.ssh.cli.commands import ssh_app
from srunx.template import get_template_info, get_template_path, list_templates
from srunx.transport import (
    resolve_transport,
)

logger = get_logger(__name__)


def _submit_via_transport(
    *,
    rt: Any,
    job: Any,
    script_path: Path | None,
    profile_name: str | None,
    sync_flag: bool | None,
    template: str | None,
    verbose: bool,
    callbacks: list[Callback],
    config: Any,
    extra_sbatch_args: list[str] | None = None,
    force_sync: bool = False,
) -> Any:
    """Dispatch a submit to the right adapter method + optional mount sync.

    Local transport keeps the rich ``Slurm.submit`` signature
    (callbacks + template_path + verbose). The SSH transport goes
    through :func:`srunx.cli.submission_plan.plan_sbatch_submission`
    to decide between:

    * IN_PLACE: rsync the owning mount (unless ``--no-sync``),
      translate to the remote path, and invoke
      ``rt.job_ops.submit_remote_sbatch`` — the script stays where
      the user edits it, preserving their own ``#SBATCH`` directives.
      The per-mount sync lock is held across both rsync and sbatch
      so a concurrent CLI invocation can't rsync stale bytes between
      our sync and our submission (Codex blocker #3).
    * TEMP_UPLOAD: fall through to ``rt.job_ops.submit`` which
      uploads a rendered script into ``$SRUNX_TEMP_DIR`` (legacy).

    ``is_rendered_artifact`` is True when the caller forced a template
    render (``--template <name>``): even if the positional script
    happens to sit under a mount, the submitted bytes came from the
    template engine, not the on-disk source, so running "in place"
    would execute the wrong thing.

    ``extra_sbatch_args`` are CLI-side resource flags (``-N`` /
    ``--gres=gpu:N`` / etc.) that need to reach the cluster's
    ``sbatch`` command line in IN_PLACE mode. SLURM treats them as
    overrides of the script's ``#SBATCH`` directives, matching real
    sbatch's precedence. Closes Codex blocker #1: previously these
    flags silently no-op'd in ShellJob (positional-script) mode.
    """
    from srunx.cli.submission_plan import (
        SubmissionMode,
        plan_sbatch_submission,
    )
    from srunx.exceptions import TransportError
    from srunx.models import ShellJob as _ShellJob
    from srunx.sync.lock import SyncLockTimeoutError
    from srunx.sync.service import SyncAbortedError, mount_sync_session

    if rt.transport_type == "local":
        client = Slurm(callbacks=callbacks)
        return client.submit(job, template_path=template, verbose=verbose)

    # --- SSH transport ---
    sub_ctx = rt.submission_context
    effective_sync = config.sync.auto if sync_flag is None else sync_flag
    is_rendered_artifact = template is not None

    from srunx.ssh.core.config import ConfigManager

    profile = ConfigManager().get_profile(profile_name) if profile_name else None
    plan = plan_sbatch_submission(
        script_path=script_path,
        profile=profile,
        cwd=Path.cwd(),
        sync_enabled=effective_sync,
        is_rendered_artifact=is_rendered_artifact,
    )

    for w in plan.warnings:
        logger.warning(w)

    if plan.mode == SubmissionMode.TEMP_UPLOAD:
        return rt.job_ops.submit(job, submission_context=sub_ctx)

    # IN_PLACE branch: hold the per-(profile,mount) lock across the
    # entire sync + sbatch handoff so a concurrent invocation can't
    # rsync different bytes in between.
    assert plan.mount is not None
    assert plan.remote_script_path is not None
    assert profile_name is not None and profile is not None

    if not hasattr(rt.job_ops, "submit_remote_sbatch"):
        raise TransportError(
            "Current transport does not support in-place submission; "
            "re-run with --no-sync to force the legacy tmp-upload path."
        )

    # We split the try/except across the sync phase and the submit
    # phase so a sbatch failure can never wear an "rsync failed"
    # error message. Codex follow-up on PR #134.
    try:
        sync_ctx = mount_sync_session(
            profile_name=profile_name,
            profile=profile,
            mount=plan.mount,
            config=config.sync,
            sync_required=plan.sync_required,
            force_sync=force_sync,
            verbose=verbose,
            # Per-script hash verification (#137 part 5): the local
            # source of truth for the file we're about to ``sbatch``.
            # Gated upstream by ``config.sync.verify_remote_hash``;
            # passing the path unconditionally keeps the CLI ignorant
            # of that flag.
            verify_paths=[str(script_path)] if script_path is not None else None,
        )
        sync_ctx_entered = sync_ctx.__enter__()
    except SyncAbortedError as exc:
        raise typer.BadParameter(str(exc)) from exc
    except SyncLockTimeoutError as exc:
        raise typer.BadParameter(str(exc)) from exc
    except RuntimeError as exc:
        raise typer.BadParameter(f"rsync failed: {exc}") from exc

    try:
        if sync_ctx_entered.performed:
            Console().print(f"⇅  Synced mount [cyan]{plan.mount.name}[/cyan]")
        try:
            submitted = rt.job_ops.submit_remote_sbatch(
                plan.remote_script_path,
                submit_cwd=plan.submit_cwd,
                job_name=job.name,
                extra_sbatch_args=extra_sbatch_args or None,
                callbacks_job=job,
            )
        except RuntimeError as exc:
            # In-place sbatch failure: surface the underlying message
            # verbatim. Distinct from the "rsync failed" wrapper above
            # so users can tell which phase failed.
            raise typer.BadParameter(f"sbatch failed: {exc}") from exc
    finally:
        sync_ctx.__exit__(None, None, None)

    # Re-mutate the original ShellJob so the wait/notification watch
    # path (which reads job_id off the original instance the caller
    # constructed) sees the post-submit state.
    if isinstance(job, _ShellJob):
        job.script_path = plan.remote_script_path
    return submitted


_SBATCH_FLAG_BY_PARAM: dict[str, str] = {
    "nodes": "--nodes",
    "gpus_per_node": "--gpus-per-node",
    "ntasks_per_node": "--ntasks-per-node",
    "cpus_per_task": "--cpus-per-task",
    "memory": "--mem",
    "time": "--time",
    "nodelist": "--nodelist",
    "partition": "--partition",
    "work_dir": "--chdir",
}


def _build_extra_sbatch_args(
    ctx: typer.Context,
    *,
    values: dict[str, object],
    log_dir_user: str | None,
) -> list[str]:
    """Forward CLI-typed flags to ``sbatch`` for ShellJob mode.

    "CLI-typed" means the user wrote the flag on the command line —
    determined via Click's :meth:`Context.get_parameter_source`. We
    deliberately do NOT compare against defaults because that
    confuses three different cases:

    * ``srunx sbatch script.sh`` — no flag typed, planner default 1.
    * ``srunx sbatch script.sh --nodes 1`` — explicit 1, must
      override any ``#SBATCH --nodes=8`` in the script.
    * ``srunx sbatch script.sh`` with config providing ``work_dir``
      — config injected, user did NOT type ``-D``, so the script's
      ``#SBATCH --chdir=`` (if any) wins.

    The default-comparison heuristic the previous version used got
    all three confused — Codex follow-up on PR #134.

    ``log_dir_user`` is passed in separately because the sbatch flag
    expansion (``--output=`` + ``--error=``) builds two args from one
    typed value, and the conversion lives at the call site (caller
    knows the configured default to suppress).
    """
    from click.core import ParameterSource

    args: list[str] = []
    for param_name, sbatch_flag in _SBATCH_FLAG_BY_PARAM.items():
        try:
            source = ctx.get_parameter_source(param_name)
        except (LookupError, AttributeError):
            # No such parameter; defensive against signature drift.
            source = None
        if source != ParameterSource.COMMANDLINE:
            continue
        value = values.get(param_name)
        if value is None or value == "":
            continue
        args.append(f"{sbatch_flag}={value}")

    if log_dir_user:
        # ``--log-dir`` was explicitly typed; expand into the
        # ``--output`` + ``--error`` pair sbatch expects.
        args.append(f"--output={log_dir_user}/%x_%j.log")
        args.append(f"--error={log_dir_user}/%x_%j.log")

    return args


def _parse_gres_gpu(gres: str | None) -> int | None:
    """Parse a sbatch-style ``--gres=gpu:N`` value into an integer GPU count.

    Returns ``None`` for falsy input; raises :class:`typer.BadParameter`
    when the resource type is not ``gpu`` or the count is not a positive
    integer. The intent is to accept the most common SLURM convention
    (``--gres=gpu:N``) so ``srunx sbatch`` reads identically to
    ``sbatch``; richer gres forms (``gpu:tesla:2`` etc.) are out of
    scope for this minimal compatibility layer.
    """
    if not gres:
        return None
    parts = gres.split(":")
    if len(parts) != 2 or parts[0] != "gpu":
        raise typer.BadParameter(
            f"--gres only supports 'gpu:N' form (got {gres!r}).",
            param_hint="--gres",
        )
    try:
        count = int(parts[1])
    except ValueError as exc:
        raise typer.BadParameter(
            f"--gres gpu count must be an integer (got {parts[1]!r}).",
            param_hint="--gres",
        ) from exc
    if count < 0:
        raise typer.BadParameter(
            "--gres gpu count must be non-negative.", param_hint="--gres"
        )
    return count


def _print_in_place_sync_preview(
    *,
    console: Console,
    script: Path | None,
    profile_name: str | None,
    local: bool,
    sync_flag: bool | None,
    config: Any,
) -> None:
    """Show the rsync ``-n -i`` preview for an SSH in-place dry-run.

    Quietly no-ops in every "this isn't an in-place candidate" case
    (local transport, no positional script, no resolvable profile, no
    profile mounts, script not under any mount). Failures from the
    rsync subprocess itself are caught and surfaced as a single
    coloured line — the preview is best-effort and must never abort
    the larger ``--dry-run`` flow.
    """
    if local or script is None:
        return

    from srunx.transport import peek_scheduler_key

    try:
        sched_key = peek_scheduler_key(profile=profile_name, local=local)
    except typer.BadParameter:
        # ``--profile foo --local`` conflict — already surfaced by the
        # main resolution path; nothing more to add here.
        return

    if not sched_key.startswith("ssh:"):
        return

    resolved_profile_name = sched_key[len("ssh:") :]

    from srunx.cli.submission_plan import resolve_mount_for_path
    from srunx.ssh.core.config import ConfigManager

    profile = ConfigManager().get_profile(resolved_profile_name)
    if profile is None or not profile.mounts:
        return

    mount = resolve_mount_for_path(script, profile)
    if mount is None:
        return

    sync_enabled = config.sync.auto if sync_flag is None else sync_flag
    if not sync_enabled:
        console.print(f"  Sync: skipped (--no-sync) for mount '{mount.name}'")
        return

    console.print(f"  Sync preview for mount [cyan]{mount.name}[/cyan]:")
    try:
        from srunx.sync.mount_helpers import sync_mount_by_name

        output = sync_mount_by_name(profile, mount.name, dry_run=True)
    except RuntimeError as exc:
        console.print(f"    [red]rsync preview failed: {exc}[/red]")
        return

    if not output.strip():
        console.print("    (no changes — remote already up to date)")
        return
    for line in output.splitlines():
        console.print(f"    {line}")


class DebugCallback(Callback):
    """Callback to display rendered SLURM scripts in debug mode."""

    def __init__(self):
        self.console = Console()

    def on_job_submitted(self, job: JobType) -> None:
        """Display the rendered SLURM script when a job is submitted."""
        try:
            # Render the script to get the content
            with tempfile.TemporaryDirectory() as temp_dir:
                if isinstance(job, Job):
                    # Debug render: we just need the default template path.
                    # Use ``get_template_path("base")`` instead of constructing a
                    # full ``Slurm()`` instance — this callback fires from
                    # inside the submit pipeline, and ``resolve_transport()``
                    # has already done its job by the time we land here.
                    # Spinning a second Slurm would risk a nested transport
                    # resolution and also bypass the test fixture patch of
                    # ``srunx.cli.main.Slurm``.
                    template_path = get_template_path("base")
                    script_path = render_job_script(
                        template_path, job, temp_dir, verbose=False
                    )
                elif isinstance(job, ShellJob):
                    script_path = render_shell_job_script(
                        job.script_path, job, temp_dir, verbose=False
                    )
                else:
                    logger.warning(f"Unknown job type for debug display: {type(job)}")
                    return

                # Read the rendered script content
                with open(script_path, encoding="utf-8") as f:
                    script_content = f.read()

                # Display the script with rich formatting
                self.console.print(
                    f"\n[bold blue]🔍 Rendered SLURM Script for Job: {job.name}[/bold blue]"
                )

                # Create syntax highlighted panel
                syntax = Syntax(
                    script_content,
                    "bash",
                    theme="monokai",
                    line_numbers=True,
                    background_color="default",
                )

                panel = Panel(
                    syntax,
                    title=f"[bold cyan]{job.name}.slurm[/bold cyan]",
                    border_style="blue",
                    padding=(1, 2),
                )

                self.console.print(panel)
                self.console.print()

        except Exception as e:
            logger.error(f"Failed to render debug script for job {job.name}: {e}")


# Create the main Typer app
app = typer.Typer(
    name="srunx",
    help="Python library for SLURM job management",
    context_settings={"help_option_names": ["-h", "--help"]},
)

# Create subapps
flow_app = typer.Typer(help="Workflow management")
config_app = typer.Typer(help="Configuration management")
template_app = typer.Typer(help="Job template management")


app.add_typer(flow_app, name="flow")
app.add_typer(config_app, name="config")
app.add_typer(watch_app, name="watch")
app.add_typer(ssh_app, name="ssh")
app.add_typer(template_app, name="template")


@app.command()
def ui(
    host: Annotated[str, typer.Option(help="Host to bind")] = "127.0.0.1",
    port: Annotated[int, typer.Option(help="Port to bind")] = 8000,
    verbose: Annotated[
        bool,
        typer.Option("--verbose", "-v", help="Show FastAPI/uvicorn logs"),
    ] = False,
    dev: Annotated[
        bool,
        typer.Option(
            "--dev",
            help="Dev mode: FastAPI --reload + spawn Vite HMR server. Requires a source checkout.",
        ),
    ] = False,
    frontend_port: Annotated[
        int,
        typer.Option(help="Vite dev server port (dev mode only)"),
    ] = 3000,
) -> None:
    """Launch the srunx Web UI."""
    import uvicorn

    from srunx.web.config import get_web_config

    config = get_web_config()
    config.host = host
    config.port = port
    config.verbose = verbose

    # Quiet mode: silence uvicorn access logs and demote srunx loguru to WARNING.
    if not verbose:
        configure_cli_logging(level="WARNING")

    if dev:
        _run_ui_dev(host=host, port=port, frontend_port=frontend_port, verbose=verbose)
        return

    uvicorn.run(
        "srunx.web.app:create_app",
        factory=True,
        host=host,
        port=port,
        log_level="info" if verbose else "warning",
        access_log=verbose,
    )


def _run_ui_dev(*, host: str, port: int, frontend_port: int, verbose: bool) -> None:
    """Launch uvicorn --reload plus Vite dev server in a single foreground process."""
    import shutil
    import signal
    import subprocess

    import uvicorn

    frontend_dir = Path(__file__).resolve().parents[1] / "web" / "frontend"
    if not (frontend_dir / "package.json").is_file():
        console = Console()
        console.print(
            "[red]--dev requires a source checkout; frontend sources not found at "
            f"{frontend_dir}.[/red]\n"
            "Clone the repo and run `uv sync` before using --dev."
        )
        sys.exit(1)
    if shutil.which("npm") is None:
        console = Console()
        console.print(
            "[red]npm not found on PATH; --dev needs Node.js installed.[/red]"
        )
        sys.exit(1)

    vite = subprocess.Popen(
        ["npm", "run", "dev", "--", "--port", str(frontend_port), "--strictPort"],
        cwd=str(frontend_dir),
    )

    console = Console()
    console.print(
        f"[bold cyan]srunx ui --dev[/bold cyan]  "
        f"backend [green]http://{host}:{port}[/green]  "
        f"frontend [green]http://localhost:{frontend_port}[/green] (HMR)"
    )
    console.print("[dim]Open the frontend URL in your browser.[/dim]")

    try:
        uvicorn.run(
            "srunx.web.app:create_app",
            factory=True,
            host=host,
            port=port,
            reload=True,
            reload_dirs=[str(Path(__file__).resolve().parents[1])],
            log_level="info" if verbose else "warning",
            access_log=verbose,
        )
    finally:
        if vite.poll() is None:
            vite.send_signal(signal.SIGTERM)
            try:
                vite.wait(timeout=5)
            except subprocess.TimeoutExpired:
                vite.kill()


def _parse_env_vars(env_var_list: list[str] | None) -> dict[str, str]:
    """Parse environment variables from list of KEY=VALUE strings."""
    if not env_var_list:
        return {}

    env_vars = {}
    for env_str in env_var_list:
        if "=" not in env_str:
            raise ValueError(f"Invalid environment variable format: {env_str}")
        key, value = env_str.split("=", 1)
        env_vars[key] = value
    return env_vars


def _parse_bool(value: str) -> bool:
    """Parse a boolean string value."""
    return value.lower() in ("true", "1", "yes")


def _parse_container_args(container_arg: str | None) -> ContainerResource | None:
    """Parse container argument into ContainerResource.

    Supports simple image path or key=value pairs separated by commas:
      image=<path>, mounts=<m1>;<m2>, bind=<m1>;<m2> (alias for mounts),
      workdir=<path>, runtime=<name>, nv=true, rocm=true, cleanenv=true,
      fakeroot=true, writable_tmpfs=true, overlay=<path>,
      env=KEY1=VAL1;KEY2=VAL2
    """
    if not container_arg:
        return None

    # Simple case: just image path (no commas, no braces, no key=value)
    if not container_arg.startswith("{") and "," not in container_arg:
        # Check if it looks like a bare key=value (e.g. "runtime=apptainer")
        if "=" in container_arg:
            first_key = container_arg.split("=", 1)[0]
            known_keys = {
                "image",
                "mounts",
                "bind",
                "workdir",
                "runtime",
                "nv",
                "rocm",
                "cleanenv",
                "fakeroot",
                "writable_tmpfs",
                "overlay",
                "env",
            }
            if first_key not in known_keys:
                return ContainerResource(image=container_arg)
        else:
            return ContainerResource(image=container_arg)

    # Complex case: parse key=value pairs
    kwargs: dict[str, Any] = {}
    raw = container_arg
    if raw.startswith("{") and raw.endswith("}"):
        raw = raw[1:-1]

    for pair in raw.split(","):
        if "=" not in pair:
            continue
        key, value = pair.strip().split("=", 1)

        match key:
            case "image":
                kwargs["image"] = value
            case "mounts" | "bind":
                kwargs["mounts"] = value.split(";")
            case "workdir":
                kwargs["workdir"] = value
            case "runtime":
                kwargs["runtime"] = value
            case "nv":
                kwargs["nv"] = _parse_bool(value)
            case "rocm":
                kwargs["rocm"] = _parse_bool(value)
            case "cleanenv":
                kwargs["cleanenv"] = _parse_bool(value)
            case "fakeroot":
                kwargs["fakeroot"] = _parse_bool(value)
            case "writable_tmpfs":
                kwargs["writable_tmpfs"] = _parse_bool(value)
            case "overlay":
                kwargs["overlay"] = value
            case "env":
                env_dict: dict[str, str] = {}
                for env_pair in value.split(";"):
                    if "=" in env_pair:
                        ek, ev = env_pair.split("=", 1)
                        env_dict[ek] = ev
                kwargs["env"] = env_dict

    if kwargs:
        return ContainerResource(**kwargs)
    else:
        return ContainerResource(image=container_arg)


@app.command("sbatch")
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
        client: Slurm | None
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
        client = Slurm(callbacks=callbacks) if rt.transport_type == "local" else None

        # Attach a durable notification watch if the user asked for one.
        if effective_endpoint and submitted_job.job_id is not None:
            from srunx.cli.notification_setup import attach_notification_watch

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
                # JobOperationsProtocol does not define a blocking
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


@app.command("squeue")
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
    show_gpus: Annotated[
        bool,
        typer.Option("--show-gpus", "-g", help="Show GPU allocation for each job"),
    ] = False,
    format: Annotated[
        str,
        typer.Option("--format", "-f", help="Output format: table or json"),
    ] = "table",
    profile: ProfileOpt = None,
    local: LocalOpt = False,
    quiet: QuietOpt = False,
) -> None:
    """List user's jobs in the queue.

    Examples:
        srunx squeue
        srunx squeue -j 12345
        srunx squeue --show-gpus
        srunx squeue --format json
        srunx squeue --show-gpus --format json
    """
    import json

    try:
        with resolve_transport(profile=profile, local=local, quiet=quiet) as rt:
            # Local keeps the ``Slurm.queue()`` direct call to preserve
            # existing test fixtures that patch ``srunx.cli.main.Slurm``.
            if rt.transport_type == "local":
                client = Slurm()
                jobs = client.queue()
            else:
                jobs = rt.job_ops.queue()

        # Filter to user-specified job IDs after the queue() call so
        # the dispatch path stays simple; SLURM's own ``squeue -j`` does
        # the same in-memory filtering at the client side.
        if job_filter:
            wanted = {int(j) for j in job_filter}
            jobs = [j for j in jobs if j.job_id in wanted]

        # JSON format output (emit before the "empty queue" banner so
        # --format json stdout stays pure JSON — AC-7.1 / AC-7.2).
        if format == "json":
            job_data = []
            for job in jobs:
                data = {
                    "job_id": job.job_id,
                    "name": job.name,
                    "status": job.status.name if hasattr(job, "status") else "UNKNOWN",
                    "nodes": getattr(getattr(job, "resources", None), "nodes", None),
                    "time_limit": getattr(
                        getattr(job, "resources", None), "time_limit", None
                    ),
                }
                if show_gpus:
                    resources = getattr(job, "resources", None)
                    if resources:
                        total_gpus = resources.nodes * resources.gpus_per_node
                        data["gpus"] = total_gpus
                    else:
                        data["gpus"] = 0
                job_data.append(data)

            console = Console()
            console.print(json.dumps(job_data, indent=2))
            return

        # Empty-queue sentinel only for human-facing table format.
        # Moved past the json branch to fix the pre-existing bug where
        # ``srunx squeue --format json`` on an empty queue emitted the
        # human-readable line instead of ``[]`` (AC-7.1 prerequisite).
        if not jobs:
            console = Console()
            console.print("No jobs in queue")
            return

        # Table format output.
        #
        # Columns read from ``BaseJob`` top-level fields populated by
        # both the local and SSH ``queue()`` implementations (squeue
        # ``%.6D`` / ``%.10M`` / ``%.9l``). Pre-Phase-2 code reached for
        # ``job.resources.nodes`` etc. but ``BaseJob`` has no
        # ``resources`` attribute, so every row came back as ``N/A`` —
        # regardless of transport. Two distinct time columns are shown
        # because users care about both "how long has this been running"
        # (Elapsed) and "when will SLURM kill it" (Limit).
        table = Table(title="Job Queue")
        table.add_column("Job ID", style="cyan")
        table.add_column("Name", style="magenta")
        table.add_column("Status", style="green")
        table.add_column("Nodes", justify="right")
        if show_gpus:
            table.add_column("GPUs", justify="right", style="yellow")
        table.add_column("Elapsed", justify="right")
        table.add_column("Limit", justify="right")

        for job in jobs:
            row = [
                str(job.job_id) if job.job_id else "N/A",
                job.name,
                job.status.name if hasattr(job, "status") else "UNKNOWN",
                str(getattr(job, "nodes", None) or "N/A"),
            ]

            if show_gpus:
                row.append(str(getattr(job, "gpus", 0) or 0))

            row.append(getattr(job, "elapsed_time", None) or "N/A")
            row.append(getattr(job, "time_limit", None) or "N/A")
            table.add_row(*row)

        console = Console()
        console.print(table)

    except TransportError as exc:
        typer.secho(f"Transport error: {exc}", err=True, fg=typer.colors.RED)
        raise typer.Exit(code=1) from None
    except Exception as e:
        typer.secho(f"Error retrieving job queue: {e}", err=True, fg=typer.colors.RED)
        raise typer.Exit(code=1) from e


@app.command("scancel")
def scancel(
    job_id: Annotated[int, typer.Argument(help="Job ID to cancel")],
    profile: ProfileOpt = None,
    local: LocalOpt = False,
    quiet: QuietOpt = False,
) -> None:
    """Cancel a running job."""
    try:
        with resolve_transport(profile=profile, local=local, quiet=quiet) as rt:
            # Local keeps the direct ``Slurm`` call so existing tests that
            # patch ``srunx.cli.main.Slurm`` keep working; SSH goes
            # through the Protocol.
            if rt.transport_type == "local":
                client = Slurm()
                client.cancel(job_id)
            else:
                rt.job_ops.cancel(job_id)

        console = Console()
        console.print(f"✅ Job {job_id} cancelled successfully")

    except JobNotFound:
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


@app.command("sinfo")
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
    """Display current GPU resource availability.

    With ``--profile <name>`` (or ``$SRUNX_SSH_PROFILE`` / current
    profile) the query runs against the remote cluster via the SSH
    adapter. Local mode keeps the legacy ``sinfo`` / ``squeue``
    subprocess path so a head-node user sees no behaviour change.

    Examples:
        srunx sinfo
        srunx sinfo --partition gpu
        srunx sinfo --format json
        srunx sinfo --partition gpu --format json
        srunx sinfo --profile dgx-server --partition gpu
    """
    import json
    from typing import cast

    from srunx.monitor.resource_monitor import ResourceMonitor
    from srunx.monitor.resource_source import ResourceSource, SSHAdapterResourceSource
    from srunx.transport import resolve_transport

    try:
        with resolve_transport(profile=profile, local=local, quiet=quiet) as rt:
            # SSH path delegates to the existing
            # ``SSHAdapterResourceSource`` so cluster-wide vs
            # per-partition dedup, error propagation, and dict→snapshot
            # coercion all match what the resource snapshotter / Web
            # ``/api/resources`` already use. Local stays on the
            # subprocess fallback (source=None) — head-node behaviour
            # is byte-for-byte identical to pre-#139.
            source: ResourceSource | None = None
            if rt.transport_type == "ssh":
                # ``rt.job_ops`` is the live ``SlurmSSHAdapter`` for
                # this profile (see ``_build_ssh_handle``). Cast away
                # the Protocol → concrete narrowing — the Protocol
                # doesn't expose ``get_resources`` /
                # ``get_cluster_snapshot`` because those are SSH-only,
                # and bouncing through a fresh Protocol adds no value
                # over the existing ``SSHAdapterResourceSource``.
                from srunx.web.ssh_adapter import SlurmSSHAdapter

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
            console = Console()
            console.print(json.dumps(data, indent=2))
            return

        partition_name = snapshot.partition or "all partitions"
        table = Table(title=f"GPU Resources - {partition_name}")

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

        console = Console()
        console.print(table)

    except Exception as e:
        logger.error(f"Error querying resources: {e}")
        console = Console()
        console.print(f"[red]Error: {e}[/red]")
        sys.exit(1)


@app.command("tail")
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
                client = Slurm()
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

    except JobNotFound:
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


@flow_app.command("run")
def flow_run(
    yaml_file: Annotated[
        Path, typer.Argument(help="Path to YAML workflow definition file")
    ],
    validate: Annotated[
        bool,
        typer.Option(
            "--validate", help="Only validate the workflow file without executing"
        ),
    ] = False,
    dry_run: Annotated[
        bool,
        typer.Option(
            "--dry-run", help="Show what would be executed without running jobs"
        ),
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
                "`/api/endpoints` / Settings UI). Attaches a watch per "
                "submitted job via the poller pipeline."
            ),
        ),
    ] = None,
    preset: Annotated[
        str | None,
        typer.Option(
            "--preset",
            help=(
                "Subscription preset for --endpoint: terminal (default), "
                "running_and_terminal, or all."
            ),
        ),
    ] = None,
    debug: Annotated[
        bool, typer.Option("--debug", help="Show rendered SLURM scripts for each job")
    ] = False,
    from_job: Annotated[
        str | None,
        typer.Option(
            "--from",
            help="Start execution from this job (ignoring dependencies before this job)",
        ),
    ] = None,
    to_job: Annotated[
        str | None, typer.Option("--to", help="Stop execution at this job (inclusive)")
    ] = None,
    job: Annotated[
        str | None,
        typer.Option(
            "--job", help="Execute only this specific job (ignoring all dependencies)"
        ),
    ] = None,
    arg: Annotated[
        list[str] | None,
        typer.Option("--arg", help="Override args: KEY=VALUE (can repeat)"),
    ] = None,
    sweep: Annotated[
        list[str] | None,
        typer.Option("--sweep", help="Sweep axis values: KEY=v1,v2,v3 (can repeat)"),
    ] = None,
    fail_fast: Annotated[
        bool,
        typer.Option(
            "--fail-fast",
            help="Cancel remaining sweep cells after the first failure",
        ),
    ] = False,
    max_parallel: Annotated[
        int | None,
        typer.Option(
            "--max-parallel",
            help="Maximum concurrent sweep cells (overrides YAML sweep.max_parallel)",
        ),
    ] = None,
    sync: Annotated[
        bool | None,
        typer.Option(
            "--sync/--no-sync",
            help=(
                "Rsync each touched mount once at the start of the run "
                "(default ``[sync] auto``). ``--no-sync`` skips rsync but "
                "still acquires the per-mount lock for race-free submission."
            ),
        ),
    ] = None,
    profile: ProfileOpt = None,
    local: LocalOpt = False,
    quiet: QuietOpt = False,
) -> None:
    """Execute workflow from YAML file."""
    # Delegate to the shared implementation in srunx.cli.workflow which
    # already handles sweep orchestration + args_override. The flags here
    # must stay in sync with that helper's signature.
    from srunx.cli.workflow import _execute_workflow

    _execute_workflow(
        yaml_file=yaml_file,
        validate=validate,
        dry_run=dry_run,
        log_level="INFO",
        slack=slack,
        endpoint=endpoint,
        preset=preset,
        from_job=from_job,
        to_job=to_job,
        job=job,
        arg=arg,
        sweep=sweep,
        fail_fast=fail_fast,
        max_parallel=max_parallel,
        debug=debug,
        profile=profile,
        local=local,
        quiet=quiet,
        sync=sync,
    )


@config_app.command("show")
def config_show() -> None:
    """Show current configuration."""
    config = get_config()

    console = Console()
    table = Table(title="srunx Configuration")
    table.add_column("Section", style="cyan")
    table.add_column("Key", style="magenta")
    table.add_column("Value", style="green")

    # Log directory
    table.add_row("General", "log_dir", str(config.log_dir))
    table.add_row("", "work_dir", str(config.work_dir))

    # Resources
    table.add_row("Resources", "nodes", str(config.resources.nodes))
    table.add_row("", "gpus_per_node", str(config.resources.gpus_per_node))
    table.add_row("", "ntasks_per_node", str(config.resources.ntasks_per_node))
    table.add_row("", "cpus_per_task", str(config.resources.cpus_per_task))
    table.add_row("", "memory_per_node", str(config.resources.memory_per_node))
    table.add_row("", "time_limit", str(config.resources.time_limit))
    table.add_row("", "partition", str(config.resources.partition))

    # Environment
    table.add_row("Environment", "conda", str(config.environment.conda))
    table.add_row("", "venv", str(config.environment.venv))
    table.add_row("", "container", str(config.environment.container))

    console.print(table)


@config_app.command("paths")
def config_paths() -> None:
    """Show configuration file paths."""
    paths = get_config_paths()

    console = Console()
    console.print("Configuration file paths (in order of precedence):")
    for i, path in enumerate(paths, 1):
        status = "✅ exists" if path.exists() else "❌ not found"
        console.print(f"{i}. {path} - {status}")


@config_app.command("init")
def config_init(
    force: Annotated[
        bool, typer.Option("--force", help="Overwrite existing config file")
    ] = False,
) -> None:
    """Initialize configuration file."""
    paths = get_config_paths()
    config_path = paths[0]  # Use the first (highest precedence) path

    if config_path.exists() and not force:
        console = Console()
        console.print(f"Configuration file already exists: {config_path}")
        console.print("Use --force to overwrite")
        return

    try:
        # Create parent directories if they don't exist
        config_path.parent.mkdir(parents=True, exist_ok=True)

        # Write example config
        example_config = create_example_config()
        with open(config_path, "w", encoding="utf-8") as f:
            f.write(example_config)

        console = Console()
        console.print(f"✅ Configuration file created: {config_path}")
        console.print("Edit this file to customize your defaults")

    except Exception as e:
        logger.error(f"Error creating configuration file: {e}")
        sys.exit(1)


@template_app.command("list")
def template_list() -> None:
    """List all available job templates."""
    templates = list_templates()

    console = Console()
    table = Table(title="Available Job Templates")
    table.add_column("Name", style="cyan")
    table.add_column("Description", style="magenta")
    table.add_column("Use Case", style="green")

    for template in templates:
        table.add_row(
            template["name"],
            template["description"],
            template["use_case"],
        )

    console.print(table)


@template_app.command("show")
def template_show(
    name: Annotated[str, typer.Argument(help="Template name")],
) -> None:
    """Show template details and content."""
    try:
        info = get_template_info(name)
        template_path = get_template_path(name)

        console = Console()
        console.print(f"\n[bold cyan]Template: {info['name']}[/bold cyan]")
        console.print(f"[yellow]Description:[/yellow] {info['description']}")
        console.print(f"[yellow]Use Case:[/yellow] {info['use_case']}")
        console.print(f"[yellow]Path:[/yellow] {template_path}\n")

        # Read and display template content
        with open(template_path, encoding="utf-8") as f:
            content = f.read()

        syntax = Syntax(
            content,
            "bash",
            theme="monokai",
            line_numbers=True,
            background_color="default",
        )

        panel = Panel(
            syntax,
            title=f"[bold cyan]{info['name']}.slurm[/bold cyan]",
            border_style="blue",
            padding=(1, 2),
        )

        console.print(panel)

    except ValueError as e:
        logger.error(str(e))
        sys.exit(1)
    except Exception as e:
        logger.error(f"Error showing template: {e}")
        sys.exit(1)


@app.command("sacct")
def sacct(
    job_filter: Annotated[
        list[int] | None,
        typer.Option(
            "-j",
            "--jobs",
            help=(
                "Filter to one or more specific job IDs. Replaces the old "
                "``srunx status <id>`` command for finished jobs — equivalent "
                "to ``sacct -j ID``."
            ),
        ),
    ] = None,
    limit: Annotated[
        int, typer.Option("--limit", "-n", help="Number of jobs to show")
    ] = 50,
    profile: ProfileOpt = None,
    local: LocalOpt = False,
    quiet: QuietOpt = False,
) -> None:
    """Show job execution history.

    With ``--profile <name>`` (or ``$SRUNX_SSH_PROFILE`` / current
    profile), results are filtered to jobs that ran against that
    cluster. Without a transport selector, history from every
    transport is shown — matching legacy behaviour.
    """
    try:
        from srunx.db.cli_helpers import list_recent_jobs
        from srunx.transport import (
            emit_transport_banner,
            peek_scheduler_key,
            resolve_transport_source,
        )

        # sacct is a pure DB query — no SSH connection needed even
        # for SSH profiles. ``peek_scheduler_key`` gives us the WHERE
        # filter without paying the round-trip cost of opening an
        # SSH adapter just to read the local SQLite history.
        source = resolve_transport_source(profile=profile, local=local)
        scheduler_key = peek_scheduler_key(profile=profile, local=local)
        emit_transport_banner(label=scheduler_key, source=source, quiet=quiet)

        # ``-j`` is pushed down into the SQL query so it finds jobs
        # older than ``--limit``. Codex follow-up #2 on PR #134.
        wanted_ids = [int(j) for j in job_filter] if job_filter else None
        jobs = list_recent_jobs(
            limit=limit, job_ids=wanted_ids, scheduler_key=scheduler_key
        )

        if not jobs:
            console = Console()
            console.print("[yellow]No job history found[/yellow]")
            return

        console = Console()
        table = Table(title=f"Job History (Last {len(jobs)} jobs)")
        table.add_column("Job ID", style="cyan")
        table.add_column("Name", style="magenta")
        table.add_column("Status", style="green")
        table.add_column("Submitted", style="yellow")
        table.add_column("Duration", justify="right")
        table.add_column("GPUs", justify="right")

        for job in jobs:
            duration = ""
            if job["duration_seconds"]:
                mins, secs = divmod(int(job["duration_seconds"]), 60)
                hours, mins = divmod(mins, 60)
                if hours > 0:
                    duration = f"{hours}h {mins}m"
                elif mins > 0:
                    duration = f"{mins}m {secs}s"
                else:
                    duration = f"{secs}s"

            submitted_at = job["submitted_at"]
            if submitted_at:
                # Parse and format date
                from datetime import datetime

                dt = datetime.fromisoformat(submitted_at)
                submitted_at = dt.strftime("%Y-%m-%d %H:%M")

            table.add_row(
                str(job["job_id"]),
                job["job_name"],
                job["status"],
                submitted_at,
                duration,
                str(job["gpus_per_node"] or 0),
            )

        console.print(table)

    except Exception as e:
        logger.error(f"Error retrieving job history: {e}")
        sys.exit(1)


@app.command("sreport")
def sreport(
    from_date: Annotated[
        str | None, typer.Option("--from", help="Start date (YYYY-MM-DD)")
    ] = None,
    to_date: Annotated[
        str | None, typer.Option("--to", help="End date (YYYY-MM-DD)")
    ] = None,
    workflow: Annotated[
        str | None, typer.Option("--workflow", help="Workflow name")
    ] = None,
    profile: ProfileOpt = None,
    local: LocalOpt = False,
    quiet: QuietOpt = False,
) -> None:
    """Generate job execution report.

    With ``--profile <name>`` (or ``$SRUNX_SSH_PROFILE`` / current
    profile), aggregates are scoped to jobs that ran on that
    cluster. Without a transport selector, every transport is
    aggregated together — matching legacy behaviour.
    """
    try:
        from srunx.db.cli_helpers import compute_job_stats, compute_workflow_stats
        from srunx.transport import (
            emit_transport_banner,
            peek_scheduler_key,
            resolve_transport_source,
        )

        # sreport is a pure DB query — see ``sacct`` for the rationale
        # behind the ``peek_scheduler_key`` shortcut.
        source = resolve_transport_source(profile=profile, local=local)
        scheduler_key = peek_scheduler_key(profile=profile, local=local)
        emit_transport_banner(label=scheduler_key, source=source, quiet=quiet)

        if workflow:
            stats = compute_workflow_stats(workflow, scheduler_key=scheduler_key)

            console = Console()
            console.print(f"\n[bold cyan]Workflow Report: {workflow}[/bold cyan]")
            console.print(f"Total Jobs: {stats['total_jobs']}")
            if stats["avg_duration_seconds"]:
                mins = int(stats["avg_duration_seconds"] / 60)
                console.print(f"Average Duration: {mins} minutes")
            console.print(f"First Submitted: {stats['first_submitted']}")
            console.print(f"Last Submitted: {stats['last_submitted']}\n")

        else:
            stats = compute_job_stats(
                from_date=from_date,
                to_date=to_date,
                scheduler_key=scheduler_key,
            )

            console = Console()
            console.print("\n[bold cyan]Job Execution Report[/bold cyan]")

            if from_date or to_date:
                date_range = []
                if from_date:
                    date_range.append(f"From: {from_date}")
                if to_date:
                    date_range.append(f"To: {to_date}")
                console.print(f"[yellow]{' | '.join(date_range)}[/yellow]\n")

            # Summary table
            summary_table = Table(title="Summary")
            summary_table.add_column("Metric", style="cyan")
            summary_table.add_column("Value", style="green", justify="right")

            summary_table.add_row("Total Jobs", str(stats["total_jobs"]))

            if stats["avg_duration_seconds"]:
                mins = int(stats["avg_duration_seconds"] / 60)
                summary_table.add_row("Average Duration", f"{mins} minutes")

            summary_table.add_row(
                "Total GPU Hours", f"{stats['total_gpu_hours']:.1f} hours"
            )

            console.print(summary_table)

            # Status breakdown
            if stats["jobs_by_status"]:
                console.print()
                status_table = Table(title="Jobs by Status")
                status_table.add_column("Status", style="cyan")
                status_table.add_column("Count", style="green", justify="right")

                for status, count in stats["jobs_by_status"].items():
                    status_table.add_row(status, str(count))

                console.print(status_table)

            console.print()

    except Exception as e:
        logger.error(f"Error generating report: {e}")
        sys.exit(1)


def main() -> None:
    """Main entry point for the CLI."""
    # Configure logging with defaults
    configure_cli_logging(level="INFO", quiet=False)

    # Run the app
    app()


if __name__ == "__main__":
    main()
