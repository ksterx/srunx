"""CLI interface for workflow management."""

import os
import signal
import sys
from pathlib import Path
from typing import Annotated, Any

import typer
import yaml  # type: ignore
from rich.console import Console

from srunx.callbacks import Callback, NotificationWatchCallback, SlackCallback
from srunx.cli.transport_options import LocalOpt, ProfileOpt, QuietOpt
from srunx.config import get_config
from srunx.exceptions import WorkflowValidationError
from srunx.logging import configure_workflow_logging, get_logger
from srunx.models import Job, ShellJob, Workflow
from srunx.runner import WorkflowRunner
from srunx.security import find_shell_script_violation
from srunx.sweep import SweepSpec
from srunx.sweep.expand import (
    merge_sweep_specs,
    parse_arg_flags,
    parse_sweep_flags,
)
from srunx.sweep.orchestrator import SweepOrchestrator
from srunx.transport import (
    ResolvedTransport,
    peek_scheduler_key,
    resolve_transport,
)


def _build_workflow_callbacks(
    *,
    endpoint: str | None,
    effective_preset: str,
    is_sweep: bool,
    slack: bool,
    debug: bool,
    scheduler_key: str,
) -> list[Callback]:
    """Assemble the callback list for a CLI workflow invocation.

    ``NotificationWatchCallback`` is omitted for sweep runs because the
    orchestrator manages a sweep-level watch + subscription; attaching a
    per-job callback there would spam one notification per cell. The
    ``scheduler_key`` is threaded in so the watch the callback creates
    targets the transport the workflow will actually submit against.

    ``SlackCallback`` is legacy in-process delivery and is still attached
    in both modes for backward compatibility.
    """
    callbacks: list[Callback] = []
    if endpoint and not is_sweep:
        callbacks.append(
            NotificationWatchCallback(
                endpoint_name=endpoint,
                preset=effective_preset,
                scheduler_key=scheduler_key,
            )
        )
    if slack:
        logger.warning(
            "`--slack` is deprecated; configure an endpoint via "
            "Settings → Notifications and pass `--endpoint <name>`."
        )
        webhook_url = os.getenv("SLACK_WEBHOOK_URL")
        if not webhook_url:
            raise ValueError("SLACK_WEBHOOK_URL environment variable is not set")
        callbacks.append(SlackCallback(webhook_url=webhook_url))

    if debug:
        # Imported lazily to avoid pulling main.py's typer surface at
        # module import time.
        from srunx.cli.main import DebugCallback

        callbacks.append(DebugCallback())

    return callbacks


def _pre_resolve_scheduler_key(profile: str | None, local: bool) -> str:
    """Return the scheduler_key ``resolve_transport`` would select.

    Thin wrapper around :func:`peek_scheduler_key` kept as a module-
    local helper so the import has a guaranteed call site (ruff's
    unused-import stripper otherwise drops the transport import).
    """
    return peek_scheduler_key(profile=profile, local=local)


logger = get_logger(__name__)

# Create Typer app for workflow management
app = typer.Typer(
    help="Execute YAML-defined workflows using SLURM",
    epilog="""
Example YAML workflow:

  name: ml_pipeline
  jobs:
    - name: preprocess
      command: ["python", "preprocess.py"]
      resources:
        nodes: 1
        gpus_per_node: 2

    - name: train
      path: /path/to/train.sh
      depends_on:
        - preprocess

    - name: evaluate
      command: ["python", "evaluate.py"]
      depends_on:
        - train
      environment:
        conda: ml_env

    - name: upload
      command: ["python", "upload_model.py"]
      depends_on:
        - train
      environment:
        venv: /path/to/venv

    - name: notify
      command: ["python", "notify.py"]
      depends_on:
        - evaluate
        - upload
      environment:
        venv: /path/to/venv

Parameter sweeps:

  # Single-arg override (no sweep)
  srunx flow run --arg dataset=imagenet train.yaml

  # Ad-hoc 3x3 sweep (9 cells, 4 at a time)
  srunx flow run --sweep lr=0.001,0.01,0.1 --sweep seed=1,2,3 \\
      --max-parallel 4 train.yaml

  # Preview cell args without submitting
  srunx flow run --sweep lr=0.001,0.01 --max-parallel 2 --dry-run train.yaml
""",
)


@app.callback(invoke_without_command=True)
def execute_yaml(
    ctx: typer.Context,
    yaml_file: Annotated[
        Path | None, typer.Argument(help="Path to YAML workflow definition file")
    ] = None,
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
    log_level: Annotated[
        str, typer.Option("--log-level", help="Set logging level")
    ] = "INFO",
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
        typer.Option(
            "--arg",
            help=(
                "Override workflow args (KEY=VALUE). Repeat to set multiple; "
                "e.g. --arg lr=0.01 --arg dataset=imagenet."
            ),
        ),
    ] = None,
    sweep: Annotated[
        list[str] | None,
        typer.Option(
            "--sweep",
            help=(
                "Add a sweep matrix axis (KEY=v1,v2,v3). Repeat to cross "
                "multiple axes; e.g. --sweep lr=0.001,0.01,0.1 "
                "--sweep seed=1,2,3. Requires --max-parallel."
            ),
        ),
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
    profile: ProfileOpt = None,
    local: LocalOpt = False,
    quiet: QuietOpt = False,
) -> None:
    """Execute workflow from YAML file."""
    # If a subcommand was invoked, don't run the callback
    if ctx.invoked_subcommand is not None:
        return

    # If no yaml_file provided when no subcommand is invoked, show help
    if yaml_file is None:
        ctx.get_help()
        ctx.exit()

    # At this point, yaml_file is guaranteed to be Path, not None
    assert yaml_file is not None  # for mypy
    _execute_workflow(
        yaml_file=yaml_file,
        validate=validate,
        dry_run=dry_run,
        log_level=log_level,
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
        profile=profile,
        local=local,
        quiet=quiet,
    )


def _load_yaml_sweep(yaml_file: Path) -> tuple[dict[str, Any], SweepSpec | None]:
    """Return ``(raw_yaml_dict, yaml_sweep_spec)``.

    ``yaml_sweep_spec`` is ``None`` when the YAML has no ``sweep:`` block.
    Invalid sweep blocks surface via ``WorkflowValidationError`` so the
    CLI path reports them consistently with the orchestrator.
    """
    with open(yaml_file, encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    if not isinstance(data, dict):
        return {}, None
    raw_sweep = data.get("sweep")
    if raw_sweep is None:
        return data, None
    if not isinstance(raw_sweep, dict):
        raise WorkflowValidationError(
            "YAML sweep: must be a mapping with matrix / fail_fast / max_parallel"
        )
    try:
        spec = SweepSpec.model_validate(raw_sweep)
    except Exception as exc:  # noqa: BLE001 — surface as validation error
        raise WorkflowValidationError(f"invalid YAML sweep block: {exc}") from exc
    return data, spec


def _resolve_endpoint_id(endpoint: str | None) -> int | None:
    """Look up a notification endpoint by name, returning its id or ``None``.

    Matches the attach logic used by :class:`NotificationWatchCallback`
    (lookup by name, skip when missing or disabled) so sweep runs honour
    ``--endpoint`` the same way non-sweep runs do.
    """
    if not endpoint:
        return None
    try:
        from srunx.db.connection import open_connection
        from srunx.db.repositories.endpoints import EndpointRepository
    except Exception:  # pragma: no cover — DB unreachable
        return None
    conn = open_connection()
    try:
        row = EndpointRepository(conn).get_by_name("slack_webhook", endpoint)
    finally:
        conn.close()
    if row is None or row.disabled_at is not None or row.id is None:
        return None
    return row.id


def _preview_sweep_dry_run(
    sweep_spec: SweepSpec,
    workflow_data: dict[str, Any],
    args_override: dict[str, Any],
) -> None:
    """Expand the matrix and print cell count / per-cell args without executing."""
    from srunx.sweep.expand import expand_matrix

    base_args: dict[str, Any] = {
        **(workflow_data.get("args") or {}),
        **args_override,
    }
    cells = expand_matrix(sweep_spec.matrix, base_args)
    console = Console()
    console.print("🔍 Sweep dry run:")
    console.print(f"  Cell count: {len(cells)}")
    console.print(f"  Matrix: {sweep_spec.matrix}")
    console.print(f"  Max parallel: {sweep_spec.max_parallel}")
    console.print(f"  Fail fast: {sweep_spec.fail_fast}")
    for i, cell in enumerate(cells):
        console.print(f"  Cell {i}: {cell}")


def _validate_all_sweep_cells(
    *,
    yaml_file: Path,
    workflow_data: dict[str, Any],
    args_override: dict[str, Any],
    sweep_spec: SweepSpec,
    callbacks: list[Callback],
) -> None:
    """Load + validate the workflow once per expanded matrix cell.

    Reuses :func:`srunx.sweep.expand.expand_matrix` to produce the same
    per-cell arg overlay the orchestrator will feed into
    :meth:`WorkflowRunner.from_yaml` at execution time. The first cell
    that fails Jinja rendering or workflow validation is reported with
    its index and effective args, matching the error shape used by the
    non-sweep validator.
    """
    from srunx.sweep.expand import expand_matrix

    base_args: dict[str, Any] = {
        **(workflow_data.get("args") or {}),
        **args_override,
    }
    cells = expand_matrix(sweep_spec.matrix, base_args)

    for idx, cell_args in enumerate(cells):
        try:
            runner = WorkflowRunner.from_yaml(
                yaml_file, callbacks=callbacks, args_override=cell_args
            )
            runner.workflow.validate()
        except WorkflowValidationError as exc:
            raise WorkflowValidationError(
                f"cell {idx} args={cell_args}: {exc}"
            ) from exc


def _enforce_shell_script_roots_cli(workflow: Workflow, rt: ResolvedTransport) -> None:
    """Reject ShellJob script paths that escape the SSH profile's mounts.

    Mirrors the Web router and MCP tool guards (see
    :func:`srunx.web.routers.workflows._enforce_shell_script_roots` and
    :func:`srunx.mcp.server._enforce_shell_script_roots_mcp`): when the
    workflow will be dispatched over SSH, every :class:`ShellJob`
    ``script_path`` must sit under one of the profile's mount ``local``
    roots so the remote executor can map it to a legitimate remote path.

    Local transport imposes no check: Phase 5b keeps local ShellJob
    behaviour unchanged.
    """
    if rt.transport_type != "ssh":
        return
    ctx = rt.submission_context
    if ctx is None or not ctx.mounts:
        # Profile has no mounts configured: fall through and warn instead
        # of raising — see _warn_missing_mounts.
        return

    allowed_roots = [Path(m.local).resolve() for m in ctx.mounts]
    violation = find_shell_script_violation(workflow, allowed_roots)
    if violation is not None:
        raise typer.BadParameter(
            f"ShellJob '{violation.job_name}' script_path "
            f"'{violation.script_path}' is not under any mount's local root "
            f"for profile '{rt.profile_name}'. "
            f"Allowed roots: {[str(r) for r in allowed_roots]}"
        )


def _warn_missing_mounts(rt: ResolvedTransport) -> None:
    """Warn when an SSH-bound flow has no profile mounts configured.

    Without mount translation, workflow ``work_dir`` / ``log_dir`` /
    ``ShellJob.script_path`` fields render verbatim on the remote side;
    they must already exist there or the submission will fail at the
    remote executor. The warning surfaces this expectation instead of
    letting the failure happen silently in a sbatch script that SLURM
    refuses.
    """
    if rt.transport_type != "ssh":
        return
    ctx = rt.submission_context
    if ctx is None or not ctx.mounts:
        logger.warning(
            "Profile '%s' has no mounts configured; workflow paths "
            "(work_dir / log_dir / script_path) will be rendered as-is "
            "on the remote cluster.",
            rt.profile_name,
        )


def _run_sweep(
    yaml_file: Path,
    workflow_data: dict[str, Any],
    args_override: dict[str, Any],
    sweep_spec: SweepSpec,
    callbacks: list[Callback],
    endpoint_id: int | None,
    preset: str,
    rt: ResolvedTransport,
) -> None:
    """Drive :class:`SweepOrchestrator` under a SIGINT → request_cancel guard.

    First Ctrl+C triggers :meth:`SweepOrchestrator.request_cancel` (drain
    pending cells). A second Ctrl+C re-raises ``KeyboardInterrupt`` so
    the user can escape a stuck orchestrator.

    The resolved transport's ``executor_factory`` and ``submission_context``
    are forwarded to the orchestrator so SSH-backed sweeps route each
    cell through the pool + mount translation path already used by the
    Web / MCP sweep surfaces.
    """
    orchestrator = SweepOrchestrator(
        workflow_yaml_path=yaml_file,
        workflow_data=workflow_data,
        args_override=args_override or None,
        sweep_spec=sweep_spec,
        submission_source="cli",
        callbacks=callbacks,
        endpoint_id=endpoint_id,
        preset=preset,
        executor_factory=rt.executor_factory,
        submission_context=rt.submission_context,
    )

    original_handler = signal.getsignal(signal.SIGINT)
    cancel_requested = {"n": 0}

    def _on_sigint(signum: int, frame: Any) -> None:
        cancel_requested["n"] += 1
        if cancel_requested["n"] == 1:
            logger.warning(
                "Received Ctrl+C — requesting sweep cancel. Press Ctrl+C again to abort."
            )
            try:
                orchestrator.request_cancel()
            except Exception:  # noqa: BLE001
                logger.warning("request_cancel raised", exc_info=True)
            # Restore default so the second Ctrl+C raises KeyboardInterrupt.
            signal.signal(signal.SIGINT, signal.default_int_handler)

    signal.signal(signal.SIGINT, _on_sigint)
    try:
        sweep_run = orchestrator.run()
    finally:
        signal.signal(signal.SIGINT, original_handler)

    logger.info(
        f"Sweep {sweep_run.id} finished: status={sweep_run.status} "
        f"cells={sweep_run.cell_count} completed={sweep_run.cells_completed} "
        f"failed={sweep_run.cells_failed} cancelled={sweep_run.cells_cancelled}"
    )


def _execute_workflow(
    yaml_file: Path,
    validate: bool = False,
    dry_run: bool = False,
    log_level: str = "INFO",
    slack: bool = False,
    endpoint: str | None = None,
    preset: str | None = None,
    from_job: str | None = None,
    to_job: str | None = None,
    job: str | None = None,
    arg: list[str] | None = None,
    sweep: list[str] | None = None,
    fail_fast: bool = False,
    max_parallel: int | None = None,
    debug: bool = False,
    profile: str | None = None,
    local: bool = False,
    quiet: bool = False,
) -> None:
    """Common workflow execution logic."""
    # Configure logging for workflow execution
    configure_workflow_logging(level=log_level)

    if job and (from_job or to_job):
        logger.error("❌ Cannot use --job with --from or --to options")
        sys.exit(1)

    try:
        if not yaml_file.exists():
            logger.error(f"Workflow file not found: {yaml_file}")
            sys.exit(1)

        # Parse --arg / --sweep; errors here are WorkflowValidationError.
        args_override = parse_arg_flags(arg or [])
        cli_sweep_axes_raw = parse_sweep_flags(sweep or [])
        # Narrow to ScalarValue lists (CLI values are str).
        cli_sweep_axes: dict[str, list[Any]] = {
            k: list(v) for k, v in cli_sweep_axes_raw.items()
        }

        workflow_data, yaml_sweep_spec = _load_yaml_sweep(yaml_file)

        # Only pass a bool when the user set --fail-fast; otherwise let
        # YAML decide (merge_sweep_specs treats ``None`` as "unset").
        cli_fail_fast: bool | None = True if fail_fast else None

        sweep_spec = merge_sweep_specs(
            yaml_sweep_spec,
            cli_sweep_axes,
            args_override,
            cli_fail_fast,
            max_parallel,
        )

        # Build callbacks BEFORE resolve_transport so the SSH adapter +
        # pool both see the full callback list at construction time (the
        # pool copies ``callbacks`` into ``_callbacks`` in ``__init__``,
        # so a post-hoc mutation wouldn't reach pooled clones). The
        # scheduler_key the transport will pick is deterministic from
        # the CLI flags + env, so we can pre-compute it via
        # :func:`peek_scheduler_key` and bind it to
        # ``NotificationWatchCallback`` up front.
        pre_scheduler_key = _pre_resolve_scheduler_key(profile, local)
        effective_preset = preset or get_config().notifications.default_preset
        is_sweep = sweep_spec is not None
        callbacks = _build_workflow_callbacks(
            endpoint=endpoint,
            effective_preset=effective_preset,
            is_sweep=is_sweep,
            slack=slack,
            debug=debug,
            scheduler_key=pre_scheduler_key,
        )

        # Resolve transport once for the whole invocation. The context
        # manager closes any SSH pool on exit. Validation / dry-run /
        # sweep / single-run all branch inside this scope so every path
        # sees the same executor_factory + submission_context.
        with resolve_transport(
            profile=profile,
            local=local,
            quiet=quiet,
            callbacks=callbacks,
            submission_source="cli",
        ) as rt:
            _warn_missing_mounts(rt)

            if sweep_spec is not None:
                if validate:
                    # Validate every matrix cell: a Jinja undefined or type
                    # error in a non-zero cell (e.g. only cell 3 references
                    # an undeclared arg) must still fail the validator.
                    _validate_all_sweep_cells(
                        yaml_file=yaml_file,
                        workflow_data=workflow_data,
                        args_override=args_override,
                        sweep_spec=sweep_spec,
                        callbacks=callbacks,
                    )
                    logger.info("Workflow validation successful")
                    return

                if dry_run:
                    _preview_sweep_dry_run(sweep_spec, workflow_data, args_override)
                    return

                # Enforce ShellJob script_path guard against the profile's
                # mounts before any cell is submitted. Load the workflow
                # once with empty cell args for the check; the orchestrator
                # will reload per cell with its own args.
                if rt.transport_type == "ssh":
                    _check_runner = WorkflowRunner.from_yaml(
                        yaml_file,
                        callbacks=callbacks,
                        args_override=args_override or None,
                    )
                    _enforce_shell_script_roots_cli(_check_runner.workflow, rt)

                endpoint_id = _resolve_endpoint_id(endpoint)
                _run_sweep(
                    yaml_file=yaml_file,
                    workflow_data=workflow_data,
                    args_override=args_override,
                    sweep_spec=sweep_spec,
                    callbacks=callbacks,
                    endpoint_id=endpoint_id,
                    preset=effective_preset,
                    rt=rt,
                )
                return

            # Non-sweep path.
            runner = WorkflowRunner.from_yaml(
                yaml_file,
                callbacks=callbacks,
                single_job=job,
                args_override=args_override or None,
                executor_factory=rt.executor_factory,
                submission_context=rt.submission_context,
            )

            runner.workflow.validate()
            _enforce_shell_script_roots_cli(runner.workflow, rt)

            if validate:
                logger.info("Workflow validation successful")
                return

            if dry_run:
                console = Console()
                console.print("🔍 Dry run mode - showing workflow structure:")
                console.print(f"Workflow: {runner.workflow.name}")

                jobs_to_execute = runner._get_jobs_to_execute(from_job, to_job, job)

                if job:
                    console.print(f"Executing single job: {job}")
                elif from_job or to_job:
                    range_info = []
                    if from_job:
                        range_info.append(f"from {from_job}")
                    if to_job:
                        range_info.append(f"to {to_job}")
                    console.print(
                        f"Executing jobs {' '.join(range_info)}: "
                        f"{len(jobs_to_execute)} jobs"
                    )
                else:
                    console.print(f"Executing all jobs: {len(jobs_to_execute)} jobs")

                for job_obj in jobs_to_execute:
                    if isinstance(job_obj, Job) and job_obj.command:
                        command_str = (
                            job_obj.command
                            if isinstance(job_obj.command, str)
                            else " ".join(job_obj.command or [])
                        )
                    elif isinstance(job_obj, ShellJob):
                        command_str = f"Shell script: {job_obj.script_path}"
                    else:
                        command_str = "N/A"
                    console.print(f"  - {job_obj.name}: {command_str}")
                return

            # Execute workflow
            results = runner.run(from_job=from_job, to_job=to_job, single_job=job)

            logger.info("Job Results:")
            for task_name, job_result in results.items():
                if hasattr(job_result, "job_id") and job_result.job_id:
                    logger.info(f"  {task_name}: Job ID {job_result.job_id}")
                else:
                    logger.info(f"  {task_name}: {job_result}")

    except WorkflowValidationError as e:
        logger.error(f"❌ Workflow validation error: {e}")
        sys.exit(1)
    except FileNotFoundError as e:
        logger.error(f"❌ Workflow file not found: {e}")
        sys.exit(1)
    except PermissionError as e:
        logger.error(f"❌ Permission denied: {e}")
        logger.error("💡 Check if you have write permissions to the target directories")
        sys.exit(1)
    except OSError as e:
        if e.errno == 30:  # Read-only file system
            logger.error(f"❌ Cannot write to read-only file system: {e}")
            logger.error(
                "💡 The target directory appears to be read-only. Check mount permissions."
            )
        else:
            logger.error(f"❌ System error: {e}")
        sys.exit(1)
    except ImportError as e:
        logger.error(f"❌ Missing dependency: {e}")
        logger.error(
            "💡 Make sure all required packages are installed in your environment"
        )
        sys.exit(1)
    except Exception as e:
        logger.error(f"❌ Workflow execution failed: {e}")
        logger.error(f"💡 Error type: {type(e).__name__}")
        import traceback

        logger.error("📍 Error location:")
        logger.error(traceback.format_exc())
        sys.exit(1)


@app.command(name="run")
def run_command(
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
    log_level: Annotated[
        str, typer.Option("--log-level", help="Set logging level")
    ] = "INFO",
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
        typer.Option(
            "--arg",
            help=(
                "Override workflow args (KEY=VALUE). Repeat to set multiple; "
                "e.g. --arg lr=0.01 --arg dataset=imagenet."
            ),
        ),
    ] = None,
    sweep: Annotated[
        list[str] | None,
        typer.Option(
            "--sweep",
            help=(
                "Add a sweep matrix axis (KEY=v1,v2,v3). Repeat to cross "
                "multiple axes; e.g. --sweep lr=0.001,0.01,0.1 "
                "--sweep seed=1,2,3. Requires --max-parallel."
            ),
        ),
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
    profile: ProfileOpt = None,
    local: LocalOpt = False,
    quiet: QuietOpt = False,
) -> None:
    """Execute workflow from YAML file."""
    _execute_workflow(
        yaml_file=yaml_file,
        validate=validate,
        dry_run=dry_run,
        log_level=log_level,
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
        profile=profile,
        local=local,
        quiet=quiet,
    )


@app.command(name="validate")
def validate_command(
    yaml_file: Annotated[
        Path, typer.Argument(help="Path to YAML workflow definition file")
    ],
    log_level: Annotated[
        str, typer.Option("--log-level", help="Set logging level")
    ] = "INFO",
) -> None:
    """Validate workflow YAML file without executing."""
    _execute_workflow(yaml_file, validate=True, log_level=log_level)


def main() -> None:
    """Main entry point for workflow CLI."""
    app()


if __name__ == "__main__":
    main()
