"""Workflow CLI orchestrator: ``_execute_workflow``, ``run_command``, ``app``, ``main``.

This module owns the top-level orchestration: parse CLI flags, resolve
transport, branch sweep vs non-sweep, and drive the helpers in
:mod:`srunx.cli.workflow.{loading,sweep,mounts,guards,notifications}`.
The standalone ``app`` Typer instance lets tests drive
``_execute_workflow`` end-to-end without spinning up the full ``srunx``
root app.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Annotated, Any

import typer
from rich.console import Console

from srunx.cli._helpers.transport_options import LocalOpt, ProfileOpt, QuietOpt
from srunx.cli.workflow.guards import (
    _enforce_shell_script_roots_cli,
    _warn_missing_mounts,
)
from srunx.cli.workflow.loading import _load_yaml_sweep, _resolve_endpoint_id
from srunx.cli.workflow.mounts import (
    _hold_workflow_mounts,
    _in_place_context,
    _resolve_sync_flag,
)
from srunx.cli.workflow.notifications import _build_workflow_callbacks
from srunx.cli.workflow.sweep import (
    _expand_sweep_cell_args,
    _preview_sweep_dry_run,
    _resolve_sweep_locked_mounts,
    _run_sweep,
    _validate_all_sweep_cells,
)
from srunx.common.config import get_config
from srunx.common.exceptions import WorkflowValidationError
from srunx.common.logging import configure_workflow_logging, get_logger
from srunx.domain import Job, ShellJob
from srunx.runtime.sweep.expand import (
    merge_sweep_specs,
    parse_arg_flags,
    parse_sweep_flags,
)
from srunx.runtime.workflow.runner import WorkflowRunner
from srunx.transport import peek_scheduler_key, resolve_transport

logger = get_logger(__name__)


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
    sync: bool | None = None,
) -> None:
    """Common workflow execution logic."""
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
        pre_scheduler_key = peek_scheduler_key(profile=profile, local=local)
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
                # Phase 2 (#135): hold the per-(profile, mount) sync
                # lock across every cell in the sweep so a concurrent
                # invocation can't rsync mid-sweep, AND so the rsync
                # itself runs once per mount instead of once per cell.
                #
                # Issue #143: when the sweep matrix can influence
                # ``ShellJob.script_path`` (e.g. ``path: "{{ scratch
                # }}/{{ seed }}/run.sh"``), the base render alone
                # misses mounts only a non-base cell can resolve to.
                # Expand the matrix here, render each cell, and union
                # the touched mounts so the lock-set covers every
                # cell. Then it is safe to flip ``allow_in_place=True``
                # for the sweep path.
                cell_args_overrides = _expand_sweep_cell_args(
                    workflow_data, args_override, sweep_spec
                )
                # Render every cell once, take the union of touched
                # mounts, and feed the SAME list to both the lock-
                # acquisition step and the SSH adapter's defence-in-
                # depth check. Sharing the resolved list avoids a
                # second N-cell render pass and guarantees the two
                # views can never disagree.
                sweep_locked_mounts = (
                    _resolve_sweep_locked_mounts(rt, cell_args_overrides, yaml_file)
                    if rt.transport_type == "ssh"
                    else []
                )
                sweep_locked_names = tuple(m.name for m in sweep_locked_mounts)
                with _hold_workflow_mounts(
                    rt=rt,
                    workflow_for_mounts=_check_runner.workflow
                    if rt.transport_type == "ssh"
                    else None,
                    sync_required=_resolve_sync_flag(sync),
                    explicit_mounts=sweep_locked_mounts
                    if rt.transport_type == "ssh"
                    else None,
                ):
                    # Lock-set is now sound for every cell — flip
                    # ``allow_in_place`` for the sweep so each cell
                    # whose rendered ShellJob bytes match its source
                    # can take the IN_PLACE shortcut on the remote.
                    # The locked-mount tuple rides the context as a
                    # defence-in-depth rejection signal: a buggy cell
                    # that escapes the aggregation hits a clear
                    # "mount X not locked" error in the SSH adapter
                    # instead of silently racing rsync.
                    #
                    # When ``sweep_locked_mounts`` is empty AND we're
                    # on SSH, we deliberately keep ``allow_in_place``
                    # at its default ``False``: an empty union means
                    # either no cell touches a mount (nothing to flip
                    # for) or the profile lookup failed (in which case
                    # we never held the lock — flipping the flag here
                    # would let the adapter race rsync against an
                    # unsynchronised remote). Both outcomes match the
                    # safer pre-#143 sweep behaviour.
                    if rt.transport_type == "ssh" and sweep_locked_mounts:
                        sweep_submission_context = _in_place_context(
                            rt, locked_mount_names=sweep_locked_names
                        )
                    else:
                        sweep_submission_context = rt.submission_context
                    _run_sweep(
                        yaml_file=yaml_file,
                        workflow_data=workflow_data,
                        args_override=args_override,
                        sweep_spec=sweep_spec,
                        callbacks=callbacks,
                        endpoint_id=endpoint_id,
                        preset=effective_preset,
                        rt=rt,
                        submission_context=sweep_submission_context,
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

            # Execute workflow.
            #
            # Phase 2 (#135): wrap the call in ``_hold_workflow_mounts``
            # so each mount that any ShellJob's ``script_path`` lives
            # under is rsynced **once** at the start, and the per-mount
            # lock is held across every job in the workflow. The lock
            # serialisation closes the same race
            # ``mount_sync_session`` closes for single-job ``sbatch``.
            with _hold_workflow_mounts(
                rt=rt,
                workflow_for_mounts=runner.workflow,
                sync_required=_resolve_sync_flag(sync),
            ):
                # Lock is held → flip ``allow_in_place`` so the SSH
                # adapter's ShellJob branch is permitted to take the
                # IN_PLACE path. Mutate the runner's stored context
                # rather than re-building the runner so all the
                # per-job state (parsed dependencies, etc.) survives.
                if rt.transport_type == "ssh":
                    in_place_ctx = _in_place_context(rt)
                    if in_place_ctx is not None:
                        runner._submission_context = in_place_ctx  # noqa: SLF001
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
    except Exception as e:  # noqa: BLE001
        # Top-level safety net for unexpected failures: log a full
        # traceback and exit non-zero so the CLI surfaces the failure
        # rather than handing back a Python stack to the user.
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
    sync: Annotated[
        bool | None,
        typer.Option(
            "--sync/--no-sync",
            help=(
                "Rsync each touched mount once at the start of the run "
                "(default ``[sync] auto``, true unless disabled). "
                "``--no-sync`` skips the rsync but still acquires the "
                "per-mount lock for race-free submission."
            ),
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
        sync=sync,
    )


def main() -> None:
    """Main entry point for workflow CLI."""
    app()


if __name__ == "__main__":
    main()
