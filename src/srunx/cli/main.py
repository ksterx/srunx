"""Main CLI interface for srunx.

Thin Typer root: owns the ``app`` object and registers every command group.
Command implementations live under ``srunx.cli.commands.*`` and shared
helpers under ``srunx.cli._helpers.*``.
"""

from pathlib import Path
from typing import Annotated

import typer

from srunx.cli._helpers.transport_options import LocalOpt, ProfileOpt, QuietOpt
from srunx.cli.commands.config import config_app
from srunx.cli.commands.jobs.gpus import gpus
from srunx.cli.commands.jobs.sbatch import sbatch
from srunx.cli.commands.jobs.scancel import scancel
from srunx.cli.commands.jobs.sinfo import sinfo
from srunx.cli.commands.jobs.squeue import squeue
from srunx.cli.commands.jobs.tail import tail
from srunx.cli.commands.reports import history, sacct
from srunx.cli.commands.templates import template_app
from srunx.cli.commands.ui import ui
from srunx.cli.watch import watch_app
from srunx.common.logging import configure_cli_logging
from srunx.ssh.cli.commands import ssh_app

# Create the main Typer app
app = typer.Typer(
    name="srunx",
    help="Python library for SLURM job management",
    context_settings={"help_option_names": ["-h", "--help"]},
)

# Create subapps (``flow`` stays here because ``flow_run`` is a thin shim
# delegating to ``srunx.cli.workflow._execute_workflow``; extracting it
# would only move one function and buy nothing).
flow_app = typer.Typer(help="Workflow management")

# Register sub-Typers first so the command order in ``srunx --help``
# matches the pre-refactor layout (ui came right after the sub-Typer
# wiring in the old monolithic file).
app.add_typer(flow_app, name="flow")
app.add_typer(config_app, name="config")
app.add_typer(watch_app, name="watch")
app.add_typer(ssh_app, name="ssh")
app.add_typer(template_app, name="template")

# Register root-level commands (moved into commands/*; wire them up here
# so ``srunx.cli.main.app`` keeps its historical surface).
app.command()(ui)
app.command("sbatch")(sbatch)
app.command("squeue")(squeue)
app.command("scancel")(scancel)
app.command("sinfo")(sinfo)
app.command("gpus")(gpus)
app.command("tail")(tail)
app.command("history")(history)
app.command("sacct")(sacct)


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


def main() -> None:
    """Main entry point for the CLI."""
    # Configure logging with defaults
    configure_cli_logging(level="INFO", quiet=False)

    # Run the app
    app()


if __name__ == "__main__":
    main()
