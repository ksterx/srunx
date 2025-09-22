"""CLI interface for workflow management."""

import os
import sys
from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console

from srunx.callbacks import SlackCallback
from srunx.logging import configure_workflow_logging, get_logger
from srunx.models import Job, ShellJob
from srunx.runner import WorkflowRunner

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
        from_job=from_job,
        to_job=to_job,
        job=job,
    )


def _execute_workflow(
    yaml_file: Path,
    validate: bool = False,
    dry_run: bool = False,
    log_level: str = "INFO",
    slack: bool = False,
    from_job: str | None = None,
    to_job: str | None = None,
    job: str | None = None,
) -> None:
    """Common workflow execution logic."""
    # Configure logging for workflow execution
    configure_workflow_logging(level=log_level)

    # Validate mutually exclusive options
    execution_options = [from_job, to_job, job]
    if job and (from_job or to_job):
        logger.error("❌ Cannot use --job with --from or --to options")
        sys.exit(1)

    try:
        if not yaml_file.exists():
            logger.error(f"Workflow file not found: {yaml_file}")
            sys.exit(1)

        # Setup callbacks if requested
        callbacks = []
        if slack:
            webhook_url = os.getenv("SLACK_WEBHOOK_URL")
            if not webhook_url:
                raise ValueError("SLACK_WEBHOOK_URL environment variable is not set")
            callbacks.append(SlackCallback(webhook_url=webhook_url))

        runner = WorkflowRunner.from_yaml(
            yaml_file, callbacks=callbacks, single_job=job
        )

        # Validate dependencies
        runner.workflow.validate()

        if validate:
            logger.info("Workflow validation successful")
            return

        if dry_run:
            console = Console()
            console.print("🔍 Dry run mode - showing workflow structure:")
            console.print(f"Workflow: {runner.workflow.name}")

            # Get jobs that would be executed
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
                    f"Executing jobs {' '.join(range_info)}: {len(jobs_to_execute)} jobs"
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
) -> None:
    """Execute workflow from YAML file."""
    _execute_workflow(
        yaml_file, validate, dry_run, log_level, slack, from_job, to_job, job
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
