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


@app.command()
def run(
    yaml_file: Annotated[
        Path, typer.Argument(help="Path to YAML workflow definition file")
    ],
    validate_only: Annotated[
        bool,
        typer.Option(
            "--validate-only", help="Only validate the workflow file without executing"
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
) -> None:
    """Execute workflow from YAML file."""
    # Configure logging for workflow execution
    configure_workflow_logging(level=log_level)

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

        runner = WorkflowRunner.from_yaml(yaml_file, callbacks=callbacks)

        # Validate dependencies
        runner.workflow.validate()

        if validate_only:
            logger.info("Workflow validation successful")
            return

        if dry_run:
            console = Console()
            console.print("🔍 Dry run mode - showing workflow structure:")
            console.print(f"Workflow: {runner.workflow.name}")
            for job in runner.workflow.jobs:
                if isinstance(job, Job) and job.command:
                    command_str = (
                        job.command
                        if isinstance(job.command, str)
                        else " ".join(job.command or [])
                    )
                elif isinstance(job, ShellJob):
                    command_str = f"Shell script: {job.script_path}"
                else:
                    command_str = "N/A"
                console.print(f"  - {job.name}: {command_str}")
            return

        # Execute workflow
        results = runner.run()

        logger.info("Job Results:")
        for task_name, job in results.items():
            if hasattr(job, "job_id") and job.job_id:
                logger.info(f"  {task_name}: Job ID {job.job_id}")
            else:
                logger.info(f"  {task_name}: {job}")

    except Exception as e:
        logger.error(f"Workflow execution failed: {e}")
        sys.exit(1)


def main() -> None:
    """Main entry point for workflow CLI."""
    app()


if __name__ == "__main__":
    main()
