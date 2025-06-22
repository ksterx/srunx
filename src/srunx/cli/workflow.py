"""CLI interface for workflow management."""

import argparse
import os
import sys
from pathlib import Path

from srunx.callbacks import SlackCallback
from srunx.logging import configure_workflow_logging, get_logger
from srunx.runner import WorkflowRunner

logger = get_logger(__name__)


def create_workflow_parser() -> argparse.ArgumentParser:
    """Create argument parser for workflow commands."""
    parser = argparse.ArgumentParser(
        description="Execute YAML-defined workflows using SLURM",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Example YAML workflow:
  name: ml_pipeline
  tasks:
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

    parser.add_argument(
        "yaml_file",
        type=str,
        help="Path to YAML workflow definition file",
    )

    parser.add_argument(
        "--validate-only",
        action="store_true",
        help="Only validate the workflow file without executing",
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be executed without running jobs",
    )

    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
        help="Set logging level (default: %(default)s)",
    )

    parser.add_argument(
        "--slack",
        action="store_true",
        help="Send notifications to Slack",
    )

    return parser


def cmd_run_workflow(args: argparse.Namespace) -> None:
    """Handle workflow execution command."""
    # Configure logging for workflow execution
    configure_workflow_logging(level=args.log_level)

    try:
        yaml_file = Path(args.yaml_file)
        if not yaml_file.exists():
            logger.error(f"Workflow file not found: {args.yaml_file}")
            sys.exit(1)

        # Setup callbacks if requested
        callbacks = []
        if getattr(args, "slack", False):
            webhook_url = os.getenv("SLACK_WEBHOOK_URL")
            if not webhook_url:
                raise ValueError("SLACK_WEBHOOK_URL environment variable is not set")
            callbacks.append(SlackCallback(webhook_url=webhook_url))

        runner = WorkflowRunner.from_yaml(yaml_file, callbacks=callbacks)

        # Validate dependencies
        runner.workflow.validate()

        if args.validate_only:
            logger.info("Workflow validation successful")
            return

        if args.dry_run:
            runner.workflow.show()
            return

        # Execute workflow
        results = runner.run()

        logger.success(f"🎉 Workflow {runner.workflow.name} completed!!")
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
    parser = create_workflow_parser()
    args = parser.parse_args()

    cmd_run_workflow(args)


if __name__ == "__main__":
    main()
