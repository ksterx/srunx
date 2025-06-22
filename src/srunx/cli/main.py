"""Main CLI interface for srunx."""

import argparse
import os
import sys
from pathlib import Path

from rich.console import Console
from rich.table import Table

from srunx.callbacks import SlackCallback
from srunx.client import Slurm
from srunx.logging import (
    configure_cli_logging,
    configure_workflow_logging,
    get_logger,
)
from srunx.models import Job, JobEnvironment, JobResource
from srunx.runner import WorkflowRunner

logger = get_logger(__name__)


def create_job_parser() -> argparse.ArgumentParser:
    """Create argument parser for job submission."""
    parser = argparse.ArgumentParser(
        description="Submit SLURM jobs with various configurations",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    # Required arguments
    parser.add_argument(
        "command",
        nargs="+",
        help="Command to execute in the SLURM job",
    )

    # Job configuration
    parser.add_argument(
        "--name",
        "--job-name",
        type=str,
        default="job",
        help="Job name (default: %(default)s)",
    )
    parser.add_argument(
        "--log-dir",
        type=str,
        default=os.getenv("SLURM_LOG_DIR", "logs"),
        help="Log directory (default: %(default)s)",
    )
    parser.add_argument(
        "--work-dir",
        "--chdir",
        type=str,
        help="Working directory for the job",
    )

    # Resource configuration
    resource_group = parser.add_argument_group("Resource Options")
    resource_group.add_argument(
        "-N",
        "--nodes",
        type=int,
        default=1,
        help="Number of nodes (default: %(default)s)",
    )
    resource_group.add_argument(
        "--gpus-per-node",
        type=int,
        default=0,
        help="Number of GPUs per node (default: %(default)s)",
    )
    resource_group.add_argument(
        "--ntasks-per-node",
        type=int,
        default=1,
        help="Number of tasks per node (default: %(default)s)",
    )
    resource_group.add_argument(
        "--cpus-per-task",
        type=int,
        default=1,
        help="Number of CPUs per task (default: %(default)s)",
    )
    resource_group.add_argument(
        "--memory",
        "--mem",
        type=str,
        help="Memory per node (e.g., '32GB', '1TB')",
    )
    resource_group.add_argument(
        "--time",
        "--time-limit",
        type=str,
        help="Time limit (e.g., '1:00:00', '30:00', '1-12:00:00')",
    )

    # Environment configuration
    env_group = parser.add_argument_group("Environment Options")
    env_group.add_argument(
        "--conda",
        type=str,
        help="Conda environment name",
    )
    env_group.add_argument(
        "--venv",
        type=str,
        help="Virtual environment path",
    )
    env_group.add_argument(
        "--sqsh",
        type=str,
        help="SquashFS image path",
    )
    env_group.add_argument(
        "--env",
        action="append",
        dest="env_vars",
        help="Environment variable KEY=VALUE (can be used multiple times)",
    )

    # Execution options
    exec_group = parser.add_argument_group("Execution Options")
    exec_group.add_argument(
        "--template",
        type=str,
        help="Path to custom SLURM template file",
    )
    exec_group.add_argument(
        "--wait",
        action="store_true",
        help="Wait for job completion",
    )
    exec_group.add_argument(
        "--poll-interval",
        type=int,
        default=5,
        help="Polling interval in seconds when waiting (default: %(default)s)",
    )

    # Logging options
    log_group = parser.add_argument_group("Logging Options")
    log_group.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
        help="Set logging level (default: %(default)s)",
    )
    log_group.add_argument(
        "--quiet",
        "-q",
        action="store_true",
        help="Only show warnings and errors",
    )

    # Callback options
    callback_group = parser.add_argument_group("Notification Options")
    callback_group.add_argument(
        "--slack",
        action="store_true",
        help="Send notifications to Slack",
    )

    # Misc options
    misc_group = parser.add_argument_group("Misc Options")
    misc_group.add_argument(
        "--verbose",
        action="store_true",
        help="Print the rendered content",
    )

    return parser


def create_status_parser() -> argparse.ArgumentParser:
    """Create argument parser for job status."""
    parser = argparse.ArgumentParser(
        description="Check SLURM job status",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument(
        "job_id",
        type=int,
        help="SLURM job ID to check",
    )

    return parser


def create_queue_parser() -> argparse.ArgumentParser:
    """Create argument parser for queueing jobs."""
    parser = argparse.ArgumentParser(
        description="Queue SLURM jobs",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument(
        "--user",
        "-u",
        type=str,
        help="Queue jobs for specific user (default: current user)",
    )

    return parser


def create_cancel_parser() -> argparse.ArgumentParser:
    """Create argument parser for job cancellation."""
    parser = argparse.ArgumentParser(
        description="Cancel SLURM job",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument(
        "job_id",
        type=int,
        help="SLURM job ID to cancel",
    )

    return parser


def create_main_parser() -> argparse.ArgumentParser:
    """Create main argument parser with subcommands."""
    parser = argparse.ArgumentParser(
        description="srunx - Python library for SLURM job management",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    # Global options
    parser.add_argument(
        "--log-level",
        "-l",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
        help="Set logging level (default: %(default)s)",
    )
    parser.add_argument(
        "--quiet",
        "-q",
        action="store_true",
        help="Only show warnings and errors",
    )

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Submit command (default)
    submit_parser = subparsers.add_parser("submit", help="Submit a SLURM job")
    submit_parser.set_defaults(func=cmd_submit)
    _copy_parser_args(create_job_parser(), submit_parser)

    # Status command
    status_parser = subparsers.add_parser("status", help="Check job status")
    status_parser.set_defaults(func=cmd_status)
    _copy_parser_args(create_status_parser(), status_parser)

    # Queue command
    queue_parser = subparsers.add_parser("queue", help="Queue jobs")
    queue_parser.set_defaults(func=cmd_queue)
    _copy_parser_args(create_queue_parser(), queue_parser)

    # Cancel command
    cancel_parser = subparsers.add_parser("cancel", help="Cancel job")
    cancel_parser.set_defaults(func=cmd_cancel)
    _copy_parser_args(create_cancel_parser(), cancel_parser)

    # Flow command
    flow_parser = subparsers.add_parser("flow", help="Workflow management")
    flow_parser.set_defaults(func=None)  # Will be overridden by subcommands

    # Flow subcommands
    flow_subparsers = flow_parser.add_subparsers(
        dest="flow_command", help="Flow commands"
    )

    # Flow run command
    flow_run_parser = flow_subparsers.add_parser("run", help="Execute workflow")
    flow_run_parser.set_defaults(func=cmd_flow_run)
    flow_run_parser.add_argument(
        "yaml_file",
        type=str,
        help="Path to YAML workflow definition file",
    )
    flow_run_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be executed without running jobs",
    )
    flow_run_parser.add_argument(
        "--slack",
        action="store_true",
        help="Send notifications to Slack",
    )

    # Flow validate command
    flow_validate_parser = flow_subparsers.add_parser(
        "validate", help="Validate workflow"
    )
    flow_validate_parser.set_defaults(func=cmd_flow_validate)
    flow_validate_parser.add_argument(
        "yaml_file",
        type=str,
        help="Path to YAML workflow definition file",
    )

    return parser


def _copy_parser_args(
    source_parser: argparse.ArgumentParser, target_parser: argparse.ArgumentParser
) -> None:
    """Copy arguments from source parser to target parser."""
    for action in source_parser._actions:
        if action.dest == "help":
            continue
        target_parser._add_action(action)


def _parse_env_vars(env_var_list: list[str] | None) -> dict[str, str]:
    """Parse environment variables from list of KEY=VALUE strings."""
    env_vars = {}
    if env_var_list:
        for env_var in env_var_list:
            if "=" in env_var:
                key, value = env_var.split("=", 1)
                env_vars[key] = value
            else:
                logger.warning(f"Invalid environment variable format: {env_var}")
    return env_vars


def cmd_submit(args: argparse.Namespace) -> None:
    """Handle job submission command."""
    try:
        # Parse environment variables
        env_vars = _parse_env_vars(getattr(args, "env_vars", None))

        # Create job configuration
        resources = JobResource(
            nodes=args.nodes,
            gpus_per_node=args.gpus_per_node,
            ntasks_per_node=args.ntasks_per_node,
            cpus_per_task=args.cpus_per_task,
            memory_per_node=getattr(args, "memory", None),
            time_limit=getattr(args, "time", None),
        )

        environment = JobEnvironment(
            conda=getattr(args, "conda", None),
            venv=getattr(args, "venv", None),
            sqsh=getattr(args, "sqsh", None),
            env_vars=env_vars,
        )

        job_data = {
            "name": args.name,
            "command": args.command,
            "resources": resources,
            "environment": environment,
            "log_dir": args.log_dir,
        }

        if args.work_dir is not None:
            job_data["work_dir"] = args.work_dir

        job = Job.model_validate(job_data)

        if args.slack:
            webhook_url = os.getenv("SLACK_WEBHOOK_URL")
            if not webhook_url:
                raise ValueError("SLACK_WEBHOOK_URL is not set")
            callbacks = [SlackCallback(webhook_url=webhook_url)]
        else:
            callbacks = []

        # Submit job
        client = Slurm(callbacks=callbacks)
        submitted_job = client.submit(
            job, getattr(args, "template", None), verbose=args.verbose
        )

        logger.info(f"Submitted job {submitted_job.job_id}: {submitted_job.name}")

        # Wait for completion if requested
        if getattr(args, "wait", False):
            logger.info(f"Waiting for job {submitted_job.job_id} to complete...")
            completed_job = client.monitor(
                submitted_job, poll_interval=args.poll_interval
            )
            status_str = (
                completed_job.status.value if completed_job.status else "Unknown"
            )
            logger.info(
                f"Job {submitted_job.job_id} completed with status: {status_str}"
            )

    except Exception as e:
        logger.error(f"Error submitting job: {e}")
        sys.exit(1)


def cmd_status(args: argparse.Namespace) -> None:
    """Handle job status command."""
    try:
        client = Slurm()
        job = client.retrieve(args.job_id)

        logger.info(f"Job ID: {job.job_id}")
        logger.info(f"Name: {job.name}")
        if job.status:
            logger.info(f"Status: {job.status.value}")
        else:
            logger.info("Status: Unknown")

    except Exception as e:
        logger.error(f"Error getting job status: {e}")
        sys.exit(1)


def cmd_queue(args: argparse.Namespace) -> None:
    """Handle job queueing command."""
    try:
        client = Slurm()
        jobs = client.queue(getattr(args, "user", None))

        if not jobs:
            logger.info("No jobs found")
            return

        logger.info(f"{'Job ID':<12} {'Name':<20} {'Status':<12}")
        logger.info("-" * 45)
        for job in jobs:
            status_str = job.status.value if job.status else "Unknown"
            logger.info(f"{job.job_id:<12} {job.name:<20} {status_str:<12}")

    except Exception as e:
        logger.error(f"Error queueing jobs: {e}")
        sys.exit(1)


def cmd_cancel(args: argparse.Namespace) -> None:
    """Handle job cancellation command."""
    try:
        client = Slurm()
        client.cancel(args.job_id)
        logger.info(f"Cancelled job {args.job_id}")

    except Exception as e:
        logger.error(f"Error cancelling job: {e}")
        sys.exit(1)


def cmd_flow_run(args: argparse.Namespace) -> None:
    """Handle flow run command."""
    # Configure logging for workflow execution
    configure_workflow_logging(level=getattr(args, "log_level", "INFO"))

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

        if args.dry_run:
            runner.workflow.show()
            return

        # Execute workflow
        results = runner.run()

        logger.success(f"🎉 Workflow {runner.workflow.name} completed!!")
        table = Table(title=f"Workflow {runner.workflow.name} Summary")
        table.add_column("Job", justify="left", style="cyan", no_wrap=True)
        table.add_column("Status", justify="left", style="cyan", no_wrap=True)
        table.add_column("ID", justify="left", style="cyan", no_wrap=True)
        for job in results.values():
            table.add_row(job.name, job.status.value, str(job.job_id))

        console = Console()
        console.print(table)

    except Exception as e:
        logger.error(f"Workflow execution failed: {e}")
        sys.exit(1)


def cmd_flow_validate(args: argparse.Namespace) -> None:
    """Handle flow validate command."""
    # Configure logging for workflow validation
    configure_workflow_logging(level=getattr(args, "log_level", "INFO"))

    try:
        yaml_file = Path(args.yaml_file)
        if not yaml_file.exists():
            logger.error(f"Workflow file not found: {args.yaml_file}")
            sys.exit(1)

        runner = WorkflowRunner.from_yaml(yaml_file)

        # Validate dependencies
        runner.workflow.validate()

        logger.info("Workflow validation successful")

    except Exception as e:
        logger.error(f"Workflow validation failed: {e}")
        sys.exit(1)


def main() -> None:
    """Main entry point for the CLI."""
    parser = create_main_parser()
    args = parser.parse_args()

    # Configure logging
    log_level = getattr(args, "log_level", "INFO")
    quiet = getattr(args, "quiet", False)
    configure_cli_logging(level=log_level, quiet=quiet)

    # If no command specified, default to submit behavior for backward compatibility
    if not hasattr(args, "func") or args.func is None:
        # Check if this is a flow command without subcommand
        if hasattr(args, "command") and args.command == "flow":
            if not hasattr(args, "flow_command") or args.flow_command is None:
                logger.error("Flow command requires a subcommand (run or validate)")
                parser.print_help()
                sys.exit(1)
        else:
            # Try to parse as submit command
            submit_parser = create_job_parser()
            try:
                submit_args = submit_parser.parse_args()
                cmd_submit(submit_args)
            except SystemExit:
                parser.print_help()
                sys.exit(1)
    else:
        args.func(args)


if __name__ == "__main__":
    main()
