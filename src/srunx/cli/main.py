"""Main CLI interface for srunx."""

import argparse
import os
import sys
from pathlib import Path

from rich.console import Console
from rich.table import Table

from srunx.callbacks import SlackCallback
from srunx.client import Slurm
from srunx.config import (
    create_example_config,
    get_config,
    get_config_paths,
)
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
    # Get configuration defaults
    config = get_config()

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
        default=config.log_dir,
        help="Log directory (default: %(default)s)",
    )
    parser.add_argument(
        "--work-dir",
        "--chdir",
        type=str,
        default=config.work_dir,
        help="Working directory for the job",
    )

    # Resource configuration
    resource_group = parser.add_argument_group("Resource Options")
    resource_group.add_argument(
        "-N",
        "--nodes",
        type=int,
        default=config.resources.nodes,
        help="Number of nodes (default: %(default)s)",
    )
    resource_group.add_argument(
        "--gpus-per-node",
        type=int,
        default=config.resources.gpus_per_node,
        help="Number of GPUs per node (default: %(default)s)",
    )
    resource_group.add_argument(
        "--ntasks-per-node",
        type=int,
        default=config.resources.ntasks_per_node,
        help="Number of tasks per node (default: %(default)s)",
    )
    resource_group.add_argument(
        "--cpus-per-task",
        type=int,
        default=config.resources.cpus_per_task,
        help="Number of CPUs per task (default: %(default)s)",
    )
    resource_group.add_argument(
        "--memory",
        "--mem",
        type=str,
        default=config.resources.memory_per_node,
        help="Memory per node (e.g., '32GB', '1TB') (default: %(default)s)",
    )
    resource_group.add_argument(
        "--time",
        "--time-limit",
        type=str,
        default=config.resources.time_limit,
        help="Time limit (e.g., '1:00:00', '30:00', '1-12:00:00') (default: %(default)s)",
    )
    resource_group.add_argument(
        "--nodelist",
        type=str,
        default=config.resources.nodelist,
        help="Specific nodes to use (e.g., 'node001,node002') (default: %(default)s)",
    )
    resource_group.add_argument(
        "--partition",
        type=str,
        default=config.resources.partition,
        help="SLURM partition to use (e.g., 'gpu', 'cpu') (default: %(default)s)",
    )

    # Environment configuration
    env_group = parser.add_argument_group("Environment Options")
    env_group.add_argument(
        "--conda",
        type=str,
        default=config.environment.conda,
        help="Conda environment name (default: %(default)s)",
    )
    env_group.add_argument(
        "--venv",
        type=str,
        default=config.environment.venv,
        help="Virtual environment path (default: %(default)s)",
    )
    env_group.add_argument(
        "--sqsh",
        type=str,
        default=config.environment.sqsh,
        help="SquashFS image path (default: %(default)s)",
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

    # Config command
    config_parser = subparsers.add_parser("config", help="Configuration management")
    config_parser.set_defaults(func=None)  # Will be overridden by subcommands

    # Config subcommands
    config_subparsers = config_parser.add_subparsers(
        dest="config_command", help="Configuration commands"
    )

    # Config show command
    config_show_parser = config_subparsers.add_parser(
        "show", help="Show current configuration"
    )
    config_show_parser.set_defaults(func=cmd_config_show)

    # Config paths command
    config_paths_parser = config_subparsers.add_parser(
        "paths", help="Show configuration file paths"
    )
    config_paths_parser.set_defaults(func=cmd_config_paths)

    # Config init command
    config_init_parser = config_subparsers.add_parser(
        "init", help="Initialize configuration file"
    )
    config_init_parser.set_defaults(func=cmd_config_init)
    config_init_parser.add_argument(
        "--global",
        action="store_true",
        dest="global_config",
        help="Create global user config instead of project config",
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
        # Parse environment variables and merge with config defaults
        config = get_config()
        env_vars = config.environment.env_vars.copy()
        cli_env_vars = _parse_env_vars(getattr(args, "env_vars", None))
        env_vars.update(cli_env_vars)

        # Create job configuration
        resources = JobResource(
            nodes=args.nodes,
            gpus_per_node=args.gpus_per_node,
            ntasks_per_node=args.ntasks_per_node,
            cpus_per_task=args.cpus_per_task,
            memory_per_node=getattr(args, "memory", None),
            time_limit=getattr(args, "time", None),
            nodelist=getattr(args, "nodelist", None),
            partition=getattr(args, "partition", None),
        )

        # Create environment with explicit handling of defaults
        # Only pass non-None values to avoid conflicts with validation
        env_config = {}
        if args.conda is not None:
            env_config["conda"] = args.conda
        if args.venv is not None:
            env_config["venv"] = args.venv
        if args.sqsh is not None:
            env_config["sqsh"] = args.sqsh
        env_config["env_vars"] = env_vars

        # If no environment was explicitly set, let JobEnvironment use its defaults
        if not any([args.conda, args.venv, args.sqsh]):
            environment = JobEnvironment(env_vars=env_vars)
        else:
            environment = JobEnvironment.model_validate(env_config)

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


def cmd_config_show(args: argparse.Namespace) -> None:
    """Handle config show command."""
    try:
        config = get_config()

        console = Console()
        console.print("[bold cyan]Current Configuration:[/bold cyan]")

        # Display config in a nice format using Rich
        table = Table(
            title="srunx Configuration", show_header=True, header_style="bold magenta"
        )
        table.add_column("Section", style="cyan")
        table.add_column("Setting", style="green")
        table.add_column("Value", style="yellow")

        # Resources
        table.add_row("resources", "nodes", str(config.resources.nodes))
        table.add_row("", "gpus_per_node", str(config.resources.gpus_per_node))
        table.add_row("", "ntasks_per_node", str(config.resources.ntasks_per_node))
        table.add_row("", "cpus_per_task", str(config.resources.cpus_per_task))
        table.add_row("", "memory_per_node", str(config.resources.memory_per_node))
        table.add_row("", "time_limit", str(config.resources.time_limit))
        table.add_row("", "nodelist", str(config.resources.nodelist))
        table.add_row("", "partition", str(config.resources.partition))

        # Environment
        table.add_row("environment", "conda", str(config.environment.conda))
        table.add_row("", "venv", str(config.environment.venv))
        table.add_row("", "sqsh", str(config.environment.sqsh))
        if config.environment.env_vars:
            for key, value in config.environment.env_vars.items():
                table.add_row("", f"env_vars.{key}", value)
        else:
            table.add_row("", "env_vars", "(empty)")

        # General
        table.add_row("general", "log_dir", config.log_dir)
        table.add_row("", "work_dir", str(config.work_dir))

        console.print(table)

    except Exception as e:
        logger.error(f"Error showing configuration: {e}")
        sys.exit(1)


def cmd_config_paths(args: argparse.Namespace) -> None:
    """Handle config paths command."""
    try:
        console = Console()
        console.print("[bold cyan]Configuration File Paths:[/bold cyan]")
        console.print("(Listed in order of precedence - last one wins)")

        paths = get_config_paths()
        for i, path in enumerate(paths, 1):
            exists = "✓" if path.exists() else "✗"
            console.print(f"{i}. [{exists}] {path}")

    except Exception as e:
        logger.error(f"Error showing configuration paths: {e}")
        sys.exit(1)


def cmd_config_init(args: argparse.Namespace) -> None:
    """Handle config init command."""
    try:
        if getattr(args, "global_config", False):
            # Create global user config
            config_paths = get_config_paths()
            config_path = config_paths[1]  # User config path
        else:
            # Create project config
            config_path = Path.cwd() / "srunx.json"

        if config_path.exists():
            logger.error(f"Configuration file already exists: {config_path}")
            sys.exit(1)

        # Create directory if it doesn't exist
        config_path.parent.mkdir(parents=True, exist_ok=True)

        # Write example config
        example_config = create_example_config()
        with open(config_path, "w", encoding="utf-8") as f:
            f.write(example_config)

        logger.info(f"Configuration file created: {config_path}")
        logger.info("Edit this file to customize your defaults")

    except Exception as e:
        logger.error(f"Error creating configuration file: {e}")
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
        # Check if this is a config command without subcommand
        elif hasattr(args, "command") and args.command == "config":
            if not hasattr(args, "config_command") or args.config_command is None:
                logger.error(
                    "Config command requires a subcommand (show, paths, or init)"
                )
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
