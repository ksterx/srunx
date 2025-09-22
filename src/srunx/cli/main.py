"""Main CLI interface for srunx."""

import os
import sys
import tempfile
import traceback
from pathlib import Path
from typing import Annotated, Any

import typer
from rich.console import Console
from rich.panel import Panel
from rich.syntax import Syntax
from rich.table import Table

from srunx.callbacks import Callback, SlackCallback
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
from srunx.runner import WorkflowRunner
from srunx.ssh.cli.commands import ssh_app

logger = get_logger(__name__)


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
                    # Get the default template path
                    client = Slurm()
                    template_path = client.default_template
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


app.add_typer(flow_app, name="flow")
app.add_typer(config_app, name="config")
app.add_typer(ssh_app, name="ssh")


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


def _parse_container_args(container_arg: str | None) -> ContainerResource | None:
    """Parse container argument into ContainerResource."""
    if not container_arg:
        return None

    # Simple case: just image path
    if not container_arg.startswith("{") and "," not in container_arg:
        return ContainerResource(image=container_arg)

    # Complex case: parse key=value pairs
    container_data: dict[str, str | list[str]] = {}
    if container_arg.startswith("{") and container_arg.endswith("}"):
        container_arg = container_arg[1:-1]

    for pair in container_arg.split(","):
        if "=" in pair:
            key, value = pair.strip().split("=", 1)
            if key == "image":
                container_data["image"] = value
            elif key == "mounts":
                container_data["mounts"] = value.split(";")
            elif key == "workdir":
                container_data["workdir"] = value

    if container_data:
        image = container_data.get("image")
        mounts = container_data.get("mounts", [])
        workdir = container_data.get("workdir")
        return ContainerResource(
            image=image if isinstance(image, str) else None,
            mounts=mounts if isinstance(mounts, list) else [],
            workdir=workdir if isinstance(workdir, str) else None,
        )
    else:
        return ContainerResource(image=container_arg)


@app.command("submit")
def submit(
    command: Annotated[
        list[str], typer.Argument(help="Command to execute in the SLURM job")
    ],
    name: Annotated[str, typer.Option("--name", "--job-name", help="Job name")] = "job",
    log_dir: Annotated[
        str | None, typer.Option("--log-dir", help="Log directory")
    ] = None,
    work_dir: Annotated[
        str | None,
        typer.Option("--work-dir", "--chdir", help="Working directory for the job"),
    ] = None,
    # Resource options
    nodes: Annotated[int, typer.Option("-N", "--nodes", help="Number of nodes")] = 1,
    gpus_per_node: Annotated[
        int, typer.Option("--gpus-per-node", help="Number of GPUs per node")
    ] = 0,
    ntasks_per_node: Annotated[
        int, typer.Option("--ntasks-per-node", help="Number of tasks per node")
    ] = 1,
    cpus_per_task: Annotated[
        int, typer.Option("--cpus-per-task", help="Number of CPUs per task")
    ] = 1,
    memory: Annotated[
        str | None,
        typer.Option("--memory", "--mem", help="Memory per node (e.g., '32GB', '1TB')"),
    ] = None,
    time: Annotated[
        str | None,
        typer.Option(
            "--time",
            "--time-limit",
            help="Time limit (e.g., '1:00:00', '30:00', '1-12:00:00')",
        ),
    ] = None,
    nodelist: Annotated[
        str | None,
        typer.Option(
            "--nodelist", help="Specific nodes to use (e.g., 'node001,node002')"
        ),
    ] = None,
    partition: Annotated[
        str | None,
        typer.Option("--partition", help="SLURM partition to use (e.g., 'gpu', 'cpu')"),
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
    template: Annotated[
        str | None, typer.Option("--template", help="Custom SLURM script template")
    ] = None,
    verbose: Annotated[
        bool, typer.Option("--verbose", "-v", help="Show verbose output")
    ] = False,
) -> None:
    """Submit a SLURM job."""
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
    if container is not None:
        env_config["container"] = _parse_container_args(container)

    environment = JobEnvironment.model_validate(env_config)

    job_data = {
        "name": name,
        "command": command,
        "resources": resources,
        "environment": environment,
        "log_dir": log_dir,
    }

    if work_dir is not None:
        job_data["work_dir"] = work_dir

    job = Job.model_validate(job_data)

    if slack:
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
        elif isinstance(job, ShellJob):
            console.print(f"  Script: {job.script_path}")
        console.print(f"  Nodes: {job.resources.nodes}")
        console.print(f"  GPUs: {job.resources.gpus_per_node}")
        return

    # Submit job
    client = Slurm(callbacks=callbacks)
    submitted_job = client.submit(job, template_path=template, verbose=verbose)

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
        try:
            final_job = client.monitor(submitted_job)
            if final_job.status.name == "COMPLETED":
                console.print("✅ Job completed successfully")
            else:
                console.print(f"❌ Job failed with status: {final_job.status.name}")
                sys.exit(1)
        except KeyboardInterrupt:
            console.print("\n⚠️  Monitoring interrupted by user")
            console.print(
                f"Job {submitted_job.job_id} is still running in the background"
            )


@app.command("status")
def status(
    job_id: Annotated[int, typer.Argument(help="Job ID to check")],
) -> None:
    """Check job status."""
    try:
        client = Slurm()
        job = client.retrieve(job_id)

        console = Console()
        console.print(f"Job ID: [bold]{job.job_id}[/bold]")
        console.print(f"Status: {job.status.name}")
        console.print(f"Name: {job.name}")
        if isinstance(job, Job) and job.command:
            command_str = (
                job.command if isinstance(job.command, str) else " ".join(job.command)
            )
            console.print(f"Command: {command_str}")
        elif isinstance(job, ShellJob):
            console.print(f"Script: {job.script_path}")

    except Exception as e:
        logger.error(f"Error retrieving job {job_id}: {e}")
        sys.exit(1)


@app.command("list")
def list_jobs() -> None:
    """List user's jobs in the queue."""
    try:
        client = Slurm()
        jobs = client.queue()

        if not jobs:
            console = Console()
            console.print("No jobs in queue")
            return

        table = Table(title="Job Queue")
        table.add_column("Job ID", style="cyan")
        table.add_column("Name", style="magenta")
        table.add_column("Status", style="green")
        table.add_column("Nodes", justify="right")
        table.add_column("Time", justify="right")

        for job in jobs:
            table.add_row(
                str(job.job_id) if job.job_id else "N/A",
                job.name,
                job.status.name if hasattr(job, "status") else "UNKNOWN",
                str(getattr(getattr(job, "resources", None), "nodes", "N/A") or "N/A"),
                getattr(getattr(job, "resources", None), "time_limit", None) or "N/A",
            )

        console = Console()
        console.print(table)

    except Exception as e:
        logger.error(f"Error retrieving job queue: {e}")
        sys.exit(1)


@app.command("cancel")
def cancel(
    job_id: Annotated[int, typer.Argument(help="Job ID to cancel")],
) -> None:
    """Cancel a running job."""
    try:
        client = Slurm()
        client.cancel(job_id)

        console = Console()
        console.print(f"✅ Job {job_id} cancelled successfully")

    except Exception as e:
        logger.error(f"Error cancelling job {job_id}: {e}")
        sys.exit(1)


@flow_app.command("run")
def flow_run(
    yaml_file: Annotated[
        Path, typer.Argument(help="Path to YAML workflow definition file")
    ],
    dry_run: Annotated[
        bool,
        typer.Option(
            "--dry-run", help="Show what would be executed without running jobs"
        ),
    ] = False,
    slack: Annotated[
        bool, typer.Option("--slack", help="Send notifications to Slack")
    ] = False,
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
) -> None:
    """Execute workflow from YAML file."""
    configure_workflow_logging()

    # Validate mutually exclusive options
    if job and (from_job or to_job):
        logger.error("❌ Cannot use --job with --from or --to options")
        sys.exit(1)

    if not yaml_file.exists():
        logger.error(f"Workflow file not found: {yaml_file}")
        sys.exit(1)

    try:
        # Setup callbacks
        callbacks: list[Callback] = []
        if debug:
            callbacks.append(DebugCallback())
        if slack:
            webhook_url = os.getenv("SLACK_WEBHOOK_URL")
            if webhook_url:
                callbacks.append(SlackCallback(webhook_url=webhook_url))
            else:
                logger.warning(
                    "SLACK_WEBHOOK_URL not set, skipping Slack notifications"
                )

        # Load and run workflow
        runner = WorkflowRunner.from_yaml(yaml_file, callbacks=callbacks)

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
                # Use hasattr checks to be robust against module boundary issues
                if hasattr(job_obj, "command") and job_obj.command:
                    command_str = (
                        job_obj.command
                        if isinstance(job_obj.command, str)
                        else " ".join(job_obj.command)
                    )
                elif hasattr(job_obj, "script_path") and job_obj.script_path:
                    command_str = f"Shell script: {job_obj.script_path}"
                else:
                    command_str = "N/A"
                console.print(f"  - {job_obj.name}: {command_str}")
        else:
            runner.run(from_job=from_job, to_job=to_job, single_job=job)

    except PermissionError as e:
        logger.error(f"❌ Permission denied: {e}")
        logger.error("💡 Check if you have write permissions to the target directories")
        logger.error(traceback.format_exc())
        sys.exit(1)
    except OSError as e:
        if e.errno == 30:  # Read-only file system
            logger.error(f"❌ Cannot write to read-only file system: {e}")
            logger.error(
                "💡 The target directory appears to be read-only. Check mount permissions."
            )
        else:
            logger.error(f"❌ System error: {e}")
        logger.error(traceback.format_exc())
        sys.exit(1)
    except ImportError as e:
        logger.error(f"❌ Missing dependency: {e}")
        logger.error(
            "💡 Make sure all required packages are installed in your environment"
        )
        logger.error(traceback.format_exc())
        sys.exit(1)
    except Exception as e:
        logger.error(f"❌ Workflow execution failed: {e}")
        logger.error(f"💡 Error type: {type(e).__name__}")
        logger.error(traceback.format_exc())
        sys.exit(1)


@flow_app.command("validate")
def flow_validate(
    yaml_file: Annotated[
        Path, typer.Argument(help="Path to YAML workflow definition file")
    ],
) -> None:
    """Validate workflow YAML file."""
    if not yaml_file.exists():
        logger.error(f"Workflow file not found: {yaml_file}")
        sys.exit(1)

    try:
        runner = WorkflowRunner.from_yaml(yaml_file)
        runner.workflow.validate()

        console = Console()
        console.print("✅ Workflow validation successful")
        console.print(f"   Workflow: {runner.workflow.name}")
        console.print(f"   Jobs: {len(runner.workflow.jobs)}")

    except Exception as e:
        logger.error(f"Workflow validation failed: {e}")
        sys.exit(1)


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


def main() -> None:
    """Main entry point for the CLI."""
    # Configure logging with defaults
    configure_cli_logging(level="INFO", quiet=False)

    # Run the app
    app()


if __name__ == "__main__":
    main()
