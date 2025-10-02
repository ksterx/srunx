#!/usr/bin/env python3
"""
Typer-based SSH CLI commands for srunx.

This module provides a clean typer-based interface for SSH SLURM operations,
replacing the mixed argparse/typer architecture.
"""

import logging
import os
import time
from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console, RenderableType
from rich.live import Live
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.status import Status
from rich.syntax import Syntax
from rich.text import Text

from ..core.client import SSHSlurmClient
from ..core.config import ConfigManager
from ..core.ssh_config import get_ssh_config_host
from .profile_impl import add_profile_impl

try:
    from slack_sdk import WebhookClient

    SLACK_AVAILABLE = True
except ImportError:
    SLACK_AVAILABLE = False

console = Console()

# Create the main SSH app
ssh_app = typer.Typer(
    name="ssh",
    help="Submit and monitor SLURM jobs on remote servers via SSH",
    add_completion=False,
)

# Profile management subcommand
profile_app = typer.Typer(
    name="profile",
    help="Manage SSH connection profiles",
    no_args_is_help=True,
)
ssh_app.add_typer(profile_app, name="profile")


def setup_logging(verbose: bool = False):
    """Configure logging for SSH operations."""
    level = logging.DEBUG if verbose else logging.WARNING
    logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s", level=level)


def send_slack_notification(slack_client, message: str) -> None:
    """Send a notification to Slack."""
    if not slack_client:
        return

    try:
        slack_client.send(
            text=message,
            blocks=[
                {
                    "type": "section",
                    "text": {"type": "mrkdwn", "text": f"`{message}`"},
                }
            ],
        )
    except Exception as e:
        console.print(f"[yellow]⚠️  Failed to send Slack notification: {e}[/yellow]")


@ssh_app.command(name="submit")
def submit_job(
    script_path: Annotated[Path, typer.Argument(help="Path to sbatch script file")],
    # Connection options
    host: Annotated[
        str | None, typer.Option("--host", "-H", help="SSH host from .ssh/config")
    ] = None,
    profile: Annotated[
        str | None, typer.Option("--profile", "-p", help="Use saved profile")
    ] = None,
    hostname: Annotated[
        str | None, typer.Option("--hostname", help="DGX server hostname")
    ] = None,
    username: Annotated[
        str | None, typer.Option("--username", help="SSH username")
    ] = None,
    key_file: Annotated[
        str | None, typer.Option("--key-file", help="SSH private key file path")
    ] = None,
    port: Annotated[int, typer.Option("--port", help="SSH port")] = 22,
    config: Annotated[
        str | None,
        typer.Option(
            "--config", help="Config file path (default: ~/.config/srunx/config.json)"
        ),
    ] = None,
    ssh_config: Annotated[
        str | None,
        typer.Option(
            "--ssh-config", help="SSH config file path (default: ~/.ssh/config)"
        ),
    ] = None,
    # Job options
    job_name: Annotated[str | None, typer.Option("--job-name", help="Job name")] = None,
    poll_interval: Annotated[
        int,
        typer.Option(
            "--poll-interval", "-i", help="Job status polling interval in seconds"
        ),
    ] = 10,
    timeout: Annotated[
        int | None, typer.Option("--timeout", help="Job monitoring timeout in seconds")
    ] = None,
    no_monitor: Annotated[
        bool, typer.Option("--no-monitor", help="Submit job without monitoring")
    ] = False,
    no_cleanup: Annotated[
        bool, typer.Option("--no-cleanup", help="Do not cleanup uploaded script files")
    ] = False,
    # Environment options
    env: Annotated[
        list[str] | None,
        typer.Option(
            "--env", help="Environment variable (KEY=VALUE, can be used multiple times)"
        ),
    ] = None,
    env_local: Annotated[
        list[str] | None,
        typer.Option(
            "--env-local",
            help="Local environment variable key (can be used multiple times)",
        ),
    ] = None,
    # Notification options
    slack: Annotated[
        bool, typer.Option("--slack", help="Send notifications to Slack")
    ] = False,
    # Other options
    verbose: Annotated[
        bool, typer.Option("--verbose", "-v", help="Enable verbose logging")
    ] = False,
):
    """Submit a SLURM job script to a remote server via SSH."""
    setup_logging(verbose)

    # Validate script path
    script_path = script_path.resolve()
    if not script_path.exists():
        console.print(f"[red]Error: Script file '{script_path}' not found[/red]")
        raise typer.Exit(1)

    try:
        config_manager = ConfigManager(config)
        connection_params, display_host = _determine_connection_params(
            host,
            profile,
            hostname,
            username,
            key_file,
            port,
            ssh_config,
            config_manager,
        )

        # Setup Slack notification if requested
        slack_client = None
        if slack:
            if not SLACK_AVAILABLE:
                console.print(
                    "[red]❌ Slack SDK not available. Install with: pip install slack-sdk[/red]"
                )
                raise typer.Exit(1)

            webhook_url = os.getenv("SLACK_WEBHOOK_URL")
            if not webhook_url:
                console.print(
                    "[red]❌ SLACK_WEBHOOK_URL environment variable is not set[/red]"
                )
                raise typer.Exit(1)

            slack_client = WebhookClient(webhook_url)
            if verbose:
                console.print("[dim]✅ Slack notifications enabled[/dim]")

        # Process environment variables
        env_vars = _process_environment_variables(
            env, env_local, profile, config_manager, verbose
        )

        # Create and connect client
        client = _create_ssh_client(connection_params, env_vars, verbose)

        # Show connection status
        with Status("[blue]Connecting to server...", console=console):
            if not client.connect():
                console.print("[red]❌ Failed to connect to server[/red]")
                console.print(
                    "[yellow]Please check your connection parameters and SSH credentials[/yellow]"
                )
                raise typer.Exit(1)
            console.print(f"[green]✅ Connected to {display_host}[/green]")

        try:
            # Submit job
            with Status("[blue]Submitting job...", console=console):
                job = client.submit_sbatch_file(
                    script_path=str(script_path),
                    job_name=job_name,
                    cleanup=not no_cleanup,
                )

            if not job:
                console.print("[red]❌ Failed to submit job[/red]")
                raise typer.Exit(1)

            # Show job submission success
            job_panel = Panel(
                f"[green]Job ID:[/green] {job.job_id}\n"
                f"[blue]Name:[/blue] {job.name}\n"
                f"[yellow]Script:[/yellow] {script_path}",
                title="🚀 Job Submitted Successfully",
                border_style="green",
            )
            console.print(job_panel)

            # Send Slack notification for job submission
            if slack_client:
                send_slack_notification(
                    slack_client,
                    f"⚡ SUBMITTED     Job {job.name:<12} (ID: {job.job_id}) on {display_host}",
                )

            # Monitor job if requested
            if not no_monitor:
                _monitor_job_with_rich(
                    client, job, poll_interval, timeout, slack_client, display_host
                )

        finally:
            client.disconnect()
            if verbose:
                console.print("[dim]🔌 Disconnected from server[/dim]")

    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        raise typer.Exit(1) from e


@ssh_app.command("test")
def test_connection(
    # Connection options
    host: Annotated[
        str | None, typer.Option("--host", "-H", help="SSH host from .ssh/config")
    ] = None,
    profile: Annotated[
        str | None, typer.Option("--profile", "-p", help="Use saved profile")
    ] = None,
    hostname: Annotated[
        str | None, typer.Option("--hostname", help="DGX server hostname")
    ] = None,
    username: Annotated[
        str | None, typer.Option("--username", help="SSH username")
    ] = None,
    key_file: Annotated[
        str | None, typer.Option("--key-file", help="SSH private key file path")
    ] = None,
    port: Annotated[int, typer.Option("--port", help="SSH port")] = 22,
    config: Annotated[
        str | None,
        typer.Option(
            "--config", help="Config file path (default: ~/.config/srunx/config.json)"
        ),
    ] = None,
    ssh_config: Annotated[
        str | None,
        typer.Option(
            "--ssh-config", help="SSH config file path (default: ~/.ssh/config)"
        ),
    ] = None,
    verbose: Annotated[
        bool, typer.Option("--verbose", "-v", help="Enable verbose logging")
    ] = False,
):
    """Test SSH connection and SLURM availability."""
    from rich.table import Table

    setup_logging(verbose)

    try:
        config_manager = ConfigManager(config)
        connection_params, display_host = _determine_connection_params(
            host,
            profile,
            hostname,
            username,
            key_file,
            port,
            ssh_config,
            config_manager,
        )

        # Show connection info
        console.print("\n[bold]Testing SSH connection to:[/bold]")
        console.print(f"  Hostname: {connection_params['hostname']}")
        console.print(f"  Username: {connection_params['username']}")
        console.print(f"  Port: {connection_params.get('port', 22)}")
        console.print(f"  Key file: {connection_params.get('key_filename', 'None')}")
        if connection_params.get("proxy_jump"):
            console.print(f"  ProxyJump: {connection_params['proxy_jump']}")
        console.print()

        # Test connection
        with Status(
            "[bold yellow]Testing connection...[/bold yellow]", console=console
        ):
            client = _create_ssh_client(connection_params, {}, verbose)
            result = client.test_connection()

        # Display results
        table = Table(title="Connection Test Results", show_header=True)
        table.add_column("Check", style="cyan", no_wrap=True)
        table.add_column("Status", style="magenta")
        table.add_column("Details", style="white")

        # SSH connection
        ssh_status = "✅ Connected" if result["ssh_connected"] else "❌ Failed"
        ssh_details = ""
        if result["ssh_connected"]:
            ssh_details = f"Host: {result['hostname']}, User: {result['user']}"
        elif "error" in result:
            ssh_details = str(result["error"])

        table.add_row("SSH Connection", ssh_status, ssh_details)

        console.print()
        console.print(table)
        console.print()

        # Summary
        if result["ssh_connected"]:
            console.print(
                Panel(
                    "[bold green]✅ Connection test successful![/bold green]\n"
                    "SSH connection is working.",
                    title="Success",
                    border_style="green",
                )
            )
        elif result["ssh_connected"]:
            console.print(
                Panel(
                    "[bold yellow]⚠️  Partial success[/bold yellow]\n"
                    "SSH connection is working.",
                    title="Warning",
                    border_style="yellow",
                )
            )
            raise typer.Exit(1)
        else:
            console.print(
                Panel(
                    "[bold red]❌ Connection test failed[/bold red]\n"
                    f"Error: {result.get('error', 'Unknown error')}",
                    title="Failed",
                    border_style="red",
                )
            )
            raise typer.Exit(1)

    except Exception as e:
        console.print(f"\n[red]Error: {e}[/red]")
        raise typer.Exit(1) from e


@ssh_app.callback(invoke_without_command=True)
def ssh_main(ctx: typer.Context):
    """
    Submit and monitor SLURM jobs on remote servers via SSH.

    This command allows you to submit local script files to remote SLURM servers
    and monitor their execution. It supports SSH config hosts, connection profiles,
    and direct connections.

    Examples:
      srunx ssh test --host dgx-server
      srunx ssh submit train.py --host dgx-server
      srunx ssh submit experiment.sh --profile ml-cluster
      srunx ssh submit script.py --hostname server.com --username user --key-file ~/.ssh/key
    """
    # If no subcommand is invoked, show help
    if ctx.invoked_subcommand is None:
        console.print(ctx.get_help())
        raise typer.Exit()


# Profile management commands
@profile_app.command("list")
def list_profiles(
    config: Annotated[
        str | None,
        typer.Option(
            "--config", help="Config file path (default: ~/.config/srunx/config.json)"
        ),
    ] = None,
):
    """List all connection profiles."""
    from .profile_impl import list_profiles_impl

    list_profiles_impl(config)


@profile_app.command("add")
def add_profile(
    name: Annotated[str, typer.Argument(help="Profile name")],
    ssh_host: Annotated[
        str | None,
        typer.Option("--ssh-host", help="SSH config host name (from ~/.ssh/config)"),
    ] = None,
    hostname: Annotated[
        str | None, typer.Option("--hostname", help="Server hostname")
    ] = None,
    username: Annotated[
        str | None, typer.Option("--username", help="SSH username")
    ] = None,
    key_file: Annotated[
        str | None, typer.Option("--key-file", help="SSH private key file path")
    ] = None,
    port: Annotated[int, typer.Option("--port", help="SSH port")] = 22,
    proxy_jump: Annotated[
        str | None, typer.Option("--proxy-jump", help="ProxyJump host")
    ] = None,
    description: Annotated[
        str | None, typer.Option("--description", help="Profile description")
    ] = None,
    config: Annotated[
        str | None,
        typer.Option(
            "--config", help="Config file path (default: ~/.config/srunx/config.json)"
        ),
    ] = None,
):
    """Add a new connection profile."""

    add_profile_impl(
        name,
        ssh_host,
        hostname,
        username,
        key_file,
        port,
        proxy_jump,
        description,
        config,
    )


@profile_app.command("remove")
def remove_profile(
    name: Annotated[str, typer.Argument(help="Profile name")],
    config: Annotated[
        str | None,
        typer.Option(
            "--config", help="Config file path (default: ~/.config/srunx/config.json)"
        ),
    ] = None,
):
    """Remove a connection profile."""
    from .profile_impl import remove_profile_impl

    remove_profile_impl(name, config)


@profile_app.command("set")
def set_current_profile(
    name: Annotated[str, typer.Argument(help="Profile name")],
    config: Annotated[
        str | None,
        typer.Option(
            "--config", help="Config file path (default: ~/.config/srunx/config.json)"
        ),
    ] = None,
):
    """Set the current default profile."""
    from .profile_impl import set_current_profile_impl

    set_current_profile_impl(name, config)


@profile_app.command("show")
def show_profile(
    name: Annotated[
        str | None, typer.Argument(help="Profile name (default: current)")
    ] = None,
    config: Annotated[
        str | None,
        typer.Option(
            "--config", help="Config file path (default: ~/.config/srunx/config.json)"
        ),
    ] = None,
):
    """Show profile details."""
    from .profile_impl import show_profile_impl

    show_profile_impl(name, config)


@profile_app.command("update")
def update_profile(
    name: Annotated[str, typer.Argument(help="Profile name")],
    ssh_host: Annotated[
        str | None, typer.Option("--ssh-host", help="SSH config host name")
    ] = None,
    hostname: Annotated[
        str | None, typer.Option("--hostname", help="Server hostname")
    ] = None,
    username: Annotated[
        str | None, typer.Option("--username", help="SSH username")
    ] = None,
    key_file: Annotated[
        str | None, typer.Option("--key-file", help="SSH private key file path")
    ] = None,
    port: Annotated[int | None, typer.Option("--port", help="SSH port")] = None,
    proxy_jump: Annotated[
        str | None, typer.Option("--proxy-jump", help="ProxyJump host")
    ] = None,
    description: Annotated[
        str | None, typer.Option("--description", help="Profile description")
    ] = None,
    config: Annotated[
        str | None,
        typer.Option(
            "--config", help="Config file path (default: ~/.config/srunx/config.json)"
        ),
    ] = None,
):
    """Update an existing profile."""
    from .profile_impl import update_profile_impl

    update_profile_impl(
        name,
        ssh_host,
        hostname,
        username,
        key_file,
        port,
        proxy_jump,
        description,
        config,
    )


# Environment variable management for profiles
profile_env_app = typer.Typer(
    name="env",
    help="Manage environment variables for profiles",
)
profile_app.add_typer(profile_env_app, name="env")


@profile_env_app.command("set")
def set_env_var(
    profile_name: Annotated[str, typer.Argument(help="Profile name")],
    key: Annotated[str, typer.Argument(help="Environment variable name")],
    value: Annotated[str, typer.Argument(help="Environment variable value")],
    config: Annotated[
        str | None,
        typer.Option(
            "--config", help="Config file path (default: ~/.config/srunx/config.json)"
        ),
    ] = None,
):
    """Set an environment variable for a profile."""
    from .profile_impl import set_env_var_impl

    set_env_var_impl(profile_name, key, value, config)


@profile_env_app.command("unset")
def unset_env_var(
    profile_name: Annotated[str, typer.Argument(help="Profile name")],
    key: Annotated[str, typer.Argument(help="Environment variable name")],
    config: Annotated[
        str | None,
        typer.Option(
            "--config", help="Config file path (default: ~/.config/srunx/config.json)"
        ),
    ] = None,
):
    """Unset an environment variable for a profile."""
    from .profile_impl import unset_env_var_impl

    unset_env_var_impl(profile_name, key, config)


@profile_env_app.command("list")
def list_env_vars(
    profile_name: Annotated[str, typer.Argument(help="Profile name")],
    config: Annotated[
        str | None,
        typer.Option(
            "--config", help="Config file path (default: ~/.config/srunx/config.json)"
        ),
    ] = None,
):
    """List environment variables for a profile."""
    from .profile_impl import list_env_vars_impl

    list_env_vars_impl(profile_name, config)


def _determine_connection_params(
    host: str | None,
    profile: str | None,
    hostname: str | None,
    username: str | None,
    key_file: str | None,
    port: int,
    ssh_config: str | None,
    config_manager: ConfigManager,
) -> tuple[dict, str]:
    """Determine connection parameters and display host name."""
    connection_params = {}
    display_host = None

    if host:
        # Use SSH config host
        ssh_host = get_ssh_config_host(host, ssh_config)
        if not ssh_host:
            console.print(f"[red]Error: SSH host '{host}' not found[/red]")
            raise typer.Exit(1)

        connection_params = {
            "hostname": ssh_host.hostname,
            "username": ssh_host.user,
            "key_filename": ssh_host.identity_file,
            "port": ssh_host.port,
            "proxy_jump": ssh_host.proxy_jump,
        }
        display_host = host

    elif profile:
        # Use saved profile
        profile_obj = config_manager.get_profile(profile)
        if not profile_obj:
            console.print(f"[red]Error: Profile '{profile}' not found[/red]")
            raise typer.Exit(1)

        if profile_obj.ssh_host:
            # Profile uses SSH config host
            ssh_host = get_ssh_config_host(profile_obj.ssh_host, ssh_config)
            if not ssh_host:
                console.print(
                    f"[red]Error: SSH host '{profile_obj.ssh_host}' not found[/red]"
                )
                raise typer.Exit(1)
            connection_params = {
                "hostname": ssh_host.hostname,
                "username": ssh_host.user,
                "key_filename": ssh_host.identity_file,
                "port": ssh_host.port,
                "proxy_jump": ssh_host.proxy_jump,
            }
            display_host = f"{profile} ({profile_obj.ssh_host})"
        else:
            # Profile uses direct connection
            connection_params = {
                "hostname": profile_obj.hostname,
                "username": profile_obj.username,
                "key_filename": profile_obj.key_filename,
                "port": profile_obj.port,
                "proxy_jump": profile_obj.proxy_jump,
            }
            display_host = profile

    elif all([hostname, username, key_file]):
        # Use direct parameters
        assert key_file is not None  # Type guard after all() check
        key_path = config_manager.expand_path(key_file)
        if not Path(key_path).exists():
            console.print(f"[red]Error: SSH key file '{key_path}' not found[/red]")
            raise typer.Exit(1)

        connection_params = {
            "hostname": hostname,
            "username": username,
            "key_filename": key_path,
            "port": port,
        }
        display_host = hostname
    else:
        # Try current profile as fallback
        profile_obj = config_manager.get_current_profile()
        if profile_obj:
            if profile_obj.ssh_host:
                # Profile uses SSH config host
                ssh_host = get_ssh_config_host(profile_obj.ssh_host, ssh_config)
                if not ssh_host:
                    console.print(
                        f"[red]Error: SSH host '{profile_obj.ssh_host}' not found[/red]"
                    )
                    raise typer.Exit(1)
                connection_params = {
                    "hostname": ssh_host.hostname,
                    "username": ssh_host.user,
                    "key_filename": ssh_host.identity_file,
                    "port": ssh_host.port,
                    "proxy_jump": ssh_host.proxy_jump,
                }
                display_host = f"current ({profile_obj.ssh_host})"
            else:
                # Profile uses direct connection
                connection_params = {
                    "hostname": profile_obj.hostname,
                    "username": profile_obj.username,
                    "key_filename": profile_obj.key_filename,
                    "port": profile_obj.port,
                    "proxy_jump": profile_obj.proxy_jump,
                }
                display_host = "current"
        else:
            console.print("[red]Error: No connection method specified[/red]")
            console.print(
                "[yellow]Use --host, --profile, or provide --hostname/--username/--key-file[/yellow]"
            )
            raise typer.Exit(1)

    return connection_params, display_host or str(connection_params["hostname"])


def _process_environment_variables(
    env: list[str] | None,
    env_local: list[str] | None,
    profile: str | None,
    config_manager: ConfigManager,
    verbose: bool,
) -> dict[str, str]:
    """Process and collect environment variables."""
    env_vars = {}

    # Add profile-specific environment variables first (if using profile)
    current_profile = None
    if profile:
        current_profile = config_manager.get_profile(profile)
    else:
        # Using current profile as fallback
        current_profile = config_manager.get_current_profile()

    if current_profile and current_profile.env_vars:
        env_vars.update(current_profile.env_vars)
        if verbose:
            console.print(
                f"Added {len(current_profile.env_vars)} environment variables from profile"
            )

    # Auto-detect common environment variables
    common_env_vars = [
        "HF_TOKEN",
        "HUGGING_FACE_HUB_TOKEN",
        "WANDB_API_KEY",
        "WANDB_ENTITY",
        "WANDB_PROJECT",
        "OPENAI_API_KEY",
        "ANTHROPIC_API_KEY",
        "CUDA_VISIBLE_DEVICES",
        "HF_HOME",
        "HF_HUB_CACHE",
        "TRANSFORMERS_CACHE",
        "TORCH_HOME",
        "SLURM_LOG_DIR",
    ]

    for key in common_env_vars:
        if key in os.environ:
            env_vars[key] = os.environ[key]
            if verbose:
                console.print(f"Auto-detected environment variable: {key}")

    # Add explicitly provided environment variables
    if env:
        for env_var in env:
            if "=" not in env_var:
                console.print(
                    f"[red]Error: Invalid environment variable format: {env_var}[/red]"
                )
                raise typer.Exit(1)
            key, value = env_var.split("=", 1)
            env_vars[key] = value

    # Add explicitly requested local environment variables
    if env_local:
        for key in env_local:
            if key in os.environ:
                env_vars[key] = os.environ[key]
            else:
                console.print(
                    f"[yellow]Warning: Local environment variable '{key}' not found[/yellow]"
                )

    return env_vars


def _create_ssh_client(
    connection_params: dict, env_vars: dict[str, str], verbose: bool
) -> SSHSlurmClient:
    """Create SSH SLURM client with proper type handling."""
    hostname = str(connection_params["hostname"])
    username = str(connection_params["username"])
    key_filename_raw = connection_params.get("key_filename")
    key_filename = key_filename_raw if isinstance(key_filename_raw, str) else None
    raw_port = connection_params.get("port")
    port = int(raw_port) if raw_port is not None else 22
    proxy_jump_raw = connection_params.get("proxy_jump")
    proxy_jump = proxy_jump_raw if isinstance(proxy_jump_raw, str) else None

    return SSHSlurmClient(
        hostname=hostname,
        username=username,
        key_filename=key_filename,
        port=port,
        proxy_jump=proxy_jump,
        env_vars=env_vars,
        verbose=verbose,
    )


def _monitor_job_with_rich(
    client: SSHSlurmClient,
    job,
    poll_interval: int,
    timeout: int | None,
    slack_client=None,
    display_host: str | None = None,
):
    """Monitor job with rich progress display."""
    start_time = time.time()

    # Status mapping for display
    status_colors = {
        "PENDING": "yellow",
        "RUNNING": "blue",
        "COMPLETED": "green",
        "FAILED": "red",
        "CANCELLED": "orange3",
        "TIMEOUT": "red",
        "NOT_FOUND": "red",
    }

    status_icons = {
        "PENDING": "⏳",
        "RUNNING": "🏃",
        "COMPLETED": "✅",
        "FAILED": "❌",
        "CANCELLED": "🚫",
        "TIMEOUT": "⏰",
        "NOT_FOUND": "❓",
    }

    # Create progress display
    progress = Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        console=console,
    )

    with Live(progress, console=console, refresh_per_second=1):
        task = progress.add_task("Monitoring job...")

        while True:
            current_time = time.time()
            elapsed_time = current_time - start_time

            # Get job status
            job.status = client.get_job_status(job.job_id)

            # Update progress description
            color = status_colors.get(job.status, "white")
            icon = status_icons.get(job.status, "❓")

            progress.update(
                task,
                description=f"{icon} Job {job.job_id}: [{color}]{job.status}[/{color}] (Elapsed: {elapsed_time:.0f}s)",
            )

            # Check if job is finished
            if job.status in [
                "COMPLETED",
                "FAILED",
                "CANCELLED",
                "TIMEOUT",
                "NOT_FOUND",
            ]:
                break

            # Check timeout
            if timeout and elapsed_time > timeout:
                progress.update(
                    task,
                    description=f"⏰ Job {job.job_id}: [orange3]TIMEOUT[/orange3] (Monitoring timed out after {timeout}s)",
                )
                break

            time.sleep(poll_interval)

    # Show final result
    final_color = status_colors.get(job.status, "white")
    final_icon = status_icons.get(job.status, "❓")

    result_panel = Panel(
        f"{final_icon} [bold {final_color}]{job.status}[/bold {final_color}]\n"
        f"[dim]Job ID: {job.job_id}\n"
        f"Total time: {elapsed_time:.1f} seconds[/dim]",
        title=f"🏁 Job {job.job_id} Finished",
        border_style=final_color,
    )
    console.print(result_panel)

    # Send Slack notification for job completion
    if slack_client:
        status_icon = status_icons.get(job.status, "❓")
        host_info = f" on {display_host}" if display_host else ""
        send_slack_notification(
            slack_client,
            f"{status_icon} {job.status:<12} Job {job.name:<12} (ID: {job.job_id}){host_info} - Total time: {elapsed_time:.1f}s",
        )

    # Show logs if job failed or had errors
    if job.status in ["FAILED", "CANCELLED", "TIMEOUT"]:
        _show_job_logs(client, job)


def _show_job_logs(client: SSHSlurmClient, job):
    """Show job logs with rich formatting when job fails."""
    console.print("\n[yellow]📋 Retrieving job logs...[/yellow]")

    # Get detailed log information
    log_info = client.get_job_output_detailed(job.job_id, job.name)

    # Extract and validate values with proper type handling
    found_files = log_info.get("found_files", [])
    if not isinstance(found_files, list):
        found_files = []

    output = log_info.get("output", "")
    if not isinstance(output, str):
        output = ""

    error = log_info.get("error", "")
    if not isinstance(error, str):
        error = ""

    primary_log = log_info.get("primary_log")
    slurm_log_dir = log_info.get("slurm_log_dir")
    searched_dirs = log_info.get("searched_dirs", [])
    if not isinstance(searched_dirs, list):
        searched_dirs = []

    if not found_files:
        # No log files found
        no_logs_panel = Panel(
            "[red]❌ No log files found[/red]\n\n"
            "[dim]Searched in:[/dim]\n"
            + "\n".join([f"  • {d}" for d in searched_dirs])
            + f"\n\n[dim]SLURM_LOG_DIR: {slurm_log_dir or 'Not set'}[/dim]",
            title="📁 Log Search Results",
            border_style="red",
        )
        console.print(no_logs_panel)
        return

    # Show found log files info
    files_info = "\n".join([f"  📄 {f}" for f in found_files])
    info_panel = Panel(
        f"[green]Found {len(found_files)} log file(s):[/green]\n\n{files_info}\n\n"
        f"[dim]Primary log: {primary_log}\n"
        f"SLURM_LOG_DIR: {slurm_log_dir or 'Not set'}[/dim]",
        title="📁 Log Files Found",
        border_style="green",
    )
    console.print(info_panel)

    # Show primary log content
    if output:
        # Truncate very long output
        max_lines = 100
        lines = output.split("\n")
        display_output = output
        if len(lines) > max_lines:
            display_output = "\n".join(lines[-max_lines:])
            display_output += f"\n\n[dim]... (truncated, showing last {max_lines} lines of {len(lines)} total)[/dim]"

        # Try to detect if this is structured log output
        log_content: RenderableType
        if any(
            keyword in display_output.lower()
            for keyword in ["error", "traceback", "exception", "failed"]
        ):
            # Syntax highlight as generic log
            log_content = Syntax(
                display_output, "log", theme="monokai", line_numbers=True
            )
        else:
            # Plain text with some styling
            log_content = Text(display_output)

        log_panel = Panel(
            log_content,
            title=f"📄 Primary Log Content - {primary_log}",
            border_style="blue",
            expand=False,
        )
        console.print(log_panel)

    # Show error content if available
    if error:
        error_syntax = Syntax(error, "log", theme="monokai", line_numbers=True)
        error_panel = Panel(
            error_syntax, title="❌ Error Log Content", border_style="red", expand=False
        )
        console.print(error_panel)
