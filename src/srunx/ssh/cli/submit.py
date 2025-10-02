#!/usr/bin/env python3

import argparse
import logging
import sys
import time
from pathlib import Path

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

try:
    from slack_sdk import WebhookClient

    SLACK_AVAILABLE = True
except ImportError:
    SLACK_AVAILABLE = False

console = Console()


def send_slack_notification(slack_client, message: str, color: str = "good") -> None:
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


def setup_logging(verbose: bool = False):
    level = (
        logging.DEBUG if verbose else logging.WARNING
    )  # Only show warnings and errors by default
    logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s", level=level)


def run_from_argv(argv: list[str]) -> None:
    parser = argparse.ArgumentParser(
        description="Submit and monitor SLURM jobs via SSH"
    )

    parser.add_argument("script_path", help="Path to sbatch script file")

    # Connection options
    conn_group = parser.add_argument_group("Connection options")
    conn_group.add_argument("--host", "-H", help="SSH host from .ssh/config")
    conn_group.add_argument("--profile", "-p", help="Use saved profile")
    conn_group.add_argument("--hostname", help="DGX server hostname")
    conn_group.add_argument("--username", help="SSH username")
    conn_group.add_argument("--key-file", help="SSH private key file path")
    conn_group.add_argument(
        "--port", type=int, default=22, help="SSH port (default: 22)"
    )
    conn_group.add_argument(
        "--config", help="Config file path (default: ~/.config/srunx/config.json)"
    )
    conn_group.add_argument(
        "--ssh-config", help="SSH config file path (default: ~/.ssh/config)"
    )

    # Job options
    job_group = parser.add_argument_group("Job options")
    job_group.add_argument("--job-name", help="Job name")
    job_group.add_argument(
        "--poll-interval",
        "-i",
        type=int,
        default=10,
        help="Job status polling interval in seconds (default: 10)",
    )
    job_group.add_argument(
        "--timeout", type=int, help="Job monitoring timeout in seconds"
    )
    job_group.add_argument(
        "--no-monitor", action="store_true", help="Submit job without monitoring"
    )
    job_group.add_argument(
        "--no-cleanup", action="store_true", help="Do not cleanup uploaded script files"
    )

    # Environment options
    env_group = parser.add_argument_group("Environment options")
    env_group.add_argument(
        "--env",
        action="append",
        metavar="KEY=VALUE",
        help="Pass environment variable to remote job (can be used multiple times)",
    )
    env_group.add_argument(
        "--env-local",
        action="append",
        metavar="KEY",
        help="Pass local environment variable to remote job (can be used multiple times)",
    )

    # Notification options
    notif_group = parser.add_argument_group("Notification options")
    notif_group.add_argument(
        "--slack", action="store_true", help="Send notifications to Slack"
    )

    # Other options
    parser.add_argument(
        "--verbose", "-v", action="store_true", help="Enable verbose logging"
    )

    args = parser.parse_args(argv)

    setup_logging(args.verbose)

    # Validate script path
    script_path = Path(args.script_path).resolve()
    if not script_path.exists():
        print(f"Error: Script file '{script_path}' not found", file=sys.stderr)
        sys.exit(1)

    # Determine connection parameters
    try:
        config_manager = ConfigManager(args.config)
        connection_params = {}
        display_host = None  # For pretty display in connection message

        if args.host:
            # Use SSH config host
            ssh_host = get_ssh_config_host(args.host, args.ssh_config)
            if not ssh_host:
                print(f"Error: SSH host '{args.host}' not found", file=sys.stderr)
                sys.exit(1)

            connection_params = {
                "hostname": ssh_host.hostname,
                "username": ssh_host.user,
                "key_filename": ssh_host.identity_file,
                "port": ssh_host.port,
                "proxy_jump": ssh_host.proxy_jump,
            }
            display_host = args.host  # Use SSH config host name for display

        elif args.profile:
            # Use saved profile
            profile = config_manager.get_profile(args.profile)
            if not profile:
                print(f"Error: Profile '{args.profile}' not found", file=sys.stderr)
                sys.exit(1)

            if profile.ssh_host:
                # Profile uses SSH config host
                ssh_host = get_ssh_config_host(profile.ssh_host, args.ssh_config)
                if not ssh_host:
                    print(
                        f"Error: SSH host '{profile.ssh_host}' not found",
                        file=sys.stderr,
                    )
                    sys.exit(1)
                connection_params = {
                    "hostname": ssh_host.hostname,
                    "username": ssh_host.user,
                    "key_filename": ssh_host.identity_file,
                    "port": ssh_host.port,
                    "proxy_jump": ssh_host.proxy_jump,
                }
                display_host = (
                    f"{args.profile} ({profile.ssh_host})"  # Profile name with SSH host
                )
            else:
                # Profile uses direct connection
                connection_params = {
                    "hostname": profile.hostname,
                    "username": profile.username,
                    "key_filename": profile.key_filename,
                    "port": profile.port,
                }
                display_host = args.profile  # Use profile name for display

        elif all([args.hostname, args.username, args.key_file]):
            # Use direct parameters
            key_path = config_manager.expand_path(args.key_file)
            if not Path(key_path).exists():
                print(f"Error: SSH key file '{key_path}' not found", file=sys.stderr)
                sys.exit(1)

            connection_params = {
                "hostname": args.hostname,
                "username": args.username,
                "key_filename": key_path,
                "port": args.port,
            }
            display_host = args.hostname  # Use hostname for direct connection
        else:
            # Try current profile as fallback
            profile = config_manager.get_current_profile()
            if profile:
                if profile.ssh_host:
                    # Profile uses SSH config host
                    ssh_host = get_ssh_config_host(profile.ssh_host, args.ssh_config)
                    if not ssh_host:
                        print(
                            f"Error: SSH host '{profile.ssh_host}' not found",
                            file=sys.stderr,
                        )
                        sys.exit(1)
                    connection_params = {
                        "hostname": ssh_host.hostname,
                        "username": ssh_host.user,
                        "key_filename": ssh_host.identity_file,
                        "port": ssh_host.port,
                        "proxy_jump": ssh_host.proxy_jump,
                    }
                    display_host = (
                        f"current ({profile.ssh_host})"  # Current profile with SSH host
                    )
                else:
                    # Profile uses direct connection
                    connection_params = {
                        "hostname": profile.hostname,
                        "username": profile.username,
                        "key_filename": profile.key_filename,
                        "port": profile.port,
                    }
                    display_host = "current"  # Current profile for direct connection
            else:
                print("Error: No connection method specified", file=sys.stderr)
                print(
                    "Use --host, --profile, or provide --hostname/--username/--key-file",
                    file=sys.stderr,
                )
                sys.exit(1)

        # Process environment variables
        import os

        env_vars = {}

        # Add profile-specific environment variables first (if using profile)
        current_profile = None
        if args.profile:
            current_profile = config_manager.get_profile(args.profile)
        elif not any([args.host, args.hostname]):
            # Using current profile as fallback
            current_profile = config_manager.get_current_profile()

        if current_profile and current_profile.env_vars:
            env_vars.update(current_profile.env_vars)
            if args.verbose:
                print(
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
            "SLURM_LOG_DIR",  # Important for log file location
        ]

        for key in common_env_vars:
            if key in os.environ:
                env_vars[key] = os.environ[key]
                if args.verbose:
                    print(f"Auto-detected environment variable: {key}")

        # Add explicitly provided environment variables
        if args.env:
            for env_var in args.env:
                if "=" not in env_var:
                    print(
                        f"Error: Invalid environment variable format: {env_var}",
                        file=sys.stderr,
                    )
                    sys.exit(1)
                key, value = env_var.split("=", 1)
                env_vars[key] = value

        # Add explicitly requested local environment variables
        if args.env_local:
            for key in args.env_local:
                if key in os.environ:
                    env_vars[key] = os.environ[key]
                else:
                    print(
                        f"Warning: Local environment variable '{key}' not found",
                        file=sys.stderr,
                    )

        # Create client and connect with explicit typed parameters for mypy
        hostname = str(connection_params["hostname"])
        username = str(connection_params["username"])
        key_filename_raw = connection_params.get("key_filename")
        key_filename = key_filename_raw if isinstance(key_filename_raw, str) else None
        raw_port = connection_params.get("port")
        port = int(raw_port) if raw_port is not None else 22
        proxy_jump_raw = connection_params.get("proxy_jump")
        proxy_jump = proxy_jump_raw if isinstance(proxy_jump_raw, str) else None

        client = SSHSlurmClient(
            hostname=hostname,
            username=username,
            key_filename=key_filename,
            port=port,
            proxy_jump=proxy_jump,
            env_vars=env_vars,
            verbose=args.verbose,
        )

        # Fallback for display_host if not set
        if display_host is None:
            display_host = connection_params["hostname"]

        # Setup Slack notification if requested
        slack_client = None
        if args.slack:
            if not SLACK_AVAILABLE:
                console.print(
                    "[red]❌ Slack SDK not available. Install with: pip install slack-sdk[/red]"
                )
                sys.exit(1)

            webhook_url = os.getenv("SLACK_WEBHOOK_URL")
            if not webhook_url:
                console.print(
                    "[red]❌ SLACK_WEBHOOK_URL environment variable is not set[/red]"
                )
                sys.exit(1)

            slack_client = WebhookClient(webhook_url)
            if args.verbose:
                console.print("[dim]✅ Slack notifications enabled[/dim]")

        # Show connection status with rich
        with Status("[blue]Connecting to server...", console=console):
            if not client.connect():
                console.print("[red]❌ Failed to connect to server[/red]")
                console.print(
                    "[yellow]Please check your connection parameters and SSH credentials[/yellow]"
                )
                sys.exit(1)
            console.print(f"[green]✅ Connected to {display_host}[/green]")

        try:
            # Submit job with rich status
            with Status("[blue]Submitting job...", console=console):
                job = client.submit_sbatch_file(
                    script_path=str(script_path),
                    job_name=args.job_name,
                    cleanup=not args.no_cleanup,
                )

            if not job:
                console.print("[red]❌ Failed to submit job[/red]")
                sys.exit(1)

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
            if not args.no_monitor:
                _monitor_job_with_rich(
                    client,
                    job,
                    args.poll_interval,
                    args.timeout,
                    slack_client,
                    str(display_host) if display_host else None,
                )

        finally:
            # Always disconnect
            client.disconnect()
            if args.verbose:
                console.print("[dim]🔌 Disconnected from server[/dim]")

    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


def _monitor_job_with_rich(
    client: SSHSlurmClient,
    job,
    poll_interval: int,
    timeout: int | None,
    slack_client=None,
    display_host: str | None = None,
):
    """Monitor job with rich progress display"""
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

    # Create progress display (spinner-based since we don't know actual job progress)
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
    """Show job logs with rich formatting when job fails"""
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
