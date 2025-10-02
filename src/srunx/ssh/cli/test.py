#!/usr/bin/env python3

import argparse
import sys
from pathlib import Path

from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from ..core.client import SSHSlurmClient
from ..core.config import ConfigManager
from ..core.ssh_config import SSHConfigParser


def handle_test_command(argv: list[str]) -> None:
    """Handle SSH connection test command."""
    parser = argparse.ArgumentParser(
        prog="srunx ssh test",
        description="Test SSH connection and SLURM availability",
    )

    # Connection options
    conn_group = parser.add_argument_group("Connection options")
    conn_group.add_argument("--host", help="SSH config host name (from ~/.ssh/config)")
    conn_group.add_argument("--profile", help="Use saved connection profile")
    conn_group.add_argument("--hostname", help="Direct connection hostname")
    conn_group.add_argument("--username", help="SSH username")
    conn_group.add_argument("--key-file", help="SSH private key file path")
    conn_group.add_argument(
        "--port", type=int, default=22, help="SSH port (default: 22)"
    )
    conn_group.add_argument(
        "--config", help="Config file path (default: ~/.config/srunx/config.json)"
    )

    args = parser.parse_args(argv)
    console = Console()

    # Determine connection parameters
    hostname = None
    username = None
    key_filename = None
    port = args.port
    proxy_jump = None
    ssh_config_path = None

    try:
        if args.profile:
            # Load from profile
            config_manager = ConfigManager(config_path=args.config)
            profile = config_manager.get_profile(args.profile)

            if not profile:
                console.print(f"[red]Error: Profile '{args.profile}' not found[/red]")
                sys.exit(1)

            if profile.ssh_host:
                # Use SSH config
                ssh_config = SSHConfigParser()
                host_config = ssh_config.get_host(profile.ssh_host)
                if host_config:
                    hostname = host_config.hostname
                    username = host_config.user
                    key_filename = host_config.identity_file
                    port = int(host_config.port)
                    proxy_jump = host_config.proxy_jump
                    ssh_config_path = str(Path.home() / ".ssh" / "config")
                else:
                    console.print(
                        f"[red]Error: SSH host '{profile.ssh_host}' not found[/red]"
                    )
                    sys.exit(1)
            else:
                # Direct connection from profile
                hostname = profile.hostname
                username = profile.username
                key_filename = profile.key_filename
                port = profile.port or 22

        elif args.host:
            # Load from SSH config
            ssh_config = SSHConfigParser()
            host_config = ssh_config.get_host(args.host)

            if not host_config:
                console.print(f"[red]Error: SSH host '{args.host}' not found[/red]")
                sys.exit(1)

            hostname = host_config.hostname
            username = host_config.user
            key_filename = host_config.identity_file
            port = int(host_config.port)
            proxy_jump = host_config.proxy_jump
            ssh_config_path = str(Path.home() / ".ssh" / "config")

        elif args.hostname:
            # Direct connection parameters
            hostname = args.hostname
            username = args.username
            key_filename = args.key_file
            port = args.port

        else:
            # Try to use current profile
            config_manager = ConfigManager(config_path=args.config)
            if config_manager.config_data.get("current_profile"):
                profile_name = config_manager.config_data["current_profile"]
                profile = config_manager.get_profile(profile_name)

                if not profile:
                    console.print(
                        f"[red]Error: Profile '{profile_name}' not found[/red]"
                    )
                    sys.exit(1)

                if profile.ssh_host:
                    ssh_config = SSHConfigParser()
                    host_config = ssh_config.get_host(profile.ssh_host)

                    if host_config:
                        hostname = host_config.hostname
                        username = host_config.user or profile.username
                        key_filename = host_config.identity_file
                        port = int(host_config.port)
                        proxy_jump = host_config.proxy_jump
                        ssh_config_path = str(Path.home() / ".ssh" / "config")
                    else:
                        console.print(
                            f"[red]Error: SSH host '{profile.ssh_host}' not found[/red]"
                        )
                        sys.exit(1)
                else:
                    hostname = profile.hostname
                    username = profile.username
                    key_filename = profile.key_filename
                    port = profile.port or 22
            else:
                console.print(
                    "[red]Error:[/red] No connection parameters provided. "
                    "Use --host, --profile, --hostname, or set a current profile."
                )
                sys.exit(1)

        if not hostname or not username:
            console.print(
                "[red]Error:[/red] Missing required connection parameters (hostname, username)"
            )
            sys.exit(1)

        # Show connection info
        console.print("\n[bold]Testing SSH connection to:[/bold]")
        console.print(f"  Hostname: {hostname}")
        console.print(f"  Username: {username}")
        console.print(f"  Port: {port}")
        if proxy_jump:
            console.print(f"  ProxyJump: {proxy_jump}")
        console.print()

        # Test connection
        with console.status("[bold yellow]Testing connection...[/bold yellow]"):
            client = SSHSlurmClient(
                hostname=hostname,
                username=username,
                key_filename=key_filename,
                port=port,
                proxy_jump=proxy_jump,
                ssh_config_path=ssh_config_path,
            )

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
                    "SSH connection is working but SLURM is not available.",
                    title="Warning",
                    border_style="yellow",
                )
            )
            sys.exit(1)
        else:
            console.print(
                Panel(
                    "[bold red]❌ Connection test failed[/bold red]\n"
                    f"Error: {result.get('error', 'Unknown error')}",
                    title="Failed",
                    border_style="red",
                )
            )
            sys.exit(1)

    except Exception as e:
        console.print(f"\n[red]Error:[/red] {e}")
        sys.exit(1)
