#!/usr/bin/env python3
"""
Profile management implementations for typer-based SSH CLI.

This module contains the actual implementation functions for profile management,
separated from the CLI command definitions for better organization.
"""

import typer
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from ..core.config import ConfigManager, ServerProfile

console = Console()


def list_profiles_impl(config: str | None = None):
    """Implementation for listing all profiles."""
    try:
        config_manager = ConfigManager(config)
        profiles = config_manager.list_profiles()
        current_profile_name = config_manager.get_current_profile_name()

        if not profiles:
            console.print("[yellow]No profiles found.[/yellow]")
            console.print("[dim]Use 'srunx ssh profile add' to create a profile.[/dim]")
            return

        # Create table
        table = Table(title="SSH Connection Profiles")
        table.add_column("Name", style="cyan", no_wrap=True)
        table.add_column("Connection", style="magenta")
        table.add_column("Description", style="green")
        table.add_column("Status", style="yellow")

        for name, profile in profiles.items():
            # Determine connection info
            if profile.ssh_host:
                connection = f"SSH Config: {profile.ssh_host}"
            else:
                connection = f"{profile.username}@{profile.hostname}:{profile.port}"

            # Current profile indicator
            status = "Current" if name == current_profile_name else ""

            table.add_row(
                name,
                connection,
                profile.description or "[dim]No description[/dim]",
                status,
            )

        console.print(table)

        if current_profile_name:
            console.print(f"\n[dim]Current profile: {current_profile_name}[/dim]")

    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        raise typer.Exit(1) from e


def add_profile_impl(
    name: str,
    ssh_host: str | None = None,
    hostname: str | None = None,
    username: str | None = None,
    key_file: str | None = None,
    port: int = 22,
    proxy_jump: str | None = None,
    description: str | None = None,
    config: str | None = None,
):
    """Implementation for adding a new profile."""
    try:
        config_manager = ConfigManager(config)

        # Validate connection parameters
        if ssh_host:
            # SSH config host mode - other params are optional
            pass
        elif all([hostname, username, key_file]):
            # Direct connection mode - all required
            pass
        else:
            console.print(
                "[red]Error: Either --ssh-host or all of --hostname/--username/--key-file must be provided[/red]"
            )
            raise typer.Exit(1)

        # Check if profile already exists
        if config_manager.get_profile(name):
            console.print(f"[red]Error: Profile '{name}' already exists[/red]")
            console.print(
                "[dim]Use 'srunx ssh profile update' to modify existing profiles[/dim]"
            )
            raise typer.Exit(1)

        # Create ServerProfile object
        profile = ServerProfile(
            hostname=hostname or "",
            username=username or "",
            key_filename=key_file or "",
            port=port,
            description=description,
            ssh_host=ssh_host,
            proxy_jump=proxy_jump,
            env_vars={},
        )

        # Add the profile
        config_manager.add_profile(name, profile)
        success = True

        if success:
            console.print(f"[green]✅ Profile '{name}' added successfully[/green]")

            # Show what was added
            profile_info = []
            if ssh_host:
                profile_info.append(f"SSH Host: {ssh_host}")
            else:
                profile_info.append(f"Hostname: {hostname}")
                profile_info.append(f"Username: {username}")
                profile_info.append(f"Key File: {key_file}")
                if port != 22:
                    profile_info.append(f"Port: {port}")
            if proxy_jump:
                profile_info.append(f"ProxyJump: {proxy_jump}")

            if description:
                profile_info.append(f"Description: {description}")

            info_panel = Panel(
                "\n".join(profile_info),
                title=f"Profile '{name}' Details",
                border_style="green",
            )
            console.print(info_panel)

            console.print(
                "[dim]Use 'srunx ssh profile set' to make this the current profile[/dim]"
            )
        else:
            console.print(f"[red]❌ Failed to add profile '{name}'[/red]")
            raise typer.Exit(1)

    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        raise typer.Exit(1) from e


def remove_profile_impl(name: str, config: str | None = None):
    """Implementation for removing a profile."""
    try:
        config_manager = ConfigManager(config)

        # Check if profile exists
        profile = config_manager.get_profile(name)
        if not profile:
            console.print(f"[red]Error: Profile '{name}' not found[/red]")
            raise typer.Exit(1)

        # Confirm removal
        current_profile_name = config_manager.get_current_profile_name()
        if name == current_profile_name:
            console.print(f"[yellow]Warning: '{name}' is the current profile[/yellow]")

        # Remove the profile
        success = config_manager.remove_profile(name)

        if success:
            console.print(f"[green]✅ Profile '{name}' removed successfully[/green]")

            if name == current_profile_name:
                console.print(
                    "[yellow]No current profile set (removed profile was current)[/yellow]"
                )
                console.print(
                    "[dim]Use 'srunx ssh profile set' to set a new current profile[/dim]"
                )
        else:
            console.print(f"[red]❌ Failed to remove profile '{name}'[/red]")
            raise typer.Exit(1)

    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        raise typer.Exit(1) from e


def set_current_profile_impl(name: str, config: str | None = None):
    """Implementation for setting the current profile."""
    try:
        config_manager = ConfigManager(config)

        # Check if profile exists
        profile = config_manager.get_profile(name)
        if not profile:
            console.print(f"[red]Error: Profile '{name}' not found[/red]")
            console.print(
                "[dim]Use 'srunx ssh profile list' to see available profiles[/dim]"
            )
            raise typer.Exit(1)

        # Set current profile
        success = config_manager.set_current_profile(name)

        if success:
            console.print(f"[green]✅ Current profile set to '{name}'[/green]")

            # Show profile details
            profile_info = []
            if profile.ssh_host:
                profile_info.append(f"SSH Host: {profile.ssh_host}")
            else:
                profile_info.append(f"Hostname: {profile.hostname}")
                profile_info.append(f"Username: {profile.username}")
                if profile.port != 22:
                    profile_info.append(f"Port: {profile.port}")

            if profile.description:
                profile_info.append(f"Description: {profile.description}")

            info_panel = Panel(
                "\n".join(profile_info),
                title=f"Current Profile: {name}",
                border_style="green",
            )
            console.print(info_panel)
        else:
            console.print(f"[red]❌ Failed to set current profile to '{name}'[/red]")
            raise typer.Exit(1)

    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        raise typer.Exit(1) from e


def show_profile_impl(name: str | None = None, config: str | None = None):
    """Implementation for showing profile details."""
    try:
        config_manager = ConfigManager(config)

        # Determine which profile to show
        profile_name: str
        if name:
            profile = config_manager.get_profile(name)
            if not profile:
                console.print(f"[red]Error: Profile '{name}' not found[/red]")
                raise typer.Exit(1)
            profile_name = name
        else:
            # Show current profile
            profile = config_manager.get_current_profile()
            current_name = config_manager.get_current_profile_name()
            if not profile or not current_name:
                console.print("[yellow]No current profile set[/yellow]")
                console.print(
                    "[dim]Use 'srunx ssh profile set' to set a current profile[/dim]"
                )
                raise typer.Exit(1)
            profile_name = current_name

        # Create detailed info
        details = []

        # Connection info
        details.append("[bold cyan]Connection Details:[/bold cyan]")
        if profile.ssh_host:
            details.append(f"  SSH Config Host: {profile.ssh_host}")
        else:
            details.append(f"  Hostname: {profile.hostname}")
            details.append(f"  Username: {profile.username}")
            details.append(f"  Key File: {profile.key_filename}")
            details.append(f"  Port: {profile.port}")

        if profile.description:
            details.append("\n[bold green]Description:[/bold green]")
            details.append(f"  {profile.description}")

        # Environment variables
        if profile.env_vars:
            details.append("\n[bold yellow]Environment Variables:[/bold yellow]")
            for key, value in profile.env_vars.items():
                # Hide sensitive values
                if any(
                    sensitive in key.upper()
                    for sensitive in ["TOKEN", "KEY", "SECRET", "PASSWORD"]
                ):
                    display_value = "***HIDDEN***"
                else:
                    display_value = value
                details.append(f"  {key}={display_value}")
        else:
            details.append("\n[bold yellow]Environment Variables:[/bold yellow]")
            details.append("  [dim]None configured[/dim]")

        # Current profile indicator
        current_profile_name = config_manager.get_current_profile_name()
        title = f"Profile: {profile_name}"
        if profile_name == current_profile_name:
            title += " (Current)"

        profile_panel = Panel(
            "\n".join(details),
            title=title,
            border_style="cyan",
        )
        console.print(profile_panel)

    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        raise typer.Exit(1) from e


def update_profile_impl(
    name: str,
    ssh_host: str | None = None,
    hostname: str | None = None,
    username: str | None = None,
    key_file: str | None = None,
    port: int | None = None,
    proxy_jump: str | None = None,
    description: str | None = None,
    config: str | None = None,
):
    """Implementation for updating an existing profile."""
    try:
        config_manager = ConfigManager(config)

        # Check if profile exists
        profile = config_manager.get_profile(name)
        if not profile:
            console.print(f"[red]Error: Profile '{name}' not found[/red]")
            raise typer.Exit(1)

        # Build update parameters (only include provided values)
        from typing import Any

        update_params: dict[str, Any] = {}
        if ssh_host is not None:
            update_params["ssh_host"] = ssh_host
        if hostname is not None:
            update_params["hostname"] = hostname
        if username is not None:
            update_params["username"] = username
        if key_file is not None:
            update_params["key_filename"] = key_file
        if port is not None:
            update_params["port"] = port
        if proxy_jump is not None:
            update_params["proxy_jump"] = proxy_jump
        if description is not None:
            update_params["description"] = description

        if not update_params:
            console.print("[yellow]No updates provided[/yellow]")
            console.print("[dim]Use --help to see available options[/dim]")
            return

        # Update the profile
        success = config_manager.update_profile(name, **update_params)

        if success:
            console.print(f"[green]✅ Profile '{name}' updated successfully[/green]")

            # Show what was updated
            updated_info = []
            for key, value in update_params.items():
                display_key = key.replace("_", " ").title()
                updated_info.append(f"{display_key}: {value}")

            info_panel = Panel(
                "\n".join(updated_info),
                title=f"Updated Fields for '{name}'",
                border_style="green",
            )
            console.print(info_panel)
        else:
            console.print(f"[red]❌ Failed to update profile '{name}'[/red]")
            raise typer.Exit(1)

    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        raise typer.Exit(1) from e


def set_env_var_impl(
    profile_name: str, key: str, value: str, config: str | None = None
):
    """Implementation for setting an environment variable for a profile."""
    try:
        config_manager = ConfigManager(config)

        # Check if profile exists
        profile = config_manager.get_profile(profile_name)
        if not profile:
            console.print(f"[red]Error: Profile '{profile_name}' not found[/red]")
            raise typer.Exit(1)

        # Set environment variable
        success = config_manager.set_profile_env_var(profile_name, key, value)

        if success:
            # Hide sensitive values in output
            if any(
                sensitive in key.upper()
                for sensitive in ["TOKEN", "KEY", "SECRET", "PASSWORD"]
            ):
                display_value = "***HIDDEN***"
            else:
                display_value = value

            console.print(
                f"[green]✅ Environment variable set for profile '{profile_name}'[/green]"
            )
            console.print(f"[dim]{key}={display_value}[/dim]")
        else:
            console.print(
                f"[red]❌ Failed to set environment variable for profile '{profile_name}'[/red]"
            )
            raise typer.Exit(1)

    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        raise typer.Exit(1) from e


def unset_env_var_impl(profile_name: str, key: str, config: str | None = None):
    """Implementation for unsetting an environment variable for a profile."""
    try:
        config_manager = ConfigManager(config)

        # Check if profile exists
        profile = config_manager.get_profile(profile_name)
        if not profile:
            console.print(f"[red]Error: Profile '{profile_name}' not found[/red]")
            raise typer.Exit(1)

        # Check if environment variable exists
        if not profile.env_vars or key not in profile.env_vars:
            console.print(
                f"[yellow]Environment variable '{key}' not set for profile '{profile_name}'[/yellow]"
            )
            return

        # Unset environment variable
        success = config_manager.unset_profile_env_var(profile_name, key)

        if success:
            console.print(
                f"[green]✅ Environment variable '{key}' removed from profile '{profile_name}'[/green]"
            )
        else:
            console.print(
                f"[red]❌ Failed to remove environment variable '{key}' from profile '{profile_name}'[/red]"
            )
            raise typer.Exit(1)

    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        raise typer.Exit(1) from e


def list_env_vars_impl(profile_name: str, config: str | None = None):
    """Implementation for listing environment variables for a profile."""
    try:
        config_manager = ConfigManager(config)

        # Check if profile exists
        profile = config_manager.get_profile(profile_name)
        if not profile:
            console.print(f"[red]Error: Profile '{profile_name}' not found[/red]")
            raise typer.Exit(1)

        if not profile.env_vars:
            console.print(
                f"[yellow]No environment variables set for profile '{profile_name}'[/yellow]"
            )
            console.print(
                "[dim]Use 'srunx ssh profile env set' to add environment variables[/dim]"
            )
            return

        # Create table
        table = Table(title=f"Environment Variables for Profile '{profile_name}'")
        table.add_column("Variable", style="cyan", no_wrap=True)
        table.add_column("Value", style="green")

        for key, value in profile.env_vars.items():
            # Hide sensitive values
            if any(
                sensitive in key.upper()
                for sensitive in ["TOKEN", "KEY", "SECRET", "PASSWORD"]
            ):
                display_value = "***HIDDEN***"
            else:
                display_value = value
            table.add_row(key, display_value)

        console.print(table)

    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        raise typer.Exit(1) from e
