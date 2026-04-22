#!/usr/bin/env python3
"""
Typer-based SSH CLI commands for srunx.

This module provides a clean typer-based interface for SSH SLURM operations,
replacing the mixed argparse/typer architecture.
"""

from pathlib import Path
from typing import Annotated, Any

import typer
from rich.console import Console
from rich.panel import Panel
from rich.status import Status

from ..core.client import SSHSlurmClient
from ..core.config import ConfigManager
from ..core.ssh_config import get_ssh_config_host
from .profile_impl import add_profile_impl

console = Console()

# Create the main SSH app
ssh_app = typer.Typer(
    name="ssh",
    help="Manage SSH connection profiles, sync mounts, and test connections",
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
    from srunx.logging import configure_cli_logging

    configure_cli_logging(level="DEBUG" if verbose else "WARNING")


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


@ssh_app.command("sync")
def sync_mount(
    profile_name: Annotated[
        str | None,
        typer.Argument(help="SSH profile name (auto-detected if omitted)"),
    ] = None,
    mount_name: Annotated[
        str | None,
        typer.Argument(help="Mount name (auto-detected from cwd if omitted)"),
    ] = None,
    exclude: Annotated[
        list[str] | None,
        typer.Option("--exclude", "-e", help="Exclude pattern (repeatable)"),
    ] = None,
    dry_run: Annotated[
        bool, typer.Option("--dry-run", "-n", help="Preview without syncing")
    ] = False,
    config: Annotated[
        str | None,
        typer.Option(
            "--config", help="Config file path (default: ~/.config/srunx/config.json)"
        ),
    ] = None,
):
    """Sync a mount's local directory to the remote via rsync.

    With no arguments, auto-detects the profile and mount from the current
    working directory. Works even when inside a subdirectory of the mount.

    Examples:
      srunx ssh sync                          # auto-detect from cwd
      srunx ssh sync pyxis ml-project         # explicit profile and mount
      srunx ssh sync pyxis ml-project --dry-run
    """
    from srunx.ssh.core.config import MountConfig
    from srunx.sync.rsync import RsyncClient

    try:
        config_manager = ConfigManager(config)

        # Resolve profile
        if profile_name is None:
            profile_name = config_manager.get_current_profile_name()
            if not profile_name:
                console.print(
                    "[red]Error: No current SSH profile set. "
                    "Specify a profile or run 'srunx ssh profile set <name>'.[/red]"
                )
                raise typer.Exit(1)

        profile = config_manager.get_profile(profile_name)
        if not profile:
            console.print(f"[red]Error: Profile '{profile_name}' not found[/red]")
            raise typer.Exit(1)

        if not profile.mounts:
            console.print(
                f"[red]Error: Profile '{profile_name}' has no mounts configured. "
                f"Add one with: srunx ssh profile mount add {profile_name} <name> "
                f"--local <path> --remote <path>[/red]"
            )
            raise typer.Exit(1)

        # Resolve mount
        mount: MountConfig | None = None
        if mount_name is not None:
            for m in profile.mounts:
                if m.name == mount_name:
                    mount = m
                    break
            if mount is None:
                console.print(
                    f"[red]Error: Mount '{mount_name}' not found in profile '{profile_name}'[/red]"
                )
                raise typer.Exit(1)
        else:
            # Auto-detect from cwd
            cwd = Path.cwd().resolve()
            for m in profile.mounts:
                mount_root = Path(m.local).resolve()
                try:
                    if cwd == mount_root or cwd.is_relative_to(mount_root):
                        mount = m
                        break
                except ValueError:
                    continue

            if mount is None:
                console.print(
                    "[red]Error: Current directory is not under any configured mount.[/red]"
                )
                console.print("\nConfigured mounts:")
                for m in profile.mounts:
                    console.print(f"  {m.name}: {m.local}")
                console.print(f"\nCurrent directory: {cwd}")
                raise typer.Exit(1)

        # Sync
        action = "Previewing" if dry_run else "Syncing"
        console.print(f"[bold]{action}[/bold] mount [cyan]{mount.name}[/cyan]")
        console.print(f"  Local:  {mount.local}")
        console.print(f"  Remote: {mount.remote}")
        console.print(f"  Profile: {profile_name}")
        if dry_run:
            console.print("  [yellow]Dry run — no files will be transferred[/yellow]")
        console.print()

        # Merge mount-level and CLI-level exclude patterns.
        # mount.exclude_patterns is an immutable tuple; normalize to list
        # so list concatenation below works.
        mount_excludes: list[str] = list(mount.exclude_patterns)
        cli_excludes = exclude or []
        all_excludes = mount_excludes + [
            p for p in cli_excludes if p not in mount_excludes
        ]

        # When ssh_host is set, delegate to ~/.ssh/config for all
        # connection params (user, key, proxy, port).
        if profile.ssh_host:
            rsync = RsyncClient(
                hostname=profile.ssh_host,
                username="",
                ssh_config_path=str(Path.home() / ".ssh" / "config"),
                exclude_patterns=all_excludes or None,
            )
        else:
            rsync = RsyncClient(
                hostname=profile.hostname,
                username=profile.username,
                key_filename=profile.key_filename,
                port=profile.port,
                proxy_jump=profile.proxy_jump,
                exclude_patterns=all_excludes or None,
            )

        result = rsync.push(mount.local, mount.remote, dry_run=dry_run)

        if result.returncode == 0:
            if dry_run:
                console.print(result.stdout or "[dim]No changes needed[/dim]")
            console.print(f"\n[green]Sync complete: {mount.name}[/green]")
        else:
            console.print(f"[red]rsync failed (exit code {result.returncode}):[/red]")
            if result.stderr:
                console.print(f"[red]{result.stderr}[/red]")
            raise typer.Exit(1)

    except RuntimeError as e:
        console.print(f"[red]Error: {e}[/red]")
        raise typer.Exit(1) from e


@ssh_app.callback(invoke_without_command=True)
def ssh_main(ctx: typer.Context):
    """
    Manage SSH connection profiles, sync mounts, and test connections.

    To submit jobs to a remote SLURM server, use ``srunx sbatch --profile <name>``.
    To stream remote job logs, use ``srunx tail --profile <name>``.

    Examples:
      srunx ssh test --host dgx-server
      srunx ssh profile add ml-cluster --hostname dgx.example.com --username researcher
      srunx ssh sync ml-cluster ml-project
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


# Mount management for profiles
profile_mount_app = typer.Typer(
    name="mount",
    help="Manage local-to-remote path mounts for profiles",
)
profile_app.add_typer(profile_mount_app, name="mount")


@profile_mount_app.command("add")
def mount_add(
    profile_name: Annotated[str, typer.Argument(help="Profile name")],
    name: Annotated[str, typer.Argument(help="Mount name (e.g. 'ml-project')")],
    local: Annotated[str, typer.Option("--local", help="Local directory path")],
    remote: Annotated[
        str, typer.Option("--remote", help="Remote directory path (absolute)")
    ],
    exclude: Annotated[
        list[str] | None,
        typer.Option("--exclude", "-e", help="Exclude pattern (repeatable)"),
    ] = None,
    config: Annotated[
        str | None,
        typer.Option(
            "--config", help="Config file path (default: ~/.config/srunx/config.json)"
        ),
    ] = None,
) -> None:
    """Add a path mount to a profile."""
    from .profile_impl import add_mount_impl

    add_mount_impl(profile_name, name, local, remote, config, exclude_patterns=exclude)


@profile_mount_app.command("list")
def mount_list(
    profile_name: Annotated[str, typer.Argument(help="Profile name")],
    config: Annotated[
        str | None,
        typer.Option(
            "--config", help="Config file path (default: ~/.config/srunx/config.json)"
        ),
    ] = None,
) -> None:
    """List all mounts for a profile."""
    from .profile_impl import list_mounts_impl

    list_mounts_impl(profile_name, config)


@profile_mount_app.command("remove")
def mount_remove(
    profile_name: Annotated[str, typer.Argument(help="Profile name")],
    name: Annotated[str, typer.Argument(help="Mount name to remove")],
    config: Annotated[
        str | None,
        typer.Option(
            "--config", help="Config file path (default: ~/.config/srunx/config.json)"
        ),
    ] = None,
) -> None:
    """Remove a mount from a profile."""
    from .profile_impl import remove_mount_impl

    remove_mount_impl(profile_name, name, config)


def _resolve_direct_profile(profile_obj: Any, ssh_config: str | None) -> dict[str, Any]:
    """Build connection params from a "direct" profile, honoring ~/.ssh/config.

    Profiles created with a bare hostname like "pyxis" may rely on an
    ssh_config(5) alias that sets HostName / IdentityFile / Port / ProxyJump.
    paramiko does not consult ssh_config on its own, so look the alias up
    explicitly and merge whatever the profile itself did not set.
    """
    hostname = profile_obj.hostname
    key_filename = profile_obj.key_filename
    port = profile_obj.port
    proxy_jump = profile_obj.proxy_jump

    ssh_host = get_ssh_config_host(profile_obj.hostname, ssh_config)
    if ssh_host and ssh_host.hostname:
        hostname = ssh_host.hostname
        if ssh_host.identity_file and not key_filename:
            key_filename = ssh_host.identity_file
        if ssh_host.port and port == 22:
            port = ssh_host.port
        if ssh_host.proxy_jump and not proxy_jump:
            proxy_jump = ssh_host.proxy_jump

    return {
        "hostname": hostname,
        "username": profile_obj.username,
        "key_filename": key_filename,
        "port": port,
        "proxy_jump": proxy_jump,
    }


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
            # Profile uses direct connection. If hostname happens to be an
            # ssh_config alias, resolve HostName/IdentityFile/Port/ProxyJump
            # from ~/.ssh/config so the user doesn't need to duplicate fields
            # already declared there.
            connection_params = _resolve_direct_profile(profile_obj, ssh_config)
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
                connection_params = _resolve_direct_profile(profile_obj, ssh_config)
                display_host = "current"
        else:
            console.print("[red]Error: No connection method specified[/red]")
            console.print(
                "[yellow]Use --host, --profile, or provide --hostname/--username/--key-file[/yellow]"
            )
            raise typer.Exit(1)

    return connection_params, display_host or str(connection_params["hostname"])


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
