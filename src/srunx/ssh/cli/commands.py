#!/usr/bin/env python3
"""Typer-based SSH CLI commands for srunx.

Command surface (flat — there is no ``profile`` sub-app):

    srunx ssh add     --profile NAME --hostname H --username U [...]
    srunx ssh list
    srunx ssh show    [--profile NAME]        # omit -> current profile
    srunx ssh use     --profile NAME          # set the current profile
    srunx ssh remove  --profile NAME
    srunx ssh update  --profile NAME [...]
    srunx ssh test    [--profile NAME] [--host ALIAS]
    srunx ssh sync    [--profile NAME] [--mount MOUNT]
    srunx ssh mount add|list|remove  --profile NAME [--mount MOUNT] [...]
    srunx ssh env  set|unset|list    --profile NAME [KEY] [VALUE]

The profile is named with ``--profile`` everywhere (never a positional, never
``-p`` — ``-p`` is reserved for ``--partition`` across srunx). A mount is named
with ``--mount``. Commands that read or target an *existing* profile accept an
optional ``--profile`` and fall back to the current profile
(``srunx ssh use``); commands that create/mutate a specific profile require it.
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

# Create the main SSH app. Profile lifecycle verbs live directly under it;
# only the per-profile sub-entities (mounts, env vars) keep a one-level group.
ssh_app = typer.Typer(
    name="ssh",
    help="Manage SSH connection profiles, sync mounts, and test connections",
    add_completion=False,
)


def setup_logging(verbose: bool = False):
    """Configure logging for SSH operations."""
    from srunx.common.logging import configure_cli_logging

    configure_cli_logging(level="DEBUG" if verbose else "WARNING")


def _resolve_optional_profile(
    profile: str | None, config_manager: ConfigManager
) -> str:
    """Return the explicit ``--profile`` or fall back to the current profile.

    Honours the same ``cli.use_current_profile`` opt-out that the
    job-management transport resolver uses, so disabling implicit
    current-profile selection there also disables it here. (The transport
    resolver's reader lives in ``srunx.transport`` — a layer ``srunx.ssh``
    may not import — so the small opt-out + lookup is mirrored locally using
    only same-layer ``ConfigManager`` and the ``common`` config.) Exits with
    an error when no profile is given and none is current (or the opt-out is
    on).
    """
    if profile:
        return profile
    from srunx.common.config import get_config

    current = (
        config_manager.get_current_profile_name()
        if get_config().cli.use_current_profile
        else None
    )
    if not current:
        console.print(
            "[red]Error: no --profile given and no current profile set. "
            "Pass --profile <name> or run 'srunx ssh use <name>'.[/red]"
        )
        raise typer.Exit(1)
    return current


# Option aliases shared across the SSH commands. ``--profile`` deliberately has
# no ``-p`` short flag (reserved for ``--partition`` everywhere in srunx).
_ProfileRequired = Annotated[str, typer.Option("--profile", help="SSH profile name")]
_ProfileOptional = Annotated[
    str | None,
    typer.Option("--profile", help="SSH profile name (default: current profile)"),
]
_MountRequired = Annotated[str, typer.Option("--mount", help="Mount name")]
_ConfigOpt = Annotated[
    str | None,
    typer.Option(
        "--config", help="Config file path (default: ~/.config/srunx/config.json)"
    ),
]


@ssh_app.command("test")
def test_connection(
    profile: Annotated[
        str | None,
        typer.Option("--profile", help="Saved profile to test (default: current)"),
    ] = None,
    host: Annotated[
        str | None,
        typer.Option("--host", "-H", help="SSH host alias from ~/.ssh/config"),
    ] = None,
    config: _ConfigOpt = None,
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
    """Test SSH connection and SLURM availability.

    Pass ``--profile <name>`` for a saved profile, ``--host <alias>`` for a
    raw ``~/.ssh/config`` alias, or neither to use the current profile.
    """
    from rich.table import Table

    setup_logging(verbose)

    try:
        config_manager = ConfigManager(config)
        connection_params, display_host = _determine_connection_params(
            host,
            profile,
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
    profile: _ProfileOptional = None,
    mount: Annotated[
        str | None,
        typer.Option("--mount", help="Mount name (auto-detected from cwd if omitted)"),
    ] = None,
    exclude: Annotated[
        list[str] | None,
        typer.Option("--exclude", "-e", help="Exclude pattern (repeatable)"),
    ] = None,
    dry_run: Annotated[
        bool, typer.Option("--dry-run", "-n", help="Preview without syncing")
    ] = False,
    pull: Annotated[
        bool,
        typer.Option(
            "--pull",
            help="Reverse direction: sync remote → local (default is local → remote)",
        ),
    ] = False,
    config: _ConfigOpt = None,
):
    """Sync a mount between local and remote via rsync.

    By default syncs local → remote (push). Pass ``--pull`` to reverse the
    direction and sync remote → local — useful for pulling back results or
    checkpoints a job wrote on the cluster.

    With no ``--profile`` / ``--mount``, auto-detects the profile (current
    profile) and the mount (from the current working directory). Works even
    when inside a subdirectory of the mount.

    Examples:
      srunx ssh sync                                  # push, current profile + cwd mount
      srunx ssh sync --profile pyxis --mount ml-project
      srunx ssh sync --profile pyxis --mount ml-project --dry-run
      srunx ssh sync --pull                           # pull remote → local
      srunx ssh sync --profile pyxis --mount ml-project --pull --dry-run
    """
    from srunx.ssh.core.config import MountConfig
    from srunx.sync.rsync import RsyncClient

    try:
        config_manager = ConfigManager(config)
        profile_name = _resolve_optional_profile(profile, config_manager)

        profile_obj = config_manager.get_profile(profile_name)
        if not profile_obj:
            console.print(f"[red]Error: Profile '{profile_name}' not found[/red]")
            raise typer.Exit(1)

        if not profile_obj.mounts:
            console.print(
                f"[red]Error: Profile '{profile_name}' has no mounts configured. "
                f"Add one with: srunx ssh mount add --profile {profile_name} "
                f"--mount <name> --local <path> --remote <path>[/red]"
            )
            raise typer.Exit(1)

        # Resolve mount
        resolved_mount: MountConfig | None = None
        if mount is not None:
            for m in profile_obj.mounts:
                if m.name == mount:
                    resolved_mount = m
                    break
            if resolved_mount is None:
                console.print(
                    f"[red]Error: Mount '{mount}' not found in profile "
                    f"'{profile_name}'[/red]"
                )
                raise typer.Exit(1)
        else:
            # Auto-detect from cwd
            cwd = Path.cwd().resolve()
            for m in profile_obj.mounts:
                mount_root = Path(m.local).resolve()
                try:
                    if cwd == mount_root or cwd.is_relative_to(mount_root):
                        resolved_mount = m
                        break
                except ValueError:
                    continue

            if resolved_mount is None:
                console.print(
                    "[red]Error: Current directory is not under any configured "
                    "mount.[/red]"
                )
                console.print("\nConfigured mounts:")
                for m in profile_obj.mounts:
                    console.print(f"  {m.name}: {m.local}")
                console.print(f"\nCurrent directory: {cwd}")
                raise typer.Exit(1)

        # Sync
        action = "Previewing" if dry_run else "Syncing"
        direction = "remote → local (pull)" if pull else "local → remote (push)"
        console.print(f"[bold]{action}[/bold] mount [cyan]{resolved_mount.name}[/cyan]")
        console.print(f"  Local:  {resolved_mount.local}")
        console.print(f"  Remote: {resolved_mount.remote}")
        console.print(f"  Direction: {direction}")
        console.print(f"  Profile: {profile_name}")
        if dry_run:
            console.print("  [yellow]Dry run — no files will be transferred[/yellow]")
        console.print()

        # Merge mount-level and CLI-level exclude patterns.
        mount_excludes: list[str] = list(resolved_mount.exclude_patterns)
        cli_excludes = exclude or []
        all_excludes = mount_excludes + [
            p for p in cli_excludes if p not in mount_excludes
        ]

        # When ssh_host is set, delegate to ~/.ssh/config for all
        # connection params (user, key, proxy, port).
        if profile_obj.ssh_host:
            rsync = RsyncClient(
                hostname=profile_obj.ssh_host,
                username="",
                ssh_config_path=str(Path.home() / ".ssh" / "config"),
                exclude_patterns=all_excludes or None,
            )
        else:
            rsync = RsyncClient(
                hostname=profile_obj.hostname,
                username=profile_obj.username,
                key_filename=profile_obj.key_filename,
                port=profile_obj.port,
                proxy_jump=profile_obj.proxy_jump,
                exclude_patterns=all_excludes or None,
            )

        # itemize=dry_run so the preview lists every file rsync *would*
        # touch; a real sync stays quiet (itemize defaults off).
        if pull:
            # Trailing slash on the remote source so rsync copies the
            # mount's *contents* into the local dir (mirroring push),
            # not the directory itself one level deeper.
            remote_src = resolved_mount.remote.rstrip("/") + "/"
            result = rsync.pull(
                remote_src, resolved_mount.local, dry_run=dry_run, itemize=dry_run
            )
        else:
            result = rsync.push(
                resolved_mount.local,
                resolved_mount.remote,
                dry_run=dry_run,
                itemize=dry_run,
            )

        if result.returncode == 0:
            if dry_run:
                console.print(result.stdout or "[dim]No changes needed[/dim]")
            console.print(f"\n[green]Sync complete: {resolved_mount.name}[/green]")
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
    """Manage SSH connection profiles, sync mounts, and test connections.

    To submit jobs to a remote SLURM server, use ``srunx sbatch --profile <name>``.
    To stream remote job logs, use ``srunx tail --profile <name>``.

    Examples:
      srunx ssh add --profile ml-cluster --hostname dgx.example.com --username researcher
      srunx ssh use --profile ml-cluster
      srunx ssh test --profile ml-cluster
      srunx ssh sync --profile ml-cluster --mount ml-project
    """
    # If no subcommand is invoked, show help
    if ctx.invoked_subcommand is None:
        console.print(ctx.get_help())
        raise typer.Exit()


# ---------------------------------------------------------------------------
# Profile lifecycle (flat verbs under ``ssh``)
# ---------------------------------------------------------------------------


@ssh_app.command("list")
def list_profiles(config: _ConfigOpt = None):
    """List all connection profiles."""
    from .profile_impl import list_profiles_impl

    list_profiles_impl(config)


@ssh_app.command("add")
def add_profile(
    profile: _ProfileRequired,
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
    config: _ConfigOpt = None,
):
    """Add a new connection profile."""
    add_profile_impl(
        profile,
        ssh_host,
        hostname,
        username,
        key_file,
        port,
        proxy_jump,
        description,
        config,
    )


@ssh_app.command("remove")
def remove_profile(profile: _ProfileRequired, config: _ConfigOpt = None):
    """Remove a connection profile."""
    from .profile_impl import remove_profile_impl

    remove_profile_impl(profile, config)


@ssh_app.command("use")
def use_profile(profile: _ProfileRequired, config: _ConfigOpt = None):
    """Set the current default profile.

    The current profile is the implicit target of commands that omit
    ``--profile`` (e.g. ``srunx ssh test``, ``srunx ssh sync``) and the
    last fallback rung of job-management transport resolution.
    """
    from .profile_impl import set_current_profile_impl

    set_current_profile_impl(profile, config)


@ssh_app.command("show")
def show_profile(profile: _ProfileOptional = None, config: _ConfigOpt = None):
    """Show profile details (defaults to the current profile)."""
    from .profile_impl import show_profile_impl

    # Resolve through the shared helper so the current-profile fallback
    # honours ``cli.use_current_profile`` (consistent with sync / mount list
    # / env list); show_profile_impl then receives a concrete name.
    config_manager = ConfigManager(config)
    profile_name = _resolve_optional_profile(profile, config_manager)
    show_profile_impl(profile_name, config)


@ssh_app.command("update")
def update_profile(
    profile: _ProfileRequired,
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
    config: _ConfigOpt = None,
):
    """Update an existing profile."""
    from .profile_impl import update_profile_impl

    update_profile_impl(
        profile,
        ssh_host,
        hostname,
        username,
        key_file,
        port,
        proxy_jump,
        description,
        config,
    )


# ---------------------------------------------------------------------------
# Environment variable management for profiles (``ssh env ...``)
# ---------------------------------------------------------------------------
env_app = typer.Typer(
    name="env",
    help="Manage environment variables for a profile",
)
ssh_app.add_typer(env_app, name="env")


@env_app.command("set")
def set_env_var(
    profile: _ProfileRequired,
    key: Annotated[str, typer.Argument(help="Environment variable name")],
    value: Annotated[str, typer.Argument(help="Environment variable value")],
    config: _ConfigOpt = None,
):
    """Set an environment variable for a profile."""
    from .profile_impl import set_env_var_impl

    set_env_var_impl(profile, key, value, config)


@env_app.command("unset")
def unset_env_var(
    profile: _ProfileRequired,
    key: Annotated[str, typer.Argument(help="Environment variable name")],
    config: _ConfigOpt = None,
):
    """Unset an environment variable for a profile."""
    from .profile_impl import unset_env_var_impl

    unset_env_var_impl(profile, key, config)


@env_app.command("list")
def list_env_vars(profile: _ProfileOptional = None, config: _ConfigOpt = None):
    """List environment variables for a profile (defaults to current)."""
    from .profile_impl import list_env_vars_impl

    config_manager = ConfigManager(config)
    profile_name = _resolve_optional_profile(profile, config_manager)
    list_env_vars_impl(profile_name, config)


# ---------------------------------------------------------------------------
# Mount management for profiles (``ssh mount ...``)
# ---------------------------------------------------------------------------
mount_app = typer.Typer(
    name="mount",
    help="Manage local-to-remote path mounts for a profile",
)
ssh_app.add_typer(mount_app, name="mount")


@mount_app.command("add")
def mount_add(
    profile: _ProfileRequired,
    mount: _MountRequired,
    local: Annotated[str, typer.Option("--local", help="Local directory path")],
    remote: Annotated[
        str, typer.Option("--remote", help="Remote directory path (absolute)")
    ],
    exclude: Annotated[
        list[str] | None,
        typer.Option("--exclude", "-e", help="Exclude pattern (repeatable)"),
    ] = None,
    config: _ConfigOpt = None,
) -> None:
    """Add a path mount to a profile."""
    from .profile_impl import add_mount_impl

    add_mount_impl(profile, mount, local, remote, config, exclude_patterns=exclude)


@mount_app.command("list")
def mount_list(profile: _ProfileOptional = None, config: _ConfigOpt = None) -> None:
    """List all mounts for a profile (defaults to current)."""
    from .profile_impl import list_mounts_impl

    config_manager = ConfigManager(config)
    profile_name = _resolve_optional_profile(profile, config_manager)
    list_mounts_impl(profile_name, config)


@mount_app.command("remove")
def mount_remove(
    profile: _ProfileRequired,
    mount: _MountRequired,
    config: _ConfigOpt = None,
) -> None:
    """Remove a mount from a profile."""
    from .profile_impl import remove_mount_impl

    remove_mount_impl(profile, mount, config)


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


def _connection_params_from_profile(
    profile_obj: Any, ssh_config: str | None
) -> dict[str, Any]:
    """Build connection params for a saved profile (ssh_host alias or direct)."""
    if profile_obj.ssh_host:
        ssh_host = get_ssh_config_host(profile_obj.ssh_host, ssh_config)
        if not ssh_host:
            console.print(
                f"[red]Error: SSH host '{profile_obj.ssh_host}' not found[/red]"
            )
            raise typer.Exit(1)
        return {
            "hostname": ssh_host.hostname,
            "username": ssh_host.user,
            "key_filename": ssh_host.identity_file,
            "port": ssh_host.port,
            "proxy_jump": ssh_host.proxy_jump,
        }
    return _resolve_direct_profile(profile_obj, ssh_config)


def _determine_connection_params(
    host: str | None,
    profile: str | None,
    ssh_config: str | None,
    config_manager: ConfigManager,
) -> tuple[dict, str]:
    """Determine connection parameters and display host name for ``ssh test``.

    Resolution order: ``--host`` alias > ``--profile`` saved profile >
    current profile. Ad-hoc ``--hostname/--username/--key-file`` connections
    were removed — a connection worth testing belongs in a profile or an
    ``~/.ssh/config`` alias.
    """
    if host:
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
        return connection_params, host

    if profile:
        profile_obj = config_manager.get_profile(profile)
        if not profile_obj:
            console.print(f"[red]Error: Profile '{profile}' not found[/red]")
            raise typer.Exit(1)
        params = _connection_params_from_profile(profile_obj, ssh_config)
        display = (
            f"{profile} ({profile_obj.ssh_host})" if profile_obj.ssh_host else profile
        )
        return params, display

    # Fall back to the current profile — but only when the implicit
    # current-profile selection is enabled (same opt-out the other
    # optional-profile commands honour via ``_resolve_optional_profile``).
    from srunx.common.config import get_config

    profile_obj = (
        config_manager.get_current_profile()
        if get_config().cli.use_current_profile
        else None
    )
    if profile_obj:
        params = _connection_params_from_profile(profile_obj, ssh_config)
        display = (
            f"current ({profile_obj.ssh_host})" if profile_obj.ssh_host else "current"
        )
        return params, display

    console.print("[red]Error: No connection method specified[/red]")
    console.print(
        "[yellow]Use --profile <name>, --host <alias>, or set a current "
        "profile with 'srunx ssh use <name>'[/yellow]"
    )
    raise typer.Exit(1)


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
