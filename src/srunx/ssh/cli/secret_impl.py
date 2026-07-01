"""CLI implementations for ``srunx ssh secret set/list/unset``.

Thin wrappers over :class:`srunx.ssh.core.secret_store.RemoteSecretStore`:
acquire the value (hidden getpass prompt or ``--from-env``), validate the KEY
and value, then drive the store. Secret values are NEVER printed — success
messages and ``list`` output name only the KEY.
"""

from __future__ import annotations

import getpass
import os

import typer
from rich.console import Console
from rich.table import Table

from ..core.config import ConfigManager
from ..core.connection import SSHConnection
from ..core.file_manager import RemoteFileManager
from ..core.secret_store import RemoteSecretStore, _validate_key, _validate_value
from ..core.ssh_config import get_ssh_config_host

console = Console()


def _build_store(
    profile_name: str, config: str | None
) -> tuple[SSHConnection, RemoteSecretStore]:
    """Resolve the profile's connection and return a (connection, store) pair.

    The connection is NOT yet connected — the caller opens it. Raises
    ``typer.Exit`` when the profile is missing.
    """
    config_manager = ConfigManager(config)
    profile = config_manager.get_profile(profile_name)
    if not profile:
        console.print(f"[red]Error: Profile '{profile_name}' not found[/red]")
        raise typer.Exit(1)

    # Resolve connection params: ssh_host alias (from ~/.ssh/config) or direct.
    if profile.ssh_host:
        ssh_host = get_ssh_config_host(profile.ssh_host, None)
        if not ssh_host:
            console.print(
                f"[red]Error: SSH host '{profile.ssh_host}' not found in ~/.ssh/config[/red]"
            )
            raise typer.Exit(1)
        hostname = ssh_host.hostname or profile.ssh_host
        username = ssh_host.user or ""
        key_filename = ssh_host.identity_file
        port = ssh_host.port or 22
        proxy_jump = ssh_host.proxy_jump
    else:
        hostname = profile.hostname
        username = profile.username
        key_filename = profile.key_filename or None
        port = profile.port
        proxy_jump = profile.proxy_jump
        resolved = get_ssh_config_host(profile.hostname, None)
        if resolved and resolved.hostname:
            hostname = resolved.hostname
            if resolved.identity_file and not key_filename:
                key_filename = resolved.identity_file
            if resolved.port:
                port = resolved.port
            if resolved.proxy_jump and not proxy_jump:
                proxy_jump = resolved.proxy_jump

    connection = SSHConnection(
        hostname=hostname,
        username=username,
        key_filename=key_filename,
        port=port,
        proxy_jump=proxy_jump,
    )
    files = RemoteFileManager(connection)
    store = RemoteSecretStore(connection, files)
    return connection, store


def set_secret_impl(
    profile_name: str,
    key: str,
    from_env: str | None,
    config: str | None = None,
) -> None:
    """Set a secret for a profile.

    Value acquisition: ``--from-env VAR`` reads ``os.environ[VAR]``; the
    default path prompts with a hidden getpass. The value is never echoed.
    """
    try:
        # Validate the KEY before touching the network or reading a value.
        try:
            _validate_key(key)
        except ValueError as exc:
            console.print(f"[red]Error: {exc}[/red]")
            raise typer.Exit(1) from exc

        # Acquire the value.
        if from_env is not None:
            if from_env not in os.environ:
                console.print(
                    f"[red]Error: environment variable '{from_env}' is not set[/red]"
                )
                raise typer.Exit(1)
            value = os.environ[from_env]
        else:
            value = getpass.getpass(f"Secret for {key}: ")

        try:
            _validate_value(value)
        except ValueError as exc:
            console.print(f"[red]Error: {exc}[/red]")
            raise typer.Exit(1) from exc

        connection, store = _build_store(profile_name, config)
        with connection:
            store.set_secret(key, value)

        console.print(
            f"[green]✅ Secret '{key}' stored for profile '{profile_name}'[/green]"
        )

    except typer.Exit:
        raise
    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        raise typer.Exit(1) from e


def list_secrets_impl(profile_name: str, config: str | None = None) -> None:
    """List secret KEY names for a profile. Values are never printed."""
    try:
        connection, store = _build_store(profile_name, config)
        with connection:
            keys = store.list_keys()

        if not keys:
            console.print(
                f"[yellow]No secrets set for profile '{profile_name}'[/yellow]"
            )
            console.print("[dim]Use 'srunx ssh secret set' to add a secret[/dim]")
            return

        table = Table(title=f"Secrets for Profile '{profile_name}'")
        table.add_column("Name", style="cyan", no_wrap=True)
        for key in keys:
            table.add_row(key)
        console.print(table)

    except typer.Exit:
        raise
    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        raise typer.Exit(1) from e


def unset_secret_impl(profile_name: str, key: str, config: str | None = None) -> None:
    """Remove a secret from a profile."""
    try:
        try:
            _validate_key(key)
        except ValueError as exc:
            console.print(f"[red]Error: {exc}[/red]")
            raise typer.Exit(1) from exc

        connection, store = _build_store(profile_name, config)
        with connection:
            store.unset_secret(key)

        console.print(
            f"[green]✅ Secret '{key}' removed from profile '{profile_name}'[/green]"
        )

    except typer.Exit:
        raise
    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        raise typer.Exit(1) from e
