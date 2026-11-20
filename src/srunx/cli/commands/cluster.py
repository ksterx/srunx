"""``srunx cluster`` — manage job targets (design §7).

Clusters live in the user config (``$XDG_CONFIG_HOME/srunx/config.toml``)
under ``[clusters.<name>]``. Which cluster a command targets is decided by
``--cluster`` > ``$SRUNX_CLUSTER`` > the project's ``cluster`` >
``default_cluster``; there is no stored "current cluster".

Sync targets (mounts) belong to the project (``.srunx/config.toml``), so
``srunx cluster sync`` works on the current project's mounts.
"""

from __future__ import annotations

import shutil
from pathlib import Path
from typing import Annotated, Any

import typer
from rich.console import Console
from rich.markup import escape
from rich.table import Table

from srunx.common.config import (
    ConfigError,
    load_settings,
    use_project,
    user_store,
    validate_name,
    validate_user_file,
)
from srunx.common.config_store import ConfigFileError

console = Console()

cluster_app = typer.Typer(
    name="cluster",
    help="Manage clusters (job targets), sync project mounts, and remote secrets",
    no_args_is_help=True,
)

NameArg = Annotated[str, typer.Argument(help="Cluster name")]
OptionalNameArg = Annotated[
    str | None,
    typer.Argument(help="Cluster name (default: the cluster commands would target)"),
]
ProjectOpt = Annotated[
    Path | None,
    typer.Option("--project", help="Project root (default: discovered from cwd)"),
]


def _settings() -> Any:
    try:
        return load_settings(reload=True)
    except ConfigError as exc:
        console.print(f"[red]Error: {exc}[/red]")
        raise typer.Exit(1) from exc


def _resolve(name: str | None) -> str:
    settings = _settings()
    try:
        return settings.resolve_cluster(name)
    except ConfigError as exc:
        console.print(f"[red]Error: {exc}[/red]")
        raise typer.Exit(1) from exc


def _target(cfg: Any) -> str:
    if cfg.transport == "local":
        return "local Slurm"
    if cfg.host:
        return f"{cfg.host} (ssh config)"
    target = f"{cfg.username + '@' if cfg.username else ''}{cfg.hostname}"
    if cfg.port != 22:
        target += f":{cfg.port}"
    if cfg.proxy_jump:
        target += f" via {cfg.proxy_jump}"
    return target


@cluster_app.command("list")
def list_clusters() -> None:
    """List configured clusters."""
    settings = _settings()
    if not settings.clusters:
        console.print("[yellow]No clusters configured.[/yellow]")
        console.print("Add one with: srunx cluster add NAME --local | --host ALIAS")
        return
    default, origin = settings.cluster_source()
    table = Table()
    table.add_column("", width=1)
    table.add_column("Name", style="cyan")
    table.add_column("Transport")
    table.add_column("Target")
    table.add_column("Description", style="dim")
    for name, cfg in sorted(settings.clusters.items()):
        table.add_row(
            "*" if name == default else "",
            name,
            cfg.transport,
            _target(cfg),
            cfg.description or "",
        )
    console.print(table)
    if default:
        console.print(f"[dim]* selected by default ({escape(origin)})[/dim]")


@cluster_app.command("show")
def show_cluster(name: OptionalNameArg = None, project: ProjectOpt = None) -> None:
    """Show a cluster's connection, defaults, and the project's mounts on it."""
    with use_project(project):
        cluster = _resolve(name)
        settings = _settings()
        cfg = settings.cluster(cluster)
        console.print(f"[bold]{cluster}[/bold]  ({cfg.transport})")
        console.print(f"  Target: {_target(cfg)}")
        if cfg.description:
            console.print(f"  Description: {cfg.description}")
        if cfg.snapshot_root:
            console.print(f"  Snapshot root: {cfg.snapshot_root}")
        effective = settings.effective(cluster)
        cluster_values = {
            k: v for k, v in effective.sources.items() if f"[clusters.{cluster}]" in v
        }
        if cluster_values:
            console.print("  Cluster-specific defaults:")
            for key, source in sorted(cluster_values.items()):
                console.print(f"    {key}  [dim]({escape(source)})[/dim]")
        if settings.project is not None and cfg.transport == "ssh":
            console.print(f"  Project '{settings.project.name}' mounts:")
            mounts = settings.mounts(cluster)
            if not mounts:
                console.print(
                    "    (none — remote environment unknown; run 'srunx cluster test')"
                )
            for m in mounts:
                state = "" if m.sync else "  [dim](no sync)[/dim]"
                console.print(f"    {m.name}: {m.local} → {m.remote}{state}")


def _connection_table(
    *,
    transport: str,
    host: str | None,
    hostname: str | None,
    username: str | None,
    key_file: str | None,
    port: int | None,
    proxy_jump: str | None,
    description: str | None,
    snapshot_root: str | None,
) -> dict[str, Any]:
    table: dict[str, Any] = {"transport": transport}
    for key, value in (
        ("host", host),
        ("hostname", hostname),
        ("username", username),
        ("key_filename", key_file),
        ("port", port),
        ("proxy_jump", proxy_jump),
        ("description", description),
        ("snapshot_root", snapshot_root),
    ):
        if value is not None and value != "":
            table[key] = value
    return table


def _history_count(name: str, transport: str) -> int:
    from srunx.common import paths
    from srunx.transport.registry import scheduler_key_for

    if not paths.db_path().exists():
        return 0
    try:
        from srunx.observability.storage.connection import initialized_connection

        with initialized_connection() as conn:
            row = conn.execute(
                "SELECT COUNT(*) FROM jobs WHERE scheduler_key = ?",
                (scheduler_key_for(name, transport),),
            ).fetchone()
            return int(row[0]) if row else 0
    except Exception:  # noqa: BLE001 — informational only
        return 0


@cluster_app.command("add")
def add_cluster(
    name: NameArg,
    local: Annotated[
        bool, typer.Option("--local", help="Submit with the local sbatch")
    ] = False,
    host: Annotated[
        str | None, typer.Option("--host", help="Host alias from ~/.ssh/config")
    ] = None,
    hostname: Annotated[str | None, typer.Option("--hostname")] = None,
    username: Annotated[str | None, typer.Option("--username")] = None,
    key_file: Annotated[str | None, typer.Option("--key-file")] = None,
    port: Annotated[int | None, typer.Option("--port")] = None,
    proxy_jump: Annotated[str | None, typer.Option("--proxy-jump")] = None,
    description: Annotated[str | None, typer.Option("--description")] = None,
    snapshot_root: Annotated[str | None, typer.Option("--snapshot-root")] = None,
    default: Annotated[
        bool, typer.Option("--default", help="Also make it the default_cluster")
    ] = False,
) -> None:
    """Add a cluster (``--local``, ``--host ALIAS`` or ``--hostname H``)."""
    try:
        validate_name("cluster", name)
    except ValueError as exc:
        console.print(f"[red]Error: {exc}[/red]")
        raise typer.Exit(1) from exc
    if sum(bool(x) for x in (local, host, hostname)) != 1:
        console.print(
            "[red]Error: pass exactly one of --local, --host, --hostname[/red]"
        )
        raise typer.Exit(1)
    if name in _settings().clusters:
        console.print(f"[red]Error: cluster '{name}' already exists[/red]")
        raise typer.Exit(1)
    table = _connection_table(
        transport="local" if local else "ssh",
        host=host,
        hostname=hostname,
        username=username,
        key_file=key_file,
        port=port,
        proxy_jump=proxy_jump,
        description=description,
        snapshot_root=snapshot_root,
    )

    def mutate(doc: Any) -> None:
        from srunx.common.config_store import set_in

        set_in(doc, ("clusters", name), table)
        if default:
            doc["default_cluster"] = name

    try:
        user_store().edit(mutate, validate=validate_user_file)
    except (ConfigError, ConfigFileError) as exc:
        console.print(f"[red]Error: {exc}[/red]")
        raise typer.Exit(1) from exc
    from srunx.common.remote_env import forget_remote_env

    forget_remote_env(name)  # never reuse a probe from a removed namesake
    console.print(f"[green]✅ Cluster '{name}' added[/green]")
    previous = _history_count(name, table["transport"])
    if previous:
        console.print(
            f"[yellow]Note: {previous} job(s) in srunx history already belong to "
            f"'{name}' and now refer to this cluster.[/yellow]"
        )


@cluster_app.command("update")
def update_cluster(
    name: NameArg,
    host: Annotated[str | None, typer.Option("--host")] = None,
    hostname: Annotated[str | None, typer.Option("--hostname")] = None,
    username: Annotated[str | None, typer.Option("--username")] = None,
    key_file: Annotated[str | None, typer.Option("--key-file")] = None,
    port: Annotated[int | None, typer.Option("--port")] = None,
    proxy_jump: Annotated[str | None, typer.Option("--proxy-jump")] = None,
    description: Annotated[str | None, typer.Option("--description")] = None,
    snapshot_root: Annotated[
        str | None,
        typer.Option("--snapshot-root", help="Empty string clears it"),
    ] = None,
) -> None:
    """Change connection settings of an existing cluster."""
    if name not in _settings().clusters:
        console.print(f"[red]Error: cluster '{name}' not found[/red]")
        raise typer.Exit(1)
    if name not in user_store().read().get("clusters", {}):
        console.print(
            f"[red]Error: cluster '{name}' is defined in the system config "
            "and cannot be edited here; add a user cluster with another name "
            "instead.[/red]"
        )
        raise typer.Exit(1)
    updates = {
        "host": host,
        "hostname": hostname,
        "username": username,
        "key_filename": key_file,
        "port": port,
        "proxy_jump": proxy_jump,
        "description": description,
        "snapshot_root": snapshot_root,
    }

    def mutate(doc: Any) -> None:
        table = doc["clusters"][name]
        for key, value in updates.items():
            if value is None:
                continue
            if value == "":
                if key in table:
                    del table[key]
            else:
                table[key] = value

    try:
        user_store().edit(mutate, validate=validate_user_file)
    except (ConfigError, ConfigFileError) as exc:
        console.print(f"[red]Error: {exc}[/red]")
        raise typer.Exit(1) from exc
    from srunx.common.remote_env import forget_remote_env

    forget_remote_env(name)  # $HOME may differ on the new host
    console.print(f"[green]✅ Cluster '{name}' updated[/green]")


@cluster_app.command("remove")
def remove_cluster(name: NameArg) -> None:
    """Remove a cluster from the user config."""
    store = user_store()
    try:
        if name not in store.read().get("clusters", {}):
            console.print(
                f"[red]Error: cluster '{name}' not found in {store.path}[/red]"
            )
            raise typer.Exit(1)

        def mutate(doc: Any) -> None:
            del doc["clusters"][name]
            if doc.get("default_cluster") == name:
                del doc["default_cluster"]

        store.edit(mutate, validate=validate_user_file)
    except (ConfigError, ConfigFileError) as exc:
        console.print(f"[red]Error: {exc}[/red]")
        raise typer.Exit(1) from exc
    from srunx.common.remote_env import forget_remote_env

    forget_remote_env(name)  # the name may later be reused for another host
    console.print(f"[green]✅ Cluster '{name}' removed[/green]")


@cluster_app.command("test")
def test_cluster(name: OptionalNameArg = None) -> None:
    """Check that a cluster is reachable (and cache its remote $HOME / XDG dirs)."""
    cluster = _resolve(name)
    cfg = _settings().cluster(cluster)
    if cfg.transport == "local":
        path = shutil.which("sbatch")
        if path:
            console.print(f"[green]✅ {cluster}: sbatch found at {path}[/green]")
            return
        console.print(f"[red]❌ {cluster}: sbatch is not on PATH[/red]")
        raise typer.Exit(1)

    from srunx.common.remote_env import fetch_remote_env

    with console.status(f"Connecting to {cluster}..."):
        env = fetch_remote_env(cluster)
    if env is None:
        console.print(
            f"[red]❌ {cluster}: connection or environment probe failed[/red]"
        )
        raise typer.Exit(1)
    console.print(f"[green]✅ {cluster}: connected[/green]")
    console.print(f"  HOME:            {env.home}")
    console.print(f"  data directory:  {env.data_home}")
    console.print(f"  config directory: {env.config_home}")


@cluster_app.command("sync")
def sync_cluster(
    name: OptionalNameArg = None,
    mount: Annotated[
        str | None,
        typer.Option("--mount", help="Mount name (default: the one owning cwd)"),
    ] = None,
    project: ProjectOpt = None,
    exclude: Annotated[
        list[str] | None,
        typer.Option("--exclude", "-e", help="Extra exclude pattern (repeatable)"),
    ] = None,
    dry_run: Annotated[
        bool, typer.Option("--dry-run", "-n", help="Preview without syncing")
    ] = False,
    pull: Annotated[
        bool, typer.Option("--pull", help="Sync remote → local instead")
    ] = False,
    delete: Annotated[
        bool,
        typer.Option("--delete", help="Also delete destination files missing locally"),
    ] = False,
    yes: Annotated[
        bool,
        typer.Option("--yes", "-y", help="Approve a first sync to a new remote path"),
    ] = False,
) -> None:
    """Rsync a project mount to (or with --pull, from) a cluster."""
    from srunx.common.config import get_config
    from srunx.runtime.submission_plan import resolve_mount_among
    from srunx.ssh.core.config import ConfigManager
    from srunx.sync.approval import SyncNotApprovedError, ensure_approved
    from srunx.sync.lock import acquire_sync_lock
    from srunx.sync.mount_helpers import (
        build_rsync_client,
        record_upload,
        snapshot_local,
    )

    with use_project(project):
        cluster = _resolve(name)
        profile = ConfigManager().get_profile(cluster)
        if profile is None:
            console.print(f"[red]Error: '{cluster}' is not an SSH cluster[/red]")
            raise typer.Exit(1)
        if not profile.mounts:
            console.print(
                "[red]Error: no project mounts for this cluster. Create "
                ".srunx/config.toml ('srunx config init-project') and set "
                f"[clusters.{cluster}] remote_dir.[/red]"
            )
            raise typer.Exit(1)
        if mount is not None:
            target = next((m for m in profile.mounts if m.name == mount), None)
        else:
            target = resolve_mount_among(Path.cwd(), profile.mounts) or (
                profile.mounts[0] if len(profile.mounts) == 1 else None
            )
        if target is None:
            console.print("[red]Error: pick a mount with --mount:[/red]")
            for m in profile.mounts:
                console.print(f"  {m.name}: {m.local} → {m.remote}")
            raise typer.Exit(1)

        if not pull and not dry_run:
            try:
                ensure_approved(cluster, target.local, target.remote, assume_yes=yes)
            except SyncNotApprovedError as exc:
                console.print(f"[red]Error: {exc}[/red]")
                raise typer.Exit(1) from exc

        action = "Previewing" if dry_run else "Syncing"
        direction = "remote → local" if pull else "local → remote"
        console.print(f"[bold]{action}[/bold] [cyan]{target.name}[/cyan] ({direction})")
        console.print(f"  Local:  {target.local}")
        console.print(f"  Remote: {cluster}:{target.remote}")

        rsync = build_rsync_client(profile)
        excludes = list(target.exclude_patterns) + [
            p for p in (exclude or []) if p not in target.exclude_patterns
        ]
        if excludes:
            rsync.exclude_patterns = list(rsync.exclude_patterns) + excludes

        timeout = get_config().sync.lock_timeout_seconds
        try:
            with acquire_sync_lock(cluster, target.name, timeout=timeout):
                if pull:
                    result = rsync.pull(
                        target.remote.rstrip("/") + "/",
                        target.local,
                        delete=delete,
                        dry_run=dry_run,
                        itemize=dry_run,
                    )
                else:
                    before = None if dry_run else snapshot_local(rsync, target)
                    result = rsync.push(
                        target.local,
                        target.remote,
                        delete=delete,
                        dry_run=dry_run,
                        itemize=dry_run,
                    )
                if result.returncode == 0 and not dry_run and not pull:
                    record_upload(rsync, target, mirrored=delete, before=before)
        except RuntimeError as exc:
            console.print(f"[red]Error: {exc}[/red]")
            raise typer.Exit(1) from exc

        if result.returncode != 0:
            console.print(f"[red]rsync failed (exit code {result.returncode})[/red]")
            if result.stderr:
                console.print(f"[red]{result.stderr}[/red]")
            raise typer.Exit(1)
        if dry_run:
            from srunx.sync.rsync import unescape_rsync_path

            console.print(
                unescape_rsync_path(result.stdout)
                if result.stdout
                else "[dim]No changes needed[/dim]"
            )
        console.print(f"[green]Sync complete: {target.name}[/green]")


secret_app = typer.Typer(
    name="secret", help="Manage account secrets stored in a remote 0600 file"
)
cluster_app.add_typer(secret_app, name="secret")


@secret_app.command("set")
def set_secret(
    name: NameArg,
    key: Annotated[str, typer.Argument(help="Secret name (shell identifier)")],
    from_env: Annotated[
        str | None,
        typer.Option("--from-env", help="Read the value from this local env var"),
    ] = None,
) -> None:
    """Set a secret on a cluster (value is prompted, never echoed)."""
    from srunx.ssh.cli.secret_impl import set_secret_impl

    set_secret_impl(name, key, from_env)


@secret_app.command("list")
def list_secrets(name: OptionalNameArg = None) -> None:
    """List secret names on a cluster. Values are never shown."""
    from srunx.ssh.cli.secret_impl import list_secrets_impl

    list_secrets_impl(_resolve(name))


@secret_app.command("unset")
def unset_secret(
    name: NameArg,
    key: Annotated[str, typer.Argument(help="Secret name to remove")],
) -> None:
    """Remove a secret from a cluster."""
    from srunx.ssh.cli.secret_impl import unset_secret_impl

    unset_secret_impl(name, key)
