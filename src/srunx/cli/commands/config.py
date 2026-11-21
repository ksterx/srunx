"""``srunx config`` sub-application: show, paths, init, set / delete / unset."""

from __future__ import annotations

from pathlib import Path
from typing import Annotated, Any

import tomlkit
import typer
from rich.console import Console
from rich.markup import escape
from rich.table import Table

from srunx.cli._helpers.transport_options import ClusterOpt
from srunx.common import paths
from srunx.common.config import (
    DEFAULTS_KEYS,
    ConfigError,
    example_project_config,
    example_user_config,
    load_settings,
    project_store,
    use_project,
    user_store,
    validate_name,
    validate_project_file,
    validate_user_file,
)
from srunx.common.config_store import ConfigConflictError, ConfigFileError

config_app = typer.Typer(help="Configuration management")

ProjectOpt = Annotated[
    Path | None,
    typer.Option("--project", help="Project root (default: discovered from cwd)"),
]


def _fail(exc: Exception) -> typer.Exit:
    typer.secho(f"Error: {exc}", err=True, fg=typer.colors.RED)
    return typer.Exit(code=1)


def _flatten(prefix: str, data: Any, out: dict[str, Any]) -> None:
    if isinstance(data, dict):
        for k, v in data.items():
            _flatten(f"{prefix}.{k}" if prefix else k, v, out)
    else:
        out[prefix] = data


@config_app.command("show")
def config_show(cluster: ClusterOpt = None, project: ProjectOpt = None) -> None:
    """Show the effective configuration and where every value comes from."""
    try:
        with use_project(project):
            settings = load_settings(reload=True)
            name, origin = settings.cluster_source(cluster)
            if name is not None:
                settings.cluster(name)
            config = settings.effective(name)
    except ConfigError as exc:
        raise _fail(exc) from exc

    console = Console()
    console.print(
        f"Cluster: [cyan]{name or '(none selected)'}[/cyan]"
        + (f"  [dim]({escape(origin)})[/dim]" if origin else "")
    )
    console.print(f"Project: {settings.project_root or '(none)'}")
    table = Table()
    table.add_column("Key", style="magenta")
    table.add_column("Value", style="green")
    table.add_column("Source", style="dim")
    values: dict[str, Any] = {}
    _flatten("", config.model_dump(), values)
    for key, value in values.items():
        table.add_row(
            key, escape(repr(value)), escape(config.sources.get(key, "built-in"))
        )
    console.print(table)


@config_app.command("paths")
def config_paths(project: ProjectOpt = None) -> None:
    """Show the files srunx reads, in order of precedence (lowest first)."""
    try:
        with use_project(project):
            root = load_settings(reload=True).project_root
    except ConfigError as exc:
        raise _fail(exc) from exc
    console = Console()
    rows = [
        ("system", paths.system_config_path()),
        ("user", paths.user_config_path()),
        ("project", paths.project_config_path(root) if root else None),
    ]
    for label, path in rows:
        if path is None:
            console.print(f"{label:8} (no project found)")
            continue
        status = "✅" if path.exists() else "—"
        console.print(f"{label:8} {status} {path}")
    console.print(f"state    {paths.state_dir()}")


@config_app.command("init")
def config_init(
    force: Annotated[
        bool, typer.Option("--force", help="Overwrite an existing user config")
    ] = False,
) -> None:
    """Create the user config file with a commented example."""
    path = paths.user_config_path()
    if path.exists() and not force:
        Console().print(f"Configuration file already exists: {path}")
        Console().print("Use --force to overwrite")
        return
    text = example_user_config()
    store = user_store()

    def mutate(doc: Any) -> None:
        doc.clear()
        doc.update(tomlkit.parse(text))

    store.edit(mutate, validate=validate_user_file)
    Console().print(f"✅ Configuration file created: {path}")


@config_app.command("init-project")
def config_init_project(
    name: Annotated[
        str | None,
        typer.Option("--name", help="Project name (default: directory name)"),
    ] = None,
    path: Annotated[
        Path, typer.Argument(help="Project root (default: current directory)")
    ] = Path("."),
) -> None:
    """Create ``.srunx/config.toml`` for a project."""
    root = path.expanduser().resolve()
    target = paths.project_config_path(root)
    if target.exists():
        Console().print(f"Project config already exists: {target}")
        return
    project_name = name or root.name
    try:
        validate_name("project", project_name)
    except ValueError as exc:
        raise _fail(exc) from exc
    text = example_project_config(project_name)
    project_store(root).edit(
        lambda doc: doc.update(tomlkit.parse(text)), validate=validate_project_file
    )
    Console().print(f"✅ Project config created: {target}")


def _forget_changed_connections(store: Any, before: dict[str, Any]) -> None:
    from srunx.common.remote_env import connection_changed, forget_remote_env

    if store.path != paths.user_config_path():
        return
    for name in connection_changed(before, store.read()):
        forget_remote_env(name)


def _target(project: bool, project_path: Path | None) -> tuple[Any, Any]:
    if not project:
        return user_store(), validate_user_file
    with use_project(project_path):
        root = load_settings(reload=True).project_root
    if root is None:
        raise ConfigError(
            "No project found (create one with 'srunx config init-project')"
        )
    return project_store(root), validate_project_file


def _key_path(key: str, cluster: str | None) -> list[str]:
    """Map a user-facing key to its TOML location.

    Job defaults (``resources.*``, ``environment.*``, ``log_dir``,
    ``work_dir``) live under ``defaults`` or ``clusters.<c>.defaults``;
    anything else is a top-level key, or a ``clusters.<c>`` key with
    ``--cluster`` (``host``, ``remote_dir``, ...).
    """
    parts = key.split(".")
    if parts[0] in DEFAULTS_KEYS:
        return (["clusters", cluster, "defaults"] if cluster else ["defaults"]) + parts
    return (["clusters", cluster] if cluster else []) + parts


def _parse_value(raw: str) -> Any:
    """TOML literal (number, bool, array, quoted string); otherwise a string.

    TOML dates/times are kept as strings: ``04:00:00`` means a Slurm time
    limit, not a TOML local time.
    """
    import datetime

    try:
        value = tomlkit.parse(f"v = {raw}")["v"].unwrap()
    except Exception:  # noqa: BLE001 — bare words are strings
        return raw
    if isinstance(value, (datetime.date, datetime.time)):
        return raw
    return value


SetProjectOpt = Annotated[
    bool,
    typer.Option(
        "--project", help="Edit the project config instead of the user config"
    ),
]
ProjectPathOpt = Annotated[
    Path | None,
    typer.Option("--project-path", help="Project root (default: discovered)"),
]


@config_app.command("set")
def config_set(
    key: Annotated[str, typer.Argument(help="Dotted key, e.g. resources.partition")],
    value: Annotated[str, typer.Argument(help="TOML value; bare words are strings")],
    cluster: ClusterOpt = None,
    project: SetProjectOpt = False,
    project_path: ProjectPathOpt = None,
) -> None:
    """Set a value (job defaults go under ``defaults`` / ``clusters.<c>.defaults``)."""
    try:
        store, validate = _target(project, project_path)
        before = store.read()
        store.set(_key_path(key, cluster), _parse_value(value), validate=validate)
        _forget_changed_connections(store, before)
    except (ConfigError, ConfigFileError, ConfigConflictError) as exc:
        raise _fail(exc) from exc
    Console().print(f"✅ {'.'.join(_key_path(key, cluster))} = {value} ({store.path})")


@config_app.command("delete")
def config_delete(
    key: Annotated[str, typer.Argument(help="Dotted key to remove")],
    cluster: ClusterOpt = None,
    project: SetProjectOpt = False,
    project_path: ProjectPathOpt = None,
) -> None:
    """Remove a key (and any ``unset`` for it) so the value is inherited again."""
    try:
        store, validate = _target(project, project_path)
        from srunx.common.config_store import inherit_in

        path = _key_path(key, cluster)
        before = store.read()
        store.edit(lambda doc: inherit_in(doc, path), validate=validate)
        _forget_changed_connections(store, before)
    except (ConfigError, ConfigFileError, ConfigConflictError) as exc:
        raise _fail(exc) from exc
    Console().print(f"✅ removed {'.'.join(_key_path(key, cluster))} ({store.path})")


@config_app.command("unset")
def config_unset(
    key: Annotated[str, typer.Argument(help="Dotted key, e.g. resources.partition")],
    cluster: ClusterOpt = None,
    project: SetProjectOpt = False,
    project_path: ProjectPathOpt = None,
) -> None:
    """Explicitly clear an inherited value (adds the key to the table's ``unset``)."""
    try:
        store, validate = _target(project, project_path)
        path = _key_path(key, cluster)
        table_path, leaf = path[:-1], path[-1]

        def mutate(doc: Any) -> None:
            node = doc
            for part in table_path:
                if part not in node:
                    node.add(part, tomlkit.table())
                node = node[part]
            if leaf in node:
                del node[leaf]
            current = list(node.get("unset", []))
            if leaf not in current:
                current.append(leaf)
            node["unset"] = current

        before = store.read()
        store.edit(mutate, validate=validate)
        _forget_changed_connections(store, before)
    except (ConfigError, ConfigFileError, ConfigConflictError) as exc:
        raise _fail(exc) from exc
    Console().print(f"✅ unset {'.'.join(path)} ({store.path})")
