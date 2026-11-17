"""``srunx config`` sub-application: show, paths, init."""

import json
import sys
from typing import Annotated, Any

import typer
from rich.console import Console
from rich.table import Table

from srunx.common.config import create_example_config, get_config, get_config_paths
from srunx.common.logging import get_logger

logger = get_logger(__name__)

config_app = typer.Typer(help="Configuration management")


@config_app.command("show")
def config_show() -> None:
    """Show current configuration."""
    config = get_config()

    console = Console()
    table = Table()
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
    # Lowest -> highest precedence: system, user, project. Only the user
    # file is writable without root and not shadowed by the system one.
    config_path = paths[1]

    if config_path.exists() and not force:
        console = Console()
        console.print(f"Configuration file already exists: {config_path}")
        console.print("Use --force to overwrite")
        return

    try:
        config_path.parent.mkdir(parents=True, exist_ok=True)
        # The same file holds SSH profiles; keep the dir private.
        try:
            config_path.parent.chmod(0o700)
        except OSError:
            pass

        # ``--force`` resets the srunx defaults but must keep keys the
        # example does not define, notably SSH profiles managed by
        # ``ConfigManager`` in this same file.
        data: dict[str, Any] = {}
        if config_path.exists():
            try:
                existing = json.loads(config_path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                existing = None
            if isinstance(existing, dict):
                data = existing
        data.update(json.loads(create_example_config()))

        with open(config_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)

        console = Console()
        console.print(f"✅ Configuration file created: {config_path}")
        console.print("Edit this file to customize your defaults")

    except Exception as e:
        logger.error(f"Error creating configuration file: {e}")
        sys.exit(1)
