"""``srunx config`` sub-application: show, paths, init."""

import sys
from typing import Annotated

import typer
from rich.console import Console
from rich.table import Table

from srunx.config import create_example_config
from srunx.logging import get_logger

logger = get_logger(__name__)

config_app = typer.Typer(help="Configuration management")


@config_app.command("show")
def config_show() -> None:
    """Show current configuration."""
    _main_module = sys.modules["srunx.cli.main"]

    config = _main_module.get_config()

    console = Console()
    table = Table(title="srunx Configuration")
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
    _main_module = sys.modules["srunx.cli.main"]

    paths = _main_module.get_config_paths()

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
    _main_module = sys.modules["srunx.cli.main"]

    paths = _main_module.get_config_paths()
    config_path = paths[0]  # Use the first (highest precedence) path

    if config_path.exists() and not force:
        console = Console()
        console.print(f"Configuration file already exists: {config_path}")
        console.print("Use --force to overwrite")
        return

    try:
        # Create parent directories if they don't exist
        config_path.parent.mkdir(parents=True, exist_ok=True)

        # Write example config
        example_config = create_example_config()
        with open(config_path, "w", encoding="utf-8") as f:
            f.write(example_config)

        console = Console()
        console.print(f"✅ Configuration file created: {config_path}")
        console.print("Edit this file to customize your defaults")

    except Exception as e:
        logger.error(f"Error creating configuration file: {e}")
        sys.exit(1)
