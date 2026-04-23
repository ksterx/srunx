"""``srunx template`` sub-application: list, show."""

import sys
from typing import Annotated

import typer
from rich.console import Console
from rich.panel import Panel
from rich.syntax import Syntax
from rich.table import Table

from srunx.common.logging import get_logger
from srunx.runtime.templates import get_template_info, get_template_path, list_templates

logger = get_logger(__name__)

template_app = typer.Typer(help="Job template management")


@template_app.command("list")
def template_list() -> None:
    """List all available job templates."""
    templates = list_templates()

    console = Console()
    table = Table(title="Available Job Templates")
    table.add_column("Name", style="cyan")
    table.add_column("Description", style="magenta")
    table.add_column("Use Case", style="green")

    for template in templates:
        table.add_row(
            template["name"],
            template["description"],
            template["use_case"],
        )

    console.print(table)


@template_app.command("show")
def template_show(
    name: Annotated[str, typer.Argument(help="Template name")],
) -> None:
    """Show template details and content."""
    try:
        info = get_template_info(name)
        template_path = get_template_path(name)

        console = Console()
        console.print(f"\n[bold cyan]Template: {info['name']}[/bold cyan]")
        console.print(f"[yellow]Description:[/yellow] {info['description']}")
        console.print(f"[yellow]Use Case:[/yellow] {info['use_case']}")
        console.print(f"[yellow]Path:[/yellow] {template_path}\n")

        # Read and display template content
        with open(template_path, encoding="utf-8") as f:
            content = f.read()

        syntax = Syntax(
            content,
            "bash",
            theme="monokai",
            line_numbers=True,
            background_color="default",
        )

        panel = Panel(
            syntax,
            title=f"[bold cyan]{info['name']}.slurm[/bold cyan]",
            border_style="blue",
            padding=(1, 2),
        )

        console.print(panel)

    except ValueError as e:
        logger.error(str(e))
        sys.exit(1)
    except Exception as e:
        logger.error(f"Error showing template: {e}")
        sys.exit(1)
