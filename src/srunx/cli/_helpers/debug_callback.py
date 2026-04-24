"""Debug callback that pretty-prints the rendered SLURM script on submit."""

import tempfile

from rich.console import Console
from rich.panel import Panel
from rich.syntax import Syntax

from srunx.callbacks import Callback
from srunx.common.logging import get_logger
from srunx.domain import (
    Job,
    JobType,
    ShellJob,
)
from srunx.runtime.rendering import render_job_script, render_shell_job_script
from srunx.runtime.templates import get_template_path

logger = get_logger(__name__)


class DebugCallback(Callback):
    """Callback to display rendered SLURM scripts in debug mode."""

    def __init__(self):
        self.console = Console()

    def on_job_submitted(self, job: JobType) -> None:
        """Display the rendered SLURM script when a job is submitted."""
        try:
            # Render the script to get the content
            with tempfile.TemporaryDirectory() as temp_dir:
                if isinstance(job, Job):
                    # Debug render: we just need the default template path.
                    # Use ``get_template_path("base")`` instead of constructing a
                    # full ``Slurm()`` instance — this callback fires from
                    # inside the submit pipeline, and ``resolve_transport()``
                    # has already done its job by the time we land here.
                    # Spinning a second Slurm would risk a nested transport
                    # resolution and also bypass the test fixture patch of
                    # ``srunx.cli.main.Slurm``.
                    template_path = get_template_path("base")
                    script_path = render_job_script(
                        template_path, job, temp_dir, verbose=False
                    )
                elif isinstance(job, ShellJob):
                    script_path = render_shell_job_script(
                        job.script_path, job, temp_dir, verbose=False
                    )
                else:
                    logger.warning(f"Unknown job type for debug display: {type(job)}")
                    return

                # Read the rendered script content
                with open(script_path, encoding="utf-8") as f:
                    script_content = f.read()

                # Display the script with rich formatting
                self.console.print(
                    f"\n[bold blue]🔍 Rendered SLURM Script for Job: {job.name}[/bold blue]"
                )

                # Create syntax highlighted panel
                syntax = Syntax(
                    script_content,
                    "bash",
                    theme="monokai",
                    line_numbers=True,
                    background_color="default",
                )

                panel = Panel(
                    syntax,
                    title=f"[bold cyan]{job.name}.slurm[/bold cyan]",
                    border_style="blue",
                    padding=(1, 2),
                )

                self.console.print(panel)
                self.console.print()

        except Exception as e:
            logger.error(f"Failed to render debug script for job {job.name}: {e}")
