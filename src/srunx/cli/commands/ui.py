"""``srunx ui`` command: launch the FastAPI + Vite web interface."""

import sys
from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console

from srunx.common.logging import configure_cli_logging, get_logger

logger = get_logger(__name__)


def ui(
    host: Annotated[str, typer.Option(help="Host to bind")] = "127.0.0.1",
    port: Annotated[int, typer.Option(help="Port to bind")] = 8000,
    verbose: Annotated[
        bool,
        typer.Option("--verbose", "-v", help="Show FastAPI/uvicorn logs"),
    ] = False,
    dev: Annotated[
        bool,
        typer.Option(
            "--dev",
            help="Dev mode: FastAPI --reload + spawn Vite HMR server. Requires a source checkout.",
        ),
    ] = False,
    frontend_port: Annotated[
        int,
        typer.Option(help="Vite dev server port (dev mode only)"),
    ] = 3000,
) -> None:
    """Launch the srunx Web UI."""
    import uvicorn

    from srunx.web.config import get_web_config

    config = get_web_config()
    config.host = host
    config.port = port
    config.verbose = verbose

    # Quiet mode: silence uvicorn access logs and demote srunx loguru to WARNING.
    if not verbose:
        configure_cli_logging(level="WARNING")

    if dev:
        _run_ui_dev(host=host, port=port, frontend_port=frontend_port, verbose=verbose)
        return

    uvicorn.run(
        "srunx.web.app:create_app",
        factory=True,
        host=host,
        port=port,
        log_level="info" if verbose else "warning",
        access_log=verbose,
    )


def _run_ui_dev(*, host: str, port: int, frontend_port: int, verbose: bool) -> None:
    """Launch uvicorn --reload plus Vite dev server in a single foreground process."""
    import shutil
    import signal
    import subprocess

    import uvicorn

    # ``Path(__file__).resolve().parents[2]`` now resolves to ``src/srunx``
    # because this module is one level deeper than the old ``cli/main.py``;
    # keep the final ``web/frontend`` segment identical.
    frontend_dir = Path(__file__).resolve().parents[2] / "web" / "frontend"
    if not (frontend_dir / "package.json").is_file():
        console = Console()
        console.print(
            "[red]--dev requires a source checkout; frontend sources not found at "
            f"{frontend_dir}.[/red]\n"
            "Clone the repo and run `uv sync` before using --dev."
        )
        sys.exit(1)
    if shutil.which("npm") is None:
        console = Console()
        console.print(
            "[red]npm not found on PATH; --dev needs Node.js installed.[/red]"
        )
        sys.exit(1)

    vite = subprocess.Popen(
        ["npm", "run", "dev", "--", "--port", str(frontend_port), "--strictPort"],
        cwd=str(frontend_dir),
    )

    console = Console()
    console.print(
        f"[bold cyan]srunx ui --dev[/bold cyan]  "
        f"backend [green]http://{host}:{port}[/green]  "
        f"frontend [green]http://localhost:{frontend_port}[/green] (HMR)"
    )
    console.print("[dim]Open the frontend URL in your browser.[/dim]")

    try:
        uvicorn.run(
            "srunx.web.app:create_app",
            factory=True,
            host=host,
            port=port,
            reload=True,
            reload_dirs=[str(Path(__file__).resolve().parents[2])],
            log_level="info" if verbose else "warning",
            access_log=verbose,
        )
    finally:
        if vite.poll() is None:
            vite.send_signal(signal.SIGTERM)
            try:
                vite.wait(timeout=5)
            except subprocess.TimeoutExpired:
                vite.kill()
