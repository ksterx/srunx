"""FastAPI application factory for srunx Web UI."""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from srunx.logging import get_logger

from .config import get_web_config
from .deps import set_adapter
from .routers import files, history, jobs, resources, workflows
from .ssh_adapter import SlurmSSHAdapter

_FRONTEND_DIST = Path(__file__).parent / "frontend" / "dist"
logger = get_logger(__name__)


def _print_ui_banner(
    *, host: str, port: int, profile: str | None, status: str, verbose: bool
) -> None:
    """Print a rich banner after SSH setup so users see actual connection state."""
    from rich.console import Console, ConsoleRenderable, Group
    from rich.panel import Panel
    from rich.table import Table
    from rich.text import Text

    try:
        from srunx import __version__ as _version
    except Exception:
        _version = "?"

    url = f"http://{host}:{port}"

    # Resolve profile details when available.
    host_str: str | None = None
    mounts: list[tuple[str, str]] = []
    if profile:
        try:
            from srunx.ssh.core.config import ConfigManager

            sp = ConfigManager().get_profile(profile)
            if sp is not None:
                host_str = f"{sp.username}@{sp.hostname}"
                mounts = [(m.local, m.remote) for m in sp.mounts]
        except Exception:
            pass

    if status == "connected":
        status_badge = "[bold green]● connected[/bold green]"
    elif status == "failed":
        status_badge = "[bold red]● failed[/bold red]"
    else:
        status_badge = "[bold yellow]○ no profile[/bold yellow]"

    # Main info grid — label / value, with optional right-aligned badge.
    info = Table.grid(padding=(0, 2), expand=True)
    info.add_column(style="dim", justify="right", no_wrap=True)
    info.add_column(ratio=1)
    info.add_column(justify="right", no_wrap=True)

    info.add_row(
        "URL", f"[cyan underline][link={url}]{url}[/link][/cyan underline]", ""
    )
    if profile:
        info.add_row("Profile", f"[bold]{profile}[/bold]", status_badge)
        if host_str:
            info.add_row("Host", f"[cyan]{host_str}[/cyan]", "")
    elif status == "failed":
        info.add_row("Profile", "[dim]—[/dim]", status_badge)
    else:
        info.add_row(
            "Profile",
            "[dim]none configured — set via `srunx ssh profile`[/dim]",
            status_badge,
        )

    # Mounts — expanded list aligned on the arrow.
    home = str(Path.home())

    def _abbr(p: str) -> str:
        return "~" + p[len(home) :] if p.startswith(home) else p

    mounts_block: Text | None = None
    if mounts:
        mounts_block = Text()
        for i, (local, remote) in enumerate(mounts):
            if i > 0:
                mounts_block.append("\n")
            mounts_block.append("  • ", style="dim")
            mounts_block.append(_abbr(local), style="magenta")
            mounts_block.append("\n       → ", style="dim")
            mounts_block.append(remote, style="cyan")
    elif profile:
        mounts_block = Text("  no mounts configured", style="dim")

    # Assemble body: info grid, blank line, mounts section (if any).
    body_parts: list[ConsoleRenderable] = [info]
    if mounts_block is not None:
        body_parts.append(Text(""))
        body_parts.append(Text("Mounts", style="dim"))
        body_parts.append(mounts_block)
    body = Group(*body_parts)

    title = Text()
    title.append(" ▲ srunx ", style="bold black on bright_cyan")
    title.append(f" v{_version} ", style="dim")

    subtitle_parts = ["[dim]ctrl+c[/dim] quit"]
    if not verbose:
        subtitle_parts.append("[dim]-v[/dim] verbose logs")
    subtitle = "   ·   ".join(subtitle_parts)

    Console().print(
        Panel(
            body,
            title=title,
            title_align="left",
            subtitle=subtitle,
            subtitle_align="right",
            border_style="bright_cyan",
            expand=False,
            padding=(1, 2),
        )
    )


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Manage SSH connection lifecycle.

    If no SSH configuration is provided, the server starts without
    a SLURM connection (API endpoints will return 503).
    """
    config = get_web_config()
    adapter: SlurmSSHAdapter | None = None
    connection_status: str  # "connected" | "failed" | "none"

    # Resolve SSH profile: explicit config > current profile > none
    profile_name = config.ssh_profile
    if not profile_name and not (config.ssh_hostname and config.ssh_username):
        from srunx.ssh.core.config import ConfigManager

        cm = ConfigManager()
        current = cm.get_current_profile_name()
        if current:
            profile_name = current
            logger.info(f"Using current SSH profile: {current}")

    has_ssh_config = profile_name or (config.ssh_hostname and config.ssh_username)

    if has_ssh_config:
        try:
            adapter = SlurmSSHAdapter(
                profile_name=profile_name,
                hostname=config.ssh_hostname,
                username=config.ssh_username,
                key_filename=config.ssh_key_filename,
                port=config.ssh_port,
            )
            logger.info("Connecting to SLURM server via SSH...")
            if adapter.connect():
                logger.info("SSH connection established")
                set_adapter(adapter, profile_name=profile_name)
                connection_status = "connected"
            else:
                logger.warning(
                    "SSH connection failed — SLURM endpoints will be unavailable"
                )
                adapter = None
                connection_status = "failed"
        except Exception as e:
            logger.warning(
                f"SSH setup failed: {e} — SLURM endpoints will be unavailable"
            )
            adapter = None
            connection_status = "failed"
    else:
        logger.info(
            "No SSH configuration provided. Set SRUNX_SSH_PROFILE or "
            "SRUNX_SSH_HOSTNAME + SRUNX_SSH_USERNAME to connect to a SLURM cluster."
        )
        connection_status = "none"

    _print_ui_banner(
        host=config.host,
        port=config.port,
        profile=profile_name,
        status=connection_status,
        verbose=config.verbose,
    )

    import anyio

    try:
        async with anyio.create_task_group() as tg:
            app.state.task_group = tg
            yield
            tg.cancel_scope.cancel()
    finally:
        # Disconnect the *current* adapter (may differ from startup adapter after profile switch)
        from .deps import get_active_profile_name

        current_adapter: SlurmSSHAdapter | None = None
        try:
            from .deps import get_adapter as _get

            current_adapter = _get()
        except Exception:
            pass
        if current_adapter is not None:
            logger.info(
                f"Closing SSH connection (profile: {get_active_profile_name()})..."
            )
            current_adapter.disconnect()


def create_app() -> FastAPI:
    """Build and configure the FastAPI application."""
    config = get_web_config()

    app = FastAPI(
        title="srunx",
        version="0.1.0",
        lifespan=lifespan,
    )

    # CORS — allow Vite dev server in development
    app.add_middleware(
        CORSMiddleware,
        allow_origins=config.cors_origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # REST routers
    from .routers import config as config_router
    from .routers import templates

    app.include_router(config_router.router)
    app.include_router(jobs.router)
    app.include_router(workflows.router)
    app.include_router(resources.router)
    app.include_router(history.router)
    app.include_router(files.router)
    app.include_router(templates.router)

    # Serve frontend static files (production) with SPA fallback
    if _FRONTEND_DIST.exists():
        app.mount(
            "/assets",
            StaticFiles(directory=str(_FRONTEND_DIST / "assets")),
            name="static-assets",
        )

        index_html = _FRONTEND_DIST / "index.html"

        @app.get("/{full_path:path}")
        async def spa_fallback(full_path: str) -> FileResponse:
            """Serve index.html for all non-API routes (SPA client-side routing)."""
            # Check if a static file exists at the path
            static_file = _FRONTEND_DIST / full_path
            if static_file.is_file():
                return FileResponse(static_file)
            return FileResponse(index_html)

    return app


def main() -> None:
    """Entry point for `srunx ui` command."""
    import uvicorn

    config = get_web_config()
    uvicorn.run(
        "srunx.web.app:create_app",
        factory=True,
        host=config.host,
        port=config.port,
    )
