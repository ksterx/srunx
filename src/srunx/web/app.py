"""FastAPI application factory for srunx Web UI."""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from .config import get_web_config
from .deps import set_adapter
from .routers import files, history, jobs, resources, workflows
from .ssh_adapter import SlurmSSHAdapter

_FRONTEND_DIST = Path(__file__).parent / "frontend" / "dist"
_logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Manage SSH connection lifecycle.

    If no SSH configuration is provided, the server starts without
    a SLURM connection (API endpoints will return 503).
    """
    config = get_web_config()
    adapter: SlurmSSHAdapter | None = None

    # Resolve SSH profile: explicit config > current profile > none
    profile_name = config.ssh_profile
    if not profile_name and not (config.ssh_hostname and config.ssh_username):
        from srunx.ssh.core.config import ConfigManager

        cm = ConfigManager()
        current = cm.get_current_profile_name()
        if current:
            profile_name = current
            _logger.info("Using current SSH profile: %s", current)

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
            _logger.info("Connecting to SLURM server via SSH...")
            if adapter.connect():
                _logger.info("SSH connection established")
                set_adapter(adapter)
            else:
                _logger.warning(
                    "SSH connection failed — SLURM endpoints will be unavailable"
                )
                adapter = None
        except Exception as e:
            _logger.warning(
                "SSH setup failed: %s — SLURM endpoints will be unavailable", e
            )
            adapter = None
    else:
        _logger.info(
            "No SSH configuration provided. Set SRUNX_SSH_PROFILE or "
            "SRUNX_SSH_HOSTNAME + SRUNX_SSH_USERNAME to connect to a SLURM cluster."
        )

    import anyio

    try:
        async with anyio.create_task_group() as tg:
            app.state.task_group = tg
            yield
            tg.cancel_scope.cancel()
    finally:
        if adapter:
            _logger.info("Closing SSH connection...")
            adapter.disconnect()


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

    app.include_router(config_router.router)
    app.include_router(jobs.router)
    app.include_router(workflows.router)
    app.include_router(resources.router)
    app.include_router(history.router)
    app.include_router(files.router)

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
    """Entry point for `srunx-web` command."""
    import uvicorn

    config = get_web_config()
    uvicorn.run(
        "srunx.web.app:create_app",
        factory=True,
        host=config.host,
        port=config.port,
    )
