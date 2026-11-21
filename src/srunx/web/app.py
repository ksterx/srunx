"""FastAPI application factory for srunx Web UI."""

from __future__ import annotations

import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from srunx.common.logging import get_logger
from srunx.observability.storage.models import SweepRun
from srunx.runtime.rendering import SubmissionRenderContext
from srunx.runtime.sweep.reconciler import (
    ExecutorFactoryBundle,
    ExecutorFactoryProvider,
)
from srunx.slurm.clients.ssh import SlurmSSHClient
from srunx.slurm.ssh_executor import SlurmSSHExecutorPool
from srunx.ssh.core.config import MountConfig

from .config import get_web_config
from .routers import deliveries as deliveries_router
from .routers import endpoints as endpoints_router
from .routers import (
    files,
    history,
    jobs,
    resources,
    workflows,
)
from .routers import subscriptions as subscriptions_router
from .routers import sweep_runs as sweep_runs_router
from .routers import watches as watches_router
from .security import WebSecurityMiddleware, assert_safe_bind

_FRONTEND_DIST = Path(__file__).parent / "frontend" / "dist"
logger = get_logger(__name__)


def _print_ui_banner(
    *,
    host: str,
    port: int,
    cluster: str | None,
    projects: dict[str, Path],
    verbose: bool,
) -> None:
    """Print a startup banner with the default cluster and served projects."""
    from rich.console import Console, Group
    from rich.markup import escape
    from rich.panel import Panel
    from rich.table import Table
    from rich.text import Text

    try:
        from srunx import __version__ as _version
    except Exception:
        _version = "?"

    url = f"http://{host}:{port}"
    info = Table.grid(padding=(0, 2), expand=True)
    info.add_column(style="dim", justify="right", no_wrap=True)
    info.add_column(ratio=1)
    info.add_row("URL", f"[cyan underline][link={url}]{url}[/link][/cyan underline]")
    info.add_row(
        "Cluster",
        f"[bold]{escape(cluster)}[/bold] [dim](default; switch in the UI)[/dim]"
        if cluster
        else "[dim]none configured — add one with `srunx cluster add`[/dim]",
    )
    if projects:
        for i, (name, root) in enumerate(sorted(projects.items())):
            info.add_row(
                "Projects" if i == 0 else "", f"{escape(name)}  [dim]{root}[/dim]"
            )
    else:
        info.add_row("Projects", "[dim]none — pass `srunx ui --project PATH`[/dim]")

    title = Text()
    title.append(" ▲ srunx ", style="bold black on bright_cyan")
    title.append(f" v{_version} ", style="dim")
    subtitle_parts = ["[dim]ctrl+c[/dim] quit"]
    if not verbose:
        subtitle_parts.append("[dim]-v[/dim] verbose logs")
    Console().print(
        Panel(
            Group(info),
            title=title,
            title_align="left",
            subtitle="   ·   ".join(subtitle_parts),
            subtitle_align="right",
            border_style="bright_cyan",
            expand=False,
            padding=(1, 2),
        )
    )


def _build_web_executor_factory_provider() -> ExecutorFactoryProvider:
    """Return a :data:`ExecutorFactoryProvider` for Web/MCP-originated sweeps.

    Each resumed sweep is routed to the cluster and project recorded when
    it was submitted (:mod:`srunx.runtime.sweep.context`), through a
    per-sweep :class:`SlurmSSHExecutorPool` plus that project's mounts.
    Without a recorded SSH target the sweep is not resumed at all, rather
    than being sent to a default cluster. CLI-originated sweeps return
    ``None`` (local :class:`Slurm` executor, as ``srunx flow`` does).
    """
    try:
        pool_size = max(1, int(os.environ.get("SRUNX_SSH_POOL_SIZE", "4")))
    except ValueError:
        pool_size = 4
    pool_size = min(pool_size, 8)

    def provider(sweep: SweepRun) -> ExecutorFactoryBundle | None:
        if sweep.submission_source == "cli":
            return None
        from srunx.common.config import ConfigError, load_settings, project_scope
        from srunx.runtime.sweep.context import load_sweep_context
        from srunx.runtime.sweep.reconciler import SweepRecoveryRefused

        assert sweep.id is not None
        ctx = load_sweep_context(sweep.id)
        if ctx is None:
            raise SweepRecoveryRefused("its cluster was not recorded")
        try:
            with project_scope(ctx.project_root):
                spec = SlurmSSHClient(profile_name=ctx.cluster).connection_spec
                effective = load_settings(reload=True).effective(ctx.cluster)
        except (ValueError, ConfigError) as exc:
            raise SweepRecoveryRefused(str(exc)) from exc
        mounts: tuple[MountConfig, ...] = tuple(spec.mounts)
        pool = SlurmSSHExecutorPool(spec, callbacks=[], size=pool_size)
        context = SubmissionRenderContext(
            mount_name=None,
            mounts=mounts,
            default_work_dir=None,
        )
        return ExecutorFactoryBundle(
            factory=pool.lease,
            submission_context=context,
            cleanup=pool.close,
            config=effective,
            project_root=ctx.project_root,
        )

    return provider


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Manage SSH connection lifecycle + background pollers.

    Steps (in order):
      1. Initialize the srunx SQLite DB (schema migration + 0600 perms)
      2. Print the banner (default cluster + served projects); no SSH
         connection is opened until a request needs one
      4. Start the ``PollerSupervisor`` with ActiveWatchPoller / DeliveryPoller /
         ResourceSnapshotter — each gated by its own ``SRUNX_DISABLE_*`` env var
         and all collectively disabled under ``uvicorn --reload``.
    """
    from srunx.observability.storage.connection import init_db

    # 1. DB bootstrap (always — cheap + idempotent).
    try:
        init_db()
    except Exception:
        logger.warning(
            "DB initialization failed; persistence may be degraded", exc_info=True
        )

    config = get_web_config()

    # No connection is opened at startup: each request names its cluster
    # and the adapter for it connects lazily (``srunx.web.deps``). The
    # default cluster's adapter backs the background resource snapshotter
    # and sweep recovery.
    from .context import known_projects, selected_cluster
    from .deps import get_adapter_or_none

    default_cluster = selected_cluster()
    adapter: SlurmSSHClient | None = get_adapter_or_none()
    _print_ui_banner(
        host=config.host,
        port=config.port,
        cluster=default_cluster,
        projects=known_projects(),
        verbose=config.verbose,
    )

    import anyio

    from srunx.observability.monitoring.pollers.active_watch_poller import (
        ActiveWatchPoller,
    )
    from srunx.observability.monitoring.pollers.delivery_poller import DeliveryPoller
    from srunx.observability.monitoring.pollers.reload_guard import should_start_pollers
    from srunx.observability.monitoring.pollers.resource_snapshotter import (
        ResourceSnapshotter,
    )
    from srunx.observability.monitoring.pollers.supervisor import (
        Poller,
        PollerSupervisor,
    )

    # 4. Sweep crash-recovery pass. Spawned as a background task on the
    # lifespan task group so the server can become ready without waiting
    # for every incomplete sweep to be reconciled — with many crashed
    # sweeps in the DB the sequential per-sweep resume would otherwise
    # block ``yield`` for tens of seconds. Running concurrently with the
    # active-watch poller is safe because ``workflow_runs.status`` is the
    # source of truth; a poller observing a cell before the orchestrator
    # has registered it just produces an extra transition row, never a
    # missed terminal. Skipped under --reload / SRUNX_DISABLE_POLLER=1
    # for parity with the poller gating.
    #
    # The Web-side provider restores, for every Web / MCP-originated
    # sweep, the per-sweep :class:`SlurmSSHExecutorPool` + mount-aware
    # :class:`SubmissionRenderContext` the dispatcher originally wired
    # in (see :func:`srunx.web.routers.workflows._dispatch_sweep`). CLI
    # sweeps (``submission_source == 'cli'``) stay on the local
    # :class:`Slurm` path by returning ``None`` from the provider.
    reconciler_provider: ExecutorFactoryProvider | None = None
    if should_start_pollers():
        reconciler_provider = _build_web_executor_factory_provider()

    async def _resume_sweeps_in_background(
        provider: ExecutorFactoryProvider | None,
    ) -> None:
        try:
            from srunx.runtime.sweep.reconciler import SweepReconciler

            await SweepReconciler.scan_and_resume_async(
                executor_factory_provider=provider,
            )
        except Exception:
            logger.warning("SweepReconciler startup pass raised", exc_info=True)

    # 5. Background pollers. All skipped in --reload dev mode or when
    # SRUNX_DISABLE_POLLER=1. Each poller is also individually toggleable.
    supervisor: PollerSupervisor | None = None
    if should_start_pollers():
        pollers: list[Poller] = []
        if os.environ.get("SRUNX_DISABLE_ACTIVE_WATCH_POLLER") != "1":
            # Watches are grouped by ``scheduler_key``; the registry builds
            # (and caches) a handle per cluster on demand.
            from srunx.transport.registry import TransportRegistry

            transport_registry = TransportRegistry()
            app.state.transport_registry = transport_registry
            pollers.append(ActiveWatchPoller(registry=transport_registry))
        if os.environ.get("SRUNX_DISABLE_DELIVERY_POLLER") != "1":
            pollers.append(DeliveryPoller(worker_id=f"delivery-{os.getpid()}"))
        if os.environ.get("SRUNX_DISABLE_RESOURCE_SNAPSHOTTER") != "1":
            # ResourceMonitor now accepts an injected ``ResourceSource``.
            # When the SSH adapter is configured we route partition
            # queries through it so a laptop driving a remote cluster
            # produces ``resource_snapshots`` rows identical to what a
            # head-node deployment would record. Fall back to the
            # local-subprocess path only when ``sinfo`` is available
            # on PATH (i.e. we actually are on a SLURM head node) or
            # when the admin explicitly wants to keep the legacy
            # behaviour via ``SRUNX_RESOURCE_SOURCE=subprocess``.
            import shutil

            source_mode = os.environ.get("SRUNX_RESOURCE_SOURCE", "auto")
            resource_source = None
            skip_reason: str | None = None

            if source_mode == "subprocess":
                if shutil.which("sinfo") is None:
                    skip_reason = (
                        "SRUNX_RESOURCE_SOURCE=subprocess but local "
                        "'sinfo' is not on PATH"
                    )
            elif adapter is not None:
                try:
                    from srunx.observability.monitoring.resource_source import (
                        SSHAdapterResourceSource,
                    )
                    from srunx.web.deps import get_adapter_or_none

                    # Resolved per cycle: follows config edits to the
                    # default cluster.
                    resource_source = SSHAdapterResourceSource(get_adapter_or_none)
                except Exception:
                    logger.warning(
                        "Could not build SSHAdapterResourceSource; falling back",
                        exc_info=True,
                    )
            elif shutil.which("sinfo") is None:
                skip_reason = "no SLURM client configured and local 'sinfo' not on PATH"

            if skip_reason is not None:
                logger.info(
                    "Skipping ResourceSnapshotter: {}. Set "
                    "SRUNX_DISABLE_RESOURCE_SNAPSHOTTER=1 to silence this.",
                    skip_reason,
                )
            else:
                try:
                    from srunx.observability.monitoring.resource_monitor import (
                        ResourceMonitor,
                    )

                    # min_gpus=0 because we're observing, not waiting on a threshold.
                    pollers.append(
                        ResourceSnapshotter(
                            resource_monitor=ResourceMonitor(
                                min_gpus=0, source=resource_source
                            ),
                        )
                    )
                except Exception:
                    logger.warning(
                        "Skipping ResourceSnapshotter: init failed",
                        exc_info=True,
                    )

        if pollers:
            supervisor = PollerSupervisor(pollers)
            logger.info("Starting {} background poller(s)", len(pollers))
    else:
        logger.info(
            "Background pollers disabled (reload mode or SRUNX_DISABLE_POLLER=1)"
        )

    try:
        async with anyio.create_task_group() as tg:
            app.state.task_group = tg
            app.state.poller_supervisor = supervisor
            if should_start_pollers():
                tg.start_soon(_resume_sweeps_in_background, reconciler_provider)
            if supervisor is not None:
                tg.start_soon(supervisor.start_all)
            yield
            if supervisor is not None:
                try:
                    await supervisor.shutdown(grace_seconds=5.0)
                except Exception:
                    logger.warning("Poller shutdown raised", exc_info=True)
            tg.cancel_scope.cancel()
    finally:
        # Release the TransportRegistry first so any SSH pools it
        # created (outside the seeded startup adapter) get closed
        # before we drop the startup adapter below. ``close()`` is
        # idempotent and never raises to the caller.
        registry_to_close = getattr(app.state, "transport_registry", None)
        if registry_to_close is not None:
            try:
                registry_to_close.close()
            except Exception:
                logger.warning("TransportRegistry close raised", exc_info=True)

        from .deps import close_adapters

        close_adapters()


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

    # Per-request ``?cluster=`` / ``?project=`` selection.
    from .context import ClusterContextMiddleware

    app.add_middleware(ClusterContextMiddleware)

    # Anti-DNS-rebinding (Host header) + optional bearer token on /api/*.
    app.add_middleware(
        WebSecurityMiddleware,
        auth_token=config.auth_token,
        bind_host=config.host,
        allowed_hosts=config.allowed_hosts,
    )

    # REST routers
    from .routers import clusters as clusters_router
    from .routers import config as config_router
    from .routers import templates

    app.include_router(config_router.router)
    app.include_router(config_router.projects_router)
    app.include_router(clusters_router.router)
    app.include_router(jobs.router)
    app.include_router(workflows.router)
    app.include_router(resources.router)
    app.include_router(history.router)
    app.include_router(files.router)
    app.include_router(templates.router)
    # New CRUD / observability routers for the notification + state overhaul
    app.include_router(endpoints_router.router)
    app.include_router(subscriptions_router.router)
    app.include_router(watches_router.router)
    app.include_router(deliveries_router.router)
    app.include_router(sweep_runs_router.router)

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
            if full_path == "api" or full_path.startswith("api/"):
                from fastapi import HTTPException

                raise HTTPException(status_code=404, detail="Not Found")
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
    assert_safe_bind(config.host, config.auth_token)
    uvicorn.run(
        "srunx.web.app:create_app",
        factory=True,
        host=config.host,
        port=config.port,
    )
