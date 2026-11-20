"""Per-request cluster and project selection for the Web API (design §9).

Every ``/api/*`` request may carry ``?cluster=<name>`` and
``?project=<name>``. :class:`ClusterContextMiddleware` stores them in
context variables for the duration of the request (pure ASGI, so they
propagate into the endpoint and into ``anyio.to_thread`` workers), points
config discovery at the selected project, and activates the effective
config so domain models pick up the right defaults. Nothing about the
selection is process-global: two tabs on different clusters never affect
each other.

Projects are addressed by name and must be one of the server's known
project roots (:func:`known_projects`); the server never guesses a
project from its own cwd per request.
"""

from __future__ import annotations

import contextvars
import json
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs

from srunx.common import paths
from srunx.common.logging import get_logger

logger = get_logger(__name__)

_cluster: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "srunx_web_cluster", default=None
)
_project: contextvars.ContextVar[Path | None] = contextvars.ContextVar(
    "srunx_web_project", default=None
)


def known_projects() -> dict[str, Path]:
    """``{project name: root}`` for every project root the server serves."""
    from srunx.common.config import ConfigError, ProjectConfigFile, _load_file

    from .config import get_web_config

    roots: list[Path] = [Path(p).expanduser() for p in get_web_config().projects]
    found = paths.find_project_root()
    if found is not None:
        roots.append(found)
    result: dict[str, Path] = {}
    for root in roots:
        root = root.resolve()
        try:
            _raw, cfg = _load_file(paths.project_config_path(root), ProjectConfigFile)
        except ConfigError as exc:
            logger.warning(f"Ignoring project {root}: {exc}")
            continue
        if cfg is not None and cfg.name not in result:
            result[cfg.name] = root
    return result


def request_cluster() -> str | None:
    """The cluster named by the current request (``None`` when absent)."""
    return _cluster.get()


def request_project() -> Path | None:
    return _project.get()


def selected_cluster() -> str | None:
    """Request cluster, else the configured default for the request's project."""
    name = _cluster.get()
    if name:
        return name
    from srunx.common.config import ConfigError, load_settings

    try:
        settings = load_settings()
    except ConfigError:
        return None
    found, origin = settings.cluster_source()
    # $SRUNX_CLUSTER of the server process is ambient state: ignore it.
    if found is None or origin.startswith("$"):
        found = (
            (settings.project.cluster if settings.project else None)
            or settings.user.default_cluster
            or settings.system.default_cluster
        )
    return found if found in settings.clusters else None


async def _reject(send: Any, status: int, detail: str) -> None:
    body = json.dumps({"detail": detail}).encode()
    await send(
        {
            "type": "http.response.start",
            "status": status,
            "headers": [(b"content-type", b"application/json")],
        }
    )
    await send({"type": "http.response.body", "body": body})


class ClusterContextMiddleware:
    """Bind ``?cluster=`` / ``?project=`` to the request's context."""

    def __init__(self, app: Any) -> None:
        self.app = app

    async def __call__(self, scope: Any, receive: Any, send: Any) -> None:
        if scope.get("type") != "http" or not scope.get("path", "").startswith("/api/"):
            await self.app(scope, receive, send)
            return
        query = parse_qs(scope.get("query_string", b"").decode())
        cluster = (query.get("cluster") or [None])[0] or None
        project_name = (query.get("project") or [None])[0] or None

        project_root: Path | None = None
        if project_name:
            project_root = known_projects().get(project_name)
            if project_root is None:
                await _reject(send, 400, f"Unknown project '{project_name}'")
                return

        from srunx.common.config import (
            _NO_PROJECT,
            ConfigError,
            _active,
            _project_override,
            load_settings,
        )

        tokens: list[tuple[Any, Any]] = [
            (_cluster, _cluster.set(cluster)),
            (_project, _project.set(project_root)),
            # No ?project= means "no project", never the server's cwd.
            (_project_override, _project_override.set(project_root or _NO_PROJECT)),
        ]
        try:
            try:
                name = selected_cluster()
                settings = load_settings(reload=True)
                if name is not None and name not in settings.clusters:
                    name = None
                active = settings.effective(name)
            except ConfigError as exc:
                logger.debug(f"No active config for request: {exc}")
                active = None
            tokens.append((_active, _active.set(active)))
            await self.app(scope, receive, send)
        finally:
            for var, token in reversed(tokens):
                var.reset(token)
