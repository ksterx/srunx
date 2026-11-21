"""Configuration endpoints: effective config, file editing, projects.

``GET /api/config`` reports the effective defaults for the request's
cluster/project (``?cluster=`` / ``?project=``) with the file every value
came from. Edits never write the effective config back: ``PATCH
/api/config/file`` changes individual keys of one file (user or project)
through the single TOML writer, guarded by an optimistic version token.
"""

from __future__ import annotations

import os
from typing import Any, Literal

import anyio
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from srunx.common import paths
from srunx.common.config import (
    ConfigError,
    load_settings,
    project_store,
    user_store,
    validate_project_file,
    validate_user_file,
)
from srunx.common.config_store import (
    ConfigConflictError,
    ConfigFileError,
    inherit_in,
    set_in,
)

from ..context import known_projects, request_project, selected_cluster

router = APIRouter(prefix="/api/config", tags=["config"])


class EffectiveConfig(BaseModel):
    cluster: str | None
    project: str | None
    project_root: str | None
    values: dict[str, Any]
    sources: dict[str, str]


class ConfigPathInfo(BaseModel):
    path: str
    exists: bool
    source: str


class ConfigFile(BaseModel):
    scope: Literal["user", "project"]
    path: str
    exists: bool
    version: str
    data: dict[str, Any]


class ConfigFilePatch(BaseModel):
    scope: Literal["user", "project"]
    version: str | None = Field(
        default=None, description="Token from GET /api/config/file; 409 if stale"
    )
    set: dict[str, Any] = Field(default_factory=dict)
    delete: list[str] = Field(default_factory=list)
    unset: list[str] = Field(default_factory=list)


def _settings() -> Any:
    try:
        return load_settings(reload=True)
    except ConfigError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


@router.get("")
async def get_effective_config() -> EffectiveConfig:
    """Effective defaults for the request's cluster and project, with sources."""
    settings = _settings()
    cluster = selected_cluster()
    if cluster is not None and cluster not in settings.clusters:
        raise HTTPException(status_code=404, detail=f"Unknown cluster '{cluster}'")
    try:
        config = settings.effective(cluster)
    except ConfigError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    return EffectiveConfig(
        cluster=cluster,
        project=settings.project.name if settings.project else None,
        project_root=str(settings.project_root) if settings.project_root else None,
        values=config.model_dump(),
        sources=config.sources,
    )


@router.get("/paths")
async def get_paths() -> list[ConfigPathInfo]:
    """Config files in precedence order (lowest first) plus the state dir."""
    root = request_project()
    entries = [
        ("system", paths.system_config_path()),
        ("user", paths.user_config_path()),
    ]
    if root is not None:
        entries.append(("project", paths.project_config_path(root)))
    entries.append(("state", paths.state_dir()))
    return [
        ConfigPathInfo(path=str(p), exists=p.exists(), source=label)
        for label, p in entries
    ]


def _store(scope: str) -> tuple[Any, Any]:
    if scope == "user":
        return user_store(), validate_user_file
    root = request_project()
    if root is None:
        raise HTTPException(
            status_code=400, detail="scope=project needs ?project=<name>"
        )
    return project_store(root), validate_project_file


def _read_file(scope: str) -> ConfigFile:
    store, _ = _store(scope)
    try:
        data, version = store.read_with_version()
    except ConfigFileError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    return ConfigFile(
        scope=scope,  # type: ignore[arg-type]
        path=str(store.path),
        exists=bool(version) and store.path.exists(),
        version=version,
        data=data,
    )


@router.get("/file")
async def get_config_file(scope: Literal["user", "project"] = "user") -> ConfigFile:
    """Raw contents of one config file plus its version token."""
    return await anyio.to_thread.run_sync(_read_file, scope)


@router.patch("/file")
async def patch_config_file(body: ConfigFilePatch) -> ConfigFile:
    """Edit individual keys of one config file.

    Keys are dotted paths from the file root, e.g.
    ``defaults.resources.partition`` or
    ``clusters.gpu.defaults.resources.time_limit``. ``delete`` removes a key
    and any ``unset`` entry for it (the value is inherited again); ``unset`` adds it to the table's
    ``unset`` list (explicitly cleared).
    """
    store, validate = _store(body.scope)

    def mutate(doc: Any) -> None:
        for key, value in body.set.items():
            set_in(doc, key.split("."), value)
        for key in body.delete:
            inherit_in(doc, key.split("."))
        for key in body.unset:
            parts = key.split(".")
            node = doc
            for part in parts[:-1]:
                if part not in node:
                    import tomlkit

                    node.add(part, tomlkit.table())
                node = node[part]
            if parts[-1] in node:
                del node[parts[-1]]
            current = list(node.get("unset", []))
            if parts[-1] not in current:
                current.append(parts[-1])
            node["unset"] = current

    def apply() -> ConfigFile:
        from srunx.common.remote_env import connection_changed

        before = store.read() if body.scope == "user" else {}
        store.edit(mutate, validate=validate, expected_version=body.version)
        result = _read_file(body.scope)
        if body.scope == "user":
            # Connection edits must not keep using the old host.
            from .clusters import _forget_connection

            for name in connection_changed(before, result.data):
                _forget_connection(name)
        return result

    try:
        return await anyio.to_thread.run_sync(apply)
    except ConfigConflictError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except (ConfigError, ConfigFileError, ValueError) as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


# ── Environment variables ───────────────────────────────────────────────────

_ENV_VAR_DESCRIPTIONS: dict[str, str] = {
    "SRUNX_CLUSTER": "Cluster selected for CLI commands (ignored by the Web UI)",
    "SRUNX_TEMP_DIR": "Temporary directory for SSH operations",
    "SRUNX_WEB_PROJECTS": "Project roots served by the Web UI",
    "SRUNX_WEB_TOKEN": "Bearer token required on /api/*",
    "SRUNX_WEB_ALLOWED_HOSTS": "Extra Host header values accepted",
    "SLACK_WEBHOOK_URL": "Slack webhook URL for the legacy --slack option",
    "XDG_CONFIG_HOME": "Base directory of the user config",
    "XDG_STATE_HOME": "Base directory of the state DB (unless state_dir is set)",
}


class EnvVarInfo(BaseModel):
    name: str
    value: str
    description: str


@router.get("/env")
async def get_env_vars() -> list[EnvVarInfo]:
    """Environment variables relevant to srunx that are currently set."""
    result: list[EnvVarInfo] = []
    for name, description in sorted(_ENV_VAR_DESCRIPTIONS.items()):
        value = os.environ.get(name)
        if value is not None:
            if name == "SRUNX_WEB_TOKEN":
                value = "********"
            result.append(EnvVarInfo(name=name, value=value, description=description))
    return result


# ── Projects ────────────────────────────────────────────────────────────────

projects_router = APIRouter(prefix="/api/projects", tags=["projects"])


class MountInfo(BaseModel):
    name: str
    local: str
    remote: str
    sync: bool
    exclude: list[str]


class ProjectInfo(BaseModel):
    name: str
    root: str
    config_path: str
    cluster: str | None


class ProjectDetail(ProjectInfo):
    mounts: dict[str, list[MountInfo]]


@projects_router.get("")
async def list_projects() -> list[ProjectInfo]:
    """Projects this server serves (``srunx ui --project`` / cwd)."""

    def collect() -> list[ProjectInfo]:
        out: list[ProjectInfo] = []
        for name, root in sorted(known_projects().items()):
            try:
                settings = load_settings(root, reload=True)
            except ConfigError:
                continue
            out.append(
                ProjectInfo(
                    name=name,
                    root=str(root),
                    config_path=str(paths.project_config_path(root)),
                    cluster=settings.project.cluster if settings.project else None,
                )
            )
        return out

    return await anyio.to_thread.run_sync(collect)


@projects_router.get("/{name}")
async def get_project(name: str) -> ProjectDetail:
    """A project with its resolved mounts on every SSH cluster."""
    root = known_projects().get(name)
    if root is None:
        raise HTTPException(status_code=404, detail=f"Unknown project '{name}'")

    def collect() -> ProjectDetail:
        try:
            settings = load_settings(root, reload=True)
        except ConfigError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        mounts: dict[str, list[MountInfo]] = {}
        for cluster, cfg in settings.clusters.items():
            if cfg.transport != "ssh":
                continue
            mounts[cluster] = [
                MountInfo(
                    name=m.name,
                    local=m.local,
                    remote=m.remote,
                    sync=m.sync,
                    exclude=list(m.exclude),
                )
                for m in settings.mounts(cluster, fetch_remote=False)
            ]
        return ProjectDetail(
            name=name,
            root=str(root),
            config_path=str(paths.project_config_path(root)),
            cluster=settings.project.cluster if settings.project else None,
            mounts=mounts,
        )

    return await anyio.to_thread.run_sync(collect)
