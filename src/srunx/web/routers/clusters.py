"""Cluster management endpoints: /api/clusters.

Clusters are ``[clusters.<name>]`` tables in the user config. There is no
server-side "current cluster": the UI keeps its selection client-side and
sends it with every request as ``?cluster=<name>``.
"""

from __future__ import annotations

import shutil
from typing import Any, Literal

import anyio
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from srunx.common.config import (
    ConfigError,
    load_settings,
    user_store,
    validate_name,
    validate_user_file,
)
from srunx.common.config_store import ConfigFileError, set_in

router = APIRouter(prefix="/api/clusters", tags=["clusters"])

_CONNECTION_FIELDS = (
    "host",
    "hostname",
    "username",
    "key_filename",
    "port",
    "proxy_jump",
    "description",
    "snapshot_root",
)


_SSH_FIELDS = ("host", "hostname", "username", "key_filename", "port", "proxy_jump")


class ClusterInfo(BaseModel):
    name: str
    transport: Literal["local", "ssh"]
    host: str | None = None
    hostname: str | None = None
    username: str | None = None
    key_filename: str | None = None
    port: int = 22
    proxy_jump: str | None = None
    description: str | None = None
    snapshot_root: str | None = None


class ClusterList(BaseModel):
    default: str | None
    clusters: list[ClusterInfo]


class ClusterCreate(ClusterInfo):
    make_default: bool = False


class ClusterTestResult(BaseModel):
    ok: bool
    transport: Literal["local", "ssh"]
    detail: str
    home: str | None = None


def _settings() -> Any:
    try:
        return load_settings(reload=True)
    except ConfigError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


def _info(name: str, cfg: Any) -> ClusterInfo:
    return ClusterInfo(
        name=name, **cfg.model_dump(include={"transport", *_CONNECTION_FIELDS})
    )


def _forget_connection(name: str) -> None:
    """Drop everything cached for *name*'s old connection settings."""
    from srunx.common.remote_env import forget_remote_env

    from ..deps import evict_adapter

    evict_adapter(name)
    forget_remote_env(name)


@router.get("")
async def list_clusters() -> ClusterList:
    settings = _settings()
    default = (
        (settings.project.cluster if settings.project else None)
        or settings.user.default_cluster
        or settings.system.default_cluster
    )
    return ClusterList(
        default=default if default in settings.clusters else None,
        clusters=[_info(n, c) for n, c in sorted(settings.clusters.items())],
    )


def _edit(mutate: Any) -> None:
    try:
        user_store().edit(mutate, validate=validate_user_file)
    except (ConfigError, ConfigFileError) as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


@router.post("")
async def create_cluster(body: ClusterCreate) -> ClusterInfo:
    try:
        validate_name("cluster", body.name)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    if body.name in _settings().clusters:
        raise HTTPException(status_code=409, detail=f"Cluster '{body.name}' exists")
    table: dict[str, Any] = {"transport": body.transport}
    for key in _CONNECTION_FIELDS:
        value = getattr(body, key)
        if value not in (None, "") and not (key == "port" and value == 22):
            table[key] = value

    def mutate(doc: Any) -> None:
        set_in(doc, ("clusters", body.name), table)
        if body.make_default:
            doc["default_cluster"] = body.name

    await anyio.to_thread.run_sync(_edit, mutate)
    return _info(body.name, _settings().clusters[body.name])


@router.put("/{name}")
async def update_cluster(name: str, body: dict[str, Any]) -> ClusterInfo:
    """Update connection fields; ``null`` / ``""`` removes a field.

    ``transport`` may change too; switching to ``local`` drops the SSH
    connection fields.
    """
    if name not in _settings().clusters:
        raise HTTPException(status_code=404, detail=f"Cluster '{name}' not found")
    updates = {k: v for k, v in body.items() if k in _CONNECTION_FIELDS}
    transport = body.get("transport")
    if transport not in (None, "local", "ssh"):
        raise HTTPException(status_code=422, detail="transport must be local or ssh")
    if transport == "local":
        updates = {
            k: (None if k in _SSH_FIELDS else v) for k, v in updates.items()
        } | dict.fromkeys(_SSH_FIELDS)

    def mutate(doc: Any) -> None:
        if "clusters" not in doc or name not in doc["clusters"]:
            raise ConfigError(f"Cluster '{name}' is not defined in the user config")
        table = doc["clusters"][name]
        if transport is not None:
            table["transport"] = transport
        for key, value in updates.items():
            if value in (None, ""):
                if key in table:
                    del table[key]
            else:
                table[key] = value

    await anyio.to_thread.run_sync(_edit, mutate)
    # Later requests must use the new connection settings.
    await anyio.to_thread.run_sync(_forget_connection, name)
    return _info(name, _settings().clusters[name])


@router.delete("/{name}")
async def delete_cluster(name: str) -> dict[str, bool]:
    def mutate(doc: Any) -> None:
        if "clusters" not in doc or name not in doc["clusters"]:
            raise ConfigError(f"Cluster '{name}' is not defined in the user config")
        del doc["clusters"][name]
        if doc.get("default_cluster") == name:
            del doc["default_cluster"]

    await anyio.to_thread.run_sync(_edit, mutate)
    await anyio.to_thread.run_sync(_forget_connection, name)
    return {"ok": True}


@router.post("/{name}/test")
async def test_cluster(name: str) -> ClusterTestResult:
    """Check reachability; for SSH also caches the remote $HOME / XDG dirs."""
    settings = _settings()
    cfg = settings.clusters.get(name)
    if cfg is None:
        raise HTTPException(status_code=404, detail=f"Cluster '{name}' not found")
    if cfg.transport == "local":
        path = shutil.which("sbatch")
        return ClusterTestResult(
            ok=path is not None,
            transport="local",
            detail=f"sbatch at {path}" if path else "sbatch is not on PATH",
        )

    from srunx.common.remote_env import fetch_remote_env

    env = await anyio.to_thread.run_sync(fetch_remote_env, name)
    if env is None:
        return ClusterTestResult(
            ok=False, transport="ssh", detail="connection or environment probe failed"
        )
    return ClusterTestResult(
        ok=True, transport="ssh", detail="connected", home=env.home
    )
