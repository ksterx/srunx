"""Configuration management endpoints: /api/config"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, ValidationError

from srunx.config import (
    SrunxConfig,
    create_example_config,
    get_config,
    get_config_paths,
    load_config_from_file,
    save_user_config,
)

router = APIRouter(prefix="/api/config", tags=["config"])

# ── Shared models ────────────────────────────────────────────────────────────


class ConfigPathInfo(BaseModel):
    path: str
    exists: bool
    source: str


# ── General config endpoints (existing) ─────────────────────────────────────


@router.get("")
async def get_current_config() -> dict[str, Any]:
    """Return the current merged configuration."""
    config = get_config(reload=True)
    return config.model_dump()


@router.put("")
async def update_config(body: dict[str, Any]) -> dict[str, Any]:
    """Validate and save configuration to the user config file."""
    try:
        config = SrunxConfig.model_validate(body)
    except ValidationError as e:
        raise HTTPException(status_code=422, detail=e.errors()) from e

    try:
        save_user_config(config)
    except OSError as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to write config file: {e}"
        ) from e

    updated = get_config(reload=True)
    return updated.model_dump()


@router.get("/paths")
async def get_paths() -> list[ConfigPathInfo]:
    """Return all config file paths with their existence status."""
    sources = ["system", "user", "project (.srunx.json)", "project (srunx.json)"]
    paths = get_config_paths()
    return [
        ConfigPathInfo(
            path=str(p),
            exists=p.exists(),
            source=sources[i] if i < len(sources) else f"config-{i}",
        )
        for i, p in enumerate(paths)
    ]


@router.post("/reset")
async def reset_config() -> dict[str, Any]:
    """Reset user config to defaults by saving a fresh SrunxConfig."""
    default_config = SrunxConfig()
    save_user_config(default_config)
    updated = get_config(reload=True)
    return updated.model_dump()


# ── SSH Profile endpoints ────────────────────────────────────────────────────


def _get_config_manager():
    """Instantiate ConfigManager per-request to read fresh state from disk."""
    from srunx.ssh.core.config import ConfigManager

    return ConfigManager()


class SSHProfileCreateRequest(BaseModel):
    name: str
    hostname: str
    username: str
    key_filename: str
    port: int = 22
    description: str | None = None
    ssh_host: str | None = None
    proxy_jump: str | None = None


class MountCreateRequest(BaseModel):
    name: str
    local: str
    remote: str


@router.get("/ssh/profiles")
async def list_ssh_profiles() -> dict[str, Any]:
    """List all SSH profiles and the current active profile."""
    cm = _get_config_manager()
    profiles = cm.list_profiles()
    return {
        "current": cm.get_current_profile_name(),
        "profiles": {name: p.model_dump() for name, p in profiles.items()},
    }


@router.post("/ssh/profiles")
async def add_ssh_profile(body: SSHProfileCreateRequest) -> dict[str, Any]:
    """Add a new SSH profile."""
    from srunx.ssh.core.config import ServerProfile

    cm = _get_config_manager()
    if cm.get_profile(body.name):
        raise HTTPException(
            status_code=409, detail=f"Profile '{body.name}' already exists"
        )

    profile = ServerProfile(
        hostname=body.hostname,
        username=body.username,
        key_filename=body.key_filename,
        port=body.port,
        description=body.description,
        ssh_host=body.ssh_host,
        proxy_jump=body.proxy_jump,
    )
    cm.add_profile(body.name, profile)
    return profile.model_dump()


@router.put("/ssh/profiles/{name}")
async def update_ssh_profile(name: str, body: dict[str, Any]) -> dict[str, Any]:
    """Update an existing SSH profile."""
    cm = _get_config_manager()
    if not cm.get_profile(name):
        raise HTTPException(status_code=404, detail=f"Profile '{name}' not found")

    # Filter to only valid ServerProfile fields
    valid_fields = {
        "hostname",
        "username",
        "key_filename",
        "port",
        "description",
        "ssh_host",
        "proxy_jump",
        "env_vars",
    }
    update_data = {k: v for k, v in body.items() if k in valid_fields}
    cm.update_profile(name, **update_data)

    updated = cm.get_profile(name)
    return updated.model_dump() if updated else {}


@router.delete("/ssh/profiles/{name}")
async def delete_ssh_profile(name: str) -> dict[str, bool]:
    """Delete an SSH profile."""
    cm = _get_config_manager()
    if not cm.remove_profile(name):
        raise HTTPException(status_code=404, detail=f"Profile '{name}' not found")
    return {"ok": True}


@router.post("/ssh/profiles/{name}/activate")
async def activate_ssh_profile(name: str) -> dict[str, bool]:
    """Set an SSH profile as the current active profile."""
    cm = _get_config_manager()
    if not cm.set_current_profile(name):
        raise HTTPException(status_code=404, detail=f"Profile '{name}' not found")
    return {"ok": True}


@router.post("/ssh/profiles/{name}/mounts")
async def add_profile_mount(name: str, body: MountCreateRequest) -> dict[str, Any]:
    """Add a mount to an SSH profile."""
    from srunx.ssh.core.config import MountConfig

    cm = _get_config_manager()
    if not cm.get_profile(name):
        raise HTTPException(status_code=404, detail=f"Profile '{name}' not found")

    try:
        mount = MountConfig(name=body.name, local=body.local, remote=body.remote)
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e)) from e

    cm.add_profile_mount(name, mount)
    return mount.model_dump()


@router.delete("/ssh/profiles/{name}/mounts/{mount_name}")
async def remove_profile_mount(name: str, mount_name: str) -> dict[str, bool]:
    """Remove a mount from an SSH profile."""
    cm = _get_config_manager()
    if not cm.remove_profile_mount(name, mount_name):
        raise HTTPException(status_code=404, detail="Mount not found")
    return {"ok": True}


# ── Environment variables endpoint ──────────────────────────────────────────

_ENV_VAR_DESCRIPTIONS: dict[str, str] = {
    "SRUNX_DEFAULT_NODES": "Default number of compute nodes",
    "SRUNX_DEFAULT_GPUS_PER_NODE": "Default GPUs per node",
    "SRUNX_DEFAULT_NTASKS_PER_NODE": "Default tasks per node",
    "SRUNX_DEFAULT_CPUS_PER_TASK": "Default CPUs per task",
    "SRUNX_DEFAULT_MEMORY_PER_NODE": "Default memory per node (e.g. 32GB)",
    "SRUNX_DEFAULT_TIME_LIMIT": "Default time limit (e.g. 2:00:00)",
    "SRUNX_DEFAULT_NODELIST": "Default nodelist",
    "SRUNX_DEFAULT_PARTITION": "Default SLURM partition",
    "SRUNX_DEFAULT_CONDA": "Default conda environment",
    "SRUNX_DEFAULT_VENV": "Default virtual environment path",
    "SRUNX_DEFAULT_CONTAINER": "Default container image",
    "SRUNX_DEFAULT_CONTAINER_RUNTIME": "Default container runtime",
    "SRUNX_DEFAULT_LOG_DIR": "Default log directory",
    "SRUNX_DEFAULT_WORK_DIR": "Default working directory",
    "SRUNX_SSH_PROFILE": "SSH profile for web server",
    "SRUNX_SSH_HOSTNAME": "SSH hostname for web server",
    "SRUNX_SSH_USERNAME": "SSH username for web server",
    "SRUNX_SSH_KEY": "SSH key path for web server",
    "SRUNX_SSH_PORT": "SSH port for web server",
    "SRUNX_WORKFLOW_DIR": "Workflow directory for web server",
    "SRUNX_TEMP_DIR": "Temporary directory for SSH operations",
    "SLACK_WEBHOOK_URL": "Slack webhook URL for notifications",
}


class EnvVarInfo(BaseModel):
    name: str
    value: str
    description: str


@router.get("/env")
async def get_env_vars() -> list[EnvVarInfo]:
    """Return all SRUNX_* and SLACK_WEBHOOK_URL environment variables currently set."""
    result: list[EnvVarInfo] = []
    for name, description in sorted(_ENV_VAR_DESCRIPTIONS.items()):
        value = os.environ.get(name)
        if value is not None:
            result.append(EnvVarInfo(name=name, value=value, description=description))
    return result


# ── Project config endpoints (mount-based) ──────────────────────────────────


class ProjectInfo(BaseModel):
    mount_name: str
    local_path: str
    remote_path: str
    config_exists: bool
    config_path: str


class ProjectConfigResponse(BaseModel):
    mount_name: str
    local_path: str
    config_path: str
    exists: bool
    config: dict[str, Any] | None


def _resolve_project_path(local_dir: str) -> Path:
    """Find .srunx.json or srunx.json in a local directory."""
    local = Path(local_dir).expanduser().resolve()
    for filename in [".srunx.json", "srunx.json"]:
        candidate = local / filename
        if candidate.exists():
            return candidate
    return local / ".srunx.json"


def _get_mount_from_profile(mount_name: str) -> tuple[str, str]:
    """Get (local, remote) paths for a mount from the current SSH profile."""
    cm = _get_config_manager()
    current_name = cm.get_current_profile_name()
    if not current_name:
        raise HTTPException(
            status_code=400, detail="No active SSH profile. Activate a profile first."
        )
    profile = cm.get_profile(current_name)
    if not profile:
        raise HTTPException(status_code=404, detail="Active profile not found")

    for m in profile.mounts:
        if m.name == mount_name:
            return m.local, m.remote

    raise HTTPException(
        status_code=404,
        detail=f"Mount '{mount_name}' not found in profile '{current_name}'",
    )


@router.get("/projects")
async def list_projects() -> list[ProjectInfo]:
    """List projects from the current SSH profile's mounts."""
    cm = _get_config_manager()
    current_name = cm.get_current_profile_name()
    if not current_name:
        return []

    profile = cm.get_profile(current_name)
    if not profile:
        return []

    result: list[ProjectInfo] = []
    for m in profile.mounts:
        config_path = _resolve_project_path(m.local)
        result.append(
            ProjectInfo(
                mount_name=m.name,
                local_path=m.local,
                remote_path=m.remote,
                config_exists=config_path.exists(),
                config_path=str(config_path),
            )
        )
    return result


@router.get("/projects/{mount_name}")
async def get_project_config(mount_name: str) -> ProjectConfigResponse:
    """Read .srunx.json from a mount's local directory."""
    local, _ = _get_mount_from_profile(mount_name)
    config_path = _resolve_project_path(local)

    if config_path.exists():
        data = load_config_from_file(config_path)
        return ProjectConfigResponse(
            mount_name=mount_name,
            local_path=local,
            config_path=str(config_path),
            exists=True,
            config=data,
        )

    return ProjectConfigResponse(
        mount_name=mount_name,
        local_path=local,
        config_path=str(config_path),
        exists=False,
        config=None,
    )


@router.put("/projects/{mount_name}")
async def update_project_config(
    mount_name: str, body: dict[str, Any]
) -> ProjectConfigResponse:
    """Save .srunx.json to a mount's local directory."""
    local, _ = _get_mount_from_profile(mount_name)

    try:
        config = SrunxConfig.model_validate(body)
    except ValidationError as e:
        raise HTTPException(status_code=422, detail=e.errors()) from e

    config_path = Path(local).expanduser().resolve() / ".srunx.json"
    try:
        with open(config_path, "w", encoding="utf-8") as f:
            json.dump(config.model_dump(exclude_unset=True), f, indent=2)
    except OSError as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to write project config: {e}"
        ) from e

    return ProjectConfigResponse(
        mount_name=mount_name,
        local_path=local,
        config_path=str(config_path),
        exists=True,
        config=config.model_dump(),
    )


@router.post("/projects/{mount_name}/init")
async def init_project_config(mount_name: str) -> ProjectConfigResponse:
    """Initialize .srunx.json in a mount's local directory."""
    local, _ = _get_mount_from_profile(mount_name)
    config_path = Path(local).expanduser().resolve() / ".srunx.json"

    if config_path.exists():
        raise HTTPException(status_code=409, detail=f"{config_path} already exists")

    example = create_example_config()
    try:
        with open(config_path, "w", encoding="utf-8") as f:
            f.write(example)
    except OSError as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to create project config: {e}"
        ) from e

    data = json.loads(example)
    return ProjectConfigResponse(
        mount_name=mount_name,
        local_path=local,
        config_path=str(config_path),
        exists=True,
        config=data,
    )
