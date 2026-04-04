"""File browsing endpoints: /api/files/*"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Literal

import anyio
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from ..config import get_web_config

_logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/files", tags=["files"])


# ---------------------------------------------------------------------------
# Response schemas
# ---------------------------------------------------------------------------


class SyncRequest(BaseModel):
    mount: str


class MountInfo(BaseModel):
    name: str
    remote: str


class FileEntry(BaseModel):
    name: str
    type: Literal["file", "directory", "symlink"]
    size: int | None = None
    accessible: bool | None = None
    target_kind: Literal["file", "directory"] | None = None


class BrowseResponse(BaseModel):
    entries: list[FileEntry]
    remote_prefix: str
    mount_name: str


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _get_current_profile():
    """Get the current SSH profile from web config or ConfigManager."""
    from ..sync_utils import get_current_profile

    return get_current_profile()


def _find_mount(profile, mount_name: str):
    """Find a mount by name within a profile's mounts.

    Raises HTTPException 404 if the mount is not found.
    """

    for m in profile.mounts:
        if m.name == mount_name:
            return m
    raise HTTPException(status_code=404, detail=f"Mount '{mount_name}' not found")


def _list_entries(target: Path, mount_root: Path) -> list[FileEntry]:
    """List directory entries, filtering hidden files and handling errors.

    This runs on a worker thread via anyio.to_thread.run_sync.
    """
    entries: list[FileEntry] = []

    try:
        children = sorted(target.iterdir())
    except PermissionError as exc:
        raise HTTPException(status_code=403, detail="Permission denied") from exc
    except OSError as exc:
        raise HTTPException(
            status_code=500, detail=f"Failed to read directory: {exc}"
        ) from exc

    for entry in children:
        # Skip hidden files
        if entry.name.startswith("."):
            continue

        try:
            if entry.is_symlink():
                target_kind: Literal["file", "directory"] | None = None
                try:
                    resolved = entry.resolve()
                    accessible = resolved.is_relative_to(mount_root)
                    target_kind = "directory" if resolved.is_dir() else "file"
                except (OSError, ValueError):
                    accessible = False
                entries.append(
                    FileEntry(
                        name=entry.name,
                        type="symlink",
                        accessible=accessible,
                        target_kind=target_kind,
                    )
                )
            elif entry.is_dir():
                entries.append(FileEntry(name=entry.name, type="directory"))
            else:
                try:
                    size = entry.stat().st_size
                except OSError:
                    size = None
                entries.append(FileEntry(name=entry.name, type="file", size=size))
        except PermissionError:
            # Skip entries we cannot stat at all
            continue
        except OSError:
            continue

    return entries


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@router.get("/mounts/config")
async def list_mounts_config() -> list[dict[str, Any]]:
    """List all mounts with full details (name, local, remote) for management."""
    profile = await anyio.to_thread.run_sync(_get_current_profile)
    if profile is None or not profile.mounts:
        return []
    return [
        {
            "name": m.name,
            "local": m.local,
            "remote": m.remote,
            "exclude_patterns": m.exclude_patterns,
        }
        for m in profile.mounts
    ]


@router.get("/mounts")
async def list_mounts() -> list[MountInfo]:
    """Return mount list from the current SSH profile.

    Only returns name and remote prefix (never local paths).
    """
    profile = await anyio.to_thread.run_sync(_get_current_profile)
    if profile is None or not profile.mounts:
        return []
    return [MountInfo(name=m.name, remote=m.remote) for m in profile.mounts]


@router.post("/mounts")
async def add_mount(body: dict) -> dict:
    """Add a new mount to the current SSH profile."""
    name = body.get("name", "")
    local = body.get("local", "")
    remote = body.get("remote", "")
    exclude_patterns: list[str] = body.get("exclude_patterns", [])

    if not name or not local or not remote:
        raise HTTPException(422, "name, local, and remote are required")

    from srunx.ssh.core.config import ConfigManager, MountConfig

    profile = await anyio.to_thread.run_sync(_get_current_profile)
    if profile is None:
        raise HTTPException(503, "No SSH profile configured")

    # Check duplicate
    if any(m.name == name for m in profile.mounts):
        raise HTTPException(409, f"Mount '{name}' already exists")

    try:
        mount = MountConfig(
            name=name,
            local=local,
            remote=remote,
            exclude_patterns=exclude_patterns,
        )
    except Exception as e:
        raise HTTPException(422, str(e)) from e

    cm = ConfigManager()
    profile_name = get_web_config().ssh_profile or cm.get_current_profile_name()
    if not profile_name:
        raise HTTPException(503, "No SSH profile configured")
    pname = profile_name  # bind for lambda
    await anyio.to_thread.run_sync(lambda: cm.add_profile_mount(pname, mount))

    return {
        "name": mount.name,
        "local": mount.local,
        "remote": mount.remote,
        "exclude_patterns": mount.exclude_patterns,
    }


@router.delete("/mounts/{mount_name}")
async def remove_mount(mount_name: str) -> dict[str, str]:
    """Remove a mount from the current SSH profile."""
    from srunx.ssh.core.config import ConfigManager

    profile = await anyio.to_thread.run_sync(_get_current_profile)
    if profile is None:
        raise HTTPException(503, "No SSH profile configured")

    if not any(m.name == mount_name for m in profile.mounts):
        raise HTTPException(404, f"Mount '{mount_name}' not found")

    cm = ConfigManager()
    profile_name = get_web_config().ssh_profile or cm.get_current_profile_name()
    if not profile_name:
        raise HTTPException(503, "No SSH profile configured")
    pname = profile_name  # bind for lambda
    await anyio.to_thread.run_sync(lambda: cm.remove_profile_mount(pname, mount_name))

    return {"status": "deleted", "name": mount_name}


@router.get("/browse")
async def browse_files(mount: str, path: str = "") -> BrowseResponse:
    """Browse local filesystem under a mount's local root.

    Security invariants:
    - Resolved path must stay within the mount's local root
    - Symlink targets outside the mount boundary are marked inaccessible
    - Local filesystem paths are never exposed in the response
    """
    profile = await anyio.to_thread.run_sync(_get_current_profile)
    if profile is None:
        raise HTTPException(status_code=404, detail="No SSH profile configured")

    mount_config = _find_mount(profile, mount)

    mount_root = Path(mount_config.local)  # already expanded/resolved by MountConfig
    target = (mount_root / path).resolve()

    # Security: ensure target is within mount root
    if not target.is_relative_to(mount_root):
        raise HTTPException(status_code=403, detail="Path outside mount boundary")

    if not target.exists():
        raise HTTPException(status_code=404, detail="Directory not found")

    if not target.is_dir():
        raise HTTPException(status_code=400, detail="Path is not a directory")

    entries = await anyio.to_thread.run_sync(lambda: _list_entries(target, mount_root))

    # Build remote path from mount remote + relative path within mount
    rel = target.relative_to(mount_root)
    if str(rel) == ".":
        remote_path = mount_config.remote
    else:
        remote_path = mount_config.remote.rstrip("/") + "/" + str(rel)

    return BrowseResponse(
        entries=entries,
        remote_prefix=remote_path,
        mount_name=mount,
    )


@router.get("/read")
async def read_file(mount: str, path: str) -> dict[str, str]:
    """Read file contents from a mount's local root.

    Security: resolved path must stay within the mount boundary.
    Only reads text files up to 1 MB.
    """
    if not path:
        raise HTTPException(status_code=400, detail="path is required")

    profile = await anyio.to_thread.run_sync(_get_current_profile)
    if profile is None:
        raise HTTPException(status_code=404, detail="No SSH profile configured")

    mount_config = _find_mount(profile, mount)

    mount_root = Path(mount_config.local)
    target = (mount_root / path).resolve()

    if not target.is_relative_to(mount_root):
        raise HTTPException(status_code=403, detail="Path outside mount boundary")
    if not target.exists():
        raise HTTPException(status_code=404, detail="File not found")
    if not target.is_file():
        raise HTTPException(status_code=400, detail="Path is not a file")

    max_size = 1 * 1024 * 1024  # 1 MB
    stat = target.stat()
    if stat.st_size > max_size:
        raise HTTPException(status_code=413, detail="File too large (max 1 MB)")

    def _read() -> str:
        return target.read_text(errors="replace")

    content = await anyio.to_thread.run_sync(_read)
    return {"content": content, "path": path, "mount": mount}


@router.post("/sync")
async def sync_mount(body: SyncRequest) -> dict[str, str]:
    """Sync a mount's local directory to the remote via rsync.

    This is a best-effort operation: if rsync is not installed or the
    SSH connection is misconfigured, the endpoint returns an appropriate
    error rather than silently failing.
    """
    profile = await anyio.to_thread.run_sync(_get_current_profile)
    if profile is None:
        raise HTTPException(status_code=503, detail="No SSH profile configured")

    try:
        from ..sync_utils import sync_mount_by_name

        await anyio.to_thread.run_sync(lambda: sync_mount_by_name(profile, body.mount))
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc

    return {"status": "synced", "mount": body.mount}
