"""File browsing endpoints: /api/files/*"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Literal

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
    """Get the current SSH profile from web config or ConfigManager.

    Returns the ServerProfile, or None if no profile is configured.
    """
    from srunx.ssh.core.config import ConfigManager

    config = get_web_config()
    cm = ConfigManager()

    profile_name = config.ssh_profile
    if not profile_name:
        profile_name = cm.get_current_profile_name()

    if not profile_name:
        return None

    return cm.get_profile(profile_name)


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


@router.get("/mounts")
async def list_mounts() -> list[MountInfo]:
    """Return mount list from the current SSH profile.

    Only returns name and remote prefix (never local paths).
    """
    profile = await anyio.to_thread.run_sync(_get_current_profile)
    if profile is None or not profile.mounts:
        return []
    return [MountInfo(name=m.name, remote=m.remote) for m in profile.mounts]


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
