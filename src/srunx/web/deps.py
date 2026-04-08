"""FastAPI dependency injection providers."""

from __future__ import annotations

import threading

from srunx.history import JobHistory, get_history

from .ssh_adapter import SlurmSSHAdapter

# Thread-safe singleton SSH adapter — connected at startup via lifespan
_adapter: SlurmSSHAdapter | None = None
_adapter_lock = threading.Lock()
_active_profile_name: str | None = None


def set_adapter(adapter: SlurmSSHAdapter, profile_name: str | None = None) -> None:
    global _adapter, _active_profile_name
    with _adapter_lock:
        _adapter = adapter
        _active_profile_name = profile_name


def swap_adapter(
    new_adapter: SlurmSSHAdapter, profile_name: str | None = None
) -> SlurmSSHAdapter | None:
    """Atomically replace the current adapter. Returns the old adapter (caller must disconnect)."""
    global _adapter, _active_profile_name
    with _adapter_lock:
        old = _adapter
        _adapter = new_adapter
        _active_profile_name = profile_name
    return old


def get_adapter() -> SlurmSSHAdapter:
    with _adapter_lock:
        adapter = _adapter
    if adapter is None:
        from fastapi import HTTPException

        raise HTTPException(
            status_code=503,
            detail="SLURM connection not configured. Set SRUNX_SSH_PROFILE or SRUNX_SSH_HOSTNAME + SRUNX_SSH_USERNAME.",
        )
    return adapter


def get_adapter_or_none() -> SlurmSSHAdapter | None:
    """Return the current adapter without raising."""
    with _adapter_lock:
        return _adapter


def get_active_profile_name() -> str | None:
    with _adapter_lock:
        return _active_profile_name


def get_history_db() -> JobHistory:
    return get_history()
