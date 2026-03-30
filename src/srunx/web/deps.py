"""FastAPI dependency injection providers."""

from __future__ import annotations

from srunx.history import JobHistory, get_history

from .ssh_adapter import SlurmSSHAdapter

# Singleton SSH adapter — connected at startup via lifespan
_adapter: SlurmSSHAdapter | None = None


def set_adapter(adapter: SlurmSSHAdapter) -> None:
    global _adapter
    _adapter = adapter


def get_adapter() -> SlurmSSHAdapter:
    if _adapter is None:
        from fastapi import HTTPException

        raise HTTPException(
            status_code=503,
            detail="SLURM connection not configured. Set SRUNX_SSH_PROFILE or SRUNX_SSH_HOSTNAME + SRUNX_SSH_USERNAME.",
        )
    return _adapter


def get_history_db() -> JobHistory:
    return get_history()
