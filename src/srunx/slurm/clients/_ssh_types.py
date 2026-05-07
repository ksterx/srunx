"""Connection spec, monitor timeout helper, and SSH-specific exceptions.

These are split out from :mod:`srunx.slurm.clients.ssh` so the main
class file doesn't carry standalone types; importers that only need
:class:`SlurmSSHClientSpec` (e.g. the executor pool) can pull it
without dragging the full client module's import chain.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from srunx.common.logging import get_logger
from srunx.ssh.core.config import MountConfig

logger = get_logger(__name__)


class SSHMonitorTimeoutError(RuntimeError):
    """Raised when ``_monitor_until_terminal`` exceeds its timeout.

    Subclass of ``RuntimeError`` so the sweep orchestrator's existing
    broad-except cell-failure handler still catches it — the typed
    subclass just lets targeted callers (e.g. tests, future UI status
    reporting) distinguish timeout from a genuine SLURM-state-derived
    failure without widening the exception surface.
    """


def _resolve_monitor_timeout_default() -> float | None:
    """Return the default per-job monitor timeout from the environment.

    ``SRUNX_SSH_MONITOR_TIMEOUT`` accepts a non-negative float (seconds).
    An unset / empty / ``"0"`` / non-numeric value means "no timeout",
    preserving the pre-Phase-3 behaviour for users who haven't opted in.
    """
    import os

    raw = os.getenv("SRUNX_SSH_MONITOR_TIMEOUT")
    if not raw:
        return None
    try:
        value = float(raw)
    except ValueError:
        logger.warning(
            f"Ignoring invalid SRUNX_SSH_MONITOR_TIMEOUT={raw!r} "
            "(expected non-negative seconds)"
        )
        return None
    if value <= 0:
        return None
    return value


@dataclass(frozen=True)
class SlurmSSHClientSpec:
    """Connection spec used to clone a :class:`SlurmSSHClient` for pooling.

    Intentionally captures only the configuration needed to re-create a
    client with an equivalent SSH session — no paramiko clients, SFTP
    channels, or in-flight state. Used by the sweep pool factory to mint
    per-cell client clones off a shared singleton template.

    ``mounts`` is a tuple of frozen :class:`MountConfig` instances so the
    spec is deeply immutable and hashable end-to-end.
    """

    profile_name: str | None
    hostname: str
    username: str
    key_filename: str | None
    port: int
    proxy_jump: str | None = None
    env_vars: tuple[tuple[str, str], ...] = ()
    mounts: tuple[MountConfig, ...] = field(default_factory=tuple)
