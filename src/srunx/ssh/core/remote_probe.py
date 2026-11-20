"""Probe a cluster's remote ``$HOME`` / XDG dirs over SSH.

Registered with :mod:`srunx.common.remote_env` on import so the ``common``
layer can request a probe without importing the SSH layer.
"""

from __future__ import annotations

from srunx.common.logging import get_logger
from srunx.common.remote_env import (
    RemoteEnv,
    parse_probe,
    probe_command,
    register_fetcher,
)

from .config import ConfigManager, connection_params
from .connection import SSHConnection

logger = get_logger(__name__)


def probe_remote_env(cluster: str) -> RemoteEnv | None:
    profile = ConfigManager().get_profile(cluster, with_mounts=False)
    if profile is None:
        return None
    conn = SSHConnection(**connection_params(profile))
    if not conn.connect():
        logger.warning(f"Could not connect to cluster '{cluster}' to probe $HOME")
        return None
    try:
        stdout, _stderr, code = conn.execute_command(probe_command())
    finally:
        conn.disconnect()
    return parse_probe(stdout) if code == 0 else None


register_fetcher(probe_remote_env)
