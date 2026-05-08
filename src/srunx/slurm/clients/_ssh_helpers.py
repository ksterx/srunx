"""Shared helpers for the SSH SLURM client + its extracted modules.

Constants (``_SAFE_IDENTIFIER`` / ``_UNAVAILABLE_STATES``), the input
validator (``_validate_identifier``), the I/O wrapper
(``_run_slurm_cmd``), and the duck-typed mount shim (``_MountsOnly``)
all live here so :mod:`._ssh_resources`, :mod:`._ssh_queries`, and the
main :class:`SlurmSSHClient` import a single shared module rather than
duplicating private helpers.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from srunx.slurm.clients.ssh import SlurmSSHClient
    from srunx.ssh.core.config import MountConfig

# Strict pattern for SLURM identifiers (user, partition) to prevent injection.
_SAFE_IDENTIFIER = re.compile(r"^[a-zA-Z0-9_.\-]+$")

# Node states that should be excluded from available counts.
_UNAVAILABLE_STATES = {"down", "drain", "maint", "reserved"}


def _validate_identifier(value: str, name: str) -> None:
    """Validate a SLURM identifier to prevent shell injection."""
    if not _SAFE_IDENTIFIER.match(value):
        raise ValueError(f"Invalid {name}: {value!r}")


def _run_slurm_cmd(client: SlurmSSHClient, cmd: str) -> str:
    """Execute a SLURM command on the remote host.

    Ensures the SSH connection is alive, then drives the command through
    ``client._client.slurm.execute_slurm_command`` (which handles SLURM
    path resolution, environment setup, and login-shell wrapping).

    Raises :class:`RuntimeError` if the command fails.

    Runs under the client's ``_io_lock`` so concurrent workflow / sweep
    threads cannot interleave SSH I/O on the shared paramiko session.
    """
    with client._io_lock:  # noqa: SLF001
        client._ensure_connected()
        stdout, stderr, exit_code = client._client.slurm.execute_slurm_command(cmd)  # noqa: SLF001
    if exit_code != 0:
        raise RuntimeError(f"Remote command failed ({exit_code}): {stderr.strip()}")
    return stdout


@dataclass(frozen=True)
class _MountsOnly:
    """Duck-typed shim that exposes just ``.mounts`` to the planner.

    ``submission_plan.resolve_mount_for_path`` is duck-typed on the
    profile's ``.mounts`` attribute, but the SSH client only carries the
    mounts tuple — not a full :class:`ServerProfile`. Wrapping in this
    one-field dataclass avoids reaching back into ``ConfigManager``
    just to satisfy the planner's type signature, and keeps the in-place
    decision local to ``run()``.
    """

    mounts: tuple[MountConfig, ...]
