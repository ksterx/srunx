"""SLURM execution transport layer.

Canonical entry points:

- :class:`Client` — Protocol every submission backend implements
- :class:`LocalClient` — in-process ``sbatch`` / ``squeue`` / ``scancel``
- :class:`SlurmSSHClient` (in :mod:`srunx.slurm.clients.ssh`) — SLURM over
  SSH (delegates to :mod:`srunx.ssh.core.client`)

Also exposes :data:`SLURM_TERMINAL_JOB_STATES` and the
``JobOperations`` / ``WorkflowJobExecutor`` protocols.
"""

from __future__ import annotations

from typing import Any

# Import leaf modules eagerly; defer ``LocalClient`` via PEP 562
# ``__getattr__`` to avoid a circular import through ``srunx.utils``
# → ``srunx.slurm.local``.
from srunx.slurm.parsing import GPU_TRES_RE, parse_slurm_datetime, parse_slurm_duration
from srunx.slurm.protocols import (
    Client,
    JobOperations,
    JobSnapshot,
    LogChunk,
    WorkflowJobExecutor,
)
from srunx.slurm.states import SLURM_TERMINAL_JOB_STATES

__all__ = [
    "Client",
    "GPU_TRES_RE",
    "JobOperations",
    "JobSnapshot",
    "LocalClient",
    "LogChunk",
    "SLURM_TERMINAL_JOB_STATES",
    "WorkflowJobExecutor",
    "parse_slurm_datetime",
    "parse_slurm_duration",
]


def __getattr__(name: str) -> Any:
    """Lazy-load :class:`LocalClient` to avoid an import cycle.

    See the module docstring above — eagerly importing ``local.py``
    from this ``__init__`` would deadlock on ``srunx.utils`` ↔
    ``srunx.slurm.parsing``'s transitive package init.
    """
    if name == "LocalClient":
        from srunx.slurm.local import LocalClient

        return LocalClient
    raise AttributeError(f"module 'srunx.slurm' has no attribute {name!r}")
