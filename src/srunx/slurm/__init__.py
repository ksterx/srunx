"""SLURM execution transport layer.

Canonical entry points:

- :class:`Client` — Protocol every submission backend implements
- :class:`LocalClient` — in-process ``sbatch`` / ``squeue`` / ``scancel``
- :class:`SSHClient` — SLURM over SSH (populated in Phase 6 / #162)

Also exposes :data:`SLURM_TERMINAL_JOB_STATES` and the
``JobOperations`` / ``WorkflowJobExecutor`` protocols.
"""

from __future__ import annotations

from typing import Any

# Import leaf modules eagerly — they have no back-edge into
# ``srunx.slurm.local``. The local client, by contrast, transitively
# imports ``srunx.utils`` which now re-exports ``GPU_TRES_RE`` from
# ``srunx.slurm.parsing``. That re-export triggers ``srunx.slurm``'s
# own ``__init__`` during ``local.py``'s own import, so eagerly
# pulling ``LocalClient`` here would deadlock on the half-initialised
# ``srunx.slurm.local`` module. Use PEP 562 ``__getattr__`` to defer
# the binding until after both modules finish loading.
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
