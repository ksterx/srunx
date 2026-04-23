"""Backward-compat shim. Canonical home: :mod:`srunx.slurm.protocols`.

Old names kept as aliases so consumers importing from
``srunx.slurm.protocols`` keep working during the migration (#156).
"""

from srunx.slurm.protocols import (
    Client,
    JobOperations,
    JobSnapshot,
    LogChunk,
    WorkflowJobExecutor,
    WorkflowJobExecutorFactory,
    parse_slurm_datetime,
    parse_slurm_duration,
)

# Backward-compat aliases (old names)
Client = Client
JobOperations = JobOperations
WorkflowJobExecutor = WorkflowJobExecutor
JobSnapshot = JobSnapshot

__all__ = [
    "Client",
    "JobOperations",
    "JobOperations",
    "JobSnapshot",
    "JobSnapshot",
    "LogChunk",
    "Client",
    "WorkflowJobExecutor",
    "WorkflowJobExecutorFactory",
    "WorkflowJobExecutor",
    "parse_slurm_datetime",
    "parse_slurm_duration",
]
