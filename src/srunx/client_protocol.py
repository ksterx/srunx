"""Backward-compat shim. Canonical home: :mod:`srunx.slurm.protocols`.

Old names kept as aliases so consumers importing from
``srunx.client_protocol`` keep working during the migration (#156).
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
SlurmClientProtocol = Client
JobOperationsProtocol = JobOperations
WorkflowJobExecutorProtocol = WorkflowJobExecutor
JobStatusInfo = JobSnapshot

__all__ = [
    "Client",
    "JobOperations",
    "JobOperationsProtocol",
    "JobSnapshot",
    "JobStatusInfo",
    "LogChunk",
    "SlurmClientProtocol",
    "WorkflowJobExecutor",
    "WorkflowJobExecutorFactory",
    "WorkflowJobExecutorProtocol",
    "parse_slurm_datetime",
    "parse_slurm_duration",
]
