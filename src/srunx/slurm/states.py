"""SLURM protocol-level state constants.

Separate from :class:`srunx.domain.JobStatus` (the domain-level enum)
because SLURM's raw state vocabulary is wider than what srunx models:
SLURM emits ``NODE_FAIL`` / ``PREEMPTED`` / ``OUT_OF_MEMORY`` for
terminal failures that srunx currently collapses into ``FAILED`` at
the domain boundary. Keeping these strings in a single module lets
every caller that speaks SLURM-native states (notification preset
filter, active-watch poller, web SSH adapter) agree on the set
without risking drift when a new terminal state is added.
"""

from __future__ import annotations

SLURM_TERMINAL_JOB_STATES: frozenset[str] = frozenset(
    {
        "COMPLETED",
        "FAILED",
        "CANCELLED",
        "TIMEOUT",
        "NODE_FAIL",
        "PREEMPTED",
        "OUT_OF_MEMORY",
    }
)
