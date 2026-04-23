"""Backward-compat shim. Scheduled-reporting models were consolidated into
:mod:`srunx.observability.monitoring.types` (#164 Phase 8c); this module
re-exports them so legacy ``from srunx.observability.monitoring.types import ...``
imports keep working.
"""

from __future__ import annotations

from srunx.observability.monitoring.types import (  # noqa: F401
    JobStats,
    Report,
    ReportConfig,
    ResourceStats,
    RunningJob,
)

__all__ = [
    "JobStats",
    "Report",
    "ReportConfig",
    "ResourceStats",
    "RunningJob",
]
