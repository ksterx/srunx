"""Backward-compat shim. Canonical home: :mod:`srunx.observability.monitoring`.

External code should migrate to ``srunx.observability.monitoring``.
Submodules are aliased via ``sys.modules`` so legacy ``srunx.observability.monitoring.X``
imports and monkey-patches continue to work during the Phase 8 transition
(#164).

Note: ``srunx.observability.monitoring.types`` has been consolidated into
``srunx.observability.monitoring.types`` but remains as a thin shim
(see ``report_types.py``) so legacy imports keep resolving.
"""

from __future__ import annotations

import sys as _sys

from srunx.observability.monitoring import (
    BaseMonitor,
    JobMonitor,
    MonitorConfig,
    ResourceMonitor,
    ResourceSnapshot,
    WatchMode,
)
from srunx.observability.monitoring import (  # noqa: F401
    base as _base,
)
from srunx.observability.monitoring import (
    job_monitor as _job_monitor,
)
from srunx.observability.monitoring import (
    resource_monitor as _resource_monitor,
)
from srunx.observability.monitoring import (
    resource_source as _resource_source,
)
from srunx.observability.monitoring import (
    scheduler as _scheduler,
)
from srunx.observability.monitoring import (
    types as _types,
)

_sys.modules[f"{__name__}.base"] = _base
_sys.modules[f"{__name__}.job_monitor"] = _job_monitor
_sys.modules[f"{__name__}.resource_monitor"] = _resource_monitor
_sys.modules[f"{__name__}.resource_source"] = _resource_source
_sys.modules[f"{__name__}.scheduler"] = _scheduler
_sys.modules[f"{__name__}.types"] = _types

base = _base
job_monitor = _job_monitor
resource_monitor = _resource_monitor
resource_source = _resource_source
scheduler = _scheduler
types = _types

__all__ = [
    "BaseMonitor",
    "JobMonitor",
    "MonitorConfig",
    "ResourceMonitor",
    "ResourceSnapshot",
    "WatchMode",
]
