"""Backward-compat shim. Canonical: :mod:`srunx.observability.monitoring.pollers`.

External code should migrate to ``srunx.observability.monitoring.pollers``.
Submodules are aliased via ``sys.modules`` so legacy
``from srunx.observability.monitoring.pollers.X import Y`` imports keep working during the Phase 8
transition (#164).
"""

from __future__ import annotations

import sys as _sys

from srunx.observability.monitoring.pollers import (  # noqa: F401
    active_watch_poller as _active_watch_poller,
)
from srunx.observability.monitoring.pollers import (
    delivery_poller as _delivery_poller,
)
from srunx.observability.monitoring.pollers import (
    reload_guard as _reload_guard,
)
from srunx.observability.monitoring.pollers import (
    resource_snapshotter as _resource_snapshotter,
)
from srunx.observability.monitoring.pollers import (
    supervisor as _supervisor,
)

_sys.modules[f"{__name__}.active_watch_poller"] = _active_watch_poller
_sys.modules[f"{__name__}.delivery_poller"] = _delivery_poller
_sys.modules[f"{__name__}.reload_guard"] = _reload_guard
_sys.modules[f"{__name__}.resource_snapshotter"] = _resource_snapshotter
_sys.modules[f"{__name__}.supervisor"] = _supervisor

active_watch_poller = _active_watch_poller
delivery_poller = _delivery_poller
reload_guard = _reload_guard
resource_snapshotter = _resource_snapshotter
supervisor = _supervisor
