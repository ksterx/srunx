"""Backward-compat shim. Canonical home: :mod:`srunx.runtime.sweep`.

External code should migrate to ``srunx.runtime.sweep``. Submodules are
aliased via ``sys.modules`` so legacy ``from srunx.sweep.X import Y``
call-sites and monkey-patches keep working during the Phase 8f
transition.
"""

from __future__ import annotations

import sys as _sys

from srunx.runtime.sweep import *  # noqa: F401, F403
from srunx.runtime.sweep import (  # noqa: F401
    aggregator as _aggregator,
)
from srunx.runtime.sweep import (
    expand as _expand,
)
from srunx.runtime.sweep import (
    orchestrator as _orchestrator,
)
from srunx.runtime.sweep import (
    reconciler as _reconciler,
)
from srunx.runtime.sweep import (
    state_service as _state_service,
)

_sys.modules[f"{__name__}.aggregator"] = _aggregator
_sys.modules[f"{__name__}.expand"] = _expand
_sys.modules[f"{__name__}.orchestrator"] = _orchestrator
_sys.modules[f"{__name__}.reconciler"] = _reconciler
_sys.modules[f"{__name__}.state_service"] = _state_service

aggregator = _aggregator
expand = _expand
orchestrator = _orchestrator
reconciler = _reconciler
state_service = _state_service
