"""Backward-compat shim. Canonical home: :mod:`srunx.observability.notifications`.

External code should migrate to ``srunx.observability.notifications``.
This module exists so that existing ``from srunx.observability.notifications.X import Y``
call-sites keep working during the Phase 8 transition (#164).

Submodules are aliased via ``sys.modules`` so that ``srunx.observability.notifications.X``
and ``srunx.observability.notifications.X`` refer to the **same** module
object — preserving monkey-patching and ``is`` identity checks.
"""

from __future__ import annotations

import sys as _sys

from srunx.observability.notifications import (  # noqa: F401
    adapters as _adapters,
)
from srunx.observability.notifications import (
    presets as _presets,
)
from srunx.observability.notifications import (
    sanitize as _sanitize,
)
from srunx.observability.notifications import (
    service as _service,
)
from srunx.observability.notifications.adapters import (  # noqa: F401
    base as _adapters_base,
)
from srunx.observability.notifications.adapters import (
    registry as _adapters_registry,
)
from srunx.observability.notifications.adapters import (
    slack_webhook as _adapters_slack_webhook,
)

_sys.modules[f"{__name__}.adapters"] = _adapters
_sys.modules[f"{__name__}.adapters.base"] = _adapters_base
_sys.modules[f"{__name__}.adapters.registry"] = _adapters_registry
_sys.modules[f"{__name__}.adapters.slack_webhook"] = _adapters_slack_webhook
_sys.modules[f"{__name__}.presets"] = _presets
_sys.modules[f"{__name__}.sanitize"] = _sanitize
_sys.modules[f"{__name__}.service"] = _service

adapters = _adapters
presets = _presets
sanitize = _sanitize
service = _service
