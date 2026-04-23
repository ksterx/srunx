"""Backward-compat shim. Canonical home: :mod:`srunx.runtime.security`.

External code should migrate to ``srunx.runtime.security``. Submodules
are aliased via ``sys.modules`` so legacy imports and monkey-patches
continue to work during the Phase 8f transition.
"""

from __future__ import annotations

import sys as _sys

from srunx.runtime.security import *  # noqa: F401, F403
from srunx.runtime.security import (  # noqa: F401
    mount_paths as _mount_paths,
)
from srunx.runtime.security import (
    python_args as _python_args,
)

_sys.modules[f"{__name__}.mount_paths"] = _mount_paths
_sys.modules[f"{__name__}.python_args"] = _python_args

mount_paths = _mount_paths
python_args = _python_args
