"""Backward-compat shim. Canonical home: :mod:`srunx.common.exceptions`.

Aliases the canonical module via ``sys.modules`` so attribute access
and ``isinstance`` checks route through one module object. The star
import below exists only so that static type-checkers see names like
``srunx.exceptions.JobNotFound`` resolve statically.
"""

from __future__ import annotations

import sys as _sys

from srunx.common import exceptions as _canonical
from srunx.common.exceptions import *  # noqa: F401, F403

_sys.modules[__name__] = _canonical
