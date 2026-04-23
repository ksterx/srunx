"""Backward-compat shim. Canonical home: :mod:`srunx.common.logging`.

Aliases the canonical module via ``sys.modules`` so the shim and the
canonical path point to the same module object. The star import below
exists only so that static type-checkers see names like
``srunx.logging.get_logger`` resolve statically.
"""

from __future__ import annotations

import sys as _sys

from srunx.common import logging as _canonical
from srunx.common.logging import *  # noqa: F401, F403

_sys.modules[__name__] = _canonical
