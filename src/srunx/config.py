"""Backward-compat shim. Canonical home: :mod:`srunx.common.config`.

External code should migrate to ``srunx.common.config``. This shim makes
``srunx.common.config`` refer to the SAME module object as
``srunx.common.config`` via a ``sys.modules`` alias — so attribute
access, monkey-patching of cached state (e.g. ``srunx.common.config._config =
None``), and ``is`` identity checks all route to the canonical module.

The star-import below is *semantically* redundant (the sys.modules
alias on the following line replaces this module's namespace entirely)
but exists to keep static type-checkers happy: mypy reads the code
statically and needs to see that ``srunx.common.config.SrunxConfig`` resolves.
"""

from __future__ import annotations

import sys as _sys

from srunx.common import config as _canonical
from srunx.common.config import *  # noqa: F401, F403

_sys.modules[__name__] = _canonical
