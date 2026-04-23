"""Backward-compat shim. Canonical home: :mod:`srunx.runtime.submission_plan`.

The submission-plan helpers are interface-agnostic (used by CLI, web
routers, and the SSH adapter), so they belong in ``runtime/`` rather
than under ``cli/``. Phase 8f of #164 moves them; this shim keeps the
legacy import path working.
"""

from __future__ import annotations

import sys as _sys

from srunx.runtime import submission_plan as _canonical
from srunx.runtime.submission_plan import *  # noqa: F401, F403

_sys.modules[__name__] = _canonical
