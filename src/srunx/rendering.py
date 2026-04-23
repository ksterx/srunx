"""Backward-compat shim. Canonical home is :mod:`srunx.runtime.rendering`.

This module is preserved so external code importing from ``srunx.runtime.rendering``
keeps working during the structural migration (#156). Internal code should
prefer ``srunx.runtime.rendering`` going forward.
"""

from srunx.runtime.rendering import *  # noqa: F401,F403
from srunx.runtime.rendering import (  # noqa: F401
    _find_mount_by_name,
    _is_absolute,
    _normalize_paths_for_mount,
    _render_one,
    _resolve_log_dir,
    _resolve_work_dir,
    _translate_abs_path,
)
