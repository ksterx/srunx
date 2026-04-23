"""Backward-compat shim. Canonical home is :mod:`srunx.runtime.templates`.

This module is preserved so external code importing from ``srunx.template``
keeps working during the structural migration (#156). Internal code should
prefer ``srunx.runtime.templates`` going forward.
"""

from srunx.runtime.templates import *  # noqa: F401,F403
from srunx.runtime.templates import (  # noqa: F401
    _VALID_NAME,
    BUILTIN_TEMPLATES,
    TEMPLATES,
    _load_user_meta,
    _save_user_meta,
    _user_meta_path,
    _user_template_file,
    _user_templates_dir,
)
