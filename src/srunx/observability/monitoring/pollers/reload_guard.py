"""Pure helpers that decide whether background pollers should start.

These helpers exist as stand-alone functions so their full input matrix
(``UVICORN_RELOAD`` env var, ``--reload`` argv, ``SRUNX_DISABLE_POLLER``
env var) can be unit-tested without touching the process environment.
"""

from __future__ import annotations

import os
import sys
from collections.abc import Mapping


def is_reload_mode(
    env: Mapping[str, str] | None = None,
    argv: list[str] | None = None,
) -> bool:
    """Return True if the current process was started in uvicorn reload mode.

    Reload mode is detected by either a truthy ``UVICORN_RELOAD`` environment
    variable or the presence of ``--reload`` in ``argv``. Both signals are
    treated as "reload" because uvicorn sets the env var in the child
    process and ``--reload`` is the user-facing CLI flag.

    An empty string value for ``UVICORN_RELOAD`` is treated as unset /
    falsy (matching typical shell semantics for "not configured").

    Args:
        env: Environment mapping to inspect. Defaults to ``os.environ``.
        argv: Argument vector to inspect. Defaults to ``sys.argv``.

    Returns:
        True when the process is running under ``--reload``.
    """
    resolved_env: Mapping[str, str] = os.environ if env is None else env
    resolved_argv: list[str] = sys.argv if argv is None else argv

    reload_env = resolved_env.get("UVICORN_RELOAD")
    if reload_env:
        return True
    return "--reload" in resolved_argv


def should_start_pollers(
    env: Mapping[str, str] | None = None,
    argv: list[str] | None = None,
) -> bool:
    """Return True when background pollers should be started.

    Pollers are skipped when running under uvicorn ``--reload`` (to
    prevent duplicate delivery) or when the operator explicitly opted
    out via ``SRUNX_DISABLE_POLLER=1``.

    Args:
        env: Environment mapping to inspect. Defaults to ``os.environ``.
        argv: Argument vector to inspect. Defaults to ``sys.argv``.

    Returns:
        True when the supervisor should schedule poller tasks.
    """
    resolved_env: Mapping[str, str] = os.environ if env is None else env

    if is_reload_mode(env=resolved_env, argv=argv):
        return False
    if resolved_env.get("SRUNX_DISABLE_POLLER") == "1":
        return False
    return True
