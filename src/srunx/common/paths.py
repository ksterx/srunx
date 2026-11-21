"""Single source of truth for srunx's local file locations.

Layout (see ``docs/design/clusters.md`` §2):

* ``/etc/srunx/config.toml``                — system config
* ``$XDG_CONFIG_HOME/srunx/config.toml``    — user config (clusters, defaults)
* ``$XDG_CONFIG_HOME/srunx/templates/``     — user templates
* ``<state_dir>/srunx.db``                  — history / notifications DB
* ``<state_dir>/locks/``                    — sync locks
* ``<project>/.srunx/config.toml``          — project config
* ``<project>/.srunx/workflows/``           — project workflows

``<state_dir>`` defaults to ``$XDG_STATE_HOME/srunx`` and may be moved via
the user config's top-level ``state_dir`` key (e.g. off an NFS home).

None of these helpers create directories; writers create what they need.
"""

from __future__ import annotations

import os
import tomllib
from pathlib import Path

PROJECT_DIR_NAME = ".srunx"
CONFIG_FILENAME = "config.toml"
DB_FILENAME = "srunx.db"


def _xdg_dir(env_var: str, fallback: Path) -> Path:
    """Return ``$env_var`` when it is set to an absolute path, else *fallback*.

    The XDG Base Directory spec says relative values must be ignored.
    """
    value = os.environ.get(env_var, "")
    if value and os.path.isabs(os.path.expanduser(value)):
        return Path(os.path.expanduser(value))
    return fallback


def config_dir() -> Path:
    """``$XDG_CONFIG_HOME/srunx`` (default ``~/.config/srunx``)."""
    return _xdg_dir("XDG_CONFIG_HOME", Path.home() / ".config") / "srunx"


def default_state_dir() -> Path:
    """``$XDG_STATE_HOME/srunx`` (default ``~/.local/state/srunx``)."""
    return _xdg_dir("XDG_STATE_HOME", Path.home() / ".local" / "state") / "srunx"


def system_config_path() -> Path:
    return Path("/etc/srunx") / CONFIG_FILENAME


def user_config_path() -> Path:
    return config_dir() / CONFIG_FILENAME


def templates_dir() -> Path:
    return config_dir() / "templates"


def state_dir() -> Path:
    """Return the state directory, honouring the user config's ``state_dir``.

    Reads the user config file directly (not through the full settings
    loader) so the DB layer does not depend on config validation.
    An unreadable config falls back to the XDG default; the settings
    loader reports the parse error on its own path.
    """
    path = user_config_path()
    try:
        with open(path, "rb") as f:
            raw = tomllib.load(f).get("state_dir")
    except (OSError, tomllib.TOMLDecodeError):
        raw = None
    if isinstance(raw, str) and raw:
        return Path(os.path.expandvars(os.path.expanduser(raw)))
    return default_state_dir()


def db_path() -> Path:
    return state_dir() / DB_FILENAME


def locks_dir() -> Path:
    return state_dir() / "locks"


def project_config_path(root: Path) -> Path:
    return root / PROJECT_DIR_NAME / CONFIG_FILENAME


def project_workflows_dir(root: Path) -> Path:
    return root / PROJECT_DIR_NAME / "workflows"


def _is_git_root(path: Path) -> bool:
    return (path / ".git").exists()


def find_project_root(start: Path | None = None) -> Path | None:
    """Walk up from *start* (default cwd) to the nearest ``.srunx/config.toml``.

    Search stops at (design §4):

    * a Git repository root — that directory itself is still checked;
    * ``$HOME`` — not checked, so ``~/.srunx`` is never a project;
    * the filesystem root.
    """
    current = (start or Path.cwd()).resolve()
    home = Path.home().resolve()
    while True:
        if current == home:
            return None
        if project_config_path(current).is_file():
            return current
        if _is_git_root(current):
            return None
        parent = current.parent
        if parent == current:
            return None
        current = parent
