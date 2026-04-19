"""SQLite connection management for the srunx state DB.

Design reference: ``.claude/specs/notification-and-state-persistence/design.md``.

Key invariants enforced here:

- DB path resolution honours ``$XDG_CONFIG_HOME`` with a ``~/.config`` fallback.
- Every opened connection applies ``foreign_keys=ON``, ``journal_mode=WAL``,
  and ``busy_timeout=5000``.
- The DB file is created with mode ``0o600`` and its parent directory with
  mode ``0o700``.
- :func:`init_db` deletes the legacy ``~/.srunx/history.db`` (or renames it
  to ``.broken`` on OSError) — no backward-compat shim.
"""

from __future__ import annotations

import contextlib
import os
import sqlite3
import stat
from collections.abc import Iterator
from pathlib import Path

from srunx.logging import get_logger

logger = get_logger(__name__)


LEGACY_HISTORY_DB_PATH = Path.home() / ".srunx" / "history.db"


def get_config_dir() -> Path:
    """Return the XDG-compliant srunx config directory.

    Resolves ``$XDG_CONFIG_HOME/srunx`` when the env var is set, otherwise
    ``~/.config/srunx`` on POSIX, ``~/AppData/Roaming/srunx`` on Windows.
    """
    xdg = os.environ.get("XDG_CONFIG_HOME")
    if xdg:
        return Path(xdg).expanduser() / "srunx"
    if os.name == "posix":
        return Path.home() / ".config" / "srunx"
    return Path.home() / "AppData" / "Roaming" / "srunx"


def get_db_path() -> Path:
    """Return the DB path: ``<config_dir>/srunx.db``."""
    return get_config_dir() / "srunx.db"


def _ensure_parent_dir(path: Path) -> None:
    parent = path.parent
    parent.mkdir(parents=True, exist_ok=True)
    # Tighten perms if possible (POSIX only; best-effort on Windows).
    try:
        parent.chmod(stat.S_IRWXU)  # 0o700
    except OSError:
        pass


def _apply_pragmas(conn: sqlite3.Connection) -> None:
    # busy_timeout must be set BEFORE any other write-side PRAGMA.
    conn.execute("PRAGMA busy_timeout = 5000")
    conn.execute("PRAGMA foreign_keys = ON")
    # journal_mode=WAL on the very first opener rewrites the DB header,
    # which needs exclusive access. Concurrent openers on a cold DB
    # can both attempt it and one receives "database is locked"
    # *without* busy_timeout kicking in for this specific PRAGMA.
    # Retry with a short backoff so losers just observe WAL already
    # set after the winner commits.
    import time as _time

    for attempt in range(10):
        try:
            conn.execute("PRAGMA journal_mode = WAL")
            break
        except sqlite3.OperationalError as exc:
            if "database is locked" not in str(exc) or attempt == 9:
                raise
            _time.sleep(0.05 * (attempt + 1))


def open_connection(db_path: Path | None = None) -> sqlite3.Connection:
    """Open a sqlite3 connection with srunx's required PRAGMAs applied.

    On first creation, the DB file is chmod'd to ``0o600``. Caller owns the
    returned connection and must close it (or use a ``with`` block).

    ``check_same_thread=False`` is set so the same connection can be
    passed across the FastAPI request-handler thread ↔ anyio worker
    thread boundary within a single request (``get_db_conn`` yields per
    request; writes run inside ``anyio.to_thread.run_sync`` blocks).
    srunx's access pattern serialises calls on any given connection —
    one request owns it at a time — so the relaxed thread check is
    safe. sqlite itself still serialises writers via the WAL lock.
    """
    path = db_path or get_db_path()
    created = not path.exists()
    _ensure_parent_dir(path)
    # isolation_level=None puts the driver in autocommit mode, leaving
    # transaction boundaries under explicit control of the caller. This
    # is required for the outbox claim pattern (BEGIN IMMEDIATE ... COMMIT)
    # because the default "deferred" mode auto-starts a transaction on
    # the first DML and then refuses BEGIN.
    conn = sqlite3.connect(path, isolation_level=None, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    _apply_pragmas(conn)
    if created:
        try:
            path.chmod(stat.S_IRUSR | stat.S_IWUSR)  # 0o600
        except OSError:
            logger.warning("Could not set 0600 perms on %s", path)
    return conn


@contextlib.contextmanager
def transaction(
    conn: sqlite3.Connection, mode: str = "DEFERRED"
) -> Iterator[sqlite3.Connection]:
    """Wrap ``BEGIN ... COMMIT/ROLLBACK`` around a block.

    ``mode`` is ``'DEFERRED'`` (default) or ``'IMMEDIATE'``. Use
    ``'IMMEDIATE'`` for any write transaction that needs to block
    concurrent writers up-front (e.g. the delivery claim loop).
    """
    mode_upper = mode.upper()
    if mode_upper not in {"DEFERRED", "IMMEDIATE", "EXCLUSIVE"}:
        raise ValueError(f"Unsupported transaction mode: {mode}")
    conn.execute(f"BEGIN {mode_upper}")
    try:
        yield conn
    except BaseException:
        conn.rollback()
        raise
    else:
        conn.commit()


def _delete_legacy_history_db() -> None:
    """Remove the legacy ``~/.srunx/history.db`` if present.

    On OSError (permission / in-use), rename to ``.broken`` and log a
    warning rather than block startup.
    """
    if not LEGACY_HISTORY_DB_PATH.exists():
        return
    try:
        LEGACY_HISTORY_DB_PATH.unlink()
        logger.info("Removed legacy history DB at %s", LEGACY_HISTORY_DB_PATH)
    except OSError:
        broken = LEGACY_HISTORY_DB_PATH.with_suffix(".db.broken")
        try:
            LEGACY_HISTORY_DB_PATH.rename(broken)
            logger.warning("Could not remove legacy history DB; renamed to %s", broken)
        except OSError as exc:
            logger.warning(
                "Could not remove or rename legacy history DB %s: %s",
                LEGACY_HISTORY_DB_PATH,
                exc,
            )


def init_db(db_path: Path | None = None, *, delete_legacy: bool = True) -> Path:
    """Initialize the srunx SQLite DB.

    Steps:

    1. Resolve path and ensure parent directory exists (mode ``0o700``).
    2. Create the DB file if missing, set permissions to ``0o600``.
    3. Apply PRAGMAs and run outstanding migrations.
    4. If ``delete_legacy`` is True (default), remove the legacy
       ``~/.srunx/history.db``. Passing ``False`` is only used by tests
       that want the helper to skip the legacy-cleanup path.

    Returns the resolved DB path for downstream callers.
    """
    from srunx.db.migrations import apply_migrations

    path = db_path or get_db_path()
    conn = open_connection(path)
    try:
        apply_migrations(conn)
    finally:
        conn.close()
    if delete_legacy:
        _delete_legacy_history_db()
    return path
