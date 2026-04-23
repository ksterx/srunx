"""Durable storage layer for srunx (SQLite).

See ``.claude/specs/notification-and-state-persistence/design.md`` for
the full schema and architecture.
"""

from __future__ import annotations

import contextlib
import sqlite3
from collections.abc import Iterator
from pathlib import Path

from srunx.observability.storage.connection import init_db, open_connection
from srunx.observability.storage.repositories.jobs import JobRepository


@contextlib.contextmanager
def get_job_repo(
    db_path: Path | None = None,
) -> Iterator[tuple[JobRepository, sqlite3.Connection]]:
    """Yield a ``(JobRepository, connection)`` pair bound to a fresh conn.

    Convenience helper for callers that want a short-lived repository
    without pulling in FastAPI-style DI. The DB is auto-migrated on first
    use via ``init_db``.

    Example::

        with get_job_repo() as (repo, conn):
            repo.record_submission(job_id=12345, name="t", status="PENDING",
                                    submission_source="cli")
    """
    init_db(db_path, delete_legacy=False)
    conn = open_connection(db_path)
    try:
        yield JobRepository(conn), conn
    finally:
        conn.close()


__all__ = ["JobRepository", "get_job_repo", "init_db", "open_connection"]
