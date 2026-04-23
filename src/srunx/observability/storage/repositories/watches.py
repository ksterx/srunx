"""Repository for the ``watches`` table.

See design.md § ``WatchRepository``. A watch represents an ongoing
interest in a SLURM object (job / workflow run / resource threshold /
scheduled report). Closing a watch is done with ``close(id)`` — watches
are **never deleted** as a side effect; downstream ``subscriptions`` are
CASCADE-deleted only when the watch row itself is explicitly DELETEd.
"""

from __future__ import annotations

from typing import Any

from srunx.observability.storage.models import Watch
from srunx.observability.storage.repositories.base import BaseRepository, now_iso


class WatchRepository(BaseRepository):
    """CRUD for the ``watches`` table."""

    JSON_FIELDS = ("filter",)
    DATETIME_FIELDS = ("created_at", "closed_at")

    _COLUMNS = (
        "id",
        "kind",
        "target_ref",
        "filter",
        "created_at",
        "closed_at",
    )

    def create(
        self,
        kind: str,
        target_ref: str,
        filter: dict | None = None,
    ) -> int:
        """Insert a new open watch row and return its ``id``."""
        cur = self.conn.execute(
            """
            INSERT INTO watches (kind, target_ref, filter, created_at)
            VALUES (?, ?, ?, ?)
            """,
            (kind, target_ref, self._encode_json(filter), now_iso()),
        )
        return int(cur.lastrowid or 0)

    def get(self, id: int) -> Watch | None:
        row = self.conn.execute(
            f"SELECT {', '.join(self._COLUMNS)} FROM watches WHERE id = ?",
            (id,),
        ).fetchone()
        return self._row_to_model(row, Watch)

    def list_open(self) -> list[Watch]:
        """Return all open watches ordered by ``created_at`` ascending."""
        rows = self.conn.execute(
            f"SELECT {', '.join(self._COLUMNS)} FROM watches "
            "WHERE closed_at IS NULL "
            "ORDER BY created_at ASC"
        ).fetchall()
        return [self._row_to_model(r, Watch) for r in rows if r is not None]  # type: ignore[misc]

    def list_by_target(
        self,
        kind: str,
        target_ref: str,
        only_open: bool = True,
    ) -> list[Watch]:
        """Return watches for a given ``(kind, target_ref)`` pair."""
        if only_open:
            sql = (
                f"SELECT {', '.join(self._COLUMNS)} FROM watches "
                "WHERE kind = ? AND target_ref = ? AND closed_at IS NULL "
                "ORDER BY created_at ASC"
            )
        else:
            sql = (
                f"SELECT {', '.join(self._COLUMNS)} FROM watches "
                "WHERE kind = ? AND target_ref = ? "
                "ORDER BY created_at ASC"
            )
        params: list[Any] = [kind, target_ref]

        rows = self.conn.execute(sql, params).fetchall()
        return [self._row_to_model(r, Watch) for r in rows if r is not None]  # type: ignore[misc]

    def close(self, id: int) -> bool:
        """Mark a watch closed (sets ``closed_at = now_iso()``)."""
        cur = self.conn.execute(
            "UPDATE watches SET closed_at = ? WHERE id = ?",
            (now_iso(), id),
        )
        return cur.rowcount > 0
