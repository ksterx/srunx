"""Repository for the ``endpoints`` table.

See design.md § ``EndpointRepository``. Endpoints represent external
notification sinks (Slack webhook, generic webhook, email, Slack bot).
The ``config`` column is a JSON blob whose shape depends on ``kind`` —
schema-level validation is performed in the service layer, not here.
"""

from __future__ import annotations

from typing import Any

from srunx.db.models import Endpoint
from srunx.db.repositories.base import BaseRepository, now_iso


class EndpointRepository(BaseRepository):
    """CRUD for the ``endpoints`` table."""

    JSON_FIELDS = ("config",)
    DATETIME_FIELDS = ("created_at", "disabled_at")

    _COLUMNS = (
        "id",
        "kind",
        "name",
        "config",
        "created_at",
        "disabled_at",
    )

    def create(self, kind: str, name: str, config: dict) -> int:
        """Insert a new endpoint row and return its ``id``."""
        cur = self.conn.execute(
            """
            INSERT INTO endpoints (kind, name, config, created_at)
            VALUES (?, ?, ?, ?)
            """,
            (kind, name, self._encode_json(config), now_iso()),
        )
        return int(cur.lastrowid or 0)

    def get(self, id: int) -> Endpoint | None:
        row = self.conn.execute(
            f"SELECT {', '.join(self._COLUMNS)} FROM endpoints WHERE id = ?",
            (id,),
        ).fetchone()
        return self._row_to_model(row, Endpoint)

    def get_by_name(self, kind: str, name: str) -> Endpoint | None:
        row = self.conn.execute(
            f"SELECT {', '.join(self._COLUMNS)} FROM endpoints "
            "WHERE kind = ? AND name = ?",
            (kind, name),
        ).fetchone()
        return self._row_to_model(row, Endpoint)

    def list(self, include_disabled: bool = True) -> list[Endpoint]:
        """Return endpoints ordered by ``created_at`` ascending.

        When ``include_disabled=False``, rows with a non-NULL
        ``disabled_at`` are filtered out.
        """
        if include_disabled:
            sql = (
                f"SELECT {', '.join(self._COLUMNS)} FROM endpoints "
                "ORDER BY created_at ASC"
            )
            params: list[Any] = []
        else:
            sql = (
                f"SELECT {', '.join(self._COLUMNS)} FROM endpoints "
                "WHERE disabled_at IS NULL "
                "ORDER BY created_at ASC"
            )
            params = []

        rows = self.conn.execute(sql, params).fetchall()
        return [self._row_to_model(r, Endpoint) for r in rows if r is not None]  # type: ignore[misc]

    def update(
        self,
        id: int,
        name: str | None = None,
        config: dict | None = None,
    ) -> bool:
        """Partially update an endpoint. Returns True if a row was touched."""
        sets: list[str] = []
        vals: list[Any] = []
        if name is not None:
            sets.append("name = ?")
            vals.append(name)
        if config is not None:
            sets.append("config = ?")
            vals.append(self._encode_json(config))

        if not sets:
            return False

        vals.append(id)
        cur = self.conn.execute(
            f"UPDATE endpoints SET {', '.join(sets)} WHERE id = ?",
            vals,
        )
        return cur.rowcount > 0

    def disable(self, id: int) -> bool:
        """Mark an endpoint disabled (sets ``disabled_at = now_iso()``)."""
        cur = self.conn.execute(
            "UPDATE endpoints SET disabled_at = ? WHERE id = ?",
            (now_iso(), id),
        )
        return cur.rowcount > 0

    def enable(self, id: int) -> bool:
        """Re-enable an endpoint (clears ``disabled_at``)."""
        cur = self.conn.execute(
            "UPDATE endpoints SET disabled_at = NULL WHERE id = ?",
            (id,),
        )
        return cur.rowcount > 0

    def delete(self, id: int) -> bool:
        """Delete an endpoint. Cascades to ``subscriptions`` via FK."""
        cur = self.conn.execute("DELETE FROM endpoints WHERE id = ?", (id,))
        return cur.rowcount > 0
