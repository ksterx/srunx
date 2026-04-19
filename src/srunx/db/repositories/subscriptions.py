"""Repository for the ``subscriptions`` table.

See design.md § ``SubscriptionRepository``. A subscription links a
``watch`` to an ``endpoint`` with a delivery ``preset`` (``terminal`` /
``running_and_terminal`` / ``all`` / ``digest``). Both foreign keys are
ON DELETE CASCADE, so removing the owning endpoint or watch removes all
its subscriptions automatically.
"""

from __future__ import annotations

from srunx.db.models import Subscription
from srunx.db.repositories.base import BaseRepository, now_iso


class SubscriptionRepository(BaseRepository):
    """CRUD for the ``subscriptions`` table."""

    DATETIME_FIELDS = ("created_at",)

    _COLUMNS = (
        "id",
        "watch_id",
        "endpoint_id",
        "preset",
        "created_at",
    )

    def create(self, watch_id: int, endpoint_id: int, preset: str) -> int:
        """Insert a new subscription row and return its ``id``."""
        cur = self.conn.execute(
            """
            INSERT INTO subscriptions (watch_id, endpoint_id, preset, created_at)
            VALUES (?, ?, ?, ?)
            """,
            (watch_id, endpoint_id, preset, now_iso()),
        )
        return int(cur.lastrowid or 0)

    def get(self, id: int) -> Subscription | None:
        row = self.conn.execute(
            f"SELECT {', '.join(self._COLUMNS)} FROM subscriptions WHERE id = ?",
            (id,),
        ).fetchone()
        return self._row_to_model(row, Subscription)

    def list_by_watch(self, watch_id: int) -> list[Subscription]:
        rows = self.conn.execute(
            f"SELECT {', '.join(self._COLUMNS)} FROM subscriptions "
            "WHERE watch_id = ? "
            "ORDER BY created_at ASC",
            (watch_id,),
        ).fetchall()
        return [self._row_to_model(r, Subscription) for r in rows if r is not None]  # type: ignore[misc]

    def list_by_endpoint(self, endpoint_id: int) -> list[Subscription]:
        rows = self.conn.execute(
            f"SELECT {', '.join(self._COLUMNS)} FROM subscriptions "
            "WHERE endpoint_id = ? "
            "ORDER BY created_at ASC",
            (endpoint_id,),
        ).fetchall()
        return [self._row_to_model(r, Subscription) for r in rows if r is not None]  # type: ignore[misc]

    def delete(self, id: int) -> bool:
        cur = self.conn.execute("DELETE FROM subscriptions WHERE id = ?", (id,))
        return cur.rowcount > 0
