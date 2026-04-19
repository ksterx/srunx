"""Repository for the ``deliveries`` table.

See design.md § ``DeliveryRepository``. This is the Outbox: one row per
``(event, subscription)`` pair, with a ``pending → sending →
delivered|abandoned`` state machine. Workers lease rows using
:meth:`DeliveryRepository.claim_one` — because the stock Python
``sqlite3`` build does NOT ship with ``SQLITE_ENABLE_UPDATE_DELETE_LIMIT``,
we implement the "pick one row" step as ``SELECT ... LIMIT 1`` followed
by an ``UPDATE ... WHERE id = ? AND status = 'pending'`` that races
cleanly against concurrent workers.

All timestamp writes use :func:`srunx.db.repositories.base.now_iso` or
``strftime('%Y-%m-%dT%H:%M:%fZ', 'now')`` so that string comparisons
remain lexicographically correct across Python and SQL code paths.
"""

from __future__ import annotations

import sqlite3
from typing import Any

from srunx.db.models import Delivery
from srunx.db.repositories.base import BaseRepository, now_iso


class DeliveryRepository(BaseRepository):
    """CRUD + outbox claim/lease mechanics for the ``deliveries`` table."""

    DATETIME_FIELDS = (
        "next_attempt_at",
        "leased_until",
        "delivered_at",
        "created_at",
    )

    _COLUMNS = (
        "id",
        "event_id",
        "subscription_id",
        "endpoint_id",
        "idempotency_key",
        "status",
        "attempt_count",
        "next_attempt_at",
        "leased_until",
        "worker_id",
        "last_error",
        "delivered_at",
        "created_at",
    )

    # -- backoff -----------------------------------------------------------

    @staticmethod
    def _backoff_secs(
        attempt_count: int, base: int = 10, factor: int = 2, cap: int = 3600
    ) -> int:
        """Return the exponential backoff in seconds for ``attempt_count``.

        ``min(base * factor**attempt_count, cap)``. Uses integer math
        throughout so callers can pass the result straight into SQL.
        Truth table (base=10, factor=2, cap=3600):

        - attempt_count=0 → 10
        - attempt_count=1 → 20
        - attempt_count=2 → 40
        - attempt_count=10 → 3600 (capped)
        """
        return min(base * (factor**attempt_count), cap)

    # -- CRUD --------------------------------------------------------------

    def insert(
        self,
        event_id: int,
        subscription_id: int,
        endpoint_id: int,
        idempotency_key: str,
        *,
        next_attempt_at: str | None = None,
    ) -> int | None:
        """Create a new pending delivery row.

        Uses ``INSERT OR IGNORE`` against the UNIQUE
        ``(endpoint_id, idempotency_key)`` index: a deterministic
        idempotency key means duplicate fan-out attempts for the same
        logical transition are silently collapsed. Returns the new
        row's ``id`` on success, or ``None`` when the UNIQUE
        constraint absorbed the insert.
        """
        created_at = now_iso()
        scheduled = next_attempt_at or created_at
        cur = self.conn.execute(
            """
            INSERT OR IGNORE INTO deliveries (
                event_id, subscription_id, endpoint_id, idempotency_key,
                status, attempt_count, next_attempt_at,
                leased_until, worker_id, last_error, delivered_at,
                created_at
            ) VALUES (?, ?, ?, ?, 'pending', 0, ?, NULL, NULL, NULL, NULL, ?)
            """,
            (
                event_id,
                subscription_id,
                endpoint_id,
                idempotency_key,
                scheduled,
                created_at,
            ),
        )
        if cur.rowcount == 0:
            return None
        last = cur.lastrowid
        return int(last) if last else None

    def get(self, id: int) -> Delivery | None:
        row = self.conn.execute(
            f"SELECT {', '.join(self._COLUMNS)} FROM deliveries WHERE id = ?",
            (id,),
        ).fetchone()
        return self._row_to_model(row, Delivery)

    def list_by_subscription(
        self,
        subscription_id: int,
        status: str | None = None,
    ) -> list[Delivery]:
        """Return deliveries for a subscription, newest first.

        When ``status`` is given, only rows with that ``status`` are
        returned.
        """
        if status is None:
            sql = (
                f"SELECT {', '.join(self._COLUMNS)} FROM deliveries "
                "WHERE subscription_id = ? "
                "ORDER BY created_at DESC"
            )
            params: list[Any] = [subscription_id]
        else:
            sql = (
                f"SELECT {', '.join(self._COLUMNS)} FROM deliveries "
                "WHERE subscription_id = ? AND status = ? "
                "ORDER BY created_at DESC"
            )
            params = [subscription_id, status]

        rows: list[sqlite3.Row] = self.conn.execute(sql, params).fetchall()
        return [self._row_to_model(r, Delivery) for r in rows if r is not None]  # type: ignore[misc]

    def list_recent(
        self,
        *,
        status: str | None = None,
        limit: int = 100,
    ) -> list[Delivery]:
        """Return the most recent deliveries across all subscriptions.

        Powers the NotificationsCenter dashboard — a read-only
        observability view of the outbox. Callers must bound ``limit``
        to avoid pulling the full table during incidents.
        """
        if status is None:
            sql = (
                f"SELECT {', '.join(self._COLUMNS)} FROM deliveries "
                "ORDER BY created_at DESC LIMIT ?"
            )
            params: list[Any] = [limit]
        else:
            sql = (
                f"SELECT {', '.join(self._COLUMNS)} FROM deliveries "
                "WHERE status = ? "
                "ORDER BY created_at DESC LIMIT ?"
            )
            params = [status, limit]

        rows: list[sqlite3.Row] = self.conn.execute(sql, params).fetchall()
        return [self._row_to_model(r, Delivery) for r in rows if r is not None]  # type: ignore[misc]

    # -- lease mechanics ---------------------------------------------------

    def reclaim_expired_leases(self) -> int:
        """Revert stale ``sending`` rows back to ``pending``.

        Called at the top of every :class:`DeliveryPoller` cycle to
        recover rows whose owning worker crashed or was terminated
        mid-send. Returns the number of rows touched.
        """
        cur = self.conn.execute(
            """
            UPDATE deliveries
               SET status = 'pending',
                   leased_until = NULL,
                   worker_id = NULL
             WHERE status = 'sending'
               AND leased_until < strftime('%Y-%m-%dT%H:%M:%fZ','now')
            """
        )
        return int(cur.rowcount)

    def claim_one(
        self, worker_id: str, lease_duration_secs: int = 300
    ) -> Delivery | None:
        """Lease the next due ``pending`` delivery for ``worker_id``.

        Implemented as SELECT-then-UPDATE inside ``BEGIN IMMEDIATE``
        because the Python stdlib sqlite3 build lacks
        ``UPDATE ... LIMIT RETURNING``. Skips rows whose endpoint is
        disabled. Returns ``None`` when nothing is due, or when the
        caller lost the race to another worker mid-transaction.
        """
        # Wrap in IMMEDIATE so concurrent workers serialise on the
        # write lock. Any pre-existing transaction on this connection
        # must be committed by the caller before calling claim_one.
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            candidate = self.conn.execute(
                """
                SELECT id FROM deliveries
                 WHERE status = 'pending'
                   AND next_attempt_at <= strftime('%Y-%m-%dT%H:%M:%fZ','now')
                   AND endpoint_id IN (
                       SELECT id FROM endpoints WHERE disabled_at IS NULL
                   )
                 ORDER BY next_attempt_at
                 LIMIT 1
                """
            ).fetchone()
            if candidate is None:
                self.conn.commit()
                return None

            delivery_id = int(candidate["id"])
            # The WHERE status='pending' guards against a concurrent
            # worker having already claimed this row between our SELECT
            # and UPDATE (defense-in-depth; the IMMEDIATE lock makes
            # that physically unreachable on one DB, but the pattern is
            # the right one for correctness-by-construction).
            row = self.conn.execute(
                f"""
                UPDATE deliveries
                   SET status = 'sending',
                       leased_until = strftime(
                           '%Y-%m-%dT%H:%M:%fZ', 'now',
                           '+' || ? || ' seconds'
                       ),
                       worker_id = ?
                 WHERE id = ?
                   AND status = 'pending'
                 RETURNING {", ".join(self._COLUMNS)}
                """,
                (lease_duration_secs, worker_id, delivery_id),
            ).fetchone()
            self.conn.commit()
        except BaseException:
            self.conn.rollback()
            raise

        return self._row_to_model(row, Delivery) if row is not None else None

    # -- terminal state transitions ---------------------------------------

    def mark_delivered(self, id: int) -> bool:
        """Mark a delivery ``delivered``. Clears lease fields."""
        cur = self.conn.execute(
            """
            UPDATE deliveries
               SET status = 'delivered',
                   delivered_at = ?,
                   leased_until = NULL,
                   worker_id = NULL
             WHERE id = ?
            """,
            (now_iso(), id),
        )
        return cur.rowcount > 0

    def mark_retry(self, id: int, error: str, backoff_secs: int) -> bool:
        """Transition ``sending → pending`` with an incremented attempt.

        Uses SQL-side ``strftime(..., '+N seconds')`` for the new
        ``next_attempt_at`` so timestamps stay consistent with the
        claim query.
        """
        cur = self.conn.execute(
            """
            UPDATE deliveries
               SET status = 'pending',
                   attempt_count = attempt_count + 1,
                   next_attempt_at = strftime(
                       '%Y-%m-%dT%H:%M:%fZ', 'now', '+' || ? || ' seconds'
                   ),
                   leased_until = NULL,
                   worker_id = NULL,
                   last_error = ?
             WHERE id = ?
            """,
            (backoff_secs, error, id),
        )
        return cur.rowcount > 0

    def mark_abandoned(self, id: int, error: str) -> bool:
        """Mark a delivery terminally ``abandoned`` with a reason."""
        cur = self.conn.execute(
            """
            UPDATE deliveries
               SET status = 'abandoned',
                   last_error = ?,
                   leased_until = NULL,
                   worker_id = NULL
             WHERE id = ?
            """,
            (error, id),
        )
        return cur.rowcount > 0

    # -- observability -----------------------------------------------------

    def count_stuck_pending(self, older_than_sec: int = 300) -> int:
        """Count pending rows whose ``next_attempt_at`` is older than N seconds.

        Surface metric for "is the delivery poller stuck?" dashboards.
        """
        row = self.conn.execute(
            """
            SELECT COUNT(*) AS c FROM deliveries
             WHERE status = 'pending'
               AND next_attempt_at < strftime(
                   '%Y-%m-%dT%H:%M:%fZ', 'now', '-' || ? || ' seconds'
               )
            """,
            (older_than_sec,),
        ).fetchone()
        return int(row["c"]) if row is not None else 0
