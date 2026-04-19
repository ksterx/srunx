"""Repository for the ``job_state_transitions`` table.

Design reference: ``.claude/specs/notification-and-state-persistence/design.md``
§ Repositories. Records every observed job status change (SSOT for
:class:`~srunx.pollers.active_watch_poller.ActiveWatchPoller` and CLI
monitor alike).
"""

from __future__ import annotations

from srunx.db.models import JobStateTransition, TransitionSource
from srunx.db.repositories.base import BaseRepository, now_iso


class JobStateTransitionRepository(BaseRepository):
    """Append-only log of job status transitions."""

    DATETIME_FIELDS = ("observed_at",)

    _COLUMNS = (
        "id",
        "job_id",
        "from_status",
        "to_status",
        "observed_at",
        "source",
    )

    def insert(
        self,
        job_id: int,
        from_status: str | None,
        to_status: str,
        source: TransitionSource,
        observed_at: str | None = None,
    ) -> int:
        """Insert a new transition row. Returns the row's ``id``."""
        observed_at = observed_at or now_iso()
        cur = self.conn.execute(
            """
            INSERT OR REPLACE INTO job_state_transitions (
                job_id, from_status, to_status, observed_at, source
            ) VALUES (?, ?, ?, ?, ?)
            """,
            (job_id, from_status, to_status, observed_at, source),
        )
        return int(cur.lastrowid or 0)

    def latest_for_job(self, job_id: int) -> JobStateTransition | None:
        """Return the most recent transition for ``job_id``, or ``None``.

        Used by the poller for dedup: it only writes when the observed
        ``to_status`` differs from the stored latest.
        """
        row = self.conn.execute(
            f"SELECT {', '.join(self._COLUMNS)} FROM job_state_transitions "
            "WHERE job_id = ? ORDER BY observed_at DESC, id DESC LIMIT 1",
            (job_id,),
        ).fetchone()
        return self._row_to_model(row, JobStateTransition)

    def history_for_job(self, job_id: int) -> list[JobStateTransition]:
        """Return all transitions for ``job_id`` in chronological order."""
        rows = self.conn.execute(
            f"SELECT {', '.join(self._COLUMNS)} FROM job_state_transitions "
            "WHERE job_id = ? ORDER BY observed_at ASC, id ASC",
            (job_id,),
        ).fetchall()
        return [
            self._row_to_model(r, JobStateTransition)  # type: ignore[misc]
            for r in rows
            if r is not None
        ]
