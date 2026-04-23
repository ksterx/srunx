"""Repository for the ``job_state_transitions`` table.

Design reference: ``.claude/specs/notification-and-state-persistence/design.md``
§ Repositories. Records every observed job status change (SSOT for
:class:`~srunx.pollers.active_watch_poller.ActiveWatchPoller` and CLI
monitor alike).

V5 note: the on-disk column is ``jobs_row_id`` (FK to ``jobs.id``),
but the public API still takes ``job_id`` (SLURM id) so existing
callers work unchanged. SF5 then hardened the API to **require**
``scheduler_key`` as a keyword argument on :meth:`insert`,
:meth:`latest_for_job`, and :meth:`history_for_job` — a silent
fallback to ``'local'`` was the root cause of bugs #1/#2/#3, where
SSH-backed jobs got their transitions looked up under the wrong axis.
Callers pass ``scheduler_key='local'`` explicitly for local SLURM, or
``'ssh:<profile>'`` for SSH transports.
"""

from __future__ import annotations

from srunx.observability.storage.models import JobStateTransition, TransitionSource
from srunx.observability.storage.repositories.base import BaseRepository, now_iso


class JobStateTransitionRepository(BaseRepository):
    """Append-only log of job status transitions."""

    DATETIME_FIELDS = ("observed_at",)

    # Projection alias: the column is ``jobs_row_id`` but we re-expose it
    # as ``job_id`` on the row dict so :class:`JobStateTransition` sees
    # the legacy field name when read through ``_row_to_model``. The
    # legacy public name ``JobStateTransition.job_id`` now carries the
    # SLURM id, resolved via ``LEFT JOIN jobs``.
    _SELECT_COLUMNS = (
        "jst.id AS id",
        "jst.jobs_row_id AS jobs_row_id",
        "j.job_id AS job_id",
        "jst.from_status AS from_status",
        "jst.to_status AS to_status",
        "jst.observed_at AS observed_at",
        "jst.source AS source",
    )

    def _resolve_jobs_row_id(self, job_id: int, scheduler_key: str) -> int | None:
        row = self.conn.execute(
            "SELECT id FROM jobs WHERE scheduler_key = ? AND job_id = ?",
            (scheduler_key, job_id),
        ).fetchone()
        return int(row["id"]) if row is not None else None

    def insert(
        self,
        job_id: int,
        from_status: str | None,
        to_status: str,
        source: TransitionSource,
        observed_at: str | None = None,
        *,
        scheduler_key: str,
    ) -> int:
        """Insert a new transition row. Returns the row's ``id``.

        Accepts the SLURM ``job_id`` (+ ``scheduler_key``). The
        ``jobs_row_id`` column on disk is resolved via a lookup against
        ``jobs``. If no matching jobs row exists the transition is
        still inserted with ``jobs_row_id=NULL`` so the append-only log
        is never silently dropped — the poller / CLI still benefits
        from observability on orphan ids.

        ``scheduler_key`` is required (SF5); pass ``'local'`` explicitly
        for local SLURM. Defaulting would silently dedup SSH transitions
        against the local row's history.
        """
        observed_at = observed_at or now_iso()
        jobs_row_id = self._resolve_jobs_row_id(job_id, scheduler_key)
        cur = self.conn.execute(
            """
            INSERT OR REPLACE INTO job_state_transitions (
                jobs_row_id, from_status, to_status, observed_at, source
            ) VALUES (?, ?, ?, ?, ?)
            """,
            (jobs_row_id, from_status, to_status, observed_at, source),
        )
        return int(cur.lastrowid or 0)

    def latest_for_job(
        self, job_id: int, *, scheduler_key: str
    ) -> JobStateTransition | None:
        """Return the most recent transition for ``(scheduler_key, job_id)``.

        Used by the poller for dedup: it only writes when the observed
        ``to_status`` differs from the stored latest. Resolves
        ``jobs_row_id`` from the ``jobs`` lookup so both pre-V5 writes
        (those with ``jobs_row_id`` set by backfill) and newly inserted
        rows match correctly.

        ``scheduler_key`` is required (SF5); pass ``'local'`` explicitly
        for local SLURM.
        """
        row = self.conn.execute(
            f"SELECT {', '.join(self._SELECT_COLUMNS)} "
            "FROM job_state_transitions jst "
            "LEFT JOIN jobs j ON j.id = jst.jobs_row_id "
            "WHERE j.scheduler_key = ? AND j.job_id = ? "
            "ORDER BY jst.observed_at DESC, jst.id DESC LIMIT 1",
            (scheduler_key, job_id),
        ).fetchone()
        return self._row_to_model(row, JobStateTransition)

    def history_for_job(
        self, job_id: int, *, scheduler_key: str
    ) -> list[JobStateTransition]:
        """Return all transitions for the given job in chronological order.

        ``scheduler_key`` is required (SF5); pass ``'local'`` explicitly
        for local SLURM.
        """
        rows = self.conn.execute(
            f"SELECT {', '.join(self._SELECT_COLUMNS)} "
            "FROM job_state_transitions jst "
            "LEFT JOIN jobs j ON j.id = jst.jobs_row_id "
            "WHERE j.scheduler_key = ? AND j.job_id = ? "
            "ORDER BY jst.observed_at ASC, jst.id ASC",
            (scheduler_key, job_id),
        ).fetchall()
        return [
            self._row_to_model(r, JobStateTransition)  # type: ignore[misc]
            for r in rows
            if r is not None
        ]
