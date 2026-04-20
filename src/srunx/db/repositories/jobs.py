"""Repository for the ``jobs`` table.

Replaces the legacy ``srunx.history.JobHistory`` pair (``record_job`` +
``update_job_completion``) with a narrower, typed API that the new DB
stack uses. See design.md § JobRepository.
"""

from __future__ import annotations

import sqlite3
from typing import Any

from srunx.db.models import Job, SubmissionSource
from srunx.db.repositories.base import BaseRepository, now_iso


class JobRepository(BaseRepository):
    """CRUD for the ``jobs`` table."""

    JSON_FIELDS = ("command", "env_vars", "metadata")
    DATETIME_FIELDS = ("submitted_at", "started_at", "completed_at")

    _COLUMNS = (
        "id",
        "job_id",
        "name",
        "command",
        "status",
        "nodes",
        "gpus_per_node",
        "memory_per_node",
        "time_limit",
        "partition",
        "nodelist",
        "conda",
        "venv",
        "container",
        "env_vars",
        "submitted_at",
        "started_at",
        "completed_at",
        "duration_secs",
        "workflow_run_id",
        "submission_source",
        "log_file",
        "metadata",
    )

    def record_submission(
        self,
        *,
        job_id: int,
        name: str,
        status: str,
        submission_source: SubmissionSource,
        command: list[str] | None = None,
        nodes: int | None = None,
        gpus_per_node: int | None = None,
        memory_per_node: str | None = None,
        time_limit: str | None = None,
        partition: str | None = None,
        nodelist: str | None = None,
        conda: str | None = None,
        venv: str | None = None,
        container: str | None = None,
        env_vars: dict | None = None,
        submitted_at: str | None = None,
        workflow_run_id: int | None = None,
        log_file: str | None = None,
        metadata: dict | None = None,
    ) -> int:
        """Insert a new row for a just-submitted job.

        Uses ``INSERT OR IGNORE`` on the ``job_id`` UNIQUE constraint:
        if a row with this SLURM id already exists, the call is a
        no-op and returns ``0``. Callers that want to mutate an
        existing row should use :meth:`update_status` /
        :meth:`update_completion` explicitly.

        Rationale for **not** using ``INSERT OR REPLACE`` here
        (P1-2 in the Codex review triage): ``REPLACE`` executes
        ``DELETE`` + ``INSERT``, which triggers
        ``ON DELETE SET NULL`` on the FK references in
        ``workflow_run_jobs.job_id`` and ``job_state_transitions.job_id``.
        A re-submission path (or a bug-induced double call) would
        silently orphan every prior transition and membership, which
        corrupts the poller's dedup / aggregation invariants. It would
        also reset a poller-advanced ``status='RUNNING'`` row back to
        ``'PENDING'`` + rewrite ``submitted_at``. With ``IGNORE`` the
        first call wins; subsequent callers observe ``lastrowid=0``
        and must decide explicitly what to do.
        """
        submitted_at = submitted_at or now_iso()
        cur = self.conn.execute(
            """
            INSERT OR IGNORE INTO jobs (
                job_id, name, command, status,
                nodes, gpus_per_node, memory_per_node, time_limit,
                partition, nodelist,
                conda, venv, container, env_vars,
                submitted_at,
                workflow_run_id, submission_source,
                log_file, metadata
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                job_id,
                name,
                self._encode_json(command),
                status,
                nodes,
                gpus_per_node,
                memory_per_node,
                time_limit,
                partition,
                nodelist,
                conda,
                venv,
                container,
                self._encode_json(env_vars),
                submitted_at,
                workflow_run_id,
                submission_source,
                log_file,
                self._encode_json(metadata),
            ),
        )
        # ``lastrowid`` is only meaningful when rowcount > 0. SQLite (and
        # the Python driver) preserve the prior successful rowid from the
        # same connection across an IGNORE no-op, so relying on it would
        # make duplicates look like fresh inserts. Gate on rowcount.
        if cur.rowcount == 0:
            return 0
        return int(cur.lastrowid or 0)

    def update_status(
        self,
        job_id: int,
        status: str,
        *,
        started_at: str | None = None,
        completed_at: str | None = None,
        duration_secs: int | None = None,
        nodelist: str | None = None,
    ) -> bool:
        """Update a live job's status and lifecycle timestamps.

        Called by :class:`~srunx.pollers.active_watch_poller.ActiveWatchPoller`
        on every detected transition. Returns True if a row was updated.
        """
        sets: list[str] = ["status = ?"]
        vals: list[Any] = [status]
        if started_at is not None:
            sets.append("started_at = ?")
            vals.append(started_at)
        if completed_at is not None:
            sets.append("completed_at = ?")
            vals.append(completed_at)
        if duration_secs is not None:
            sets.append("duration_secs = ?")
            vals.append(duration_secs)
        if nodelist is not None:
            sets.append("nodelist = ?")
            vals.append(nodelist)
        vals.append(job_id)

        cur = self.conn.execute(
            f"UPDATE jobs SET {', '.join(sets)} WHERE job_id = ?",
            vals,
        )
        return cur.rowcount > 0

    def update_completion(
        self,
        job_id: int,
        status: str,
        completed_at: str | None = None,
    ) -> bool:
        """Compatibility wrapper for the historical ``update_job_completion``.

        Computes ``duration_secs`` from ``submitted_at`` when not provided.
        """
        completed_at = completed_at or now_iso()
        row = self.conn.execute(
            "SELECT submitted_at FROM jobs WHERE job_id = ?", (job_id,)
        ).fetchone()
        duration: int | None = None
        if row is not None:
            from srunx.db.repositories.base import _parse_dt

            submitted = _parse_dt(row["submitted_at"])
            done = _parse_dt(completed_at)
            if submitted and done:
                duration = int((done - submitted).total_seconds())

        return self.update_status(
            job_id,
            status,
            completed_at=completed_at,
            duration_secs=duration,
        )

    def get(self, job_id: int) -> Job | None:
        row = self.conn.execute(
            f"SELECT {', '.join(self._COLUMNS)} FROM jobs WHERE job_id = ?",
            (job_id,),
        ).fetchone()
        return self._row_to_model(row, Job)

    def list_all(
        self,
        *,
        limit: int = 50,
        offset: int = 0,
        workflow_run_id: int | None = None,
    ) -> list[Job]:
        sql = f"SELECT {', '.join(self._COLUMNS)} FROM jobs"
        params: list[Any] = []
        if workflow_run_id is not None:
            sql += " WHERE workflow_run_id = ?"
            params.append(workflow_run_id)
        sql += " ORDER BY submitted_at DESC LIMIT ? OFFSET ?"
        params.extend([limit, offset])

        rows = self.conn.execute(sql, params).fetchall()
        return [self._row_to_model(r, Job) for r in rows if r is not None]  # type: ignore[misc]

    def count_by_status_in_range(
        self,
        from_at: str,
        to_at: str,
        statuses: list[str] | None = None,
    ) -> dict[str, int]:
        """Return per-status counts for jobs submitted in ``[from_at, to_at)``.

        Provided for the future ``ScheduledReporter`` replacement
        (design.md § "Phase 1 で変更しない領域"). ``statuses=None`` counts
        every distinct status.
        """
        if statuses:
            placeholders = ",".join(["?"] * len(statuses))
            sql = (
                "SELECT status, COUNT(*) AS c FROM jobs "
                f"WHERE submitted_at >= ? AND submitted_at < ? AND status IN ({placeholders}) "
                "GROUP BY status"
            )
            params: list[Any] = [from_at, to_at, *statuses]
        else:
            sql = (
                "SELECT status, COUNT(*) AS c FROM jobs "
                "WHERE submitted_at >= ? AND submitted_at < ? "
                "GROUP BY status"
            )
            params = [from_at, to_at]

        rows = self.conn.execute(sql, params).fetchall()
        return {r["status"]: int(r["c"]) for r in rows}

    def delete(self, job_id: int) -> bool:
        cur = self.conn.execute("DELETE FROM jobs WHERE job_id = ?", (job_id,))
        return cur.rowcount > 0

    # Convenience — for callers that hold the raw connection/cursor.
    def _raw(self) -> sqlite3.Connection:
        return self.conn
