"""Repository for the ``jobs`` table.

Replaces the legacy ``record_job`` / ``update_job_completion`` pair
that used to live in the removed ``srunx.history`` module (see
P2-4 #A cutover) with a narrower, typed API.
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

        Uses ``INSERT OR REPLACE`` on ``job_id``: a resubmission under the
        same SLURM ID replaces the prior record. Returns the row's ``id``.
        """
        submitted_at = submitted_at or now_iso()
        cur = self.conn.execute(
            """
            INSERT OR REPLACE INTO jobs (
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

    # ------------------------------------------------------------------
    # Display-shaped readers (history cutover — P2-4 #A)
    # ------------------------------------------------------------------
    #
    # These helpers return dicts with the historical ``JobHistory`` key
    # shape so that the ``/api/history`` router and the
    # ``srunx history`` / ``srunx report`` CLI commands migrated off
    # the legacy ``~/.srunx/history.db`` without churning their
    # response/display formats. Column renames + dropped columns are
    # handled here:
    #
    #   legacy key            → new schema source
    #   --------------------  --------------------------------------
    #   job_name              → jobs.name
    #   conda_env             → jobs.conda
    #   duration_seconds      → jobs.duration_secs
    #   workflow_name         → LEFT JOIN workflow_runs.workflow_name
    #   cpus_per_task         → (absent; returned as None — the column
    #                           was dropped from SCHEMA_V1)
    #
    # Keys that do exist under the same name (job_id, status, nodes,
    # gpus_per_node, memory_per_node, time_limit, partition, command,
    # submitted_at, completed_at, log_file, metadata) are passed
    # through unchanged.

    def list_recent_as_dict(self, limit: int = 100) -> list[dict[str, Any]]:
        """Return the ``limit`` most recent jobs in the legacy dict shape.

        Used by the ``/api/history`` router + ``srunx history`` CLI to
        keep the response/display formats stable across the cutover
        from the removed ``srunx.history`` module (P2-4 #A).
        """
        rows = self.conn.execute(
            """
            SELECT
                j.job_id,
                j.name           AS job_name,
                j.command,
                j.status,
                j.nodes,
                j.gpus_per_node,
                -- ``gpus`` is the total allocation the ``/api/history``
                -- serializer expects (``serialize_history_entry`` reads
                -- ``entry.get('gpus')``). Compute nodes * gpus_per_node
                -- here so the field is populated; legacy JobHistory
                -- stored only ``gpus_per_node`` and the serializer was
                -- getting None for every row — surfaced under P3-10
                -- review, fixed inline with the cutover.
                (COALESCE(j.nodes, 0) * COALESCE(j.gpus_per_node, 0)) AS gpus,
                NULL             AS cpus_per_task,
                j.memory_per_node,
                j.time_limit,
                j.partition,
                j.conda          AS conda_env,
                j.submitted_at,
                j.completed_at,
                j.duration_secs  AS duration_seconds,
                wr.workflow_name AS workflow_name,
                j.log_file,
                j.metadata
            FROM jobs j
            LEFT JOIN workflow_runs wr ON wr.id = j.workflow_run_id
            ORDER BY j.submitted_at DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
        return [dict(r) for r in rows]

    def compute_stats(
        self,
        from_date: str | None = None,
        to_date: str | None = None,
    ) -> dict[str, Any]:
        """Return aggregate stats in the legacy ``JobHistory.get_job_stats`` shape.

        ``from_date`` / ``to_date`` are inclusive ISO-8601 dates (e.g.
        ``"2026-04-01"``). ``to_date`` is interpreted as ``<= day-end``,
        matching the legacy behaviour.
        """
        conditions: list[str] = []
        params: list[Any] = []
        if from_date:
            conditions.append("submitted_at >= ?")
            params.append(from_date)
        if to_date:
            conditions.append("submitted_at < ?")
            # Match legacy behaviour: include the full ``to_date`` day.
            params.append(to_date + "T23:59:59" if "T" not in to_date else to_date)
        where_clause = (" WHERE " + " AND ".join(conditions)) if conditions else ""

        total = int(
            self.conn.execute(
                f"SELECT COUNT(*) AS c FROM jobs{where_clause}", params
            ).fetchone()["c"]
        )

        by_status_rows = self.conn.execute(
            f"SELECT status, COUNT(*) AS c FROM jobs{where_clause} GROUP BY status",
            params,
        ).fetchall()
        jobs_by_status = {r["status"]: int(r["c"]) for r in by_status_rows}

        # Averaged / summed metrics only over rows that have the
        # relevant columns populated.
        avg_duration_row = self.conn.execute(
            f"SELECT AVG(duration_secs) AS a FROM jobs{where_clause} "
            "AND duration_secs IS NOT NULL"
            if where_clause
            else "SELECT AVG(duration_secs) AS a FROM jobs "
            "WHERE duration_secs IS NOT NULL",
            params,
        ).fetchone()
        avg_duration_seconds = (
            float(avg_duration_row["a"]) if avg_duration_row["a"] is not None else None
        )

        total_gpu_hours_row = self.conn.execute(
            (
                f"SELECT SUM(duration_secs * gpus_per_node * nodes) / 3600.0 AS h "
                f"FROM jobs{where_clause} "
                "AND duration_secs IS NOT NULL AND gpus_per_node IS NOT NULL"
            )
            if where_clause
            else (
                "SELECT SUM(duration_secs * gpus_per_node * nodes) / 3600.0 AS h "
                "FROM jobs "
                "WHERE duration_secs IS NOT NULL AND gpus_per_node IS NOT NULL"
            ),
            params,
        ).fetchone()
        total_gpu_hours = (
            float(total_gpu_hours_row["h"])
            if total_gpu_hours_row["h"] is not None
            else 0
        )

        return {
            "total_jobs": total,
            "jobs_by_status": jobs_by_status,
            "avg_duration_seconds": avg_duration_seconds,
            "total_gpu_hours": total_gpu_hours,
            "from_date": from_date,
            "to_date": to_date,
        }

    # Convenience — for callers that hold the raw connection/cursor.
    def _raw(self) -> sqlite3.Connection:
        return self.conn
