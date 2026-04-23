"""Repository for the ``jobs`` table.

Replaces the legacy ``record_job`` / ``update_job_completion`` pair
that used to live in the removed ``srunx.history`` module (see
P2-4 #A cutover) with a narrower, typed API.
"""

from __future__ import annotations

import sqlite3
from typing import Any

from srunx.db.models import Job, SubmissionSource, TransportType
from srunx.db.repositories.base import BaseRepository, now_iso

# Validate that the (transport_type, profile_name, scheduler_key) triple
# matches the V5 CHECK constraint on the ``jobs`` table. Raising here
# gives callers a Python-level error with a clear message instead of a
# sqlite IntegrityError that surfaces at INSERT time.
_SCHEDULER_KEY_LOCAL = "local"


def _validate_transport_triple(
    transport_type: TransportType,
    profile_name: str | None,
    scheduler_key: str,
) -> None:
    if transport_type == "local":
        if profile_name is not None:
            raise ValueError("profile_name must be None for transport_type='local'")
        if scheduler_key != _SCHEDULER_KEY_LOCAL:
            raise ValueError(
                f"scheduler_key must be 'local' for transport_type='local'; "
                f"got {scheduler_key!r}"
            )
    elif transport_type == "ssh":
        if not profile_name:
            raise ValueError("profile_name is required for transport_type='ssh'")
        if ":" in profile_name:
            raise ValueError(f"profile_name must not contain ':'; got {profile_name!r}")
        expected = f"ssh:{profile_name}"
        if scheduler_key != expected:
            raise ValueError(
                f"scheduler_key must be {expected!r} for "
                f"transport_type='ssh', profile_name={profile_name!r}; "
                f"got {scheduler_key!r}"
            )
    else:  # pragma: no cover — Literal narrows this out
        raise ValueError(f"unknown transport_type: {transport_type!r}")


class JobRepository(BaseRepository):
    """CRUD for the ``jobs`` table.

    V5 (CLI transport unification) added ``transport_type`` /
    ``profile_name`` / ``scheduler_key`` columns and broadened the
    uniqueness key from ``job_id`` alone to ``(scheduler_key, job_id)``
    so the same SLURM id can safely co-exist across ``local`` and
    ``ssh:<profile>`` transports.

    SF5 hardened the read / update API: :meth:`get`, :meth:`update_status`,
    :meth:`update_completion`, and :meth:`delete` now **require**
    ``scheduler_key`` as a keyword argument. Callers must pass the
    matching value (``'local'`` for local SLURM, ``'ssh:<profile>'`` for
    SSH) so the axis is explicit at every call site and bugs #1/#2/#3
    (silent fallback to ``'local'`` for SSH-backed jobs) cannot recur.
    :meth:`record_submission` keeps its local-triple default because its
    three transport columns are consistent with each other.
    """

    JSON_FIELDS = ("command", "env_vars", "metadata")
    DATETIME_FIELDS = ("submitted_at", "started_at", "completed_at")

    _COLUMNS = (
        "id",
        "job_id",
        "transport_type",
        "profile_name",
        "scheduler_key",
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
        transport_type: TransportType = "local",
        profile_name: str | None = None,
        scheduler_key: str = "local",
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

        Uses ``INSERT OR IGNORE`` on the ``(scheduler_key, job_id)``
        UNIQUE constraint (V5+): if a row with this combined key already
        exists, the call is a no-op and returns ``0``. Callers that want
        to mutate an existing row should use :meth:`update_status` /
        :meth:`update_completion` explicitly.

        ``transport_type`` / ``profile_name`` / ``scheduler_key`` default
        to the local-SLURM triple so pre-V5 callers remain source-compat.
        For SSH submissions the caller must provide all three with a
        consistent shape (``transport_type='ssh'``, non-None
        ``profile_name`` without ``:``, ``scheduler_key='ssh:<profile>'``);
        :func:`_validate_transport_triple` rejects mismatches up front.

        Rationale for **not** using ``INSERT OR REPLACE`` here
        (P1-2 in the Codex review triage): ``REPLACE`` executes
        ``DELETE`` + ``INSERT``, which triggers
        ``ON DELETE SET NULL`` on the FK references in
        ``workflow_run_jobs.jobs_row_id`` and
        ``job_state_transitions.jobs_row_id``. A re-submission path
        would silently orphan every prior transition and membership.
        With ``IGNORE`` the first call wins; subsequent callers observe
        ``lastrowid=0`` and must decide explicitly what to do.
        """
        _validate_transport_triple(transport_type, profile_name, scheduler_key)

        submitted_at = submitted_at or now_iso()
        cur = self.conn.execute(
            """
            INSERT OR IGNORE INTO jobs (
                job_id, transport_type, profile_name, scheduler_key,
                name, command, status,
                nodes, gpus_per_node, memory_per_node, time_limit,
                partition, nodelist,
                conda, venv, container, env_vars,
                submitted_at,
                workflow_run_id, submission_source,
                log_file, metadata
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                job_id,
                transport_type,
                profile_name,
                scheduler_key,
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
        scheduler_key: str,
        started_at: str | None = None,
        completed_at: str | None = None,
        duration_secs: int | None = None,
        nodelist: str | None = None,
    ) -> bool:
        """Update a live job's status and lifecycle timestamps.

        Called by :class:`~srunx.pollers.active_watch_poller.ActiveWatchPoller`
        on every detected transition. Returns True if a row was updated.

        ``scheduler_key`` is required (SF5) so callers cannot accidentally
        target the local-SLURM row when they meant an SSH transport.
        Pass ``scheduler_key='local'`` explicitly for local SLURM.
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
        vals.extend([scheduler_key, job_id])

        cur = self.conn.execute(
            f"UPDATE jobs SET {', '.join(sets)} WHERE scheduler_key = ? AND job_id = ?",
            vals,
        )
        return cur.rowcount > 0

    def update_completion(
        self,
        job_id: int,
        status: str,
        completed_at: str | None = None,
        *,
        scheduler_key: str,
    ) -> bool:
        """Compatibility wrapper for the historical ``update_job_completion``.

        Computes ``duration_secs`` from ``submitted_at`` when not provided.

        ``scheduler_key`` is required (SF5); pass ``'local'`` explicitly
        for local SLURM. Defaulting silently would silently fall back to
        the local row for SSH transports.
        """
        completed_at = completed_at or now_iso()
        row = self.conn.execute(
            "SELECT submitted_at FROM jobs WHERE scheduler_key = ? AND job_id = ?",
            (scheduler_key, job_id),
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
            scheduler_key=scheduler_key,
            completed_at=completed_at,
            duration_secs=duration,
        )

    def get(self, job_id: int, *, scheduler_key: str) -> Job | None:
        """Return the jobs row for ``(scheduler_key, job_id)``.

        ``scheduler_key`` is required (SF5). Bugs #1/#2/#3 all originated
        from code that forgot to pass it and silently got the local row
        back for SSH-backed jobs. Pass ``scheduler_key='local'`` for
        local SLURM, or use :meth:`get_by_row_id` when the caller
        already has ``jobs.id``.
        """
        row = self.conn.execute(
            f"SELECT {', '.join(self._COLUMNS)} FROM jobs "
            "WHERE scheduler_key = ? AND job_id = ?",
            (scheduler_key, job_id),
        ).fetchone()
        return self._row_to_model(row, Job)

    def get_by_row_id(self, row_id: int) -> Job | None:
        """Return the jobs row for ``jobs.id`` (AUTOINCREMENT PK).

        Used by the V5 FK-retargeted child tables
        (``workflow_run_jobs.jobs_row_id`` /
        ``job_state_transitions.jobs_row_id``) that carry the row id
        directly instead of the SLURM ``job_id``.
        """
        row = self.conn.execute(
            f"SELECT {', '.join(self._COLUMNS)} FROM jobs WHERE id = ?",
            (row_id,),
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

    _ALLOWED_RANGE_FIELDS = ("submitted_at", "started_at", "completed_at")

    def count_by_status_in_range(
        self,
        from_at: str,
        to_at: str,
        statuses: list[str] | None = None,
        *,
        timestamp_field: str = "submitted_at",
    ) -> dict[str, int]:
        """Return per-status counts for jobs whose chosen timestamp is in ``[from_at, to_at)``.

        ``timestamp_field`` picks the lifecycle column the range filters on.
        ``submitted_at`` (default) matches "jobs queued in this window".
        ``completed_at`` matches ``sacct --starttime`` + terminal-state
        semantics more closely — used by
        :class:`srunx.monitor.scheduler.ScheduledReporter` so a 24h
        report counts what *finished* in the window, not what was merely
        submitted.

        Raises ``ValueError`` for an unsupported field rather than
        injecting it into the SQL (this parameter is interpolated into
        the query because sqlite can't bind column names).
        """
        if timestamp_field not in self._ALLOWED_RANGE_FIELDS:
            raise ValueError(
                f"timestamp_field must be one of {self._ALLOWED_RANGE_FIELDS}; "
                f"got {timestamp_field!r}"
            )

        if statuses:
            placeholders = ",".join(["?"] * len(statuses))
            sql = (
                "SELECT status, COUNT(*) AS c FROM jobs "
                f"WHERE {timestamp_field} >= ? AND {timestamp_field} < ? "
                f"AND status IN ({placeholders}) "
                "GROUP BY status"
            )
            params: list[Any] = [from_at, to_at, *statuses]
        else:
            sql = (
                "SELECT status, COUNT(*) AS c FROM jobs "
                f"WHERE {timestamp_field} >= ? AND {timestamp_field} < ? "
                "GROUP BY status"
            )
            params = [from_at, to_at]

        rows = self.conn.execute(sql, params).fetchall()
        return {r["status"]: int(r["c"]) for r in rows}

    def delete(self, job_id: int, *, scheduler_key: str) -> bool:
        """Delete the jobs row for ``(scheduler_key, job_id)``.

        ``scheduler_key`` is required (SF5). Pass ``'local'`` explicitly
        for local SLURM.
        """
        cur = self.conn.execute(
            "DELETE FROM jobs WHERE scheduler_key = ? AND job_id = ?",
            (scheduler_key, job_id),
        )
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

    def list_recent_as_dict(
        self,
        limit: int = 100,
        *,
        job_ids: list[int] | None = None,
        scheduler_key: str | None = None,
    ) -> list[dict[str, Any]]:
        """Return the ``limit`` most recent jobs in the legacy dict shape.

        Used by the ``/api/history`` router + ``srunx sacct`` CLI to
        keep the response/display formats stable across the cutover
        from the removed ``srunx.history`` module (P2-4 #A).

        ``job_ids`` filters to a specific subset of jobs and bypasses
        the ``LIMIT`` so ``srunx sacct -j <id>`` finds the job even
        when it falls outside the most recent ``limit`` rows. Codex
        follow-up #2 on PR #134 — without this push-down the CLI
        would silently report "no history found" for any job older
        than the page size.

        ``scheduler_key`` (e.g. ``"local"`` / ``"ssh:dgx"``) scopes
        the query to a single transport so ``srunx sacct --profile X``
        only sees jobs that ran against that cluster. ``None`` keeps
        the legacy "all transports" behaviour for the Web UI history.
        """
        scheduler_clause = ""
        scheduler_param: tuple[Any, ...] = ()
        if scheduler_key is not None:
            scheduler_clause = " AND j.scheduler_key = ?"
            scheduler_param = (scheduler_key,)

        if job_ids:
            placeholders = ",".join("?" * len(job_ids))
            rows = self.conn.execute(
                f"""
                SELECT
                    j.job_id,
                    j.name           AS job_name,
                    j.command,
                    j.status,
                    j.nodes,
                    j.gpus_per_node,
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
                WHERE j.job_id IN ({placeholders}){scheduler_clause}
                ORDER BY j.submitted_at DESC
                """,
                tuple(job_ids) + scheduler_param,
            ).fetchall()
            return [dict(r) for r in rows]

        where_clause = " WHERE j.scheduler_key = ?" if scheduler_key is not None else ""
        rows = self.conn.execute(
            f"""
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
            LEFT JOIN workflow_runs wr ON wr.id = j.workflow_run_id{where_clause}
            ORDER BY j.submitted_at DESC
            LIMIT ?
            """,
            scheduler_param + (limit,),
        ).fetchall()
        return [dict(r) for r in rows]

    def compute_stats(
        self,
        from_date: str | None = None,
        to_date: str | None = None,
        *,
        scheduler_key: str | None = None,
    ) -> dict[str, Any]:
        """Return aggregate stats in the legacy ``JobHistory.get_job_stats`` shape.

        ``from_date`` / ``to_date`` are inclusive ISO-8601 dates (e.g.
        ``"2026-04-01"``). ``to_date`` is interpreted as ``<= day-end``,
        matching the legacy behaviour.

        ``scheduler_key`` (e.g. ``"local"`` / ``"ssh:dgx"``) scopes the
        aggregate to a single transport so ``srunx sreport --profile X``
        reflects only that cluster.
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
        if scheduler_key is not None:
            conditions.append("scheduler_key = ?")
            params.append(scheduler_key)
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
