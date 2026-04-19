"""Job execution history tracking with SQLite.

.. deprecated:: 2026-Q2
    The legacy ``~/.srunx/history.db`` is being phased out in favour of
    the unified ``~/.config/srunx/srunx.db`` (see :mod:`srunx.db`).
    :class:`JobHistory` now *dual-writes* to both DBs so the new
    :class:`~srunx.db.repositories.jobs.JobRepository` receives every
    CLI-submitted job and every monitor-observed completion. Read paths
    continue to hit the legacy DB until a follow-up migration lands.
"""

import json
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any

from srunx.logging import get_logger
from srunx.models import JobStatus, JobType

logger = get_logger(__name__)


def _dual_write_record_submission(job: JobType, workflow_name: str | None) -> None:
    """Mirror a job submission into the new SQLite state DB.

    Best-effort: any failure is logged and swallowed so the legacy CLI
    path is never broken by new-DB issues. Used by :meth:`JobHistory.record_job`.
    """
    try:
        from srunx.db.connection import init_db, open_connection
        from srunx.db.repositories.job_state_transitions import (
            JobStateTransitionRepository,
        )
        from srunx.db.repositories.jobs import JobRepository

        if job.job_id is None:
            return

        # Ensure the DB + schema exist (idempotent; cheap on repeat).
        init_db(delete_legacy=False)
        conn = open_connection()
        try:
            resources = getattr(job, "resources", None)
            environment = getattr(job, "environment", None)
            command_val = getattr(job, "command", None)
            JobRepository(conn).record_submission(
                job_id=int(job.job_id),
                name=job.name,
                status=(job._status.value if hasattr(job, "_status") else "PENDING"),
                submission_source=("workflow" if workflow_name else "cli"),
                command=command_val if isinstance(command_val, list) else None,
                nodes=getattr(resources, "nodes", None) if resources else None,
                gpus_per_node=(
                    getattr(resources, "gpus_per_node", None) if resources else None
                ),
                memory_per_node=(
                    getattr(resources, "memory_per_node", None) if resources else None
                ),
                time_limit=(
                    getattr(resources, "time_limit", None) if resources else None
                ),
                partition=(
                    getattr(resources, "partition", None) if resources else None
                ),
                nodelist=(getattr(resources, "nodelist", None) if resources else None),
                conda=(getattr(environment, "conda", None) if environment else None),
                venv=(getattr(environment, "venv", None) if environment else None),
                env_vars=(
                    getattr(environment, "env_vars", None) if environment else None
                ),
            )
            # Seed a baseline transition so the active-watch poller's first
            # observation is treated as a real state change.
            JobStateTransitionRepository(conn).insert(
                job_id=int(job.job_id),
                from_status=None,
                to_status="PENDING",
                source="webhook",
            )
        finally:
            conn.close()
    except Exception as exc:  # noqa: BLE001 — best-effort mirror
        logger.debug(f"dual-write (new DB) of job submission skipped: {exc}")


def _dual_write_update_completion(job_id: int, status: JobStatus) -> None:
    """Mirror a terminal-status observation into the new SQLite state DB."""
    try:
        from srunx.db.connection import init_db, open_connection, transaction
        from srunx.db.repositories.job_state_transitions import (
            JobStateTransitionRepository,
        )
        from srunx.db.repositories.jobs import JobRepository

        init_db(delete_legacy=False)
        conn = open_connection()
        try:
            repo = JobRepository(conn)
            # Only update rows that already exist (CLI jobs recorded at submit).
            if repo.get(job_id) is None:
                return
            transition_repo = JobStateTransitionRepository(conn)
            # R5: two observers (CLI monitor + poller, or two CLIs) can
            # race the read-modify-write and both append a terminal
            # transition for the same (job_id, to_status) pair. Wrap
            # the latest-then-insert pair inside BEGIN IMMEDIATE so
            # only one caller's re-check sees ``latest_status != status``
            # and writes; the other sees its own insert reflected and
            # skips.
            with transaction(conn, "IMMEDIATE"):
                latest = transition_repo.latest_for_job(job_id)
                latest_status = latest.to_status if latest is not None else None
                if latest_status != status.value:
                    transition_repo.insert(
                        job_id=job_id,
                        from_status=latest_status,
                        to_status=status.value,
                        source="cli_monitor",
                    )
                repo.update_completion(job_id, status.value)
        finally:
            conn.close()
    except Exception as exc:  # noqa: BLE001 — best-effort mirror
        logger.debug(f"dual-write (new DB) of job completion skipped: {exc}")


# Current schema version
SCHEMA_VERSION = 1


class JobHistory:
    """Manage job execution history in SQLite database."""

    def __init__(self, db_path: str | Path | None = None):
        """Initialize job history manager.

        Args:
            db_path: Path to SQLite database file. Defaults to ~/.srunx/history.db
        """
        if db_path is None:
            db_path = Path.home() / ".srunx" / "history.db"
        else:
            db_path = Path(db_path)

        # Create directory if it doesn't exist
        db_path.parent.mkdir(parents=True, exist_ok=True)

        self.db_path = db_path
        self._init_database()
        self._run_migrations()

    def _init_database(self) -> None:
        """Initialize database schema with version tracking."""
        with sqlite3.connect(self.db_path) as conn:
            # Create schema version table
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS schema_version (
                    version INTEGER PRIMARY KEY,
                    applied_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS jobs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    job_id INTEGER NOT NULL,
                    job_name TEXT NOT NULL,
                    command TEXT,
                    status TEXT NOT NULL,
                    nodes INTEGER,
                    gpus_per_node INTEGER,
                    cpus_per_task INTEGER,
                    memory_per_node TEXT,
                    time_limit TEXT,
                    partition TEXT,
                    conda_env TEXT,
                    submitted_at TIMESTAMP NOT NULL,
                    completed_at TIMESTAMP,
                    duration_seconds REAL,
                    workflow_name TEXT,
                    log_file TEXT,
                    metadata TEXT
                )
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_job_id ON jobs(job_id)
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_submitted_at ON jobs(submitted_at)
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_status ON jobs(status)
                """
            )
            conn.commit()

    def _get_current_version(self) -> int:
        """Get current database schema version.

        Returns:
            Current schema version, or 0 if no version is set
        """
        with sqlite3.connect(self.db_path) as conn:
            try:
                cursor = conn.execute("SELECT MAX(version) FROM schema_version")
                result = cursor.fetchone()
                version = result[0] if result and result[0] else 0

                # Handle backward compatibility: if version is 0 but jobs table exists,
                # assume schema v1 and set it
                if version == 0:
                    cursor = conn.execute(
                        "SELECT name FROM sqlite_master WHERE type='table' AND name='jobs'"
                    )
                    if cursor.fetchone():
                        logger.info(
                            "Detected existing database without version, setting to v1"
                        )
                        self._set_version(1)
                        return 1

                return version
            except sqlite3.OperationalError:
                # Table doesn't exist yet
                return 0

    def _set_version(self, version: int) -> None:
        """Set database schema version.

        Args:
            version: Version number to set
        """
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "INSERT OR REPLACE INTO schema_version (version) VALUES (?)",
                (version,),
            )
            conn.commit()

    def _run_migrations(self) -> None:
        """Run database migrations if needed."""
        current_version = self._get_current_version()

        if current_version < SCHEMA_VERSION:
            logger.info(
                f"Running database migrations from version {current_version} to {SCHEMA_VERSION}"
            )

            # Run migrations in order
            for version in range(current_version + 1, SCHEMA_VERSION + 1):
                migration_method = getattr(self, f"_migrate_to_v{version}", None)
                if migration_method:
                    logger.info(f"Applying migration to version {version}")
                    migration_method()
                    self._set_version(version)
                else:
                    logger.warning(f"No migration method found for version {version}")

            logger.info("Database migrations completed successfully")
        elif current_version == SCHEMA_VERSION:
            logger.debug(f"Database schema is up to date (version {SCHEMA_VERSION})")
        else:
            logger.warning(
                f"Database schema version ({current_version}) is newer than expected ({SCHEMA_VERSION})"
            )

    # Migration methods for future schema changes
    # def _migrate_to_v2(self) -> None:
    #     """Migrate database schema from v1 to v2."""
    #     with sqlite3.connect(self.db_path) as conn:
    #         # Example: Add new column
    #         conn.execute("ALTER TABLE jobs ADD COLUMN new_field TEXT")
    #         conn.commit()

    def record_job(
        self,
        job: JobType,
        workflow_name: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """Record a job execution.

        Args:
            job: Job object to record
            workflow_name: Name of the workflow if part of a workflow
            metadata: Additional metadata to store
        """
        try:
            from srunx.models import Job

            with sqlite3.connect(self.db_path) as conn:
                command_str = None
                conda_env = None
                log_file = None

                if isinstance(job, Job):
                    command_str = (
                        job.command
                        if isinstance(job.command, str)
                        else " ".join(job.command or [])
                    )
                    conda_env = job.environment.conda
                    log_file = (
                        f"{job.log_dir}/{job.name}_{job.job_id}.log"
                        if job.job_id
                        else None
                    )

                conn.execute(
                    """
                    INSERT INTO jobs (
                        job_id, job_name, command, status,
                        nodes, gpus_per_node, cpus_per_task,
                        memory_per_node, time_limit, partition,
                        conda_env, submitted_at, workflow_name,
                        log_file, metadata
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        job.job_id,
                        job.name,
                        command_str,
                        job._status.value if hasattr(job, "_status") else "UNKNOWN",
                        getattr(getattr(job, "resources", None), "nodes", None) or None,
                        getattr(getattr(job, "resources", None), "gpus_per_node", None)
                        or None,
                        getattr(getattr(job, "resources", None), "cpus_per_task", None)
                        or None,
                        getattr(
                            getattr(job, "resources", None), "memory_per_node", None
                        )
                        or None,
                        getattr(getattr(job, "resources", None), "time_limit", None)
                        or None,
                        getattr(getattr(job, "resources", None), "partition", None)
                        or None,
                        conda_env,
                        datetime.now().isoformat(),
                        workflow_name,
                        log_file,
                        json.dumps(metadata) if metadata else None,
                    ),
                )
                conn.commit()

        except Exception as e:
            logger.warning(f"Failed to record job history: {e}")

        # Mirror into the new state DB so JobRepository stays the SSOT.
        _dual_write_record_submission(job, workflow_name)

    def update_job_completion(
        self, job_id: int, status: JobStatus, completed_at: datetime | None = None
    ) -> None:
        """Update job completion information.

        Args:
            job_id: SLURM job ID
            status: Final job status
            completed_at: Completion timestamp (defaults to now)
        """
        try:
            if completed_at is None:
                completed_at = datetime.now()

            with sqlite3.connect(self.db_path) as conn:
                # Get submitted_at time
                cursor = conn.execute(
                    "SELECT submitted_at FROM jobs WHERE job_id = ? ORDER BY id DESC LIMIT 1",
                    (job_id,),
                )
                row = cursor.fetchone()

                if row:
                    submitted_at = datetime.fromisoformat(row[0])
                    duration_seconds = (completed_at - submitted_at).total_seconds()

                    conn.execute(
                        """
                        UPDATE jobs
                        SET status = ?, completed_at = ?, duration_seconds = ?
                        WHERE job_id = ?
                        """,
                        (
                            status.value,
                            completed_at.isoformat(),
                            duration_seconds,
                            job_id,
                        ),
                    )
                    conn.commit()

        except Exception as e:
            logger.warning(f"Failed to update job completion: {e}")

        # Mirror terminal status into the new state DB.
        _dual_write_update_completion(job_id, status)

    def get_recent_jobs(self, limit: int = 100) -> list[dict[str, Any]]:
        """Get recent job executions.

        Args:
            limit: Maximum number of jobs to return

        Returns:
            List of job records
        """
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute(
                """
                SELECT * FROM jobs
                ORDER BY submitted_at DESC
                LIMIT ?
                """,
                (limit,),
            )
            return [dict(row) for row in cursor.fetchall()]

    def get_job_stats(
        self, from_date: str | None = None, to_date: str | None = None
    ) -> dict[str, Any]:
        """Get job statistics for a date range.

        Args:
            from_date: Start date (ISO format)
            to_date: End date (ISO format)

        Returns:
            Dictionary with job statistics
        """
        with sqlite3.connect(self.db_path) as conn:
            where_clause = ""
            params: list[Any] = []

            if from_date:
                where_clause += " WHERE submitted_at >= ?"
                params.append(from_date)

            if to_date:
                if where_clause:
                    where_clause += " AND submitted_at < ?"
                else:
                    where_clause += " WHERE submitted_at < ?"
                # Use next day to include the entire to_date day
                params.append(to_date + "T23:59:59" if "T" not in to_date else to_date)

            # Total jobs
            cursor = conn.execute(f"SELECT COUNT(*) FROM jobs{where_clause}", params)
            total_jobs = cursor.fetchone()[0]

            # Jobs by status
            cursor = conn.execute(
                f"SELECT status, COUNT(*) FROM jobs{where_clause} GROUP BY status",
                params,
            )
            jobs_by_status = dict(cursor.fetchall())

            # Average duration
            duration_filter = " AND duration_seconds IS NOT NULL"
            if where_clause:
                duration_query = f"SELECT AVG(duration_seconds) FROM jobs{where_clause}{duration_filter}"
            else:
                duration_query = "SELECT AVG(duration_seconds) FROM jobs WHERE duration_seconds IS NOT NULL"
            cursor = conn.execute(duration_query, params)
            avg_duration = cursor.fetchone()[0]

            # Total GPU hours (approximate)
            gpu_filter = (
                " AND duration_seconds IS NOT NULL AND gpus_per_node IS NOT NULL"
            )
            if where_clause:
                gpu_query = f"""
                    SELECT SUM(duration_seconds * gpus_per_node * nodes) / 3600.0
                    FROM jobs{where_clause}{gpu_filter}
                """
            else:
                gpu_query = """
                    SELECT SUM(duration_seconds * gpus_per_node * nodes) / 3600.0
                    FROM jobs WHERE duration_seconds IS NOT NULL AND gpus_per_node IS NOT NULL
                """
            cursor = conn.execute(gpu_query, params)
            total_gpu_hours = cursor.fetchone()[0] or 0

            return {
                "total_jobs": total_jobs,
                "jobs_by_status": jobs_by_status,
                "avg_duration_seconds": avg_duration,
                "total_gpu_hours": total_gpu_hours,
                "from_date": from_date,
                "to_date": to_date,
            }

    def get_workflow_stats(self, workflow_name: str) -> dict[str, Any]:
        """Get statistics for a specific workflow.

        Args:
            workflow_name: Name of the workflow

        Returns:
            Dictionary with workflow statistics
        """
        with sqlite3.connect(self.db_path) as conn:
            # Count all jobs in this workflow
            cursor = conn.execute(
                "SELECT COUNT(*), MIN(submitted_at), MAX(submitted_at) FROM jobs WHERE workflow_name = ?",
                (workflow_name,),
            )
            count_row = cursor.fetchone()

            # Average duration only for completed jobs
            cursor = conn.execute(
                "SELECT AVG(duration_seconds) FROM jobs WHERE workflow_name = ? AND duration_seconds IS NOT NULL",
                (workflow_name,),
            )
            avg_row = cursor.fetchone()

            return {
                "workflow_name": workflow_name,
                "total_jobs": count_row[0],
                "avg_duration_seconds": avg_row[0],
                "first_submitted": count_row[1],
                "last_submitted": count_row[2],
            }


# Global history instance
_history: JobHistory | None = None
_history_lock = __import__("threading").Lock()


def get_history(db_path: str | Path | None = None) -> JobHistory:
    """Get or create global job history instance.

    Args:
        db_path: Path to SQLite database file

    Returns:
        JobHistory instance
    """
    global _history
    if _history is None:
        with _history_lock:
            if _history is None:
                _history = JobHistory(db_path)
    return _history
