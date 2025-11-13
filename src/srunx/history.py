"""Job execution history tracking with SQLite."""

import json
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any

from srunx.logging import get_logger
from srunx.models import JobStatus, JobType

logger = get_logger(__name__)

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
                cursor = conn.execute(
                    "SELECT MAX(version) FROM schema_version"
                )
                result = cursor.fetchone()
                version = result[0] if result and result[0] else 0

                # Handle backward compatibility: if version is 0 but jobs table exists,
                # assume schema v1 and set it
                if version == 0:
                    cursor = conn.execute(
                        "SELECT name FROM sqlite_master WHERE type='table' AND name='jobs'"
                    )
                    if cursor.fetchone():
                        logger.info("Detected existing database without version, setting to v1")
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
                        job.status.value if hasattr(job, "status") else "UNKNOWN",
                        getattr(
                            getattr(job, "resources", None), "nodes", None
                        ) or None,
                        getattr(
                            getattr(job, "resources", None), "gpus_per_node", None
                        ) or None,
                        getattr(
                            getattr(job, "resources", None), "cpus_per_task", None
                        ) or None,
                        getattr(
                            getattr(job, "resources", None), "memory_per_node", None
                        ) or None,
                        getattr(
                            getattr(job, "resources", None), "time_limit", None
                        ) or None,
                        getattr(
                            getattr(job, "resources", None), "partition", None
                        ) or None,
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
                    where_clause += " AND submitted_at <= ?"
                else:
                    where_clause += " WHERE submitted_at <= ?"
                params.append(to_date)

            # Total jobs
            cursor = conn.execute(
                f"SELECT COUNT(*) FROM jobs{where_clause}", params
            )
            total_jobs = cursor.fetchone()[0]

            # Jobs by status
            cursor = conn.execute(
                f"SELECT status, COUNT(*) FROM jobs{where_clause} GROUP BY status",
                params,
            )
            jobs_by_status = dict(cursor.fetchall())

            # Average duration
            cursor = conn.execute(
                f"SELECT AVG(duration_seconds) FROM jobs{where_clause} WHERE duration_seconds IS NOT NULL",
                params,
            )
            avg_duration = cursor.fetchone()[0]

            # Total GPU hours (approximate)
            cursor = conn.execute(
                f"""
                SELECT SUM(duration_seconds * gpus_per_node * nodes) / 3600.0
                FROM jobs{where_clause}
                WHERE duration_seconds IS NOT NULL AND gpus_per_node IS NOT NULL
                """,
                params,
            )
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
            cursor = conn.execute(
                """
                SELECT COUNT(*), AVG(duration_seconds), MIN(submitted_at), MAX(submitted_at)
                FROM jobs
                WHERE workflow_name = ? AND duration_seconds IS NOT NULL
                """,
                (workflow_name,),
            )
            row = cursor.fetchone()

            return {
                "workflow_name": workflow_name,
                "total_executions": row[0],
                "avg_duration_seconds": row[1],
                "first_execution": row[2],
                "last_execution": row[3],
            }


# Global history instance
_history: JobHistory | None = None


def get_history(db_path: str | Path | None = None) -> JobHistory:
    """Get or create global job history instance.

    Args:
        db_path: Path to SQLite database file

    Returns:
        JobHistory instance
    """
    global _history
    if _history is None:
        _history = JobHistory(db_path)
    return _history
