"""Tests for srunx.history module."""

import sqlite3
from datetime import datetime, timedelta

import pytest

from srunx.history import JobHistory
from srunx.models import Job, JobEnvironment, JobResource, JobStatus, ShellJob


@pytest.fixture
def history(tmp_path, monkeypatch):
    """Create a JobHistory backed by a temp SQLite file.

    Also isolates ``XDG_CONFIG_HOME`` so the dual-write mirror into the
    new srunx state DB does not pollute the user's real
    ``~/.config/srunx/srunx.db``.
    """
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / "xdg"))
    return JobHistory(db_path=tmp_path / "test_history.db")


# ---------------------------------------------------------------------------
# __init__
# ---------------------------------------------------------------------------


class TestJobHistoryInit:
    def test_creates_database_file(self, tmp_path):
        db_path = tmp_path / "subdir" / "history.db"
        JobHistory(db_path=db_path)
        assert db_path.exists()

    def test_creates_parent_directories(self, tmp_path):
        db_path = tmp_path / "a" / "b" / "c" / "history.db"
        JobHistory(db_path=db_path)
        assert db_path.parent.is_dir()

    def test_initializes_schema_tables(self, tmp_path):
        db_path = tmp_path / "history.db"
        JobHistory(db_path=db_path)
        with sqlite3.connect(db_path) as conn:
            cursor = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
            )
            tables = {row[0] for row in cursor.fetchall()}
        assert "jobs" in tables
        assert "schema_version" in tables

    def test_initializes_indexes(self, tmp_path):
        db_path = tmp_path / "history.db"
        JobHistory(db_path=db_path)
        with sqlite3.connect(db_path) as conn:
            cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='index'")
            indexes = {row[0] for row in cursor.fetchall()}
        assert "idx_job_id" in indexes
        assert "idx_submitted_at" in indexes
        assert "idx_status" in indexes

    def test_sets_schema_version(self, tmp_path):
        db_path = tmp_path / "history.db"
        h = JobHistory(db_path=db_path)
        assert h._get_current_version() == 1

    def test_idempotent_init(self, tmp_path):
        """Calling __init__ twice on the same DB does not raise."""
        db_path = tmp_path / "history.db"
        JobHistory(db_path=db_path)
        h2 = JobHistory(db_path=db_path)
        assert h2._get_current_version() == 1


# ---------------------------------------------------------------------------
# record_job
# ---------------------------------------------------------------------------


def _make_job(*, job_id: int | None = 100, name: str = "test_job") -> Job:
    return Job(
        name=name,
        job_id=job_id,
        command=["python", "train.py"],
        resources=JobResource(
            nodes=2,
            gpus_per_node=4,
            cpus_per_task=8,
            memory_per_node="64GB",
            time_limit="4:00:00",
            partition="gpu",
        ),
        environment=JobEnvironment(conda="ml_env"),
        log_dir="logs",
        work_dir="/tmp",
    )


def _make_shell_job(*, job_id: int | None = 200, name: str = "shell_job") -> ShellJob:
    return ShellJob(
        name=name,
        job_id=job_id,
        script_path="/scripts/run.sh",
        script_vars={"EPOCHS": 10},
    )


class TestRecordJob:
    def test_record_basic_job(self, history):
        job = _make_job()
        history.record_job(job)
        rows = history.get_recent_jobs()
        assert len(rows) == 1
        row = rows[0]
        assert row["job_id"] == 100
        assert row["job_name"] == "test_job"
        assert row["command"] == "python train.py"
        assert row["nodes"] == 2
        assert row["gpus_per_node"] == 4
        assert row["cpus_per_task"] == 8
        assert row["memory_per_node"] == "64GB"
        assert row["time_limit"] == "4:00:00"
        assert row["partition"] == "gpu"
        assert row["conda_env"] == "ml_env"
        assert row["status"] == "PENDING"

    def test_record_job_with_string_command(self, history):
        job = Job(
            name="str_cmd",
            job_id=101,
            command="echo hello",
            resources=JobResource(),
            environment=JobEnvironment(),
            log_dir="logs",
            work_dir="/tmp",
        )
        history.record_job(job)
        rows = history.get_recent_jobs()
        assert rows[0]["command"] == "echo hello"

    def test_record_shell_job(self, history):
        """ShellJob has no command/conda; those columns should be None."""
        sjob = _make_shell_job()
        history.record_job(sjob)
        rows = history.get_recent_jobs()
        assert len(rows) == 1
        row = rows[0]
        assert row["job_id"] == 200
        assert row["job_name"] == "shell_job"
        assert row["command"] is None
        assert row["conda_env"] is None

    def test_record_job_with_workflow_name(self, history):
        job = _make_job()
        history.record_job(job, workflow_name="ml_pipeline")
        rows = history.get_recent_jobs()
        assert rows[0]["workflow_name"] == "ml_pipeline"

    def test_record_job_with_metadata(self, history):
        import json

        job = _make_job()
        meta = {"experiment": "v1", "seed": 42}
        history.record_job(job, metadata=meta)
        rows = history.get_recent_jobs()
        assert json.loads(rows[0]["metadata"]) == meta

    def test_record_job_no_metadata_stores_none(self, history):
        job = _make_job()
        history.record_job(job)
        rows = history.get_recent_jobs()
        assert rows[0]["metadata"] is None

    def test_record_job_log_file_populated_for_job_with_id(self, history):
        job = _make_job(job_id=42)
        history.record_job(job)
        rows = history.get_recent_jobs()
        assert rows[0]["log_file"] == "logs/test_job_42.log"

    def test_record_job_none_job_id_silently_fails(self, history):
        """Jobs without job_id fail the NOT NULL constraint and are skipped."""
        job = _make_job(job_id=None)
        history.record_job(job)  # should not raise
        rows = history.get_recent_jobs()
        assert len(rows) == 0  # not inserted due to NOT NULL constraint

    def test_record_multiple_jobs(self, history):
        for i in range(5):
            history.record_job(_make_job(job_id=i, name=f"job_{i}"))
        rows = history.get_recent_jobs()
        assert len(rows) == 5


# ---------------------------------------------------------------------------
# update_job_completion
# ---------------------------------------------------------------------------


class TestUpdateJobCompletion:
    def test_updates_status_and_duration(self, history):
        job = _make_job(job_id=300)
        history.record_job(job)

        # Wait-free: provide an explicit completed_at a known distance from submitted_at
        rows_before = history.get_recent_jobs()
        submitted_at = datetime.fromisoformat(rows_before[0]["submitted_at"])
        completed_at = submitted_at + timedelta(seconds=120)

        history.update_job_completion(
            300, JobStatus.COMPLETED, completed_at=completed_at
        )

        rows = history.get_recent_jobs()
        assert rows[0]["status"] == "COMPLETED"
        assert rows[0]["completed_at"] is not None
        assert rows[0]["duration_seconds"] == pytest.approx(120.0)

    def test_updates_failed_status(self, history):
        job = _make_job(job_id=301)
        history.record_job(job)
        rows = history.get_recent_jobs()
        submitted_at = datetime.fromisoformat(rows[0]["submitted_at"])
        history.update_job_completion(
            301, JobStatus.FAILED, completed_at=submitted_at + timedelta(seconds=10)
        )
        rows = history.get_recent_jobs()
        assert rows[0]["status"] == "FAILED"

    def test_defaults_completed_at_to_now(self, history):
        job = _make_job(job_id=302)
        history.record_job(job)
        before = datetime.now()
        history.update_job_completion(302, JobStatus.COMPLETED)
        after = datetime.now()
        rows = history.get_recent_jobs()
        completed = datetime.fromisoformat(rows[0]["completed_at"])
        assert before <= completed <= after

    def test_nonexistent_job_id_is_noop(self, history):
        """Updating a job_id not in DB should not raise."""
        history.update_job_completion(99999, JobStatus.COMPLETED)
        # No rows affected, no error
        rows = history.get_recent_jobs()
        assert len(rows) == 0


# ---------------------------------------------------------------------------
# get_recent_jobs
# ---------------------------------------------------------------------------


class TestGetRecentJobs:
    def test_empty_database_returns_empty_list(self, history):
        assert history.get_recent_jobs() == []

    def test_returns_ordered_by_submitted_at_desc(self, history):
        for i in range(3):
            history.record_job(_make_job(job_id=i, name=f"job_{i}"))
        rows = history.get_recent_jobs()
        timestamps = [row["submitted_at"] for row in rows]
        assert timestamps == sorted(timestamps, reverse=True)

    def test_limit_parameter(self, history):
        for i in range(10):
            history.record_job(_make_job(job_id=i, name=f"job_{i}"))
        rows = history.get_recent_jobs(limit=3)
        assert len(rows) == 3

    def test_limit_larger_than_data(self, history):
        history.record_job(_make_job())
        rows = history.get_recent_jobs(limit=100)
        assert len(rows) == 1

    def test_rows_are_dicts(self, history):
        history.record_job(_make_job())
        rows = history.get_recent_jobs()
        assert isinstance(rows[0], dict)
        assert "job_id" in rows[0]


# ---------------------------------------------------------------------------
# get_job_stats
# ---------------------------------------------------------------------------


class TestGetJobStats:
    def test_empty_database_stats(self, history):
        stats = history.get_job_stats()
        assert stats["total_jobs"] == 0
        assert stats["jobs_by_status"] == {}
        assert stats["avg_duration_seconds"] is None
        assert stats["total_gpu_hours"] == 0

    def test_aggregates_by_status(self, history):
        # Record 3 jobs, update 2 to different terminal states
        for i in range(3):
            history.record_job(_make_job(job_id=i, name=f"j{i}"))

        rows = history.get_recent_jobs()
        sub0 = datetime.fromisoformat(rows[2]["submitted_at"])
        sub1 = datetime.fromisoformat(rows[1]["submitted_at"])

        history.update_job_completion(
            0, JobStatus.COMPLETED, completed_at=sub0 + timedelta(seconds=60)
        )
        history.update_job_completion(
            1, JobStatus.FAILED, completed_at=sub1 + timedelta(seconds=30)
        )

        stats = history.get_job_stats()
        assert stats["total_jobs"] == 3
        assert stats["jobs_by_status"]["COMPLETED"] == 1
        assert stats["jobs_by_status"]["FAILED"] == 1
        assert stats["jobs_by_status"]["PENDING"] == 1

    def test_avg_duration(self, history):
        for i in range(2):
            history.record_job(_make_job(job_id=i, name=f"j{i}"))

        rows = history.get_recent_jobs()
        sub0 = datetime.fromisoformat(rows[1]["submitted_at"])
        sub1 = datetime.fromisoformat(rows[0]["submitted_at"])

        history.update_job_completion(
            0, JobStatus.COMPLETED, completed_at=sub0 + timedelta(seconds=100)
        )
        history.update_job_completion(
            1, JobStatus.COMPLETED, completed_at=sub1 + timedelta(seconds=200)
        )

        stats = history.get_job_stats()
        assert stats["avg_duration_seconds"] == pytest.approx(150.0)

    def test_gpu_hours_calculation(self, history):
        # Job with 2 nodes, 4 GPUs per node, duration 3600s -> 8 GPU-hours
        job = _make_job(job_id=500)
        history.record_job(job)
        rows = history.get_recent_jobs()
        sub = datetime.fromisoformat(rows[0]["submitted_at"])
        history.update_job_completion(
            500, JobStatus.COMPLETED, completed_at=sub + timedelta(seconds=3600)
        )
        stats = history.get_job_stats()
        # 3600 * 4 * 2 / 3600 = 8.0
        assert stats["total_gpu_hours"] == pytest.approx(8.0)

    def test_date_filtering_from_date(self, history):
        job = _make_job(job_id=600)
        history.record_job(job)
        # Use a future date so nothing matches
        stats = history.get_job_stats(from_date="2099-01-01")
        assert stats["total_jobs"] == 0

    def test_date_filtering_to_date(self, history):
        job = _make_job(job_id=601)
        history.record_job(job)
        # Use a past date so nothing matches
        stats = history.get_job_stats(to_date="2000-01-01")
        assert stats["total_jobs"] == 0

    def test_stats_none_values_for_no_durations(self, history):
        """Jobs with no completion have None avg_duration."""
        history.record_job(_make_job(job_id=700))
        stats = history.get_job_stats()
        assert stats["avg_duration_seconds"] is None
        assert stats["total_gpu_hours"] == 0


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


class TestEdgeCases:
    def test_job_with_none_job_id_is_skipped(self, history):
        """Jobs without job_id hit NOT NULL constraint and are silently skipped."""
        job = _make_job(job_id=None)
        history.record_job(job)  # should not raise
        rows = history.get_recent_jobs()
        assert len(rows) == 0

    def test_shell_job_has_no_resources(self, history):
        """ShellJob lacks resources attribute; columns should be None."""
        sjob = _make_shell_job()
        history.record_job(sjob)
        row = history.get_recent_jobs()[0]
        assert row["nodes"] is None
        assert row["gpus_per_node"] is None
        assert row["cpus_per_task"] is None
        assert row["memory_per_node"] is None
        assert row["time_limit"] is None
        assert row["partition"] is None

    def test_concurrent_init_same_path(self, tmp_path):
        """Two instances on the same DB file should not conflict."""
        db_path = tmp_path / "shared.db"
        h1 = JobHistory(db_path=db_path)
        h2 = JobHistory(db_path=db_path)
        h1.record_job(_make_job(job_id=1, name="from_h1"))
        h2.record_job(_make_job(job_id=2, name="from_h2"))
        assert len(h1.get_recent_jobs()) == 2
        assert len(h2.get_recent_jobs()) == 2

    def test_get_recent_jobs_limit_zero(self, history):
        history.record_job(_make_job())
        rows = history.get_recent_jobs(limit=0)
        assert rows == []


# ---------------------------------------------------------------------------
# Dual-write mirror into the new srunx state DB
# ---------------------------------------------------------------------------


class TestDualWriteToNewDb:
    """Verify legacy :class:`JobHistory` writes also populate the new DB.

    The dual-write path is best-effort and silent on failure, so these
    tests assert the happy-path only. The ``history`` fixture already
    isolates ``XDG_CONFIG_HOME`` to a tmp dir, so the mirror lands in a
    per-test ``srunx.db`` under that tmp dir.
    """

    def test_record_job_mirrors_to_new_jobs_table(self, history):
        from srunx.db.connection import open_connection
        from srunx.db.repositories.jobs import JobRepository

        history.record_job(_make_job(job_id=42, name="mirror-me"))

        conn = open_connection()
        try:
            row = JobRepository(conn).get(42)
        finally:
            conn.close()

        assert row is not None
        assert row.name == "mirror-me"
        assert row.submission_source == "cli"
        assert row.status == "PENDING"

    def test_record_job_with_workflow_sets_source_to_workflow(self, history):
        from srunx.db.connection import open_connection
        from srunx.db.repositories.jobs import JobRepository

        history.record_job(
            _make_job(job_id=43, name="wf-child"),
            workflow_name="ml_pipeline",
        )

        conn = open_connection()
        try:
            row = JobRepository(conn).get(43)
        finally:
            conn.close()

        assert row is not None
        assert row.submission_source == "workflow"

    def test_update_job_completion_mirrors_terminal_state(self, history):
        from srunx.db.connection import open_connection
        from srunx.db.repositories.job_state_transitions import (
            JobStateTransitionRepository,
        )
        from srunx.db.repositories.jobs import JobRepository

        history.record_job(_make_job(job_id=44, name="to-complete"))
        history.update_job_completion(44, JobStatus.COMPLETED)

        conn = open_connection()
        try:
            row = JobRepository(conn).get(44)
            latest = JobStateTransitionRepository(conn).latest_for_job(44)
        finally:
            conn.close()

        assert row is not None
        assert row.status == "COMPLETED"
        assert latest is not None
        assert latest.to_status == "COMPLETED"
        assert latest.source == "cli_monitor"

    def test_update_completion_skips_unknown_job_in_new_db(self, history):
        """Update on a job that only exists in legacy DB is a silent no-op."""
        from srunx.db.connection import open_connection
        from srunx.db.repositories.jobs import JobRepository

        # Directly write to legacy DB only — bypass dual-write for setup.
        with sqlite3.connect(history.db_path) as conn:
            conn.execute(
                """
                INSERT INTO jobs (job_id, job_name, status, submitted_at)
                VALUES (?, ?, ?, ?)
                """,
                (999, "only-legacy", "PENDING", datetime.now().isoformat()),
            )
            conn.commit()

        history.update_job_completion(999, JobStatus.COMPLETED)

        conn = open_connection()
        try:
            row = JobRepository(conn).get(999)
        finally:
            conn.close()
        # No mirror row was created since the job wasn't in the new DB.
        assert row is None
