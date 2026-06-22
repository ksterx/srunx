"""Tests for the V6 migration: widen jobs.submission_source to admit 'mcp'."""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from srunx.observability.storage.connection import open_connection
from srunx.observability.storage.migrations import (
    MIGRATIONS,
    _apply_fk_off_migration,
    _apply_tx_migration,
)


def _apply(conn: sqlite3.Connection, max_version: int) -> None:
    for mig in [m for m in MIGRATIONS if m.version <= max_version]:
        if mig.requires_fk_off:
            _apply_fk_off_migration(conn, mig)
        else:
            _apply_tx_migration(conn, mig)


class TestV6WidensSubmissionSource:
    def test_v6_is_registered_last(self):
        v6 = next(m for m in MIGRATIONS if m.version == 6)
        assert v6.name == "v6_widen_submission_source_mcp"
        assert v6.requires_fk_off is True
        assert max(m.version for m in MIGRATIONS) == 6

    def test_mcp_submission_source_accepted_after_v6(self, tmp_path: Path):
        conn = open_connection(tmp_path / "v6.db")
        try:
            _apply(conn, 6)
            conn.execute(
                """
                INSERT INTO jobs (job_id, name, status, submitted_at, submission_source)
                VALUES (?, 'mcp-job', 'PENDING', '2026-06-22T00:00:00Z', 'mcp')
                """,
                (101,),
            )
            conn.commit()
            row = conn.execute(
                "SELECT submission_source FROM jobs WHERE job_id = 101"
            ).fetchone()
            assert row[0] == "mcp"
        finally:
            conn.close()

    def test_bogus_submission_source_still_rejected(self, tmp_path: Path):
        conn = open_connection(tmp_path / "v6_bad.db")
        try:
            _apply(conn, 6)
            with pytest.raises(sqlite3.IntegrityError):
                conn.execute(
                    """
                    INSERT INTO jobs (job_id, name, status, submitted_at, submission_source)
                    VALUES (?, 'x', 'PENDING', '2026-06-22T00:00:00Z', 'bogus')
                    """,
                    (102,),
                )
            conn.commit()
        finally:
            conn.close()

    def test_v6_rejected_mcp_before_upgrade(self, tmp_path: Path):
        """Sanity: V5 schema rejects 'mcp' (so V6 is doing real work)."""
        conn = open_connection(tmp_path / "v5_only.db")
        try:
            _apply(conn, 5)
            with pytest.raises(sqlite3.IntegrityError):
                conn.execute(
                    """
                    INSERT INTO jobs (job_id, name, status, submitted_at, submission_source)
                    VALUES (?, 'x', 'PENDING', '2026-06-22T00:00:00Z', 'mcp')
                    """,
                    (103,),
                )
            conn.commit()
        finally:
            conn.close()

    def test_v6_preserves_ids_and_fk(self, tmp_path: Path):
        """V5 rows (id + transport columns) survive the V6 rebuild; FK intact."""
        conn = open_connection(tmp_path / "v6_fk.db")
        try:
            _apply(conn, 5)
            # Seed a job + a workflow_run + a workflow_run_jobs FK row.
            conn.execute(
                """
                INSERT INTO workflow_runs (workflow_name, status, started_at, triggered_by)
                VALUES ('wf', 'running', '2026-06-22T00:00:00Z', 'cli')
                """
            )
            wr_id = conn.execute("SELECT id FROM workflow_runs").fetchone()[0]
            conn.execute(
                """
                INSERT INTO jobs (
                    job_id, transport_type, profile_name, scheduler_key,
                    name, status, submitted_at, submission_source, workflow_run_id
                ) VALUES (
                    777, 'ssh', 'dgx', 'ssh:dgx',
                    'seed', 'COMPLETED', '2026-06-22T00:00:00Z', 'workflow', ?
                )
                """,
                (wr_id,),
            )
            jobs_row_id = conn.execute("SELECT id FROM jobs").fetchone()[0]
            conn.execute(
                """
                INSERT INTO workflow_run_jobs (workflow_run_id, jobs_row_id, job_name)
                VALUES (?, ?, 'seed')
                """,
                (wr_id, jobs_row_id),
            )
            conn.commit()

            _apply(conn, 6)

            # id preserved, transport columns carried forward (not force-reset).
            row = conn.execute(
                "SELECT id, transport_type, profile_name, scheduler_key "
                "FROM jobs WHERE job_id = 777"
            ).fetchone()
            assert row[0] == jobs_row_id
            assert row[1] == "ssh"
            assert row[2] == "dgx"
            assert row[3] == "ssh:dgx"

            # FK still resolves the renamed table.
            conn.execute("PRAGMA foreign_key_check")
            wrj = conn.execute(
                "SELECT jobs_row_id FROM workflow_run_jobs WHERE job_name = 'seed'"
            ).fetchone()
            assert wrj[0] == jobs_row_id

            # Indexes recreated.
            idx = {
                r[0]
                for r in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='index' "
                    "AND tbl_name='jobs'"
                ).fetchall()
            }
            assert "idx_jobs_scheduler_key" in idx
            assert "idx_jobs_status" in idx
        finally:
            conn.close()
