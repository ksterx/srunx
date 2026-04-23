"""Migration V5 tests — schema + data migration for CLI transport unification.

AC references (see ``specs/cli-transport-unification/spec.md``):
- AC-5.1: transport_type / profile_name / scheduler_key present on jobs.
- AC-5.2: scheduler_key NOT NULL for every row after migration.
- AC-5.3: legacy 2-segment target_ref forms are backfilled to 3-segment.
- AC-5.4: same job_id can coexist under different scheduler_keys.
- AC-5.5: workflow_run_jobs + job_state_transitions FK → jobs.id.
- AC-5.6: jobs.submission_source CHECK allowlist unchanged.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest
from srunx.db.connection import open_connection
from srunx.db.migrations import apply_migrations


def _migrated(tmp_path: Path) -> sqlite3.Connection:
    conn = open_connection(tmp_path / "v5.db")
    apply_migrations(conn)
    return conn


class TestJobsColumns:
    """AC-5.1 / AC-5.2 — transport columns exist and are NOT NULL."""

    def test_v5_adds_transport_columns(self, tmp_path: Path) -> None:
        conn = _migrated(tmp_path)
        try:
            cols = {row[1] for row in conn.execute("PRAGMA table_info(jobs)")}
        finally:
            conn.close()
        assert "transport_type" in cols
        assert "profile_name" in cols
        assert "scheduler_key" in cols

    def test_scheduler_key_not_null_after_insert(self, tmp_path: Path) -> None:
        conn = _migrated(tmp_path)
        try:
            conn.execute(
                """
                INSERT INTO jobs (
                    job_id, transport_type, profile_name, scheduler_key,
                    name, status, submitted_at, submission_source
                ) VALUES (?, 'local', NULL, 'local', 'x', 'PENDING',
                         '2026-04-22T00:00:00Z', 'cli')
                """,
                (1,),
            )
            conn.commit()
            null_count = conn.execute(
                "SELECT COUNT(*) FROM jobs WHERE scheduler_key IS NULL"
            ).fetchone()[0]
        finally:
            conn.close()
        assert null_count == 0


class TestCompositeUniqueness:
    """AC-5.4 — ``UNIQUE(scheduler_key, job_id)`` allows cross-transport duplicates."""

    def test_same_job_id_under_different_scheduler_keys(self, tmp_path: Path) -> None:
        conn = _migrated(tmp_path)
        try:
            conn.execute(
                """
                INSERT INTO jobs (
                    job_id, transport_type, profile_name, scheduler_key,
                    name, status, submitted_at, submission_source
                ) VALUES (?, 'local', NULL, 'local', 'a', 'PENDING',
                         '2026-04-22T00:00:00Z', 'cli')
                """,
                (12345,),
            )
            conn.execute(
                """
                INSERT INTO jobs (
                    job_id, transport_type, profile_name, scheduler_key,
                    name, status, submitted_at, submission_source
                ) VALUES (?, 'ssh', 'dgx', 'ssh:dgx', 'b', 'PENDING',
                         '2026-04-22T00:00:00Z', 'cli')
                """,
                (12345,),
            )
            conn.commit()
            count = conn.execute(
                "SELECT COUNT(*) FROM jobs WHERE job_id = 12345"
            ).fetchone()[0]
        finally:
            conn.close()
        assert count == 2

    def test_duplicate_job_id_same_scheduler_key_fails(self, tmp_path: Path) -> None:
        """Sanity: the UNIQUE still fires when the pair matches."""
        conn = _migrated(tmp_path)
        try:
            conn.execute(
                """
                INSERT INTO jobs (
                    job_id, transport_type, profile_name, scheduler_key,
                    name, status, submitted_at, submission_source
                ) VALUES (?, 'local', NULL, 'local', 'a', 'PENDING',
                         '2026-04-22T00:00:00Z', 'cli')
                """,
                (42,),
            )
            conn.commit()
            with pytest.raises(sqlite3.IntegrityError):
                conn.execute(
                    """
                    INSERT INTO jobs (
                        job_id, transport_type, profile_name, scheduler_key,
                        name, status, submitted_at, submission_source
                    ) VALUES (?, 'local', NULL, 'local', 'b', 'PENDING',
                             '2026-04-22T00:00:00Z', 'cli')
                    """,
                    (42,),
                )
                conn.commit()
        finally:
            conn.close()


class TestTripleCheckConstraint:
    """CHECK constraint guards the (transport_type, profile_name, scheduler_key) triple."""

    def test_local_with_profile_name_is_rejected(self, tmp_path: Path) -> None:
        conn = _migrated(tmp_path)
        try:
            with pytest.raises(sqlite3.IntegrityError):
                conn.execute(
                    """
                    INSERT INTO jobs (
                        job_id, transport_type, profile_name, scheduler_key,
                        name, status, submitted_at, submission_source
                    ) VALUES (?, 'local', 'dgx', 'local', 'x', 'PENDING',
                             '2026-04-22T00:00:00Z', 'cli')
                    """,
                    (1,),
                )
                conn.commit()
        finally:
            conn.close()

    def test_ssh_without_profile_name_is_rejected(self, tmp_path: Path) -> None:
        conn = _migrated(tmp_path)
        try:
            with pytest.raises(sqlite3.IntegrityError):
                conn.execute(
                    """
                    INSERT INTO jobs (
                        job_id, transport_type, profile_name, scheduler_key,
                        name, status, submitted_at, submission_source
                    ) VALUES (?, 'ssh', NULL, 'ssh:', 'x', 'PENDING',
                             '2026-04-22T00:00:00Z', 'cli')
                    """,
                    (1,),
                )
                conn.commit()
        finally:
            conn.close()

    def test_profile_name_with_colon_is_rejected(self, tmp_path: Path) -> None:
        """``profile_name`` must not contain ``:`` (scheduler_key parser relies on this)."""
        conn = _migrated(tmp_path)
        try:
            with pytest.raises(sqlite3.IntegrityError):
                conn.execute(
                    """
                    INSERT INTO jobs (
                        job_id, transport_type, profile_name, scheduler_key,
                        name, status, submitted_at, submission_source
                    ) VALUES (?, 'ssh', 'bad:name', 'ssh:bad:name', 'x',
                             'PENDING', '2026-04-22T00:00:00Z', 'cli')
                    """,
                    (1,),
                )
                conn.commit()
        finally:
            conn.close()


class TestForeignKeyRetarget:
    """AC-5.5 — workflow_run_jobs / job_state_transitions FK → jobs.id."""

    def test_workflow_run_jobs_fk_points_at_jobs_id(self, tmp_path: Path) -> None:
        conn = _migrated(tmp_path)
        try:
            fks = conn.execute("PRAGMA foreign_key_list(workflow_run_jobs)").fetchall()
        finally:
            conn.close()
        jobs_fks = [fk for fk in fks if fk[2] == "jobs"]
        assert jobs_fks, f"no FK to jobs found in {fks}"
        # fk tuple: (id, seq, table, from, to, on_update, on_delete, match)
        to_cols = {fk[4] for fk in jobs_fks}
        assert "id" in to_cols, f"expected FK → jobs.id, got {to_cols}"

    def test_job_state_transitions_fk_points_at_jobs_id(self, tmp_path: Path) -> None:
        conn = _migrated(tmp_path)
        try:
            fks = conn.execute(
                "PRAGMA foreign_key_list(job_state_transitions)"
            ).fetchall()
        finally:
            conn.close()
        jobs_fks = [fk for fk in fks if fk[2] == "jobs"]
        assert jobs_fks, f"no FK to jobs found in {fks}"
        to_cols = {fk[4] for fk in jobs_fks}
        assert "id" in to_cols, f"expected FK → jobs.id, got {to_cols}"

    def test_workflow_run_jobs_has_jobs_row_id_column(self, tmp_path: Path) -> None:
        conn = _migrated(tmp_path)
        try:
            cols = {
                row[1] for row in conn.execute("PRAGMA table_info(workflow_run_jobs)")
            }
        finally:
            conn.close()
        assert "jobs_row_id" in cols
        assert "job_id" not in cols  # old column retired

    def test_job_state_transitions_has_jobs_row_id_column(self, tmp_path: Path) -> None:
        conn = _migrated(tmp_path)
        try:
            cols = {
                row[1]
                for row in conn.execute("PRAGMA table_info(job_state_transitions)")
            }
        finally:
            conn.close()
        assert "jobs_row_id" in cols
        assert "job_id" not in cols


class TestSubmissionSourcePreserved:
    """AC-5.6 — submission_source CHECK allowlist untouched."""

    def test_allowed_values_present_in_table_sql(self, tmp_path: Path) -> None:
        conn = _migrated(tmp_path)
        try:
            sql = conn.execute(
                "SELECT sql FROM sqlite_master WHERE name = 'jobs'"
            ).fetchone()[0]
        finally:
            conn.close()
        for value in ("cli", "web", "workflow"):
            assert f"'{value}'" in sql, (
                f"expected submission_source allowlist to include {value!r}; "
                f"sql=\n{sql}"
            )


class TestTargetRefBackfill:
    """AC-5.3 — legacy ``job:<N>`` refs are backfilled to ``job:local:<N>``."""

    def test_pre_v5_legacy_refs_are_rewritten(self, tmp_path: Path) -> None:
        """Apply v1..v4, seed legacy 2-segment rows, then apply V5 and check."""
        from srunx.db.migrations import (
            MIGRATIONS,
            _apply_fk_off_migration,
            _apply_tx_migration,
        )

        conn = open_connection(tmp_path / "pre_v5.db")
        try:
            for mig in MIGRATIONS[:-1]:  # everything except V5
                if mig.requires_fk_off:
                    _apply_fk_off_migration(conn, mig)
                else:
                    _apply_tx_migration(conn, mig)

            # Seed legacy-form watch and event rows, representative of
            # what a pre-V5 deployment would have written.
            conn.execute(
                """
                INSERT INTO watches (kind, target_ref, created_at)
                VALUES ('job', 'job:111', '2026-04-22T00:00:00Z')
                """
            )
            conn.execute(
                """
                INSERT INTO events
                    (kind, source_ref, payload, payload_hash, observed_at)
                VALUES (
                    'job.status_changed', 'job:111', '{}', 'hash-1',
                    '2026-04-22T00:00:00Z'
                )
                """
            )
            conn.commit()

            v5 = MIGRATIONS[-1]
            assert v5.name == "v5_transport_scheduler_key"
            _apply_fk_off_migration(conn, v5)

            # After V5 the legacy ``job:<N>`` form should be completely
            # gone; every row uses the 3-segment form.
            bad_watches = conn.execute(
                """
                SELECT COUNT(*) FROM watches
                WHERE kind = 'job'
                  AND target_ref NOT LIKE 'job:local:%'
                  AND target_ref NOT LIKE 'job:ssh:%'
                """
            ).fetchone()[0]
            bad_events = conn.execute(
                """
                SELECT COUNT(*) FROM events
                WHERE kind IN ('job.submitted','job.status_changed')
                  AND source_ref NOT LIKE 'job:local:%'
                  AND source_ref NOT LIKE 'job:ssh:%'
                """
            ).fetchone()[0]
            backfilled_watch = conn.execute(
                "SELECT target_ref FROM watches WHERE kind = 'job'"
            ).fetchone()
            backfilled_event = conn.execute(
                "SELECT source_ref FROM events WHERE kind = 'job.status_changed'"
            ).fetchone()
        finally:
            conn.close()

        assert bad_watches == 0, "legacy 2-segment target_ref survived V5"
        assert bad_events == 0, "legacy 2-segment source_ref survived V5"
        assert backfilled_watch[0] == "job:local:111"
        assert backfilled_event[0] == "job:local:111"


class TestOpenWatchForceClose:
    """V5 migration force-closes only ``kind='job'`` open watches.

    Pre-V5 WebUI-submitted SSH jobs are backfilled as
    ``transport_type='local'`` because the schema had no column to tell
    them apart. Their open job watches would drive the poller to query
    local SLURM for remote job ids, leading to false terminal
    transitions. The migration closes all open **job** watches so the
    user can re-open the ones that still matter.

    Non-job watches (``workflow_run`` / ``sweep_run`` /
    ``resource_threshold`` / ``scheduled_report``) are transport-
    agnostic and therefore preserved across migration — closing them
    would silently break in-flight workflow cancellations, sweep
    aggregations, and scheduled reports.
    """

    def test_open_job_watches_force_closed_by_migration(self, tmp_path: Path) -> None:
        # Reach into the migration machinery: open the DB, apply only
        # up through V4, insert an open watch, then apply V5 and check.
        from srunx.db.migrations import (
            MIGRATIONS,
            _apply_fk_off_migration,
            _apply_tx_migration,
        )

        conn = open_connection(tmp_path / "pre_v5.db")
        try:
            for mig in MIGRATIONS[:-1]:  # everything except V5
                if mig.requires_fk_off:
                    _apply_fk_off_migration(conn, mig)
                else:
                    _apply_tx_migration(conn, mig)

            # Seed an open watch (closed_at IS NULL).
            conn.execute(
                """
                INSERT INTO watches (kind, target_ref, created_at)
                VALUES ('job', 'job:555', '2026-04-22T00:00:00Z')
                """
            )
            conn.commit()
            open_before = conn.execute(
                "SELECT COUNT(*) FROM watches WHERE closed_at IS NULL"
            ).fetchone()[0]
            assert open_before == 1

            # Now apply V5.
            v5 = MIGRATIONS[-1]
            assert v5.name == "v5_transport_scheduler_key"
            _apply_fk_off_migration(conn, v5)

            open_after = conn.execute(
                "SELECT COUNT(*) FROM watches WHERE closed_at IS NULL AND kind = 'job'"
            ).fetchone()[0]
            backfilled = conn.execute(
                "SELECT target_ref FROM watches WHERE kind = 'job'"
            ).fetchall()
        finally:
            conn.close()
        assert open_after == 0, "V5 migration must close every open job watch"
        # And the target_ref was backfilled to 3-segment form.
        assert all(r[0] == "job:local:555" for r in backfilled), backfilled


class TestDataBackfill:
    """Pre-V5 data is preserved and labelled as local transport."""

    def test_existing_jobs_rows_backfilled_as_local(self, tmp_path: Path) -> None:
        from srunx.db.migrations import (
            MIGRATIONS,
            _apply_fk_off_migration,
            _apply_tx_migration,
        )

        conn = open_connection(tmp_path / "pre_v5.db")
        try:
            for mig in MIGRATIONS[:-1]:
                if mig.requires_fk_off:
                    _apply_fk_off_migration(conn, mig)
                else:
                    _apply_tx_migration(conn, mig)

            conn.execute(
                """
                INSERT INTO jobs (
                    job_id, name, status, submitted_at, submission_source
                ) VALUES (?, 'legacy', 'COMPLETED',
                         '2026-04-22T00:00:00Z', 'cli')
                """,
                (777,),
            )
            conn.commit()

            v5 = MIGRATIONS[-1]
            _apply_fk_off_migration(conn, v5)

            row = conn.execute(
                "SELECT job_id, transport_type, profile_name, scheduler_key "
                "FROM jobs WHERE job_id = 777"
            ).fetchone()
        finally:
            conn.close()

        assert row is not None
        assert row["transport_type"] == "local"
        assert row["profile_name"] is None
        assert row["scheduler_key"] == "local"
