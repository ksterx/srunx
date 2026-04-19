"""Tests for ``srunx.db.cli_helpers`` — the CLI-side state-DB bridge.

Focused on the workflow-identity round-trip introduced to fix the
``srunx report --workflow`` regression: CLI-launched workflows must
create a ``workflow_runs`` row and link submitted jobs back via
``workflow_run_id`` so ``compute_workflow_stats`` (which JOINs on
``workflow_run_id``) actually picks them up.
"""

from __future__ import annotations

import pytest

from srunx.db.cli_helpers import (
    compute_workflow_stats,
    create_cli_workflow_run,
    mark_workflow_run_status,
    record_submission_from_job,
)
from srunx.models import Job, JobEnvironment, JobResource


@pytest.fixture
def _isolated_db(tmp_path, monkeypatch):
    """Redirect the state DB to a per-test tmp dir."""
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path))
    # Clear any cached config so the fresh XDG_CONFIG_HOME applies.
    import srunx.config

    srunx.config._config = None
    yield
    srunx.config._config = None


def _make_job(name: str, job_id: int, gpus: int = 0) -> Job:
    return Job(
        name=name,
        command=["echo", name],
        resources=JobResource(nodes=1, gpus_per_node=gpus),
        environment=JobEnvironment(),
        job_id=job_id,
    )


class TestCreateCliWorkflowRun:
    def test_returns_new_id(self, _isolated_db):
        run_id = create_cli_workflow_run(workflow_name="pipeline")
        assert isinstance(run_id, int)
        assert run_id > 0

    def test_returns_none_on_db_failure(self, _isolated_db, monkeypatch):
        """Best-effort contract — any DB error returns None, not raises."""
        import srunx.db.cli_helpers as cli_helpers

        def boom(*a, **kw):
            raise RuntimeError("disk full")

        monkeypatch.setattr(cli_helpers, "init_db", boom, raising=False)
        # The import-time binding is inside the function body, so we
        # patch the concrete symbol the function imports.
        import srunx.db.connection as connection_mod

        monkeypatch.setattr(connection_mod, "init_db", boom)
        assert create_cli_workflow_run(workflow_name="pipeline") is None


class TestWorkflowStatsRoundTrip:
    """End-to-end: create run → record jobs linked to it → stats non-empty.

    Guards the regression that ``compute_workflow_stats`` silently
    returned zero counts for CLI-launched workflows (the JOIN missed
    every row because jobs weren't linked).
    """

    def test_cli_workflow_jobs_show_up_in_report(self, _isolated_db):
        run_id = create_cli_workflow_run(workflow_name="ml_pipeline")
        assert run_id is not None

        # Simulate two CLI-launched workflow jobs.
        record_submission_from_job(
            _make_job("preprocess", 100, gpus=2),
            workflow_name="ml_pipeline",
            workflow_run_id=run_id,
        )
        record_submission_from_job(
            _make_job("train", 101, gpus=4),
            workflow_name="ml_pipeline",
            workflow_run_id=run_id,
        )

        stats = compute_workflow_stats("ml_pipeline")
        assert stats["workflow_name"] == "ml_pipeline"
        assert stats["total_jobs"] == 2

    def test_missing_workflow_run_id_falls_out_of_report(self, _isolated_db):
        """The bug this PR fixes — without linking, stats return zero."""
        create_cli_workflow_run(workflow_name="unlinked_flow")
        # Deliberately NOT passing workflow_run_id — matches the old
        # CLI behaviour before P3-7.1 #93.
        record_submission_from_job(
            _make_job("orphan_job", 200),
            workflow_name="unlinked_flow",
            workflow_run_id=None,
        )

        stats = compute_workflow_stats("unlinked_flow")
        assert stats["total_jobs"] == 0  # JOIN misses the row


class TestMarkWorkflowRunStatus:
    def test_updates_status(self, _isolated_db):
        run_id = create_cli_workflow_run(workflow_name="flow")
        assert run_id is not None

        mark_workflow_run_status(run_id, "completed")

        from srunx.db.connection import open_connection
        from srunx.db.repositories.workflow_runs import WorkflowRunRepository

        conn = open_connection()
        try:
            row = WorkflowRunRepository(conn).get(run_id)
        finally:
            conn.close()
        assert row is not None
        assert row.status == "completed"
        assert row.completed_at is not None

    def test_failed_records_error(self, _isolated_db):
        run_id = create_cli_workflow_run(workflow_name="flow")
        assert run_id is not None

        mark_workflow_run_status(run_id, "failed", error="boom")

        from srunx.db.connection import open_connection
        from srunx.db.repositories.workflow_runs import WorkflowRunRepository

        conn = open_connection()
        try:
            row = WorkflowRunRepository(conn).get(run_id)
        finally:
            conn.close()
        assert row is not None
        assert row.status == "failed"
        assert row.error == "boom"

    def test_swallows_db_errors(self, _isolated_db, monkeypatch):
        """Best-effort — caller never sees the exception."""
        import srunx.db.connection as connection_mod

        def boom(*a, **kw):
            raise RuntimeError("disk full")

        monkeypatch.setattr(connection_mod, "init_db", boom)
        # Must not raise.
        mark_workflow_run_status(12345, "completed")
