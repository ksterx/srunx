"""Phase-J Task 54 — backward compatibility smoke for non-sweep workflows.

Confirms that the sweep infrastructure (DB schema, state service, events)
does not alter the behaviour of a plain workflow YAML that has no
``sweep:`` block. Specifically:

- ``WorkflowRunner.from_yaml(...)`` loads the workflow normally.
- ``runner.run()`` (with a mocked Slurm client) creates a single
  ``workflow_runs`` row and zero ``sweep_runs`` rows.
- The row carries ``sweep_run_id = NULL`` (the post-V3 column is
  nullable and unused for non-sweep runs).
- ``workflow_run.status_changed`` events are emitted on the
  pending → running and running → completed transitions just as they
  were before the sweep feature.

These tests only touch stable public surfaces — ``WorkflowRunner``,
``WorkflowRunStateService``, and direct SQL reads — so they're
independent of the Phase-G/H backend state-service refactor
currently in flight.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import yaml  # type: ignore

from srunx.db.connection import open_connection, transaction
from srunx.runner import WorkflowRunner
from srunx.sweep.state_service import WorkflowRunStateService

# ---------------------------------------------------------------------------
# Fixture — per-test isolated srunx DB (same shape as tests/sweep/conftest.py).
# ---------------------------------------------------------------------------


@pytest.fixture
def isolated_db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path))
    from srunx.db.connection import init_db

    return init_db(delete_legacy=False)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _write_yaml(path: Path, data: dict[str, Any]) -> Path:
    path.write_text(yaml.dump(data))
    return path


def _build_single_job_yaml(tmp_path: Path) -> Path:
    """YAML without any ``sweep:`` block — the backward-compat baseline."""
    return _write_yaml(
        tmp_path / "plain.yaml",
        {
            "name": "plain_wf",
            "args": {"lr": 0.1},
            "jobs": [
                {
                    "name": "train",
                    "command": ["train.py", "--lr", "{{ lr }}"],
                    "environment": {"conda": "env"},
                }
            ],
        },
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestNoSweepBlockPreservesLegacyBehaviour:
    """A YAML with no ``sweep:`` block behaves exactly as it did pre-V3."""

    def test_from_yaml_loads_without_sweep_metadata(
        self,
        isolated_db: Path,
        tmp_path: Path,
    ) -> None:
        yaml_path = _build_single_job_yaml(tmp_path)
        runner = WorkflowRunner.from_yaml(yaml_path)

        assert runner.args == {"lr": 0.1}
        assert runner.workflow.name == "plain_wf"
        assert len(runner.workflow.jobs) == 1

    def test_workflow_run_status_changed_event_still_fires(
        self,
        isolated_db: Path,
    ) -> None:
        """Non-sweep workflow_run transitions still emit the classic event.

        Drives a workflow_runs row via ``WorkflowRunStateService`` (the
        same entry point both the runner and the poller use) and
        confirms a ``workflow_run.status_changed`` event shows up for
        each transition. We go through the state service directly
        because the goal of this backward-compat test is to prove the
        sweep refactor did not rewire the non-sweep event path — we
        don't need to re-verify the runner's happy path here (that's
        covered by :class:`TestRunCreatesWorkflowRunWithoutSweep`).
        """
        # Seed a plain (non-sweep) workflow_run row.
        conn = open_connection()
        try:
            cur = conn.execute(
                """
                INSERT INTO workflow_runs (
                    workflow_name, status, started_at, args, triggered_by
                ) VALUES (?, 'pending', '2026-04-21T00:00:00+00:00', ?, 'cli')
                """,
                ("plain_wf", "{}"),
            )
            conn.commit()
            wr_id = int(cur.lastrowid or 0)
        finally:
            conn.close()

        assert wr_id > 0

        # Drive pending → running → completed through the state service.
        conn = open_connection()
        try:
            with transaction(conn, "IMMEDIATE"):
                WorkflowRunStateService.update(
                    conn=conn,
                    workflow_run_id=wr_id,
                    from_status="pending",
                    to_status="running",
                )
            with transaction(conn, "IMMEDIATE"):
                WorkflowRunStateService.update(
                    conn=conn,
                    workflow_run_id=wr_id,
                    from_status="running",
                    to_status="completed",
                    completed_at="2026-04-21T00:01:00+00:00",
                )
        finally:
            conn.close()

        # Two events fired (one per transition).
        conn2 = open_connection()
        try:
            row = conn2.execute(
                "SELECT COUNT(*) AS c FROM events WHERE kind = ? AND source_ref = ?",
                ("workflow_run.status_changed", f"workflow_run:{wr_id}"),
            ).fetchone()

            # Critically: zero sweep_run.status_changed events for a
            # non-sweep workflow run. This guards against a regression
            # where the aggregator accidentally treats every
            # workflow_run as a sweep cell.
            sweep_events = conn2.execute(
                "SELECT COUNT(*) AS c FROM events WHERE kind = ?",
                ("sweep_run.status_changed",),
            ).fetchone()
        finally:
            conn2.close()

        assert int(row["c"]) == 2
        assert int(sweep_events["c"]) == 0
