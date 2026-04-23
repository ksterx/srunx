"""Integration test for ActiveWatchPoller + sweep aggregation.

Proves that the poller's workflow_run observations funnel through
:class:`WorkflowRunStateService`, which (1) updates the cell status,
(2) emits a ``workflow_run.status_changed`` event, (3) rolls the sweep
counters, and (4) fires a ``sweep_run.status_changed`` event whenever
the aggregator determines the sweep's status should transition.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import anyio
import pytest

from srunx.client_protocol import JobStatusInfo
from srunx.db.repositories.deliveries import DeliveryRepository
from srunx.db.repositories.endpoints import EndpointRepository
from srunx.db.repositories.jobs import JobRepository
from srunx.db.repositories.subscriptions import SubscriptionRepository
from srunx.db.repositories.watches import WatchRepository
from srunx.db.repositories.workflow_run_jobs import WorkflowRunJobRepository
from srunx.pollers.active_watch_poller import ActiveWatchPoller

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class StubSlurmClient:
    """Minimal ``SlurmClientProtocol`` stub (no job watches in this test)."""

    def __init__(self) -> None:
        self.calls: list[list[int]] = []

    def queue_by_ids(self, job_ids: list[int]) -> dict[int, JobStatusInfo]:
        self.calls.append(list(job_ids))
        return {}


def _seed_sweep(conn: sqlite3.Connection, *, cell_count: int) -> int:
    """Insert a ``sweep_runs`` row with ``cells_pending = cell_count``."""
    cur = conn.execute(
        """
        INSERT INTO sweep_runs (
            name, status, matrix, args,
            fail_fast, max_parallel, cell_count,
            cells_pending, cells_running, cells_completed,
            cells_failed, cells_cancelled,
            submission_source, started_at
        ) VALUES (?, 'pending', ?, ?, 0, ?, ?, ?, 0, 0, 0, 0, 'cli', '2026-04-18T10:00:00+00:00')
        """,
        (
            "poller_integration_sweep",
            '{"lr":[1,2,3,4]}',
            "{}",
            4,
            cell_count,
            cell_count,
        ),
    )
    return int(cur.lastrowid or 0)


def _seed_cell(
    conn: sqlite3.Connection,
    sweep_id: int,
    *,
    index: int,
) -> int:
    """Insert one child workflow_run (status='pending') linked to ``sweep_id``."""
    cur = conn.execute(
        """
        INSERT INTO workflow_runs (
            workflow_name, status, started_at, args, triggered_by, sweep_run_id
        ) VALUES (?, 'pending', '2026-04-18T10:00:00+00:00', ?, 'cli', ?)
        """,
        ("cell_wf", f'{{"idx":{index}}}', sweep_id),
    )
    return int(cur.lastrowid or 0)


def _seed_job(conn: sqlite3.Connection, job_id: int, *, status: str) -> None:
    JobRepository(conn).record_submission(
        job_id=job_id,
        name=f"job_{job_id}",
        status=status,
        submission_source="workflow",
    )


def _run_once(poller: ActiveWatchPoller) -> None:
    anyio.run(poller.run_cycle)


def _count_events(conn: sqlite3.Connection, kind: str, source_ref: str) -> int:
    row = conn.execute(
        "SELECT COUNT(*) FROM events WHERE kind = ? AND source_ref = ?",
        (kind, source_ref),
    ).fetchone()
    return int(row[0])


# ---------------------------------------------------------------------------
# Integration test
# ---------------------------------------------------------------------------


def test_poller_drives_sweep_from_pending_through_completed(
    tmp_srunx_db: tuple[sqlite3.Connection, Path],
) -> None:
    """Parent sweep + 4 child workflow_runs — poller observations drive
    the sweep through ``pending → running → completed``, firing sweep
    aggregation events along the way.

    Scenario:
      1. Cycle 1: child #0 has RUNNING jobs; the other three have PENDING.
         Poller aggregates child #0 to ``running`` → sweep flips to
         ``running`` (first sweep-level event fires).
      2. Cycles 2-5: each subsequent cycle flips one child from running
         to completed. Final cycle flips the sweep to ``completed``
         (second sweep-level event fires).
    """
    conn, db_path = tmp_srunx_db

    sweep_id = _seed_sweep(conn, cell_count=4)
    cell_ids = [_seed_cell(conn, sweep_id, index=i) for i in range(4)]

    # Each cell owns one SLURM job. Watch each workflow_run.
    job_ids = [1000 + i for i in range(4)]
    for cell_id, job_id in zip(cell_ids, job_ids, strict=False):
        _seed_job(conn, job_id, status="PENDING")
        WorkflowRunJobRepository(conn).create(
            workflow_run_id=cell_id, job_name="train", job_id=job_id
        )
        WatchRepository(conn).create(
            kind="workflow_run", target_ref=f"workflow_run:{cell_id}"
        )

    # Subscribe the sweep to a Slack endpoint so deliveries are exercised.
    endpoint_id = EndpointRepository(conn).create(
        kind="slack_webhook",
        name="sweep_e2e",
        config={"webhook_url": "https://hooks.slack.com/services/X/Y/Z"},
    )
    sweep_watch_id = WatchRepository(conn).create(
        kind="sweep_run", target_ref=f"sweep_run:{sweep_id}"
    )
    sweep_subscription_id = SubscriptionRepository(conn).create(
        watch_id=sweep_watch_id, endpoint_id=endpoint_id, preset="terminal"
    )

    stub = StubSlurmClient()
    poller = ActiveWatchPoller(stub, db_path=db_path)

    # --- Cycle 1: first cell running, rest pending. Sweep flips to running.
    conn.execute("UPDATE jobs SET status = 'RUNNING' WHERE job_id = ?", (job_ids[0],))
    conn.commit()

    _run_once(poller)

    sweep_row = conn.execute(
        "SELECT status, cells_pending, cells_running, cells_completed "
        "FROM sweep_runs WHERE id = ?",
        (sweep_id,),
    ).fetchone()
    assert sweep_row["status"] == "running"
    assert sweep_row["cells_pending"] == 3
    assert sweep_row["cells_running"] == 1
    sweep_events_after_cycle_1 = _count_events(
        conn, "sweep_run.status_changed", f"sweep_run:{sweep_id}"
    )
    assert sweep_events_after_cycle_1 == 1  # pending → running

    # --- Cycles 2-4: one-by-one flip jobs to COMPLETED.
    for i in range(3):
        job_id = job_ids[i]
        conn.execute("UPDATE jobs SET status = 'COMPLETED' WHERE job_id = ?", (job_id,))
        conn.commit()
        _run_once(poller)

    sweep_row = conn.execute(
        "SELECT status, cells_pending, cells_running, cells_completed "
        "FROM sweep_runs WHERE id = ?",
        (sweep_id,),
    ).fetchone()
    # Three cells completed; the fourth (index 3) is still PENDING at the
    # SLURM level so its workflow_run stays 'pending'. Sweep remains 'running'.
    assert sweep_row["cells_completed"] == 3
    assert sweep_row["cells_pending"] == 1
    assert sweep_row["status"] == "running"

    # --- Cycle 5: flip the last cell from PENDING → COMPLETED directly.
    # The aggregator promotes the cell's workflow_run through
    # pending → completed in a single step (both the runner's and the
    # poller's legal transitions).
    conn.execute("UPDATE jobs SET status = 'COMPLETED' WHERE job_id = ?", (job_ids[3],))
    conn.commit()
    _run_once(poller)

    sweep_row = conn.execute(
        "SELECT status, cells_pending, cells_running, cells_completed, "
        "       completed_at "
        "FROM sweep_runs WHERE id = ?",
        (sweep_id,),
    ).fetchone()
    assert sweep_row["cells_completed"] == 4
    assert sweep_row["cells_pending"] == 0
    assert sweep_row["status"] == "completed"
    assert sweep_row["completed_at"] is not None

    # --- Sweep-level events: exactly two (pending→running + running→completed).
    sweep_events = _count_events(
        conn, "sweep_run.status_changed", f"sweep_run:{sweep_id}"
    )
    assert sweep_events == 2

    # --- Workflow-run-level events: one per cell transition that
    # actually flipped (pending→running for cell #0 in cycle 1 and
    # running→completed for cells #0-#2; cell #3 went pending→completed
    # in one step in cycle 5 — four transition events + the one extra
    # for cell #0's pending→running = 5.
    wr_events_total = sum(
        _count_events(conn, "workflow_run.status_changed", f"workflow_run:{cid}")
        for cid in cell_ids
    )
    # Cells #0-#2: 2 events each (pending→running, running→completed) = 6.
    # Cell #3: 1 event (pending→completed).
    # But cells #1 and #2 only get pending→completed (they never went to
    # running in their own right: their jobs flipped directly from PENDING
    # to COMPLETED, and workflow_run aggregation reads the cell's child
    # jobs — any COMPLETED child with no RUNNING siblings yields
    # 'completed' directly). So actually each of cells #1/#2/#3 fires
    # exactly 1 event. Cell #0 fires 2 (pending→running + running→completed).
    assert wr_events_total == 5

    # --- Deliveries. Subscription has preset=terminal, so only the sweep
    # transition to 'completed' queues a delivery. 'running' is skipped.
    deliveries = DeliveryRepository(conn).list_by_subscription(
        subscription_id=sweep_subscription_id, status="pending"
    )
    assert len(deliveries) == 1
    assert deliveries[0].endpoint_id == endpoint_id


def test_poller_sweep_failure_fires_single_failed_event(
    tmp_srunx_db: tuple[sqlite3.Connection, Path],
) -> None:
    """A single FAILED child short-circuits the aggregator to ``failed``.

    Once every in-flight cell is terminal, the aggregator fires exactly
    one ``sweep_run.status_changed`` event with ``to_status='failed'``.
    """
    conn, db_path = tmp_srunx_db

    sweep_id = _seed_sweep(conn, cell_count=2)
    cell_ids = [_seed_cell(conn, sweep_id, index=i) for i in range(2)]

    job_ids = [2000, 2001]
    for cell_id, job_id in zip(cell_ids, job_ids, strict=False):
        _seed_job(conn, job_id, status="PENDING")
        WorkflowRunJobRepository(conn).create(
            workflow_run_id=cell_id, job_name="train", job_id=job_id
        )
        WatchRepository(conn).create(
            kind="workflow_run", target_ref=f"workflow_run:{cell_id}"
        )

    stub = StubSlurmClient()
    poller = ActiveWatchPoller(stub, db_path=db_path)

    # First cell completes, second fails — both in the same cycle.
    conn.execute("UPDATE jobs SET status = 'COMPLETED' WHERE job_id = ?", (job_ids[0],))
    conn.execute("UPDATE jobs SET status = 'FAILED' WHERE job_id = ?", (job_ids[1],))
    conn.commit()

    _run_once(poller)

    sweep_row = conn.execute(
        "SELECT status, cells_completed, cells_failed, cells_pending "
        "FROM sweep_runs WHERE id = ?",
        (sweep_id,),
    ).fetchone()
    assert sweep_row["status"] == "failed"
    assert sweep_row["cells_completed"] == 1
    assert sweep_row["cells_failed"] == 1
    assert sweep_row["cells_pending"] == 0

    sweep_events_rows = conn.execute(
        "SELECT payload FROM events WHERE kind = ? AND source_ref = ?",
        ("sweep_run.status_changed", f"sweep_run:{sweep_id}"),
    ).fetchall()
    assert len(sweep_events_rows) == 1


def test_poller_skips_workflow_without_subscription(
    tmp_srunx_db: tuple[sqlite3.Connection, Path],
) -> None:
    """Regression: sweep transitions with no sweep-level subscription still
    fire the event but queue zero deliveries (no wasted fan-out)."""
    conn, db_path = tmp_srunx_db

    sweep_id = _seed_sweep(conn, cell_count=1)
    cell_id = _seed_cell(conn, sweep_id, index=0)
    job_id = 3000
    _seed_job(conn, job_id, status="PENDING")
    WorkflowRunJobRepository(conn).create(
        workflow_run_id=cell_id, job_name="train", job_id=job_id
    )
    WatchRepository(conn).create(
        kind="workflow_run", target_ref=f"workflow_run:{cell_id}"
    )
    # No sweep-level watch → no fan-out target for sweep events.

    stub = StubSlurmClient()
    conn.execute("UPDATE jobs SET status = 'COMPLETED' WHERE job_id = ?", (job_id,))
    conn.commit()

    _run_once(ActiveWatchPoller(stub, db_path=db_path))

    # Sweep terminal, event present, zero deliveries.
    assert _count_events(conn, "sweep_run.status_changed", f"sweep_run:{sweep_id}") == 1
    deliveries = conn.execute("SELECT COUNT(*) AS c FROM deliveries").fetchone()
    assert deliveries["c"] == 0


@pytest.mark.parametrize("preset", ["terminal", "running_and_terminal", "all"])
def test_poller_respects_preset_for_sweep_deliveries(
    preset: str,
    tmp_srunx_db: tuple[sqlite3.Connection, Path],
) -> None:
    """Deliveries honour the subscription preset for sweep transitions.

    * ``terminal`` → 1 delivery (only completed).
    * ``running_and_terminal`` → 2 deliveries (running + completed).
    * ``all`` → 2 deliveries (running + completed, same as R&T for a
      happy-path sweep because no extra kinds apply here).
    """
    conn, db_path = tmp_srunx_db

    sweep_id = _seed_sweep(conn, cell_count=1)
    cell_id = _seed_cell(conn, sweep_id, index=0)
    job_id = 4000
    _seed_job(conn, job_id, status="PENDING")
    WorkflowRunJobRepository(conn).create(
        workflow_run_id=cell_id, job_name="train", job_id=job_id
    )
    WatchRepository(conn).create(
        kind="workflow_run", target_ref=f"workflow_run:{cell_id}"
    )

    endpoint_id = EndpointRepository(conn).create(
        kind="slack_webhook",
        name=f"ep_{preset}",
        config={"webhook_url": "https://hooks.slack.com/services/X/Y/Z"},
    )
    sweep_watch_id = WatchRepository(conn).create(
        kind="sweep_run", target_ref=f"sweep_run:{sweep_id}"
    )
    sub_id = SubscriptionRepository(conn).create(
        watch_id=sweep_watch_id, endpoint_id=endpoint_id, preset=preset
    )

    # Cycle 1: job RUNNING → sweep running.
    conn.execute("UPDATE jobs SET status = 'RUNNING' WHERE job_id = ?", (job_id,))
    conn.commit()
    _run_once(ActiveWatchPoller(StubSlurmClient(), db_path=db_path))

    # Cycle 2: job COMPLETED → sweep completed.
    conn.execute("UPDATE jobs SET status = 'COMPLETED' WHERE job_id = ?", (job_id,))
    conn.commit()
    _run_once(ActiveWatchPoller(StubSlurmClient(), db_path=db_path))

    deliveries = DeliveryRepository(conn).list_by_subscription(
        subscription_id=sub_id, status="pending"
    )
    if preset == "terminal":
        assert len(deliveries) == 1
    else:
        assert len(deliveries) == 2
