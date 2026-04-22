"""Integration-style tests for :class:`srunx.pollers.active_watch_poller.ActiveWatchPoller`.

These tests drive the poller end-to-end against a real (file-backed)
srunx DB. The SLURM side is faked by a tiny stub that satisfies
:class:`srunx.client_protocol.SlurmClientProtocol` and returns a
pre-canned ``dict[int, JobStatusInfo]`` per test.
"""

from __future__ import annotations

import sqlite3
from datetime import UTC, datetime
from pathlib import Path

import anyio

from srunx.client_protocol import JobStatusInfo
from srunx.db.repositories.deliveries import DeliveryRepository
from srunx.db.repositories.endpoints import EndpointRepository
from srunx.db.repositories.events import EventRepository
from srunx.db.repositories.job_state_transitions import (
    JobStateTransitionRepository,
)
from srunx.db.repositories.jobs import JobRepository
from srunx.db.repositories.subscriptions import SubscriptionRepository
from srunx.db.repositories.watches import WatchRepository
from srunx.db.repositories.workflow_run_jobs import WorkflowRunJobRepository
from srunx.db.repositories.workflow_runs import WorkflowRunRepository
from srunx.pollers.active_watch_poller import ActiveWatchPoller

# ---------------------------------------------------------------------------
# Test helpers
# ---------------------------------------------------------------------------


class StubSlurmClient:
    """Minimal ``SlurmClientProtocol`` stub returning pre-canned status."""

    def __init__(self, responses: dict[int, JobStatusInfo]) -> None:
        self.responses = responses
        self.calls: list[list[int]] = []

    def queue_by_ids(self, job_ids: list[int]) -> dict[int, JobStatusInfo]:
        self.calls.append(list(job_ids))
        return {jid: self.responses[jid] for jid in job_ids if jid in self.responses}


def _seed_job(conn: sqlite3.Connection, job_id: int, *, status: str = "PENDING") -> int:
    repo = JobRepository(conn)
    return repo.record_submission(
        job_id=job_id,
        name=f"job_{job_id}",
        status=status,
        submission_source="web",
    )


def _seed_open_job_watch(conn: sqlite3.Connection, job_id: int) -> int:
    # V5+ grammar: ``job:<scheduler_key>:<id>``. Local SLURM jobs always
    # use ``scheduler_key='local'``.
    return WatchRepository(conn).create(kind="job", target_ref=f"job:local:{job_id}")


def _seed_pending_transition(conn: sqlite3.Connection, job_id: int, status: str) -> int:
    return JobStateTransitionRepository(conn).insert(
        job_id=job_id,
        from_status=None,
        to_status=status,
        source="poller",
    )


def _run_once(poller: ActiveWatchPoller) -> None:
    """Drive one ``run_cycle`` to completion."""
    anyio.run(poller.run_cycle)


# ---------------------------------------------------------------------------
# Job branch tests
# ---------------------------------------------------------------------------


class TestJobTransitions:
    """Exercise the job watch → transition → event → delivery pipeline."""

    def test_transition_detected_and_event_emitted(
        self,
        tmp_srunx_db: tuple[sqlite3.Connection, Path],
    ) -> None:
        conn, db_path = tmp_srunx_db

        job_id = 4242
        _seed_job(conn, job_id, status="PENDING")
        watch_id = _seed_open_job_watch(conn, job_id)

        endpoint_id = EndpointRepository(conn).create(
            "slack_webhook", "primary", {"webhook_url": "https://example/hook"}
        )
        SubscriptionRepository(conn).create(
            watch_id=watch_id, endpoint_id=endpoint_id, preset="running_and_terminal"
        )

        _seed_pending_transition(conn, job_id, "PENDING")

        stub = StubSlurmClient(
            {
                job_id: JobStatusInfo(
                    status="RUNNING",
                    started_at=datetime(2026, 4, 18, 10, 0, 0, tzinfo=UTC),
                    nodelist="node01",
                ),
            }
        )
        poller = ActiveWatchPoller(stub, db_path=db_path)
        _run_once(poller)

        transitions = JobStateTransitionRepository(conn).history_for_job(job_id)
        assert len(transitions) == 2
        assert transitions[0].to_status == "PENDING"
        assert transitions[1].from_status == "PENDING"
        assert transitions[1].to_status == "RUNNING"
        assert transitions[1].source == "poller"

        recent_events = EventRepository(conn).list_recent(limit=10)
        status_events = [e for e in recent_events if e.kind == "job.status_changed"]
        assert len(status_events) == 1
        event = status_events[0]
        # V5 grammar: ``job:<scheduler_key>:<id>``; local SLURM jobs use
        # ``scheduler_key='local'``.
        assert event.source_ref == f"job:local:{job_id}"
        assert event.payload.get("from_status") == "PENDING"
        assert event.payload.get("to_status") == "RUNNING"
        # Payload is enriched with job_id + job_name so adapters do not
        # have to re-query the DB or parse source_ref.
        assert event.payload.get("job_id") == job_id
        assert event.payload.get("job_name") == f"job_{job_id}"

        job_row = JobRepository(conn).get(job_id)
        assert job_row is not None
        assert job_row.status == "RUNNING"
        assert job_row.nodelist == "node01"

        deliveries = DeliveryRepository(conn).list_by_subscription(
            subscription_id=1, status="pending"
        )
        assert len(deliveries) == 1
        assert deliveries[0].endpoint_id == endpoint_id

        # Stub was called once with the single job id we seeded.
        assert stub.calls == [[job_id]]

    def test_no_subscription_means_no_delivery(
        self,
        tmp_srunx_db: tuple[sqlite3.Connection, Path],
    ) -> None:
        conn, db_path = tmp_srunx_db

        job_id = 77
        _seed_job(conn, job_id, status="PENDING")
        _seed_open_job_watch(conn, job_id)
        _seed_pending_transition(conn, job_id, "PENDING")

        stub = StubSlurmClient({job_id: JobStatusInfo(status="RUNNING")})
        _run_once(ActiveWatchPoller(stub, db_path=db_path))

        # Transition + event present…
        transitions = JobStateTransitionRepository(conn).history_for_job(job_id)
        assert any(t.to_status == "RUNNING" for t in transitions)
        events = EventRepository(conn).list_recent()
        assert any(e.kind == "job.status_changed" for e in events)

        # …but no deliveries.
        deliveries_rows = conn.execute(
            "SELECT COUNT(*) AS c FROM deliveries"
        ).fetchone()
        assert deliveries_rows["c"] == 0

    def test_terminal_transition_closes_watch(
        self,
        tmp_srunx_db: tuple[sqlite3.Connection, Path],
    ) -> None:
        conn, db_path = tmp_srunx_db

        job_id = 1001
        _seed_job(conn, job_id, status="RUNNING")
        watch_id = _seed_open_job_watch(conn, job_id)
        _seed_pending_transition(conn, job_id, "RUNNING")

        stub = StubSlurmClient(
            {
                job_id: JobStatusInfo(
                    status="COMPLETED",
                    started_at=datetime(2026, 4, 18, 10, 0, 0, tzinfo=UTC),
                    completed_at=datetime(2026, 4, 18, 11, 0, 0, tzinfo=UTC),
                    duration_secs=3600,
                ),
            }
        )
        _run_once(ActiveWatchPoller(stub, db_path=db_path))

        watch = WatchRepository(conn).get(watch_id)
        assert watch is not None
        assert watch.closed_at is not None

        job_row = JobRepository(conn).get(job_id)
        assert job_row is not None
        assert job_row.status == "COMPLETED"
        assert job_row.duration_secs == 3600

    def test_unchanged_status_is_noop(
        self,
        tmp_srunx_db: tuple[sqlite3.Connection, Path],
    ) -> None:
        conn, db_path = tmp_srunx_db

        job_id = 9000
        _seed_job(conn, job_id, status="RUNNING")
        _seed_open_job_watch(conn, job_id)
        _seed_pending_transition(conn, job_id, "RUNNING")

        stub = StubSlurmClient({job_id: JobStatusInfo(status="RUNNING")})
        _run_once(ActiveWatchPoller(stub, db_path=db_path))

        transitions = JobStateTransitionRepository(conn).history_for_job(job_id)
        # Only the seed transition — the poller should not insert a duplicate.
        assert len(transitions) == 1

        events = EventRepository(conn).list_recent()
        assert [e for e in events if e.kind == "job.status_changed"] == []

    def test_missing_from_queue_result_is_skipped(
        self,
        tmp_srunx_db: tuple[sqlite3.Connection, Path],
    ) -> None:
        conn, db_path = tmp_srunx_db

        job_id = 5555
        _seed_job(conn, job_id, status="PENDING")
        _seed_open_job_watch(conn, job_id)
        _seed_pending_transition(conn, job_id, "PENDING")

        # Stub has no response for this job (empty dict return).
        stub = StubSlurmClient({})
        _run_once(ActiveWatchPoller(stub, db_path=db_path))

        transitions = JobStateTransitionRepository(conn).history_for_job(job_id)
        assert len(transitions) == 1  # still only the seed row

        events = EventRepository(conn).list_recent()
        assert [e for e in events if e.kind == "job.status_changed"] == []

    def test_first_observation_without_seed_transition_is_skipped(
        self,
        tmp_srunx_db: tuple[sqlite3.Connection, Path],
    ) -> None:
        """Watches created mid-lifecycle must not fire an event on first observation.

        Per the task spec, the poller only emits on transitions; if
        ``latest_for_job`` is ``None``, the poller waits for the next
        actual change before emitting anything.
        """
        conn, db_path = tmp_srunx_db

        job_id = 1234
        _seed_job(conn, job_id, status="PENDING")
        _seed_open_job_watch(conn, job_id)
        # No seed transition — poller sees this job for the first time.

        stub = StubSlurmClient({job_id: JobStatusInfo(status="RUNNING")})
        _run_once(ActiveWatchPoller(stub, db_path=db_path))

        transitions = JobStateTransitionRepository(conn).history_for_job(job_id)
        assert transitions == []

        events = EventRepository(conn).list_recent()
        assert [e for e in events if e.kind == "job.status_changed"] == []


# ---------------------------------------------------------------------------
# Workflow-run branch tests
# ---------------------------------------------------------------------------


class TestWorkflowAggregation:
    """Exercise the workflow_run watch → aggregation → event pipeline."""

    def test_all_completed_marks_run_completed(
        self,
        tmp_srunx_db: tuple[sqlite3.Connection, Path],
    ) -> None:
        conn, db_path = tmp_srunx_db

        run_id = WorkflowRunRepository(conn).create(
            workflow_name="pipeline",
            yaml_path=None,
            args=None,
            triggered_by="web",
        )
        WorkflowRunRepository(conn).update_status(run_id, "running")

        job_a = 11
        job_b = 22
        _seed_job(conn, job_a, status="COMPLETED")
        _seed_job(conn, job_b, status="COMPLETED")
        WorkflowRunJobRepository(conn).create(
            workflow_run_id=run_id, job_name="train", job_id=job_a
        )
        WorkflowRunJobRepository(conn).create(
            workflow_run_id=run_id, job_name="eval", job_id=job_b
        )

        watch_id = WatchRepository(conn).create(
            kind="workflow_run", target_ref=f"workflow_run:{run_id}"
        )
        endpoint_id = EndpointRepository(conn).create(
            "slack_webhook", "primary", {"webhook_url": "https://example/hook"}
        )
        SubscriptionRepository(conn).create(
            watch_id=watch_id, endpoint_id=endpoint_id, preset="terminal"
        )

        stub = StubSlurmClient({})  # no job watches
        _run_once(ActiveWatchPoller(stub, db_path=db_path))

        run = WorkflowRunRepository(conn).get(run_id)
        assert run is not None
        assert run.status == "completed"
        assert run.completed_at is not None

        events = EventRepository(conn).list_recent()
        wf_events = [e for e in events if e.kind == "workflow_run.status_changed"]
        assert len(wf_events) == 1
        assert wf_events[0].payload.get("to_status") == "completed"
        assert wf_events[0].payload.get("from_status") == "running"
        # Payload is enriched with workflow_run_id + workflow_name.
        assert wf_events[0].payload.get("workflow_run_id") == run_id
        assert wf_events[0].payload.get("workflow_name") == "pipeline"

        # Delivery row should exist (preset=terminal + terminal status).
        deliveries = DeliveryRepository(conn).list_by_subscription(
            subscription_id=1, status="pending"
        )
        assert len(deliveries) == 1

        # Terminal workflow_run → watch closed.
        watch = WatchRepository(conn).get(watch_id)
        assert watch is not None
        assert watch.closed_at is not None

    def test_any_cancelled_marks_run_cancelled(
        self,
        tmp_srunx_db: tuple[sqlite3.Connection, Path],
    ) -> None:
        conn, db_path = tmp_srunx_db

        run_id = WorkflowRunRepository(conn).create(
            workflow_name="pipeline",
            yaml_path=None,
            args=None,
            triggered_by="cli",
        )
        WorkflowRunRepository(conn).update_status(run_id, "running")

        _seed_job(conn, 101, status="COMPLETED")
        _seed_job(conn, 102, status="CANCELLED")
        WorkflowRunJobRepository(conn).create(
            workflow_run_id=run_id, job_name="train", job_id=101
        )
        WorkflowRunJobRepository(conn).create(
            workflow_run_id=run_id, job_name="eval", job_id=102
        )
        watch_id = WatchRepository(conn).create(
            kind="workflow_run", target_ref=f"workflow_run:{run_id}"
        )

        _run_once(ActiveWatchPoller(StubSlurmClient({}), db_path=db_path))

        run = WorkflowRunRepository(conn).get(run_id)
        assert run is not None
        assert run.status == "cancelled"

        watch = WatchRepository(conn).get(watch_id)
        assert watch is not None
        assert watch.closed_at is not None

    def test_pending_run_with_all_pending_children_is_quiet(
        self,
        tmp_srunx_db: tuple[sqlite3.Connection, Path],
    ) -> None:
        """Regression: P1-1 (#E).

        A just-submitted workflow run sits at ``status='pending'`` with
        every child job at ``PENDING``. Rule 5 (Otherwise → pending) of
        the aggregator should match the current status and emit
        nothing. Pre-P1-1 the router wrote ``running`` eagerly here, so
        this cycle emitted a spurious ``running → pending`` transition.
        """
        conn, db_path = tmp_srunx_db

        run_id = WorkflowRunRepository(conn).create(
            workflow_name="pipeline",
            yaml_path=None,
            args=None,
            triggered_by="web",
        )
        # Leave the run at its default 'pending' — exactly what the
        # fixed /run endpoint yields now.
        _seed_job(conn, 601, status="PENDING")
        _seed_job(conn, 602, status="PENDING")
        WorkflowRunJobRepository(conn).create(
            workflow_run_id=run_id, job_name="a", job_id=601
        )
        WorkflowRunJobRepository(conn).create(
            workflow_run_id=run_id, job_name="b", job_id=602
        )
        WatchRepository(conn).create(
            kind="workflow_run", target_ref=f"workflow_run:{run_id}"
        )

        _run_once(ActiveWatchPoller(StubSlurmClient({}), db_path=db_path))

        run = WorkflowRunRepository(conn).get(run_id)
        assert run is not None
        assert run.status == "pending"
        wf_events = [
            e
            for e in EventRepository(conn).list_recent()
            if e.kind == "workflow_run.status_changed"
        ]
        assert wf_events == []

    def test_running_child_leaves_run_running_no_event(
        self,
        tmp_srunx_db: tuple[sqlite3.Connection, Path],
    ) -> None:
        """A run already in ``running`` should not re-emit an event."""
        conn, db_path = tmp_srunx_db

        run_id = WorkflowRunRepository(conn).create(
            workflow_name="pipeline",
            yaml_path=None,
            args=None,
            triggered_by="web",
        )
        WorkflowRunRepository(conn).update_status(run_id, "running")

        _seed_job(conn, 501, status="RUNNING")
        _seed_job(conn, 502, status="PENDING")
        WorkflowRunJobRepository(conn).create(
            workflow_run_id=run_id, job_name="a", job_id=501
        )
        WorkflowRunJobRepository(conn).create(
            workflow_run_id=run_id, job_name="b", job_id=502
        )
        WatchRepository(conn).create(
            kind="workflow_run", target_ref=f"workflow_run:{run_id}"
        )

        _run_once(ActiveWatchPoller(StubSlurmClient({}), db_path=db_path))

        events = EventRepository(conn).list_recent()
        assert [e for e in events if e.kind == "workflow_run.status_changed"] == []

        run = WorkflowRunRepository(conn).get(run_id)
        assert run is not None
        assert run.status == "running"


# ---------------------------------------------------------------------------
# Logging sanity
# ---------------------------------------------------------------------------


class TestObservability:
    def test_empty_cycle_runs_without_error(
        self,
        tmp_srunx_db: tuple[sqlite3.Connection, Path],
    ) -> None:
        """No open watches → one no-op cycle with no SLURM calls."""
        _conn, db_path = tmp_srunx_db
        stub = StubSlurmClient({})
        _run_once(ActiveWatchPoller(stub, db_path=db_path))
        # Never queried SLURM because there were no job watches.
        assert stub.calls == []
