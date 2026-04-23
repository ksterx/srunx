"""FastAPI dependency injection providers."""

from __future__ import annotations

import sqlite3
import threading
from collections.abc import Iterator

from srunx.db.connection import open_connection
from srunx.db.repositories.deliveries import DeliveryRepository
from srunx.db.repositories.endpoints import EndpointRepository
from srunx.db.repositories.events import EventRepository
from srunx.db.repositories.job_state_transitions import JobStateTransitionRepository
from srunx.db.repositories.jobs import JobRepository
from srunx.db.repositories.resource_snapshots import ResourceSnapshotRepository
from srunx.db.repositories.subscriptions import SubscriptionRepository
from srunx.db.repositories.watches import WatchRepository
from srunx.db.repositories.workflow_run_jobs import WorkflowRunJobRepository
from srunx.db.repositories.workflow_runs import WorkflowRunRepository
from srunx.slurm.ssh import SlurmSSHAdapter

# Thread-safe singleton SSH adapter — connected at startup via lifespan
_adapter: SlurmSSHAdapter | None = None
_adapter_lock = threading.Lock()
_active_profile_name: str | None = None


def set_adapter(adapter: SlurmSSHAdapter, profile_name: str | None = None) -> None:
    global _adapter, _active_profile_name
    with _adapter_lock:
        _adapter = adapter
        _active_profile_name = profile_name


def swap_adapter(
    new_adapter: SlurmSSHAdapter, profile_name: str | None = None
) -> SlurmSSHAdapter | None:
    """Atomically replace the current adapter. Returns the old adapter (caller must disconnect)."""
    global _adapter, _active_profile_name
    with _adapter_lock:
        old = _adapter
        _adapter = new_adapter
        _active_profile_name = profile_name
    return old


def get_adapter() -> SlurmSSHAdapter:
    with _adapter_lock:
        adapter = _adapter
    if adapter is None:
        from fastapi import HTTPException

        raise HTTPException(
            status_code=503,
            detail="SLURM connection not configured. Set SRUNX_SSH_PROFILE or SRUNX_SSH_HOSTNAME + SRUNX_SSH_USERNAME.",
        )
    return adapter


def get_adapter_or_none() -> SlurmSSHAdapter | None:
    """Return the current adapter without raising."""
    with _adapter_lock:
        return _adapter


def get_active_profile_name() -> str | None:
    with _adapter_lock:
        return _active_profile_name


# ----- New DB connection + repository providers (PR 2/3) -----
#
# Each request receives its OWN sqlite3.Connection. Connections are NOT
# shared across requests or with lifespan poller tasks (sqlite3 is
# threadlocal-by-default). WAL + busy_timeout are applied automatically
# by ``open_connection``.


def get_db_conn() -> Iterator[sqlite3.Connection]:
    conn = open_connection()
    try:
        yield conn
    finally:
        conn.close()


def get_job_repo(
    conn: sqlite3.Connection,
) -> JobRepository:
    return JobRepository(conn)


def get_workflow_run_repo(conn: sqlite3.Connection) -> WorkflowRunRepository:
    return WorkflowRunRepository(conn)


def get_workflow_run_job_repo(conn: sqlite3.Connection) -> WorkflowRunJobRepository:
    return WorkflowRunJobRepository(conn)


def get_job_state_transition_repo(
    conn: sqlite3.Connection,
) -> JobStateTransitionRepository:
    return JobStateTransitionRepository(conn)


def get_endpoint_repo(conn: sqlite3.Connection) -> EndpointRepository:
    return EndpointRepository(conn)


def get_watch_repo(conn: sqlite3.Connection) -> WatchRepository:
    return WatchRepository(conn)


def get_subscription_repo(conn: sqlite3.Connection) -> SubscriptionRepository:
    return SubscriptionRepository(conn)


def get_event_repo(conn: sqlite3.Connection) -> EventRepository:
    return EventRepository(conn)


def get_delivery_repo(conn: sqlite3.Connection) -> DeliveryRepository:
    return DeliveryRepository(conn)


def get_resource_snapshot_repo(conn: sqlite3.Connection) -> ResourceSnapshotRepository:
    return ResourceSnapshotRepository(conn)
