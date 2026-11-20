"""FastAPI dependency injection providers."""

from __future__ import annotations

import sqlite3
import threading
from collections.abc import Iterator

from srunx.observability.storage.connection import open_connection
from srunx.observability.storage.repositories.deliveries import DeliveryRepository
from srunx.observability.storage.repositories.endpoints import EndpointRepository
from srunx.observability.storage.repositories.events import EventRepository
from srunx.observability.storage.repositories.job_state_transitions import (
    JobStateTransitionRepository,
)
from srunx.observability.storage.repositories.jobs import JobRepository
from srunx.observability.storage.repositories.resource_snapshots import (
    ResourceSnapshotRepository,
)
from srunx.observability.storage.repositories.subscriptions import (
    SubscriptionRepository,
)
from srunx.observability.storage.repositories.watches import WatchRepository
from srunx.observability.storage.repositories.workflow_run_jobs import (
    WorkflowRunJobRepository,
)
from srunx.observability.storage.repositories.workflow_runs import WorkflowRunRepository
from srunx.slurm.clients.ssh import SlurmSSHClient

# One lazily-connected SSH adapter per cluster. Which cluster a request
# uses comes from its ``?cluster=`` parameter (see ``srunx.web.context``),
# never from process-global "active profile" state.
_adapters: dict[str, SlurmSSHClient] = {}
_adapter_lock = threading.Lock()


def _http_error(status: int, detail: str) -> Exception:
    from fastapi import HTTPException

    return HTTPException(status_code=status, detail=detail)


def adapter_for(cluster: str) -> SlurmSSHClient:
    """Return (creating on first use) the SSH adapter of *cluster*.

    The adapter connects lazily on its first command. Raises
    ``HTTPException`` for unknown or non-SSH clusters.
    """
    from srunx.common.config import ConfigError, load_settings

    try:
        cfg = load_settings().cluster(cluster)
    except ConfigError as exc:
        raise _http_error(404, str(exc)) from exc
    if cfg.transport != "ssh":
        raise _http_error(
            400,
            f"Cluster '{cluster}' uses local transport; the Web UI needs an SSH cluster",
        )
    with _adapter_lock:
        adapter = _adapters.get(cluster)
        if adapter is None:
            try:
                adapter = SlurmSSHClient(profile_name=cluster, submission_source="web")
            except ValueError as exc:
                raise _http_error(400, str(exc)) from exc
            _adapters[cluster] = adapter
        return adapter


def get_adapter() -> SlurmSSHClient:
    """FastAPI dependency: the adapter of the request's cluster."""
    from .context import selected_cluster

    name = selected_cluster()
    if name is None:
        raise _http_error(
            503,
            "No cluster selected. Pass ?cluster=<name> or set default_cluster "
            "in the srunx config.",
        )
    return adapter_for(name)


def get_adapter_or_none() -> SlurmSSHClient | None:
    """Adapter for the selected (or default) cluster, or ``None``."""
    from .context import selected_cluster

    name = selected_cluster()
    if name is None:
        return None
    try:
        return adapter_for(name)
    except Exception:  # noqa: BLE001 — "none" covers every failure mode
        return None


def get_active_profile_name() -> str | None:
    """Name of the cluster the current request targets (SSH or not)."""
    from .context import selected_cluster

    return selected_cluster()


def evict_adapter(cluster: str) -> None:
    """Drop and disconnect the cached adapter of *cluster* (config changed)."""
    with _adapter_lock:
        adapter = _adapters.pop(cluster, None)
    if adapter is not None:
        try:
            adapter.disconnect()
        except Exception:  # noqa: BLE001 — best-effort
            pass


def close_adapters() -> None:
    with _adapter_lock:
        adapters = list(_adapters.values())
        _adapters.clear()
    for adapter in adapters:
        try:
            adapter.disconnect()
        except Exception:  # noqa: BLE001 — best-effort shutdown
            pass


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
