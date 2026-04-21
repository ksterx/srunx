"""Schema migrations for the srunx state DB.

Design reference: ``.claude/specs/notification-and-state-persistence/design.md``.

Idempotency model: migrations are keyed by ``name`` (not by version
comparison). The ``schema_version`` table stores one row per applied
migration; ``apply_migrations`` skips any row that already exists there.
"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from srunx.logging import get_logger

logger = get_logger(__name__)


def _now_iso() -> str:
    return datetime.now(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


# ---------------------------------------------------------------------------
# v1 schema
# ---------------------------------------------------------------------------

SCHEMA_V1 = """
-- schema_version is bootstrap infrastructure created by
-- _ensure_schema_version_table() before any migration runs; it is NOT
-- re-declared here to avoid "table already exists" errors.

CREATE TABLE workflow_runs (
    id                 INTEGER PRIMARY KEY AUTOINCREMENT,
    workflow_name      TEXT NOT NULL,
    workflow_yaml_path TEXT,
    status             TEXT NOT NULL
                         CHECK (status IN ('pending','running','completed','failed','cancelled')),
    started_at         TEXT NOT NULL,
    completed_at       TEXT,
    args               TEXT,
    error              TEXT,
    triggered_by       TEXT NOT NULL CHECK (triggered_by IN ('cli','web','schedule'))
);
CREATE INDEX idx_workflow_runs_status     ON workflow_runs(status);
CREATE INDEX idx_workflow_runs_started_at ON workflow_runs(started_at);

CREATE TABLE jobs (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id            INTEGER NOT NULL UNIQUE,
    name              TEXT NOT NULL,
    command           TEXT,
    status            TEXT NOT NULL,
    nodes             INTEGER,
    gpus_per_node     INTEGER,
    memory_per_node   TEXT,
    time_limit        TEXT,
    partition         TEXT,
    nodelist          TEXT,
    conda             TEXT,
    venv              TEXT,
    container         TEXT,
    env_vars          TEXT,
    submitted_at      TEXT NOT NULL,
    started_at        TEXT,
    completed_at      TEXT,
    duration_secs     INTEGER,
    workflow_run_id   INTEGER REFERENCES workflow_runs(id) ON DELETE SET NULL,
    submission_source TEXT NOT NULL CHECK (submission_source IN ('cli','web','workflow')),
    log_file          TEXT,
    metadata          TEXT
);
CREATE INDEX idx_jobs_status          ON jobs(status);
CREATE INDEX idx_jobs_submitted_at    ON jobs(submitted_at);
CREATE INDEX idx_jobs_workflow_run_id ON jobs(workflow_run_id);

CREATE TABLE workflow_run_jobs (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    workflow_run_id INTEGER NOT NULL REFERENCES workflow_runs(id) ON DELETE CASCADE,
    job_id          INTEGER REFERENCES jobs(job_id) ON DELETE SET NULL,
    job_name        TEXT NOT NULL,
    depends_on      TEXT,
    UNIQUE (workflow_run_id, job_name)
);
CREATE INDEX idx_wrj_run ON workflow_run_jobs(workflow_run_id);

CREATE TABLE job_state_transitions (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id      INTEGER REFERENCES jobs(job_id) ON DELETE SET NULL,
    from_status TEXT,
    to_status   TEXT NOT NULL,
    observed_at TEXT NOT NULL,
    source      TEXT NOT NULL CHECK (source IN ('poller','cli_monitor','webhook'))
);
CREATE INDEX idx_jst_job_id      ON job_state_transitions(job_id, observed_at);
CREATE INDEX idx_jst_observed_at ON job_state_transitions(observed_at);

CREATE TABLE resource_snapshots (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    observed_at     TEXT NOT NULL,
    partition       TEXT,
    gpus_total      INTEGER NOT NULL,
    gpus_available  INTEGER NOT NULL,
    gpus_in_use     INTEGER NOT NULL,
    nodes_total     INTEGER NOT NULL,
    nodes_idle      INTEGER NOT NULL,
    nodes_down      INTEGER NOT NULL,
    gpu_utilization REAL GENERATED ALWAYS AS (
        CASE WHEN gpus_total > 0
             THEN CAST(gpus_in_use AS REAL) / gpus_total
             ELSE NULL END
    ) STORED
);
CREATE INDEX idx_rs_observed_at ON resource_snapshots(observed_at);
CREATE INDEX idx_rs_partition   ON resource_snapshots(partition, observed_at);

CREATE TABLE endpoints (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    kind        TEXT NOT NULL CHECK (kind IN ('slack_webhook','generic_webhook','email','slack_bot')),
    name        TEXT NOT NULL,
    config      TEXT NOT NULL,
    created_at  TEXT NOT NULL,
    disabled_at TEXT,
    UNIQUE (kind, name)
);

CREATE TABLE watches (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    kind       TEXT NOT NULL CHECK (kind IN ('job','workflow_run','resource_threshold','scheduled_report')),
    target_ref TEXT NOT NULL,
    filter     TEXT,
    created_at TEXT NOT NULL,
    closed_at  TEXT
);
CREATE INDEX idx_watches_kind_target ON watches(kind, target_ref);
CREATE INDEX idx_watches_open        ON watches(closed_at) WHERE closed_at IS NULL;

CREATE TABLE subscriptions (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    watch_id    INTEGER NOT NULL REFERENCES watches(id) ON DELETE CASCADE,
    endpoint_id INTEGER NOT NULL REFERENCES endpoints(id) ON DELETE CASCADE,
    preset      TEXT NOT NULL CHECK (preset IN ('terminal','running_and_terminal','all','digest')),
    created_at  TEXT NOT NULL,
    UNIQUE (watch_id, endpoint_id)
);
CREATE INDEX idx_subs_watch_id    ON subscriptions(watch_id);
CREATE INDEX idx_subs_endpoint_id ON subscriptions(endpoint_id);

CREATE TABLE events (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    kind         TEXT NOT NULL CHECK (kind IN (
        'job.submitted',
        'job.status_changed',
        'workflow_run.status_changed',
        'resource.threshold_crossed',
        'scheduled_report.due'
    )),
    source_ref   TEXT NOT NULL,
    payload      TEXT NOT NULL,
    payload_hash TEXT NOT NULL,
    observed_at  TEXT NOT NULL
);
CREATE UNIQUE INDEX idx_events_dedup      ON events(kind, source_ref, payload_hash);
CREATE INDEX        idx_events_source_ref ON events(source_ref, observed_at);
CREATE INDEX        idx_events_kind       ON events(kind, observed_at);

CREATE TABLE deliveries (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id        INTEGER NOT NULL REFERENCES events(id) ON DELETE CASCADE,
    subscription_id INTEGER NOT NULL REFERENCES subscriptions(id) ON DELETE CASCADE,
    endpoint_id     INTEGER NOT NULL REFERENCES endpoints(id) ON DELETE CASCADE,
    idempotency_key TEXT NOT NULL,
    status          TEXT NOT NULL
                      CHECK (status IN ('pending','sending','delivered','abandoned')),
    attempt_count   INTEGER NOT NULL DEFAULT 0,
    next_attempt_at TEXT NOT NULL,
    leased_until    TEXT,
    worker_id       TEXT,
    last_error      TEXT,
    delivered_at    TEXT,
    created_at      TEXT NOT NULL,
    UNIQUE (endpoint_id, idempotency_key)
);
CREATE INDEX idx_deliveries_claim        ON deliveries(next_attempt_at) WHERE status = 'pending';
CREATE INDEX idx_deliveries_event_id     ON deliveries(event_id);
CREATE INDEX idx_deliveries_lease_active ON deliveries(leased_until) WHERE status = 'sending';
"""


# v2: non-schema index additions to support the NotificationsCenter
# dashboard queries. ``list_recent`` does ``ORDER BY created_at DESC``
# with an optional ``WHERE status = ?``; without these indexes the
# 10-second dashboard poll degenerates into a full scan + sort on a
# growing outbox.
SCHEMA_V2_DASHBOARD_INDEXES = """
CREATE INDEX IF NOT EXISTS idx_deliveries_created_at
    ON deliveries(created_at);
CREATE INDEX IF NOT EXISTS idx_deliveries_status_created_at
    ON deliveries(status, created_at);
"""


# ---------------------------------------------------------------------------
# v3 schema: sweep_runs + workflow_runs.sweep_run_id + CHECK widening
# for events.kind and watches.kind via table rebuild.
# ---------------------------------------------------------------------------

SCHEMA_V3 = """
CREATE TABLE sweep_runs (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    name                TEXT NOT NULL,
    workflow_yaml_path  TEXT,
    status              TEXT NOT NULL
                          CHECK (status IN ('pending','running','draining','completed','failed','cancelled')),
    matrix              TEXT NOT NULL,
    args                TEXT,
    fail_fast           INTEGER NOT NULL DEFAULT 0 CHECK (fail_fast IN (0,1)),
    max_parallel        INTEGER NOT NULL,
    cell_count          INTEGER NOT NULL,
    cells_pending       INTEGER NOT NULL DEFAULT 0,
    cells_running       INTEGER NOT NULL DEFAULT 0,
    cells_completed     INTEGER NOT NULL DEFAULT 0,
    cells_failed        INTEGER NOT NULL DEFAULT 0,
    cells_cancelled     INTEGER NOT NULL DEFAULT 0,
    submission_source   TEXT NOT NULL CHECK (submission_source IN ('cli','web','mcp')),
    started_at          TEXT NOT NULL,
    completed_at        TEXT,
    cancel_requested_at TEXT,
    error               TEXT
);
CREATE INDEX idx_sweep_runs_status     ON sweep_runs(status);
CREATE INDEX idx_sweep_runs_started_at ON sweep_runs(started_at);

ALTER TABLE workflow_runs
    ADD COLUMN sweep_run_id INTEGER REFERENCES sweep_runs(id) ON DELETE SET NULL;
CREATE INDEX idx_workflow_runs_sweep_run_id ON workflow_runs(sweep_run_id);

-- Rebuild events with widened kind CHECK to admit 'sweep_run.status_changed'.
CREATE TABLE events_v3 (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    kind         TEXT NOT NULL CHECK (kind IN (
        'job.submitted',
        'job.status_changed',
        'workflow_run.status_changed',
        'sweep_run.status_changed',
        'resource.threshold_crossed',
        'scheduled_report.due'
    )),
    source_ref   TEXT NOT NULL,
    payload      TEXT NOT NULL,
    payload_hash TEXT NOT NULL,
    observed_at  TEXT NOT NULL
);
INSERT INTO events_v3 (id, kind, source_ref, payload, payload_hash, observed_at)
    SELECT id, kind, source_ref, payload, payload_hash, observed_at FROM events;
DROP TABLE events;
ALTER TABLE events_v3 RENAME TO events;
CREATE UNIQUE INDEX idx_events_dedup      ON events(kind, source_ref, payload_hash);
CREATE INDEX        idx_events_source_ref ON events(source_ref, observed_at);
CREATE INDEX        idx_events_kind       ON events(kind, observed_at);

-- Rebuild watches with widened kind CHECK to admit 'sweep_run'.
CREATE TABLE watches_v3 (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    kind       TEXT NOT NULL CHECK (kind IN (
        'job',
        'workflow_run',
        'sweep_run',
        'resource_threshold',
        'scheduled_report'
    )),
    target_ref TEXT NOT NULL,
    filter     TEXT,
    created_at TEXT NOT NULL,
    closed_at  TEXT
);
INSERT INTO watches_v3 (id, kind, target_ref, filter, created_at, closed_at)
    SELECT id, kind, target_ref, filter, created_at, closed_at FROM watches;
DROP TABLE watches;
ALTER TABLE watches_v3 RENAME TO watches;
CREATE INDEX idx_watches_kind_target ON watches(kind, target_ref);
CREATE INDEX idx_watches_open        ON watches(closed_at) WHERE closed_at IS NULL;
"""


# ---------------------------------------------------------------------------
# Migration registry
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Migration:
    version: int
    name: str
    sql: str
    # When True, ``apply_migrations`` runs the script in autocommit mode
    # with ``PRAGMA foreign_keys=OFF`` toggled around it so that a table
    # rebuild (CREATE new, INSERT SELECT, DROP old, RENAME) can complete
    # without tripping FK references from other tables.
    requires_fk_off: bool = False


MIGRATIONS: list[Migration] = [
    Migration(
        version=1,
        name="v1_initial",
        sql=SCHEMA_V1,
        requires_fk_off=False,
    ),
    Migration(
        version=2,
        name="v2_dashboard_indexes",
        sql=SCHEMA_V2_DASHBOARD_INDEXES,
        requires_fk_off=False,
    ),
    Migration(
        version=3,
        name="v3_sweep_runs",
        sql=SCHEMA_V3,
        requires_fk_off=True,
    ),
]


def _ensure_schema_version_table(conn: sqlite3.Connection) -> None:
    """Create ``schema_version`` if it does not yet exist.

    This is needed because the table itself is declared inside the v1
    migration, so a fresh DB lacks it before any migration runs.
    """
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS schema_version (
            version    INTEGER NOT NULL,
            name       TEXT NOT NULL,
            applied_at TEXT NOT NULL,
            PRIMARY KEY (version, name)
        )
        """
    )


def _applied_names(conn: sqlite3.Connection) -> set[str]:
    _ensure_schema_version_table(conn)
    cur = conn.execute("SELECT name FROM schema_version")
    return {row[0] for row in cur.fetchall()}


def apply_migrations(conn: sqlite3.Connection) -> list[str]:
    """Apply every pending migration in :data:`MIGRATIONS` order.

    Migrations with ``requires_fk_off=False`` (the default) run inside
    their own ``BEGIN IMMEDIATE`` transaction; on failure the whole
    migration is rolled back and no ``schema_version`` row is written.

    Migrations with ``requires_fk_off=True`` run in autocommit mode with
    ``PRAGMA foreign_keys=OFF`` toggled around them (SQLite ignores this
    pragma inside a transaction). Table-rebuild migrations need this to
    DROP the old table without tripping inbound foreign-key references
    from sibling tables. The pragma is always restored in a ``finally``
    block.

    Returns the list of migration names that were applied in this call.

    Concurrency safety: the ``applied`` set is re-read **after**
    acquiring the IMMEDIATE write lock (for TX-wrapped migrations) or
    immediately before the pragma toggle (for FK-off migrations).
    Without that re-check, two concurrent callers on a cold DB both see
    an empty ``applied`` set outside the lock; one wins, runs the
    CREATE TABLE scripts, commits; the other then attempts the same
    CREATE TABLE statements on tables that now exist — which fails
    because ``SCHEMA_V1`` uses bare ``CREATE TABLE`` (not
    ``IF NOT EXISTS``) for the real domain tables.
    """
    newly_applied: list[str] = []

    for mig in MIGRATIONS:
        if mig.name in _applied_names(conn):
            continue
        logger.info("Applying migration %s (v%d)", mig.name, mig.version)
        if mig.requires_fk_off:
            _apply_fk_off_migration(conn, mig)
        else:
            _apply_tx_migration(conn, mig)
        newly_applied.append(mig.name)

    return newly_applied


def _apply_tx_migration(conn: sqlite3.Connection, mig: Migration) -> None:
    """Apply a migration inside a single BEGIN IMMEDIATE transaction."""
    try:
        conn.execute("BEGIN IMMEDIATE")
        # Re-check inside the write lock. If a peer has applied the
        # same migration between our initial check and the BEGIN we
        # just acquired, skip the DDL to avoid duplicate CREATE TABLE.
        if mig.name in _applied_names(conn):
            conn.rollback()
            return
        conn.executescript(mig.sql)
        # v1_initial creates schema_version itself; the IF NOT EXISTS
        # guard earlier ensures the insert below always succeeds.
        conn.execute(
            "INSERT INTO schema_version (version, name, applied_at) VALUES (?, ?, ?)",
            (mig.version, mig.name, _now_iso()),
        )
        conn.commit()
    except Exception:
        conn.rollback()
        logger.error("Migration %s failed; rolled back", mig.name)
        raise


def _apply_fk_off_migration(conn: sqlite3.Connection, mig: Migration) -> None:
    """Apply a table-rebuild migration with foreign_keys temporarily OFF.

    ``PRAGMA foreign_keys`` is a no-op inside an active transaction in
    SQLite, so the pragma must be toggled in autocommit mode *outside*
    any ``BEGIN`` block. ``executescript`` itself commits any pending
    transaction and runs its statements in autocommit mode, which is
    what we want for the table-rebuild script.

    The pragma is always restored to ``ON`` in the ``finally`` clause,
    even on failure, so subsequent connections observe FK enforcement.
    """
    # Re-check right before the pragma toggle; if a concurrent caller
    # applied this migration between our original check and here, skip.
    if mig.name in _applied_names(conn):
        return

    try:
        conn.execute("PRAGMA foreign_keys = OFF")
        try:
            conn.executescript(mig.sql)
            conn.execute(
                "INSERT INTO schema_version (version, name, applied_at) VALUES (?, ?, ?)",
                (mig.version, mig.name, _now_iso()),
            )
        finally:
            conn.execute("PRAGMA foreign_keys = ON")
    except (sqlite3.IntegrityError, sqlite3.OperationalError):
        # Concurrent migrator won the race (either the PK on
        # schema_version fired, or one of the DDL statements tripped
        # "table already exists"). Re-check the ledger; if the peer did
        # record this migration, skip silently.
        if mig.name in _applied_names(conn):
            logger.info(
                "Migration %s already applied by concurrent caller; skipping",
                mig.name,
            )
            return
        logger.error("Migration %s failed", mig.name)
        raise
    except Exception:
        logger.error("Migration %s failed", mig.name)
        raise


# ---------------------------------------------------------------------------
# Legacy config bootstrap
# ---------------------------------------------------------------------------


_BOOTSTRAP_NAME = "bootstrap_slack_webhook_url"
_BOOTSTRAP_VERSION = 1


def _bootstrap_already_applied(conn: sqlite3.Connection) -> bool:
    cur = conn.execute(
        "SELECT 1 FROM schema_version WHERE name = ? LIMIT 1",
        (_BOOTSTRAP_NAME,),
    )
    return cur.fetchone() is not None


def bootstrap_from_config(conn: sqlite3.Connection, config: Any) -> bool:
    """Migrate ``config.notifications.slack_webhook_url`` into ``endpoints``.

    Runs at most once per DB (guarded by a ``schema_version`` row named
    ``bootstrap_slack_webhook_url``). If the config has no webhook URL the
    guard row is still recorded so subsequent startups short-circuit.

    On INSERT failure (e.g. a conflicting endpoint already exists), both
    the INSERT and the guard row are rolled back, so the migration can be
    retried on the next startup.

    Returns True if an endpoint row was inserted, False otherwise.
    """
    if _bootstrap_already_applied(conn):
        return False

    webhook_url: str | None = None
    try:
        webhook_url = config.notifications.slack_webhook_url
    except AttributeError:
        webhook_url = None

    # Case: nothing to migrate — still record so we don't re-read next time.
    if not webhook_url:
        conn.execute(
            "INSERT INTO schema_version (version, name, applied_at) VALUES (?, ?, ?)",
            (_BOOTSTRAP_VERSION, _BOOTSTRAP_NAME, _now_iso()),
        )
        conn.commit()
        return False

    # Case: migrate, record guard, commit atomically.
    payload = json.dumps({"webhook_url": webhook_url})
    try:
        conn.execute("BEGIN IMMEDIATE")
        conn.execute(
            "INSERT INTO endpoints (kind, name, config, created_at) "
            "VALUES (?, ?, ?, ?)",
            ("slack_webhook", "default", payload, _now_iso()),
        )
        conn.execute(
            "INSERT INTO schema_version (version, name, applied_at) VALUES (?, ?, ?)",
            (_BOOTSTRAP_VERSION, _BOOTSTRAP_NAME, _now_iso()),
        )
        conn.commit()
        logger.info("Bootstrapped 'default' Slack webhook endpoint from config.json")
        return True
    except Exception:
        conn.rollback()
        logger.warning(
            "Failed to bootstrap Slack webhook from config.json; "
            "will retry on next startup",
            exc_info=True,
        )
        return False
