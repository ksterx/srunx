"""Repository for the ``events`` table.

See design.md § ``EventRepository``. Events are the input side of the
Outbox pattern: every state transition / scheduled tick that may need
to produce notifications lands here first. A deterministic
``payload_hash`` provides producer-side deduplication — the UNIQUE
index on ``(kind, source_ref, payload_hash)`` protects against the
"poller accidentally started twice" class of bugs (see Error Handling
scenario #4 in design.md).
"""

from __future__ import annotations

import hashlib
import json
import sqlite3

from srunx.db.models import Event
from srunx.db.repositories.base import BaseRepository, now_iso


class EventRepository(BaseRepository):
    """CRUD for the ``events`` table."""

    JSON_FIELDS = ("payload",)
    DATETIME_FIELDS = ("observed_at",)

    _COLUMNS = (
        "id",
        "kind",
        "source_ref",
        "payload",
        "payload_hash",
        "observed_at",
    )

    # -- internal helpers --------------------------------------------------

    @staticmethod
    def _extract_source_id(source_ref: str) -> str:
        """Return the id portion of a structured ``source_ref``.

        Grammar (V5+):
        - ``job:local:<N>`` → ``<N>``
        - ``job:ssh:<profile>:<N>`` → ``<N>``
        - ``workflow_run:<N>`` → ``<N>``
        - ``sweep_run:<N>`` → ``<N>``
        - ``resource:<partition>:<threshold>:<window>`` → remainder after first ``:``

        For ``job:*`` refs we always take the trailing numeric segment
        so the dedup hash is stable across transports. Non-``job`` refs
        keep the pre-V5 "remainder after first ``:``" semantics.

        Falls back to the full string when no ``:`` is present.
        """
        if ":" not in source_ref:
            return source_ref
        kind, _, remainder = source_ref.partition(":")
        if kind == "job":
            # job:local:N → N ; job:ssh:profile:N → N
            return remainder.rsplit(":", 1)[-1]
        return remainder

    @staticmethod
    def _compute_payload_hash(kind: str, source_ref: str, payload: dict) -> str:
        """Return a deterministic SHA-256 hex digest for dedup.

        The input is a kind-specific logical key string (matching the
        idempotency-key policy in design.md § "Idempotency Key 生成ポリシー"
        and documented inline on the ``events`` table). A second INSERT
        with the same ``(kind, source_ref, payload_hash)`` triple is
        silently ignored by the UNIQUE index.
        """
        source_id = EventRepository._extract_source_id(source_ref)

        if kind == "job.submitted":
            logical = f"job:{source_id}:submitted"
        elif kind == "job.status_changed":
            to_status = payload.get("to_status", "")
            logical = f"job:{source_id}:status:{to_status}"
        elif kind == "workflow_run.status_changed":
            to_status = payload.get("to_status", "")
            logical = f"workflow_run:{source_id}:status:{to_status}"
        elif kind == "sweep_run.status_changed":
            to_status = payload.get("to_status", "")
            logical = f"sweep_run:{source_id}:status:{to_status}"
        elif kind == "resource.threshold_crossed":
            partition = payload.get("partition", "")
            threshold_id = payload.get("threshold_id", "")
            window_iso = payload.get("window_iso", "")
            logical = f"resource:{partition}:{threshold_id}:{window_iso}"
        elif kind == "scheduled_report.due":
            schedule_id = payload.get("schedule_id", "")
            scheduled_run_at_iso = payload.get("scheduled_run_at_iso", "")
            logical = f"scheduled_report:{schedule_id}:{scheduled_run_at_iso}"
        else:
            # Defensive fallback: any unknown kind still dedups stably,
            # keyed on the full payload content.
            logical = f"{kind}:{source_ref}:{json.dumps(payload, sort_keys=True)}"

        return hashlib.sha256(logical.encode("utf-8")).hexdigest()

    # -- CRUD --------------------------------------------------------------

    def insert(
        self,
        kind: str,
        source_ref: str,
        payload: dict,
        observed_at: str | None = None,
    ) -> int | None:
        """Insert a new event row.

        Uses ``INSERT OR IGNORE`` against the UNIQUE
        ``(kind, source_ref, payload_hash)`` index. Returns the new
        row's ``id`` on success, or ``None`` when the UNIQUE
        constraint silently absorbed the insert (i.e. a duplicate
        event — the caller should treat this as a no-op).
        """
        payload_hash = self._compute_payload_hash(kind, source_ref, payload)
        observed_at = observed_at or now_iso()

        cur = self.conn.execute(
            """
            INSERT OR IGNORE INTO events (
                kind, source_ref, payload, payload_hash, observed_at
            ) VALUES (?, ?, ?, ?, ?)
            """,
            (
                kind,
                source_ref,
                self._encode_json(payload),
                payload_hash,
                observed_at,
            ),
        )
        if cur.rowcount == 0:
            return None
        last = cur.lastrowid
        return int(last) if last else None

    def get(self, id: int) -> Event | None:
        row = self.conn.execute(
            f"SELECT {', '.join(self._COLUMNS)} FROM events WHERE id = ?",
            (id,),
        ).fetchone()
        return self._row_to_model(row, Event)

    def list_recent(self, limit: int = 100) -> list[Event]:
        """Return recent events ordered by ``observed_at`` descending."""
        rows: list[sqlite3.Row] = self.conn.execute(
            f"SELECT {', '.join(self._COLUMNS)} FROM events "
            "ORDER BY observed_at DESC LIMIT ?",
            (limit,),
        ).fetchall()
        return [self._row_to_model(r, Event) for r in rows if r is not None]  # type: ignore[misc]
