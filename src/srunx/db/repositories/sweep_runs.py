"""Repository for the ``sweep_runs`` table.

Design reference: ``.claude/specs/workflow-parameter-sweep/design.md``
§ SweepRunRepository.
"""

from __future__ import annotations

import sqlite3
from typing import Any

from srunx.db.models import SweepRun, SweepSubmissionSource
from srunx.db.repositories.base import BaseRepository, now_iso


class SweepRunRepository(BaseRepository):
    """CRUD for the ``sweep_runs`` table plus the atomic cell transition."""

    JSON_FIELDS = ("matrix", "args")
    DATETIME_FIELDS = ("started_at", "completed_at", "cancel_requested_at")

    _COLUMNS = (
        "id",
        "name",
        "workflow_yaml_path",
        "status",
        "matrix",
        "args",
        "fail_fast",
        "max_parallel",
        "cell_count",
        "cells_pending",
        "cells_running",
        "cells_completed",
        "cells_failed",
        "cells_cancelled",
        "submission_source",
        "started_at",
        "completed_at",
        "cancel_requested_at",
        "error",
    )

    _INCOMPLETE_STATUSES: tuple[str, ...] = ("pending", "running", "draining")

    def create(
        self,
        *,
        name: str,
        matrix: dict[str, Any],
        args: dict[str, Any] | None,
        fail_fast: bool,
        max_parallel: int,
        cell_count: int,
        submission_source: SweepSubmissionSource,
        workflow_yaml_path: str | None = None,
        status: str = "pending",
        cells_pending: int | None = None,
        error: str | None = None,
    ) -> int:
        """Insert a new sweep_runs row.

        ``cells_pending`` defaults to ``cell_count`` (every cell starts
        in ``pending``). Callers override it only for failure-audit rows
        (R4.7) where ``cell_count=0``.
        """
        initial_pending = cell_count if cells_pending is None else cells_pending
        cur = self.conn.execute(
            """
            INSERT INTO sweep_runs (
                name, workflow_yaml_path, status, matrix, args,
                fail_fast, max_parallel, cell_count,
                cells_pending, cells_running, cells_completed,
                cells_failed, cells_cancelled,
                submission_source, started_at, error
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 0, 0, 0, ?, ?, ?)
            """,
            (
                name,
                workflow_yaml_path,
                status,
                self._encode_json(matrix),
                self._encode_json(args),
                1 if fail_fast else 0,
                max_parallel,
                cell_count,
                initial_pending,
                submission_source,
                now_iso(),
                error,
            ),
        )
        return int(cur.lastrowid or 0)

    def get(self, id: int) -> SweepRun | None:
        row = self.conn.execute(
            f"SELECT {', '.join(self._COLUMNS)} FROM sweep_runs WHERE id = ?",
            (id,),
        ).fetchone()
        return self._row_to_model(row, SweepRun)

    def list_all(self, limit: int = 200) -> list[SweepRun]:
        """Return sweep_runs newest-first, capped at ``limit``."""
        rows = self.conn.execute(
            f"SELECT {', '.join(self._COLUMNS)} FROM sweep_runs "
            "ORDER BY started_at DESC, id DESC LIMIT ?",
            (limit,),
        ).fetchall()
        return [self._row_to_model(r, SweepRun) for r in rows if r is not None]  # type: ignore[misc]

    def list_incomplete(self) -> list[SweepRun]:
        """Return sweeps still in ``pending``, ``running``, or ``draining``."""
        placeholders = ", ".join("?" for _ in self._INCOMPLETE_STATUSES)
        rows = self.conn.execute(
            f"SELECT {', '.join(self._COLUMNS)} FROM sweep_runs "
            f"WHERE status IN ({placeholders}) "
            "ORDER BY started_at DESC, id DESC",
            self._INCOMPLETE_STATUSES,
        ).fetchall()
        return [self._row_to_model(r, SweepRun) for r in rows if r is not None]  # type: ignore[misc]

    def update_status(
        self,
        id: int,
        status: str,
        *,
        error: str | None = None,
        completed_at: str | None = None,
    ) -> bool:
        """Update the sweep's status (and optionally error/completed_at).

        Returns True if a row was updated.
        """
        sets: list[str] = ["status = ?"]
        vals: list[Any] = [status]
        if error is not None:
            sets.append("error = ?")
            vals.append(error)
        if completed_at is not None:
            sets.append("completed_at = ?")
            vals.append(completed_at)
        vals.append(id)

        cur = self.conn.execute(
            f"UPDATE sweep_runs SET {', '.join(sets)} WHERE id = ?",
            vals,
        )
        return cur.rowcount > 0

    def request_cancel(self, id: int) -> bool:
        """Stamp ``cancel_requested_at`` on the sweep without touching status.

        Returns True if the row exists and had no prior cancel timestamp.
        """
        cur = self.conn.execute(
            "UPDATE sweep_runs SET cancel_requested_at = ? "
            "WHERE id = ? AND cancel_requested_at IS NULL",
            (now_iso(), id),
        )
        return cur.rowcount > 0

    _COUNTER_COLUMNS: frozenset[str] = frozenset(
        {"pending", "running", "completed", "failed", "cancelled"}
    )

    def transition_cell(
        self,
        *,
        conn: sqlite3.Connection,
        workflow_run_id: int,
        from_status: str,
        to_status: str,
        error: str | None = None,
        completed_at: str | None = None,
    ) -> bool:
        """Atomically transition a child workflow_run and sync sweep counters.

        ``conn`` is required: the caller owns the enclosing
        ``BEGIN IMMEDIATE`` transaction. This method never opens its own.

        Steps (all under caller TX):

        1. ``UPDATE workflow_runs SET status=?, completed_at=?, error=?
           WHERE id=? AND status=?`` — optimistic lock on current status.
           If rowcount is 0 another actor won the race; return False.
        2. Read the workflow_run's ``sweep_run_id``. If NULL the row is
           not sweep-backed; return True without touching sweep counters.
        3. ``UPDATE sweep_runs SET cells_<from>=cells_<from>-1,
           cells_<to>=cells_<to>+1 WHERE id=?`` in a single statement.
        4. Return True.

        Idempotent: a second call with the same ``from_status`` observes
        rowcount=0 at step 1 and returns False (no counter change).
        """
        if from_status not in self._COUNTER_COLUMNS:
            raise ValueError(f"Unknown sweep counter source status: {from_status!r}")
        if to_status not in self._COUNTER_COLUMNS:
            raise ValueError(f"Unknown sweep counter target status: {to_status!r}")

        update_sets: list[str] = ["status = ?"]
        update_vals: list[Any] = [to_status]
        if completed_at is not None:
            update_sets.append("completed_at = ?")
            update_vals.append(completed_at)
        if error is not None:
            update_sets.append("error = ?")
            update_vals.append(error)
        update_vals.extend([workflow_run_id, from_status])

        cur = conn.execute(
            f"UPDATE workflow_runs SET {', '.join(update_sets)} "
            "WHERE id = ? AND status = ?",
            update_vals,
        )
        if cur.rowcount == 0:
            return False

        row = conn.execute(
            "SELECT sweep_run_id FROM workflow_runs WHERE id = ?",
            (workflow_run_id,),
        ).fetchone()
        sweep_run_id = row["sweep_run_id"] if row is not None else None
        if sweep_run_id is None:
            return True

        if from_status == to_status:
            # No counter movement when the status didn't actually change.
            return True

        from_col = f"cells_{from_status}"
        to_col = f"cells_{to_status}"
        conn.execute(
            f"UPDATE sweep_runs SET {from_col} = {from_col} - 1, "
            f"{to_col} = {to_col} + 1 WHERE id = ?",
            (sweep_run_id,),
        )
        return True
