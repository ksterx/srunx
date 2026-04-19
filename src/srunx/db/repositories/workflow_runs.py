"""Repository for the ``workflow_runs`` table.

Design reference: ``.claude/specs/notification-and-state-persistence/design.md``
§ Repositories.
"""

from __future__ import annotations

from typing import Any

from srunx.db.models import WorkflowRun, WorkflowRunTriggeredBy
from srunx.db.repositories.base import BaseRepository, now_iso


class WorkflowRunRepository(BaseRepository):
    """CRUD for the ``workflow_runs`` table."""

    JSON_FIELDS = ("args",)
    DATETIME_FIELDS = ("started_at", "completed_at")

    _COLUMNS = (
        "id",
        "workflow_name",
        "workflow_yaml_path",
        "status",
        "started_at",
        "completed_at",
        "args",
        "error",
        "triggered_by",
    )

    def create(
        self,
        workflow_name: str,
        yaml_path: str | None,
        args: dict | None,
        triggered_by: WorkflowRunTriggeredBy,
    ) -> int:
        """Insert a new workflow run in ``pending`` status.

        Uses ``INSERT OR REPLACE`` for parity with the other repositories.
        Returns the new row's ``id``.
        """
        cur = self.conn.execute(
            """
            INSERT OR REPLACE INTO workflow_runs (
                workflow_name, workflow_yaml_path, status, started_at,
                args, triggered_by
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                workflow_name,
                yaml_path,
                "pending",
                now_iso(),
                self._encode_json(args),
                triggered_by,
            ),
        )
        return int(cur.lastrowid or 0)

    def get(self, id: int) -> WorkflowRun | None:
        row = self.conn.execute(
            f"SELECT {', '.join(self._COLUMNS)} FROM workflow_runs WHERE id = ?",
            (id,),
        ).fetchone()
        return self._row_to_model(row, WorkflowRun)

    def list_all(self) -> list[WorkflowRun]:
        """Return every workflow run ordered by ``started_at`` DESC."""
        rows = self.conn.execute(
            f"SELECT {', '.join(self._COLUMNS)} FROM workflow_runs "
            "ORDER BY started_at DESC"
        ).fetchall()
        return [self._row_to_model(r, WorkflowRun) for r in rows if r is not None]  # type: ignore[misc]

    def list_incomplete(self) -> list[WorkflowRun]:
        """Return runs still in ``pending`` or ``running`` (for resume)."""
        rows = self.conn.execute(
            f"SELECT {', '.join(self._COLUMNS)} FROM workflow_runs "
            "WHERE status IN ('pending','running') "
            "ORDER BY started_at DESC"
        ).fetchall()
        return [self._row_to_model(r, WorkflowRun) for r in rows if r is not None]  # type: ignore[misc]

    def update_status(
        self,
        id: int,
        status: str,
        error: str | None = None,
        completed_at: str | None = None,
    ) -> bool:
        """Update a workflow run's status and lifecycle fields.

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
            f"UPDATE workflow_runs SET {', '.join(sets)} WHERE id = ?",
            vals,
        )
        return cur.rowcount > 0
