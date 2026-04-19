"""Repository for the ``workflow_run_jobs`` table.

Design reference: ``.claude/specs/notification-and-state-persistence/design.md``
§ Repositories.
"""

from __future__ import annotations

from srunx.db.models import WorkflowRunJob
from srunx.db.repositories.base import BaseRepository


class WorkflowRunJobRepository(BaseRepository):
    """CRUD for the ``workflow_run_jobs`` table."""

    JSON_FIELDS = ("depends_on",)

    _COLUMNS = (
        "id",
        "workflow_run_id",
        "job_id",
        "job_name",
        "depends_on",
    )

    def create(
        self,
        workflow_run_id: int,
        job_name: str,
        depends_on: list[str] | None = None,
        job_id: int | None = None,
    ) -> int:
        """Insert (or replace) a workflow_run_jobs row.

        ``job_id`` is typically ``None`` until the underlying SLURM job has
        been submitted; callers then invoke :meth:`update_job_id`. Returns
        the new row's ``id``.
        """
        cur = self.conn.execute(
            """
            INSERT OR REPLACE INTO workflow_run_jobs (
                workflow_run_id, job_id, job_name, depends_on
            ) VALUES (?, ?, ?, ?)
            """,
            (
                workflow_run_id,
                job_id,
                job_name,
                self._encode_json(depends_on),
            ),
        )
        return int(cur.lastrowid or 0)

    def update_job_id(self, row_id: int, job_id: int) -> bool:
        """Associate a SLURM ``job_id`` with a previously-created row.

        Returns True if a row was updated.
        """
        cur = self.conn.execute(
            "UPDATE workflow_run_jobs SET job_id = ? WHERE id = ?",
            (job_id, row_id),
        )
        return cur.rowcount > 0

    def list_by_run(self, workflow_run_id: int) -> list[WorkflowRunJob]:
        """Return every row belonging to ``workflow_run_id`` in insertion order."""
        rows = self.conn.execute(
            f"SELECT {', '.join(self._COLUMNS)} FROM workflow_run_jobs "
            "WHERE workflow_run_id = ? ORDER BY id ASC",
            (workflow_run_id,),
        ).fetchall()
        return [self._row_to_model(r, WorkflowRunJob) for r in rows if r is not None]  # type: ignore[misc]
