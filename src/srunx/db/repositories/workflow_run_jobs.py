"""Repository for the ``workflow_run_jobs`` table.

Design reference: ``.claude/specs/notification-and-state-persistence/design.md``
§ Repositories.

V5 note: the on-disk column is ``jobs_row_id`` (FK to ``jobs.id``). The
public API still accepts ``job_id`` (SLURM id, ``scheduler_key='local'``
by default) so pre-V5 callers work unchanged — the repository performs
the ``jobs.id`` lookup internally. Callers that have ``jobs.id`` in
hand can use the new ``jobs_row_id`` kwarg directly.
"""

from __future__ import annotations

from srunx.db.models import WorkflowRunJob
from srunx.db.repositories.base import BaseRepository


class WorkflowRunJobRepository(BaseRepository):
    """CRUD for the ``workflow_run_jobs`` table."""

    JSON_FIELDS = ("depends_on",)

    # Select joins ``jobs`` so the legacy ``job_id`` attribute on
    # :class:`WorkflowRunJob` carries the SLURM id. ``jobs_row_id`` is
    # the authoritative V5 FK.
    _SELECT_COLUMNS = (
        "wrj.id AS id",
        "wrj.workflow_run_id AS workflow_run_id",
        "wrj.jobs_row_id AS jobs_row_id",
        "j.job_id AS job_id",
        "wrj.job_name AS job_name",
        "wrj.depends_on AS depends_on",
    )

    def _resolve_jobs_row_id(self, job_id: int, scheduler_key: str) -> int | None:
        row = self.conn.execute(
            "SELECT id FROM jobs WHERE scheduler_key = ? AND job_id = ?",
            (scheduler_key, job_id),
        ).fetchone()
        return int(row["id"]) if row is not None else None

    def create(
        self,
        workflow_run_id: int,
        job_name: str,
        depends_on: list[str] | None = None,
        job_id: int | None = None,
        *,
        scheduler_key: str = "local",
        jobs_row_id: int | None = None,
    ) -> int:
        """Insert (or replace) a workflow_run_jobs row.

        ``job_id`` (SLURM id) is resolved to ``jobs.id`` via
        ``(scheduler_key, job_id)``. Alternatively callers that already
        know the jobs row id can pass ``jobs_row_id`` directly. Returns
        the new row's ``id``.
        """
        resolved_row_id: int | None
        if jobs_row_id is not None:
            resolved_row_id = jobs_row_id
        elif job_id is not None:
            resolved_row_id = self._resolve_jobs_row_id(job_id, scheduler_key)
        else:
            resolved_row_id = None

        cur = self.conn.execute(
            """
            INSERT OR REPLACE INTO workflow_run_jobs (
                workflow_run_id, jobs_row_id, job_name, depends_on
            ) VALUES (?, ?, ?, ?)
            """,
            (
                workflow_run_id,
                resolved_row_id,
                job_name,
                self._encode_json(depends_on),
            ),
        )
        return int(cur.lastrowid or 0)

    def update_job_id(
        self,
        row_id: int,
        job_id: int,
        *,
        scheduler_key: str = "local",
    ) -> bool:
        """Associate a SLURM ``job_id`` with a previously-created row.

        Resolves ``job_id`` to ``jobs.id`` via ``(scheduler_key, job_id)``
        and writes the V5 ``jobs_row_id`` column. Returns True if a row
        was updated.
        """
        resolved_row_id = self._resolve_jobs_row_id(job_id, scheduler_key)
        cur = self.conn.execute(
            "UPDATE workflow_run_jobs SET jobs_row_id = ? WHERE id = ?",
            (resolved_row_id, row_id),
        )
        return cur.rowcount > 0

    def list_by_run(self, workflow_run_id: int) -> list[WorkflowRunJob]:
        """Return every row belonging to ``workflow_run_id`` in insertion order."""
        rows = self.conn.execute(
            f"SELECT {', '.join(self._SELECT_COLUMNS)} "
            "FROM workflow_run_jobs wrj "
            "LEFT JOIN jobs j ON j.id = wrj.jobs_row_id "
            "WHERE wrj.workflow_run_id = ? ORDER BY wrj.id ASC",
            (workflow_run_id,),
        ).fetchall()
        return [self._row_to_model(r, WorkflowRunJob) for r in rows if r is not None]  # type: ignore[misc]
