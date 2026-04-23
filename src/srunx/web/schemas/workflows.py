"""Pydantic DTOs for ``/api/workflows/*`` endpoints.

Extracted from ``srunx.web.routers.workflows`` so the router and service
modules can both import them without creating a circular import.
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field, model_validator


class WorkflowJobInput(BaseModel):
    name: str
    command: list[str]
    depends_on: list[str] = []
    template: str | None = None
    exports: dict[str, str] = Field(default_factory=dict)
    resources: dict[str, Any] | None = None
    environment: dict[str, Any] | None = None
    work_dir: str | None = None
    log_dir: str | None = None
    retry: int | None = None
    retry_delay: int | None = None
    srun_args: str | None = None
    launch_prefix: str | None = None

    model_config = {"extra": "forbid"}

    @model_validator(mode="before")
    @classmethod
    def reject_legacy_outputs(cls, data: Any) -> Any:
        if isinstance(data, dict) and "outputs" in data:
            raise ValueError(
                "The 'outputs' field was renamed to 'exports' (see CHANGELOG). "
                "Dependent jobs now reference values as '{{ deps.<job_name>.<key> }}'."
            )
        return data


class WorkflowCreateRequest(BaseModel):
    name: str = Field(..., pattern=r"^[\w\-]+$")
    args: dict[str, Any] = Field(default_factory=dict)
    jobs: list[WorkflowJobInput]
    default_project: str | None = None
    overwrite: bool = False


class SweepSpecRequest(BaseModel):
    """Sweep payload accepted by ``POST /api/workflows/{name}/run``.

    Mirrors :class:`srunx.sweep.SweepSpec` but with a server-side default
    for ``max_parallel`` (R7.9) so the client can omit it for small
    sweeps.
    """

    model_config = {"extra": "forbid"}

    matrix: dict[str, list[Any]] = Field(default_factory=dict)
    fail_fast: bool = False
    max_parallel: int = 4


class WorkflowRunRequest(BaseModel):
    from_job: str | None = None
    to_job: str | None = None
    single_job: str | None = None
    dry_run: bool = False
    # Notification subscription wiring. When ``notify`` is true and
    # ``endpoint_id`` resolves to an enabled endpoint row, the run's
    # auto-created ``kind='workflow_run'`` watch is paired with a
    # subscription so the delivery poller fans status-transition
    # events out to that endpoint. Matches the shape accepted by
    # ``/api/jobs`` submit (R6 in design.md §Request models).
    notify: bool = False
    endpoint_id: int | None = Field(default=None, gt=0)
    preset: str = "terminal"
    # Sweep wiring (Phase G). ``args_override`` expands workflow-level
    # ``args`` before Jinja rendering; ``sweep`` switches the request
    # onto the :class:`SweepOrchestrator` path.
    args_override: dict[str, Any] = Field(default_factory=dict)
    sweep: SweepSpecRequest | None = None
