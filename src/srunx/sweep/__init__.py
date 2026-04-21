"""Sweep domain: matrix expansion, cell spec, orchestrator helpers.

Design reference: ``.claude/specs/workflow-parameter-sweep/design.md``.
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator

ScalarValue = str | int | float | bool


class SweepSpec(BaseModel):
    """Declarative matrix spec: axes, fail-fast, parallelism cap."""

    model_config = ConfigDict(extra="forbid")

    matrix: dict[str, list[ScalarValue]] = Field(default_factory=dict)
    fail_fast: bool = False
    max_parallel: int

    @field_validator("max_parallel")
    @classmethod
    def _validate_max_parallel(cls, value: int) -> int:
        if value < 1:
            raise ValueError("max_parallel must be >= 1")
        return value


class CellSpec(BaseModel):
    """Materialized cell: workflow_run id plus effective args."""

    model_config = ConfigDict(extra="forbid")

    workflow_run_id: int
    effective_args: dict[str, Any]
    cell_index: int


__all__ = ["CellSpec", "ScalarValue", "SweepSpec"]
