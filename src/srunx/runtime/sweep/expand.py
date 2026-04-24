"""Pure functions for matrix expansion, sweep-spec merging, and CLI flag parsing.

All validation routes through ``WorkflowValidationError`` so CLI, Web API, and
MCP paths surface a consistent error category.
"""

from __future__ import annotations

import itertools
from typing import Any

from srunx.common.exceptions import WorkflowValidationError
from srunx.runtime.sweep import ScalarValue, SweepSpec

_MAX_CELL_COUNT = 1000
_SCALAR_TYPES: tuple[type, ...] = (str, int, float, bool)
_RESERVED_AXIS_NAMES: frozenset[str] = frozenset({"deps"})


def expand_matrix(
    matrix: dict[str, list[Any]],
    base_args: dict[str, Any],
) -> list[dict[str, Any]]:
    """Cross-product of ``matrix`` axes merged into ``base_args``.

    Axis iteration order follows insertion order of ``matrix``. Matrix values
    override any identically-keyed entry in ``base_args`` (matrix wins at the
    args level; the ``deps.<parent>.<key>`` channel remains a separate space).

    Raises:
        WorkflowValidationError: empty matrix (R2.10), empty axis list (R2.4),
            non-scalar axis value (R2.5), axis named ``deps`` (R2.3), or
            cell_count > 1000 (R2.8).
    """
    if not matrix:
        raise WorkflowValidationError("matrix must declare at least one axis")

    for axis, values in matrix.items():
        if axis in _RESERVED_AXIS_NAMES:
            raise WorkflowValidationError(
                f"matrix axis name {axis!r} is reserved (collides with Jinja2 context)"
            )
        if not isinstance(values, list):
            raise WorkflowValidationError(
                f"matrix axis {axis!r} must be a list (got {type(values).__name__})"
            )
        if len(values) == 0:
            raise WorkflowValidationError(
                f"matrix axis {axis!r} must contain at least one value"
            )
        for value in values:
            # bool is a subclass of int so isinstance works; we explicitly allow it.
            if not isinstance(value, _SCALAR_TYPES):
                raise WorkflowValidationError(
                    f"matrix axis {axis!r} contains non-scalar value "
                    f"{value!r} (only str/int/float/bool allowed)"
                )

    axis_names = list(matrix.keys())
    cell_count = 1
    for axis in axis_names:
        cell_count *= len(matrix[axis])
    if cell_count > _MAX_CELL_COUNT:
        raise WorkflowValidationError(
            f"matrix cell_count={cell_count} exceeds limit {_MAX_CELL_COUNT}"
        )

    cells: list[dict[str, Any]] = []
    for combo in itertools.product(*(matrix[axis] for axis in axis_names)):
        effective = dict(base_args)
        for axis, value in zip(axis_names, combo, strict=True):
            effective[axis] = value
        cells.append(effective)
    return cells


def merge_sweep_specs(
    yaml_sweep: SweepSpec | None,
    cli_sweep_axes: dict[str, list[ScalarValue]],
    cli_arg_overrides: dict[str, str],
    cli_fail_fast: bool | None,
    cli_max_parallel: int | None,
) -> SweepSpec | None:
    """Merge YAML ``sweep:`` block with CLI flags at axis granularity.

    - If neither YAML nor CLI provides any matrix axis, returns ``None``
      (caller runs the non-sweep path).
    - CLI axes replace same-named YAML axes; CLI-only axes are added.
    - ``--arg KEY`` colliding with ``--sweep KEY`` is rejected (R3.6).
    - The final ``max_parallel`` must be set and >= 1 (R2.6).

    Raises:
        WorkflowValidationError: on ``--arg``/``--sweep`` key collision or
            missing/invalid final ``max_parallel``.
    """
    collisions = set(cli_arg_overrides.keys()) & set(cli_sweep_axes.keys())
    if collisions:
        names = ", ".join(sorted(collisions))
        raise WorkflowValidationError(
            f"keys cannot be specified as both --arg and --sweep: {names}"
        )

    has_yaml_matrix = yaml_sweep is not None and bool(yaml_sweep.matrix)
    has_cli_matrix = bool(cli_sweep_axes)
    if not has_yaml_matrix and not has_cli_matrix:
        return None

    merged_matrix: dict[str, list[ScalarValue]] = {}
    if yaml_sweep is not None:
        merged_matrix.update(yaml_sweep.matrix)
    for axis, values in cli_sweep_axes.items():
        merged_matrix[axis] = list(values)

    # Also reject ``--arg KEY=...`` that collides with a merged sweep
    # matrix axis. Without this check the matrix value silently wins at
    # expand time, masking the user's intent.
    arg_matrix_collisions = set(cli_arg_overrides.keys()) & set(merged_matrix.keys())
    if arg_matrix_collisions:
        names = ", ".join(sorted(arg_matrix_collisions))
        raise WorkflowValidationError(
            f"key(s) {names!r} cannot be both in sweep.matrix and --arg "
            "(use --sweep for matrix axes, or remove from sweep.matrix for "
            "a single override)"
        )

    if cli_fail_fast is not None:
        fail_fast = cli_fail_fast
    elif yaml_sweep is not None:
        fail_fast = yaml_sweep.fail_fast
    else:
        fail_fast = False

    if cli_max_parallel is not None:
        max_parallel: int | None = cli_max_parallel
    elif yaml_sweep is not None:
        max_parallel = yaml_sweep.max_parallel
    else:
        max_parallel = None

    if max_parallel is None or max_parallel < 1:
        raise WorkflowValidationError(
            "sweep.max_parallel must be specified and >= 1 "
            "(set via YAML sweep.max_parallel or CLI --max-parallel)"
        )

    return SweepSpec(
        matrix=merged_matrix,
        fail_fast=fail_fast,
        max_parallel=max_parallel,
    )


def parse_arg_flags(raw: list[str]) -> dict[str, str]:
    """Tokenize ``--arg KEY=VALUE`` occurrences.

    Rules:
    - Split on the FIRST ``=`` (later ``=`` characters stay in the value).
    - Duplicate keys: last occurrence wins (R1.2).
    - Missing ``=`` raises ``WorkflowValidationError`` (R3.8).
    - Values are always strings; no int/float/bool auto-cast (R3.10).
    """
    result: dict[str, str] = {}
    for entry in raw:
        if "=" not in entry:
            raise WorkflowValidationError(
                f"--arg value {entry!r} is not in KEY=VALUE format"
            )
        key, _, value = entry.partition("=")
        if not key:
            raise WorkflowValidationError(f"--arg value {entry!r} has empty key")
        result[key] = value
    return result


def parse_sweep_flags(raw: list[str]) -> dict[str, list[str]]:
    """Tokenize ``--sweep KEY=v1,v2,v3`` occurrences.

    - Split axis at the first ``=`` (axis names cannot contain ``=``).
    - Values are split on ``,`` with no escape handling (Phase 1, R3.5).
    - Empty elements (``a,,b``) are preserved as empty strings (R3.9).
    - Missing ``=`` raises ``WorkflowValidationError`` (R3.8).
    - Duplicate axis: last occurrence wins (consistent with ``parse_arg_flags``).
    """
    result: dict[str, list[str]] = {}
    for entry in raw:
        if "=" not in entry:
            raise WorkflowValidationError(
                f"--sweep value {entry!r} is not in KEY=v1,v2,... format"
            )
        key, _, value_part = entry.partition("=")
        if not key:
            raise WorkflowValidationError(f"--sweep value {entry!r} has empty key")
        result[key] = value_part.split(",")
    return result


__all__ = [
    "expand_matrix",
    "merge_sweep_specs",
    "parse_arg_flags",
    "parse_sweep_flags",
]
