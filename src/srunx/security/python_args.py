"""Reject ``python:`` prefix in user-supplied args / matrix values.

Runs on Web API submission (YAML + JSON) and MCP tool calls. The
``python:`` prefix in ``args`` is a server-side evaluation escape hatch
reserved for CLI-local use; exposing it over transport boundaries is a
security concern (remote code execution via workflow mutation). See
:func:`srunx.runner._has_python_prefix` for the CLI-side parser that
actually evaluates these values — the check here mirrors its matching
rules (prefix match, leading-whitespace-tolerant, case-insensitive).
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class PythonPrefixViolation:
    """Structured violation record. Caller converts to transport-specific error."""

    source: str
    """Logical origin of the payload (e.g. ``"args"``, ``"sweep.matrix"``)."""

    path: str
    """Dotted / indexed path to the offending value (e.g. ``"x[2]"``, ``"lr"``)."""

    value: str
    """The offending value, reproduced for error messaging."""


def _has_python_prefix(value: str) -> bool:
    """Match runner's parser: leading-whitespace-tolerant, case-insensitive prefix."""
    return value.lstrip().lower().startswith("python:")


def find_python_prefix(
    payload: Any,
    *,
    source: str,
    _path: str = "",
) -> PythonPrefixViolation | None:
    """Recursively scan a dict / list / scalar payload; return the first violation.

    Traverses nested dict -> list -> str. Non-string scalars
    (int / float / bool / None) are ignored. Returns ``None`` when no
    violation is found.
    """
    if isinstance(payload, str):
        if _has_python_prefix(payload):
            return PythonPrefixViolation(source=source, path=_path, value=payload)
        return None
    if isinstance(payload, Mapping):
        for key, val in payload.items():
            child_path = f"{_path}.{key}" if _path else str(key)
            violation = find_python_prefix(val, source=source, _path=child_path)
            if violation is not None:
                return violation
        return None
    if isinstance(payload, list):
        for i, element in enumerate(payload):
            child_path = f"{_path}[{i}]"
            violation = find_python_prefix(element, source=source, _path=child_path)
            if violation is not None:
                return violation
        return None
    return None
