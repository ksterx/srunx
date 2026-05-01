"""Tests for the shared ``python:`` prefix guard.

Pins the matching rules to runner's CLI-side ``_has_python_prefix``
(prefix match, leading-whitespace-tolerant, case-insensitive) and the
recursive traversal semantics (dict -> list -> str).
"""

from __future__ import annotations

import dataclasses

from srunx.runtime.security.python_args import (
    PythonPrefixViolation,
    find_python_prefix,
)


class TestFindPythonPrefix:
    def test_scalar_string_with_prefix_returns_violation(self) -> None:
        result = find_python_prefix("python: 1 + 1", source="args")
        assert isinstance(result, PythonPrefixViolation)
        assert result.source == "args"
        assert result.path == ""
        assert result.value == "python: 1 + 1"

    def test_scalar_string_without_prefix_returns_none(self) -> None:
        assert find_python_prefix("safe value", source="args") is None

    def test_leading_whitespace_tolerance(self) -> None:
        """Matches runner parser: leading whitespace does not bypass the guard."""
        assert find_python_prefix("  python: x", source="args") is not None
        assert find_python_prefix("\tpython: x", source="args") is not None

    def test_case_insensitive_prefix(self) -> None:
        for variant in ("Python: x", "PYTHON: x", "pYtHoN: x"):
            assert find_python_prefix(variant, source="args") is not None

    def test_substring_not_at_start_passes(self) -> None:
        # Prefix match, not substring: legitimate values containing
        # the token ``python:`` elsewhere must NOT be flagged.
        assert find_python_prefix("my_python_arg value", source="args") is None
        assert find_python_prefix("run with python: flag", source="args") is None

    def test_nested_dict_finds_violation(self) -> None:
        result = find_python_prefix(
            {"lr": "0.1", "cmd": "python: evil"},
            source="args",
        )
        assert result is not None
        assert result.path == "cmd"
        assert result.value == "python: evil"

    def test_list_element_finds_violation(self) -> None:
        # Bug-fix coverage: YAML list elements were previously not checked
        # by the Web path. The unified helper scans recursively.
        result = find_python_prefix(
            {"x": ["safe", "python: evil"]},
            source="sweep.matrix",
        )
        assert result is not None
        assert result.source == "sweep.matrix"
        assert result.path == "x[1]"
        assert result.value == "python: evil"

    def test_non_string_scalars_are_ignored(self) -> None:
        assert find_python_prefix(42, source="args") is None
        assert find_python_prefix(3.14, source="args") is None
        assert find_python_prefix(True, source="args") is None
        assert find_python_prefix(None, source="args") is None
        assert find_python_prefix({"a": 1, "b": 2.0, "c": False}, source="args") is None

    def test_deeply_nested_mapping_list_mapping(self) -> None:
        payload = {
            "matrix": {
                "group_a": [
                    {"safe": "ok", "inner": ["literal", "python: boom"]},
                ],
            },
        }
        result = find_python_prefix(payload, source="sweep")
        assert result is not None
        assert result.path == "matrix.group_a[0].inner[1]"

    def test_empty_mapping_and_list_return_none(self) -> None:
        assert find_python_prefix({}, source="args") is None
        assert find_python_prefix([], source="args") is None

    def test_violation_dataclass_is_frozen(self) -> None:
        violation = PythonPrefixViolation(source="args", path="x", value="python:")

        assert dataclasses.is_dataclass(violation)
        # frozen=True -> attribute assignment must fail
        try:
            violation.source = "other"  # type: ignore[misc, union-attr]
        except dataclasses.FrozenInstanceError:
            return
        raise AssertionError("PythonPrefixViolation must be frozen")
