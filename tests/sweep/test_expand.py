"""Unit tests for ``srunx.sweep.expand``."""

from __future__ import annotations

import pytest

from srunx.exceptions import WorkflowValidationError
from srunx.sweep import SweepSpec
from srunx.sweep.expand import (
    expand_matrix,
    merge_sweep_specs,
    parse_arg_flags,
    parse_sweep_flags,
)


class TestExpandMatrix:
    def test_cross_product_two_axes(self) -> None:
        cells = expand_matrix(
            {"lr": [0.001, 0.01], "seed": [1, 2]},
            {"dataset": "cifar10"},
        )
        assert cells == [
            {"lr": 0.001, "seed": 1, "dataset": "cifar10"},
            {"lr": 0.001, "seed": 2, "dataset": "cifar10"},
            {"lr": 0.01, "seed": 1, "dataset": "cifar10"},
            {"lr": 0.01, "seed": 2, "dataset": "cifar10"},
        ]

    def test_single_value_axis_yields_single_cell(self) -> None:
        cells = expand_matrix({"lr": [0.01]}, {"dataset": "cifar10"})
        assert cells == [{"lr": 0.01, "dataset": "cifar10"}]

    def test_matrix_value_overrides_base_arg(self) -> None:
        cells = expand_matrix({"lr": [0.5]}, {"lr": 0.001, "dataset": "x"})
        assert cells == [{"lr": 0.5, "dataset": "x"}]

    def test_axis_insertion_order_preserved(self) -> None:
        cells = expand_matrix(
            {"b": [1, 2], "a": ["x"]},
            {},
        )
        # Keys in each cell follow matrix insertion order then base_args order.
        assert list(cells[0].keys()) == ["b", "a"]

    def test_empty_matrix_rejected(self) -> None:
        with pytest.raises(WorkflowValidationError, match="at least one axis"):
            expand_matrix({}, {})

    def test_empty_axis_rejected(self) -> None:
        with pytest.raises(WorkflowValidationError, match="at least one value"):
            expand_matrix({"lr": []}, {})

    def test_non_list_axis_rejected(self) -> None:
        with pytest.raises(WorkflowValidationError, match="must be a list"):
            expand_matrix({"lr": "0.01"}, {})  # type: ignore[dict-item]

    def test_non_scalar_value_rejected_dict(self) -> None:
        with pytest.raises(WorkflowValidationError, match="non-scalar"):
            expand_matrix({"lr": [{"nested": 1}]}, {})

    def test_non_scalar_value_rejected_list(self) -> None:
        with pytest.raises(WorkflowValidationError, match="non-scalar"):
            expand_matrix({"lr": [[1, 2]]}, {})

    def test_non_scalar_value_rejected_none(self) -> None:
        with pytest.raises(WorkflowValidationError, match="non-scalar"):
            expand_matrix({"lr": [None]}, {})

    def test_bool_is_allowed_scalar(self) -> None:
        cells = expand_matrix({"flag": [True, False]}, {})
        assert cells == [{"flag": True}, {"flag": False}]

    def test_axis_named_deps_rejected(self) -> None:
        with pytest.raises(WorkflowValidationError, match="reserved"):
            expand_matrix({"deps": [1]}, {})

    def test_cell_count_above_limit_rejected(self) -> None:
        # 10 * 10 * 11 = 1100 > 1000
        matrix = {
            "a": list(range(10)),
            "b": list(range(10)),
            "c": list(range(11)),
        }
        with pytest.raises(WorkflowValidationError, match="exceeds limit"):
            expand_matrix(matrix, {})

    def test_cell_count_at_limit_accepted(self) -> None:
        matrix = {"a": list(range(10)), "b": list(range(10)), "c": list(range(10))}
        cells = expand_matrix(matrix, {})
        assert len(cells) == 1000


class TestMergeSweepSpecs:
    def test_no_yaml_no_cli_returns_none(self) -> None:
        result = merge_sweep_specs(None, {}, {}, None, None)
        assert result is None

    def test_yaml_only(self) -> None:
        yaml_spec = SweepSpec(matrix={"lr": [1, 2]}, fail_fast=False, max_parallel=2)
        merged = merge_sweep_specs(yaml_spec, {}, {}, None, None)
        assert merged is not None
        assert merged.matrix == {"lr": [1, 2]}
        assert merged.max_parallel == 2
        assert merged.fail_fast is False

    def test_cli_only_requires_max_parallel(self) -> None:
        with pytest.raises(WorkflowValidationError, match="max_parallel"):
            merge_sweep_specs(None, {"lr": ["1", "2"]}, {}, None, None)

    def test_cli_only_with_max_parallel(self) -> None:
        merged = merge_sweep_specs(None, {"lr": ["1", "2"]}, {}, None, 4)
        assert merged is not None
        assert merged.matrix == {"lr": ["1", "2"]}
        assert merged.max_parallel == 4
        assert merged.fail_fast is False

    def test_cli_axis_replaces_yaml_axis(self) -> None:
        yaml_spec = SweepSpec(
            matrix={"lr": [0.001, 0.01], "seed": [1]},
            fail_fast=False,
            max_parallel=2,
        )
        merged = merge_sweep_specs(
            yaml_spec,
            {"lr": ["0.5", "1.0"]},
            {},
            None,
            None,
        )
        assert merged is not None
        assert merged.matrix == {"lr": ["0.5", "1.0"], "seed": [1]}

    def test_cli_adds_new_axis(self) -> None:
        yaml_spec = SweepSpec(matrix={"lr": [0.01]}, fail_fast=False, max_parallel=2)
        merged = merge_sweep_specs(
            yaml_spec,
            {"seed": ["1", "2"]},
            {},
            None,
            None,
        )
        assert merged is not None
        assert merged.matrix == {"lr": [0.01], "seed": ["1", "2"]}

    def test_arg_sweep_collision_rejected(self) -> None:
        with pytest.raises(WorkflowValidationError, match="cannot be specified"):
            merge_sweep_specs(
                None,
                {"lr": ["1", "2"]},
                {"lr": "0.5"},
                None,
                4,
            )

    def test_arg_collides_with_yaml_matrix_axis_rejected(self) -> None:
        """Regression for I4: ``--arg`` must not silently override a YAML matrix axis.

        Without this check the YAML matrix values would silently win at
        expand-time, masking the user's intent.
        """
        yaml_spec = SweepSpec(
            matrix={"lr": [0.001, 0.01]},
            fail_fast=False,
            max_parallel=2,
        )
        with pytest.raises(
            WorkflowValidationError, match="both in sweep.matrix and --arg"
        ):
            merge_sweep_specs(
                yaml_spec,
                {},  # no CLI --sweep override
                {"lr": "0.5"},  # CLI --arg lr=0.5 collides with YAML matrix lr
                None,
                None,
            )

    def test_cli_fail_fast_overrides_yaml(self) -> None:
        yaml_spec = SweepSpec(matrix={"lr": [1]}, fail_fast=False, max_parallel=2)
        merged = merge_sweep_specs(yaml_spec, {}, {}, True, None)
        assert merged is not None
        assert merged.fail_fast is True

    def test_cli_max_parallel_overrides_yaml(self) -> None:
        yaml_spec = SweepSpec(matrix={"lr": [1]}, fail_fast=False, max_parallel=2)
        merged = merge_sweep_specs(yaml_spec, {}, {}, None, 8)
        assert merged is not None
        assert merged.max_parallel == 8

    def test_final_max_parallel_zero_rejected(self) -> None:
        with pytest.raises(WorkflowValidationError, match="max_parallel"):
            merge_sweep_specs(None, {"lr": ["1"]}, {}, None, 0)


class TestParseArgFlags:
    def test_basic(self) -> None:
        assert parse_arg_flags(["lr=0.01", "seed=42"]) == {"lr": "0.01", "seed": "42"}

    def test_last_wins(self) -> None:
        assert parse_arg_flags(["lr=0.01", "lr=0.5"]) == {"lr": "0.5"}

    def test_multiple_equals_split_at_first(self) -> None:
        assert parse_arg_flags(["cmd=echo=hi"]) == {"cmd": "echo=hi"}

    def test_empty_value_allowed(self) -> None:
        assert parse_arg_flags(["flag="]) == {"flag": ""}

    def test_missing_equals_rejected(self) -> None:
        with pytest.raises(WorkflowValidationError, match="KEY=VALUE"):
            parse_arg_flags(["noequals"])

    def test_empty_key_rejected(self) -> None:
        with pytest.raises(WorkflowValidationError, match="empty key"):
            parse_arg_flags(["=value"])


class TestParseSweepFlags:
    def test_basic(self) -> None:
        assert parse_sweep_flags(["lr=0.001,0.01,0.1"]) == {
            "lr": ["0.001", "0.01", "0.1"]
        }

    def test_multiple_axes(self) -> None:
        assert parse_sweep_flags(["lr=1,2", "seed=3,4"]) == {
            "lr": ["1", "2"],
            "seed": ["3", "4"],
        }

    def test_empty_element_preserved(self) -> None:
        assert parse_sweep_flags(["x=a,,b"]) == {"x": ["a", "", "b"]}

    def test_single_value_axis(self) -> None:
        assert parse_sweep_flags(["lr=0.01"]) == {"lr": ["0.01"]}

    def test_missing_equals_rejected(self) -> None:
        with pytest.raises(WorkflowValidationError, match="KEY=v1,v2"):
            parse_sweep_flags(["noequals"])

    def test_empty_key_rejected(self) -> None:
        with pytest.raises(WorkflowValidationError, match="empty key"):
            parse_sweep_flags(["=a,b"])

    def test_value_with_equals_kept(self) -> None:
        # axis split at first '=', subsequent '=' stay in values
        assert parse_sweep_flags(["cmd=a=1,b=2"]) == {"cmd": ["a=1", "b=2"]}
