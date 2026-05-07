"""CLI integration tests for `srunx flow` sweep wiring.

Exercises the new ``--arg`` / ``--sweep`` / ``--fail-fast`` / ``--max-parallel``
flags through :mod:`srunx.cli.workflow`. SweepOrchestrator is patched so
no real SLURM traffic occurs — tests verify that the CLI parses flags,
merges YAML + CLI sweeps, and dispatches correctly to the orchestrator
vs. the non-sweep path.
"""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
import yaml  # type: ignore
from typer.testing import CliRunner

from srunx.cli.workflow.orchestrator import app as workflow_app


@pytest.fixture
def isolated_db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[Path]:
    """Point the srunx state DB at a per-test tmp dir."""
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path))
    from srunx.observability.storage.connection import init_db

    db_path = init_db(delete_legacy=False)
    yield db_path


def _write_workflow(
    tmp_path: Path,
    *,
    with_sweep: bool = False,
) -> Path:
    data: dict[str, Any] = {
        "name": "sweep_cli_test",
        "args": {"lr": 0.01, "seed": 1},
        "jobs": [
            {
                "name": "train",
                "command": ["echo", "train"],
                "environment": {"conda": "env"},
            }
        ],
    }
    if with_sweep:
        data["sweep"] = {
            "matrix": {"lr": [0.1, 0.01]},
            "fail_fast": False,
            "max_parallel": 2,
        }
    path = tmp_path / "wf.yaml"
    path.write_text(yaml.dump(data))
    return path


class _FakeSweepRun:
    id = 42
    status = "completed"
    cell_count = 3
    cells_completed = 3
    cells_failed = 0
    cells_cancelled = 0


class TestNonSweepArgOverride:
    """``--arg KEY=VALUE`` alone must skip the sweep path entirely."""

    def test_single_arg_override_runs_non_sweep(
        self, tmp_path: Path, isolated_db: Path
    ) -> None:
        yaml_path = _write_workflow(tmp_path)

        runner = CliRunner()
        with (
            patch("srunx.cli.workflow.orchestrator.WorkflowRunner") as runner_cls,
            patch("srunx.cli.workflow.sweep.SweepOrchestrator") as orch_cls,
        ):
            mock_runner = MagicMock()
            runner_cls.from_yaml.return_value = mock_runner
            mock_runner.run.return_value = {}

            result = runner.invoke(
                workflow_app,
                ["--arg", "lr=0.05", str(yaml_path)],
            )

        assert result.exit_code == 0, result.stdout
        # args_override passed through; sweep orchestrator untouched.
        call = runner_cls.from_yaml.call_args
        assert call.kwargs["args_override"] == {"lr": "0.05"}
        orch_cls.assert_not_called()


class TestSweepFlag:
    def test_sweep_flag_invokes_orchestrator(
        self, tmp_path: Path, isolated_db: Path
    ) -> None:
        yaml_path = _write_workflow(tmp_path)

        runner = CliRunner()
        with (
            patch("srunx.cli.workflow.sweep.SweepOrchestrator") as orch_cls,
            patch("srunx.cli.workflow.orchestrator.WorkflowRunner") as runner_cls,
        ):
            mock_orch = MagicMock()
            mock_orch.run.return_value = _FakeSweepRun()
            orch_cls.return_value = mock_orch

            result = runner.invoke(
                workflow_app,
                [
                    "--sweep",
                    "lr=0.001,0.01,0.1",
                    "--max-parallel",
                    "2",
                    str(yaml_path),
                ],
            )

        assert result.exit_code == 0, result.stdout
        orch_cls.assert_called_once()
        sweep_spec = orch_cls.call_args.kwargs["sweep_spec"]
        assert sweep_spec.matrix == {"lr": ["0.001", "0.01", "0.1"]}
        assert sweep_spec.max_parallel == 2
        mock_orch.run.assert_called_once()
        # Non-sweep runner should NOT be executed for the sweep path.
        runner_cls.from_yaml.assert_not_called()

    def test_arg_and_sweep_collision_exits_1(
        self, tmp_path: Path, isolated_db: Path
    ) -> None:
        yaml_path = _write_workflow(tmp_path)

        runner = CliRunner()
        result = runner.invoke(
            workflow_app,
            [
                "--arg",
                "lr=0.01",
                "--sweep",
                "lr=0.001,0.01",
                "--max-parallel",
                "2",
                str(yaml_path),
            ],
        )
        assert result.exit_code == 1
        output = result.stdout + (result.stderr or "")
        assert "lr" in output
        assert "both --arg and --sweep" in output or "--arg" in output

    def test_empty_string_values_preserved(
        self, tmp_path: Path, isolated_db: Path
    ) -> None:
        """``--sweep lr=a,,b`` must keep the empty element."""
        yaml_path = _write_workflow(tmp_path)

        runner = CliRunner()
        with patch("srunx.cli.workflow.sweep.SweepOrchestrator") as orch_cls:
            mock_orch = MagicMock()
            mock_orch.run.return_value = _FakeSweepRun()
            orch_cls.return_value = mock_orch

            result = runner.invoke(
                workflow_app,
                [
                    "--sweep",
                    "lr=a,,b",
                    "--max-parallel",
                    "2",
                    str(yaml_path),
                ],
            )

        assert result.exit_code == 0, result.stdout
        sweep_spec = orch_cls.call_args.kwargs["sweep_spec"]
        assert sweep_spec.matrix == {"lr": ["a", "", "b"]}

    def test_yaml_sweep_merged_with_cli_sweep(
        self, tmp_path: Path, isolated_db: Path
    ) -> None:
        """YAML declares ``lr`` axis; CLI adds a ``seed`` axis."""
        yaml_path = _write_workflow(tmp_path, with_sweep=True)

        runner = CliRunner()
        with patch("srunx.cli.workflow.sweep.SweepOrchestrator") as orch_cls:
            mock_orch = MagicMock()
            mock_orch.run.return_value = _FakeSweepRun()
            orch_cls.return_value = mock_orch

            result = runner.invoke(
                workflow_app,
                [
                    "--sweep",
                    "seed=1,2",
                    str(yaml_path),
                ],
            )

        assert result.exit_code == 0, result.stdout
        sweep_spec = orch_cls.call_args.kwargs["sweep_spec"]
        # YAML axis + CLI axis should both appear.
        assert set(sweep_spec.matrix.keys()) == {"lr", "seed"}
        assert sweep_spec.matrix["seed"] == ["1", "2"]
        # max_parallel falls back to YAML value (2).
        assert sweep_spec.max_parallel == 2

    def test_endpoint_skipped_for_sweep_mode_callbacks(
        self, tmp_path: Path, isolated_db: Path
    ) -> None:
        """Regression for I2: ``--endpoint`` must NOT attach
        NotificationWatchCallback in sweep mode.

        Sweep-level notifications flow through the sweep_run watch +
        subscription created by the orchestrator itself. Adding a
        per-job ``NotificationWatchCallback`` on top would spam one
        Slack delivery per submitted cell job, defeating the sweep's
        aggregated-notification contract.
        """
        yaml_path = _write_workflow(tmp_path)

        runner = CliRunner()
        with (
            patch("srunx.cli.workflow.sweep.SweepOrchestrator") as orch_cls,
            patch(
                "srunx.cli.workflow.notifications.NotificationWatchCallback"
            ) as cb_cls,
        ):
            mock_orch = MagicMock()
            mock_orch.run.return_value = _FakeSweepRun()
            orch_cls.return_value = mock_orch

            result = runner.invoke(
                workflow_app,
                [
                    "--sweep",
                    "lr=0.1,0.01",
                    "--max-parallel",
                    "2",
                    "--endpoint",
                    "myslack",
                    str(yaml_path),
                ],
            )

        assert result.exit_code == 0, result.stdout
        # NotificationWatchCallback must not be constructed for the
        # sweep path — even though --endpoint was supplied.
        cb_cls.assert_not_called()
        # And the callbacks list handed to the orchestrator must not
        # contain a NotificationWatchCallback instance.
        callbacks = orch_cls.call_args.kwargs.get("callbacks") or []
        from srunx.callbacks import NotificationWatchCallback

        assert not any(isinstance(c, NotificationWatchCallback) for c in callbacks)

    def test_dry_run_with_sweep_does_not_execute(
        self, tmp_path: Path, isolated_db: Path
    ) -> None:
        yaml_path = _write_workflow(tmp_path)

        runner = CliRunner()
        with patch("srunx.cli.workflow.sweep.SweepOrchestrator") as orch_cls:
            result = runner.invoke(
                workflow_app,
                [
                    "--sweep",
                    "lr=0.1,0.01,0.001",
                    "--max-parallel",
                    "2",
                    "--dry-run",
                    str(yaml_path),
                ],
            )

        assert result.exit_code == 0, result.stdout
        assert "Sweep dry run" in result.stdout
        assert "Cell count: 3" in result.stdout
        orch_cls.assert_not_called()


def _write_cell_validation_workflow(tmp_path: Path) -> Path:
    """Workflow whose per-cell Jinja render depends on ``stage_pick``.

    ``stage1`` exports ``model_a`` / ``model_b``; ``stage2`` references
    ``deps.stage1[stage_pick]`` so the cell's ``stage_pick`` value
    determines whether the render succeeds under ``StrictUndefined``.
    """
    data: dict[str, Any] = {
        "name": "cell_validate",
        "args": {"stage_pick": "model_a"},
        "jobs": [
            {
                "name": "stage1",
                "command": ["echo", "stage1"],
                "environment": {"conda": "env"},
                "exports": {
                    "model_a": "/path/a",
                    "model_b": "/path/b",
                },
            },
            {
                "name": "stage2",
                "command": [
                    "echo",
                    "{{ stage_pick }}",
                    "{{ deps.stage1[stage_pick] }}",
                ],
                "environment": {"conda": "env"},
                "depends_on": ["stage1"],
            },
        ],
    }
    path = tmp_path / "wf.yaml"
    path.write_text(yaml.dump(data))
    return path


class TestSweepValidateAllCells:
    """Regression tests for I2: ``--validate`` must check every matrix cell."""

    def test_validate_detects_error_in_non_zero_cell(
        self, tmp_path: Path, isolated_db: Path
    ) -> None:
        """Cells 0-1 render fine; cell 2 references a missing export key."""
        yaml_path = _write_cell_validation_workflow(tmp_path)

        runner = CliRunner()
        result = runner.invoke(
            workflow_app,
            [
                "--sweep",
                "stage_pick=model_a,model_b,bogus",
                "--max-parallel",
                "2",
                "--validate",
                str(yaml_path),
            ],
        )

        assert result.exit_code == 1, result.stdout
        output = result.stdout + (result.stderr or "")
        assert "validation error" in output.lower()
        assert "bogus" in output

    def test_validate_passes_for_fully_valid_sweep(
        self, tmp_path: Path, isolated_db: Path
    ) -> None:
        """Every cell renders → exit 0."""
        yaml_path = _write_cell_validation_workflow(tmp_path)

        runner = CliRunner()
        result = runner.invoke(
            workflow_app,
            [
                "--sweep",
                "stage_pick=model_a,model_b",
                "--max-parallel",
                "2",
                "--validate",
                str(yaml_path),
            ],
        )

        assert result.exit_code == 0, result.stdout
        assert "Workflow validation successful" in (
            result.stdout + (result.stderr or "")
        )

    def test_validate_reports_failing_cell_args(
        self, tmp_path: Path, isolated_db: Path
    ) -> None:
        """The error message must name the cell index + its effective args."""
        yaml_path = _write_cell_validation_workflow(tmp_path)

        runner = CliRunner()
        result = runner.invoke(
            workflow_app,
            [
                "--sweep",
                "stage_pick=model_a,bogus",
                "--max-parallel",
                "2",
                "--validate",
                str(yaml_path),
            ],
        )

        assert result.exit_code == 1, result.stdout
        output = result.stdout + (result.stderr or "")
        assert "cell 1" in output
        assert "stage_pick" in output
        assert "bogus" in output
