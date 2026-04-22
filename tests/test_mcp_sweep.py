"""MCP integration tests for the sweep wiring.

Covers:
- ``run_workflow(yaml_path, args={...})`` merges args_override.
- ``run_workflow(yaml_path, sweep={...})`` routes to the orchestrator.
- ``python:`` rejection in both args and sweep.matrix.
- Existing kwarg-less calls stay compatible.
"""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import yaml  # type: ignore

from srunx.mcp.server import run_workflow


@pytest.fixture
def isolated_db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[Path]:
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path))
    from srunx.db.connection import init_db

    db_path = init_db(delete_legacy=False)
    yield db_path


def _write_workflow(tmp_path: Path) -> Path:
    data = {
        "name": "mcp_wf",
        "args": {"lr": 0.01, "seed": 1},
        "jobs": [
            {
                "name": "train",
                "command": ["echo", "train"],
                "environment": {"conda": "env"},
            }
        ],
    }
    path = tmp_path / "wf.yaml"
    path.write_text(yaml.dump(data))
    return path


class _FakeSweepRun:
    id = 77
    status = "completed"
    cell_count = 3
    cells_completed = 3
    cells_failed = 0
    cells_cancelled = 0


class TestMCPArgsOverride:
    def test_args_override_reaches_runner(
        self, tmp_path: Path, isolated_db: Path
    ) -> None:
        yaml_path = _write_workflow(tmp_path)
        with patch("srunx.runner.WorkflowRunner.from_yaml") as from_yaml:
            mock_runner = MagicMock()
            mock_runner.run.return_value = {}
            mock_runner.workflow.name = "mcp_wf"
            from_yaml.return_value = mock_runner

            result = run_workflow(str(yaml_path), args={"lr": 0.5})

        assert result["success"] is True
        call = from_yaml.call_args
        assert call.kwargs["args_override"] == {"lr": 0.5}

    def test_submission_context_none_reaches_runner(
        self, tmp_path: Path, isolated_db: Path
    ) -> None:
        """MCP non-sweep path must forward ``submission_context=None``.

        Phase 1 MCP runs always pin the canonical render context to
        ``None`` (local SLURM, no mount translation). The kwarg is passed
        explicitly so tests can pin the contract between MCP and the
        canonical render entry.
        """
        yaml_path = _write_workflow(tmp_path)
        with patch("srunx.runner.WorkflowRunner.from_yaml") as from_yaml:
            mock_runner = MagicMock()
            mock_runner.run.return_value = {}
            mock_runner.workflow.name = "mcp_wf"
            from_yaml.return_value = mock_runner

            result = run_workflow(str(yaml_path))

        assert result["success"] is True
        call = from_yaml.call_args
        assert "submission_context" in call.kwargs
        assert call.kwargs["submission_context"] is None

    def test_python_prefix_rejected_in_args(
        self, tmp_path: Path, isolated_db: Path
    ) -> None:
        yaml_path = _write_workflow(tmp_path)
        result = run_workflow(str(yaml_path), args={"cmd": "python: os.system('x')"})
        assert result["success"] is False
        assert "python:" in result["error"]


class TestMCPSweep:
    def test_sweep_dispatch_returns_sweep_run_id(
        self, tmp_path: Path, isolated_db: Path
    ) -> None:
        yaml_path = _write_workflow(tmp_path)

        with patch("srunx.sweep.orchestrator.SweepOrchestrator") as orch_cls:
            mock_orch = MagicMock()
            mock_orch.run.return_value = _FakeSweepRun()
            orch_cls.return_value = mock_orch

            result = run_workflow(
                str(yaml_path),
                sweep={
                    "matrix": {"lr": [0.1, 0.01, 0.001]},
                    "max_parallel": 4,
                },
            )

        assert result["success"] is True, result
        assert result["sweep_run_id"] == 77
        assert result["cell_count"] == 3
        kwargs = orch_cls.call_args.kwargs
        assert kwargs["submission_source"] == "mcp"
        # Phase 1: MCP sweep cells always run with no mount translation.
        assert "submission_context" in kwargs
        assert kwargs["submission_context"] is None

    def test_python_prefix_rejected_in_sweep_matrix(
        self, tmp_path: Path, isolated_db: Path
    ) -> None:
        yaml_path = _write_workflow(tmp_path)
        result = run_workflow(
            str(yaml_path),
            sweep={
                "matrix": {"cmd": ["python: os.system('x')"]},
                "max_parallel": 2,
            },
        )
        assert result["success"] is False
        assert "python:" in result["error"]


class TestMCPBackwardCompat:
    def test_positional_only_call_still_works(
        self, tmp_path: Path, isolated_db: Path
    ) -> None:
        yaml_path = _write_workflow(tmp_path)
        with patch("srunx.runner.WorkflowRunner.from_yaml") as from_yaml:
            mock_runner = MagicMock()
            mock_runner.run.return_value = {}
            mock_runner.workflow.name = "mcp_wf"
            from_yaml.return_value = mock_runner

            result = run_workflow(str(yaml_path))
        assert result["success"] is True
