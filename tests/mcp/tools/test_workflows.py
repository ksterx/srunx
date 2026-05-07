"""Tests for srunx.mcp.tools.workflows.

Covers all five workflow tools (``create_workflow`` / ``validate_workflow``
/ ``run_workflow`` / ``list_workflows`` / ``get_workflow``) plus the
sweep + mount-routing integration paths through ``run_workflow``.
"""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import yaml

from srunx.mcp.tools.workflows import (
    create_workflow,
    list_workflows,
    run_workflow,
    validate_workflow,
)


@pytest.fixture
def isolated_db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[Path]:
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path))
    from srunx.observability.storage.connection import init_db

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


class TestValidateWorkflow:
    """Test validate_workflow tool."""

    def test_valid_workflow(self, tmp_path):
        workflow = {
            "name": "test_pipeline",
            "jobs": [
                {"name": "preprocess", "command": ["python", "preprocess.py"]},
                {
                    "name": "train",
                    "command": ["python", "train.py"],
                    "depends_on": ["preprocess"],
                },
            ],
        }
        yaml_file = tmp_path / "workflow.yaml"
        with open(yaml_file, "w") as f:
            yaml.dump(workflow, f)

        result = validate_workflow(str(yaml_file))
        assert result["success"] is True
        assert result["valid"] is True
        assert result["name"] == "test_pipeline"
        assert result["job_count"] == 2

    def test_nonexistent_file(self):
        result = validate_workflow("/nonexistent/path/workflow.yaml")
        assert result["success"] is False

    def test_invalid_yaml_structure(self, tmp_path):
        yaml_file = tmp_path / "bad.yaml"
        yaml_file.write_text("not: a: valid: workflow\n")

        result = validate_workflow(str(yaml_file))
        assert result["success"] is False

    def test_circular_dependency(self, tmp_path):
        workflow = {
            "name": "circular",
            "jobs": [
                {
                    "name": "a",
                    "command": ["echo", "a"],
                    "depends_on": ["b"],
                },
                {
                    "name": "b",
                    "command": ["echo", "b"],
                    "depends_on": ["a"],
                },
            ],
        }
        yaml_file = tmp_path / "circular.yaml"
        with open(yaml_file, "w") as f:
            yaml.dump(workflow, f)

        result = validate_workflow(str(yaml_file))
        assert result["success"] is False


class TestCreateWorkflow:
    """Test create_workflow tool."""

    def test_create_basic_workflow(self, tmp_path):
        output = str(tmp_path / "out.yaml")
        result = create_workflow(
            name="my_flow",
            jobs=[
                {"name": "step1", "command": ["echo", "hello"]},
                {
                    "name": "step2",
                    "command": ["echo", "world"],
                    "depends_on": ["step1"],
                },
            ],
            output_path=output,
        )
        assert result["success"] is True
        assert result["name"] == "my_flow"
        assert result["job_count"] == 2

        with open(output) as f:
            data = yaml.safe_load(f)
        assert data["name"] == "my_flow"
        assert len(data["jobs"]) == 2

    def test_python_arg_rejected(self, tmp_path):
        output = str(tmp_path / "out.yaml")
        result = create_workflow(
            name="my_flow",
            jobs=[{"name": "step1", "command": ["echo", "hello"]}],
            output_path=output,
            args={"bad_var": "python: import os; os.system('rm -rf /')"},
        )
        assert result["success"] is False
        assert "python:" in result["error"].lower()

    def test_python_arg_rejected_case_insensitive(self, tmp_path):
        output = str(tmp_path / "out.yaml")
        result = create_workflow(
            name="my_flow",
            jobs=[{"name": "step1", "command": ["echo", "hello"]}],
            output_path=output,
            args={"bad_var": "Python: some expression"},
        )
        assert result["success"] is False
        assert "python:" in result["error"].lower()

    def test_missing_name_in_job(self, tmp_path):
        output = str(tmp_path / "out.yaml")
        result = create_workflow(
            name="my_flow",
            jobs=[{"command": ["echo", "hello"]}],
            output_path=output,
            args={"some_var": "value"},
        )
        assert result["success"] is False
        assert "name" in result["error"].lower()

    def test_missing_command_in_job(self, tmp_path):
        output = str(tmp_path / "out.yaml")
        result = create_workflow(
            name="my_flow",
            jobs=[{"name": "step1"}],
            output_path=output,
            args={"some_var": "value"},
        )
        assert result["success"] is False
        assert (
            "command" in result["error"].lower()
            or "script_path" in result["error"].lower()
        )

    def test_with_args_and_default_project(self, tmp_path):
        output = str(tmp_path / "out.yaml")
        result = create_workflow(
            name="templated_flow",
            jobs=[
                {"name": "step1", "command": ["echo", "{{ base_dir }}"]},
            ],
            output_path=output,
            args={"base_dir": "/data/exp"},
            default_project="myproject",
        )
        assert result["success"] is True

        with open(output) as f:
            data = yaml.safe_load(f)
        assert data["args"]["base_dir"] == "/data/exp"
        assert data["default_project"] == "myproject"


class TestListWorkflows:
    """Test list_workflows tool."""

    def test_finds_workflows(self, tmp_path):
        workflow = {"name": "my_flow", "jobs": [{"name": "step1", "command": ["echo"]}]}
        wf_file = tmp_path / "flow.yaml"
        with open(wf_file, "w") as f:
            yaml.dump(workflow, f)

        other_file = tmp_path / "config.yaml"
        with open(other_file, "w") as f:
            yaml.dump({"key": "value"}, f)

        result = list_workflows(directory=str(tmp_path))
        assert result["success"] is True
        assert result["count"] == 1
        assert result["workflows"][0]["name"] == "my_flow"

    def test_skips_hidden_dirs(self, tmp_path):
        hidden = tmp_path / ".hidden"
        hidden.mkdir()
        wf_file = hidden / "flow.yaml"
        with open(wf_file, "w") as f:
            yaml.dump(
                {"name": "hidden", "jobs": [{"name": "a", "command": ["echo"]}]}, f
            )

        result = list_workflows(directory=str(tmp_path))
        assert result["success"] is True
        assert result["count"] == 0

    def test_empty_directory(self, tmp_path):
        result = list_workflows(directory=str(tmp_path))
        assert result["success"] is True
        assert result["count"] == 0

    def test_nonexistent_directory(self):
        result = list_workflows(directory="/nonexistent/dir/abc123")
        assert result["success"] is True
        assert result["count"] == 0


class TestRunWorkflowArgsOverride:
    """Test run_workflow args=... merge behaviour."""

    def test_args_override_reaches_runner(
        self, tmp_path: Path, isolated_db: Path
    ) -> None:
        yaml_path = _write_workflow(tmp_path)
        with patch(
            "srunx.runtime.workflow.runner.WorkflowRunner.from_yaml"
        ) as from_yaml:
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
        with patch(
            "srunx.runtime.workflow.runner.WorkflowRunner.from_yaml"
        ) as from_yaml:
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


class TestRunWorkflowSweep:
    """Test run_workflow sweep dispatch."""

    def test_sweep_dispatch_returns_sweep_run_id(
        self, tmp_path: Path, isolated_db: Path
    ) -> None:
        yaml_path = _write_workflow(tmp_path)

        with patch("srunx.runtime.sweep.orchestrator.SweepOrchestrator") as orch_cls:
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


class TestRunWorkflowBackwardCompat:
    def test_positional_only_call_still_works(
        self, tmp_path: Path, isolated_db: Path
    ) -> None:
        yaml_path = _write_workflow(tmp_path)
        with patch(
            "srunx.runtime.workflow.runner.WorkflowRunner.from_yaml"
        ) as from_yaml:
            mock_runner = MagicMock()
            mock_runner.run.return_value = {}
            mock_runner.workflow.name = "mcp_wf"
            from_yaml.return_value = mock_runner

            result = run_workflow(str(yaml_path))
        assert result["success"] is True


class TestRunWorkflowMountRouting:
    """Phase 5a: ``mount=...`` threads MCP runs through the SSH adapter."""

    def test_mount_without_profile_returns_error(
        self, tmp_path: Path, isolated_db: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """No current SSH profile → readable error, no partial execution."""
        yaml_path = _write_workflow(tmp_path)
        monkeypatch.setattr(
            "srunx.ssh.core.config.ConfigManager.get_current_profile",
            lambda self: None,
        )
        monkeypatch.setattr(
            "srunx.ssh.core.config.ConfigManager.get_current_profile_name",
            lambda self: None,
        )
        result = run_workflow(str(yaml_path), mount="cookbook2")
        assert result["success"] is False
        assert "SSH profile" in result["error"]

    def test_mount_name_not_in_profile_returns_error(
        self, tmp_path: Path, isolated_db: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Unknown mount name → error, no adapter instantiation."""
        yaml_path = _write_workflow(tmp_path)
        fake_profile = MagicMock(
            mounts=[MagicMock(name="other", local="/l", remote="/r")]
        )
        fake_profile.mounts[0].name = "other"
        monkeypatch.setattr(
            "srunx.ssh.core.config.ConfigManager.get_current_profile",
            lambda self: fake_profile,
        )
        monkeypatch.setattr(
            "srunx.ssh.core.config.ConfigManager.get_current_profile_name",
            lambda self: "pyxis",
        )
        result = run_workflow(str(yaml_path), mount="cookbook2")
        assert result["success"] is False
        assert "cookbook2" in result["error"]

    def test_mount_routes_sweep_through_pool(
        self, tmp_path: Path, isolated_db: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """``mount=...`` + sweep constructs a pool and threads executor_factory."""
        yaml_path = _write_workflow(tmp_path)

        fake_mount = MagicMock()
        fake_mount.name = "cookbook2"
        fake_mount.local = str(tmp_path)
        fake_mount.remote = "/home/remote/cookbook2"
        fake_profile = MagicMock()
        fake_profile.mounts = [fake_mount]

        monkeypatch.setattr(
            "srunx.ssh.core.config.ConfigManager.get_current_profile",
            lambda self: fake_profile,
        )
        monkeypatch.setattr(
            "srunx.ssh.core.config.ConfigManager.get_current_profile_name",
            lambda self: "pyxis",
        )
        with (
            patch(
                "srunx.slurm.clients.ssh.SlurmSSHClient.__init__", return_value=None
            ) as adapter_init,
            patch(
                "srunx.slurm.clients.ssh.SlurmSSHClient.connection_spec",
                new=MagicMock(),
            ),
            patch("srunx.slurm.ssh_executor.SlurmSSHExecutorPool") as pool_cls,
            patch("srunx.runtime.sweep.orchestrator.SweepOrchestrator") as orch_cls,
        ):
            mock_pool = MagicMock()
            pool_cls.return_value = mock_pool
            mock_orch = MagicMock()
            mock_orch.run.return_value = _FakeSweepRun()
            orch_cls.return_value = mock_orch

            result = run_workflow(
                str(yaml_path),
                sweep={"matrix": {"lr": [0.1, 0.01]}, "max_parallel": 2},
                mount="cookbook2",
            )

        assert result["success"] is True, result
        assert result["sweep_run_id"] == 77
        adapter_init.assert_called_once()
        pool_cls.assert_called_once()
        orch_kwargs = orch_cls.call_args.kwargs
        assert orch_kwargs["executor_factory"] is mock_pool.lease
        ctx = orch_kwargs["submission_context"]
        assert ctx is not None
        assert ctx.mount_name == "cookbook2"
        assert ctx.default_work_dir == "/home/remote/cookbook2"
        mock_pool.close.assert_called_once()

    def test_mount_routes_non_sweep_through_pool(
        self, tmp_path: Path, isolated_db: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """``mount=...`` without sweep still routes the runner through the pool."""
        yaml_path = _write_workflow(tmp_path)

        fake_mount = MagicMock()
        fake_mount.name = "cookbook2"
        fake_mount.local = str(tmp_path)
        fake_mount.remote = "/home/remote/cookbook2"
        fake_profile = MagicMock()
        fake_profile.mounts = [fake_mount]

        monkeypatch.setattr(
            "srunx.ssh.core.config.ConfigManager.get_current_profile",
            lambda self: fake_profile,
        )
        monkeypatch.setattr(
            "srunx.ssh.core.config.ConfigManager.get_current_profile_name",
            lambda self: "pyxis",
        )
        with (
            patch("srunx.slurm.clients.ssh.SlurmSSHClient.__init__", return_value=None),
            patch(
                "srunx.slurm.clients.ssh.SlurmSSHClient.connection_spec",
                new=MagicMock(),
            ),
            patch("srunx.slurm.ssh_executor.SlurmSSHExecutorPool") as pool_cls,
            patch(
                "srunx.runtime.workflow.runner.WorkflowRunner.from_yaml"
            ) as from_yaml,
        ):
            mock_pool = MagicMock()
            pool_cls.return_value = mock_pool
            mock_runner = MagicMock()
            mock_runner.workflow.name = "mcp_wf"
            mock_runner.run.return_value = {}
            from_yaml.return_value = mock_runner

            result = run_workflow(str(yaml_path), mount="cookbook2")

        assert result["success"] is True, result
        pool_cls.assert_called_once()
        run_call_kwargs = from_yaml.call_args_list[-1].kwargs
        assert run_call_kwargs["executor_factory"] is mock_pool.lease
        assert run_call_kwargs["submission_context"].mount_name == "cookbook2"
        mock_pool.close.assert_called_once()

    def test_mount_rejects_shell_job_escaping_mount_root(
        self, tmp_path: Path, isolated_db: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """ShellJob ``script_path`` outside mount local root → error."""
        mount_root = tmp_path / "proj"
        mount_root.mkdir()
        escape = tmp_path / "escape.sh"
        escape.write_text("#!/bin/bash\necho hi\n")
        wf = {
            "name": "shell_wf",
            "jobs": [
                {
                    "name": "bad",
                    "template": "shell",
                    "script_path": str(escape),
                }
            ],
        }
        yaml_path = tmp_path / "shell_wf.yaml"
        yaml_path.write_text(yaml.dump(wf))

        fake_mount = MagicMock()
        fake_mount.name = "cookbook2"
        fake_mount.local = str(mount_root)
        fake_mount.remote = "/home/remote/cookbook2"
        fake_profile = MagicMock()
        fake_profile.mounts = [fake_mount]

        monkeypatch.setattr(
            "srunx.ssh.core.config.ConfigManager.get_current_profile",
            lambda self: fake_profile,
        )
        monkeypatch.setattr(
            "srunx.ssh.core.config.ConfigManager.get_current_profile_name",
            lambda self: "pyxis",
        )
        with patch(
            "srunx.slurm.clients.ssh.SlurmSSHClient.__init__", return_value=None
        ):
            result = run_workflow(str(yaml_path), mount="cookbook2")

        assert result["success"] is False
        assert "outside allowed directories" in result["error"]
