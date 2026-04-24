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


class TestMCPArgsOverride:
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


class TestMCPSweep:
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


class TestMCPBackwardCompat:
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


class TestMCPMountRouting:
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
                "srunx.slurm.ssh.SlurmSSHAdapter.__init__", return_value=None
            ) as adapter_init,
            patch(
                "srunx.slurm.ssh.SlurmSSHAdapter.connection_spec",
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
        adapter_init.assert_called_once()  # adapter built from the profile
        pool_cls.assert_called_once()  # pool built once
        # Orchestrator received the pool's lease + a populated render context.
        orch_kwargs = orch_cls.call_args.kwargs
        assert orch_kwargs["executor_factory"] is mock_pool.lease
        ctx = orch_kwargs["submission_context"]
        assert ctx is not None
        assert ctx.mount_name == "cookbook2"
        assert ctx.default_work_dir == "/home/remote/cookbook2"
        # Pool is closed after the orchestrator returns (or raises).
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
            patch("srunx.slurm.ssh.SlurmSSHAdapter.__init__", return_value=None),
            patch(
                "srunx.slurm.ssh.SlurmSSHAdapter.connection_spec",
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
            # Two from_yaml calls: ShellJob guard (bare) + run (with factory).
            from_yaml.return_value = mock_runner

            result = run_workflow(str(yaml_path), mount="cookbook2")

        assert result["success"] is True, result
        pool_cls.assert_called_once()
        # Second from_yaml call receives the executor_factory + context.
        run_call_kwargs = from_yaml.call_args_list[-1].kwargs
        assert run_call_kwargs["executor_factory"] is mock_pool.lease
        assert run_call_kwargs["submission_context"].mount_name == "cookbook2"
        mock_pool.close.assert_called_once()

    def test_mount_rejects_shell_job_escaping_mount_root(
        self, tmp_path: Path, isolated_db: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """ShellJob ``script_path`` outside mount local root → error."""
        # Write a ShellJob workflow whose script_path is ../escape.sh
        mount_root = tmp_path / "proj"
        mount_root.mkdir()
        escape = tmp_path / "escape.sh"  # NOT under mount_root
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
        with patch("srunx.slurm.ssh.SlurmSSHAdapter.__init__", return_value=None):
            result = run_workflow(str(yaml_path), mount="cookbook2")

        assert result["success"] is False
        assert "outside allowed directories" in result["error"]
