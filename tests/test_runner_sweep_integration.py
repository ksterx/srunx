"""Integration tests for the Phase-C sweep hooks on ``WorkflowRunner``.

Covers the two kwargs added to support the sweep orchestrator:

* ``WorkflowRunner.from_yaml(args_override=...)`` — CLI/Web/MCP hub for
  overriding workflow ``args`` without editing the YAML.
* ``WorkflowRunner.run(workflow_run_id=...)`` — lets the orchestrator
  attach runner output to a pre-materialised ``workflow_runs`` row
  instead of creating one itself.
"""

from __future__ import annotations

from unittest.mock import Mock, patch

import yaml  # type: ignore

from srunx.models import Job, JobEnvironment, JobStatus, Workflow
from srunx.runner import WorkflowRunner


def _write_yaml(path, data: dict) -> None:
    with open(path, "w") as f:
        yaml.dump(data, f)


class TestFromYamlArgsOverride:
    """``from_yaml(args_override=...)`` semantics."""

    def test_override_existing_arg(self, temp_dir):
        yaml_content = {
            "name": "wf",
            "args": {"lr": 0.1, "epochs": 10},
            "jobs": [
                {
                    "name": "train",
                    "command": ["train.py", "--lr", "{{ lr }}"],
                    "environment": {"conda": "env"},
                }
            ],
        }
        yaml_path = temp_dir / "wf.yaml"
        _write_yaml(yaml_path, yaml_content)

        runner = WorkflowRunner.from_yaml(yaml_path, args_override={"lr": 0.01})

        assert runner.args["lr"] == 0.01
        # Untouched key survives.
        assert runner.args["epochs"] == 10
        # Override is propagated into the rendered command.
        train_job = runner.workflow.jobs[0]
        assert train_job.command == ["train.py", "--lr", "0.01"]

    def test_override_adds_new_key(self, temp_dir):
        yaml_content = {
            "name": "wf",
            "args": {"lr": 0.1},
            "jobs": [
                {
                    "name": "train",
                    "command": ["train.py", "--lr", "{{ lr }}", "--tag", "{{ tag }}"],
                    "environment": {"conda": "env"},
                }
            ],
        }
        yaml_path = temp_dir / "wf.yaml"
        _write_yaml(yaml_path, yaml_content)

        runner = WorkflowRunner.from_yaml(yaml_path, args_override={"tag": "exp-1"})

        assert runner.args["tag"] == "exp-1"
        assert runner.args["lr"] == 0.1  # preserved
        train_job = runner.workflow.jobs[0]
        assert train_job.command == [
            "train.py",
            "--lr",
            "0.1",
            "--tag",
            "exp-1",
        ]

    def test_override_merges_not_replaces(self, temp_dir):
        """Override must be a merge, not a full replacement."""
        yaml_content = {
            "name": "wf",
            "args": {
                "lr": 0.1,
                "epochs": 10,
                "base_dir": "/data",
            },
            "jobs": [
                {
                    "name": "train",
                    "command": ["train.py"],
                    "environment": {"conda": "env"},
                }
            ],
        }
        yaml_path = temp_dir / "wf.yaml"
        _write_yaml(yaml_path, yaml_content)

        runner = WorkflowRunner.from_yaml(yaml_path, args_override={"lr": 0.5})

        assert runner.args == {"lr": 0.5, "epochs": 10, "base_dir": "/data"}

    def test_no_override_preserves_original(self, temp_dir):
        yaml_content = {
            "name": "wf",
            "args": {"lr": 0.1},
            "jobs": [
                {
                    "name": "train",
                    "command": ["train.py"],
                    "environment": {"conda": "env"},
                }
            ],
        }
        yaml_path = temp_dir / "wf.yaml"
        _write_yaml(yaml_path, yaml_content)

        runner = WorkflowRunner.from_yaml(yaml_path)

        assert runner.args == {"lr": 0.1}


class TestRunWorkflowRunIdInjection:
    """``run(workflow_run_id=...)`` semantics."""

    def _make_runner(self) -> tuple[WorkflowRunner, Job]:
        job = Job(
            name="test_job",
            command=["echo", "hi"],
            environment=JobEnvironment(conda="env"),
        )
        job.status = JobStatus.COMPLETED
        workflow = Workflow(name="injected", jobs=[job])
        runner = WorkflowRunner(workflow)
        return runner, job

    @patch("srunx.runner._transition_workflow_run")
    @patch("srunx.db.cli_helpers.create_cli_workflow_run")
    @patch("srunx.runner.Slurm")
    def test_injected_id_skips_create(
        self,
        mock_slurm_class,
        mock_create,
        mock_transition,
    ):
        mock_slurm = Mock()
        mock_slurm_class.return_value = mock_slurm

        runner, job = self._make_runner()
        mock_slurm.run.return_value = job

        injected_id = 424242
        results = runner.run(workflow_run_id=injected_id)

        assert "test_job" in results
        # The orchestrator's id must not be shadowed by a CLI-created row.
        mock_create.assert_not_called()
        # And the id must be forwarded to the Slurm submission call so
        # ``jobs.workflow_run_id`` links to the sweep cell.
        call_kwargs = mock_slurm.run.call_args.kwargs
        assert call_kwargs["workflow_run_id"] == injected_id
        # State service was called with the injected id.
        assert mock_transition.called
        assert mock_transition.call_args_list[0].args[0] == injected_id

    @patch("srunx.runner._transition_workflow_run")
    @patch("srunx.db.cli_helpers.create_cli_workflow_run")
    @patch("srunx.runner.Slurm")
    def test_no_id_falls_back_to_create(
        self,
        mock_slurm_class,
        mock_create,
        mock_transition,
    ):
        mock_slurm = Mock()
        mock_slurm_class.return_value = mock_slurm
        mock_create.return_value = 999

        runner, job = self._make_runner()
        mock_slurm.run.return_value = job

        results = runner.run()

        assert "test_job" in results
        mock_create.assert_called_once()
        call_kwargs = mock_slurm.run.call_args.kwargs
        assert call_kwargs["workflow_run_id"] == 999

    @patch("srunx.runner._transition_workflow_run")
    @patch("srunx.db.cli_helpers.create_cli_workflow_run")
    @patch("srunx.runner.Slurm")
    def test_create_returning_none_does_not_call_state_service(
        self,
        mock_slurm_class,
        mock_create,
        mock_transition,
    ):
        """When ``create_cli_workflow_run`` returns None (DB outage), the
        state-service hook must be skipped so no spurious log noise fires.
        """
        mock_slurm = Mock()
        mock_slurm_class.return_value = mock_slurm
        mock_create.return_value = None

        runner, job = self._make_runner()
        mock_slurm.run.return_value = job

        runner.run()

        mock_create.assert_called_once()
        mock_transition.assert_not_called()
