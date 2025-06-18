"""Tests for srunx.cli.workflow module."""

import argparse
import tempfile
from pathlib import Path
from unittest.mock import Mock, patch

import pytest
import yaml

from srunx.cli.workflow import (
    cmd_run_workflow,
    create_workflow_parser,
    main,
)
from srunx.models import Job, JobEnvironment, JobResource, Workflow, WorkflowTask


class TestParserCreation:
    """Test workflow parser creation."""

    def test_create_workflow_parser(self) -> None:
        """Test create_workflow_parser function."""
        parser = create_workflow_parser()

        assert isinstance(parser, argparse.ArgumentParser)

        # Test with required argument
        args = parser.parse_args(["workflow.yaml"])
        assert args.yaml_file == "workflow.yaml"
        assert args.validate_only is False
        assert args.dry_run is False
        assert args.log_level == "INFO"

    def test_create_workflow_parser_with_options(self) -> None:
        """Test create_workflow_parser with all options."""
        parser = create_workflow_parser()

        args = parser.parse_args(
            [
                "workflow.yaml",
                "--validate-only",
                "--dry-run",
                "--log-level",
                "DEBUG",
            ]
        )

        assert args.yaml_file == "workflow.yaml"
        assert args.validate_only is True
        assert args.dry_run is True
        assert args.log_level == "DEBUG"


class TestValidationFunctions:
    """Test workflow validation functions."""

    def test_validate_workflow_dependencies_valid(self) -> None:
        """Test validate with valid workflow."""
        job1 = Job(
            name="job1",
            command=["echo", "1"],
            environment=JobEnvironment(conda="env1"),
        )
        job2 = Job(
            name="job2",
            command=["echo", "2"],
            environment=JobEnvironment(conda="env2"),
        )

        task1 = WorkflowTask(name="task1", job=job1)
        task2 = WorkflowTask(name="task2", job=job2, depends_on=["task1"])

        workflow = Workflow(name="valid_workflow", tasks=[task1, task2])

        # Should not raise any exception
        workflow.validate()

    def test_validate_workflow_dependencies_unknown_dependency(self) -> None:
        """Test validate with unknown dependency."""
        job = Job(
            name="job1",
            command=["echo", "1"],
            environment=JobEnvironment(conda="env1"),
        )

        task = WorkflowTask(name="task1", job=job, depends_on=["unknown_task"])
        workflow = Workflow(name="invalid_workflow", tasks=[task])

        with pytest.raises(ValueError, match="depends on unknown task"):
            workflow.validate()

    def test_validate_workflow_dependencies_circular(self) -> None:
        """Test validate with circular dependency."""
        job1 = Job(
            name="job1",
            command=["echo", "1"],
            environment=JobEnvironment(conda="env1"),
        )
        job2 = Job(
            name="job2",
            command=["echo", "2"],
            environment=JobEnvironment(conda="env2"),
        )

        task1 = WorkflowTask(name="task1", job=job1, depends_on=["task2"])
        task2 = WorkflowTask(name="task2", job=job2, depends_on=["task1"])

        workflow = Workflow(name="circular_workflow", tasks=[task1, task2])

        with pytest.raises(ValueError, match="Circular dependency detected"):
            workflow.validate()

    def test_validate_workflow_dependencies_complex_circular(self) -> None:
        """Test validate with complex circular dependency."""
        job1 = Job(
            name="job1", command=["echo", "1"], environment=JobEnvironment(conda="env1")
        )
        job2 = Job(
            name="job2", command=["echo", "2"], environment=JobEnvironment(conda="env2")
        )
        job3 = Job(
            name="job3", command=["echo", "3"], environment=JobEnvironment(conda="env3")
        )

        # task1 -> task2 -> task3 -> task1 (circular)
        task1 = WorkflowTask(name="task1", job=job1, depends_on=["task3"])
        task2 = WorkflowTask(name="task2", job=job2, depends_on=["task1"])
        task3 = WorkflowTask(name="task3", job=job3, depends_on=["task2"])

        workflow = Workflow(name="complex_circular", tasks=[task1, task2, task3])

        with pytest.raises(ValueError, match="Circular dependency detected"):
            workflow.validate()

    def test_validate_workflow_dependencies_self_dependency(self) -> None:
        """Test validate with self dependency."""
        job = Job(
            name="job1",
            command=["echo", "1"],
            environment=JobEnvironment(conda="env1"),
        )

        task = WorkflowTask(name="task1", job=job, depends_on=["task1"])
        workflow = Workflow(name="self_dep_workflow", tasks=[task])

        with pytest.raises(ValueError, match="Circular dependency detected"):
            workflow.validate()


class TestShowWorkflowPlan:
    """Test show function."""

    @patch("builtins.print")
    def test_show_workflow_plan_basic(self, mock_print: Mock) -> None:
        """Test show with basic workflow."""
        job = Job(
            name="plan_job",
            command=["python", "script.py"],
            environment=JobEnvironment(conda="plan_env"),
        )

        task = WorkflowTask(name="plan_task", job=job)
        workflow = Workflow(name="plan_workflow", tasks=[task])

        workflow.show()

        # Verify that print was called exactly once
        mock_print.assert_called_once()

        # Get the printed message
        printed_message = mock_print.call_args[0][0]

        # Verify workflow name is logged
        assert "plan_workflow" in printed_message

        # Verify task name is logged
        assert "plan_task" in printed_message

        # Verify command is logged
        assert "python script.py" in printed_message

        # Verify conda environment is logged
        assert "plan_env" in printed_message

        # Verify resources information is logged (defaults would be applied)
        assert "Resources:" in printed_message

    @patch("builtins.print")
    def test_show_workflow_plan_complex(self, mock_print: Mock) -> None:
        """Test show with complex workflow."""
        resources = JobResource(nodes=2, gpus_per_node=1, memory_per_node="32GB")
        job1 = Job(
            name="gpu_job",
            command=["python", "gpu_script.py"],
            resources=resources,
            environment=JobEnvironment(conda="gpu_env"),
        )
        job2 = Job(
            name="container_job",
            command=["python", "container_script.py"],
            environment=JobEnvironment(sqsh="/path/to/image.sqsh"),
        )

        task1 = WorkflowTask(name="gpu_task", job=job1)
        task2 = WorkflowTask(
            name="container_task",
            job=job2,
            depends_on=["gpu_task"],
            async_execution=True,
        )

        workflow = Workflow(name="complex_workflow", tasks=[task1, task2])

        workflow.show()

        # Verify that complex details are printed
        printed_output = mock_print.call_args[0][0]

        assert "2 nodes" in printed_output
        assert "1 GPUs/node" in printed_output
        assert "gpu_env" in printed_output
        assert "/path/to/image.sqsh" in printed_output
        assert "gpu_task" in printed_output
        assert "asynchronous" in printed_output


class TestCommandFunction:
    """Test cmd_run_workflow function."""

    @patch("srunx.cli.workflow.configure_workflow_logging")
    @patch("srunx.cli.workflow.WorkflowRunner")
    def test_cmd_run_workflow_validate_only(
        self, mock_runner_class: Mock, mock_configure: Mock
    ) -> None:
        """Test cmd_run_workflow with validate-only option."""
        # Setup
        yaml_content = {
            "name": "test_workflow",
            "tasks": [
                {
                    "name": "test_task",
                    "command": ["echo", "test"],
                    "conda": "test_env",
                }
            ],
        }

        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            yaml.dump(yaml_content, f)
            yaml_path = f.name

        try:
            mock_runner = Mock()
            workflow = Workflow(
                name="test_workflow",
                tasks=[
                    WorkflowTask(
                        name="test_task",
                        job=Job(
                            name="test_task",
                            command=["echo", "test"],
                            environment=JobEnvironment(conda="test_env"),
                        ),
                    )
                ],
            )
            mock_runner.load_from_yaml.return_value = workflow
            mock_runner_class.return_value = mock_runner

            args = argparse.Namespace(
                yaml_file=yaml_path,
                validate_only=True,
                dry_run=False,
                log_level="INFO",
            )

            # Execute
            cmd_run_workflow(args)

            # Verify
            mock_configure.assert_called_once_with(level="INFO")
            mock_runner.load_from_yaml.assert_called_once()
            mock_runner.execute_workflow.assert_not_called()

        finally:
            Path(yaml_path).unlink()

    @patch("srunx.cli.workflow.configure_workflow_logging")
    @patch("srunx.cli.workflow.WorkflowRunner")
    @patch("builtins.print")
    def test_cmd_run_workflow_dry_run(
        self, mock_print: Mock, mock_runner_class: Mock, mock_configure: Mock
    ) -> None:
        """Test cmd_run_workflow with dry-run option."""
        # Setup
        yaml_content = {
            "name": "dry_run_workflow",
            "tasks": [
                {
                    "name": "dry_run_task",
                    "command": ["echo", "dry_run"],
                    "conda": "test_env",
                }
            ],
        }

        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            yaml.dump(yaml_content, f)
            yaml_path = f.name

        try:
            mock_runner = Mock()
            workflow = Workflow(
                name="dry_run_workflow",
                tasks=[
                    WorkflowTask(
                        name="dry_run_task",
                        job=Job(
                            name="dry_run_task",
                            command=["echo", "dry_run"],
                            environment=JobEnvironment(conda="test_env"),
                        ),
                    )
                ],
            )
            mock_runner.load_from_yaml.return_value = workflow
            mock_runner_class.return_value = mock_runner

            args = argparse.Namespace(
                yaml_file=yaml_path,
                validate_only=False,
                dry_run=True,
                log_level="INFO",
            )

            # Execute
            cmd_run_workflow(args)

            # Verify
            mock_runner.load_from_yaml.assert_called_once()
            mock_runner.execute_workflow.assert_not_called()

        finally:
            Path(yaml_path).unlink()

    @patch("srunx.cli.workflow.configure_workflow_logging")
    @patch("srunx.cli.workflow.WorkflowRunner")
    def test_cmd_run_workflow_execute(
        self, mock_runner_class: Mock, mock_configure: Mock
    ) -> None:
        """Test cmd_run_workflow with actual execution."""
        # Setup
        yaml_content = {
            "name": "execute_workflow",
            "tasks": [
                {
                    "name": "execute_task",
                    "command": ["echo", "execute"],
                    "conda": "test_env",
                }
            ],
        }

        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            yaml.dump(yaml_content, f)
            yaml_path = f.name

        try:
            mock_runner = Mock()
            workflow = Workflow(
                name="execute_workflow",
                tasks=[
                    WorkflowTask(
                        name="execute_task",
                        job=Job(
                            name="execute_task",
                            command=["echo", "execute"],
                            environment=JobEnvironment(conda="test_env"),
                        ),
                    )
                ],
            )
            mock_runner.load_from_yaml.return_value = workflow
            mock_runner.execute_workflow.return_value = {
                "execute_task": Mock(job_id=12345)
            }
            mock_runner_class.return_value = mock_runner

            args = argparse.Namespace(
                yaml_file=yaml_path,
                validate_only=False,
                dry_run=False,
                log_level="INFO",
            )

            # Execute
            cmd_run_workflow(args)

            # Verify
            mock_runner.load_from_yaml.assert_called_once()
            mock_runner.execute_workflow.assert_called_once_with(workflow)

        finally:
            Path(yaml_path).unlink()

    @patch("srunx.cli.workflow.configure_workflow_logging")
    @patch("srunx.cli.workflow.sys.exit")
    def test_cmd_run_workflow_file_not_found(
        self, mock_exit: Mock, mock_configure: Mock
    ) -> None:
        """Test cmd_run_workflow with non-existent file."""
        args = argparse.Namespace(
            yaml_file="/nonexistent/workflow.yaml",
            validate_only=False,
            dry_run=False,
            log_level="INFO",
        )

        # Execute
        cmd_run_workflow(args)

        # Verify - sys.exit may be called more than once in some error scenarios
        # Just check that it was called with exit code 1
        assert mock_exit.called
        # Check that at least one call was with exit code 1
        exit_calls = [call[0][0] for call in mock_exit.call_args_list if call[0]]
        assert 1 in exit_calls

    @patch("srunx.cli.workflow.configure_workflow_logging")
    @patch("srunx.cli.workflow.WorkflowRunner")
    @patch("srunx.cli.workflow.sys.exit")
    def test_cmd_run_workflow_validation_error(
        self, mock_exit: Mock, mock_runner_class: Mock, mock_configure: Mock
    ) -> None:
        """Test cmd_run_workflow with validation error."""
        # Setup
        yaml_content = {
            "name": "invalid_workflow",
            "tasks": [
                {
                    "name": "invalid_task",
                    "command": ["echo", "test"],
                    "depends_on": ["nonexistent_task"],  # Invalid dependency
                    "conda": "test_env",
                }
            ],
        }

        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            yaml.dump(yaml_content, f)
            yaml_path = f.name

        try:
            mock_runner = Mock()
            workflow = Workflow(
                name="invalid_workflow",
                tasks=[
                    WorkflowTask(
                        name="invalid_task",
                        job=Job(
                            name="invalid_task",
                            command=["echo", "test"],
                            environment=JobEnvironment(conda="test_env"),
                        ),
                        depends_on=["nonexistent_task"],
                    )
                ],
            )
            mock_runner.load_from_yaml.return_value = workflow
            mock_runner_class.return_value = mock_runner

            args = argparse.Namespace(
                yaml_file=yaml_path,
                validate_only=False,
                dry_run=False,
                log_level="INFO",
            )

            # Execute
            cmd_run_workflow(args)

            # Verify
            mock_exit.assert_called_once_with(1)

        finally:
            Path(yaml_path).unlink()

    @patch("srunx.cli.workflow.configure_workflow_logging")
    @patch("srunx.cli.workflow.WorkflowRunner")
    @patch("srunx.cli.workflow.sys.exit")
    def test_cmd_run_workflow_execution_error(
        self, mock_exit: Mock, mock_runner_class: Mock, mock_configure: Mock
    ) -> None:
        """Test cmd_run_workflow with execution error."""
        # Setup
        yaml_content = {
            "name": "error_workflow",
            "tasks": [
                {
                    "name": "error_task",
                    "command": ["false"],  # Command that will fail
                    "conda": "test_env",
                }
            ],
        }

        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            yaml.dump(yaml_content, f)
            yaml_path = f.name

        try:
            mock_runner = Mock()
            workflow = Workflow(
                name="error_workflow",
                tasks=[
                    WorkflowTask(
                        name="error_task",
                        job=Job(
                            name="error_task",
                            command=["false"],
                            environment=JobEnvironment(conda="test_env"),
                        ),
                    )
                ],
            )
            mock_runner.load_from_yaml.return_value = workflow
            mock_runner.execute_workflow.side_effect = Exception("Execution failed")
            mock_runner_class.return_value = mock_runner

            args = argparse.Namespace(
                yaml_file=yaml_path,
                validate_only=False,
                dry_run=False,
                log_level="INFO",
            )

            # Execute
            cmd_run_workflow(args)

            # Verify
            mock_exit.assert_called_once_with(1)

        finally:
            Path(yaml_path).unlink()


class TestMainFunction:
    """Test main function."""

    @patch("srunx.cli.workflow.create_workflow_parser")
    @patch("srunx.cli.workflow.cmd_run_workflow")
    def test_main(self, mock_cmd: Mock, mock_create_parser: Mock) -> None:
        """Test main function."""
        # Setup
        mock_parser = Mock()
        mock_args = Mock()
        mock_parser.parse_args.return_value = mock_args
        mock_create_parser.return_value = mock_parser

        # Execute
        with patch("sys.argv", ["srunx", "flow", "run", "workflow.yaml"]):
            main()

        # Verify
        mock_create_parser.assert_called_once()
        mock_parser.parse_args.assert_called_once()
        mock_cmd.assert_called_once_with(mock_args)


class TestIntegration:
    """Test integration scenarios."""

    def test_full_workflow_parsing_and_validation(self) -> None:
        """Test complete workflow parsing and validation."""
        yaml_content = {
            "name": "integration_workflow",
            "tasks": [
                {
                    "name": "preprocess",
                    "command": ["python", "preprocess.py"],
                    "conda": "data_env",
                    "nodes": 1,
                },
                {
                    "name": "train",
                    "command": ["python", "train.py"],
                    "depends_on": ["preprocess"],
                    "conda": "ml_env",
                    "nodes": 2,
                    "gpus_per_node": 1,
                    "memory_per_node": "32GB",
                    "time_limit": "4:00:00",
                },
                {
                    "name": "evaluate",
                    "command": ["python", "evaluate.py"],
                    "depends_on": ["train"],
                    "conda": "ml_env",
                    "async": True,
                },
                {
                    "name": "notify",
                    "command": ["python", "notify.py"],
                    "depends_on": ["train", "evaluate"],
                    "venv": "/path/to/notification_env",
                },
            ],
        }

        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            yaml.dump(yaml_content, f)
            yaml_path = f.name

        try:
            parser = create_workflow_parser()

            # Test parsing
            args = parser.parse_args([yaml_path, "--validate-only"])
            assert args.yaml_file == yaml_path
            assert args.validate_only is True

            # Test validation (would normally be called by cmd_run_workflow)
            from srunx.workflows.runner import WorkflowRunner

            runner = WorkflowRunner()
            workflow = runner.load_from_yaml(yaml_path)

            # Should not raise validation error
            workflow.validate()

            # Check workflow structure
            assert workflow.name == "integration_workflow"
            assert len(workflow.tasks) == 4

            task_names = [task.name for task in workflow.tasks]
            assert "preprocess" in task_names
            assert "train" in task_names
            assert "evaluate" in task_names
            assert "notify" in task_names

        finally:
            Path(yaml_path).unlink()
