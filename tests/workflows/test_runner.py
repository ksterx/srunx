"""Tests for srunx.workflows.runner module."""

import tempfile
from pathlib import Path
from unittest.mock import Mock, patch

import pytest
import yaml

from srunx.models import Job, JobEnvironment, JobStatus, Workflow, WorkflowTask
from srunx.workflows.runner import WorkflowRunner, run_workflow_from_file


class TestWorkflowRunner:
    """Test WorkflowRunner class."""

    def test_init(self) -> None:
        """Test WorkflowRunner initialization."""
        runner = WorkflowRunner()
        assert runner.executed_tasks == {}

    def test_load_from_yaml_basic(self) -> None:
        """Test loading basic workflow from YAML."""
        yaml_content = {
            "name": "test_workflow",
            "tasks": [
                {
                    "name": "task1",
                    "command": ["python", "script1.py"],
                    "conda": "env1",
                },
                {
                    "name": "task2",
                    "command": ["python", "script2.py"],
                    "depends_on": ["task1"],
                    "venv": "/path/to/venv",
                },
            ],
        }

        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            yaml.dump(yaml_content, f)
            yaml_path = f.name

        try:
            runner = WorkflowRunner()
            workflow = runner.load_from_yaml(yaml_path)

            assert workflow.name == "test_workflow"
            assert len(workflow.tasks) == 2

            task1 = workflow.tasks[0]
            assert isinstance(task1.job, Job)
            assert task1.name == "task1"
            assert task1.job.command == ["python", "script1.py"]
            assert task1.job.environment.conda == "env1"
            assert task1.depends_on == []

            task2 = workflow.tasks[1]
            assert isinstance(task2.job, Job)
            assert task2.name == "task2"
            assert task2.job.command == ["python", "script2.py"]
            assert task2.job.environment.venv == "/path/to/venv"
            assert task2.depends_on == ["task1"]

        finally:
            Path(yaml_path).unlink()

    def test_load_from_yaml_with_resources(self) -> None:
        """Test loading workflow with resource specifications."""
        yaml_content = {
            "name": "resource_workflow",
            "tasks": [
                {
                    "name": "gpu_task",
                    "command": ["python", "gpu_script.py"],
                    "conda": "gpu_env",
                    "nodes": 2,
                    "gpus_per_node": 1,
                    "memory_per_node": "32GB",
                    "time_limit": "2:00:00",
                    "cpus_per_task": 4,
                },
            ],
        }

        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            yaml.dump(yaml_content, f)
            yaml_path = f.name

        try:
            runner = WorkflowRunner()
            workflow = runner.load_from_yaml(yaml_path)

            task = workflow.tasks[0]
            assert isinstance(task.job, Job)
            assert task.job.resources.nodes == 2
            assert task.job.resources.gpus_per_node == 1
            assert task.job.resources.memory_per_node == "32GB"
            assert task.job.resources.time_limit == "2:00:00"
            assert task.job.resources.cpus_per_task == 4

        finally:
            Path(yaml_path).unlink()

    def test_load_from_yaml_with_container(self) -> None:
        """Test loading workflow with container specification."""
        yaml_content = {
            "name": "container_workflow",
            "tasks": [
                {
                    "name": "container_task",
                    "command": ["python", "containerized_script.py"],
                    "sqsh": "/path/to/image.sqsh",
                    "env_vars": {"CUDA_VISIBLE_DEVICES": "0,1"},
                },
            ],
        }

        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            yaml.dump(yaml_content, f)
            yaml_path = f.name

        try:
            runner = WorkflowRunner()
            workflow = runner.load_from_yaml(yaml_path)

            task = workflow.tasks[0]
            assert isinstance(task.job, Job)
            assert task.job.environment.sqsh == "/path/to/image.sqsh"
            assert task.job.environment.env_vars == {"CUDA_VISIBLE_DEVICES": "0,1"}

        finally:
            Path(yaml_path).unlink()

    def test_load_from_yaml_with_async(self) -> None:
        """Test loading workflow with async task."""
        yaml_content = {
            "name": "async_workflow",
            "tasks": [
                {
                    "name": "async_task",
                    "command": ["python", "background_script.py"],
                    "conda": "test_env",
                    "async": True,
                },
            ],
        }

        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            yaml.dump(yaml_content, f)
            yaml_path = f.name

        try:
            runner = WorkflowRunner()
            workflow = runner.load_from_yaml(yaml_path)

            task = workflow.tasks[0]
            assert task.async_execution is True

        finally:
            Path(yaml_path).unlink()

    def test_load_from_yaml_file_not_found(self) -> None:
        """Test loading workflow from non-existent file."""
        runner = WorkflowRunner()

        with pytest.raises(FileNotFoundError):
            runner.load_from_yaml("/nonexistent/workflow.yaml")

    def test_load_from_yaml_invalid_yaml(self) -> None:
        """Test loading workflow from invalid YAML."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write("invalid: yaml: content: [")
            yaml_path = f.name

        try:
            runner = WorkflowRunner()

            with pytest.raises(yaml.YAMLError):
                runner.load_from_yaml(yaml_path)
        finally:
            Path(yaml_path).unlink()

    def test_parse_task_data_minimal(self) -> None:
        """Test parsing minimal task data."""
        runner = WorkflowRunner()

        task_data = {
            "name": "minimal_task",
            "command": ["echo", "hello"],
            "conda": "base",
        }

        task = runner._parse_task_data(task_data)

        assert task.name == "minimal_task"
        assert isinstance(task.job, Job)
        assert task.job.command == ["echo", "hello"]
        assert task.job.environment.conda == "base"
        assert task.depends_on == []
        assert task.async_execution is False
        assert task.job.resources.nodes == 1
        assert task.job.log_dir == "logs"

    def test_parse_task_data_full(self) -> None:
        """Test parsing complete task data."""
        runner = WorkflowRunner()

        task_data = {
            "name": "full_task",
            "command": ["python", "full_script.py"],
            "depends_on": ["task1", "task2"],
            "async": True,
            "nodes": 4,
            "gpus_per_node": 2,
            "ntasks_per_node": 8,
            "cpus_per_task": 2,
            "memory_per_node": "64GB",
            "time_limit": "4:00:00",
            "sqsh": "/path/to/container.sqsh",
            "env_vars": {"OMP_NUM_THREADS": "2"},
            "log_dir": "/custom/log/dir",
            "work_dir": "/custom/work/dir",
        }

        task = runner._parse_task_data(task_data)

        assert task.name == "full_task"
        assert task.depends_on == ["task1", "task2"]
        assert task.async_execution is True
        assert isinstance(task.job, Job)
        assert task.job.resources.nodes == 4
        assert task.job.resources.gpus_per_node == 2
        assert task.job.resources.ntasks_per_node == 8
        assert task.job.resources.cpus_per_task == 2
        assert task.job.resources.memory_per_node == "64GB"
        assert task.job.resources.time_limit == "4:00:00"
        assert task.job.environment.sqsh == "/path/to/container.sqsh"
        assert task.job.environment.env_vars == {"OMP_NUM_THREADS": "2"}
        assert task.job.log_dir == "/custom/log/dir"
        assert task.job.work_dir == "/custom/work/dir"

    def test_parse_task_data_container_alias(self) -> None:
        """Test parsing task data with 'container' alias for 'sqsh'."""
        runner = WorkflowRunner()

        task_data = {
            "name": "container_task",
            "command": ["python", "script.py"],
            "container": "/path/to/image.sqsh",  # Using 'container' instead of 'sqsh'
        }

        task = runner._parse_task_data(task_data)

        # The 'container' field should be mapped to 'sqsh' in the environment
        assert isinstance(task.job, Job)
        assert task.job.environment.sqsh == "/path/to/image.sqsh"
        assert task.job.environment.conda is None
        assert task.job.environment.venv is None

    @patch("srunx.workflows.runner.submit_and_monitor_job")
    @patch("srunx.workflows.runner.submit_job_async")
    def test_execute_workflow_simple(
        self, mock_async: Mock, mock_monitor: Mock
    ) -> None:
        """Test executing simple workflow."""
        # Setup
        job1 = Job(
            name="job1",
            command=["echo", "task1"],
            environment=JobEnvironment(conda="env1"),
        )
        job2 = Job(
            name="job2",
            command=["echo", "task2"],
            environment=JobEnvironment(conda="env2"),
        )

        task1 = WorkflowTask(name="task1", job=job1)
        task2 = WorkflowTask(name="task2", job=job2, depends_on=["task1"])

        workflow = Workflow(name="test_workflow", tasks=[task1, task2])

        # Mock returns
        completed_job1 = Job(
            name="job1",
            command=["echo", "task1"],
            environment=JobEnvironment(conda="env1"),
            job_id=123,
            status=JobStatus.COMPLETED,
        )
        completed_job2 = Job(
            name="job2",
            command=["echo", "task2"],
            environment=JobEnvironment(conda="env2"),
            job_id=456,
            status=JobStatus.COMPLETED,
        )
        mock_monitor.side_effect = [completed_job1, completed_job2]

        runner = WorkflowRunner()

        # Execute
        results = runner.execute_workflow(workflow)

        # Verify
        assert len(results) == 2
        assert "task1" in results
        assert "task2" in results

        # Both tasks should use submit_and_monitor_job (not async)
        assert mock_monitor.call_count == 2
        assert mock_async.call_count == 0

    @patch("srunx.workflows.runner.submit_and_monitor_job")
    @patch("srunx.workflows.runner.submit_job_async")
    def test_execute_workflow_with_async(
        self, mock_async: Mock, mock_monitor: Mock
    ) -> None:
        """Test executing workflow with async task."""
        # Setup
        job1 = Job(
            name="job1",
            command=["echo", "task1"],
            environment=JobEnvironment(conda="env1"),
        )
        job2 = Job(
            name="job2",
            command=["echo", "task2"],
            environment=JobEnvironment(conda="env2"),
        )

        task1 = WorkflowTask(name="task1", job=job1)
        task2 = WorkflowTask(name="task2", job=job2, async_execution=True)

        workflow = Workflow(name="async_workflow", tasks=[task1, task2])

        # Mock returns
        completed_job1 = Job(
            name="job1",
            command=["echo", "task1"],
            environment=JobEnvironment(conda="env1"),
            job_id=123,
            status=JobStatus.COMPLETED,
        )
        submitted_job2 = Job(
            name="job2",
            command=["echo", "task2"],
            environment=JobEnvironment(conda="env2"),
            job_id=456,
            status=JobStatus.PENDING,
        )

        mock_monitor.return_value = completed_job1
        mock_async.return_value = submitted_job2

        runner = WorkflowRunner()

        # Execute
        results = runner.execute_workflow(workflow)

        # Verify
        assert len(results) == 2

        # task1 should use submit_and_monitor_job, task2 should use submit_job_async
        mock_monitor.assert_called_once()
        mock_async.assert_called_once()

    @patch("srunx.workflows.runner.submit_and_monitor_job")
    def test_execute_workflow_with_dependencies(self, mock_monitor: Mock) -> None:
        """Test executing workflow with complex dependencies."""
        # Setup: task3 depends on both task1 and task2
        job1 = Job(
            name="job1", command=["echo", "1"], environment=JobEnvironment(conda="env1")
        )
        job2 = Job(
            name="job2", command=["echo", "2"], environment=JobEnvironment(conda="env2")
        )
        job3 = Job(
            name="job3", command=["echo", "3"], environment=JobEnvironment(conda="env3")
        )

        task1 = WorkflowTask(name="task1", job=job1)
        task2 = WorkflowTask(name="task2", job=job2)
        task3 = WorkflowTask(name="task3", job=job3, depends_on=["task1", "task2"])

        workflow = Workflow(name="dependency_workflow", tasks=[task1, task2, task3])

        # Mock returns
        # Create separate Job objects for mock returns
        result_job1 = Job(
            name="job1",
            command=["echo", "1"],
            environment=JobEnvironment(conda="env1"),
            job_id=111,
        )
        result_job2 = Job(
            name="job2",
            command=["echo", "2"],
            environment=JobEnvironment(conda="env2"),
            job_id=222,
        )
        result_job3 = Job(
            name="job3",
            command=["echo", "3"],
            environment=JobEnvironment(conda="env3"),
            job_id=333,
        )

        mock_monitor.side_effect = [result_job1, result_job2, result_job3]

        runner = WorkflowRunner()

        # Execute
        results = runner.execute_workflow(workflow)

        # Verify
        assert len(results) == 3
        assert mock_monitor.call_count == 3

    def test_execute_from_yaml(self) -> None:
        """Test executing workflow from YAML file."""
        yaml_content = {
            "name": "yaml_workflow",
            "tasks": [
                {
                    "name": "yaml_task",
                    "command": ["echo", "from_yaml"],
                    "conda": "yaml_env",
                },
            ],
        }

        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            yaml.dump(yaml_content, f)
            yaml_path = f.name

        try:
            runner = WorkflowRunner()

            with patch.object(runner, "execute_workflow") as mock_execute:
                mock_execute.return_value = {"yaml_task": Mock()}

                results = runner.execute_from_yaml(yaml_path)

                # Verify
                assert mock_execute.called
                workflow_arg = mock_execute.call_args[0][0]
                assert workflow_arg.name == "yaml_workflow"
                assert len(workflow_arg.tasks) == 1
                assert workflow_arg.tasks[0].name == "yaml_task"

        finally:
            Path(yaml_path).unlink()


class TestRunWorkflowFromFile:
    """Test run_workflow_from_file convenience function."""

    def test_run_workflow_from_file(self) -> None:
        """Test run_workflow_from_file convenience function."""
        yaml_content = {
            "name": "convenience_workflow",
            "tasks": [
                {
                    "name": "convenience_task",
                    "command": ["echo", "convenience"],
                    "conda": "convenience_env",
                },
            ],
        }

        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            yaml.dump(yaml_content, f)
            yaml_path = f.name

        try:
            with patch("srunx.workflows.runner.WorkflowRunner") as mock_runner_class:
                mock_runner = Mock()
                mock_runner.execute_from_yaml.return_value = {
                    "convenience_task": Mock()
                }
                mock_runner_class.return_value = mock_runner

                results = run_workflow_from_file(yaml_path)

                # Verify
                mock_runner_class.assert_called_once()
                mock_runner.execute_from_yaml.assert_called_once_with(yaml_path)
                assert "convenience_task" in results

        finally:
            Path(yaml_path).unlink()


class TestWorkflowValidation:
    """Test workflow validation scenarios."""

    def test_workflow_with_missing_environment(self) -> None:
        """Test workflow task without environment specification."""
        runner = WorkflowRunner()

        task_data = {
            "name": "no_env_task",
            "command": ["echo", "hello"],
            # Missing environment specification
        }

        # This should raise a validation error when creating the JobEnvironment
        with pytest.raises(Exception):  # Could be ValidationError or ValueError
            runner._parse_task_data(task_data)

    def test_workflow_with_invalid_resources(self) -> None:
        """Test workflow task with invalid resource specifications."""
        runner = WorkflowRunner()

        task_data = {
            "name": "invalid_resource_task",
            "command": ["echo", "hello"],
            "conda": "test_env",
            "nodes": 0,  # Invalid: must be >= 1
        }

        with pytest.raises(Exception):  # ValidationError from Pydantic
            runner._parse_task_data(task_data)

    def test_workflow_empty_tasks(self) -> None:
        """Test workflow with no tasks."""
        runner = WorkflowRunner()

        data = {
            "name": "empty_workflow",
            "tasks": [],
        }

        workflow = runner._parse_workflow_data(data)

        assert workflow.name == "empty_workflow"
        assert len(workflow.tasks) == 0

    def test_workflow_default_name(self) -> None:
        """Test workflow without name specification."""
        runner = WorkflowRunner()

        data = {
            "tasks": [
                {
                    "name": "unnamed_task",
                    "command": ["echo", "hello"],
                    "conda": "test_env",
                }
            ],
        }

        workflow = runner._parse_workflow_data(data)

        assert workflow.name == "unnamed_workflow"

    def test_build_execution_levels_simple(self) -> None:
        """Test building execution levels for simple linear workflow."""
        runner = WorkflowRunner()
        
        # Create a simple linear workflow: task1 -> task2 -> task3
        job1 = Job(name="job1", command=["echo", "1"], environment=JobEnvironment(conda="env1"))
        job2 = Job(name="job2", command=["echo", "2"], environment=JobEnvironment(conda="env2"))
        job3 = Job(name="job3", command=["echo", "3"], environment=JobEnvironment(conda="env3"))
        
        task1 = WorkflowTask(name="task1", job=job1)
        task2 = WorkflowTask(name="task2", job=job2, depends_on=["task1"])
        task3 = WorkflowTask(name="task3", job=job3, depends_on=["task2"])
        
        workflow = Workflow(name="linear_workflow", tasks=[task1, task2, task3])
        
        # Test execution levels
        levels = runner._build_execution_levels(workflow)
        
        assert len(levels) == 3
        assert levels[0] == ["task1"]
        assert levels[1] == ["task2"]
        assert levels[2] == ["task3"]

    def test_build_execution_levels_parallel_branches(self) -> None:
        """Test building execution levels for workflow with parallel branches."""
        runner = WorkflowRunner()
        
        # Create workflow with parallel branches:
        # task1 -> task2, task3 (parallel)
        # task4 depends on both task2 and task3
        job1 = Job(name="job1", command=["echo", "1"], environment=JobEnvironment(conda="env1"))
        job2 = Job(name="job2", command=["echo", "2"], environment=JobEnvironment(conda="env2"))
        job3 = Job(name="job3", command=["echo", "3"], environment=JobEnvironment(conda="env3"))
        job4 = Job(name="job4", command=["echo", "4"], environment=JobEnvironment(conda="env4"))
        
        task1 = WorkflowTask(name="task1", job=job1)
        task2 = WorkflowTask(name="task2", job=job2, depends_on=["task1"])
        task3 = WorkflowTask(name="task3", job=job3, depends_on=["task1"])
        task4 = WorkflowTask(name="task4", job=job4, depends_on=["task2", "task3"])
        
        workflow = Workflow(name="parallel_workflow", tasks=[task1, task2, task3, task4])
        
        # Test execution levels
        levels = runner._build_execution_levels(workflow)
        
        assert len(levels) == 3
        assert levels[0] == ["task1"]
        assert set(levels[1]) == {"task2", "task3"}  # These can run in parallel
        assert levels[2] == ["task4"]

    def test_build_execution_levels_multiple_parallel_branches(self) -> None:
        """Test building execution levels for workflow with multiple parallel branches."""
        runner = WorkflowRunner()
        
        # Create workflow with multiple parallel branches:
        # task1 -> task2, task3, task4 (all parallel)
        # task5 depends on task2 and task3
        # task6 depends on task4
        # task7 depends on task5 and task6
        job1 = Job(name="job1", command=["echo", "1"], environment=JobEnvironment(conda="env1"))
        job2 = Job(name="job2", command=["echo", "2"], environment=JobEnvironment(conda="env2"))
        job3 = Job(name="job3", command=["echo", "3"], environment=JobEnvironment(conda="env3"))
        job4 = Job(name="job4", command=["echo", "4"], environment=JobEnvironment(conda="env4"))
        job5 = Job(name="job5", command=["echo", "5"], environment=JobEnvironment(conda="env5"))
        job6 = Job(name="job6", command=["echo", "6"], environment=JobEnvironment(conda="env6"))
        job7 = Job(name="job7", command=["echo", "7"], environment=JobEnvironment(conda="env7"))
        
        task1 = WorkflowTask(name="task1", job=job1)
        task2 = WorkflowTask(name="task2", job=job2, depends_on=["task1"])
        task3 = WorkflowTask(name="task3", job=job3, depends_on=["task1"])
        task4 = WorkflowTask(name="task4", job=job4, depends_on=["task1"])
        task5 = WorkflowTask(name="task5", job=job5, depends_on=["task2", "task3"])
        task6 = WorkflowTask(name="task6", job=job6, depends_on=["task4"])
        task7 = WorkflowTask(name="task7", job=job7, depends_on=["task5", "task6"])
        
        workflow = Workflow(name="complex_parallel_workflow", tasks=[task1, task2, task3, task4, task5, task6, task7])
        
        # Test execution levels
        levels = runner._build_execution_levels(workflow)
        
        assert len(levels) == 4
        assert levels[0] == ["task1"]
        assert set(levels[1]) == {"task2", "task3", "task4"}  # These can run in parallel
        assert set(levels[2]) == {"task5", "task6"}  # These can run in parallel
        assert levels[3] == ["task7"]

    def test_build_execution_levels_no_dependencies(self) -> None:
        """Test building execution levels for workflow with no dependencies."""
        runner = WorkflowRunner()
        
        # Create workflow where all tasks can run in parallel
        job1 = Job(name="job1", command=["echo", "1"], environment=JobEnvironment(conda="env1"))
        job2 = Job(name="job2", command=["echo", "2"], environment=JobEnvironment(conda="env2"))
        job3 = Job(name="job3", command=["echo", "3"], environment=JobEnvironment(conda="env3"))
        
        task1 = WorkflowTask(name="task1", job=job1)
        task2 = WorkflowTask(name="task2", job=job2)
        task3 = WorkflowTask(name="task3", job=job3)
        
        workflow = Workflow(name="all_parallel_workflow", tasks=[task1, task2, task3])
        
        # Test execution levels
        levels = runner._build_execution_levels(workflow)
        
        assert len(levels) == 1
        assert set(levels[0]) == {"task1", "task2", "task3"}

    def test_build_execution_levels_circular_dependency(self) -> None:
        """Test building execution levels with circular dependency detection."""
        runner = WorkflowRunner()
        
        # Create workflow with circular dependency: task1 -> task2 -> task1
        job1 = Job(name="job1", command=["echo", "1"], environment=JobEnvironment(conda="env1"))
        job2 = Job(name="job2", command=["echo", "2"], environment=JobEnvironment(conda="env2"))
        
        task1 = WorkflowTask(name="task1", job=job1, depends_on=["task2"])
        task2 = WorkflowTask(name="task2", job=job2, depends_on=["task1"])
        
        workflow = Workflow(name="circular_workflow", tasks=[task1, task2])
        
        # Test that circular dependency is detected
        with pytest.raises(ValueError, match="Circular dependency detected"):
            runner._build_execution_levels(workflow)

    @patch("srunx.workflows.runner.submit_and_monitor_job")
    def test_execute_workflow_parallel_execution(self, mock_monitor: Mock) -> None:
        """Test that tasks at the same level are executed in parallel."""
        runner = WorkflowRunner()
        
        # Create workflow with parallel branches
        job1 = Job(name="job1", command=["echo", "1"], environment=JobEnvironment(conda="env1"))
        job2 = Job(name="job2", command=["echo", "2"], environment=JobEnvironment(conda="env2"))
        job3 = Job(name="job3", command=["echo", "3"], environment=JobEnvironment(conda="env3"))
        job4 = Job(name="job4", command=["echo", "4"], environment=JobEnvironment(conda="env4"))
        
        task1 = WorkflowTask(name="task1", job=job1)
        task2 = WorkflowTask(name="task2", job=job2, depends_on=["task1"])
        task3 = WorkflowTask(name="task3", job=job3, depends_on=["task1"])
        task4 = WorkflowTask(name="task4", job=job4, depends_on=["task2", "task3"])
        
        workflow = Workflow(name="parallel_execution_workflow", tasks=[task1, task2, task3, task4])
        
        # Mock returns
        result_jobs = [
            Job(name="job1", command=["echo", "1"], environment=JobEnvironment(conda="env1"), job_id=111),
            Job(name="job2", command=["echo", "2"], environment=JobEnvironment(conda="env2"), job_id=222),
            Job(name="job3", command=["echo", "3"], environment=JobEnvironment(conda="env3"), job_id=333),
            Job(name="job4", command=["echo", "4"], environment=JobEnvironment(conda="env4"), job_id=444),
        ]
        mock_monitor.side_effect = result_jobs
        
        # Execute
        results = runner.execute_workflow(workflow)
        
        # Verify
        assert len(results) == 4
        assert mock_monitor.call_count == 4
        
        # Check that execution levels were built correctly
        levels = runner._build_execution_levels(workflow)
        assert len(levels) == 3
        assert levels[0] == ["task1"]
        assert set(levels[1]) == {"task2", "task3"}
        assert levels[2] == ["task4"]
