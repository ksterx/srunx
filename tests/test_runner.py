"""Tests for srunx.runner module."""

from unittest.mock import Mock, patch

import pytest
import yaml  # type: ignore

from srunx.exceptions import WorkflowValidationError
from srunx.models import (
    Job,
    JobDependency,
    JobEnvironment,
    JobStatus,
    ShellJob,
    Workflow,
)
from srunx.runner import WorkflowRunner, run_workflow_from_file


class TestWorkflowRunner:
    """Test WorkflowRunner class."""

    def test_workflow_runner_init(self):
        """Test WorkflowRunner initialization."""
        job = Job(
            name="test_job",
            command=["echo", "hello"],
            environment=JobEnvironment(conda="test_env"),
        )
        workflow = Workflow(name="test_workflow", jobs=[job])

        runner = WorkflowRunner(workflow)

        assert runner.workflow is workflow
        assert runner.slurm is not None

    def test_get_independent_jobs(self):
        """Test getting independent jobs."""
        job1 = Job(
            name="independent1",
            command=["echo", "1"],
            environment=JobEnvironment(conda="env"),
        )
        job2 = Job(
            name="dependent",
            command=["echo", "2"],
            environment=JobEnvironment(conda="env"),
            depends_on=["independent1"],
        )
        job3 = Job(
            name="independent2",
            command=["echo", "3"],
            environment=JobEnvironment(conda="env"),
        )

        workflow = Workflow(name="test", jobs=[job1, job2, job3])
        runner = WorkflowRunner(workflow)

        independent = runner.get_independent_jobs()

        assert len(independent) == 2
        assert job1 in independent
        assert job3 in independent
        assert job2 not in independent

    def test_get_independent_jobs_empty(self):
        """Test getting independent jobs when all have dependencies."""
        job1 = Job(
            name="job1",
            command=["echo", "1"],
            environment=JobEnvironment(conda="env"),
            depends_on=["job2"],
        )
        job2 = Job(
            name="job2",
            command=["echo", "2"],
            environment=JobEnvironment(conda="env"),
            depends_on=["job1"],  # Circular dependency
        )

        workflow = Workflow(name="test", jobs=[job1, job2])
        runner = WorkflowRunner(workflow)

        independent = runner.get_independent_jobs()

        assert len(independent) == 0

    def test_from_yaml_simple(self, temp_dir):
        """Test loading workflow from simple YAML."""
        yaml_content = {
            "name": "test_workflow",
            "jobs": [
                {
                    "name": "job1",
                    "command": ["echo", "hello"],
                    "environment": {"conda": "test_env"},
                }
            ],
        }

        yaml_path = temp_dir / "workflow.yaml"
        with open(yaml_path, "w") as f:
            yaml.dump(yaml_content, f)

        runner = WorkflowRunner.from_yaml(yaml_path)

        assert runner.workflow.name == "test_workflow"
        assert len(runner.workflow.jobs) == 1
        assert runner.workflow.jobs[0].name == "job1"
        assert runner.workflow.jobs[0].command == ["echo", "hello"]

    def test_from_yaml_complex(self, temp_dir):
        """Test loading workflow from complex YAML."""
        yaml_content = {
            "name": "complex_workflow",
            "jobs": [
                {
                    "name": "preprocess",
                    "command": ["python", "preprocess.py"],
                    "environment": {"conda": "ml_env"},
                    "resources": {
                        "nodes": 1,
                        "cpus_per_task": 4,
                        "memory_per_node": "16GB",
                    },
                },
                {
                    "name": "train",
                    "command": ["python", "train.py"],
                    "depends_on": ["preprocess"],
                    "environment": {"conda": "ml_env"},
                    "resources": {
                        "nodes": 1,
                        "gpus_per_node": 1,
                        "time_limit": "2:00:00",
                    },
                },
            ],
        }

        yaml_path = temp_dir / "complex.yaml"
        with open(yaml_path, "w") as f:
            yaml.dump(yaml_content, f)

        runner = WorkflowRunner.from_yaml(yaml_path)

        assert runner.workflow.name == "complex_workflow"
        assert len(runner.workflow.jobs) == 2

        preprocess_job = next(j for j in runner.workflow.jobs if j.name == "preprocess")
        train_job = next(j for j in runner.workflow.jobs if j.name == "train")

        assert preprocess_job.resources.cpus_per_task == 4
        assert preprocess_job.resources.memory_per_node == "16GB"
        assert train_job.depends_on == ["preprocess"]
        assert train_job.resources.gpus_per_node == 1

    def test_from_yaml_shell_job(self, temp_dir):
        """Test loading workflow with shell job."""
        yaml_content = {
            "name": "shell_workflow",
            "jobs": [{"name": "shell_job", "path": "/path/to/script.sh"}],
        }

        yaml_path = temp_dir / "shell.yaml"
        with open(yaml_path, "w") as f:
            yaml.dump(yaml_content, f)

        runner = WorkflowRunner.from_yaml(yaml_path)

        assert len(runner.workflow.jobs) == 1
        job = runner.workflow.jobs[0]
        assert isinstance(job, ShellJob)
        assert job.script_path == "/path/to/script.sh"

    def test_from_yaml_nonexistent_file(self):
        """Test loading workflow from nonexistent file."""
        with pytest.raises(FileNotFoundError):
            WorkflowRunner.from_yaml("/nonexistent/file.yaml")

    def test_from_yaml_malformed_yaml(self, temp_dir):
        """Test loading workflow from malformed YAML."""
        yaml_path = temp_dir / "malformed.yaml"
        with open(yaml_path, "w") as f:
            f.write("invalid: yaml: content: [")

        with pytest.raises(yaml.YAMLError):
            WorkflowRunner.from_yaml(yaml_path)

    def test_from_yaml_missing_name(self, temp_dir):
        """Test loading workflow without name (uses default)."""
        yaml_content = {
            "jobs": [
                {
                    "name": "job1",
                    "command": ["echo", "test"],
                    "environment": {"conda": "env"},
                }
            ]
        }

        yaml_path = temp_dir / "no_name.yaml"
        with open(yaml_path, "w") as f:
            yaml.dump(yaml_content, f)

        runner = WorkflowRunner.from_yaml(yaml_path)

        assert runner.workflow.name == "unnamed"

    def test_from_yaml_with_args(self, temp_dir):
        """Test loading workflow with args and template rendering."""
        yaml_content = {
            "name": "args_workflow",
            "args": {
                "dataset_name": "test-dataset",
                "model_path": "/models/bert",
                "batch_size": 16,
                "output_dir": "/outputs/experiment",
            },
            "jobs": [
                {
                    "name": "preprocess",
                    "command": [
                        "python",
                        "preprocess.py",
                        "--dataset",
                        "{{ dataset_name }}",
                        "--output",
                        "{{ output_dir }}/preprocessed",
                    ],
                    "work_dir": "{{ output_dir }}",
                    "environment": {"conda": "ml_env"},
                },
                {
                    "name": "train",
                    "command": [
                        "python",
                        "train.py",
                        "--model",
                        "{{ model_path }}",
                        "--data",
                        "{{ output_dir }}/preprocessed",
                        "--batch-size",
                        "{{ batch_size }}",
                    ],
                    "depends_on": ["preprocess"],
                    "work_dir": "{{ output_dir }}",
                    "environment": {"conda": "ml_env"},
                },
            ],
        }

        yaml_path = temp_dir / "args_workflow.yaml"
        with open(yaml_path, "w") as f:
            yaml.dump(yaml_content, f)

        runner = WorkflowRunner.from_yaml(yaml_path)

        assert runner.workflow.name == "args_workflow"
        assert runner.args == {
            "dataset_name": "test-dataset",
            "model_path": "/models/bert",
            "batch_size": 16,
            "output_dir": "/outputs/experiment",
        }
        assert len(runner.workflow.jobs) == 2

        # Check that Jinja templates were rendered correctly
        preprocess_job = next(j for j in runner.workflow.jobs if j.name == "preprocess")
        train_job = next(j for j in runner.workflow.jobs if j.name == "train")

        # Check preprocess job
        assert preprocess_job.command == [
            "python",
            "preprocess.py",
            "--dataset",
            "test-dataset",
            "--output",
            "/outputs/experiment/preprocessed",
        ]
        assert preprocess_job.work_dir == "/outputs/experiment"

        # Check train job
        assert train_job.command == [
            "python",
            "train.py",
            "--model",
            "/models/bert",
            "--data",
            "/outputs/experiment/preprocessed",
            "--batch-size",
            "16",
        ]
        assert train_job.work_dir == "/outputs/experiment"
        assert train_job.depends_on == ["preprocess"]

    def test_from_yaml_no_args(self, temp_dir):
        """Test loading workflow without args section."""
        yaml_content = {
            "name": "no_args_workflow",
            "jobs": [
                {
                    "name": "simple_job",
                    "command": ["echo", "hello"],
                    "environment": {"conda": "test_env"},
                }
            ],
        }

        yaml_path = temp_dir / "no_args.yaml"
        with open(yaml_path, "w") as f:
            yaml.dump(yaml_content, f)

        runner = WorkflowRunner.from_yaml(yaml_path)

        assert runner.workflow.name == "no_args_workflow"
        assert runner.args == {}
        assert len(runner.workflow.jobs) == 1

        job = runner.workflow.jobs[0]
        assert job.command == ["echo", "hello"]

    def test_from_yaml_undefined_var_raises(self, temp_dir):
        """Unresolved Jinja refs fail at load time (StrictUndefined)."""
        yaml_content = {
            "name": "invalid_template_workflow",
            "args": {"valid_var": "value"},
            "jobs": [
                {
                    "name": "invalid_job",
                    "command": ["echo", "{{ undefined_var }}"],
                    "environment": {"conda": "test_env"},
                }
            ],
        }

        yaml_path = temp_dir / "invalid_template.yaml"
        with open(yaml_path, "w") as f:
            yaml.dump(yaml_content, f)

        with pytest.raises(WorkflowValidationError):
            WorkflowRunner.from_yaml(yaml_path)

    def test_render_jobs_with_args_and_deps_static_method(self):
        """_render_jobs_with_args_and_deps resolves args in per-job render."""
        args = {
            "dataset": "mnist",
            "epochs": 10,
            "output": "/results",
        }

        jobs_data = [
            {
                "name": "train",
                "command": [
                    "python",
                    "train.py",
                    "--dataset",
                    "{{ dataset }}",
                    "--epochs",
                    "{{ epochs }}",
                ],
                "work_dir": "{{ output }}",
            }
        ]

        rendered_jobs = WorkflowRunner._render_jobs_with_args_and_deps(jobs_data, args)

        assert len(rendered_jobs) == 1
        job = rendered_jobs[0]
        assert job["command"] == [
            "python",
            "train.py",
            "--dataset",
            "mnist",
            "--epochs",
            "10",
        ]
        assert job["work_dir"] == "/results"

    def test_render_jobs_with_args_and_deps_no_args(self):
        """Empty args still round-trips jobs through the render pipeline."""
        jobs_data = [
            {
                "name": "simple_job",
                "command": ["echo", "hello"],
            }
        ]

        rendered_jobs = WorkflowRunner._render_jobs_with_args_and_deps(jobs_data, {})

        assert len(rendered_jobs) == 1
        assert rendered_jobs[0]["name"] == "simple_job"
        assert rendered_jobs[0]["command"] == ["echo", "hello"]

    @patch("srunx.runtime.workflow.runner.Slurm")
    def test_run_simple_workflow(self, mock_slurm_class):
        """Test running simple workflow."""
        mock_slurm = Mock()
        mock_slurm_class.return_value = mock_slurm

        # Create a job that will be "completed"
        job = Job(
            name="test_job",
            command=["echo", "test"],
            environment=JobEnvironment(conda="env"),
        )
        job.status = JobStatus.COMPLETED

        # Mock the slurm.run method to return the completed job
        mock_slurm.run.return_value = job

        workflow = Workflow(name="test", jobs=[job])
        runner = WorkflowRunner(workflow)

        results = runner.run()

        assert len(results) == 1
        assert "test_job" in results
        assert results["test_job"] is job
        # ``workflow_run_id`` is passed through from the state DB write
        # P2-4 #A added — we don't assert its exact value (sqlite
        # autoincrement) but it must be an int so ``compute_workflow_stats``
        # can JOIN on it.
        mock_slurm.run.assert_called_once()
        call_kwargs = mock_slurm.run.call_args.kwargs
        assert call_kwargs["workflow_name"] == "test"
        assert isinstance(call_kwargs["workflow_run_id"], int)

    @patch("srunx.runtime.workflow.runner.Slurm")
    def test_run_workflow_with_dependencies(self, mock_slurm_class):
        """Test running workflow with dependencies."""
        mock_slurm = Mock()
        mock_slurm_class.return_value = mock_slurm

        job1 = Job(
            name="job1", command=["echo", "1"], environment=JobEnvironment(conda="env")
        )
        job2 = Job(
            name="job2",
            command=["echo", "2"],
            environment=JobEnvironment(conda="env"),
            depends_on=["job1"],
        )

        # Ensure jobs start in PENDING status for dependency tests
        job1._status = JobStatus.PENDING
        job2._status = JobStatus.PENDING

        # Set up mock to return completed jobs
        def mock_run(job, **kwargs):
            job.status = JobStatus.COMPLETED
            return job

        mock_slurm.run.side_effect = mock_run

        workflow = Workflow(name="test", jobs=[job1, job2])
        runner = WorkflowRunner(workflow)

        results = runner.run()

        assert len(results) == 2
        assert "job1" in results
        assert "job2" in results
        assert mock_slurm.run.call_count == 2

    @patch("srunx.runtime.workflow.runner.Slurm")
    def test_run_workflow_job_failure(self, mock_slurm_class):
        """Test running workflow with job failure."""
        mock_slurm = Mock()
        mock_slurm_class.return_value = mock_slurm

        job = Job(
            name="failing_job",
            command=["false"],
            environment=JobEnvironment(conda="env"),
        )

        # Mock slurm.run to raise an exception
        mock_slurm.run.side_effect = RuntimeError("Job failed")

        workflow = Workflow(name="test", jobs=[job])
        runner = WorkflowRunner(workflow)

        with pytest.raises(RuntimeError):
            runner.run()

    def test_parse_job_simple(self):
        """Test parsing simple job from dict."""
        job_data = {
            "name": "test_job",
            "command": ["python", "script.py"],
            "environment": {"conda": "env"},
        }

        job = WorkflowRunner.parse_job(job_data)

        assert isinstance(job, Job)
        assert job.name == "test_job"
        assert job.command == ["python", "script.py"]
        assert job.environment.conda == "env"

    def test_parse_job_with_resources(self):
        """Test parsing job with resources."""
        job_data = {
            "name": "gpu_job",
            "command": ["python", "train.py"],
            "environment": {"conda": "ml_env"},
            "resources": {"nodes": 2, "gpus_per_node": 1, "memory_per_node": "32GB"},
        }

        job = WorkflowRunner.parse_job(job_data)

        assert job.resources.nodes == 2
        assert job.resources.gpus_per_node == 1
        assert job.resources.memory_per_node == "32GB"

    def test_parse_job_with_dependencies(self):
        """Test parsing job with dependencies."""
        job_data = {
            "name": "dependent_job",
            "command": ["python", "process.py"],
            "environment": {"conda": "env"},
            "depends_on": ["job1", "job2"],
        }

        job = WorkflowRunner.parse_job(job_data)

        assert job.depends_on == ["job1", "job2"]

    def test_parse_shell_job(self):
        """Test parsing shell job."""
        job_data = {"name": "shell_job", "path": "/path/to/script.sh"}

        job = WorkflowRunner.parse_job(job_data)

        assert isinstance(job, ShellJob)
        assert job.name == "shell_job"
        assert job.script_path == "/path/to/script.sh"

    def test_parse_job_both_path_and_command(self):
        """Test parsing job with both path and command (should fail)."""
        job_data = {
            "name": "invalid_job",
            "command": ["echo", "test"],
            "path": "/path/to/script.sh",
        }

        with pytest.raises(WorkflowValidationError):
            WorkflowRunner.parse_job(job_data)

    def test_parse_job_with_directories(self):
        """Test parsing job with custom directories."""
        job_data = {
            "name": "dir_job",
            "command": ["python", "script.py"],
            "environment": {"conda": "env"},
            "log_dir": "/custom/logs",
            "work_dir": "/custom/work",
        }

        job = WorkflowRunner.parse_job(job_data)

        assert job.log_dir == "/custom/logs"
        assert job.work_dir == "/custom/work"

    @patch("srunx.runtime.workflow.runner.WorkflowRunner.from_yaml")
    @patch("srunx.runtime.workflow.runner.WorkflowRunner.run")
    def test_execute_from_yaml(self, mock_run, mock_from_yaml):
        """Test execute_from_yaml method."""
        mock_runner = Mock()
        mock_from_yaml.return_value = mock_runner
        mock_results = {"job1": Mock()}
        mock_runner.run.return_value = mock_results

        runner = WorkflowRunner(Workflow(name="test", jobs=[]))
        results = runner.execute_from_yaml("test.yaml")

        mock_from_yaml.assert_called_once_with("test.yaml")
        mock_runner.run.assert_called_once()
        assert results == mock_results


class TestRunWorkflowFromFile:
    """Test run_workflow_from_file convenience function."""

    @patch("srunx.runtime.workflow.runner.WorkflowRunner")
    def test_run_workflow_from_file(self, mock_runner_class):
        """Test run_workflow_from_file convenience function."""
        mock_runner = Mock()
        mock_runner_class.from_yaml.return_value = mock_runner
        mock_results = {"job1": Mock()}
        mock_runner.run.return_value = mock_results

        results = run_workflow_from_file("test.yaml")

        mock_runner_class.from_yaml.assert_called_once_with(
            "test.yaml", single_job=None
        )
        mock_runner.run.assert_called_once_with(single_job=None)
        assert results == mock_results


class TestWorkflowExecutionControl:
    """Test workflow execution control features (--from, --to, --job)."""

    def test_get_jobs_to_execute_full_workflow(self):
        """Test getting all jobs for full workflow execution."""
        job1 = Job(
            name="job1", command=["echo", "1"], environment=JobEnvironment(conda="env")
        )
        job2 = Job(
            name="job2", command=["echo", "2"], environment=JobEnvironment(conda="env")
        )
        job3 = Job(
            name="job3", command=["echo", "3"], environment=JobEnvironment(conda="env")
        )

        workflow = Workflow(name="test", jobs=[job1, job2, job3])
        runner = WorkflowRunner(workflow)

        jobs_to_execute = runner._get_jobs_to_execute()

        assert len(jobs_to_execute) == 3
        assert all(job in jobs_to_execute for job in [job1, job2, job3])

    def test_get_jobs_to_execute_single_job(self):
        """Test executing single job."""
        job1 = Job(
            name="job1", command=["echo", "1"], environment=JobEnvironment(conda="env")
        )
        job2 = Job(
            name="job2", command=["echo", "2"], environment=JobEnvironment(conda="env")
        )
        job3 = Job(
            name="job3", command=["echo", "3"], environment=JobEnvironment(conda="env")
        )

        workflow = Workflow(name="test", jobs=[job1, job2, job3])
        runner = WorkflowRunner(workflow)

        jobs_to_execute = runner._get_jobs_to_execute(single_job="job2")

        assert len(jobs_to_execute) == 1
        assert jobs_to_execute[0].name == "job2"

    def test_get_jobs_to_execute_from_job(self):
        """Test executing from specific job to end."""
        job1 = Job(
            name="job1", command=["echo", "1"], environment=JobEnvironment(conda="env")
        )
        job2 = Job(
            name="job2", command=["echo", "2"], environment=JobEnvironment(conda="env")
        )
        job3 = Job(
            name="job3", command=["echo", "3"], environment=JobEnvironment(conda="env")
        )

        workflow = Workflow(name="test", jobs=[job1, job2, job3])
        runner = WorkflowRunner(workflow)

        jobs_to_execute = runner._get_jobs_to_execute(from_job="job2")

        assert len(jobs_to_execute) == 2
        job_names = [job.name for job in jobs_to_execute]
        assert job_names == ["job2", "job3"]

    def test_get_jobs_to_execute_to_job(self):
        """Test executing from beginning to specific job."""
        job1 = Job(
            name="job1", command=["echo", "1"], environment=JobEnvironment(conda="env")
        )
        job2 = Job(
            name="job2", command=["echo", "2"], environment=JobEnvironment(conda="env")
        )
        job3 = Job(
            name="job3", command=["echo", "3"], environment=JobEnvironment(conda="env")
        )

        workflow = Workflow(name="test", jobs=[job1, job2, job3])
        runner = WorkflowRunner(workflow)

        jobs_to_execute = runner._get_jobs_to_execute(to_job="job2")

        assert len(jobs_to_execute) == 2
        job_names = [job.name for job in jobs_to_execute]
        assert job_names == ["job1", "job2"]

    def test_get_jobs_to_execute_from_to_job(self):
        """Test executing from one job to another."""
        job1 = Job(
            name="job1", command=["echo", "1"], environment=JobEnvironment(conda="env")
        )
        job2 = Job(
            name="job2", command=["echo", "2"], environment=JobEnvironment(conda="env")
        )
        job3 = Job(
            name="job3", command=["echo", "3"], environment=JobEnvironment(conda="env")
        )
        job4 = Job(
            name="job4", command=["echo", "4"], environment=JobEnvironment(conda="env")
        )

        workflow = Workflow(name="test", jobs=[job1, job2, job3, job4])
        runner = WorkflowRunner(workflow)

        jobs_to_execute = runner._get_jobs_to_execute(from_job="job2", to_job="job3")

        assert len(jobs_to_execute) == 2
        job_names = [job.name for job in jobs_to_execute]
        assert job_names == ["job2", "job3"]

    def test_get_jobs_to_execute_reverse_range(self):
        """Test executing from later job to earlier job."""
        job1 = Job(
            name="job1", command=["echo", "1"], environment=JobEnvironment(conda="env")
        )
        job2 = Job(
            name="job2", command=["echo", "2"], environment=JobEnvironment(conda="env")
        )
        job3 = Job(
            name="job3", command=["echo", "3"], environment=JobEnvironment(conda="env")
        )
        job4 = Job(
            name="job4", command=["echo", "4"], environment=JobEnvironment(conda="env")
        )

        workflow = Workflow(name="test", jobs=[job1, job2, job3, job4])
        runner = WorkflowRunner(workflow)

        jobs_to_execute = runner._get_jobs_to_execute(from_job="job3", to_job="job2")

        assert len(jobs_to_execute) == 2
        job_names = [job.name for job in jobs_to_execute]
        assert job_names == ["job2", "job3"]

    def test_get_jobs_to_execute_nonexistent_job(self):
        """Test error when specifying nonexistent job."""
        job1 = Job(
            name="job1", command=["echo", "1"], environment=JobEnvironment(conda="env")
        )

        workflow = Workflow(name="test", jobs=[job1])
        runner = WorkflowRunner(workflow)

        with pytest.raises(
            WorkflowValidationError, match="Job 'nonexistent' not found in workflow"
        ):
            runner._get_jobs_to_execute(single_job="nonexistent")

        with pytest.raises(
            WorkflowValidationError, match="Job 'nonexistent' not found in workflow"
        ):
            runner._get_jobs_to_execute(from_job="nonexistent")

        with pytest.raises(
            WorkflowValidationError, match="Job 'nonexistent' not found in workflow"
        ):
            runner._get_jobs_to_execute(to_job="nonexistent")

    @patch("srunx.runtime.workflow.runner.Slurm")
    def test_run_single_job_ignores_dependencies(self, mock_slurm_class):
        """Test that single job execution ignores dependencies."""
        mock_slurm = Mock()
        mock_slurm_class.return_value = mock_slurm

        job1 = Job(
            name="job1", command=["echo", "1"], environment=JobEnvironment(conda="env")
        )
        job2 = Job(
            name="job2",
            command=["echo", "2"],
            environment=JobEnvironment(conda="env"),
            depends_on=["job1"],
        )

        # Set up mock to return completed jobs
        def mock_run(job, **kwargs):
            job.status = JobStatus.COMPLETED
            return job

        mock_slurm.run.side_effect = mock_run

        workflow = Workflow(name="test", jobs=[job1, job2])
        runner = WorkflowRunner(workflow)

        # Execute only job2, ignoring its dependency on job1
        results = runner.run(single_job="job2")

        assert len(results) == 1
        assert "job2" in results
        assert "job1" not in results
        mock_slurm.run.assert_called_once()

    @patch("srunx.runtime.workflow.runner.Slurm")
    def test_run_from_job_ignores_external_dependencies(self, mock_slurm_class):
        """Test that --from execution ignores dependencies outside the execution range."""
        mock_slurm = Mock()
        mock_slurm_class.return_value = mock_slurm

        job1 = Job(
            name="job1", command=["echo", "1"], environment=JobEnvironment(conda="env")
        )
        job2 = Job(
            name="job2",
            command=["echo", "2"],
            environment=JobEnvironment(conda="env"),
            depends_on=["job1"],
        )
        job3 = Job(
            name="job3",
            command=["echo", "3"],
            environment=JobEnvironment(conda="env"),
            depends_on=["job2"],
        )

        # Set job status to PENDING initially
        for job in [job1, job2, job3]:
            job._status = JobStatus.PENDING

        # Set up mock to return completed jobs
        def mock_run(job, **kwargs):
            job.status = JobStatus.COMPLETED
            return job

        mock_slurm.run.side_effect = mock_run

        workflow = Workflow(name="test", jobs=[job1, job2, job3])
        runner = WorkflowRunner(workflow)

        # Execute from job2 onwards, ignoring job2's dependency on job1
        results = runner.run(from_job="job2")

        assert len(results) == 2
        assert "job2" in results
        assert "job3" in results
        assert "job1" not in results
        # Should run both job2 and job3 (job3 should run after job2 completes)
        assert mock_slurm.run.call_count == 2

    def test_from_yaml_with_execution_options(self, temp_dir):
        """Test loading workflow and using execution control options."""
        yaml_content = {
            "name": "test_workflow",
            "jobs": [
                {
                    "name": "job1",
                    "command": ["echo", "1"],
                    "environment": {"conda": "env"},
                },
                {
                    "name": "job2",
                    "command": ["echo", "2"],
                    "environment": {"conda": "env"},
                    "depends_on": ["job1"],
                },
                {
                    "name": "job3",
                    "command": ["echo", "3"],
                    "environment": {"conda": "env"},
                    "depends_on": ["job2"],
                },
            ],
        }

        yaml_path = temp_dir / "workflow.yaml"
        with open(yaml_path, "w") as f:
            yaml.dump(yaml_content, f)

        runner = WorkflowRunner.from_yaml(yaml_path)

        # Test single job
        jobs_to_execute = runner._get_jobs_to_execute(single_job="job2")
        assert len(jobs_to_execute) == 1
        assert jobs_to_execute[0].name == "job2"

        # Test from job
        jobs_to_execute = runner._get_jobs_to_execute(from_job="job2")
        assert len(jobs_to_execute) == 2
        job_names = [job.name for job in jobs_to_execute]
        assert job_names == ["job2", "job3"]

        # Test to job
        jobs_to_execute = runner._get_jobs_to_execute(to_job="job2")
        assert len(jobs_to_execute) == 2
        job_names = [job.name for job in jobs_to_execute]
        assert job_names == ["job1", "job2"]

    def test_parse_job_with_retry(self):
        """Test parsing job with retry configuration."""
        job_data = {
            "name": "retry_job",
            "command": ["python", "might_fail.py"],
            "environment": {"conda": "env"},
            "retry": 3,
            "retry_delay": 120,
        }
        job = WorkflowRunner.parse_job(job_data)
        assert isinstance(job, Job)
        assert job.name == "retry_job"
        assert job.retry == 3
        assert job.retry_delay == 120
        assert job.retry_count == 0

    def test_parse_job_without_retry(self):
        """Test parsing job without retry configuration uses defaults."""
        job_data = {
            "name": "no_retry_job",
            "command": ["python", "script.py"],
            "environment": {"conda": "env"},
        }
        job = WorkflowRunner.parse_job(job_data)
        assert isinstance(job, Job)
        assert job.name == "no_retry_job"
        assert job.retry == 0  # Default value
        assert job.retry_delay == 60  # Default value
        assert job.retry_count == 0

    def test_parse_shell_job_with_retry(self):
        """Test parsing shell job with retry configuration."""
        job_data = {
            "name": "retry_shell_job",
            "script_path": "/path/to/script.sh",
            "retry": 2,
            "retry_delay": 30,
        }
        job = WorkflowRunner.parse_job(job_data)
        assert isinstance(job, ShellJob)
        assert job.name == "retry_shell_job"
        assert job.retry == 2
        assert job.retry_delay == 30
        assert job.retry_count == 0

    def test_parse_job_with_apptainer_container(self):
        """Test parsing job with environment.container.runtime: apptainer (T6.8, AC-6)."""
        job_data = {
            "name": "apptainer_job",
            "command": ["python", "train.py"],
            "environment": {
                "container": {
                    "runtime": "apptainer",
                    "image": "test.sif",
                    "nv": True,
                    "mounts": ["/data:/data"],
                },
            },
        }
        job = WorkflowRunner.parse_job(job_data)
        assert isinstance(job, Job)
        assert job.name == "apptainer_job"
        assert job.environment.container is not None
        assert job.environment.container.runtime == "apptainer"
        assert job.environment.container.image == "test.sif"
        assert job.environment.container.nv is True
        assert job.environment.container.mounts == ["/data:/data"]

    def test_parse_job_with_singularity_container(self):
        """Test parsing job with singularity runtime."""
        job_data = {
            "name": "singularity_job",
            "command": ["python", "train.py"],
            "environment": {
                "container": {
                    "runtime": "singularity",
                    "image": "test.sif",
                },
            },
        }
        job = WorkflowRunner.parse_job(job_data)
        assert isinstance(job, Job)
        assert job.environment.container is not None
        assert job.environment.container.runtime == "singularity"

    def test_parse_job_with_pyxis_container(self):
        """Test parsing job with pyxis runtime (default)."""
        job_data = {
            "name": "pyxis_job",
            "command": ["python", "train.py"],
            "environment": {
                "container": {
                    "image": "pytorch/pytorch:latest",
                    "mounts": ["/data:/workspace/data"],
                },
            },
        }
        job = WorkflowRunner.parse_job(job_data)
        assert isinstance(job, Job)
        assert job.environment.container is not None
        assert job.environment.container.runtime == "pyxis"

    def test_parse_job_with_conda_and_container(self):
        """Test parsing job with both conda and container."""
        job_data = {
            "name": "hybrid_job",
            "command": ["python", "train.py"],
            "environment": {
                "conda": "ml_env",
                "container": {
                    "runtime": "apptainer",
                    "image": "test.sif",
                },
            },
        }
        job = WorkflowRunner.parse_job(job_data)
        assert isinstance(job, Job)
        assert job.environment.conda == "ml_env"
        assert job.environment.container is not None
        assert job.environment.container.runtime == "apptainer"


class TestJobDependency:
    """Test JobDependency class."""

    def test_parse_simple_dependency(self):
        """Test parsing simple dependency (default afterok)."""
        dep = JobDependency.parse("job_a")
        assert dep.job_name == "job_a"
        assert dep.dep_type == "afterok"

    def test_parse_after_dependency(self):
        """Test parsing 'after' dependency."""
        dep = JobDependency.parse("after:job_a")
        assert dep.job_name == "job_a"
        assert dep.dep_type == "after"

    def test_parse_afterany_dependency(self):
        """Test parsing 'afterany' dependency."""
        dep = JobDependency.parse("afterany:job_a")
        assert dep.job_name == "job_a"
        assert dep.dep_type == "afterany"

    def test_parse_afternotok_dependency(self):
        """Test parsing 'afternotok' dependency."""
        dep = JobDependency.parse("afternotok:job_a")
        assert dep.job_name == "job_a"
        assert dep.dep_type == "afternotok"

    def test_parse_explicit_afterok_dependency(self):
        """Test parsing explicit 'afterok' dependency."""
        dep = JobDependency.parse("afterok:job_a")
        assert dep.job_name == "job_a"
        assert dep.dep_type == "afterok"

    def test_parse_invalid_dependency_type(self):
        """Test parsing invalid dependency type."""
        with pytest.raises(WorkflowValidationError, match="Invalid dependency type"):
            JobDependency.parse("invalid:job_a")

    def test_str_representation(self):
        """Test string representation of dependencies."""
        # Default afterok should show just job name
        dep1 = JobDependency(job_name="job_a", dep_type="afterok")
        assert str(dep1) == "job_a"

        # Other types should show full format
        dep2 = JobDependency(job_name="job_a", dep_type="after")
        assert str(dep2) == "after:job_a"

        dep3 = JobDependency(job_name="job_a", dep_type="afterany")
        assert str(dep3) == "afterany:job_a"

        dep4 = JobDependency(job_name="job_a", dep_type="afternotok")
        assert str(dep4) == "afternotok:job_a"


class TestEnhancedDependencies:
    """Test enhanced dependency functionality."""

    def test_job_parses_dependencies_on_init(self):
        """Test that jobs parse dependencies on initialization."""
        job = Job(
            name="test_job",
            command=["echo", "test"],
            depends_on=["job_a", "after:job_b", "afterany:job_c", "afternotok:job_d"],
            environment=JobEnvironment(conda="env"),
        )

        assert len(job.parsed_dependencies) == 4

        dep1 = job.parsed_dependencies[0]
        assert dep1.job_name == "job_a"
        assert dep1.dep_type == "afterok"

        dep2 = job.parsed_dependencies[1]
        assert dep2.job_name == "job_b"
        assert dep2.dep_type == "after"

        dep3 = job.parsed_dependencies[2]
        assert dep3.job_name == "job_c"
        assert dep3.dep_type == "afterany"

        dep4 = job.parsed_dependencies[3]
        assert dep4.job_name == "job_d"
        assert dep4.dep_type == "afternotok"

    def test_dependencies_satisfied_afterok(self):
        """Test dependencies_satisfied with afterok dependency."""
        job = Job(
            name="dependent",
            command=["echo", "test"],
            depends_on=["job_a"],
            environment=JobEnvironment(conda="env"),
        )

        # Should not be satisfied if dependency is not completed
        job_statuses = {"job_a": JobStatus.RUNNING}
        assert not job.dependencies_satisfied(job_statuses)

        # Should be satisfied if dependency is completed
        job_statuses = {"job_a": JobStatus.COMPLETED}
        assert job.dependencies_satisfied(job_statuses)

        # Should not be satisfied if dependency failed
        job_statuses = {"job_a": JobStatus.FAILED}
        assert not job.dependencies_satisfied(job_statuses)

    def test_dependencies_satisfied_after(self):
        """Test dependencies_satisfied with after dependency."""
        job = Job(
            name="dependent",
            command=["echo", "test"],
            depends_on=["after:job_a"],
            environment=JobEnvironment(conda="env"),
        )

        # Should not be satisfied if dependency is pending
        job_statuses = {"job_a": JobStatus.PENDING}
        assert not job.dependencies_satisfied(job_statuses)

        # Should be satisfied if dependency is running
        job_statuses = {"job_a": JobStatus.RUNNING}
        assert job.dependencies_satisfied(job_statuses)

        # Should be satisfied if dependency is completed
        job_statuses = {"job_a": JobStatus.COMPLETED}
        assert job.dependencies_satisfied(job_statuses)

        # Should be satisfied if dependency failed
        job_statuses = {"job_a": JobStatus.FAILED}
        assert job.dependencies_satisfied(job_statuses)

    def test_dependencies_satisfied_afterany(self):
        """Test dependencies_satisfied with afterany dependency."""
        job = Job(
            name="dependent",
            command=["echo", "test"],
            depends_on=["afterany:job_a"],
            environment=JobEnvironment(conda="env"),
        )

        # Should not be satisfied if dependency is pending
        job_statuses = {"job_a": JobStatus.PENDING}
        assert not job.dependencies_satisfied(job_statuses)

        # Should not be satisfied if dependency is running
        job_statuses = {"job_a": JobStatus.RUNNING}
        assert not job.dependencies_satisfied(job_statuses)

        # Should be satisfied if dependency completed successfully
        job_statuses = {"job_a": JobStatus.COMPLETED}
        assert job.dependencies_satisfied(job_statuses)

        # Should be satisfied if dependency failed
        job_statuses = {"job_a": JobStatus.FAILED}
        assert job.dependencies_satisfied(job_statuses)

        # Should be satisfied if dependency was cancelled
        job_statuses = {"job_a": JobStatus.CANCELLED}
        assert job.dependencies_satisfied(job_statuses)

    def test_dependencies_satisfied_afternotok(self):
        """Test dependencies_satisfied with afternotok dependency."""
        job = Job(
            name="dependent",
            command=["echo", "test"],
            depends_on=["afternotok:job_a"],
            environment=JobEnvironment(conda="env"),
        )

        # Should not be satisfied if dependency is pending
        job_statuses = {"job_a": JobStatus.PENDING}
        assert not job.dependencies_satisfied(job_statuses)

        # Should not be satisfied if dependency is running
        job_statuses = {"job_a": JobStatus.RUNNING}
        assert not job.dependencies_satisfied(job_statuses)

        # Should not be satisfied if dependency completed successfully
        job_statuses = {"job_a": JobStatus.COMPLETED}
        assert not job.dependencies_satisfied(job_statuses)

        # Should be satisfied if dependency failed
        job_statuses = {"job_a": JobStatus.FAILED}
        assert job.dependencies_satisfied(job_statuses)

        # Should be satisfied if dependency was cancelled
        job_statuses = {"job_a": JobStatus.CANCELLED}
        assert job.dependencies_satisfied(job_statuses)

    def test_mixed_dependencies(self):
        """Test job with multiple different dependency types."""
        job = Job(
            name="dependent",
            command=["echo", "test"],
            depends_on=["job_a", "after:job_b", "afterany:job_c"],
            environment=JobEnvironment(conda="env"),
        )

        # All dependencies must be satisfied
        job_statuses = {
            "job_a": JobStatus.COMPLETED,  # afterok: needs COMPLETED
            "job_b": JobStatus.RUNNING,  # after: needs not PENDING
            "job_c": JobStatus.FAILED,  # afterany: needs terminal status
        }
        assert job.dependencies_satisfied(job_statuses)

        # If any dependency is not satisfied, should return False
        job_statuses = {
            "job_a": JobStatus.RUNNING,  # afterok: needs COMPLETED (not satisfied)
            "job_b": JobStatus.RUNNING,  # after: needs not PENDING (satisfied)
            "job_c": JobStatus.FAILED,  # afterany: needs terminal status (satisfied)
        }
        assert not job.dependencies_satisfied(job_statuses)

    def test_backward_compatibility(self):
        """Test backward compatibility with old interface."""
        job = Job(
            name="dependent",
            command=["echo", "test"],
            depends_on=["job_a", "job_b"],
            environment=JobEnvironment(conda="env"),
        )

        # Old interface should still work
        completed_jobs = ["job_a", "job_b"]
        assert job.dependencies_satisfied({}, completed_job_names=completed_jobs)

        completed_jobs = ["job_a"]  # job_b not completed
        assert not job.dependencies_satisfied({}, completed_job_names=completed_jobs)


class TestWorkflowExports:
    """Tests for workflow exports feature (load-time deps resolution)."""

    @pytest.fixture
    def temp_dir(self, tmp_path):
        return tmp_path

    def test_from_yaml_with_exports(self, temp_dir):
        """Test loading workflow YAML with exports declared on jobs."""
        yaml_content = {
            "name": "exports_workflow",
            "args": {"base_dir": "/data/experiments"},
            "jobs": [
                {
                    "name": "train",
                    "command": ["python", "train.py"],
                    "exports": {
                        "model_path": "{{ base_dir }}/models/best.pt",
                        "metrics_dir": "{{ base_dir }}/metrics",
                    },
                    "environment": {"conda": "ml_env"},
                },
                {
                    "name": "evaluate",
                    "command": ["python", "eval.py"],
                    "depends_on": ["train"],
                    "environment": {"conda": "ml_env"},
                },
            ],
        }

        yaml_path = temp_dir / "exports_workflow.yaml"
        with open(yaml_path, "w") as f:
            yaml.dump(yaml_content, f)

        runner = WorkflowRunner.from_yaml(yaml_path)

        train_job = next(j for j in runner.workflow.jobs if j.name == "train")
        assert train_job.exports == {
            "model_path": "/data/experiments/models/best.pt",
            "metrics_dir": "/data/experiments/metrics",
        }

    def test_parse_job_with_exports(self):
        """Test parse_job correctly passes exports."""
        data = {
            "name": "train",
            "command": ["python", "train.py"],
            "exports": {"model_path": "/data/model.pt"},
        }
        job = WorkflowRunner.parse_job(data)
        assert job.exports == {"model_path": "/data/model.pt"}

    def test_parse_job_without_exports(self):
        """Test parse_job defaults to empty exports."""
        data = {
            "name": "train",
            "command": ["python", "train.py"],
        }
        job = WorkflowRunner.parse_job(data)
        assert job.exports == {}

    def test_deps_reference_resolved_at_load_time(self, temp_dir):
        """deps.<name>.<key> is resolved to literal string at from_yaml."""
        yaml_content = {
            "name": "wf",
            "args": {"base": "/data"},
            "jobs": [
                {
                    "name": "preprocess",
                    "command": ["echo", "pre"],
                    "exports": {"data_path": "{{ base }}/processed"},
                },
                {
                    "name": "train",
                    "command": [
                        "python",
                        "train.py",
                        "--data",
                        "{{ deps.preprocess.data_path }}",
                    ],
                    "depends_on": ["preprocess"],
                },
            ],
        }
        yaml_path = temp_dir / "wf.yaml"
        with open(yaml_path, "w") as f:
            yaml.dump(yaml_content, f)

        runner = WorkflowRunner.from_yaml(yaml_path)
        train = next(j for j in runner.workflow.jobs if j.name == "train")
        assert train.command == [
            "python",
            "train.py",
            "--data",
            "/data/processed",
        ]

    def test_deps_missing_key_raises(self, temp_dir):
        """Reference to non-existent deps.X.Y fails at load time (StrictUndefined)."""
        yaml_content = {
            "name": "wf",
            "jobs": [
                {"name": "a", "command": ["echo", "a"], "exports": {"good": "x"}},
                {
                    "name": "b",
                    "command": ["echo", "{{ deps.a.bad }}"],
                    "depends_on": ["a"],
                },
            ],
        }
        yaml_path = temp_dir / "wf.yaml"
        with open(yaml_path, "w") as f:
            yaml.dump(yaml_content, f)
        with pytest.raises(WorkflowValidationError):
            WorkflowRunner.from_yaml(yaml_path)

    def test_deps_undeclared_dep_raises(self, temp_dir):
        """Referencing a job not in depends_on fails at load time."""
        yaml_content = {
            "name": "wf",
            "jobs": [
                {"name": "a", "command": ["echo", "a"], "exports": {"x": "1"}},
                {
                    "name": "b",
                    "command": ["echo", "{{ deps.a.x }}"],
                    # Note: depends_on NOT declared
                },
            ],
        }
        yaml_path = temp_dir / "wf.yaml"
        with open(yaml_path, "w") as f:
            yaml.dump(yaml_content, f)
        with pytest.raises(WorkflowValidationError):
            WorkflowRunner.from_yaml(yaml_path)

    def test_transitive_deps_composition(self, temp_dir):
        """Exports can reference earlier deps' exports at load time."""
        yaml_content = {
            "name": "wf",
            "args": {"base": "/exp"},
            "jobs": [
                {
                    "name": "a",
                    "command": ["echo", "a"],
                    "exports": {"dir": "{{ base }}/a"},
                },
                {
                    "name": "b",
                    "command": ["echo", "b"],
                    "depends_on": ["a"],
                    "exports": {"dir": "{{ deps.a.dir }}/b"},
                },
                {
                    "name": "c",
                    "command": ["echo", "{{ deps.b.dir }}"],
                    "depends_on": ["b"],
                },
            ],
        }
        yaml_path = temp_dir / "wf.yaml"
        with open(yaml_path, "w") as f:
            yaml.dump(yaml_content, f)
        runner = WorkflowRunner.from_yaml(yaml_path)
        c = next(j for j in runner.workflow.jobs if j.name == "c")
        assert c.command == ["echo", "/exp/a/b"]

    def test_cycle_detection_raises(self, temp_dir):
        """Circular job dependencies fail at from_yaml (CycleError)."""
        yaml_content = {
            "name": "cyc",
            "jobs": [
                {"name": "a", "command": ["echo", "a"], "depends_on": ["b"]},
                {"name": "b", "command": ["echo", "b"], "depends_on": ["a"]},
            ],
        }
        yaml_path = temp_dir / "cyc.yaml"
        with open(yaml_path, "w") as f:
            yaml.dump(yaml_content, f)
        with pytest.raises(WorkflowValidationError, match="[Cc]ircular"):
            WorkflowRunner.from_yaml(yaml_path)

    def test_python_arg_feeds_exports_feeds_deps(self, temp_dir):
        """python: args → exports → deps.X.Y chain resolves at load time."""
        yaml_content = {
            "name": "wf",
            "args": {
                "stamp": "python: 'run_2026'",
                "base": "{{ stamp }}_/data",
            },
            "jobs": [
                {
                    "name": "prep",
                    "command": ["echo", "prep"],
                    "exports": {"out": "{{ base }}/prep"},
                },
                {
                    "name": "train",
                    "command": ["python", "t.py", "--in", "{{ deps.prep.out }}"],
                    "depends_on": ["prep"],
                },
            ],
        }
        yaml_path = temp_dir / "wf.yaml"
        with open(yaml_path, "w") as f:
            yaml.dump(yaml_content, f)
        runner = WorkflowRunner.from_yaml(yaml_path)
        train = next(j for j in runner.workflow.jobs if j.name == "train")
        assert train.command == ["python", "t.py", "--in", "run_2026_/data/prep"]

    def test_deps_reference_in_work_dir_and_log_dir(self, temp_dir):
        """deps.X.Y resolves when placed in non-command fields."""
        yaml_content = {
            "name": "wf",
            "jobs": [
                {
                    "name": "a",
                    "command": ["echo", "a"],
                    "exports": {"root": "/shared/job_a"},
                },
                {
                    "name": "b",
                    "command": ["echo", "b"],
                    "depends_on": ["a"],
                    "work_dir": "{{ deps.a.root }}/work",
                    "log_dir": "{{ deps.a.root }}/logs",
                },
            ],
        }
        yaml_path = temp_dir / "wf.yaml"
        with open(yaml_path, "w") as f:
            yaml.dump(yaml_content, f)
        runner = WorkflowRunner.from_yaml(yaml_path)
        b = next(j for j in runner.workflow.jobs if j.name == "b")
        assert b.work_dir == "/shared/job_a/work"
        assert b.log_dir == "/shared/job_a/logs"

    def test_single_job_filter_preserves_deps_resolution(self, temp_dir):
        """single_job='train' still resolves {{ deps.prep.* }} via full DAG render."""
        yaml_content = {
            "name": "wf",
            "args": {"base": "/data"},
            "jobs": [
                {
                    "name": "prep",
                    "command": ["echo", "prep"],
                    "exports": {"p": "{{ base }}/prep"},
                },
                {
                    "name": "train",
                    "command": ["python", "t.py", "--in", "{{ deps.prep.p }}"],
                    "depends_on": ["prep"],
                },
            ],
        }
        yaml_path = temp_dir / "wf.yaml"
        with open(yaml_path, "w") as f:
            yaml.dump(yaml_content, f)
        runner = WorkflowRunner.from_yaml(yaml_path, single_job="train")
        assert len(runner.workflow.jobs) == 1
        assert runner.workflow.jobs[0].name == "train"
        assert runner.workflow.jobs[0].command == [
            "python",
            "t.py",
            "--in",
            "/data/prep",
        ]

    def test_shell_job_with_exports_resolves_for_dependents(self, temp_dir):
        """ShellJob.exports is consumable by downstream jobs at load time."""
        script_path = temp_dir / "prep.slurm.jinja"
        script_path.write_text("#!/bin/bash\necho prep\n")
        yaml_content = {
            "name": "wf",
            "jobs": [
                {
                    "name": "prep",
                    "script_path": str(script_path),
                    "exports": {"out": "/shared/prep_result"},
                },
                {
                    "name": "train",
                    "command": ["python", "t.py", "--in", "{{ deps.prep.out }}"],
                    "depends_on": ["prep"],
                },
            ],
        }
        yaml_path = temp_dir / "wf.yaml"
        with open(yaml_path, "w") as f:
            yaml.dump(yaml_content, f)
        runner = WorkflowRunner.from_yaml(yaml_path)
        train = next(j for j in runner.workflow.jobs if j.name == "train")
        assert train.command == [
            "python",
            "t.py",
            "--in",
            "/shared/prep_result",
        ]

    def test_exports_key_shadowing_dict_method(self, temp_dir):
        """Export keys colliding with dict methods (items/get/keys/...) must
        still resolve to the user's value, not the method reference."""
        yaml_content = {
            "name": "wf",
            "jobs": [
                {
                    "name": "a",
                    "command": ["echo", "a"],
                    "exports": {
                        "items": "ITEMS_VALUE",
                        "keys": "KEYS_VALUE",
                        "get": "GET_VALUE",
                    },
                },
                {
                    "name": "b",
                    "command": [
                        "echo",
                        "{{ deps.a.items }}",
                        "{{ deps.a.keys }}",
                        "{{ deps.a.get }}",
                    ],
                    "depends_on": ["a"],
                },
            ],
        }
        yaml_path = temp_dir / "wf.yaml"
        with open(yaml_path, "w") as f:
            yaml.dump(yaml_content, f)
        runner = WorkflowRunner.from_yaml(yaml_path)
        b = next(j for j in runner.workflow.jobs if j.name == "b")
        assert b.command == [
            "echo",
            "ITEMS_VALUE",
            "KEYS_VALUE",
            "GET_VALUE",
        ]

    def test_legacy_outputs_key_rejected(self, temp_dir):
        """Legacy 'outputs:' key must fail fast with a migration message,
        not silently drop the values."""
        yaml_content = {
            "name": "wf",
            "jobs": [
                {"name": "a", "command": ["echo", "a"], "outputs": {"x": "1"}},
            ],
        }
        yaml_path = temp_dir / "wf.yaml"
        with open(yaml_path, "w") as f:
            yaml.dump(yaml_content, f)
        with pytest.raises(WorkflowValidationError, match="'outputs'"):
            WorkflowRunner.from_yaml(yaml_path)

    def test_single_job_ignores_unrelated_broken_jinja(self, temp_dir):
        """A broken Jinja reference in a sibling job should not prevent
        single_job rendering of the target."""
        yaml_content = {
            "name": "wf",
            "jobs": [
                {"name": "target", "command": ["echo", "target"]},
                {
                    "name": "broken",
                    "command": ["echo", "{{ undefined_var }}"],
                },
            ],
        }
        yaml_path = temp_dir / "wf.yaml"
        with open(yaml_path, "w") as f:
            yaml.dump(yaml_content, f)
        runner = WorkflowRunner.from_yaml(yaml_path, single_job="target")
        assert [j.name for j in runner.workflow.jobs] == ["target"]

    def test_single_job_still_requires_its_own_deps(self, temp_dir):
        """single_job=target must still render target's transitive
        dependency chain so {{ deps.X.Y }} in target resolves."""
        yaml_content = {
            "name": "wf",
            "args": {"base": "/d"},
            "jobs": [
                {
                    "name": "prep",
                    "command": ["echo", "prep"],
                    "exports": {"p": "{{ base }}/out"},
                },
                {
                    "name": "train",
                    "command": ["python", "t.py", "--in", "{{ deps.prep.p }}"],
                    "depends_on": ["prep"],
                },
                {
                    "name": "sibling",
                    "command": ["echo", "{{ undefined_var }}"],
                },
            ],
        }
        yaml_path = temp_dir / "wf.yaml"
        with open(yaml_path, "w") as f:
            yaml.dump(yaml_content, f)
        runner = WorkflowRunner.from_yaml(yaml_path, single_job="train")
        train = runner.workflow.jobs[0]
        assert train.name == "train"
        assert train.command == ["python", "t.py", "--in", "/d/out"]


class TestSafeEvalExec:
    """Test that the eval/exec sandbox blocks dangerous operations."""

    def test_safe_eval_allows_datetime(self):
        """Safe builtins should include datetime module."""
        from srunx.runner import _safe_eval

        # Use str() on datetime to avoid CPython __import__ in .today()
        result = _safe_eval("str(datetime.date(2025, 1, 15))", {})
        assert result == "2025-01-15"

    def test_safe_eval_allows_math(self):
        """Safe builtins should include math module."""
        from srunx.runner import _safe_eval

        assert _safe_eval("math.ceil(3.2)", {}) == 4
        assert _safe_eval("math.pi > 3.14", {}) is True

    def test_safe_eval_allows_basic_types(self):
        """Safe builtins should include basic constructors."""
        from srunx.runner import _safe_eval

        assert _safe_eval("int('42')", {}) == 42
        assert _safe_eval("str(123)", {}) == "123"
        assert _safe_eval("len([1,2,3])", {}) == 3

    def test_safe_eval_blocks_os_system(self):
        """os.system must not be accessible."""
        from srunx.runner import _safe_eval

        with pytest.raises((NameError, AttributeError)):
            _safe_eval("os.system('echo pwned')", {})

    def test_safe_eval_blocks_import(self):
        """__import__ must not be accessible."""
        from srunx.runner import _safe_eval

        with pytest.raises(NameError):
            _safe_eval("__import__('os')", {})

    def test_safe_eval_blocks_open(self):
        """open() must not be accessible."""
        from srunx.runner import _safe_eval

        with pytest.raises(NameError):
            _safe_eval("open('/etc/passwd')", {})

    def test_safe_eval_blocks_subprocess(self):
        """subprocess must not be accessible."""
        from srunx.runner import _safe_eval

        with pytest.raises(NameError):
            _safe_eval("subprocess.run(['ls'])", {})

    def test_safe_exec_blocks_import_statement(self):
        """import statements must fail in sandbox."""
        from srunx.runner import _safe_exec

        with pytest.raises(ValueError, match="Unsupported statement"):
            _safe_exec("import os", {})

    def test_safe_eval_blocks_class_escape(self):
        """Prevent sandbox escape via __class__.__bases__.__subclasses__."""
        from srunx.runner import _safe_eval

        with pytest.raises((AttributeError, ValueError)):
            _safe_eval("().__class__.__bases__[0].__subclasses__()", {})

    def test_safe_eval_blocks_type_escape(self):
        """Prevent sandbox escape via type()."""
        from srunx.runner import _safe_eval

        with pytest.raises(NameError):
            _safe_eval("type(()).__bases__[0].__subclasses__()", {})

    def test_safe_exec_allows_result(self):
        """exec sandbox should allow setting result variable."""
        from srunx.runner import _safe_exec

        ns = _safe_exec("result = 2 + 2", {})
        assert ns["result"] == 4

    def test_safe_eval_with_args(self):
        """Safe eval should accept local variables."""
        from srunx.runner import _safe_eval

        result = _safe_eval("args['x'] + 1", {"args": {"x": 41}})
        assert result == 42

    def test_python_prefix_case_insensitive(self):
        """python: prefix detection must be case-insensitive."""
        from srunx.runner import _has_python_prefix

        assert _has_python_prefix("python: x")
        assert _has_python_prefix("Python: x")
        assert _has_python_prefix("PYTHON: x")
        assert _has_python_prefix("  python: x")
        assert not _has_python_prefix("not python at all")
