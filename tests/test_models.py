"""Tests for srunx.models module."""

import os
import tempfile
from pathlib import Path

import pytest
from pydantic import ValidationError

from srunx.models import (
    Job,
    JobEnvironment,
    JobResource,
    JobStatus,
    ShellJob,
    Workflow,
    WorkflowTask,
    render_job_script,
)


class TestJobStatus:
    """Test JobStatus enum."""

    def test_job_status_values(self) -> None:
        """Test JobStatus enum values."""
        assert JobStatus.PENDING.value == "PENDING"
        assert JobStatus.RUNNING.value == "RUNNING"
        assert JobStatus.COMPLETED.value == "COMPLETED"
        assert JobStatus.FAILED.value == "FAILED"
        assert JobStatus.CANCELLED.value == "CANCELLED"
        assert JobStatus.TIMEOUT.value == "TIMEOUT"


class TestJobResource:
    """Test JobResource model."""

    def test_job_resource_defaults(self) -> None:
        """Test JobResource with default values."""
        resource = JobResource()
        assert resource.nodes == 1
        assert resource.gpus_per_node == 0
        assert resource.ntasks_per_node == 1
        assert resource.cpus_per_task == 1
        assert resource.memory_per_node is None
        assert resource.time_limit is None

    def test_job_resource_custom_values(self) -> None:
        """Test JobResource with custom values."""
        resource = JobResource(
            nodes=4,
            gpus_per_node=2,
            ntasks_per_node=8,
            cpus_per_task=4,
            memory_per_node="64GB",
            time_limit="2:00:00",
        )
        assert resource.nodes == 4
        assert resource.gpus_per_node == 2
        assert resource.ntasks_per_node == 8
        assert resource.cpus_per_task == 4
        assert resource.memory_per_node == "64GB"
        assert resource.time_limit == "2:00:00"

    def test_job_resource_validation(self) -> None:
        """Test JobResource validation."""
        with pytest.raises(ValidationError):
            JobResource(nodes=0)
        with pytest.raises(ValidationError):
            JobResource(gpus_per_node=-1)
        with pytest.raises(ValidationError):
            JobResource(ntasks_per_node=0)
        with pytest.raises(ValidationError):
            JobResource(cpus_per_task=0)


class TestJobEnvironment:
    """Test JobEnvironment model."""

    def test_job_environment_conda(self) -> None:
        """Test JobEnvironment with conda."""
        env = JobEnvironment(conda="my_env")
        assert env.conda == "my_env"
        assert env.venv is None
        assert env.sqsh is None
        assert env.env_vars == {}

    def test_job_environment_venv(self) -> None:
        """Test JobEnvironment with venv."""
        env = JobEnvironment(venv="/path/to/venv")
        assert env.venv == "/path/to/venv"
        assert env.conda is None
        assert env.sqsh is None

    def test_job_environment_sqsh(self) -> None:
        """Test JobEnvironment with sqsh."""
        env = JobEnvironment(sqsh="/path/to/image.sqsh")
        assert env.sqsh == "/path/to/image.sqsh"
        assert env.conda is None
        assert env.venv is None

    def test_job_environment_env_vars(self) -> None:
        """Test JobEnvironment with environment variables."""
        env_vars = {"PATH": "/custom/path", "PYTHONPATH": "/custom/python"}
        env = JobEnvironment(conda="test_env", env_vars=env_vars)
        assert env.env_vars == env_vars

    def test_job_environment_validation_none(self) -> None:
        """Test JobEnvironment validation with no environment set."""
        with pytest.raises(ValidationError, match="Exactly one of"):
            JobEnvironment()

    def test_job_environment_validation_multiple(self) -> None:
        """Test JobEnvironment validation with multiple environments set."""
        with pytest.raises(ValidationError, match="Exactly one of"):
            JobEnvironment(conda="test", venv="/path/to/venv")

        with pytest.raises(ValidationError, match="Exactly one of"):
            JobEnvironment(conda="test", sqsh="/path/to/image")

        with pytest.raises(ValidationError, match="Exactly one of"):
            JobEnvironment(venv="/path/to/venv", sqsh="/path/to/image")

        with pytest.raises(ValidationError, match="Exactly one of"):
            JobEnvironment(conda="test", venv="/path/to/venv", sqsh="/path/to/image")


class TestJob:
    """Test Job model."""

    def test_job_with_command(self) -> None:
        """Test Job with command."""
        job = Job(
            name="test_job",
            command=["python", "script.py"],
            environment=JobEnvironment(conda="test_env"),
        )
        assert job.name == "test_job"
        assert job.command == ["python", "script.py"]
        assert job.environment is not None
        assert job.environment.conda == "test_env"
        assert job.job_id is None
        assert job.status is None

    def test_job_with_file(self) -> None:
        """Test Job with file."""
        job = ShellJob(
            name="test_job",
            path="/path/to/script.slurm",
        )
        assert job.name == "test_job"

    def test_job_default_values(self) -> None:
        """Test Job with default values."""
        job = Job(
            command=["echo", "hello"],
            environment=JobEnvironment(conda="test_env"),
        )
        assert job.name == "job"
        assert job.log_dir == "logs"
        assert job.work_dir == os.getcwd()
        assert job.dependencies == []
        assert isinstance(job.resources, JobResource)

    def test_job_validation_no_command_or_file(self) -> None:
        """Test Job validation when no command is set."""
        with pytest.raises(ValidationError):
            Job(environment=JobEnvironment(conda="test_env"))  # type: ignore

    def test_job_validation_both_command_and_file(self) -> None:
        """Test ShellJob validation when no path is set."""
        with pytest.raises(ValidationError):
            ShellJob(name="test_job")  # type: ignore

    def test_job_with_status(self) -> None:
        """Test Job with status set."""
        job = Job(
            command=["python", "script.py"],
            environment=JobEnvironment(conda="test_env"),
            job_id=12345,
            status=JobStatus.RUNNING,
        )
        assert job.job_id == 12345
        assert job.status == JobStatus.RUNNING


class TestWorkflowTask:
    """Test WorkflowTask model."""

    def test_workflow_task_basic(self) -> None:
        """Test basic WorkflowTask."""
        job = Job(
            name="test_job",
            command=["python", "script.py"],
            environment=JobEnvironment(conda="test_env"),
        )
        task = WorkflowTask(name="task1", job=job)
        assert task.name == "task1"
        assert task.job == job
        assert task.depends_on == []
        assert task.async_execution is False

    def test_workflow_task_with_dependencies(self) -> None:
        """Test WorkflowTask with dependencies."""
        job = Job(
            name="test_job",
            command=["python", "script.py"],
            environment=JobEnvironment(conda="test_env"),
        )
        task = WorkflowTask(
            name="task2",
            job=job,
            depends_on=["task1"],
            async_execution=True,
        )
        assert task.depends_on == ["task1"]
        assert task.async_execution is True


class TestWorkflow:
    """Test Workflow model."""

    def test_workflow_basic(self) -> None:
        """Test basic Workflow."""
        tasks: list[WorkflowTask] = []
        workflow = Workflow(name="test_workflow", tasks=tasks)
        assert workflow.name == "test_workflow"
        assert workflow.tasks == []

    def test_workflow_with_tasks(self) -> None:
        """Test Workflow with tasks."""
        job1 = Job(
            name="job1",
            command=["python", "script1.py"],
            environment=JobEnvironment(conda="test_env"),
        )
        job2 = Job(
            name="job2",
            command=["python", "script2.py"],
            environment=JobEnvironment(conda="test_env"),
        )
        task1 = WorkflowTask(name="task1", job=job1)
        task2 = WorkflowTask(name="task2", job=job2, depends_on=["task1"])

        workflow = Workflow(name="test_workflow", tasks=[task1, task2])
        assert len(workflow.tasks) == 2

    def test_workflow_get_task(self) -> None:
        """Test Workflow.get_task method."""
        job = Job(
            name="test_job",
            command=["python", "script.py"],
            environment=JobEnvironment(conda="test_env"),
        )
        task = WorkflowTask(name="task1", job=job)
        workflow = Workflow(name="test_workflow", tasks=[task])

        assert workflow.get_task("task1") == task
        assert workflow.get_task("nonexistent") is None

    def test_workflow_get_task_dependencies(self) -> None:
        """Test Workflow.get_task_dependencies method."""
        job = Job(
            name="test_job",
            command=["python", "script.py"],
            environment=JobEnvironment(conda="test_env"),
        )
        task = WorkflowTask(name="task1", job=job, depends_on=["task0"])
        workflow = Workflow(name="test_workflow", tasks=[task])

        assert workflow.get_task_dependencies("task1") == ["task0"]
        assert workflow.get_task_dependencies("nonexistent") == []


class TestRenderJobScript:
    """Test render_job_script function."""

    def test_render_job_script_basic(self) -> None:
        """Test basic job script rendering."""
        job = Job(
            name="test_job",
            command=["python", "script.py"],
            environment=JobEnvironment(conda="test_env"),
        )

        # Create a simple template
        template_content = """#!/bin/bash
#SBATCH --job-name={{ job_name }}
#SBATCH --nodes={{ nodes }}

{{ environment_setup }}
{{ command }}
"""

        with tempfile.TemporaryDirectory() as temp_dir:
            template_path = Path(temp_dir) / "test.slurm.jinja"
            with open(template_path, "w") as f:
                f.write(template_content)

            script_path = render_job_script(template_path, job, temp_dir)

            assert os.path.exists(script_path)
            assert script_path.endswith("test_job.slurm")

            with open(script_path) as f:
                content = f.read()
                assert "#SBATCH --job-name=test_job" in content
                assert "#SBATCH --nodes=1" in content
                assert "conda activate test_env" in content
                assert "python script.py" in content

    def test_render_job_script_with_resources(self) -> None:
        """Test job script rendering with custom resources."""
        resources = JobResource(
            nodes=2,
            gpus_per_node=1,
            memory_per_node="32GB",
            time_limit="1:00:00",
        )
        job = Job(
            name="gpu_job",
            command=["python", "gpu_script.py"],
            resources=resources,
            environment=JobEnvironment(conda="gpu_env"),
        )

        template_content = """#!/bin/bash
#SBATCH --job-name={{ job_name }}
#SBATCH --nodes={{ nodes }}
{% if gpus_per_node > 0 %}
#SBATCH --gres=gpu:{{ gpus_per_node }}
{% endif %}
{% if memory_per_node %}
#SBATCH --mem={{ memory_per_node }}
{% endif %}
{% if time_limit %}
#SBATCH --time={{ time_limit }}
{% endif %}

{{ environment_setup }}
{{ command }}
"""

        with tempfile.TemporaryDirectory() as temp_dir:
            template_path = Path(temp_dir) / "test.slurm.jinja"
            with open(template_path, "w") as f:
                f.write(template_content)

            script_path = render_job_script(template_path, job, temp_dir)

            with open(script_path) as f:
                content = f.read()
                assert "#SBATCH --nodes=2" in content
                assert "#SBATCH --gres=gpu:1" in content
                assert "#SBATCH --mem=32GB" in content
                assert "#SBATCH --time=1:00:00" in content

    def test_render_job_script_file_not_found(self) -> None:
        """Test render_job_script with non-existent template."""
        job = Job(
            name="test_job",
            command=["python", "script.py"],
            environment=JobEnvironment(conda="test_env"),
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            with pytest.raises(FileNotFoundError):
                render_job_script("/nonexistent/template.jinja", job, temp_dir)

    def test_render_job_script_with_venv(self) -> None:
        """Test job script rendering with virtual environment."""
        job = Job(
            name="venv_job",
            command=["python", "script.py"],
            environment=JobEnvironment(venv="/path/to/venv"),
        )

        template_content = """#!/bin/bash
{{ environment_setup }}
{{ command }}
"""

        with tempfile.TemporaryDirectory() as temp_dir:
            template_path = Path(temp_dir) / "test.slurm.jinja"
            with open(template_path, "w") as f:
                f.write(template_content)

            script_path = render_job_script(template_path, job, temp_dir)

            with open(script_path) as f:
                content = f.read()
                assert "source /path/to/venv/bin/activate" in content

    def test_render_job_script_with_sqsh(self) -> None:
        """Test job script rendering with sqsh container."""
        job = Job(
            name="container_job",
            command=["python", "script.py"],
            environment=JobEnvironment(sqsh="/path/to/image.sqsh"),
        )

        template_content = """#!/bin/bash
{{ environment_setup }}
{{ command }}
"""

        with tempfile.TemporaryDirectory() as temp_dir:
            template_path = Path(temp_dir) / "test.slurm.jinja"
            with open(template_path, "w") as f:
                f.write(template_content)

            script_path = render_job_script(template_path, job, temp_dir)

            with open(script_path) as f:
                content = f.read()
                assert "/path/to/image.sqsh" in content
                assert "--container-image" in content
