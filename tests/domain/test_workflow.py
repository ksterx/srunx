"""Tests for srunx.domain.workflow (Workflow validation, dependency cycles, add())."""

import pytest

from srunx.common.exceptions import WorkflowValidationError
from srunx.domain import (
    Job,
    JobEnvironment,
    Workflow,
)


class TestWorkflow:
    """Test Workflow model."""

    def test_workflow_creation(self):
        """Test Workflow creation."""
        job1 = Job(
            name="job1",
            command=["echo", "hello"],
            environment=JobEnvironment(conda="env1"),
        )
        job2 = Job(
            name="job2",
            command=["echo", "world"],
            environment=JobEnvironment(conda="env2"),
            depends_on=["job1"],
        )

        workflow = Workflow(name="test_workflow", jobs=[job1, job2])
        assert workflow.name == "test_workflow"
        assert len(workflow.jobs) == 2

    def test_workflow_get_job(self):
        """Test Workflow get method."""
        job = Job(
            name="test_job",
            command=["echo", "test"],
            environment=JobEnvironment(conda="env"),
        )
        workflow = Workflow(name="test", jobs=[job])

        found_job = workflow.get("test_job")
        assert found_job is not None
        assert found_job.name == "test_job"

        not_found = workflow.get("nonexistent")
        assert not_found is None

    def test_workflow_get_dependencies(self):
        """Test Workflow get_dependencies method."""
        job1 = Job(
            name="job1", command=["echo", "1"], environment=JobEnvironment(conda="env")
        )
        job2 = Job(
            name="job2",
            command=["echo", "2"],
            environment=JobEnvironment(conda="env"),
            depends_on=["job1"],
        )

        workflow = Workflow(name="test", jobs=[job1, job2])

        deps = workflow.get_dependencies("job2")
        assert deps == ["job1"]

        deps = workflow.get_dependencies("job1")
        assert deps == []

        deps = workflow.get_dependencies("nonexistent")
        assert deps == []

    def test_workflow_validate_success(self):
        """Test successful workflow validation."""
        job1 = Job(
            name="job1", command=["echo", "1"], environment=JobEnvironment(conda="env")
        )
        job2 = Job(
            name="job2",
            command=["echo", "2"],
            environment=JobEnvironment(conda="env"),
            depends_on=["job1"],
        )

        workflow = Workflow(name="test", jobs=[job1, job2])
        workflow.validate()  # Should not raise

    def test_workflow_validate_duplicate_names(self):
        """Test workflow validation with duplicate job names."""
        job1 = Job(
            name="job1", command=["echo", "1"], environment=JobEnvironment(conda="env")
        )
        job2 = Job(
            name="job1",  # Duplicate name
            command=["echo", "2"],
            environment=JobEnvironment(conda="env"),
        )

        workflow = Workflow(name="test", jobs=[job1, job2])
        with pytest.raises(WorkflowValidationError, match="Duplicate job names"):
            workflow.validate()

    def test_workflow_validate_unknown_dependency(self):
        """Test workflow validation with unknown dependency."""
        job = Job(
            name="job1",
            command=["echo", "1"],
            environment=JobEnvironment(conda="env"),
            depends_on=["unknown_job"],
        )

        workflow = Workflow(name="test", jobs=[job])
        with pytest.raises(WorkflowValidationError, match="depends on unknown job"):
            workflow.validate()

    def test_workflow_validate_circular_dependency(self):
        """Test workflow validation with circular dependency."""
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
            depends_on=["job1"],
        )

        workflow = Workflow(name="test", jobs=[job1, job2])
        with pytest.raises(WorkflowValidationError, match="Circular dependency"):
            workflow.validate()


class TestWorkflowAdd:
    """Test Workflow.add() method with dependency validation."""

    def test_add_job_without_dependencies(self):
        """Adding a job with no dependencies should succeed."""
        wf = Workflow(name="test")
        job = Job(name="job1", command=["echo", "1"])
        wf.add(job)
        assert len(wf.jobs) == 1
        assert wf.jobs[0].name == "job1"

    def test_add_job_with_valid_dependency(self):
        """Adding a job whose dependencies exist should succeed."""
        wf = Workflow(name="test")
        job1 = Job(name="job1", command=["echo", "1"])
        job2 = Job(name="job2", command=["echo", "2"], depends_on=["job1"])
        wf.add(job1)
        wf.add(job2)
        assert len(wf.jobs) == 2

    def test_add_job_with_invalid_dependency_raises(self):
        """Adding a job with unknown dependency should raise."""
        from srunx.common.exceptions import WorkflowValidationError

        wf = Workflow(name="test")
        job = Job(name="job1", command=["echo", "1"], depends_on=["nonexistent"])
        with pytest.raises(WorkflowValidationError, match="unknown job 'nonexistent'"):
            wf.add(job)
