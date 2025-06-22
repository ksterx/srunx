"""Workflow runner for executing YAML-defined workflows with SLURM"""

import time
from collections import defaultdict
from collections.abc import Sequence
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any, Self

import yaml

from srunx.callbacks import Callback
from srunx.client import Slurm
from srunx.exceptions import WorkflowValidationError
from srunx.logging import get_logger
from srunx.models import (
    Job,
    JobEnvironment,
    JobResource,
    JobStatus,
    RunableJobType,
    ShellJob,
    Workflow,
)

logger = get_logger(__name__)


class WorkflowRunner:
    """Runner for executing workflows defined in YAML with dynamic job scheduling.

    Jobs are executed as soon as their dependencies are satisfied,
    rather than waiting for entire dependency levels to complete.
    """

    def __init__(
        self, workflow: Workflow, callbacks: Sequence[Callback] | None = None
    ) -> None:
        """Initialize workflow runner.

        Args:
            workflow: Workflow to execute.
            callbacks: List of callbacks for job notifications.
        """
        self.workflow = workflow
        self.slurm = Slurm(callbacks=callbacks)
        self.callbacks = callbacks or []

    @classmethod
    def from_yaml(
        cls, yaml_path: str | Path, callbacks: Sequence[Callback] | None = None
    ) -> Self:
        """Load and validate a workflow from a YAML file.

        Args:
            yaml_path: Path to the YAML workflow definition file.
            callbacks: List of callbacks for job notifications.

        Returns:
            WorkflowRunner instance with loaded workflow.

        Raises:
            FileNotFoundError: If the YAML file doesn't exist.
            yaml.YAMLError: If the YAML is malformed.
            ValidationError: If the workflow structure is invalid.
        """
        yaml_file = Path(yaml_path)
        if not yaml_file.exists():
            raise FileNotFoundError(f"Workflow file not found: {yaml_path}")

        with open(yaml_file, encoding="utf-8") as f:
            data = yaml.safe_load(f)

        name = data.get("name", "unnamed")
        jobs_data = data.get("jobs", [])

        jobs = []
        for job_data in jobs_data:
            job = cls.parse_job(job_data)
            jobs.append(job)
        return cls(workflow=Workflow(name=name, jobs=jobs), callbacks=callbacks)

    def get_independent_jobs(self) -> list[RunableJobType]:
        """Get all jobs that are independent of any other job."""
        independent_jobs = []
        for job in self.workflow.jobs:
            if not job.depends_on:
                independent_jobs.append(job)
        return independent_jobs

    def run(self) -> dict[str, RunableJobType]:
        """Run a workflow with dynamic job scheduling.

        Jobs are executed as soon as their dependencies are satisfied.

        Returns:
            Dictionary mapping job names to completed Job instances.
        """
        logger.info(
            f"🚀 Starting Workflow {self.workflow.name} with {len(self.workflow.jobs)} jobs"
        )
        for callback in self.callbacks:
            callback.on_workflow_started(self.workflow)

        # Track all jobs and results
        all_jobs = self.workflow.jobs.copy()
        results: dict[str, RunableJobType] = {}
        running_futures: dict[str, Any] = {}

        # Build reverse dependency map for efficient lookups
        dependents = defaultdict(set)
        for job in all_jobs:
            for dep in job.depends_on:
                dependents[dep].add(job.name)

        def execute_job(job: RunableJobType) -> RunableJobType:
            """Execute a single job."""
            logger.info(f"🌋 {'SUBMITTED':<12} Job {job.name:<12}")

            try:
                result = self.slurm.run(job)
                return result
            except Exception as e:
                raise

        def on_job_complete(job_name: str, result: RunableJobType) -> list[str]:
            """Handle job completion and return newly ready job names."""
            results[job_name] = result
            completed_job_names = list(set(results.keys()))

            # Find newly ready jobs
            newly_ready = []
            for dependent_name in dependents[job_name]:
                dependent_job = next(j for j in all_jobs if j.name == dependent_name)
                if (
                    dependent_job.status == JobStatus.PENDING
                    and dependent_job.dependencies_satisfied(completed_job_names)
                ):
                    newly_ready.append(dependent_name)

            return newly_ready

        # Execute workflow with ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=8) as executor:
            # Submit initial ready jobs
            initial_jobs = self.get_independent_jobs()

            for job in initial_jobs:
                future = executor.submit(execute_job, job)
                running_futures[job.name] = future

            # Process completed jobs and schedule new ones
            while running_futures:
                # Check for completed futures
                completed = []
                for job_name, future in list(running_futures.items()):
                    if future.done():
                        completed.append((job_name, future))
                        del running_futures[job_name]

                if not completed:
                    time.sleep(0.1)  # Brief sleep to avoid busy waiting
                    continue

                # Handle completed jobs
                for job_name, future in completed:
                    try:
                        result = future.result()
                        newly_ready_names = on_job_complete(job_name, result)

                        # Schedule newly ready jobs
                        for ready_name in newly_ready_names:
                            if ready_name not in running_futures:
                                ready_job = next(
                                    j for j in all_jobs if j.name == ready_name
                                )
                                new_future = executor.submit(execute_job, ready_job)
                                running_futures[ready_name] = new_future

                    except Exception as e:
                        logger.error(f"❌ Job {job_name} failed: {e}")
                        raise

        # Verify all jobs completed successfully
        failed_jobs = [j.name for j in all_jobs if j.status == JobStatus.FAILED]
        incomplete_jobs = [
            j.name
            for j in all_jobs
            if j.status not in [JobStatus.COMPLETED, JobStatus.FAILED]
        ]

        if failed_jobs:
            logger.error(f"❌ Jobs failed: {failed_jobs}")
            raise RuntimeError(f"Workflow execution failed: {failed_jobs}")

        if incomplete_jobs:
            logger.error(f"❌ Jobs did not complete: {incomplete_jobs}")
            raise RuntimeError(f"Workflow execution incomplete: {incomplete_jobs}")

        logger.success(f"🎉 Workflow {self.workflow.name} completed!!")

        for callback in self.callbacks:
            callback.on_workflow_completed(self.workflow)

        return results

    def execute_from_yaml(self, yaml_path: str | Path) -> dict[str, RunableJobType]:
        """Load and execute a workflow from YAML file.

        Args:
            yaml_path: Path to YAML workflow file.

        Returns:
            Dictionary mapping job names to completed Job instances.
        """
        logger.info(f"Loading workflow from {yaml_path}")
        runner = self.from_yaml(yaml_path)
        return runner.run()

    @staticmethod
    def parse_job(data: dict[str, Any]) -> RunableJobType:
        if data.get("path") and data.get("command"):
            raise WorkflowValidationError("Job cannot have both 'path' and 'command'")

        base = {"name": data["name"], "depends_on": data.get("depends_on", [])}

        if data.get("path"):
            return ShellJob.model_validate({**base, "path": data["path"]})

        resource = JobResource.model_validate(data.get("resources", {}))
        environment = JobEnvironment.model_validate(data.get("environment", {}))

        job_data = {
            **base,
            "command": data["command"],
            "resources": resource,
            "environment": environment,
        }
        if data.get("log_dir"):
            job_data["log_dir"] = data["log_dir"]
        if data.get("work_dir"):
            job_data["work_dir"] = data["work_dir"]

        return Job.model_validate(job_data)


def run_workflow_from_file(yaml_path: str | Path) -> dict[str, RunableJobType]:
    """Convenience function to run workflow from YAML file.

    Args:
        yaml_path: Path to YAML workflow file.

    Returns:
        Dictionary mapping job names to completed Job instances.
    """
    runner = WorkflowRunner.from_yaml(yaml_path)
    return runner.run()
