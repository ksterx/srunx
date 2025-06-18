"""Workflow runner for executing YAML-defined workflows with SLURM and Prefect."""

from pathlib import Path
from typing import Any

import yaml
from prefect import flow
from prefect.states import State

from srunx.logging import get_logger
from srunx.models import (
    Job,
    JobEnvironment,
    JobResource,
    ShellJob,
    Workflow,
    WorkflowTask,
)
from srunx.workflows.tasks import submit_and_monitor_job, submit_job_async

logger = get_logger(__name__)


class WorkflowRunner:
    """Runner for executing workflows defined in YAML."""

    def __init__(self) -> None:
        """Initialize workflow runner."""
        self.executed_tasks: dict[str, State[Job | ShellJob]] = {}

    def load_from_yaml(self, yaml_path: str | Path) -> Workflow:
        """Load and validate a workflow from a YAML file.

        Args:
            yaml_path: Path to the YAML workflow definition file.

        Returns:
            Validated Workflow object.

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

        return self._parse_workflow_data(data)

    def _parse_workflow_data(self, data: dict) -> Workflow:
        """Parse workflow data from dictionary."""
        workflow_name = data.get("name", "unnamed_workflow")
        tasks_data = data.get("tasks", [])

        tasks = []
        for task_data in tasks_data:
            task = self._parse_task_data(task_data)
            tasks.append(task)

        return Workflow(name=workflow_name, tasks=tasks)

    def _parse_task_data(self, task_data: dict) -> WorkflowTask:
        """Parse a single task from dictionary."""
        # Basic task properties
        name = task_data["name"]
        path = task_data.get("path")
        async_execution = task_data.get("async", False)
        depends_on = task_data.get("depends_on", [])

        job_data: dict[str, Any] = {"name": name}

        job: Job | ShellJob
        if path:
            job_data |= {"path": path}
            job = ShellJob.model_validate(job_data)
        else:
            command = task_data.get("command")
            if task_data.get("log_dir") is not None:
                job_data["log_dir"] = task_data.get("log_dir")
            if task_data.get("work_dir") is not None:
                job_data["work_dir"] = task_data.get("work_dir")

            # Resource configuration
            resources = JobResource(
                nodes=task_data.get("nodes", 1),
                gpus_per_node=task_data.get("gpus_per_node", 0),
                ntasks_per_node=task_data.get("ntasks_per_node", 1),
                cpus_per_task=task_data.get("cpus_per_task", 1),
                memory_per_node=task_data.get("memory_per_node"),
                time_limit=task_data.get("time_limit"),
            )

            # Environment configuration
            environment = JobEnvironment(
                conda=task_data.get("conda"),
                venv=task_data.get("venv"),
                sqsh=task_data.get("sqsh") or task_data.get("container"),
                env_vars=task_data.get("env_vars", {}),
            )

            job_data |= {
                "command": command,
                "resources": resources,
                "environment": environment,
            }

            # Create job
            job = Job.model_validate(job_data)

        return WorkflowTask(
            name=name,
            job=job,
            depends_on=depends_on,
            async_execution=async_execution,
        )

    def execute_workflow(self, workflow: Workflow) -> dict[str, State[Job | ShellJob]]:
        """Execute a workflow using Prefect.

        Args:
            workflow: Workflow to execute.

        Returns:
            Dictionary mapping task names to Job instances.
        """
        task_map = {task.name: task for task in workflow.tasks}

        @flow(name=workflow.name)
        def workflow_flow() -> dict[str, State[Job | ShellJob]]:
            """Prefect flow for workflow execution."""

            def execute_task(task_name: str) -> State[Job | ShellJob]:
                """Execute a task and its dependencies recursively."""
                if task_name in self.executed_tasks:
                    return self.executed_tasks[task_name]

                task = task_map[task_name]

                # Execute dependencies first
                for dependency in task.depends_on:
                    execute_task(dependency)

                # Execute the task
                if task.async_execution:
                    job_future = submit_job_async(task.job)
                else:
                    job_future = submit_and_monitor_job(task.job)

                self.executed_tasks[task_name] = job_future
                return job_future

            # Execute all tasks
            results: dict[str, State[Job | ShellJob]] = {}
            for task in workflow.tasks:
                results[task.name] = execute_task(task.name)

            return results

        return workflow_flow()

    def execute_from_yaml(
        self, yaml_path: str | Path
    ) -> dict[str, State[Job | ShellJob]]:
        """Load and execute a workflow from YAML file.

        Args:
            yaml_path: Path to YAML workflow file.

        Returns:
            Dictionary mapping task names to Job instances.
        """
        logger.info(f"Loading workflow from {yaml_path}")
        workflow = self.load_from_yaml(yaml_path)

        logger.info(
            f"Executing workflow '{workflow.name}' with {len(workflow.tasks)} tasks"
        )
        results = self.execute_workflow(workflow)

        logger.info("Workflow execution completed")
        return results


def run_workflow_from_file(yaml_path: str | Path) -> dict[str, State[Job | ShellJob]]:
    """Convenience function to run workflow from YAML file.

    Args:
        yaml_path: Path to YAML workflow file.

    Returns:
        Dictionary mapping task names to Job instances.
    """
    runner = WorkflowRunner()
    return runner.execute_from_yaml(yaml_path)


def validate_workflow_dependencies(workflow: Workflow) -> None:
    """Validate workflow task dependencies."""
    task_names = {task.name for task in workflow.tasks}

    for task in workflow.tasks:
        for dependency in task.depends_on:
            if dependency not in task_names:
                raise ValueError(
                    f"Task '{task.name}' depends on unknown task '{dependency}'"
                )

    # Check for circular dependencies (simple check)
    visited = set()
    rec_stack = set()

    def has_cycle(task_name: str) -> bool:
        if task_name in rec_stack:
            return True
        if task_name in visited:
            return False

        visited.add(task_name)
        rec_stack.add(task_name)

        task = workflow.get_task(task_name)
        if task:
            for dependency in task.depends_on:
                if has_cycle(dependency):
                    return True

        rec_stack.remove(task_name)
        return False

    for task in workflow.tasks:
        if has_cycle(task.name):
            raise ValueError(
                f"Circular dependency detected involving task '{task.name}'"
            )


def show_workflow_plan(workflow: Workflow) -> None:
    """Show workflow execution plan."""
    msg = f"""\
{" PLAN ":=^80}
Workflow: {workflow.name}
Tasks: {len(workflow.tasks)}
"""

    for task in workflow.tasks:
        msg += f"    Task: {task.name}\n"
        if isinstance(task.job, Job):
            msg += f"{'        Command:': <21} {' '.join(task.job.command or [])}\n"
            msg += f"{'        Resources:': <21} {task.job.resources.nodes} nodes, {task.job.resources.gpus_per_node} GPUs/node\n"
            if task.job.environment.conda:
                msg += f"{'        Conda env:': <21} {task.job.environment.conda}\n"
            if task.job.environment.sqsh:
                msg += f"{'        Sqsh:': <21} {task.job.environment.sqsh}\n"
            if task.job.environment.venv:
                msg += f"{'        Venv:': <21} {task.job.environment.venv}\n"
        elif isinstance(task.job, ShellJob):
            msg += f"{'        Path:': <21} {task.job.path}\n"
        if task.depends_on:
            msg += f"{'        Dependencies:': <21} {', '.join(task.depends_on)}\n"
        if task.async_execution:
            msg += f"{'        Execution:': <21} asynchronous\n"

    msg += f"{'=' * 80}\n"
    print(msg)
