"""Workflow runner for executing YAML-defined workflows with SLURM"""

import threading
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

import yaml

from srunx.client import Slurm
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
    """Runner for executing workflows defined in YAML with dynamic task scheduling.

    Tasks are executed as soon as their dependencies are satisfied,
    rather than waiting for entire dependency levels to complete.
    """

    def __init__(self) -> None:
        """Initialize workflow runner."""
        self.executed_tasks: dict[str, Job | ShellJob] = {}
        self.slurm = Slurm()

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
        )

    def run(self, workflow: Workflow) -> dict[str, Job | ShellJob]:
        """Execute a workflow with dynamic task scheduling.

        Tasks are executed as soon as their dependencies are satisfied,
        rather than waiting for entire levels to complete.

        Args:
            workflow: Workflow to execute.

        Returns:
            Dictionary mapping task names to Job instances.
        """
        task_map = {task.name: task for task in workflow.tasks}

        # Track task states: 'pending', 'running', 'completed'
        task_states = {task.name: "pending" for task in workflow.tasks}

        # Build reverse dependency map: task -> tasks that depend on it
        reverse_deps = defaultdict(set)
        for task in workflow.tasks:
            for dep in task.depends_on:
                reverse_deps[dep].add(task.name)

        # Results and futures tracking
        results: dict[str, Job | ShellJob] = {}
        running_futures: dict[str, Any] = {}

        # Thread-safe lock for state updates
        state_lock = threading.Lock()

        def is_ready_to_run(task_name: str) -> bool:
            """Check if all dependencies for a task are completed."""
            task = task_map[task_name]
            return all(task_states[dep] == "completed" for dep in task.depends_on)

        def get_ready_tasks() -> list[str]:
            """Get all tasks that are ready to run."""
            return [
                task_name
                for task_name in task_states
                if task_states[task_name] == "pending" and is_ready_to_run(task_name)
            ]

        def execute_task(task_name: str) -> Job | ShellJob:
            """Execute a single task."""
            logger.info(f"🚀 Starting task: {task_name}")
            task = task_map[task_name]

            # Type narrow the job to the expected union type
            job = task.job
            if not isinstance(job, Job | ShellJob):
                raise TypeError(f"Unexpected job type: {type(job)}")

            job_result = submit_job_async(job)

            logger.success(f"✅ Completed task: {task_name}")
            return job_result

        def on_task_complete(task_name: str, result: Job | ShellJob) -> list[str]:
            """Handle task completion and schedule dependent tasks.

            Returns:
                List of newly ready task names.
            """
            with state_lock:
                task_states[task_name] = "completed"
                results[task_name] = result
                self.executed_tasks[task_name] = result

                # Check if any dependent tasks are now ready
                newly_ready = []
                for dependent_task in reverse_deps[task_name]:
                    if task_states[dependent_task] == "pending" and is_ready_to_run(
                        dependent_task
                    ):
                        newly_ready.append(dependent_task)

                logger.info(
                    f"📋 Task {task_name} completed. Ready to start: {newly_ready}"
                )
                return newly_ready

        # Use ThreadPoolExecutor for parallel execution
        with ThreadPoolExecutor(max_workers=8) as executor:
            # Submit initial tasks (those with no dependencies)
            initial_tasks = get_ready_tasks()
            logger.info(f"🌋 Starting initial tasks: {initial_tasks}")

            for task_name in initial_tasks:
                task_states[task_name] = "running"
                future = executor.submit(execute_task, task_name)
                running_futures[task_name] = future

            # Process completed tasks and schedule new ones
            while running_futures:
                # Wait for at least one task to complete
                completed_futures = []
                for task_name, future in list(running_futures.items()):
                    if future.done():
                        completed_futures.append((task_name, future))
                        del running_futures[task_name]

                if not completed_futures:
                    # Sleep briefly to avoid busy waiting
                    time.sleep(0.1)
                    continue

                # Handle completed tasks
                for task_name, future in completed_futures:
                    try:
                        result = future.result()
                        newly_ready = on_task_complete(task_name, result)

                        # Schedule newly ready tasks
                        for ready_task in newly_ready:
                            if ready_task not in running_futures:
                                task_states[ready_task] = "running"
                                new_future = executor.submit(execute_task, ready_task)
                                running_futures[ready_task] = new_future

                    except Exception as e:
                        logger.error(f"❌ Task {task_name} failed: {e}")
                        # Mark as completed to avoid infinite loop
                        with state_lock:
                            task_states[task_name] = "completed"
                        raise

        # Verify all tasks completed
        incomplete_tasks = [
            name for name, state in task_states.items() if state != "completed"
        ]
        if incomplete_tasks:
            logger.error(f"❌ Some tasks did not complete: {incomplete_tasks}")
            raise RuntimeError(f"Workflow execution incomplete: {incomplete_tasks}")

        return results

    def execute_from_yaml(self, yaml_path: str | Path) -> dict[str, Job | ShellJob]:
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
        results = self.run(workflow)

        logger.success("🎉 Workflow completed successfully")
        return results

    def _build_execution_levels(self, workflow: Workflow) -> dict[int, list[str]]:
        """Build execution levels for parallel task execution.

        Tasks in the same level can be executed in parallel.
        This method is kept for backward compatibility but is no longer used
        in the new dynamic scheduling approach.

        Args:
            workflow: Workflow to analyze.

        Returns:
            Dictionary mapping level numbers to lists of task names.
        """
        task_map = {task.name: task for task in workflow.tasks}
        levels: dict[int, list[str]] = defaultdict(list)
        task_levels: dict[str, int] = {}

        # Calculate the maximum depth for each task
        def calculate_depth(task_name: str, visited: set[str]) -> int:
            if task_name in visited:
                raise ValueError(
                    f"Circular dependency detected involving task '{task_name}'"
                )

            if task_name in task_levels:
                return task_levels[task_name]

            task = task_map[task_name]
            if not task.depends_on:
                # No dependencies, can execute at level 0
                task_levels[task_name] = 0
                return 0

            visited.add(task_name)
            max_dep_level = -1

            for dep in task.depends_on:
                dep_level = calculate_depth(dep, visited)
                max_dep_level = max(max_dep_level, dep_level)

            visited.remove(task_name)

            # This task executes after all its dependencies
            task_level = max_dep_level + 1
            task_levels[task_name] = task_level
            return task_level

        # Calculate levels for all tasks
        for task in workflow.tasks:
            level = calculate_depth(task.name, set())
            levels[level].append(task.name)

        return dict(levels)


def run_workflow_from_file(yaml_path: str | Path) -> dict[str, Job | ShellJob]:
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
Execution: Dynamic scheduling (tasks run as dependencies are satisfied)
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
