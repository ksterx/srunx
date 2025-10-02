"""Workflow runner for executing YAML-defined workflows with SLURM"""

import time
from collections import defaultdict
from collections.abc import Sequence
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from textwrap import dedent
from typing import Any, Self

import jinja2
import yaml  # type: ignore

from srunx.callbacks import Callback
from srunx.client import Slurm
from srunx.exceptions import WorkflowValidationError
from srunx.logging import get_logger
from srunx.models import (
    DependencyType,
    Job,
    JobEnvironment,
    JobResource,
    JobStatus,
    RunnableJobType,
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
        self,
        workflow: Workflow,
        callbacks: Sequence[Callback] | None = None,
        args: dict[str, Any] | None = None,
    ) -> None:
        """Initialize workflow runner.

        Args:
            workflow: Workflow to execute.
            callbacks: List of callbacks for job notifications.
            args: Template variables from the YAML args section.
        """
        self.workflow = workflow
        self.slurm = Slurm(callbacks=callbacks)
        self.callbacks = callbacks or []
        self.args = args or {}

    @classmethod
    def from_yaml(
        cls,
        yaml_path: str | Path,
        callbacks: Sequence[Callback] | None = None,
        single_job: str | None = None,
    ) -> Self:
        """Load and validate a workflow from a YAML file.

        Args:
            yaml_path: Path to the YAML workflow definition file.
            callbacks: List of callbacks for job notifications.
            single_job: If specified, only load and process this job.

        Returns:
            WorkflowRunner instance with loaded workflow.

        Raises:
            FileNotFoundError: If the YAML file doesn't exist.
            yaml.YAMLError: If the YAML is malformed.
            WorkflowValidationError: If the workflow structure is invalid.
        """
        yaml_file = Path(yaml_path)
        if not yaml_file.exists():
            raise FileNotFoundError(f"Workflow file not found: {yaml_path}")

        with open(yaml_file, encoding="utf-8") as f:
            data = yaml.safe_load(f)

        name = data.get("name", "unnamed")
        args = data.get("args", {})
        jobs_data = data.get("jobs", [])

        # If single_job is specified, filter jobs_data to only include that job
        if single_job:
            filtered_jobs_data = [
                job for job in jobs_data if job.get("name") == single_job
            ]
            if not filtered_jobs_data:
                raise WorkflowValidationError(
                    f"Job '{single_job}' not found in workflow"
                )
            jobs_data = filtered_jobs_data

        # Render Jinja templates in jobs_data using args
        rendered_jobs_data = cls._render_jobs_with_args(jobs_data, args)

        jobs = []
        for job_data in rendered_jobs_data:
            job = cls.parse_job(job_data)
            jobs.append(job)
        return cls(
            workflow=Workflow(name=name, jobs=jobs), callbacks=callbacks, args=args
        )

    @staticmethod
    def _render_jobs_with_args(
        jobs_data: list[dict[str, Any]], args: dict[str, Any]
    ) -> list[dict[str, Any]]:
        """Render Jinja templates in job data using args.

        - Evaluate any "python:" values in args (Jinja → eval/exec).
        - Then render the whole jobs section with Jinja.
        """
        # 1) Find which variables are actually used in the jobs
        jobs_yaml = yaml.dump(jobs_data, default_flow_style=False)
        used_variables = set()

        # Extract variable names from Jinja templates in the jobs YAML
        import re

        jinja_pattern = r"\{\{\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*\}\}"
        matches = re.findall(jinja_pattern, jobs_yaml)
        used_variables.update(matches)

        # logger.info(f"Jobs YAML: {jobs_yaml}")
        # logger.info(f"Used variables: {used_variables}")

        # 2) Find all variables that the used variables depend on (transitively)
        def find_dependencies(
            var_name: str, args: dict[str, Any], visited: set[str] | None = None
        ) -> set[str]:
            """Find all variables that a given variable depends on, transitively."""
            if visited is None:
                visited = set()

            if var_name in visited or var_name not in args:
                return set()

            visited.add(var_name)
            deps = {var_name}

            var_value = args[var_name]
            if isinstance(var_value, str):
                # Find variables referenced in this variable's value
                referenced_vars = re.findall(jinja_pattern, var_value)
                for ref_var in referenced_vars:
                    deps.update(find_dependencies(ref_var, args, visited.copy()))

            return deps

        def find_all_jinja_vars(text: str) -> set[str]:
            """Find all Jinja variables in a text, including nested ones."""
            all_vars = set()
            current_vars = set(re.findall(jinja_pattern, text))
            all_vars.update(current_vars)

            # For each variable found, check if its value contains more variables
            for var in current_vars:
                if var in args:
                    var_value = args[var]
                    if isinstance(var_value, str):
                        nested_vars = find_all_jinja_vars(var_value)
                        all_vars.update(nested_vars)

            return all_vars

        # Find all required variables (including transitive dependencies)
        # Also look for nested Jinja variables in the jobs_yaml itself
        all_text_vars = find_all_jinja_vars(jobs_yaml)
        used_variables.update(all_text_vars)

        required_variables = set()
        for used_var in used_variables:
            required_variables.update(find_dependencies(used_var, args))

        # logger.info(f"Required variables: {required_variables}")
        # logger.info(f"Available args: {list(args.keys())}")

        # 3) Evaluate all required variables in correct dependency order
        if args:
            evaluated_args: dict[str, Any] = {}

            # Separate Python and non-Python variables
            python_vars = {
                k: v
                for k, v in args.items()
                if isinstance(v, str)
                and v.startswith("python:")
                and k in required_variables
            }
            non_python_vars = {
                k: v
                for k, v in args.items()
                if k in required_variables
                and not (isinstance(v, str) and v.startswith("python:"))
            }

            # First, evaluate Python variables that don't depend on other variables or have simple dependencies
            # This handles cases like 'today' which is used by non-Python variables
            processed_python: set[str] = set()

            # Find Python variables that non-Python variables depend on
            python_vars_needed_by_non_python = set()
            for _, value in non_python_vars.items():
                if isinstance(value, str):
                    var_deps = set(re.findall(jinja_pattern, value))
                    python_vars_needed_by_non_python.update(
                        var_deps & set(python_vars.keys())
                    )

            # Evaluate these critical Python variables first
            for key in python_vars_needed_by_non_python:
                if key in python_vars:
                    value = python_vars[key]
                    code = value[len("python:") :].lstrip()
                    try:
                        # Simple evaluation for variables that don't depend on others
                        code = jinja2.Template(
                            code, undefined=jinja2.DebugUndefined
                        ).render(**evaluated_args)
                        code = dedent(code).lstrip()

                        try:
                            evaluated = eval(code, globals(), {"args": evaluated_args})
                        except SyntaxError:
                            exec_ns: dict[str, Any] = {"args": evaluated_args}
                            exec(code, globals(), exec_ns)
                            evaluated = exec_ns.get("result")

                        evaluated_args[key] = evaluated
                        processed_python.add(key)
                    except Exception as e:
                        logger.warning(
                            f"Failed to evaluate critical Python variable {key}: {e}"
                        )

            # Then process non-python vars in dependency order
            processed_non_python: set[str] = set()
            while len(processed_non_python) < len(non_python_vars):
                made_progress = False
                for key, value in non_python_vars.items():
                    if key in processed_non_python:
                        continue

                    # Check if this variable depends on other variables
                    if isinstance(value, str):
                        var_deps = set(re.findall(jinja_pattern, value))
                        required_non_python_deps = var_deps & set(
                            non_python_vars.keys()
                        )

                        # All required dependencies must be available
                        if required_non_python_deps.issubset(processed_non_python):
                            # All dependencies are ready, render this variable
                            if var_deps:
                                template = jinja2.Template(
                                    value, undefined=jinja2.StrictUndefined
                                )
                                evaluated_args[key] = template.render(**evaluated_args)
                            else:
                                evaluated_args[key] = value
                            processed_non_python.add(key)
                            made_progress = True
                    else:
                        # Non-string value, no dependencies
                        evaluated_args[key] = value
                        processed_non_python.add(key)
                        made_progress = True

                if not made_progress:
                    # Handle remaining variables
                    for key, value in non_python_vars.items():
                        if key not in processed_non_python:
                            if isinstance(value, str) and re.findall(
                                jinja_pattern, value
                            ):
                                # Try to render with DebugUndefined
                                template = jinja2.Template(
                                    value, undefined=jinja2.DebugUndefined
                                )
                                evaluated_args[key] = template.render(**evaluated_args)
                            else:
                                evaluated_args[key] = value
                            processed_non_python.add(key)
                    break

            # Finally, evaluate remaining Python variables in dependency order
            # Remove already processed Python variables
            remaining_python_vars = {
                k: v for k, v in python_vars.items() if k not in processed_python
            }

            while remaining_python_vars:
                made_progress = False
                for key, value in list(remaining_python_vars.items()):
                    code = value[len("python:") :].lstrip()

                    # Check if this code depends on other variables
                    depends_on = set()
                    for other_key in args.keys():
                        if other_key != key and other_key in code:
                            depends_on.add(other_key)

                    # Can evaluate if all dependencies are already evaluated
                    # Check both python variables and non-python variables
                    required_deps = depends_on & required_variables
                    available_vars = set(evaluated_args.keys())
                    if required_deps.issubset(available_vars):
                        try:
                            # Allow {{ ... }} inside "python:" by rendering with Jinja first
                            code = jinja2.Template(
                                code, undefined=jinja2.StrictUndefined
                            ).render(**evaluated_args)
                            code = dedent(code).lstrip()

                            try:
                                evaluated = eval(
                                    code, globals(), {"args": evaluated_args}
                                )
                            except SyntaxError:
                                exec_ns_remaining: dict[str, Any] = {
                                    "args": evaluated_args
                                }
                                exec(code, globals(), exec_ns_remaining)
                                evaluated = exec_ns_remaining.get("result")

                            evaluated_args[key] = evaluated
                            del remaining_python_vars[key]
                            made_progress = True
                        except Exception as e:
                            logger.warning(f"Failed to evaluate variable {key}: {e}")
                            del remaining_python_vars[key]
                            made_progress = True

                if not made_progress:
                    # Circular dependency or other issue, try to evaluate remaining
                    for key, value in remaining_python_vars.items():
                        try:
                            code = value[len("python:") :].lstrip()
                            code = jinja2.Template(
                                code, undefined=jinja2.DebugUndefined
                            ).render(**evaluated_args)
                            code = dedent(code).lstrip()
                            try:
                                evaluated = eval(
                                    code, globals(), {"args": evaluated_args}
                                )
                            except SyntaxError:
                                exec_ns2: dict[str, Any] = {"args": evaluated_args}
                                exec(code, globals(), exec_ns2)
                                evaluated = exec_ns2.get("result")
                            evaluated_args[key] = evaluated
                        except Exception:
                            # Skip variables that can't be evaluated
                            pass
                    break

            args = evaluated_args

        # logger.info(f"Final evaluated args: {args}")

        # 4) Render the jobs section with the evaluated variables
        template = jinja2.Template(jobs_yaml, undefined=jinja2.DebugUndefined)
        try:
            rendered_yaml = template.render(**(args or {}))
            # logger.info(f"Rendered YAML: {rendered_yaml}")
            return yaml.safe_load(rendered_yaml)
        except jinja2.TemplateError as e:
            logger.error(f"Jinja template rendering failed: {e}")
            raise WorkflowValidationError(f"Template rendering failed: {e}") from e

    def get_independent_jobs(self) -> list[RunnableJobType]:
        """Get all jobs that are independent of any other job."""
        independent_jobs = []
        for job in self.workflow.jobs:
            if not job.depends_on:
                independent_jobs.append(job)
        return independent_jobs

    def _get_jobs_to_execute(
        self,
        from_job: str | None = None,
        to_job: str | None = None,
        single_job: str | None = None,
    ) -> list[RunnableJobType]:
        """Determine which jobs to execute based on the execution control options.

        Args:
            from_job: Start execution from this job (inclusive)
            to_job: Stop execution at this job (inclusive)
            single_job: Execute only this specific job

        Returns:
            List of jobs to execute.

        Raises:
            WorkflowValidationError: If specified jobs are not found.
        """
        all_jobs = self.workflow.jobs
        job_names = {job.name for job in all_jobs}

        # Validate job names exist
        if single_job and single_job not in job_names:
            raise WorkflowValidationError(f"Job '{single_job}' not found in workflow")
        if from_job and from_job not in job_names:
            raise WorkflowValidationError(f"Job '{from_job}' not found in workflow")
        if to_job and to_job not in job_names:
            raise WorkflowValidationError(f"Job '{to_job}' not found in workflow")

        # Single job execution - return just that job
        if single_job:
            return [job for job in all_jobs if job.name == single_job]

        # Full workflow execution - return all jobs
        if not from_job and not to_job:
            return all_jobs

        # Partial execution - determine job range
        jobs_to_execute = []

        if from_job and to_job:
            # Execute from from_job to to_job (inclusive)
            start_idx = None
            end_idx = None
            for i, job in enumerate(all_jobs):
                if job.name == from_job:
                    start_idx = i
                if job.name == to_job:
                    end_idx = i

            if start_idx is not None and end_idx is not None:
                if start_idx <= end_idx:
                    jobs_to_execute = all_jobs[start_idx : end_idx + 1]
                else:
                    # Handle reverse order - get all jobs between them
                    jobs_to_execute = all_jobs[end_idx : start_idx + 1]
            else:
                jobs_to_execute = all_jobs

        elif from_job:
            # Execute from from_job to end
            start_idx = None
            for i, job in enumerate(all_jobs):
                if job.name == from_job:
                    start_idx = i
                    break
            if start_idx is not None:
                jobs_to_execute = all_jobs[start_idx:]
            else:
                jobs_to_execute = all_jobs

        elif to_job:
            # Execute from beginning to to_job
            end_idx = None
            for i, job in enumerate(all_jobs):
                if job.name == to_job:
                    end_idx = i
                    break
            if end_idx is not None:
                jobs_to_execute = all_jobs[: end_idx + 1]
            else:
                jobs_to_execute = all_jobs

        return jobs_to_execute

    def run(
        self,
        from_job: str | None = None,
        to_job: str | None = None,
        single_job: str | None = None,
    ) -> dict[str, RunnableJobType]:
        """Run a workflow with dynamic job scheduling.

        Jobs are executed as soon as their dependencies are satisfied.

        Args:
            from_job: Start execution from this job (inclusive), ignoring dependencies
            to_job: Stop execution at this job (inclusive)
            single_job: Execute only this specific job, ignoring all dependencies

        Returns:
            Dictionary mapping job names to completed Job instances.
        """
        # Get the jobs to execute based on options
        jobs_to_execute = self._get_jobs_to_execute(from_job, to_job, single_job)

        # Log execution plan
        if single_job:
            logger.info(f"🚀 Executing single job: {single_job}")
        elif from_job or to_job:
            job_range = []
            if from_job:
                job_range.append(f"from {from_job}")
            if to_job:
                job_range.append(f"to {to_job}")
            logger.info(
                f"🚀 Executing workflow {self.workflow.name} ({' '.join(job_range)}) - {len(jobs_to_execute)} jobs"
            )
        else:
            logger.info(
                f"🚀 Starting Workflow {self.workflow.name} with {len(jobs_to_execute)} jobs"
            )

        for callback in self.callbacks:
            callback.on_workflow_started(self.workflow)

        # Track jobs to execute and results
        all_jobs = jobs_to_execute.copy()
        results: dict[str, RunnableJobType] = {}
        running_futures: dict[str, Any] = {}

        # For partial execution, we need to handle dependencies differently
        ignore_dependencies = from_job is not None

        def _show_job_logs_on_failure(job: RunnableJobType) -> None:
            """Show job logs when a job fails."""
            try:
                if not job.job_id:
                    logger.warning("No job ID available for log retrieval")
                    return

                log_info = self.slurm.get_job_output_detailed(job.job_id, job.name)

                found_files = log_info.get("found_files", [])
                output = log_info.get("output", "")
                error = log_info.get("error", "")
                primary_log = log_info.get("primary_log")
                slurm_log_dir = log_info.get("slurm_log_dir")
                searched_dirs = log_info.get("searched_dirs", [])

                # Ensure types are correct
                if not isinstance(found_files, list):
                    found_files = []
                if not isinstance(output, str):
                    output = ""
                if not isinstance(error, str):
                    error = ""
                if not isinstance(searched_dirs, list):
                    searched_dirs = []

                if not found_files:
                    logger.error("❌ No log files found")
                    logger.info(f"📁 Searched in: {', '.join(searched_dirs)}")
                    if slurm_log_dir:
                        logger.info(f"💡 SLURM_LOG_DIR: {slurm_log_dir}")
                    else:
                        logger.info("💡 SLURM_LOG_DIR not set")
                    return

                logger.info(f"📁 Found {len(found_files)} log file(s)")
                for log_file in found_files:
                    logger.info(f"  📄 {log_file}")

                if output:
                    logger.error("📋 Job output:")
                    # Truncate very long output
                    lines = output.split("\n")
                    max_lines = 50
                    if len(lines) > max_lines:
                        truncated_output = "\n".join(lines[-max_lines:])
                        logger.error(
                            f"{truncated_output}\n... (showing last {max_lines} lines of {len(lines)} total)"
                        )
                    else:
                        logger.error(output)

                if error:
                    logger.error("❌ Error output:")
                    logger.error(error)

                if primary_log:
                    logger.info(f"💡 Full log available at: {primary_log}")

            except Exception as e:
                logger.warning(f"Failed to retrieve job logs: {e}")

        def execute_job(job: RunnableJobType) -> RunnableJobType:
            """Execute a single job."""
            logger.info(f"⚡ {'SUBMITTED':<12} Job {job.name:<12}")

            try:
                result = self.slurm.run(job)
                return result
            except Exception as e:
                # Show SLURM logs when job fails
                if hasattr(job, "job_id") and job.job_id:
                    _show_job_logs_on_failure(job)
                raise

        def execute_job_with_retry(job: RunnableJobType) -> RunnableJobType:
            """Execute a job with retry logic."""
            while True:
                try:
                    result = execute_job(job)

                    # If job completed successfully, reset retry count and return
                    if result.status == JobStatus.COMPLETED:
                        job.reset_retry()
                        return result

                    # If job failed and can be retried
                    if result.status == JobStatus.FAILED and job.can_retry():
                        job.increment_retry()
                        retry_msg = f"(retry {job.retry_count}/{job.retry})"
                        logger.warning(
                            f"⚠️  Job {job.name} failed, retrying {retry_msg}"
                        )

                        # Wait before retrying
                        if job.retry_delay > 0:
                            logger.info(
                                f"⏳ Waiting {job.retry_delay}s before retry..."
                            )
                            time.sleep(job.retry_delay)

                        # Reset job_id for retry
                        job.job_id = None
                        job.status = JobStatus.PENDING
                        continue

                    # Job failed and no more retries, or job cancelled/timeout
                    # Show logs on final failure
                    if result.status == JobStatus.FAILED:
                        _show_job_logs_on_failure(result)
                    return result

                except Exception as e:
                    # Handle job submission/execution errors
                    if job.can_retry():
                        job.increment_retry()
                        retry_msg = f"(retry {job.retry_count}/{job.retry})"
                        logger.warning(
                            f"⚠️  Job {job.name} error: {e}, retrying {retry_msg}"
                        )

                        if job.retry_delay > 0:
                            logger.info(
                                f"⏳ Waiting {job.retry_delay}s before retry..."
                            )
                            time.sleep(job.retry_delay)

                        # Reset job state for retry
                        job.job_id = None
                        job.status = JobStatus.PENDING
                        continue
                    else:
                        # No more retries, re-raise the exception
                        raise

        # Special handling for single job execution - completely ignore all dependencies
        if single_job is not None:
            # Execute only the single job without any dependency processing
            single_job_obj = next(job for job in all_jobs if job.name == single_job)

            try:
                result = execute_job_with_retry(single_job_obj)
                results[single_job] = result

                if result.status == JobStatus.FAILED:
                    logger.error(f"❌ Job {single_job} failed")
                    raise RuntimeError(f"Job {single_job} failed")

                logger.success(f"🎉 Job {single_job} completed!!")

                for callback in self.callbacks:
                    callback.on_workflow_completed(self.workflow)

                return results

            except Exception as e:
                logger.error(f"❌ Job {single_job} failed: {e}")
                raise

        # Build reverse dependency map for efficient lookups (only for jobs we're executing)
        dependents = defaultdict(set)
        job_names_to_execute = {job.name for job in all_jobs}

        for job in all_jobs:
            if not ignore_dependencies:
                # Normal dependency handling
                for parsed_dep in job.parsed_dependencies:
                    dependents[parsed_dep.job_name].add(job.name)
            else:
                # For partial execution, only consider dependencies within the execution set
                for parsed_dep in job.parsed_dependencies:
                    if parsed_dep.job_name in job_names_to_execute:
                        dependents[parsed_dep.job_name].add(job.name)

        def on_job_started(job_name: str) -> list[str]:
            """Handle job start and return newly ready job names (for 'after' dependencies)."""
            # Build current job status map
            job_statuses = {}
            for job in all_jobs:
                job_statuses[job.name] = job.status
            # Mark the started job as RUNNING (or whatever status it should be)
            job_statuses[job_name] = JobStatus.RUNNING

            # Find newly ready jobs that depend on this job starting
            newly_ready = []
            for dependent_name in dependents[job_name]:
                dependent_job = next(
                    (j for j in all_jobs if j.name == dependent_name), None
                )
                if dependent_job is None:
                    continue

                if dependent_job.status == JobStatus.PENDING:
                    # Check if this job has "after" dependency on the started job
                    has_after_dep = any(
                        dep.job_name == job_name
                        and dep.dep_type == DependencyType.AFTER
                        for dep in dependent_job.parsed_dependencies
                    )

                    if has_after_dep:
                        if ignore_dependencies:
                            partial_job_statuses = {
                                name: status
                                for name, status in job_statuses.items()
                                if name in job_names_to_execute
                            }
                            deps_satisfied = dependent_job.dependencies_satisfied(
                                partial_job_statuses
                            )
                        else:
                            deps_satisfied = dependent_job.dependencies_satisfied(
                                job_statuses
                            )

                        if deps_satisfied:
                            newly_ready.append(dependent_name)

            return newly_ready

        def on_job_complete(job_name: str, result: RunnableJobType) -> list[str]:
            """Handle job completion and return newly ready job names."""
            results[job_name] = result

            # Build current job status map
            job_statuses = {}
            for job in all_jobs:
                job_statuses[job.name] = job.status
            # Update the completed job's status
            job_statuses[job_name] = result.status

            # Find newly ready jobs
            newly_ready = []
            for dependent_name in dependents[job_name]:
                dependent_job = next(
                    (j for j in all_jobs if j.name == dependent_name), None
                )
                if dependent_job is None:
                    continue

                if dependent_job.status == JobStatus.PENDING:
                    if ignore_dependencies:
                        # For partial execution, only check dependencies within our execution set
                        partial_job_statuses = {
                            name: status
                            for name, status in job_statuses.items()
                            if name in job_names_to_execute
                        }
                        deps_satisfied = dependent_job.dependencies_satisfied(
                            partial_job_statuses
                        )
                    else:
                        # Normal dependency checking with new interface
                        deps_satisfied = dependent_job.dependencies_satisfied(
                            job_statuses
                        )

                    if deps_satisfied:
                        newly_ready.append(dependent_name)

            return newly_ready

        # Execute workflow with ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=8) as executor:
            # Submit initial ready jobs
            if ignore_dependencies:
                # For partial execution, start with all jobs (dependencies are ignored or filtered)
                initial_jobs = all_jobs
            else:
                # Normal execution - start with independent jobs or jobs whose dependencies are satisfied
                initial_jobs = []
                job_statuses = {job.name: job.status for job in all_jobs}

                for job in all_jobs:
                    if not job.parsed_dependencies:
                        # Jobs with no dependencies
                        initial_jobs.append(job)
                    else:
                        # Check if dependencies are already satisfied
                        if job.dependencies_satisfied(job_statuses):
                            initial_jobs.append(job)

            for job in initial_jobs:
                future = executor.submit(execute_job_with_retry, job)
                running_futures[job.name] = future

                # Check for jobs that should start immediately after this job starts
                newly_ready_on_start = on_job_started(job.name)
                for ready_name in newly_ready_on_start:
                    if ready_name not in running_futures:
                        ready_job = next(j for j in all_jobs if j.name == ready_name)
                        new_future = executor.submit(execute_job_with_retry, ready_job)
                        running_futures[ready_name] = new_future

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
                                new_future = executor.submit(
                                    execute_job_with_retry, ready_job
                                )
                                running_futures[ready_name] = new_future

                                # Check for jobs that should start immediately after this job starts
                                newly_ready_on_start = on_job_started(ready_name)
                                for start_ready_name in newly_ready_on_start:
                                    if start_ready_name not in running_futures:
                                        start_ready_job = next(
                                            j
                                            for j in all_jobs
                                            if j.name == start_ready_name
                                        )
                                        start_future = executor.submit(
                                            execute_job_with_retry, start_ready_job
                                        )
                                        running_futures[start_ready_name] = start_future

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

    def execute_from_yaml(self, yaml_path: str | Path) -> dict[str, RunnableJobType]:
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
    def parse_job(data: dict[str, Any]) -> RunnableJobType:
        # Check for conflicting job types
        has_shell_fields = data.get("script_path") or data.get("path")
        has_command = data.get("command")

        if has_shell_fields and has_command:
            raise WorkflowValidationError(
                "Job cannot have both shell script fields (script_path/path) and 'command'"
            )

        base = {
            "name": data["name"],
            "depends_on": data.get("depends_on", []),
            "retry": data.get("retry", 0),
            "retry_delay": data.get("retry_delay", 60),
        }

        # Handle ShellJob (script_path or path)
        if data.get("script_path"):
            shell_job_data = {
                **base,
                "script_path": data["script_path"],
                "script_vars": data.get("script_vars", {}),
            }
            return ShellJob.model_validate(shell_job_data)

        if data.get("path"):
            return ShellJob.model_validate({**base, "script_path": data["path"]})

        # Handle regular Job (command)
        if not has_command:
            raise WorkflowValidationError(
                "Job must have either 'command' or 'script_path'"
            )

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


def run_workflow_from_file(
    yaml_path: str | Path, single_job: str | None = None
) -> dict[str, RunnableJobType]:
    """Convenience function to run workflow from YAML file.

    Args:
        yaml_path: Path to YAML workflow file.
        single_job: If specified, only run this job.

    Returns:
        Dictionary mapping job names to completed Job instances.
    """
    runner = WorkflowRunner.from_yaml(yaml_path, single_job=single_job)
    return runner.run(single_job=single_job)
