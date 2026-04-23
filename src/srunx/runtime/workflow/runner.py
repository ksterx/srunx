"""Workflow runner: DAG scheduling + executor dispatch.

Canonical home of :class:`WorkflowRunner` since Phase 7 (#163). The
top-level :mod:`srunx.runtime.workflow.runner` module is a thin backward-compat shim
re-exporting the public symbols defined here.
"""

import time
from collections import defaultdict
from collections.abc import Sequence
from concurrent.futures import ThreadPoolExecutor
from contextlib import AbstractContextManager, nullcontext
from functools import cached_property
from pathlib import Path
from typing import TYPE_CHECKING, Any, Self

import jinja2
import yaml  # type: ignore

from srunx.callbacks import Callback
from srunx.common.exceptions import WorkflowValidationError
from srunx.common.logging import get_logger
from srunx.domain import (
    DependencyType,
    Job,
    JobEnvironment,
    JobResource,
    JobStatus,
    RunnableJobType,
    ShellJob,
    Workflow,
)
from srunx.runtime.workflow.loader import (
    _dependency_closure,
    _DepsNamespace,
    _evaluate_variables,
    _find_required_variables,
)
from srunx.runtime.workflow.transitions import _transition_workflow_run
from srunx.slurm.local import Slurm
from srunx.slurm.protocols import (
    WorkflowJobExecutor,
    WorkflowJobExecutorFactory,
)

if TYPE_CHECKING:
    from srunx.runtime.rendering import SubmissionRenderContext

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
        default_project: str | None = None,
        *,
        executor_factory: WorkflowJobExecutorFactory | None = None,
        submission_context: "SubmissionRenderContext | None" = None,
    ) -> None:
        """Initialize workflow runner.

        Args:
            workflow: Workflow to execute.
            callbacks: List of callbacks for job notifications.
            args: Template variables from the YAML args section.
            default_project: Default project (mount name) for file syncing.
            executor_factory: Optional context-manager factory producing a
                :class:`WorkflowJobExecutor` per lease. When ``None``
                (default) the runner falls back to a shared local
                :class:`Slurm` singleton wrapped in ``nullcontext`` — i.e.
                existing CLI behaviour is preserved bit-for-bit. Inject a
                factory to route submissions through an alternative
                executor (e.g. SSH-backed pool for the Web UI sweep path)
                without otherwise changing runner semantics.
            submission_context: Optional mount / default-path metadata
                forwarded verbatim to every ``executor.run`` call. SSH
                executors consume it to rewrite local ``work_dir`` /
                ``log_dir`` values to their remote equivalents just before
                render; the local :class:`Slurm` executor accepts it for
                protocol conformance and ignores it. Defaults to ``None``,
                matching the pre-Batch-2a CLI path where no translation
                is performed.
        """
        self.workflow = workflow
        self.callbacks = callbacks or []
        self.args = args or {}
        self.default_project = default_project
        self._executor_factory = executor_factory
        self._submission_context = submission_context

    @cached_property
    def slurm(self) -> Slurm:
        """Local :class:`Slurm` client, constructed on first access.

        Lazy construction matters when an ``executor_factory`` is
        injected (SSH-backed pool for the Web sweep path): that path
        never touches ``self.slurm`` and eagerly instantiating a local
        :class:`Slurm` on a machine without local SLURM would emit a
        spurious warning. :func:`_get_executor_cm` guards access behind
        the ``executor_factory is None`` branch so this property is only
        materialised when actually needed.
        """
        return Slurm(callbacks=self.callbacks)

    def _get_executor_cm(
        self,
    ) -> AbstractContextManager[WorkflowJobExecutor]:
        """Return a context manager yielding a workflow job executor.

        When an ``executor_factory`` is injected, defers to it (bounded
        pool lease, SSH adapter, etc.). Otherwise falls back to the
        legacy :class:`Slurm` singleton on ``self.slurm`` via
        ``nullcontext`` so backward-compatible call sites keep using the
        same underlying client.
        """
        if self._executor_factory is not None:
            return self._executor_factory()
        return nullcontext(self.slurm)

    @classmethod
    def from_yaml(
        cls,
        yaml_path: str | Path,
        callbacks: Sequence[Callback] | None = None,
        single_job: str | None = None,
        *,
        args_override: dict[str, Any] | None = None,
        executor_factory: WorkflowJobExecutorFactory | None = None,
        submission_context: "SubmissionRenderContext | None" = None,
    ) -> Self:
        """Load and validate a workflow from a YAML file.

        Args:
            yaml_path: Path to the YAML workflow definition file.
            callbacks: List of callbacks for job notifications.
            single_job: If specified, only load and process this job.
            args_override: Optional mapping merged over the YAML ``args``
                section before Jinja evaluation. Override entries win on
                key collision; keys absent from the YAML are added.
            executor_factory: Optional executor factory passed through to
                :class:`WorkflowRunner`; see ``__init__`` for semantics.
            submission_context: Optional submission-time render context
                forwarded verbatim to :class:`WorkflowRunner.__init__`.
                See that method's docstring for semantics.

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
        args = {**(data.get("args") or {}), **(args_override or {})}
        default_project = data.get("default_project")
        jobs_data = data.get("jobs", [])

        # For `single_job`, restrict rendering to the target and its
        # transitive dependencies so unrelated broken jobs don't block
        # a targeted re-run. Without single_job, render the full DAG.
        if single_job:
            if not any(jd.get("name") == single_job for jd in jobs_data):
                raise WorkflowValidationError(
                    f"Job '{single_job}' not found in workflow"
                )
            closure_names = _dependency_closure(jobs_data, single_job)
            render_input = [jd for jd in jobs_data if jd.get("name") in closure_names]
        else:
            render_input = jobs_data

        rendered_jobs_data = cls._render_jobs_with_args_and_deps(render_input, args)

        if single_job:
            rendered_jobs_data = [
                jd for jd in rendered_jobs_data if jd.get("name") == single_job
            ]

        jobs = []
        for job_data in rendered_jobs_data:
            job = cls.parse_job(job_data)
            jobs.append(job)
        return cls(
            workflow=Workflow(name=name, jobs=jobs),
            callbacks=callbacks,
            args=args,
            default_project=default_project,
            executor_factory=executor_factory,
            submission_context=submission_context,
        )

    @staticmethod
    def _render_jobs_with_args_and_deps(
        jobs_data: list[dict[str, Any]], args: dict[str, Any]
    ) -> list[dict[str, Any]]:
        """Render Jinja templates per-job in dependency order.

        Each job is rendered with `{**args, 'deps': <DepsNamespace>}`
        where ``deps`` exposes the already-rendered ``exports`` of every
        predecessor listed in the job's ``depends_on``. Uses
        StrictUndefined so typos in ``deps.X.Y`` fail fast at load time.

        Reject legacy ``outputs:`` keys explicitly rather than silently
        dropping them, so stale YAML surfaces a clear error instead of
        producing empty exports and a broken ``deps.X.Y`` resolution.
        """
        from graphlib import CycleError, TopologicalSorter

        for jd in jobs_data:
            if "outputs" in jd:
                raise WorkflowValidationError(
                    f"Job '{jd.get('name', '?')}' uses the removed 'outputs' key. "
                    "Rename to 'exports' and update consumers to reference the value "
                    "as '{{ deps.<job_name>.<key> }}' (load-time resolution). "
                    "See CHANGELOG migration guide."
                )

        # Evaluate args up front (supports `python:` prefix).
        if args:
            jobs_yaml = yaml.dump(jobs_data, default_flow_style=False)
            required = _find_required_variables(jobs_yaml, args)
            evaluated_args = _evaluate_variables(args, required)
        else:
            evaluated_args = {}

        name_to_data = {j["name"]: j for j in jobs_data if "name" in j}
        name_to_deps = {
            name: set(jd.get("depends_on", []) or []) & name_to_data.keys()
            for name, jd in name_to_data.items()
        }

        try:
            order = list(TopologicalSorter(name_to_deps).static_order())
        except CycleError as e:
            raise WorkflowValidationError(f"Circular job dependency: {e}") from e

        rendered: dict[str, dict[str, Any]] = {}
        for job_name in order:
            raw = name_to_data[job_name]
            deps_ctx = _DepsNamespace(
                {
                    dep: rendered[dep].get("exports", {}) or {}
                    for dep in name_to_deps[job_name]
                    if dep in rendered
                }
            )
            context = {**evaluated_args, "deps": deps_ctx}

            job_yaml = yaml.dump(raw, default_flow_style=False)
            try:
                template = jinja2.Template(job_yaml, undefined=jinja2.StrictUndefined)
                rendered_yaml = template.render(**context)
            except jinja2.TemplateError as e:
                raise WorkflowValidationError(
                    f"Failed to render job '{job_name}': {e}"
                ) from e
            rendered[job_name] = yaml.safe_load(rendered_yaml)

        return [rendered[j["name"]] for j in jobs_data if j.get("name") in rendered]

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
        *,
        workflow_run_id: int | None = None,
    ) -> dict[str, RunnableJobType]:
        """Run a workflow with dynamic job scheduling.

        Jobs are executed as soon as their dependencies are satisfied.

        Args:
            from_job: Start execution from this job (inclusive), ignoring dependencies
            to_job: Stop execution at this job (inclusive)
            single_job: Execute only this specific job, ignoring all dependencies
            workflow_run_id: Pre-materialized ``workflow_runs`` row id to
                attach this run to. When ``None`` (default), the runner
                creates one itself via ``create_cli_workflow_run``. The
                sweep orchestrator passes a non-None id so all cells of
                the sweep share materialised ``workflow_runs`` rows.

        Returns:
            Dictionary mapping job names to completed Job instances.
        """
        # Get the jobs to execute based on options
        jobs_to_execute = self._get_jobs_to_execute(from_job, to_job, single_job)

        # Persist a ``workflow_runs`` row so CLI-submitted jobs share
        # the same identity model the Web UI uses. Without this,
        # ``srunx report --workflow`` (which JOINs jobs to workflow_runs
        # on ``workflow_run_id``) returns zero rows for every CLI run.
        # Best-effort: a DB outage must not block the workflow itself.
        if workflow_run_id is None:
            from srunx.observability.storage.cli_helpers import create_cli_workflow_run

            workflow_run_id = create_cli_workflow_run(
                workflow_name=self.workflow.name,
                args=self.args or None,
            )
        if workflow_run_id is not None:
            # Flip from the default ``pending`` to ``running`` up-front so
            # ``workflow_runs`` reflects the live state; the final
            # completed/failed transition is recorded at the exit points
            # below. Terminal-status is cheap to re-mark, so a missed
            # update here is not fatal.
            _transition_workflow_run(workflow_run_id, "pending", "running")

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

                with self._get_executor_cm() as executor:
                    log_info = executor.get_job_output_detailed(job.job_id, job.name)

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
                with self._get_executor_cm() as executor:
                    result = executor.run(
                        job,
                        workflow_name=self.workflow.name,
                        workflow_run_id=workflow_run_id,
                        submission_context=self._submission_context,
                    )
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
                    if workflow_run_id is not None:
                        _transition_workflow_run(
                            workflow_run_id,
                            "running",
                            "failed",
                            error=f"Job {single_job} failed",
                        )
                    raise RuntimeError(f"Job {single_job} failed")

                logger.success(f"🎉 Job {single_job} completed!!")

                if workflow_run_id is not None:
                    _transition_workflow_run(workflow_run_id, "running", "completed")

                for callback in self.callbacks:
                    callback.on_workflow_completed(self.workflow)

                return results

            except Exception as e:
                logger.error(f"❌ Job {single_job} failed: {e}")
                if workflow_run_id is not None:
                    _transition_workflow_run(
                        workflow_run_id, "running", "failed", error=str(e)
                    )
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
                        and dep.dep_type == DependencyType.AFTER.value
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
            if workflow_run_id is not None:
                _transition_workflow_run(
                    workflow_run_id,
                    "running",
                    "failed",
                    error=f"Jobs failed: {failed_jobs}",
                )
            raise RuntimeError(f"Workflow execution failed: {failed_jobs}")

        if incomplete_jobs:
            logger.error(f"❌ Jobs did not complete: {incomplete_jobs}")
            if workflow_run_id is not None:
                _transition_workflow_run(
                    workflow_run_id,
                    "running",
                    "failed",
                    error=f"Jobs did not complete: {incomplete_jobs}",
                )
            raise RuntimeError(f"Workflow execution incomplete: {incomplete_jobs}")

        logger.success(f"🎉 Workflow {self.workflow.name} completed!!")

        if workflow_run_id is not None:
            _transition_workflow_run(workflow_run_id, "running", "completed")

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
            "exports": data.get("exports", {}),
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
        # Step 2 render-hint fields. Pulled only when present so jobs
        # without them keep Job's default=None (preserves pre-Phase-2
        # behaviour and avoids polluting model_dump output).
        if data.get("template") is not None:
            job_data["template"] = data["template"]
        if data.get("srun_args") is not None:
            job_data["srun_args"] = data["srun_args"]
        if data.get("launch_prefix") is not None:
            job_data["launch_prefix"] = data["launch_prefix"]

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
