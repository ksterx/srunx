"""Workflow aggregate — a named DAG of jobs with dependency validation."""

from srunx.domain.jobs import Job, RunnableJobType, ShellJob
from srunx.exceptions import WorkflowValidationError


class Workflow:
    """Represents a workflow containing multiple jobs with dependencies."""

    def __init__(self, name: str, jobs: list[RunnableJobType] | None = None) -> None:
        if jobs is None:
            jobs = []

        self.name = name
        self.jobs = jobs

    def add(self, job: RunnableJobType) -> None:
        # Check that all dependency jobs exist in the workflow
        if job.depends_on:
            job_names = {j.name for j in self.jobs}
            for dep in job.depends_on:
                if dep not in job_names:
                    raise WorkflowValidationError(
                        f"Job '{job.name}' depends on unknown job '{dep}'"
                    )
        self.jobs.append(job)

    def remove(self, job: RunnableJobType) -> None:
        self.jobs.remove(job)

    def get(self, name: str) -> RunnableJobType | None:
        """Get a job by name."""
        for job in self.jobs:
            if job.name == name:
                return job
        return None

    def get_dependencies(self, job_name: str) -> list[str]:
        """Get dependencies for a specific job."""
        job = self.get(job_name)
        return job.depends_on if job else []

    def show(self):
        msg = f"""\
{" PLAN ":=^80}
Workflow: {self.name}
Jobs: {len(self.jobs)}
"""

        def add_indent(indent: int, msg: str) -> str:
            return "    " * indent + msg

        for job in self.jobs:
            msg += add_indent(1, f"Job: {job.name}\n")
            if isinstance(job, Job):
                command_str = (
                    job.command
                    if isinstance(job.command, str)
                    else " ".join(job.command or [])
                )
                msg += add_indent(2, f"{'Command:': <13} {command_str}\n")
                msg += add_indent(
                    2,
                    f"{'Resources:': <13} {job.resources.nodes} nodes, {job.resources.gpus_per_node} GPUs/node\n",
                )
                if job.environment.conda:
                    msg += add_indent(
                        2, f"{'Conda env:': <13} {job.environment.conda}\n"
                    )
                if job.environment.container:
                    msg += add_indent(
                        2, f"{'Container:': <13} {job.environment.container}\n"
                    )
                if job.environment.venv:
                    msg += add_indent(2, f"{'Venv:': <13} {job.environment.venv}\n")
            elif isinstance(job, ShellJob):
                msg += add_indent(2, f"{'Script path:': <13} {job.script_path}\n")
                if job.script_vars:
                    msg += add_indent(2, f"{'Script vars:': <13} {job.script_vars}\n")
            if job.depends_on:
                dep_strs = [str(dep) for dep in job.parsed_dependencies]
                msg += add_indent(2, f"{'Dependencies:': <13} {', '.join(dep_strs)}\n")

        msg += f"{'=' * 80}\n"
        print(msg)

    def validate(self):
        """Validate workflow job dependencies."""
        job_names = {job.name for job in self.jobs}

        if len(job_names) != len(self.jobs):
            raise WorkflowValidationError("Duplicate job names found in workflow")

        for job in self.jobs:
            # Check that all dependency job names exist
            for parsed_dep in job.parsed_dependencies:
                if parsed_dep.job_name not in job_names:
                    raise WorkflowValidationError(
                        f"Job '{job.name}' depends on unknown job '{parsed_dep.job_name}'"
                    )

        # Check for circular dependencies
        visited = set()
        rec_stack = set()

        def has_cycle(job_name: str) -> bool:
            if job_name in rec_stack:
                return True
            if job_name in visited:
                return False

            visited.add(job_name)
            rec_stack.add(job_name)

            job = self.get(job_name)
            if job:
                for parsed_dep in job.parsed_dependencies:
                    if has_cycle(parsed_dep.job_name):
                        return True

            rec_stack.remove(job_name)
            return False

        for job in self.jobs:
            if has_cycle(job.name):
                raise WorkflowValidationError(
                    f"Circular dependency detected involving job '{job.name}'"
                )
