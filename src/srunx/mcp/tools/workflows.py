"""MCP tools for workflow YAML lifecycle: create / validate / run / list / get.

Workflow-specific guards (``_enforce_shell_script_roots`` /
``_resolve_mount_context``) live here rather than in
:mod:`srunx.mcp.helpers` because they are only consumed by ``run_workflow``
and reference workflow-layer types.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any

import yaml

from srunx.mcp.app import mcp
from srunx.mcp.helpers import err, ok, reject_python_prefix

if TYPE_CHECKING:
    from srunx.runtime.rendering import SubmissionRenderContext


@mcp.tool()
def create_workflow(
    name: str,
    jobs: list[dict[str, Any]],
    output_path: str,
    args: dict[str, Any] | None = None,
    default_project: str | None = None,
) -> dict[str, Any]:
    """Create a SLURM workflow YAML file.

    Generates a YAML workflow definition that can be executed with run_workflow.
    Each job in the workflow can depend on other jobs, forming a DAG.

    Args:
        name: Workflow name for identification
        jobs: List of job definitions. Each job dict should contain:
            - name (required): Job identifier
            - command (required for regular jobs): Command as string or list of strings
            - script_path (required for shell jobs): Path to shell script
            - depends_on: List of job names this job depends on (e.g. ["preprocess"])
              Supports dependency types: "afterok:job_a", "after:job_a", "afterany:job_a"
            - retry: Number of retry attempts on failure (default 0)
            - retry_delay: Seconds between retries (default 60)
            - resources: Dict with nodes, gpus_per_node, ntasks_per_node,
              cpus_per_task, memory_per_node, time_limit, partition, nodelist
            - environment: Dict with conda, venv, env_vars, container
            - log_dir: Log directory path
            - work_dir: Working directory path
        output_path: File path to write the YAML workflow (e.g. "workflow.yaml")
        args: Optional template variables for Jinja2 templating in job definitions
        default_project: Default SSH project/mount name for file syncing
    """
    try:
        from srunx.domain import Workflow
        from srunx.runtime.workflow.runner import WorkflowRunner

        # Reject python: args for security (arbitrary code execution).
        if args:
            try:
                reject_python_prefix(args, source="args")
            except ValueError as exc:
                return err(str(exc))

        # Validate the workflow structure before writing.
        # Skip job parsing validation when args are present (Jinja templates
        # in job fields would fail parse before rendering).
        if not args:
            parsed_jobs = []
            for job_data in jobs:
                parsed_jobs.append(WorkflowRunner.parse_job(job_data))
            workflow = Workflow(name=name, jobs=parsed_jobs)
            workflow.validate()
        else:
            # Basic structural validation only
            for job_data in jobs:
                if "name" not in job_data:
                    return err("Each job must have a 'name' field")
                if "command" not in job_data and "script_path" not in job_data:
                    return err(
                        f"Job '{job_data.get('name', '?')}' must have "
                        "'command' or 'script_path'"
                    )

        # Build the YAML structure
        workflow_dict: dict[str, Any] = {"name": name}
        if args:
            workflow_dict["args"] = args
        if default_project:
            workflow_dict["default_project"] = default_project
        workflow_dict["jobs"] = jobs

        output_file = Path(output_path)
        output_file.parent.mkdir(parents=True, exist_ok=True)
        with open(output_file, "w") as f:
            yaml.dump(workflow_dict, f, default_flow_style=False, sort_keys=False)

        return ok(
            path=str(output_file.resolve()),
            name=name,
            job_count=len(jobs),
            job_names=[j["name"] for j in jobs],
            message=f"Workflow '{name}' created at {output_path}",
        )
    except Exception as e:
        return err(str(e))


@mcp.tool()
def validate_workflow(yaml_path: str) -> dict[str, Any]:
    """Validate a workflow YAML file for correctness.

    Checks for valid YAML syntax, correct job structure, dependency resolution,
    and circular dependency detection.

    Args:
        yaml_path: Path to the YAML workflow file to validate
    """
    try:
        from srunx.runtime.workflow.runner import WorkflowRunner

        runner = WorkflowRunner.from_yaml(yaml_path)
        runner.workflow.validate()

        jobs_info = []
        for job in runner.workflow.jobs:
            info: dict[str, Any] = {"name": job.name, "depends_on": job.depends_on}
            if hasattr(job, "command"):
                cmd = job.command
                info["command"] = cmd if isinstance(cmd, str) else " ".join(cmd or [])
            if hasattr(job, "script_path"):
                info["script_path"] = job.script_path
            jobs_info.append(info)

        return ok(
            name=runner.workflow.name,
            valid=True,
            job_count=len(runner.workflow.jobs),
            jobs=jobs_info,
        )
    except Exception as e:
        return err(str(e))


def _enforce_shell_script_roots(workflow: Any, profile: Any) -> None:
    """Guard ShellJob script paths for the MCP transport.

    Mirrors the Web router's ``_enforce_shell_script_roots`` semantics:
    every ShellJob's ``script_path`` must sit under one of the profile's
    mount ``local`` roots. Raises :class:`ValueError` (caller converts to
    :func:`err`) instead of ``HTTPException`` because MCP has no HTTP
    layer.
    """
    from srunx.runtime.security import find_shell_script_violation

    allowed_roots = [Path(m.local).resolve() for m in (profile.mounts or [])]
    violation = find_shell_script_violation(workflow, allowed_roots)
    if violation is not None:
        raise ValueError(
            f"Script path '{violation.script_path}' is outside allowed directories"
        )


def _resolve_mount_context(
    mount: str,
) -> tuple[Any, SubmissionRenderContext, Any]:
    """Resolve ``(adapter, submission_context, profile)`` for an MCP mount run.

    Returns the :class:`SlurmSSHAdapter` for the active SSH profile, a
    :class:`SubmissionRenderContext` pinned to ``mount.remote`` as the
    default work dir, and the profile object itself (needed by the
    ShellJob guard). Raises :class:`ValueError` on any misconfiguration —
    no current profile, unknown mount name, etc.
    """
    from srunx.runtime.rendering import SubmissionRenderContext
    from srunx.slurm.ssh import SlurmSSHAdapter
    from srunx.ssh.core.config import ConfigManager

    cm = ConfigManager()
    profile = cm.get_current_profile()
    profile_name = cm.get_current_profile_name()
    if profile is None:
        raise ValueError(
            "mount requires a current SSH profile; configure one via "
            "`srunx ssh profile add` and select it with `srunx ssh profile use`"
        )

    mount_found = next(
        (m for m in (profile.mounts or []) if m.name == mount),
        None,
    )
    if mount_found is None:
        raise ValueError(f"Mount '{mount}' not found in profile '{profile_name}'")

    adapter = SlurmSSHAdapter(profile_name=profile_name)
    context = SubmissionRenderContext(
        mount_name=mount,
        mounts=tuple(profile.mounts),
        default_work_dir=mount_found.remote,
    )
    return adapter, context, profile


@mcp.tool()
def run_workflow(
    yaml_path: str,
    from_job: str | None = None,
    to_job: str | None = None,
    single_job: str | None = None,
    dry_run: bool = False,
    args: dict[str, Any] | None = None,
    sweep: dict[str, Any] | None = None,
    mount: str | None = None,
) -> dict[str, Any]:
    """Execute a SLURM workflow from a YAML file.

    Jobs are executed in dependency order - independent jobs run in parallel,
    dependent jobs wait for their prerequisites to complete.

    Args:
        yaml_path: Path to the YAML workflow file
        from_job: Start execution from this job (skip earlier jobs)
        to_job: Stop execution at this job (skip later jobs)
        single_job: Execute only this specific job, ignoring dependencies
        dry_run: If true, show what would be executed without actually running
        args: Optional mapping merged over the YAML ``args`` section before
            Jinja rendering. ``python:`` prefix values are rejected.
        sweep: Optional sweep spec: ``{"matrix": {...}, "fail_fast": bool,
            "max_parallel": int}``. When present, the request goes through
            :class:`SweepOrchestrator` and the response contains
            ``sweep_run_id``.
        mount: Optional mount name from the active SSH profile. When
            provided, the run is routed through the configured cluster
            adapter with mount-aware path translation for ``work_dir`` /
            ``log_dir``. When omitted (default), the run stays on the
            local SLURM client — same behaviour as pre-5a.
    """
    try:
        from srunx.runtime.workflow.runner import WorkflowRunner
        from srunx.slurm.ssh_executor import SlurmSSHExecutorPool

        if args is not None:
            reject_python_prefix(args, source="args")

        # Resolve the SSH adapter + render context + profile when the caller
        # opted into mount-aware mode. ``submission_context=None`` keeps the
        # legacy "local SLURM only" path intact when ``mount`` is omitted.
        submission_context: SubmissionRenderContext | None = None
        adapter = None
        profile = None
        if mount is not None:
            try:
                adapter, submission_context, profile = _resolve_mount_context(mount)
            except ValueError as exc:
                return err(str(exc))

        if sweep is not None:
            from srunx.runtime.sweep import SweepSpec
            from srunx.runtime.sweep.orchestrator import SweepOrchestrator

            matrix = sweep.get("matrix") or {}
            if not isinstance(matrix, dict):
                return err("sweep.matrix must be a mapping")
            reject_python_prefix(matrix, source="sweep.matrix")

            try:
                sweep_spec = SweepSpec(
                    matrix=matrix,
                    fail_fast=bool(sweep.get("fail_fast", False)),
                    max_parallel=int(sweep.get("max_parallel", 4)),
                )
            except (ValueError, TypeError) as exc:
                # ValueError covers pydantic.ValidationError (Pydantic v2)
                # and int() conversion failures; TypeError covers int(None).
                return err(f"invalid sweep spec: {exc}")

            yaml_file = Path(yaml_path)
            with open(yaml_file, encoding="utf-8") as f:
                raw = yaml.safe_load(f) or {}
            if not isinstance(raw, dict):
                raw = {}

            # Mount-aware branch: apply the same ShellJob script-root guard
            # the Web sweep path runs, and submit every cell through a
            # bounded :class:`SlurmSSHExecutorPool` so concurrent cells
            # share a small set of SSH sessions against the cluster.
            pool: SlurmSSHExecutorPool | None = None
            executor_factory = None
            if adapter is not None and profile is not None:
                try:
                    guard_runner = WorkflowRunner.from_yaml(
                        yaml_file,
                        args_override=args or None,
                    )
                    _enforce_shell_script_roots(guard_runner.workflow, profile)
                except ValueError as exc:
                    return err(str(exc))
                pool_size = max(1, min(sweep_spec.max_parallel, 8))
                pool = SlurmSSHExecutorPool(
                    adapter.connection_spec,
                    callbacks=[],
                    size=pool_size,
                )
                executor_factory = pool.lease

            orchestrator = SweepOrchestrator(
                workflow_yaml_path=yaml_file,
                workflow_data=raw,
                args_override=args or None,
                sweep_spec=sweep_spec,
                submission_source="mcp",
                executor_factory=executor_factory,
                submission_context=submission_context,
            )
            try:
                sweep_run = orchestrator.run()
                return ok(
                    sweep_run_id=sweep_run.id,
                    status=sweep_run.status,
                    cell_count=sweep_run.cell_count,
                    cells_completed=sweep_run.cells_completed,
                    cells_failed=sweep_run.cells_failed,
                    cells_cancelled=sweep_run.cells_cancelled,
                )
            finally:
                if pool is not None:
                    pool.close()

        # Non-sweep path — mount-aware branch routes submission through a
        # small SSH executor pool so WorkflowRunner's parallel execution
        # (``ThreadPoolExecutor(max_workers=8)``) can submit concurrently
        # against the remote cluster. The pool is closed in ``finally``.
        pool = None
        executor_factory = None
        if adapter is not None and profile is not None:
            try:
                guard_runner = WorkflowRunner.from_yaml(
                    yaml_path,
                    args_override=args or None,
                )
                _enforce_shell_script_roots(guard_runner.workflow, profile)
            except ValueError as exc:
                return err(str(exc))
            pool = SlurmSSHExecutorPool(
                adapter.connection_spec,
                callbacks=[],
                size=8,
            )
            executor_factory = pool.lease

        try:
            runner = WorkflowRunner.from_yaml(
                yaml_path,
                args_override=args or None,
                executor_factory=executor_factory,
                submission_context=submission_context,
            )

            if dry_run:
                jobs_to_execute = runner._get_jobs_to_execute(
                    from_job, to_job, single_job
                )
                jobs_info = []
                for job in jobs_to_execute:
                    info: dict[str, Any] = {
                        "name": job.name,
                        "depends_on": job.depends_on,
                    }
                    if hasattr(job, "command"):
                        cmd = job.command
                        info["command"] = (
                            cmd if isinstance(cmd, str) else " ".join(cmd or [])
                        )
                    if hasattr(job, "script_path"):
                        info["script_path"] = job.script_path
                    jobs_info.append(info)
                return ok(
                    dry_run=True,
                    workflow=runner.workflow.name,
                    jobs_to_execute=jobs_info,
                    count=len(jobs_info),
                )

            results = runner.run(
                from_job=from_job, to_job=to_job, single_job=single_job
            )
            completed = {}
            for job_name, job in results.items():
                completed[job_name] = {
                    "job_id": job.job_id,
                    "status": job._status.value,
                }
            return ok(
                workflow=runner.workflow.name,
                results=completed,
                all_completed=all(
                    v["status"] == "COMPLETED" for v in completed.values()
                ),
            )
        finally:
            if pool is not None:
                pool.close()
    except Exception as e:
        return err(str(e))


@mcp.tool()
def list_workflows(directory: str = ".") -> dict[str, Any]:
    """List workflow YAML files in a directory.

    Scans the directory for YAML files that contain a valid srunx workflow
    structure (must have 'name' and 'jobs' keys).

    Args:
        directory: Directory to search for workflow files (default: current directory)
    """
    try:
        search_dir = Path(directory).resolve()
        yaml_files = list(search_dir.glob("**/*.yaml")) + list(
            search_dir.glob("**/*.yml")
        )

        workflows = []
        for yf in yaml_files:
            parts = yf.relative_to(search_dir).parts
            if any(
                p.startswith(".") or p in ("node_modules", ".venv", "__pycache__")
                for p in parts
            ):
                continue
            try:
                with open(yf) as f:
                    data = yaml.safe_load(f)
                if isinstance(data, dict) and "jobs" in data:
                    workflows.append(
                        {
                            "path": str(yf),
                            "name": data.get("name", "unnamed"),
                            "job_count": len(data["jobs"]),
                            "job_names": [j.get("name", "?") for j in data["jobs"]],
                        }
                    )
            except Exception:
                continue

        return ok(workflows=workflows, count=len(workflows))
    except Exception as e:
        return err(str(e))


@mcp.tool()
def get_workflow(yaml_path: str) -> dict[str, Any]:
    """Read and parse a workflow YAML file, returning its full structure.

    Args:
        yaml_path: Path to the YAML workflow file
    """
    try:
        path = Path(yaml_path)
        if not path.exists():
            return err(f"File not found: {yaml_path}")

        with open(path) as f:
            data = yaml.safe_load(f)

        if not isinstance(data, dict) or "jobs" not in data:
            return err("File is not a valid srunx workflow (missing 'jobs' key)")

        from srunx.runtime.workflow.runner import WorkflowRunner

        runner = WorkflowRunner.from_yaml(yaml_path)

        jobs_detail = []
        for job in runner.workflow.jobs:
            info: dict[str, Any] = {
                "name": job.name,
                "depends_on": job.depends_on,
                "retry": job.retry,
                "retry_delay": job.retry_delay,
            }
            from srunx.domain import Job, ShellJob

            if isinstance(job, Job):
                cmd = job.command
                info["command"] = cmd if isinstance(cmd, str) else " ".join(cmd or [])
                info["resources"] = job.resources.model_dump()
                info["environment"] = {
                    "conda": job.environment.conda,
                    "venv": job.environment.venv,
                    "env_vars": job.environment.env_vars,
                }
            elif isinstance(job, ShellJob):
                info["script_path"] = job.script_path
                info["script_vars"] = job.script_vars
            jobs_detail.append(info)

        return ok(
            name=runner.workflow.name,
            args=data.get("args"),
            default_project=data.get("default_project"),
            jobs=jobs_detail,
            raw_yaml=path.read_text(),
        )
    except Exception as e:
        return err(str(e))
