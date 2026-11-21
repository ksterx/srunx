"""MCP tools for SLURM job lifecycle: submit / list / status / cancel / logs.

Every tool takes an explicit ``cluster`` name (see
:mod:`srunx.mcp.transport`); there is no implicit default. All clusters run
through the shared
``resolve_transport`` handle, so local and SSH share one code path
(``rt.job_ops``) instead of forking into hand-rolled ``squeue`` / ``scancel``
shell calls.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, cast

from srunx.mcp.app import mcp
from srunx.mcp.helpers import (
    err,
    job_to_dict,
    ok,
    validate_job_id,
)
from srunx.mcp.transport import mcp_transport


def _job_id_as_int(job_id: str) -> int:
    """Convert a validated job id to int for the int-typed status/cancel ops.

    ``validate_job_id`` accepts array task ids like ``12345_7``, but the
    status / cancel / queue_by_ids Protocol is int-only (the SSH adapter even
    re-applies ``int()`` internally), and ``int("12345_7")`` silently yields
    ``123457`` — addressing a *different* job. Reject array ids explicitly so a
    status/cancel never lands on the wrong job. (Full array-task addressing
    would require widening the int-based slurm Protocol — out of scope here;
    log retrieval keeps array support since it passes the id through as a
    string.)
    """
    if "_" in job_id:
        raise ValueError(
            f"Array task ID '{job_id}' is not supported for status/cancel "
            "(the job-operations API is numeric-id only); pass the plain "
            "numeric job ID."
        )
    return int(job_id)


@mcp.tool()
def submit_job(
    command: str,
    name: str = "job",
    nodes: int | None = None,
    gpus_per_node: int | None = None,
    ntasks_per_node: int | None = None,
    cpus_per_task: int | None = None,
    memory_per_node: str | None = None,
    time_limit: str | None = None,
    partition: str | None = None,
    nodelist: str | None = None,
    conda: str | None = None,
    venv: str | None = None,
    env_vars: dict[str, str] | None = None,
    log_dir: str | None = None,
    work_dir: str | None = None,
    cluster: str | None = None,
    project: str | None = None,
) -> dict[str, Any]:
    """Submit a SLURM job.

    Omitted resource / environment arguments come from the cluster's
    configured defaults (user config, and the project's when ``project`` is
    given), not from fixed values.

    Args:
        command: Shell command to execute (e.g. "python train.py --epochs 100")
        name: Job name for identification in SLURM queue
        nodes: Number of compute nodes to allocate
        gpus_per_node: Number of GPUs per node (0 for CPU-only)
        ntasks_per_node: Number of tasks per node
        cpus_per_task: Number of CPUs per task
        memory_per_node: Memory per node (e.g. "32GB", "64G")
        time_limit: Wall time limit (e.g. "4:00:00", "1-00:00:00")
        partition: SLURM partition name (e.g. "gpu", "cpu")
        nodelist: Specific nodes to use (e.g. "node001,node002")
        conda: Conda environment name to activate before running
        venv: Path to Python virtual environment to activate
        env_vars: Additional environment variables as key-value pairs
        log_dir: Directory for stdout/stderr log files
        work_dir: Working directory for the job (defaults to cwd)
        cluster: Cluster name (required; see ``list_clusters``).
        project: Project root whose ``.srunx/config.toml`` defaults apply.
    """
    try:
        from srunx.domain import Job, JobEnvironment, JobResource

        def given(**values: Any) -> dict[str, Any]:
            return {k: v for k, v in values.items() if v is not None}

        with mcp_transport(cluster, project=project) as rt:
            resource = JobResource.model_validate(
                given(
                    nodes=nodes,
                    gpus_per_node=gpus_per_node,
                    ntasks_per_node=ntasks_per_node,
                    cpus_per_task=cpus_per_task,
                    memory_per_node=memory_per_node,
                    time_limit=time_limit,
                    partition=partition,
                    nodelist=nodelist,
                )
            )
            environment = JobEnvironment.model_validate(
                given(conda=conda, venv=venv, env_vars=env_vars)
            )
            if rt.transport_type == "ssh" and not work_dir:
                return err(
                    "work_dir is required for SSH job submission "
                    "(local cwd does not exist on the remote cluster)"
                )

            job = Job.model_validate(
                {
                    "name": name,
                    "command": command,
                    "resources": resource,
                    "environment": environment,
                    **given(log_dir=log_dir),
                    "work_dir": work_dir or str(Path.cwd()),
                }
            )

            # One submit path for both transports. The SSH adapter renders
            # the script, applies mount translation via submission_context,
            # uploads, and runs sbatch; the local client runs sbatch
            # directly. MCP no longer hand-rolls Jinja rendering.
            result = rt.job_ops.submit(job, submission_context=rt.submission_context)
            return ok(
                job_id=result.job_id,
                name=result.name,
                status=job_to_dict(result)["status"],
            )
    except Exception as e:
        return err(str(e))


@mcp.tool()
def list_jobs(cluster: str | None = None) -> dict[str, Any]:
    """List SLURM jobs in the queue (all users, like ``squeue``).

    Args:
        cluster: Cluster name (required; see ``list_clusters``).
    """
    try:
        with mcp_transport(cluster) as rt:
            queued = rt.job_ops.queue()
            return ok(
                jobs=[job_to_dict(j) for j in queued],
                count=len(queued),
            )
    except Exception as e:
        return err(str(e))


@mcp.tool()
def get_job_status(job_id: str, cluster: str | None = None) -> dict[str, Any]:
    """Get the status of a specific SLURM job.

    Args:
        job_id: SLURM job ID to check
        cluster: Cluster name (required; see ``list_clusters``).
    """
    try:
        validate_job_id(job_id)
        with mcp_transport(cluster) as rt:
            job = rt.job_ops.status(_job_id_as_int(job_id))
            # job_to_dict already carries job_id; don't double-pass it.
            return ok(**job_to_dict(job))
    except Exception as e:
        return err(str(e))


@mcp.tool()
def cancel_job(job_id: str, cluster: str | None = None) -> dict[str, Any]:
    """Cancel a running or pending SLURM job.

    Args:
        job_id: SLURM job ID to cancel
        cluster: Cluster name (required; see ``list_clusters``).
    """
    try:
        validate_job_id(job_id)
        with mcp_transport(cluster) as rt:
            rt.job_ops.cancel(_job_id_as_int(job_id))
            return ok(job_id=job_id, message="Job cancelled")
    except Exception as e:
        return err(str(e))


@mcp.tool()
def get_job_logs(
    job_id: str,
    job_name: str | None = None,
    cluster: str | None = None,
) -> dict[str, Any]:
    """Get stdout/stderr logs for a SLURM job.

    Args:
        job_id: SLURM job ID
        job_name: Optional job name to help locate log files
        cluster: Cluster name (required; see ``list_clusters``).
    """
    try:
        validate_job_id(job_id)
        with mcp_transport(cluster) as rt:
            # Full-text log retrieval is the WorkflowJobExecutor face, which
            # both the local client and the SSH adapter implement. It's not
            # on the JobOperations Protocol that ``job_ops`` is typed as, so
            # cast to the protocol that declares it (both concrete clients
            # satisfy it).
            from srunx.slurm.protocols import WorkflowJobExecutor

            executor = cast(WorkflowJobExecutor, rt.job_ops)
            details = executor.get_job_output_detailed(job_id, job_name)
            return ok(
                job_id=job_id,
                stdout=details.get("output", ""),
                stderr=details.get("error", ""),
                log_files=details.get("found_files", []),
            )
    except Exception as e:
        return err(str(e))
