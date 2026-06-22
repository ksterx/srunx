"""MCP tools for SLURM job lifecycle: submit / list / status / cancel / logs.

Every tool selects its cluster through the single ``transport`` argument
(see :mod:`srunx.mcp.transport`): ``None`` / ``"local"`` -> local SLURM,
``"<profile>"`` -> that SSH profile. Cluster choice is never a boolean
toggle and never an implicit current-profile fallback — an MCP caller
reaches a remote only by naming it. All transports run through the shared
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


@mcp.tool()
def submit_job(
    command: str,
    name: str = "job",
    nodes: int = 1,
    gpus_per_node: int = 0,
    ntasks_per_node: int = 1,
    cpus_per_task: int = 1,
    memory_per_node: str | None = None,
    time_limit: str | None = None,
    partition: str | None = None,
    nodelist: str | None = None,
    conda: str | None = None,
    venv: str | None = None,
    env_vars: dict[str, str] | None = None,
    log_dir: str = "logs",
    work_dir: str | None = None,
    transport: str | None = None,
) -> dict[str, Any]:
    """Submit a SLURM job.

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
        transport: Cluster selector — omit / "local" for local SLURM, or an
            SSH profile name to submit to that remote cluster.
    """
    try:
        from srunx.domain import Job, JobEnvironment, JobResource

        resource = JobResource(
            nodes=nodes,
            gpus_per_node=gpus_per_node,
            ntasks_per_node=ntasks_per_node,
            cpus_per_task=cpus_per_task,
            memory_per_node=memory_per_node,
            time_limit=time_limit,
            partition=partition,
            nodelist=nodelist,
        )
        environment = JobEnvironment(
            conda=conda,
            venv=venv,
            env_vars=env_vars or {},
        )

        with mcp_transport(transport) as rt:
            if rt.transport_type == "ssh" and not work_dir:
                return err(
                    "work_dir is required for SSH job submission "
                    "(local cwd does not exist on the remote cluster)"
                )

            job = Job(
                name=name,
                command=command,
                resources=resource,
                environment=environment,
                log_dir=log_dir,
                work_dir=work_dir or str(Path.cwd()),
            )

            # One submit path for both transports. The SSH adapter renders
            # the script, applies mount translation via submission_context,
            # uploads, and runs sbatch; the local client runs sbatch
            # directly. MCP no longer hand-rolls Jinja rendering.
            result = rt.job_ops.submit(
                job, submission_context=rt.submission_context
            )
            return ok(
                job_id=result.job_id,
                name=result.name,
                status=job_to_dict(result)["status"],
            )
    except Exception as e:
        return err(str(e))


@mcp.tool()
def list_jobs(transport: str | None = None) -> dict[str, Any]:
    """List SLURM jobs in the queue (all users, like ``squeue``).

    Args:
        transport: Cluster selector — omit / "local" for local SLURM, or an
            SSH profile name to query that remote cluster.
    """
    try:
        with mcp_transport(transport) as rt:
            queued = rt.job_ops.queue()
            return ok(
                jobs=[job_to_dict(j) for j in queued],
                count=len(queued),
            )
    except Exception as e:
        return err(str(e))


@mcp.tool()
def get_job_status(job_id: str, transport: str | None = None) -> dict[str, Any]:
    """Get the status of a specific SLURM job.

    Args:
        job_id: SLURM job ID to check
        transport: Cluster selector — omit / "local" for local SLURM, or an
            SSH profile name to query that remote cluster.
    """
    try:
        validate_job_id(job_id)
        with mcp_transport(transport) as rt:
            job = rt.job_ops.status(int(job_id))
            # job_to_dict already carries job_id; don't double-pass it.
            return ok(**job_to_dict(job))
    except Exception as e:
        return err(str(e))


@mcp.tool()
def cancel_job(job_id: str, transport: str | None = None) -> dict[str, Any]:
    """Cancel a running or pending SLURM job.

    Args:
        job_id: SLURM job ID to cancel
        transport: Cluster selector — omit / "local" for local SLURM, or an
            SSH profile name to cancel on that remote cluster.
    """
    try:
        validate_job_id(job_id)
        with mcp_transport(transport) as rt:
            rt.job_ops.cancel(int(job_id))
            return ok(job_id=job_id, message="Job cancelled")
    except Exception as e:
        return err(str(e))


@mcp.tool()
def get_job_logs(
    job_id: str,
    job_name: str | None = None,
    transport: str | None = None,
) -> dict[str, Any]:
    """Get stdout/stderr logs for a SLURM job.

    Args:
        job_id: SLURM job ID
        job_name: Optional job name to help locate log files
        transport: Cluster selector — omit / "local" for local SLURM, or an
            SSH profile name to fetch logs from that remote cluster.
    """
    try:
        validate_job_id(job_id)
        with mcp_transport(transport) as rt:
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
