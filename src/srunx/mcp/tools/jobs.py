"""MCP tools for SLURM job lifecycle: submit / list / status / cancel / logs."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from srunx.mcp.app import mcp
from srunx.mcp.helpers import (
    err,
    get_ssh_client,
    job_to_dict,
    ok,
    validate_job_id,
)


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
    use_ssh: bool = False,
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
        use_ssh: If true, submit via SSH to remote SLURM cluster
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
        if use_ssh and not work_dir:
            return err(
                "work_dir is required for SSH job submission "
                "(local cwd does not exist on remote cluster)"
            )

        job = Job(
            name=name,
            command=command,
            resources=resource,
            environment=environment,
            log_dir=log_dir,
            work_dir=work_dir or str(Path.cwd()),
        )

        if use_ssh:
            import jinja2

            from srunx.runtime.rendering import _build_environment_setup
            from srunx.runtime.templates import get_template_path

            template_path = get_template_path("base")
            with open(template_path) as f:
                tmpl = jinja2.Template(f.read(), undefined=jinja2.StrictUndefined)

            env_setup, srun_args, launch_prefix = _build_environment_setup(environment)
            cmd_str = command if isinstance(command, str) else " ".join(command)
            script_content = tmpl.render(
                job_name=name,
                command=cmd_str,
                log_dir=log_dir,
                work_dir=work_dir,
                environment_setup=env_setup,
                srun_args=srun_args,
                launch_prefix=launch_prefix,
                container=environment.container,
                **resource.model_dump(),
            )
            ssh_client = get_ssh_client()
            with ssh_client:
                slurm_job = ssh_client.submit_sbatch_job(script_content, job_name=name)
                if slurm_job is None:
                    return err("SSH job submission failed")
                return ok(
                    job_id=slurm_job.job_id, name=slurm_job.name, status="PENDING"
                )

        from srunx.slurm.local import Slurm

        slurm = Slurm()
        result = slurm.submit(job)
        return ok(job_id=result.job_id, name=result.name, status=result._status.value)
    except Exception as e:
        return err(str(e))


@mcp.tool()
def list_jobs(use_ssh: bool = False) -> dict[str, Any]:
    """List current user's SLURM jobs in the queue.

    Args:
        use_ssh: If true, query jobs via SSH on remote cluster
    """
    try:
        if use_ssh:
            ssh_client = get_ssh_client()
            with ssh_client:
                stdout, stderr, rc = ssh_client._execute_slurm_command(
                    'squeue --me --format "%.18i %.9P %.30j %.12u %.8T %.10M %.9l %.6D %R %b" --noheader'
                )
                if rc != 0:
                    return err(f"squeue failed: {stderr}")
                jobs = []
                for line in stdout.strip().splitlines():
                    parts = line.split()
                    if len(parts) >= 5:
                        jobs.append(
                            {
                                "job_id": parts[0],
                                "partition": parts[1],
                                "name": parts[2],
                                "user": parts[3],
                                "status": parts[4],
                                "time": parts[5] if len(parts) > 5 else None,
                                "nodes": parts[7] if len(parts) > 7 else None,
                            }
                        )
                return ok(jobs=jobs, count=len(jobs))

        from srunx.slurm.local import Slurm

        slurm = Slurm()
        queued = slurm.queue()
        return ok(
            jobs=[job_to_dict(j) for j in queued],
            count=len(queued),
        )
    except Exception as e:
        return err(str(e))


@mcp.tool()
def get_job_status(job_id: str, use_ssh: bool = False) -> dict[str, Any]:
    """Get the status of a specific SLURM job.

    Args:
        job_id: SLURM job ID to check
        use_ssh: If true, query via SSH on remote cluster
    """
    try:
        validate_job_id(job_id)
        if use_ssh:
            ssh_client = get_ssh_client()
            with ssh_client:
                status = ssh_client.get_job_status(str(job_id))
                if status in ("ERROR", "NOT_FOUND"):
                    return err(f"Job {job_id}: {status}")
                return ok(job_id=job_id, status=status)

        from srunx.slurm.local import Slurm

        job = Slurm.retrieve(int(job_id))
        return ok(job_id=job_id, **job_to_dict(job))
    except Exception as e:
        return err(str(e))


@mcp.tool()
def cancel_job(job_id: str, use_ssh: bool = False) -> dict[str, Any]:
    """Cancel a running or pending SLURM job.

    Args:
        job_id: SLURM job ID to cancel
        use_ssh: If true, cancel via SSH on remote cluster
    """
    try:
        validate_job_id(job_id)
        if use_ssh:
            ssh_client = get_ssh_client()
            with ssh_client:
                stdout, stderr, rc = ssh_client._execute_slurm_command(
                    f"scancel {job_id}"
                )
                if rc != 0:
                    return err(f"scancel failed: {stderr}")
                return ok(job_id=job_id, message="Job cancelled")

        from srunx.slurm.local import Slurm

        slurm = Slurm()
        slurm.cancel(int(job_id))
        return ok(job_id=job_id, message="Job cancelled")
    except Exception as e:
        return err(str(e))


@mcp.tool()
def get_job_logs(
    job_id: str,
    job_name: str | None = None,
    use_ssh: bool = False,
) -> dict[str, Any]:
    """Get stdout/stderr logs for a SLURM job.

    Args:
        job_id: SLURM job ID
        job_name: Optional job name to help locate log files
        use_ssh: If true, fetch logs via SSH from remote cluster
    """
    try:
        validate_job_id(job_id)
        if use_ssh:
            ssh_client = get_ssh_client()
            with ssh_client:
                stdout, stderr, _, _ = ssh_client.get_job_output(str(job_id), job_name)
                if not stdout and not stderr:
                    return err(f"No logs found for job {job_id}")
                return ok(job_id=job_id, stdout=stdout, stderr=stderr)

        from srunx.slurm.local import Slurm

        slurm = Slurm()
        details = slurm.get_job_output_detailed(job_id, job_name)
        return ok(
            job_id=job_id,
            stdout=details.get("output", ""),
            stderr=details.get("error", ""),
            log_files=details.get("found_files", []),
        )
    except Exception as e:
        return err(str(e))
