"""MCP server exposing srunx SLURM operations as tools for Claude Code."""

from __future__ import annotations

import re
import sys
from pathlib import Path
from typing import Any

import yaml  # type: ignore

try:
    from mcp.server.fastmcp import FastMCP
except ModuleNotFoundError:
    sys.stderr.write(
        "srunx-mcp: the 'mcp' package is not installed in this Python "
        "environment.\n"
        "\n"
        "Fix:\n"
        "  1. Preferred (zero-install):\n"
        "       uvx --from 'srunx[mcp]' srunx-mcp\n"
        "     Register it with Claude Code as:\n"
        "       claude mcp add --scope user srunx -- "
        "uvx --from 'srunx[mcp]' srunx-mcp\n"
        "\n"
        "  2. Globally installed binary:\n"
        "       uv tool install --force --with 'mcp[cli]' srunx\n"
        "     then register:\n"
        "       claude mcp add --scope user srunx -- srunx-mcp\n"
        "\n"
        "Note: 'uv run --extra mcp srunx-mcp' resolves extras against the\n"
        "current working directory's pyproject.toml, so it only works when\n"
        "launched from inside the srunx source tree.\n"
    )
    sys.exit(1)

mcp = FastMCP(
    "srunx",
    instructions=(
        "SLURM job management tools. Use these to submit jobs, monitor status, "
        "manage workflows, check GPU resources, and sync files to remote clusters. "
        "Most operations require either local SLURM access or a configured SSH profile."
    ),
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _ok(data: Any = None, **kwargs: Any) -> dict[str, Any]:
    result: dict[str, Any] = {"success": True}
    if data is not None:
        result["data"] = data
    result.update(kwargs)
    return result


def _err(message: str) -> dict[str, Any]:
    return {"success": False, "error": message}


_SAFE_JOB_ID = re.compile(r"^\d+(_\d+)?$")
_SAFE_PARTITION = re.compile(r"^[a-zA-Z0-9_\-]+$")


def _validate_job_id(job_id: str) -> str:
    """Validate that job_id is a numeric SLURM job ID (e.g. '12345' or '12345_1')."""
    if not _SAFE_JOB_ID.match(job_id):
        raise ValueError(f"Invalid job ID: {job_id!r}. Must be numeric (e.g. '12345').")
    return job_id


def _validate_partition(partition: str) -> str:
    """Validate that partition name contains only safe characters."""
    if not _SAFE_PARTITION.match(partition):
        raise ValueError(
            f"Invalid partition name: {partition!r}. "
            "Must contain only alphanumeric, underscore, or hyphen."
        )
    return partition


def _job_to_dict(job: Any) -> dict[str, Any]:
    """Convert a BaseJob / Job / ShellJob to a JSON-serializable dict."""
    d: dict[str, Any] = {
        "name": job.name,
        "job_id": job.job_id,
        "status": job._status.value if hasattr(job, "_status") else "UNKNOWN",
    }
    for field in (
        "partition",
        "user",
        "elapsed_time",
        "nodes",
        "nodelist",
        "cpus",
        "gpus",
    ):
        val = getattr(job, field, None)
        if val is not None:
            d[field] = val
    if hasattr(job, "command"):
        cmd = job.command
        d["command"] = cmd if isinstance(cmd, str) else " ".join(cmd or [])
    if hasattr(job, "script_path"):
        d["script_path"] = job.script_path
    return d


def _get_ssh_client() -> Any:
    """Get an SSHSlurmClient from the current SSH profile."""
    from srunx.ssh.core.config import ConfigManager

    cm = ConfigManager()
    profile_name = cm.get_current_profile_name()
    if not profile_name:
        raise RuntimeError(
            "No active SSH profile. Set one with: srunx ssh profile use <name>"
        )
    profile = cm.get_profile(profile_name)
    if not profile:
        raise RuntimeError(f"SSH profile '{profile_name}' not found")

    from srunx.ssh.core.client import SSHSlurmClient
    from srunx.ssh.core.ssh_config import SSHConfigParser

    # Resolve connection params: ssh_host (via ~/.ssh/config) or direct fields
    if profile.ssh_host:
        parser = SSHConfigParser()
        ssh_host = parser.get_host(profile.ssh_host)
        if not ssh_host:
            raise RuntimeError(
                f"SSH host '{profile.ssh_host}' not found in ~/.ssh/config"
            )
        client = SSHSlurmClient(
            hostname=ssh_host.hostname or profile.ssh_host,
            username=ssh_host.user or "",
            key_filename=ssh_host.identity_file,
            port=ssh_host.port or 22,
            proxy_jump=ssh_host.proxy_jump,
            env_vars=dict(profile.env_vars) if profile.env_vars else None,
        )
    else:
        # Resolve hostname via ~/.ssh/config if it's an alias
        resolved_hostname = profile.hostname
        resolved_key = profile.key_filename
        resolved_port = profile.port
        resolved_proxy = profile.proxy_jump

        parser = SSHConfigParser()
        ssh_host = parser.get_host(profile.hostname)
        if ssh_host and ssh_host.hostname:
            resolved_hostname = ssh_host.hostname
            if ssh_host.identity_file and not resolved_key:
                resolved_key = ssh_host.identity_file
            if ssh_host.port:
                resolved_port = ssh_host.port
            if ssh_host.proxy_jump and not resolved_proxy:
                resolved_proxy = ssh_host.proxy_jump

        client = SSHSlurmClient(
            hostname=resolved_hostname,
            username=profile.username,
            key_filename=resolved_key,
            port=resolved_port,
            proxy_jump=resolved_proxy,
            env_vars=dict(profile.env_vars) if profile.env_vars else None,
        )
    return client


# ---------------------------------------------------------------------------
# Job Management Tools
# ---------------------------------------------------------------------------


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
        from srunx.models import Job, JobEnvironment, JobResource

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
            return _err(
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
            from srunx.models import _build_environment_setup

            template_path = (
                Path(__file__).parent.parent / "templates" / "base.slurm.jinja"
            )

            import jinja2

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
                outputs_dir=None,
                job_outputs=job.outputs,
                dependency_names=[],
                **resource.model_dump(),
            )
            ssh_client = _get_ssh_client()
            with ssh_client:
                slurm_job = ssh_client.submit_sbatch_job(script_content, job_name=name)
                if slurm_job is None:
                    return _err("SSH job submission failed")
                return _ok(
                    job_id=slurm_job.job_id, name=slurm_job.name, status="PENDING"
                )
        else:
            from srunx.client import Slurm

            slurm = Slurm()
            result = slurm.submit(job)
            return _ok(
                job_id=result.job_id, name=result.name, status=result._status.value
            )
    except Exception as e:
        return _err(str(e))


@mcp.tool()
def list_jobs(use_ssh: bool = False) -> dict[str, Any]:
    """List current user's SLURM jobs in the queue.

    Args:
        use_ssh: If true, query jobs via SSH on remote cluster
    """
    try:
        if use_ssh:
            ssh_client = _get_ssh_client()
            with ssh_client:
                stdout, stderr, rc = ssh_client._execute_slurm_command(
                    'squeue --me --format "%.18i %.9P %.30j %.12u %.8T %.10M %.9l %.6D %R %b" --noheader'
                )
                if rc != 0:
                    return _err(f"squeue failed: {stderr}")
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
                return _ok(jobs=jobs, count=len(jobs))
        else:
            from srunx.client import Slurm

            slurm = Slurm()
            queued = slurm.queue()
            return _ok(
                jobs=[_job_to_dict(j) for j in queued],
                count=len(queued),
            )
    except Exception as e:
        return _err(str(e))


@mcp.tool()
def get_job_status(job_id: str, use_ssh: bool = False) -> dict[str, Any]:
    """Get the status of a specific SLURM job.

    Args:
        job_id: SLURM job ID to check
        use_ssh: If true, query via SSH on remote cluster
    """
    try:
        _validate_job_id(job_id)
        if use_ssh:
            ssh_client = _get_ssh_client()
            with ssh_client:
                status = ssh_client.get_job_status(str(job_id))
                if status in ("ERROR", "NOT_FOUND"):
                    return _err(f"Job {job_id}: {status}")
                return _ok(job_id=job_id, status=status)
        else:
            from srunx.client import Slurm

            job = Slurm.retrieve(int(job_id))
            return _ok(job_id=job_id, **_job_to_dict(job))
    except Exception as e:
        return _err(str(e))


@mcp.tool()
def cancel_job(job_id: str, use_ssh: bool = False) -> dict[str, Any]:
    """Cancel a running or pending SLURM job.

    Args:
        job_id: SLURM job ID to cancel
        use_ssh: If true, cancel via SSH on remote cluster
    """
    try:
        _validate_job_id(job_id)
        if use_ssh:
            ssh_client = _get_ssh_client()
            with ssh_client:
                stdout, stderr, rc = ssh_client._execute_slurm_command(
                    f"scancel {job_id}"
                )
                if rc != 0:
                    return _err(f"scancel failed: {stderr}")
                return _ok(job_id=job_id, message="Job cancelled")
        else:
            from srunx.client import Slurm

            slurm = Slurm()
            slurm.cancel(int(job_id))
            return _ok(job_id=job_id, message="Job cancelled")
    except Exception as e:
        return _err(str(e))


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
        _validate_job_id(job_id)
        if use_ssh:
            ssh_client = _get_ssh_client()
            with ssh_client:
                stdout, stderr, _, _ = ssh_client.get_job_output(str(job_id), job_name)
                if not stdout and not stderr:
                    return _err(f"No logs found for job {job_id}")
                return _ok(job_id=job_id, stdout=stdout, stderr=stderr)
        else:
            from srunx.client import Slurm

            slurm = Slurm()
            details = slurm.get_job_output_detailed(job_id, job_name)
            return _ok(
                job_id=job_id,
                stdout=details.get("output", ""),
                stderr=details.get("error", ""),
                log_files=details.get("found_files", []),
            )
    except Exception as e:
        return _err(str(e))


# ---------------------------------------------------------------------------
# Resource Tools
# ---------------------------------------------------------------------------


@mcp.tool()
def get_resources(
    partition: str | None = None, use_ssh: bool = False
) -> dict[str, Any]:
    """Get current GPU and node resource availability on the SLURM cluster.

    Args:
        partition: Specific partition to check (None for all partitions)
        use_ssh: If true, query resources via SSH on remote cluster
    """
    try:
        if partition:
            _validate_partition(partition)
        if use_ssh:
            ssh_client = _get_ssh_client()
            with ssh_client:
                partition_flag = f"-p {partition}" if partition else ""
                stdout, stderr, rc = ssh_client._execute_slurm_command(
                    f'sinfo {partition_flag} -o "%n %G %T %P" --noheader'
                )
                if rc != 0:
                    return _err(f"sinfo failed: {stderr}")
                return _ok(partition=partition, raw_output=stdout.strip())
        else:
            from srunx.monitor.resource_monitor import ResourceMonitor

            monitor = ResourceMonitor(min_gpus=0, partition=partition)
            snapshot = monitor.get_partition_resources()
            return _ok(
                partition=snapshot.partition,
                total_gpus=snapshot.total_gpus,
                gpus_in_use=snapshot.gpus_in_use,
                gpus_available=snapshot.gpus_available,
                gpu_utilization=round(snapshot.gpu_utilization, 3),
                jobs_running=snapshot.jobs_running,
                nodes_total=snapshot.nodes_total,
                nodes_idle=snapshot.nodes_idle,
                nodes_down=snapshot.nodes_down,
            )
    except Exception as e:
        return _err(str(e))


# ---------------------------------------------------------------------------
# Workflow Tools
# ---------------------------------------------------------------------------


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
        from srunx.models import Workflow
        from srunx.runner import WorkflowRunner

        # Reject python: args for security (arbitrary code execution)
        if args:
            for arg_key, arg_val in args.items():
                if isinstance(arg_val, str) and "python:" in arg_val.lower():
                    return _err(
                        f"Arg '{arg_key}' contains 'python:' prefix which is not "
                        "allowed for security reasons. Use plain values or Jinja2 "
                        "templates instead."
                    )

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
                    return _err("Each job must have a 'name' field")
                if "command" not in job_data and "script_path" not in job_data:
                    return _err(
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

        return _ok(
            path=str(output_file.resolve()),
            name=name,
            job_count=len(jobs),
            job_names=[j["name"] for j in jobs],
            message=f"Workflow '{name}' created at {output_path}",
        )
    except Exception as e:
        return _err(str(e))


@mcp.tool()
def validate_workflow(yaml_path: str) -> dict[str, Any]:
    """Validate a workflow YAML file for correctness.

    Checks for valid YAML syntax, correct job structure, dependency resolution,
    and circular dependency detection.

    Args:
        yaml_path: Path to the YAML workflow file to validate
    """
    try:
        from srunx.runner import WorkflowRunner

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

        return _ok(
            name=runner.workflow.name,
            valid=True,
            job_count=len(runner.workflow.jobs),
            jobs=jobs_info,
        )
    except Exception as e:
        return _err(str(e))


@mcp.tool()
def run_workflow(
    yaml_path: str,
    from_job: str | None = None,
    to_job: str | None = None,
    single_job: str | None = None,
    dry_run: bool = False,
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
    """
    try:
        from srunx.runner import WorkflowRunner

        runner = WorkflowRunner.from_yaml(yaml_path)

        if dry_run:
            jobs_to_execute = runner._get_jobs_to_execute(from_job, to_job, single_job)
            jobs_info = []
            for job in jobs_to_execute:
                info: dict[str, Any] = {"name": job.name, "depends_on": job.depends_on}
                if hasattr(job, "command"):
                    cmd = job.command
                    info["command"] = (
                        cmd if isinstance(cmd, str) else " ".join(cmd or [])
                    )
                if hasattr(job, "script_path"):
                    info["script_path"] = job.script_path
                jobs_info.append(info)
            return _ok(
                dry_run=True,
                workflow=runner.workflow.name,
                jobs_to_execute=jobs_info,
                count=len(jobs_info),
            )

        results = runner.run(from_job=from_job, to_job=to_job, single_job=single_job)
        completed = {}
        for job_name, job in results.items():
            completed[job_name] = {
                "job_id": job.job_id,
                "status": job._status.value,
            }
        return _ok(
            workflow=runner.workflow.name,
            results=completed,
            all_completed=all(v["status"] == "COMPLETED" for v in completed.values()),
        )
    except Exception as e:
        return _err(str(e))


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

        # Filter to only files that look like srunx workflows
        workflows = []
        for yf in yaml_files:
            # Skip files in hidden dirs, node_modules, .venv
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

        return _ok(workflows=workflows, count=len(workflows))
    except Exception as e:
        return _err(str(e))


@mcp.tool()
def get_workflow(yaml_path: str) -> dict[str, Any]:
    """Read and parse a workflow YAML file, returning its full structure.

    Args:
        yaml_path: Path to the YAML workflow file
    """
    try:
        path = Path(yaml_path)
        if not path.exists():
            return _err(f"File not found: {yaml_path}")

        with open(path) as f:
            data = yaml.safe_load(f)

        if not isinstance(data, dict) or "jobs" not in data:
            return _err("File is not a valid srunx workflow (missing 'jobs' key)")

        # Also validate it can be parsed
        from srunx.runner import WorkflowRunner

        runner = WorkflowRunner.from_yaml(yaml_path)

        jobs_detail = []
        for job in runner.workflow.jobs:
            info: dict[str, Any] = {
                "name": job.name,
                "depends_on": job.depends_on,
                "retry": job.retry,
                "retry_delay": job.retry_delay,
            }
            from srunx.models import Job, ShellJob

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

        return _ok(
            name=runner.workflow.name,
            args=data.get("args"),
            default_project=data.get("default_project"),
            jobs=jobs_detail,
            raw_yaml=path.read_text(),
        )
    except Exception as e:
        return _err(str(e))


# ---------------------------------------------------------------------------
# File Sync Tools
# ---------------------------------------------------------------------------


@mcp.tool()
def sync_files(
    profile_name: str | None = None,
    mount_name: str | None = None,
    local_path: str | None = None,
    remote_path: str | None = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Sync files between local machine and remote SLURM cluster using rsync.

    Can sync using a configured mount point (profile_name + mount_name),
    or using explicit paths (local_path + remote_path).

    Args:
        profile_name: SSH profile name (uses current profile if not specified)
        mount_name: Mount point name from the SSH profile to sync
        local_path: Local directory path (alternative to mount_name)
        remote_path: Remote directory path (alternative to mount_name)
        dry_run: If true, show what would be transferred without actually syncing
    """
    try:
        from srunx.ssh.core.config import ConfigManager

        cm = ConfigManager()

        if mount_name:
            # Use mount-based sync
            pname = profile_name or cm.get_current_profile_name()
            if not pname:
                return _err("No SSH profile specified and no current profile set")
            profile = cm.get_profile(pname)
            if not profile:
                return _err(f"SSH profile '{pname}' not found")

            mount = next((m for m in profile.mounts if m.name == mount_name), None)
            if not mount:
                available = [m.name for m in profile.mounts]
                return _err(
                    f"Mount '{mount_name}' not found in profile '{pname}'. "
                    f"Available: {available}"
                )

            from srunx.web.sync_utils import build_rsync_client

            rsync = build_rsync_client(profile)
            result = rsync.push(
                mount.local,
                mount.remote,
                dry_run=dry_run,
                exclude_patterns=mount.exclude_patterns,
            )
            if not result.success:
                return _err(
                    f"rsync failed (exit {result.returncode}): "
                    f"{result.stderr[:500] if result.stderr else 'unknown error'}"
                )
            return _ok(
                profile=pname,
                mount=mount_name,
                local=mount.local,
                remote=mount.remote,
                dry_run=dry_run,
                output=result.stdout[:2000] if result.stdout else "",
            )

        elif local_path:
            # Use explicit path sync
            pname = profile_name or cm.get_current_profile_name()
            if not pname:
                return _err("No SSH profile specified and no current profile set")
            profile = cm.get_profile(pname)
            if not profile:
                return _err(f"SSH profile '{pname}' not found")

            from srunx.web.sync_utils import build_rsync_client

            rsync = build_rsync_client(profile)
            result = rsync.push(local_path, remote_path, dry_run=dry_run)
            if not result.success:
                return _err(
                    f"rsync failed (exit {result.returncode}): "
                    f"{result.stderr[:500] if result.stderr else 'unknown error'}"
                )
            return _ok(
                profile=pname,
                local=local_path,
                remote=remote_path or rsync.get_default_remote_path(local_path),
                dry_run=dry_run,
                output=result.stdout[:2000] if result.stdout else "",
            )
        else:
            return _err("Specify either mount_name or local_path for sync")

    except Exception as e:
        return _err(str(e))


# ---------------------------------------------------------------------------
# Configuration Tools
# ---------------------------------------------------------------------------


@mcp.tool()
def get_config() -> dict[str, Any]:
    """Get the current srunx configuration including resource defaults and environment settings."""
    try:
        from srunx.config import get_config as _get_config

        config = _get_config()
        return _ok(
            resources=config.resources.model_dump(),
            environment={
                "conda": config.environment.conda,
                "venv": config.environment.venv,
                "env_vars": config.environment.env_vars,
            },
            log_dir=config.log_dir,
            work_dir=config.work_dir,
        )
    except Exception as e:
        return _err(str(e))


@mcp.tool()
def list_ssh_profiles() -> dict[str, Any]:
    """List all configured SSH connection profiles for remote SLURM clusters.

    Shows profile names, hostnames, and configured mount points.
    """
    try:
        from srunx.ssh.core.config import ConfigManager

        cm = ConfigManager()
        profiles = cm.list_profiles()
        current = cm.get_current_profile_name()

        result = []
        for name, profile in profiles.items():
            mounts = [
                {"name": m.name, "local": m.local, "remote": m.remote}
                for m in profile.mounts
            ]
            result.append(
                {
                    "name": name,
                    "hostname": profile.hostname,
                    "username": profile.username,
                    "port": profile.port,
                    "description": profile.description,
                    "is_current": name == current,
                    "mounts": mounts,
                }
            )

        return _ok(profiles=result, current=current, count=len(result))
    except Exception as e:
        return _err(str(e))


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> None:
    """Run the srunx MCP server."""
    mcp.run()


if __name__ == "__main__":
    main()
