"""SSH-based SLURM adapter for the Web UI.

Wraps SSHSlurmClient to provide all operations needed by the REST API,
including list_jobs, cancel_job, and get_resources which SSHSlurmClient
does not natively support.
"""

from __future__ import annotations

import re
from datetime import UTC, datetime
from typing import Any

from srunx.client_protocol import (
    JobStatusInfo,
    parse_slurm_datetime,
    parse_slurm_duration,
)
from srunx.logging import get_logger
from srunx.ssh.core.client import SSHSlurmClient
from srunx.ssh.core.config import ConfigManager
from srunx.ssh.core.ssh_config import SSHConfigParser  # noqa: F811
from srunx.utils import GPU_TRES_RE  # noqa: E402

logger = get_logger(__name__)

# Strict pattern for SLURM identifiers (user, partition) to prevent injection
_SAFE_IDENTIFIER = re.compile(r"^[a-zA-Z0-9_.\-]+$")

# Node states that should be excluded from available counts
_UNAVAILABLE_STATES = {"down", "drain", "maint", "reserved"}

# SLURM terminal job states (used to filter sacct output)
_TERMINAL_STATES = {
    "COMPLETED",
    "FAILED",
    "CANCELLED",
    "TIMEOUT",
    "NODE_FAIL",
    "PREEMPTED",
    "OUT_OF_MEMORY",
}


def _validate_identifier(value: str, name: str) -> None:
    """Validate a SLURM identifier to prevent shell injection."""
    if not _SAFE_IDENTIFIER.match(value):
        raise ValueError(f"Invalid {name}: {value!r}")


def _run_slurm_cmd(adapter: SlurmSSHAdapter, cmd: str) -> str:
    """Execute a SLURM command on the remote host.

    Ensures SSH connection is alive, then uses SSHSlurmClient._execute_slurm_command()
    which handles SLURM path resolution, environment setup, and login shell wrapping.

    Raises RuntimeError if the command fails.
    """
    adapter._ensure_connected()
    stdout, stderr, exit_code = adapter._client._execute_slurm_command(cmd)  # noqa: SLF001
    if exit_code != 0:
        raise RuntimeError(f"Remote command failed ({exit_code}): {stderr.strip()}")
    return stdout


class SlurmSSHAdapter:
    """Adapter providing a unified API for the Web UI over SSH."""

    def __init__(
        self,
        *,
        profile_name: str | None = None,
        hostname: str | None = None,
        username: str | None = None,
        key_filename: str | None = None,
        port: int = 22,
    ) -> None:
        if profile_name:
            cm = ConfigManager()
            profile = cm.get_profile(profile_name)
            if not profile:
                raise ValueError(f"SSH profile '{profile_name}' not found")

            # Resolve connection: ssh_host (from ~/.ssh/config) or direct fields
            if profile.ssh_host:
                parser = SSHConfigParser()
                ssh_host = parser.get_host(profile.ssh_host)
                if not ssh_host:
                    raise ValueError(
                        f"SSH host '{profile.ssh_host}' not found in ~/.ssh/config"
                    )
                self._client = SSHSlurmClient(
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

                self._client = SSHSlurmClient(
                    hostname=resolved_hostname,
                    username=profile.username,
                    key_filename=resolved_key,
                    port=resolved_port,
                    proxy_jump=resolved_proxy,
                    env_vars=dict(profile.env_vars) if profile.env_vars else None,
                )
        elif hostname and username:
            self._client = SSHSlurmClient(
                hostname=hostname,
                username=username,
                key_filename=key_filename,
                port=port,
            )
        else:
            raise ValueError("Either profile_name or (hostname, username) required")

    def _set_keepalive(self) -> None:
        ssh = self._client.ssh_client
        if ssh is not None:
            transport = ssh.get_transport()
            if transport:
                transport.set_keepalive(30)

    def connect(self) -> bool:
        result = self._client.connect()
        if result:
            self._set_keepalive()
        return result

    def disconnect(self) -> None:
        self._client.disconnect()

    def _ensure_connected(self) -> None:
        """Reconnect if the SSH connection has dropped."""
        ssh = self._client.ssh_client
        needs_reconnect = ssh is None
        if not needs_reconnect:
            transport = ssh.get_transport()  # type: ignore[union-attr]
            needs_reconnect = transport is None or not transport.is_active()

        if needs_reconnect:
            logger.warning("SSH connection lost, reconnecting...")
            self._client.disconnect()
            if not self._client.connect():
                raise RuntimeError("SSH reconnection failed")
            self._set_keepalive()
            logger.info("SSH reconnection successful")

    def __enter__(self) -> SlurmSSHAdapter:
        self.connect()
        return self

    def __exit__(self, *args: object) -> None:
        self.disconnect()

    # ── Job Operations ────────────────────────────

    def list_jobs(self, user: str | None = None) -> list[dict[str, Any]]:
        """List SLURM jobs via squeue + recent completed/failed jobs via sacct."""
        # --- Active jobs from squeue ---
        fmt = "%.18i %.9P %.30j %.12u %.8T %.10M %.9l %.6D %R %b"
        cmd = f'squeue --format "{fmt}" --noheader'
        if user:
            _validate_identifier(user, "user")
            cmd += f" --user {user}"

        output = _run_slurm_cmd(self, cmd)
        jobs: list[dict[str, Any]] = []
        seen_ids: set[int] = set()

        for line in output.strip().splitlines():
            parts = line.split()
            if len(parts) < 9:
                continue

            job_id_str = parts[0].strip()
            try:
                job_id = int(job_id_str)
            except ValueError:
                continue

            # parts[7] = %D (node count), parts[9] = %b (TRES per node)
            num_nodes = int(parts[7]) if parts[7].strip().isdigit() else 1
            gpus_per_node = 0
            if len(parts) >= 10:
                gpu_match = GPU_TRES_RE.search(parts[9])
                if gpu_match:
                    gpus_per_node = int(gpu_match.group(1))

            seen_ids.add(job_id)
            jobs.append(
                {
                    "name": parts[2].strip(),
                    "job_id": job_id,
                    "status": parts[4].strip(),
                    "depends_on": [],
                    "command": [],
                    "resources": {
                        "nodes": num_nodes,
                        "gpus_per_node": gpus_per_node,
                        "partition": parts[1].strip(),
                        "time_limit": parts[6].strip(),
                    },
                    "partition": parts[1].strip(),
                    "nodes": num_nodes,
                    "gpus": gpus_per_node * num_nodes,
                    "elapsed_time": parts[5].strip(),
                }
            )

        # --- Recently finished jobs from sacct (last 6 hours) ---
        # NOTE: --state filter is omitted because some SLURM versions
        # return empty output when --state is combined with --parsable2.
        # We filter by status in Python instead.
        try:
            sacct_cmd = (
                "sacct -S now-6hours "
                "--format=JobID,JobName,State,Partition,NNodes,Elapsed,TimelimitRaw,AllocTRES "
                "--noheader --parsable2"
            )
            if user:
                sacct_cmd += f" --user {user}"

            sacct_output = _run_slurm_cmd(self, sacct_cmd)

            for line in sacct_output.strip().splitlines():
                parts = line.split("|")
                if len(parts) < 6:
                    continue
                # Skip sub-steps (e.g., "12345.batch", "12345.extern")
                if "." in parts[0]:
                    continue

                try:
                    job_id = int(parts[0].strip())
                except ValueError:
                    continue

                if job_id in seen_ids:
                    continue

                # sacct may return e.g. "CANCELLED by 1000" — take first word only
                # Skip non-terminal states (already covered by squeue)
                raw_state = parts[2].strip()
                status = raw_state.split()[0] if raw_state else "UNKNOWN"
                if status not in _TERMINAL_STATES:
                    continue

                gpus = 0
                if len(parts) >= 8:
                    gpu_match = GPU_TRES_RE.search(parts[7])
                    if gpu_match:
                        gpus = int(gpu_match.group(1))

                num_nodes = int(parts[4]) if parts[4].strip().isdigit() else 1

                seen_ids.add(job_id)
                jobs.append(
                    {
                        "name": parts[1].strip(),
                        "job_id": job_id,
                        "status": status,
                        "depends_on": [],
                        "command": [],
                        "resources": {
                            "nodes": num_nodes,
                            "gpus_per_node": gpus,
                            "partition": parts[3].strip(),
                            "time_limit": parts[6].strip() if len(parts) > 6 else None,
                        },
                        "partition": parts[3].strip(),
                        "nodes": num_nodes,
                        "gpus": gpus * num_nodes,
                        "elapsed_time": parts[5].strip(),
                    }
                )
        except Exception:
            logger.warning(
                "sacct query failed; returning squeue results only", exc_info=True
            )

        return jobs

    def queue_by_ids(self, job_ids: list[int]) -> dict[int, JobStatusInfo]:
        """Return a mapping of ``job_id`` -> :class:`JobStatusInfo` for active jobs.

        Implements :class:`SlurmClientProtocol`. Active jobs are queried via
        ``squeue --jobs=...``; jobs no longer in the queue fall back to
        ``sacct``. Jobs found in neither source are omitted.
        """
        if not job_ids:
            return {}

        for jid in job_ids:
            if jid <= 0:
                raise ValueError(f"Invalid job_id: {jid}")

        id_arg = ",".join(str(i) for i in job_ids)
        results: dict[int, JobStatusInfo] = {}

        # --- squeue: active jobs ---
        try:
            squeue_out = _run_slurm_cmd(
                self,
                f'squeue --jobs {id_arg} --format "%i|%T|%S|%M|%N" --noheader',
            )
        except RuntimeError:
            # squeue may fail if NO job IDs are currently in the queue;
            # this is normal when all queried jobs have already completed.
            squeue_out = ""

        for line in squeue_out.strip().splitlines():
            parts = line.split("|")
            if len(parts) < 5:
                continue
            try:
                jid = int(parts[0].strip())
            except ValueError:
                continue
            results[jid] = JobStatusInfo(
                status=parts[1].strip(),
                started_at=parse_slurm_datetime(parts[2]),
                duration_secs=parse_slurm_duration(parts[3]),
                nodelist=(parts[4].strip() or None),
            )

        # --- sacct fallback: terminal jobs ---
        missing = [j for j in job_ids if j not in results]
        if missing:
            missing_arg = ",".join(str(i) for i in missing)
            try:
                sacct_out = _run_slurm_cmd(
                    self,
                    f"sacct --jobs {missing_arg} "
                    f"--format=JobID,State,Start,End,Elapsed,NodeList "
                    f"--noheader --parsable2",
                )
            except RuntimeError:
                sacct_out = ""

            for line in sacct_out.strip().splitlines():
                parts = line.split("|")
                if len(parts) < 6:
                    continue
                raw_id = parts[0].strip()
                if "." in raw_id:
                    continue
                try:
                    jid = int(raw_id)
                except ValueError:
                    continue
                if jid in results:
                    continue
                raw_state = parts[1].strip()
                status = raw_state.split()[0] if raw_state else "UNKNOWN"
                results[jid] = JobStatusInfo(
                    status=status,
                    started_at=parse_slurm_datetime(parts[2]),
                    completed_at=parse_slurm_datetime(parts[3]),
                    duration_secs=parse_slurm_duration(parts[4]),
                    nodelist=(parts[5].strip() or None),
                )

        return results

    def get_job(self, job_id: int) -> dict[str, Any]:
        """Get detailed job info via sacct."""
        cmd = (
            f"sacct -j {job_id} "
            "--format=JobID,JobName,State,Partition,NNodes,NCPUS,Elapsed,TimelimitRaw,AllocTRES "
            "--noheader --parsable2"
        )
        output = _run_slurm_cmd(self, cmd)

        for line in output.strip().splitlines():
            parts = line.split("|")
            if len(parts) < 5:
                continue
            # Skip sub-steps (e.g., "12345.batch")
            if "." in parts[0]:
                continue

            gpus = 0
            if len(parts) >= 9:
                tres = parts[8]
                gpu_match = GPU_TRES_RE.search(tres)
                if gpu_match:
                    gpus = int(gpu_match.group(1))

            return {
                "name": parts[1].strip(),
                "job_id": job_id,
                "status": parts[2].strip(),
                "depends_on": [],
                "command": [],
                "resources": {
                    "nodes": int(parts[4]) if parts[4].strip().isdigit() else 1,
                    "gpus_per_node": gpus,
                    "partition": parts[3].strip(),
                },
                "partition": parts[3].strip(),
                "nodes": int(parts[4]) if parts[4].strip().isdigit() else None,
                "gpus": gpus,
                "elapsed_time": parts[6].strip() if len(parts) > 6 else None,
            }

        raise ValueError(f"No job information found for job {job_id}")

    def cancel_job(self, job_id: int) -> None:
        """Cancel a SLURM job via scancel."""
        _run_slurm_cmd(self, f"scancel {job_id}")

    def submit_job(
        self,
        script_content: str,
        job_name: str | None = None,
        dependency: str | None = None,
    ) -> dict[str, Any]:
        """Submit a job via sbatch. Returns job info dict."""
        result = self._client.submit_sbatch_job(
            script_content, job_name=job_name, dependency=dependency
        )
        if result is None:
            raise RuntimeError("sbatch submission failed")
        return {
            "name": result.name or job_name or "job",
            "job_id": int(result.job_id) if result.job_id else None,
            "status": "PENDING",
            "depends_on": [],
            "command": [],
            "resources": {},
        }

    def get_job_output(
        self,
        job_id: int,
        job_name: str | None = None,
        stdout_offset: int = 0,
        stderr_offset: int = 0,
    ) -> tuple[str, str, int, int]:
        """Get job stdout/stderr log contents from remote.

        Returns ``(stdout, stderr, new_stdout_offset, new_stderr_offset)``.
        """
        return self._client.get_job_output(
            str(job_id),
            job_name=job_name,
            stdout_offset=stdout_offset,
            stderr_offset=stderr_offset,
        )

    def get_job_status(self, job_id: int) -> str:
        """Get job status string."""
        return self._client.get_job_status(str(job_id))

    # ── Resource Operations ───────────────────────

    def get_resources(self, partition: str | None = None) -> list[dict[str, Any]]:
        """Get cluster resource information via sinfo + squeue.

        The ``partition=None`` case is the per-partition listing used
        by ``/api/resources`` — errors are caught per-partition so a
        single broken partition doesn't sink the whole dashboard call.
        Callers that need a single aggregated cluster-wide snapshot
        (e.g. the resource snapshotter) should use
        :meth:`get_cluster_snapshot` instead — that path fails closed
        and dedups nodes across partitions, which summing this list
        does not.
        """
        if partition:
            _validate_identifier(partition, "partition")
            return [self._get_partition_resources(partition)]

        # List all partitions
        output = _run_slurm_cmd(self, "sinfo -o '%P' --noheader")
        partitions = {
            line.strip().rstrip("*")
            for line in output.strip().splitlines()
            if line.strip()
        }

        results: list[dict[str, Any]] = []
        for p in sorted(partitions):
            try:
                results.append(self._get_partition_resources(p))
            except Exception as e:
                logger.warning("Failed to get resources for partition %s: %s", p, e)
                continue
        return results

    def get_cluster_snapshot(self) -> dict[str, Any]:
        """Return a single cluster-wide resource snapshot dict.

        Used by the resource snapshotter for a single row in
        ``resource_snapshots``. Differs from ``get_resources(None)``
        in two important ways:

        1. Runs ONE ``sinfo`` (no ``-p`` filter) and ONE ``squeue``,
           then dedups nodes by name via ``seen_nodes``. Summing the
           per-partition output of ``get_resources(None)`` would
           double-count any node that belongs to multiple partitions
           (a common SLURM setup — e.g. ``debug`` and ``gpu`` sharing
           the same physical nodes).
        2. Exceptions propagate instead of being swallowed per
           partition. Transient SSH/SLURM failures surface to the
           poller supervisor for exponential backoff rather than
           silently writing understated totals to the DB.

        Returned keys match :meth:`_get_partition_resources` with
        ``partition=None``.
        """
        # One cluster-wide sinfo call — seen_nodes dedup handles
        # multi-partition membership correctly.
        sinfo_output = _run_slurm_cmd(self, 'sinfo -o "%n %G %T" --noheader')

        nodes_total = 0
        nodes_idle = 0
        nodes_down = 0
        total_gpus = 0
        seen_nodes: set[str] = set()

        for line in sinfo_output.strip().splitlines():
            parts = line.split()
            if len(parts) < 3:
                continue
            node_name, gres, state = parts[0], parts[1], parts[2].lower()
            if node_name in seen_nodes:
                continue
            seen_nodes.add(node_name)
            nodes_total += 1

            if any(s in state for s in _UNAVAILABLE_STATES):
                nodes_down += 1
                continue
            if "idle" in state:
                nodes_idle += 1

            if gres and gres != "(null)":
                for entry in gres.split(","):
                    gpu_match = GPU_TRES_RE.search(entry)
                    if gpu_match:
                        total_gpus += int(gpu_match.group(1))

        # Cluster-wide squeue — same TRES parsing as the per-partition path.
        squeue_output = _run_slurm_cmd(self, 'squeue -o "%i %T %b %D" --noheader')
        gpus_in_use = 0
        jobs_running = 0
        for line in squeue_output.strip().splitlines():
            parts = line.split()
            if len(parts) < 2 or parts[1] != "RUNNING":
                continue
            jobs_running += 1
            if len(parts) >= 3:
                gpu_match = GPU_TRES_RE.search(parts[2])
                if gpu_match:
                    per_node_gpus = int(gpu_match.group(1))
                    num_nodes = 1
                    if len(parts) >= 4 and parts[3].isdigit():
                        num_nodes = int(parts[3])
                    gpus_in_use += per_node_gpus * num_nodes

        gpus_available = max(0, total_gpus - gpus_in_use)
        gpu_utilization = gpus_in_use / total_gpus if total_gpus > 0 else 0.0

        return {
            "timestamp": datetime.now(UTC).isoformat(),
            "partition": None,
            "total_gpus": total_gpus,
            "gpus_in_use": gpus_in_use,
            "gpus_available": gpus_available,
            "jobs_running": jobs_running,
            "nodes_total": nodes_total,
            "nodes_idle": nodes_idle,
            "nodes_down": nodes_down,
            "gpu_utilization": gpu_utilization,
            "has_available_gpus": gpus_available > 0,
        }

    def _get_partition_resources(self, partition: str) -> dict[str, Any]:
        """Get resources for a single partition."""
        _validate_identifier(partition, "partition")

        sinfo_output = _run_slurm_cmd(
            self,
            f'sinfo -o "%n %G %T" --noheader -p {partition}',
        )

        nodes_total = 0
        nodes_idle = 0
        nodes_down = 0
        total_gpus = 0
        seen_nodes: set[str] = set()

        for line in sinfo_output.strip().splitlines():
            parts = line.split()
            if len(parts) < 3:
                continue
            node_name, gres, state = parts[0], parts[1], parts[2].lower()
            if node_name in seen_nodes:
                continue
            seen_nodes.add(node_name)
            nodes_total += 1

            # Filter unavailable states (aligned with core ResourceMonitor)
            if any(s in state for s in _UNAVAILABLE_STATES):
                nodes_down += 1
                continue
            if "idle" in state:
                nodes_idle += 1

            # Parse GPU count using shared regex (handles gpu:NVIDIA-A100:8 etc.)
            if gres and gres != "(null)":
                for entry in gres.split(","):
                    gpu_match = GPU_TRES_RE.search(entry)
                    if gpu_match:
                        total_gpus += int(gpu_match.group(1))

        # squeue for GPU usage — include %D (node count) to handle multi-node jobs
        # %b is TRES_PER_NODE, so actual GPU usage = per_node_gpus * num_nodes
        squeue_output = _run_slurm_cmd(
            self,
            f'squeue -o "%i %T %b %D" --noheader -p {partition}',
        )

        gpus_in_use = 0
        jobs_running = 0
        for line in squeue_output.strip().splitlines():
            parts = line.split()
            if len(parts) < 2 or parts[1] != "RUNNING":
                continue
            jobs_running += 1
            if len(parts) >= 3:
                gpu_match = GPU_TRES_RE.search(parts[2])
                if gpu_match:
                    per_node_gpus = int(gpu_match.group(1))
                    # Multiply by node count for multi-node jobs
                    num_nodes = 1
                    if len(parts) >= 4 and parts[3].isdigit():
                        num_nodes = int(parts[3])
                    gpus_in_use += per_node_gpus * num_nodes

        gpus_available = max(0, total_gpus - gpus_in_use)
        gpu_utilization = gpus_in_use / total_gpus if total_gpus > 0 else 0.0

        return {
            "timestamp": datetime.now(UTC).isoformat(),
            "partition": partition,
            "total_gpus": total_gpus,
            "gpus_in_use": gpus_in_use,
            "gpus_available": gpus_available,
            "jobs_running": jobs_running,
            "nodes_total": nodes_total,
            "nodes_idle": nodes_idle,
            "nodes_down": nodes_down,
            "gpu_utilization": gpu_utilization,
            "has_available_gpus": gpus_available > 0,
        }
