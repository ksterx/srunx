"""SSH-based SLURM adapter for the Web UI.

Wraps SSHSlurmClient to provide all operations needed by the REST API,
including list_jobs, cancel_job, and get_resources which SSHSlurmClient
does not natively support.
"""

from __future__ import annotations

import re
import threading
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from srunx.callbacks import Callback
from srunx.client_protocol import (
    JobStatusInfo,
    parse_slurm_datetime,
    parse_slurm_duration,
)
from srunx.logging import get_logger
from srunx.slurm.states import SLURM_TERMINAL_JOB_STATES
from srunx.ssh.core.client import SSHSlurmClient
from srunx.ssh.core.config import ConfigManager, MountConfig
from srunx.ssh.core.ssh_config import SSHConfigParser  # noqa: F811
from srunx.utils import GPU_TRES_RE  # noqa: E402

if TYPE_CHECKING:
    from srunx.client_protocol import LogChunk
    from srunx.models import BaseJob, JobStatus, RunnableJobType
    from srunx.rendering import SubmissionRenderContext

logger = get_logger(__name__)

# Strict pattern for SLURM identifiers (user, partition) to prevent injection
_SAFE_IDENTIFIER = re.compile(r"^[a-zA-Z0-9_.\-]+$")

# Node states that should be excluded from available counts
_UNAVAILABLE_STATES = {"down", "drain", "maint", "reserved"}


class SSHMonitorTimeoutError(RuntimeError):
    """Raised when ``_monitor_until_terminal`` exceeds its timeout.

    Subclass of ``RuntimeError`` so the sweep orchestrator's existing
    broad-except cell-failure handler still catches it — the typed
    subclass just lets targeted callers (e.g. tests, future UI status
    reporting) distinguish timeout from a genuine SLURM-state-derived
    failure without widening the exception surface.
    """


def _resolve_monitor_timeout_default() -> float | None:
    """Return the default per-job monitor timeout from the environment.

    ``SRUNX_SSH_MONITOR_TIMEOUT`` accepts a non-negative float (seconds).
    An unset / empty / ``"0"`` / non-numeric value means "no timeout",
    preserving the pre-Phase-3 behaviour for users who haven't opted in.
    """
    import os

    raw = os.getenv("SRUNX_SSH_MONITOR_TIMEOUT")
    if not raw:
        return None
    try:
        value = float(raw)
    except ValueError:
        logger.warning(
            f"Ignoring invalid SRUNX_SSH_MONITOR_TIMEOUT={raw!r} "
            "(expected non-negative seconds)"
        )
        return None
    if value <= 0:
        return None
    return value


@dataclass(frozen=True)
class SlurmSSHAdapterSpec:
    """Connection spec used to clone a :class:`SlurmSSHAdapter` for pooling.

    Intentionally captures only the configuration needed to re-create an
    adapter with an equivalent SSH session -- no paramiko clients, SFTP
    channels, or in-flight state. Used by Step 4's pool factory to mint
    per-cell adapter clones off a shared singleton template.

    ``mounts`` is a tuple of frozen :class:`MountConfig` instances so the
    spec is deeply immutable and hashable end-to-end.
    """

    profile_name: str | None
    hostname: str
    username: str
    key_filename: str | None
    port: int
    proxy_jump: str | None = None
    env_vars: tuple[tuple[str, str], ...] = ()
    mounts: tuple[MountConfig, ...] = field(default_factory=tuple)


def _validate_identifier(value: str, name: str) -> None:
    """Validate a SLURM identifier to prevent shell injection."""
    if not _SAFE_IDENTIFIER.match(value):
        raise ValueError(f"Invalid {name}: {value!r}")


def _run_slurm_cmd(adapter: SlurmSSHAdapter, cmd: str) -> str:
    """Execute a SLURM command on the remote host.

    Ensures SSH connection is alive, then uses SSHSlurmClient._execute_slurm_command()
    which handles SLURM path resolution, environment setup, and login shell wrapping.

    Raises RuntimeError if the command fails.

    Runs under the adapter's ``_io_lock`` so concurrent workflow / sweep
    threads cannot interleave SSH I/O on the shared paramiko session.
    """
    with adapter._io_lock:  # noqa: SLF001
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
        proxy_jump: str | None = None,
        env_vars: dict[str, str] | None = None,
        mounts: Sequence[MountConfig] | None = None,
        callbacks: Sequence[Callback] | None = None,
        submission_source: str = "web",
    ) -> None:
        # Reentrant lock: some code paths call _ensure_connected() explicitly
        # from inside another locked section (e.g. _run_slurm_cmd). An RLock
        # lets the same thread re-enter without self-deadlocking while still
        # serializing I/O across threads.
        self._io_lock: threading.RLock = threading.RLock()

        # Mutable origin tag for ``record_submission`` writes. The Web router
        # default is ``'web'``; the CLI wrapper passes ``'cli'`` via
        # ``_build_ssh_handle(..., submission_source='cli')``. MCP callers
        # pass ``'mcp'``. Exposed as a public attribute so the transport
        # registry can rebind it without threading a kwarg through every
        # Protocol signature (see review fix #7).
        self.submission_source: str = submission_source

        # Resolved connection params — persisted so connection_spec() can
        # reproduce this adapter in Step 4's pool factory. Populated below
        # in both the profile_name and direct-hostname branches.
        self._profile_name: str | None = profile_name
        self._hostname: str = ""
        self._username: str = ""
        self._key_filename: str | None = None
        self._port: int = port
        self._proxy_jump: str | None = None
        self._env_vars: dict[str, str] = dict(env_vars) if env_vars else {}
        self._mounts: tuple[MountConfig, ...] = (
            tuple(mounts) if mounts is not None else ()
        )

        # Callbacks attached to this adapter; invoked by :meth:`run` on the
        # sweep path. Mirrors ``Slurm.callbacks`` in ``srunx.client``.
        self.callbacks: list[Callback] = list(callbacks) if callbacks else []

        if profile_name:
            cm = ConfigManager()
            profile = cm.get_profile(profile_name)
            if not profile:
                raise ValueError(f"SSH profile '{profile_name}' not found")

            self._mounts = tuple(profile.mounts) if profile.mounts else ()
            if profile.env_vars:
                self._env_vars = dict(profile.env_vars)

            # Resolve connection: ssh_host (from ~/.ssh/config) or direct fields
            if profile.ssh_host:
                parser = SSHConfigParser()
                ssh_host = parser.get_host(profile.ssh_host)
                if not ssh_host:
                    raise ValueError(
                        f"SSH host '{profile.ssh_host}' not found in ~/.ssh/config"
                    )
                self._hostname = ssh_host.hostname or profile.ssh_host
                self._username = ssh_host.user or ""
                self._key_filename = ssh_host.identity_file
                self._port = ssh_host.port or 22
                self._proxy_jump = ssh_host.proxy_jump
                self._client = SSHSlurmClient(
                    hostname=self._hostname,
                    username=self._username,
                    key_filename=self._key_filename,
                    port=self._port,
                    proxy_jump=self._proxy_jump,
                    env_vars=self._env_vars or None,
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

                self._hostname = resolved_hostname
                self._username = profile.username
                self._key_filename = resolved_key
                self._port = resolved_port
                self._proxy_jump = resolved_proxy
                self._client = SSHSlurmClient(
                    hostname=resolved_hostname,
                    username=profile.username,
                    key_filename=resolved_key,
                    port=resolved_port,
                    proxy_jump=resolved_proxy,
                    env_vars=self._env_vars or None,
                )
        elif hostname and username:
            self._hostname = hostname
            self._username = username
            self._key_filename = key_filename
            self._port = port
            self._proxy_jump = proxy_jump
            self._client = SSHSlurmClient(
                hostname=hostname,
                username=username,
                key_filename=key_filename,
                port=port,
                proxy_jump=proxy_jump,
                env_vars=self._env_vars or None,
            )
        else:
            raise ValueError("Either profile_name or (hostname, username) required")

    # ── Public introspection ──────────────────────

    @property
    def scheduler_key(self) -> str:
        """Return the V5 transport axis for this adapter.

        ``"local"`` when no profile is bound (legacy direct-hostname
        tests) or ``f"ssh:{profile_name}"`` otherwise. Exposed publicly
        so callers (Web routers, poller, etc.) don't reach into
        ``_profile_name`` to build target_refs / scheduler_keys.
        """
        if self._profile_name is None:
            return "local"
        return f"ssh:{self._profile_name}"

    # ── Connection spec (for Step 4 pool factory) ─────

    @property
    def connection_spec(self) -> SlurmSSHAdapterSpec:
        """Return the immutable connection spec for cloning this adapter.

        Step 4's sweep pool uses this spec to mint per-cell adapter clones
        off the shared singleton template without copying any live
        paramiko / SFTP state. Reading the spec does NOT touch the wire,
        so it is safe to call without holding ``_io_lock``.
        """
        return SlurmSSHAdapterSpec(
            profile_name=self._profile_name,
            hostname=self._hostname,
            username=self._username,
            key_filename=self._key_filename,
            port=self._port,
            proxy_jump=self._proxy_jump,
            env_vars=tuple(sorted(self._env_vars.items())),
            mounts=self._mounts,
        )

    @classmethod
    def from_spec(
        cls,
        spec: SlurmSSHAdapterSpec,
        *,
        callbacks: Sequence[Callback] | None = None,
        submission_source: str = "web",
    ) -> SlurmSSHAdapter:
        """Create a fresh adapter from a connection spec.

        The returned adapter is NOT connected; it connects lazily on first
        SSH I/O (via ``_ensure_connected``). Used by the Step 4 pool
        factory to mint per-lease adapter clones off the singleton template
        without copying any live paramiko / SFTP state.

        ``callbacks`` are attached on construction so the pool's chosen
        callback list propagates into each cloned adapter's ``run`` path.

        ``submission_source`` is carried through from the pool's origin
        tag so per-cell sweep jobs record the correct transport origin
        in the ``jobs.submission_source`` column.
        """
        # NOTE: ``profile_name`` is intentionally NOT forwarded into the
        # clone's constructor — setting ``profile_name`` there triggers
        # the full ``ConfigManager`` + ``~/.ssh/config`` resolution path,
        # which would re-parse the profile on every pooled lease (and
        # fail in test environments that stub out the ConfigManager).
        # The spec already captures the fully-resolved connection params,
        # so we use the direct-hostname branch and then manually bind
        # ``_profile_name`` so ``scheduler_key`` / completion recording
        # target the correct SSH axis.
        adapter = cls(
            hostname=spec.hostname,
            username=spec.username,
            key_filename=spec.key_filename,
            port=spec.port,
            proxy_jump=spec.proxy_jump,
            env_vars=dict(spec.env_vars) if spec.env_vars else None,
            mounts=list(spec.mounts) if spec.mounts else None,
            callbacks=callbacks,
            submission_source=submission_source,
        )
        adapter._profile_name = spec.profile_name
        return adapter

    @property
    def is_connected(self) -> bool:
        """Return True when the underlying paramiko session is live.

        Used by the Step 4 pool to decide whether a released adapter is
        safe to return to the free queue or should be discarded. Never
        raises — any transport-level error is treated as "not connected".
        """
        try:
            ssh = self._client.ssh_client
            if ssh is None:
                return False
            transport = ssh.get_transport()
            return bool(transport is not None and transport.is_active())
        except Exception:  # noqa: BLE001 — diagnostic only
            return False

    def _set_keepalive(self) -> None:
        ssh = self._client.ssh_client
        if ssh is not None:
            transport = ssh.get_transport()
            if transport:
                transport.set_keepalive(30)

    def connect(self) -> bool:
        with self._io_lock:
            result = self._client.connect()
            if result:
                self._set_keepalive()
            return result

    def disconnect(self) -> None:
        with self._io_lock:
            self._client.disconnect()

    def _ensure_connected(self) -> None:
        """Connect (or reconnect) the SSH session if needed.

        Three states:

        1. ``ssh_client is None`` — adapter was never connected. Happens
           for every adapter built by
           :func:`srunx.transport.registry._build_ssh_handle` (CLI
           scope, MCP tool handlers, tests). Log as "connecting" —
           calling this a "reconnect" would be misleading.
        2. ``transport`` absent or inactive — the session was open but
           dropped (idle timeout, network blip). Log as "reconnecting".
        3. Transport active — no-op.

        Safe to call from inside another ``_io_lock`` region because the
        lock is reentrant. Callers that invoke SSH I/O directly on
        ``self._client`` must wrap both ``_ensure_connected`` and the
        subsequent call in a single ``with self._io_lock`` block so that
        a competing thread cannot swap the paramiko session between the
        check and the use.
        """
        with self._io_lock:
            ssh = self._client.ssh_client
            if ssh is None:
                logger.debug("SSH adapter connecting for the first time")
                if not self._client.connect():
                    raise RuntimeError("SSH connection failed")
                self._set_keepalive()
                return

            transport = ssh.get_transport()
            if transport is not None and transport.is_active():
                return  # happy path — connection already up

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
                    "time_limit": parts[6].strip(),
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
                if status not in SLURM_TERMINAL_JOB_STATES:
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

        # --- scontrol fallback: pyxis clusters where sacct is unreachable ---
        # slurmdbd outages leave sacct returning empty for just-finished
        # jobs. scontrol keeps the record in memory for ~5 minutes
        # (MinJobAge) without needing the accounting DB, so we probe it
        # per-missing-id. Per-id cost is acceptable because the poller
        # runs every 15s and the missing set is only the final-transition
        # tail, not the full active set.
        from srunx.ssh.core.utils import parse_scontrol_job_state

        still_missing = [j for j in job_ids if j not in results]
        for jid in still_missing:
            try:
                scontrol_out = _run_slurm_cmd(
                    self, f"scontrol show job {jid} 2>/dev/null"
                )
            except RuntimeError:
                continue
            parsed = parse_scontrol_job_state(scontrol_out)
            if parsed is None:
                continue
            results[jid] = JobStatusInfo(status=parsed)

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
        """Cancel a SLURM job via scancel.

        Legacy alias retained for pre-Protocol callers
        (``web.routers.jobs`` / ``web.routers.workflows``). New code
        should call :meth:`cancel` instead, which raises typed
        transport exceptions and aligns with
        :class:`~srunx.client_protocol.JobOperationsProtocol`. This
        alias will remain until the remaining web router call sites
        are migrated.
        """
        _run_slurm_cmd(self, f"scancel {job_id}")

    def submit_job(
        self,
        script_content: str,
        job_name: str | None = None,
        dependency: str | None = None,
    ) -> dict[str, Any]:
        """Submit a job via sbatch. Returns job info dict.

        Legacy alias retained for pre-Protocol callers
        (``web.routers.jobs`` / ``web.routers.workflows`` /
        ``web.routers.templates``). New code should call :meth:`submit`
        with a :class:`~srunx.models.Job` instance. This alias will
        remain until those routers migrate to the Protocol surface.
        """
        with self._io_lock:
            self._ensure_connected()
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

        Ensures the SSH connection is live before reading — callers that
        reach this method via a fresh :class:`SlurmSSHAdapter` (e.g. CLI
        ``srunx logs --profile foo``, which builds the adapter via
        :func:`srunx.transport.registry._build_ssh_handle` without the
        Web app's startup connect) would otherwise hit
        ``SSH client is not connected`` on the first call.
        """
        with self._io_lock:
            self._ensure_connected()
            return self._client.get_job_output(
                str(job_id),
                job_name=job_name,
                stdout_offset=stdout_offset,
                stderr_offset=stderr_offset,
            )

    def get_job_status(self, job_id: int) -> str:
        """Get job status string.

        Legacy alias retained for callers that want just the raw SLURM
        state string (``mcp.server`` and the adapter's own
        :meth:`_monitor_until_terminal` loop). New code should call
        :meth:`status` which returns a full :class:`BaseJob` snapshot
        conforming to
        :class:`~srunx.client_protocol.JobOperationsProtocol`.
        """
        with self._io_lock:
            self._ensure_connected()
            return self._client.get_job_status(str(job_id))

    def get_job_output_detailed(
        self,
        job_id: int | str,
        job_name: str | None = None,
        skip_content: bool = False,
    ) -> dict[str, str | list[str] | None]:
        """Return log-file metadata (and optionally content) for ``job_id``.

        Signature + return shape match :meth:`srunx.client.Slurm.get_job_output_detailed`
        so ``SSHWorkflowJobExecutor`` satisfies
        :class:`WorkflowJobExecutorProtocol` transparently.

        ``skip_content=True`` suppresses file content reads so callers that
        only want the primary-log path pay only the ``find`` round-trips.
        """
        with self._io_lock:
            self._ensure_connected()
            info = self._client.get_job_output_detailed(str(job_id), job_name=job_name)
        if skip_content:
            # Preserve the list[str] / None / str shape expected by callers.
            info["output"] = ""
            info["error"] = ""
        return info

    # ── JobOperationsProtocol surface ────────────────
    #
    # These methods align SlurmSSHAdapter with
    # :class:`srunx.client_protocol.JobOperationsProtocol` so the Web UI,
    # MCP, and (future) top-level CLI can all drive a remote SLURM via
    # the same 5 entry points they use for the local ``Slurm`` client.
    # The existing ``submit_job`` / ``cancel_job`` / ``get_job_status`` /
    # ``list_jobs`` / ``get_job_output`` methods are intentionally kept as
    # backwards-compatible aliases so callers that haven't migrated yet
    # stay working.

    def submit(
        self,
        job: RunnableJobType,
        *,
        submission_context: SubmissionRenderContext | None = None,
    ) -> RunnableJobType:
        """Submit *job* over SSH and return it with ``job_id`` populated.

        Renders the SLURM script locally (Jinja), uploads it via sftp,
        invokes ``sbatch`` on the remote, mutates *job* in place to set
        ``job_id`` and ``status = PENDING``, and returns the same object.
        See :meth:`run` for the full lifecycle (render + submit + monitor
        + callbacks). This method is submit-only; it does not block on
        SLURM state transitions.

        ``submission_context`` (when provided) applies mount-aware
        :func:`normalize_job_for_submission` before rendering so absolute
        local ``work_dir`` / ``log_dir`` paths get rewritten to the
        profile's remote mount root. Passing ``None`` preserves the
        pre-Bug-6 behaviour of rendering the job verbatim.

        Records the submission in the state DB with the
        (``transport_type='ssh'``, ``profile_name=<this adapter's
        profile>``, ``scheduler_key='ssh:<profile>'``) triple so the
        poller can look the job up under the right transport. DB writes
        are best-effort and never mask an sbatch success.
        """
        import tempfile as _tempfile

        import paramiko

        from srunx.exceptions import (
            SubmissionError,
            TransportAuthError,
            TransportConnectionError,
            TransportTimeoutError,
        )
        from srunx.models import (
            Job,
            JobStatus,
            ShellJob,
            render_job_script,
            render_shell_job_script,
        )
        from srunx.rendering import normalize_job_for_submission
        from srunx.template import get_template_path

        # Apply mount-aware path translation before we inspect the job's
        # type for template resolution. ``normalize_job_for_submission``
        # returns a ``model_copy`` when a rewrite is needed, so we rebind
        # ``job`` and use the normalized instance for rendering + DB
        # recording. See :meth:`run` for the mirror call-site.
        job = normalize_job_for_submission(job, submission_context)

        if isinstance(job, Job):
            template_path = job.template if job.template else get_template_path("base")
        elif isinstance(job, ShellJob):
            template_path = None
        else:
            raise ValueError(f"Unsupported job type: {type(job).__name__}")

        with _tempfile.TemporaryDirectory() as tmpdir:
            if isinstance(job, Job):
                assert template_path is not None  # narrow for mypy
                script_path = render_job_script(template_path, job, output_dir=tmpdir)
            else:  # ShellJob — narrowed by the elif above
                script_path = render_shell_job_script(job.script_path, job, tmpdir)
            with open(script_path, encoding="utf-8") as f:
                script_content = f.read()

        try:
            with self._io_lock:
                self._ensure_connected()
                result = self._client.submit_sbatch_job(
                    script_content, job_name=job.name
                )
        except paramiko.AuthenticationException as exc:
            raise TransportAuthError(f"SSH authentication failed: {exc}") from exc
        except TimeoutError as exc:
            raise TransportTimeoutError(f"SSH timed out: {exc}") from exc
        except (paramiko.SSHException, OSError) as exc:
            raise TransportConnectionError(f"SSH connection failed: {exc}") from exc

        if result is None or not result.job_id:
            raise SubmissionError(f"sbatch rejected submission for job '{job.name}'")

        job.job_id = int(result.job_id)
        job.status = JobStatus.PENDING

        # Record submission with SSH transport metadata. The adapter
        # carries its ``submission_source`` as mutable state set by the
        # transport registry (``_build_ssh_handle``) — the Web path
        # leaves the default ``'web'``, the CLI wrapper passes
        # ``'cli'``, MCP passes ``'mcp'``. This avoids widening the
        # JobOperationsProtocol signature with a kwarg that would
        # break every existing Protocol implementor.
        if self._profile_name is not None:
            self._record_job_submission(
                job,
                workflow_name=None,
                workflow_run_id=None,
                transport_type="ssh",
                profile_name=self._profile_name,
                scheduler_key=f"ssh:{self._profile_name}",
                submission_source=self.submission_source,
            )

        return job

    def cancel(self, job_id: int) -> None:
        """Cancel *job_id* on the remote cluster.

        Raises :class:`~srunx.exceptions.JobNotFound` when ``scancel``
        reports the job is missing, :class:`TransportError` subclasses
        for SSH-layer failures. The legacy :meth:`cancel_job` API is
        preserved below as a no-op alias for backwards compat.
        """

        import paramiko

        from srunx.exceptions import (
            JobNotFound,
            RemoteCommandError,
            TransportAuthError,
            TransportConnectionError,
            TransportTimeoutError,
        )

        try:
            _run_slurm_cmd(self, f"scancel {int(job_id)}")
        except paramiko.AuthenticationException as exc:
            raise TransportAuthError(f"SSH authentication failed: {exc}") from exc
        except TimeoutError as exc:
            raise TransportTimeoutError(f"SSH timed out: {exc}") from exc
        except paramiko.SSHException as exc:
            raise TransportConnectionError(f"SSH connection failed: {exc}") from exc
        except RuntimeError as exc:
            # ``_run_slurm_cmd`` wraps non-zero-exit stderr into RuntimeError.
            # SLURM's scancel emits "Invalid job id specified" for unknown
            # ids; surface that as JobNotFound so callers can handle it as
            # a user-level condition rather than a transport failure.
            msg = str(exc).lower()
            if "invalid job id" in msg or "invalid job specification" in msg:
                raise JobNotFound(f"Job {job_id} not found on remote cluster") from exc
            raise RemoteCommandError(str(exc)) from exc

    def status(self, job_id: int) -> BaseJob:
        """Return a snapshot :class:`BaseJob` for *job_id*.

        Uses :meth:`queue_by_ids` (already Protocol-compliant) so both
        active and terminal jobs resolve through the same code path as
        the notification poller. Raises
        :class:`~srunx.exceptions.JobNotFound` when SLURM has no record
        of ``job_id``.

        The returned :class:`BaseJob` is a static snapshot — per the
        :class:`JobOperationsProtocol.status` contract it must NOT
        trigger a lazy ``sacct`` refresh on ``.status`` access (a local
        ``sacct`` probe against an SSH-only job id would either miss
        entirely or return a misleading result). ``_last_refresh`` is
        parked in the far future so ``BaseJob.status`` observes the
        snapshot verbatim.
        """
        import time as _time

        from srunx.exceptions import JobNotFound
        from srunx.models import BaseJob, JobStatus

        info_map = self.queue_by_ids([int(job_id)])
        info = info_map.get(int(job_id))
        if info is None:
            raise JobNotFound(f"Job {job_id} not found on remote cluster")

        try:
            status_enum = JobStatus(info.status)
        except ValueError:
            status_enum = JobStatus.UNKNOWN

        job = BaseJob(
            name=f"job_{job_id}",
            job_id=int(job_id),
            nodelist=info.nodelist,
        )
        job.status = status_enum
        # Park the lazy-refresh clock far in the future so ``.status``
        # access never triggers a local ``sacct`` subprocess for an SSH
        # job id. See :class:`JobOperationsProtocol.status` contract.
        job._last_refresh = _time.time() + 10**9
        return job

    def queue(self, user: str | None = None) -> list[BaseJob]:
        """List jobs for *user* (defaults to the profile's username).

        Adapts :meth:`list_jobs` (which yields dicts) into Pydantic
        :class:`BaseJob` objects so the return type matches
        :class:`~srunx.client_protocol.JobOperationsProtocol.queue`.
        ``user=None`` uses the profile's configured username, matching
        the Protocol's "transport's current user" contract.
        """
        from srunx.models import BaseJob, JobStatus

        effective_user = user if user is not None else self._username or None

        raw_entries = self.list_jobs(user=effective_user)
        out: list[BaseJob] = []
        for entry in raw_entries:
            status_str = str(entry.get("status", "UNKNOWN"))
            try:
                status_enum = JobStatus(status_str)
            except ValueError:
                status_enum = JobStatus.UNKNOWN
            try:
                job_id_val = int(entry["job_id"]) if entry.get("job_id") else None
            except (TypeError, ValueError):
                continue
            job = BaseJob(
                name=str(entry.get("name", "job")),
                job_id=job_id_val,
                partition=entry.get("partition"),
                nodes=entry.get("nodes"),
                gpus=entry.get("gpus"),
                elapsed_time=entry.get("elapsed_time"),
                time_limit=entry.get("time_limit"),
                user=effective_user,
            )
            job.status = status_enum
            out.append(job)
        return out

    def tail_log_incremental(
        self,
        job_id: int,
        stdout_offset: int = 0,
        stderr_offset: int = 0,
    ) -> LogChunk:
        """Return new log content since the given byte offsets.

        Thin wrapper around :meth:`get_job_output` which already returns
        ``(stdout, stderr, new_stdout_offset, new_stderr_offset)``. Pure
        function: no stdout writes, no blocking. Callers that want
        ``tail -f`` semantics poll this method in a loop.
        """

        import paramiko

        from srunx.client_protocol import LogChunk
        from srunx.exceptions import (
            TransportAuthError,
            TransportConnectionError,
            TransportTimeoutError,
        )

        try:
            stdout, stderr, new_stdout_offset, new_stderr_offset = self.get_job_output(
                int(job_id),
                stdout_offset=stdout_offset,
                stderr_offset=stderr_offset,
            )
        except paramiko.AuthenticationException as exc:
            raise TransportAuthError(f"SSH authentication failed: {exc}") from exc
        except TimeoutError as exc:
            raise TransportTimeoutError(f"SSH timed out: {exc}") from exc
        except paramiko.SSHException as exc:
            raise TransportConnectionError(f"SSH connection failed: {exc}") from exc

        return LogChunk(
            stdout=stdout,
            stderr=stderr,
            stdout_offset=new_stdout_offset,
            stderr_offset=new_stderr_offset,
        )

    # ── Workflow executor surface (Step 4) ───────────

    def run(
        self,
        job: RunnableJobType,
        *,
        workflow_name: str | None = None,
        workflow_run_id: int | None = None,
        submission_context: SubmissionRenderContext | None = None,
    ) -> RunnableJobType:
        """Submit *job* over SSH and block until it reaches a terminal status.

        Mirrors :meth:`srunx.client.Slurm.run` on the sweep/web path:

        1. If ``submission_context`` is provided, apply mount-aware
           :func:`normalize_job_for_submission` so absolute local
           ``work_dir`` / ``log_dir`` paths are rewritten to the remote
           ``mount.remote`` equivalent (and a missing ``work_dir`` falls
           back to ``context.default_work_dir``). When
           ``submission_context`` is ``None`` the job is rendered verbatim
           — preserves pre-Batch-2a behaviour for callers that haven't
           plumbed a context through yet.
        2. Render the SLURM script locally from the job's template
           (``job.template`` → default template), using
           :func:`render_job_script` so ``Job.srun_args`` / ``Job.launch_prefix``
           fallbacks apply consistently.
        3. Submit the rendered content via ``submit_sbatch_job`` (the SSH
           client writes it to a remote temp path before ``sbatch``).
        4. Record the submission in the state DB (best-effort, mirrors
           ``Slurm.submit``'s ``record_submission_from_job`` call).
        5. Fire ``on_job_submitted`` callbacks.
        6. Poll the remote status until terminal, then fire the matching
           ``on_job_completed`` / ``on_job_failed`` / ``on_job_cancelled``
           callback and return the updated job. Raises :class:`RuntimeError`
           on terminal failure so the workflow runner's retry / failure
           path triggers identically to the local ``Slurm`` executor.
        """
        # Inline imports keep the module import cost flat and mirror the
        # pattern used in ``srunx.client.Slurm`` (e.g. ``record_submission_from_job``).
        from srunx.models import (
            Job,
            JobStatus,
            ShellJob,
            render_job_script,
            render_shell_job_script,
        )
        from srunx.rendering import normalize_job_for_submission
        from srunx.template import get_template_path

        # --- 0. Mount-aware path normalization (no-op when context is None). ---
        # ``normalize_job_for_submission`` returns a ``model_copy`` when any
        # path needs rewriting, so the normalized copy is only safe to use
        # for rendering/submission; we must propagate the terminal status
        # and ``job_id`` back to the caller's original instance at the end
        # of :meth:`run`, otherwise ``WorkflowRunner``'s ``all_jobs`` check
        # sees the untouched PENDING status and declares the cell
        # incomplete even when SLURM reported COMPLETED.
        original_job = job
        job = normalize_job_for_submission(job, submission_context)

        # Resolve the template path once, honouring the Job-level override
        # if present. ``get_template_path("base")`` returns the packaged
        # default — same source of truth ``Slurm._get_default_template``
        # uses so rendered output is bit-identical to the local path.
        if isinstance(job, Job):
            template_path = job.template if job.template else get_template_path("base")
        else:
            template_path = None  # ShellJob uses its own script path

        with self._io_lock:
            self._ensure_connected()

            # --- 1. Render script locally ---
            import tempfile as _tempfile

            with _tempfile.TemporaryDirectory() as tmpdir:
                if isinstance(job, Job):
                    assert template_path is not None  # narrow for mypy
                    script_path = render_job_script(
                        template_path,
                        job,
                        output_dir=tmpdir,
                    )
                elif isinstance(job, ShellJob):
                    script_path = render_shell_job_script(job.script_path, job, tmpdir)
                else:  # pragma: no cover — Pydantic union guard
                    raise TypeError(f"Unsupported job type: {type(job).__name__}")

                with open(script_path, encoding="utf-8") as f:
                    script_content = f.read()

            # --- 2. Submit via SSH ---
            result = self._client.submit_sbatch_job(script_content, job_name=job.name)
            if result is None or not result.job_id:
                raise RuntimeError(f"Failed to submit job '{job.name}' via SSH")
            job_id = int(result.job_id)
            job.job_id = job_id
            job.status = JobStatus.PENDING

        logger.debug(f"Submitted job '{job.name}' via SSH with ID {job_id}")

        # --- 3. Record submission in state DB (best-effort) ---
        # When the adapter knows which SSH profile it represents, record
        # the true (ssh, profile, scheduler_key) triple so the poller
        # looks the job up under the right transport. When no profile
        # is bound (direct hostname constructor, legacy sweep tests),
        # fall back to the pre-V5 local triple to preserve mock-based
        # test expectations.
        if self._profile_name is not None:
            self._record_job_submission(
                job,
                workflow_name=workflow_name,
                workflow_run_id=workflow_run_id,
                transport_type="ssh",
                profile_name=self._profile_name,
                scheduler_key=f"ssh:{self._profile_name}",
                submission_source=self.submission_source,
            )
        else:
            self._record_job_submission(
                job,
                workflow_name=workflow_name,
                workflow_run_id=workflow_run_id,
            )

        # --- 4. Fire on_job_submitted callbacks ---
        for callback in self.callbacks:
            try:
                callback.on_job_submitted(job)
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    f"on_job_submitted callback failed for job {job.name}: {exc}"
                )

        # --- 5. Monitor to terminal ---
        terminal_status = self._monitor_until_terminal(job_id)
        # NOT_FOUND means all three of sacct/squeue/scontrol lost track of
        # the job (typically because MinJobAge expired before we polled on
        # a pyxis cluster where sacct is unreachable). The job almost
        # certainly ran; we just can't prove COMPLETED vs FAILED. Surfacing
        # UNKNOWN is strictly more informative than silent FAILED and lets
        # the sweep runner distinguish "provably failed" from "unknowable".
        if terminal_status == "NOT_FOUND":
            logger.warning(
                f"Job {job_id} ({job.name!r}) disappeared from sacct/squeue/"
                "scontrol before a terminal status could be confirmed; "
                "recording UNKNOWN"
            )
            job.status = JobStatus.UNKNOWN
        else:
            try:
                job.status = JobStatus(terminal_status)
            except ValueError:
                logger.warning(
                    f"Job {job_id} ({job.name!r}) returned unrecognised "
                    f"SLURM state {terminal_status!r}; recording UNKNOWN"
                )
                job.status = JobStatus.UNKNOWN

        # --- 6. Dispatch terminal callback + handle failure ---
        self._record_completion_safe(job_id, job.status)
        for callback in self.callbacks:
            try:
                if job.status == JobStatus.COMPLETED:
                    callback.on_job_completed(job)
                elif job.status == JobStatus.FAILED:
                    callback.on_job_failed(job)
                elif job.status in {JobStatus.CANCELLED, JobStatus.TIMEOUT}:
                    callback.on_job_cancelled(job)
            except Exception as exc:  # noqa: BLE001
                logger.warning(f"Terminal callback failed for job {job.name}: {exc}")

        # Propagate terminal status + job_id back to the caller's
        # original instance so the ``WorkflowRunner.all_jobs`` check
        # observes them. See the rationale above step 0.
        if original_job is not job:
            original_job.status = job.status
            original_job.job_id = job.job_id

        if job.status in {JobStatus.FAILED, JobStatus.CANCELLED, JobStatus.TIMEOUT}:
            raise RuntimeError(f"Job '{job.name}' ended with status {job.status.value}")

        return original_job

    def _monitor_until_terminal(
        self,
        job_id: int,
        poll_interval: int = 10,
        *,
        timeout: float | None = -1.0,
    ) -> str:
        """Poll remote job status until it reaches a terminal SLURM state.

        Returns the raw SLURM state string (e.g. ``"COMPLETED"``). Uses
        :meth:`get_job_status` so the lock + reconnection discipline is
        uniform with the rest of the adapter.

        ``timeout`` caps the total wait (wall-clock seconds); on expiry
        raises :class:`SSHMonitorTimeoutError` so the sweep orchestrator's
        cell-failure path records the cell as failed without tearing down
        the pooled adapter's SSH session. ``None`` waits indefinitely.
        The ``-1.0`` default is a sentinel meaning "unspecified" — in that
        case we fall back to ``SRUNX_SSH_MONITOR_TIMEOUT`` (seconds,
        ``""`` or unset means no timeout) so ops can bound hung jobs
        cluster-wide without a code change.
        """
        import time as _time

        effective_timeout: float | None
        if timeout is not None and timeout < 0:
            # Sentinel — caller did not specify a timeout. Pick up the env
            # var default (which may itself be None to preserve the
            # pre-Phase-3 "wait forever" semantics).
            effective_timeout = _resolve_monitor_timeout_default()
        else:
            effective_timeout = timeout

        terminal = {"COMPLETED", "FAILED", "CANCELLED", "TIMEOUT", "NOT_FOUND"}
        start = _time.monotonic()
        while True:
            status = self.get_job_status(job_id)
            if status in terminal:
                return status
            if (
                effective_timeout is not None
                and (_time.monotonic() - start) >= effective_timeout
            ):
                raise SSHMonitorTimeoutError(
                    f"Timed out after {effective_timeout:.1f}s waiting for "
                    f"job {job_id} to reach a terminal state (last status: "
                    f"{status!r})"
                )
            _time.sleep(poll_interval)

    @staticmethod
    def _record_job_submission(
        job: RunnableJobType,
        *,
        workflow_name: str | None,
        workflow_run_id: int | None,
        transport_type: str = "ssh",
        profile_name: str | None = None,
        scheduler_key: str | None = None,
        submission_source: str | None = None,
    ) -> None:
        """Insert a ``jobs`` row for the SSH-submitted job.

        Thin wrapper around :func:`record_submission_from_job` — same
        best-effort contract as the local :class:`Slurm` executor. For
        backward compatibility with the :meth:`run` callsite that only
        passes ``workflow_name`` / ``workflow_run_id``, when
        ``profile_name`` / ``scheduler_key`` are not provided we record
        the row as local (the original pre-V5 behaviour); callsites that
        want the SSH triple must pass them explicitly.
        """
        try:
            from srunx.db.cli_helpers import record_submission_from_job

            if profile_name is None:
                # Legacy callsite (pre-V5 style) — pass only the two
                # original kwargs so tests that mock
                # ``record_submission_from_job`` with the original
                # signature (``(job, *, workflow_name, workflow_run_id)``)
                # keep working. The DB default is local anyway.
                record_submission_from_job(
                    job,
                    workflow_name=workflow_name,
                    workflow_run_id=workflow_run_id,
                )
            else:
                resolved_scheduler_key = (
                    scheduler_key
                    if scheduler_key is not None
                    else f"ssh:{profile_name}"
                )
                record_submission_from_job(
                    job,
                    workflow_name=workflow_name,
                    workflow_run_id=workflow_run_id,
                    transport_type=transport_type,  # type: ignore[arg-type]
                    profile_name=profile_name,
                    scheduler_key=resolved_scheduler_key,
                    submission_source=submission_source,  # type: ignore[arg-type]
                )
        except Exception as exc:  # noqa: BLE001 — best-effort
            logger.debug(f"_record_job_submission failed: {exc}")

    def _record_completion_safe(self, job_id: int, status: JobStatus) -> None:
        """Record terminal status in ``jobs`` / ``job_state_transitions``.

        Targets the adapter's own ``scheduler_key`` so SSH-backed jobs
        update the remote-cluster row rather than a (possibly
        nonexistent) local row. When no profile is bound (legacy
        direct-hostname tests), falls back to ``'local'`` to preserve
        pre-V5 behaviour.
        """
        try:
            from srunx.db.cli_helpers import record_completion

            record_completion(job_id, status, scheduler_key=self.scheduler_key)
        except Exception as exc:  # noqa: BLE001 — best-effort
            logger.debug(f"_record_completion_safe failed: {exc}")

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
