"""SSH-based SLURM client for the Web UI / CLI / MCP transport surface.

Wraps :class:`srunx.ssh.core.client.SSHSlurmClient` to provide all
operations the web / CLI / MCP layers need (``list_jobs``, ``cancel``,
``get_resources``, ``run`` for the workflow executor surface). Sibling
of :class:`srunx.slurm.clients.local.LocalClient` — both implement the
same :class:`~srunx.slurm.protocols.JobOperations` /
:class:`~srunx.slurm.protocols.WorkflowJobExecutor` Protocols, just
against different transports.

Cohesive helper clusters live alongside in private modules:

* :mod:`._ssh_helpers` — shared free helpers (``_run_slurm_cmd``,
  ``_validate_identifier``, ``_MountsOnly``, regexes).
* :mod:`._ssh_types` — :class:`SlurmSSHClientSpec`,
  :class:`SSHMonitorTimeoutError`, ``_resolve_monitor_timeout_default``.
* :mod:`._ssh_resources` — sinfo / squeue resource queries.
* :mod:`._ssh_queries` — squeue / sacct job listing + parsing.
* :mod:`._ssh_recording` — best-effort DB recording wrappers.

The class methods (``get_resources`` / ``list_jobs`` / etc.) are 1-line
forwards into those modules so the parsing / resource logic is
independently reviewable and testable, while the class still owns the
connection lock + lifecycle.
"""

from __future__ import annotations

import threading
from collections.abc import Sequence
from pathlib import Path
from typing import TYPE_CHECKING, Any

from srunx.callbacks import Callback
from srunx.common.logging import get_logger

# Imported at runtime (not under TYPE_CHECKING) because the literal types
# are referenced in default-value position on ``_record_job_submission``,
# which ``from __future__ import annotations`` does not defer.
from srunx.slurm.clients._ssh_helpers import _MountsOnly, _run_slurm_cmd
from srunx.slurm.clients._ssh_queries import (
    get_job as _get_job,
)
from srunx.slurm.clients._ssh_queries import (
    list_active_jobs as _list_active_jobs_impl,
)
from srunx.slurm.clients._ssh_queries import (
    list_jobs as _list_jobs_impl,
)
from srunx.slurm.clients._ssh_queries import (
    queue_by_ids as _queue_by_ids_impl,
)
from srunx.slurm.clients._ssh_recording import (
    record_completion_safe as _record_completion_safe_impl,
)
from srunx.slurm.clients._ssh_recording import (
    record_job_submission as _record_job_submission_impl,
)
from srunx.slurm.clients._ssh_resources import (
    cluster_snapshot as _cluster_snapshot_impl,
)
from srunx.slurm.clients._ssh_resources import (
    list_partition_resources as _list_partition_resources_impl,
)
from srunx.slurm.clients._ssh_types import (
    SlurmSSHClientSpec,
    SSHMonitorTimeoutError,
    _resolve_monitor_timeout_default,
)
from srunx.ssh.core.client import SSHSlurmClient
from srunx.ssh.core.config import ConfigManager, MountConfig
from srunx.ssh.core.ssh_config import SSHConfigParser

if TYPE_CHECKING:
    from srunx.domain import BaseJob, JobStatus, RunnableJobType
    from srunx.runtime.rendering import SubmissionRenderContext
    from srunx.slurm.protocols import JobSnapshot, LogChunk

# Re-exports so ``from srunx.slurm.clients.ssh import X`` keeps working
# for the spec / exception types — both used by importers that don't
# need the client itself (executor pool, transport registry, tests).
__all__ = ["SlurmSSHClient", "SlurmSSHClientSpec", "SSHMonitorTimeoutError"]

logger = get_logger(__name__)


class SlurmSSHClient:
    """SSH-transport SLURM client. Sibling of :class:`LocalClient`."""

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
        # reproduce this client in the pool factory. Populated below in
        # both the profile_name and direct-hostname branches.
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

        # Callbacks attached to this client; invoked by :meth:`run` on the
        # sweep path. Mirrors ``Slurm.callbacks`` in ``srunx.slurm.local``.
        self.callbacks: list[Callback] = list(callbacks) if callbacks else []

        if profile_name:
            cm = ConfigManager()
            profile = cm.get_profile(profile_name)
            if not profile:
                raise ValueError(f"SSH profile '{profile_name}' not found")

            self._mounts = tuple(profile.mounts) if profile.mounts else ()
            if profile.env_vars:
                self._env_vars = dict(profile.env_vars)

            # Resolve connection: ssh_host (from ~/.ssh/config) or direct fields.
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
                # Resolve hostname via ~/.ssh/config if it's an alias.
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
        """Return the V5 transport axis for this client.

        ``"local"`` when no profile is bound (legacy direct-hostname
        tests) or ``f"ssh:{profile_name}"`` otherwise. Exposed publicly
        so callers (Web routers, poller, etc.) don't reach into
        ``_profile_name`` to build target_refs / scheduler_keys.
        """
        if self._profile_name is None:
            return "local"
        return f"ssh:{self._profile_name}"

    # ── Connection spec (for the pool factory) ────

    @property
    def connection_spec(self) -> SlurmSSHClientSpec:
        """Return the immutable connection spec for cloning this client.

        The sweep pool uses this spec to mint per-cell client clones off
        the shared singleton template without copying any live paramiko
        / SFTP state. Reading the spec does NOT touch the wire, so it is
        safe to call without holding ``_io_lock``.
        """
        return SlurmSSHClientSpec(
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
        spec: SlurmSSHClientSpec,
        *,
        callbacks: Sequence[Callback] | None = None,
        submission_source: str = "web",
    ) -> SlurmSSHClient:
        """Create a fresh client from a connection spec.

        The returned client is NOT connected; it connects lazily on first
        SSH I/O (via ``_ensure_connected``). Used by the pool factory to
        mint per-lease client clones off the singleton template without
        copying any live paramiko / SFTP state.

        ``callbacks`` are attached on construction so the pool's chosen
        callback list propagates into each cloned client's ``run`` path.

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
        client = cls(
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
        client._profile_name = spec.profile_name
        return client

    @property
    def is_connected(self) -> bool:
        """Return True when the underlying paramiko session is live.

        Used by the sweep pool to decide whether a released client is
        safe to return to the free queue or should be discarded. Never
        raises — any transport-level error is treated as "not connected".
        """
        try:
            ssh = self._client.connection.ssh_client
            if ssh is None:
                return False
            transport = ssh.get_transport()
            return bool(transport is not None and transport.is_active())
        except Exception:  # noqa: BLE001
            # Connection-state probe: any failure is observably "down".
            return False

    def _set_keepalive(self) -> None:
        ssh = self._client.connection.ssh_client
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

        1. ``ssh_client is None`` — client was never connected. Happens
           for every client built by
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
            ssh = self._client.connection.ssh_client
            if ssh is None:
                logger.debug("SSH client connecting for the first time")
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

    def __enter__(self) -> SlurmSSHClient:
        self.connect()
        return self

    def __exit__(self, *args: object) -> None:
        self.disconnect()

    # ── Job query surface (delegated to _ssh_queries) ─────

    def _list_active_jobs(
        self, user: str | None = None
    ) -> tuple[list[dict[str, Any]], set[int]]:
        return _list_active_jobs_impl(self, user)

    def list_jobs(self, user: str | None = None) -> list[dict[str, Any]]:
        return _list_jobs_impl(self, user)

    def queue_by_ids(self, job_ids: list[int]) -> dict[int, JobSnapshot]:
        return _queue_by_ids_impl(self, job_ids)

    def get_job(self, job_id: int) -> dict[str, Any]:
        return _get_job(self, job_id)

    # ── Legacy aliases retained for pre-Protocol callers ──

    def cancel_job(self, job_id: int) -> None:
        """Cancel a SLURM job via scancel.

        Legacy alias retained for pre-Protocol callers
        (``web.routers.jobs`` / ``web.routers.workflows``). New code
        should call :meth:`cancel` instead, which raises typed
        transport exceptions and aligns with
        :class:`~srunx.slurm.protocols.JobOperations`. This
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
        with a :class:`~srunx.domain.Job` instance. This alias will
        remain until those routers migrate to the Protocol surface.
        """
        with self._io_lock:
            self._ensure_connected()
            result = self._client.slurm.submit_sbatch_job(
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

    def submit_remote_sbatch(
        self,
        remote_path: str,
        *,
        submit_cwd: str | None = None,
        job_name: str | None = None,
        dependency: str | None = None,
        extra_sbatch_args: list[str] | None = None,
        callbacks_job: Any = None,
    ) -> Any:
        """Submit a script already present on the remote cluster.

        Used by the CLI's in-place execution path (mount-resident
        ShellJob after rsync). Distinct from :meth:`submit_job` —
        which ships a fresh ``script_content`` to ``/tmp/srunx/`` —
        because here the script is user-managed under a synced mount
        and must be executed verbatim without tmp-copy or
        ``-o`` auto-override.

        Records the submission to the state DB and fires the
        ``on_job_submitted`` callbacks just like :meth:`submit`, so
        Notification watches and ``srunx history`` see the job. Codex
        blocker #2 on PR #134 — the previous implementation skipped
        both, leaving in-place submissions invisible to the rest of
        the system.

        ``callbacks_job`` is the in-memory :class:`ShellJob` /
        :class:`Job` to record + callback against. When ``None`` we
        fall back to a synthesised :class:`ShellJob` so the legacy
        ``dict``-returning call sites keep working, but new callers
        should pass the real instance so the DB row carries the
        full job metadata.
        """
        from srunx.domain import JobStatus, ShellJob

        with self._io_lock:
            self._ensure_connected()
            result = self._client.slurm.submit_remote_sbatch_file(
                remote_path,
                submit_cwd=submit_cwd,
                job_name=job_name,
                dependency=dependency,
                extra_sbatch_args=extra_sbatch_args,
            )
        if result is None or not result.job_id:
            raise RuntimeError("remote sbatch submission failed")
        job_id = int(result.job_id)

        # Bind the result to a Job-shaped object so DB recording +
        # callbacks have a real object to pass around. When the caller
        # supplied one we mutate it in place (the CLI uses the returned
        # job_id + status downstream); otherwise we synthesise a thin
        # ShellJob.
        if callbacks_job is None:
            callbacks_job = ShellJob(
                name=result.name or job_name or "job",
                script_path=remote_path,
            )
        callbacks_job.job_id = job_id
        callbacks_job.status = JobStatus.PENDING
        if isinstance(callbacks_job, ShellJob):
            callbacks_job.script_path = remote_path

        # Record to DB so `srunx history` / poller pick it up. Mirrors
        # :meth:`submit` step 3.
        if self._profile_name is not None:
            self._record_job_submission(
                callbacks_job,
                workflow_name=None,
                workflow_run_id=None,
                transport_type="ssh",
                profile_name=self._profile_name,
                scheduler_key=f"ssh:{self._profile_name}",
                submission_source=self.submission_source,
            )
        else:
            self._record_job_submission(
                callbacks_job, workflow_name=None, workflow_run_id=None
            )

        # Fire on_job_submitted callbacks. Mirrors :meth:`submit` step 4.
        for callback in self.callbacks:
            try:
                callback.on_job_submitted(callbacks_job)
            except Exception as exc:  # noqa: BLE001
                # Callback failures must not abort submission — the
                # job is already in SLURM. Mirror :meth:`run` policy.
                logger.warning(
                    f"on_job_submitted callback failed for job "
                    f"{callbacks_job.name}: {exc}"
                )

        return callbacks_job

    def get_job_output(
        self,
        job_id: int,
        job_name: str | None = None,
        stdout_offset: int = 0,
        stderr_offset: int = 0,
        last_n: int | None = None,
    ) -> tuple[str, str, int, int]:
        """Get job stdout/stderr log contents from remote.

        Returns ``(stdout, stderr, new_stdout_offset, new_stderr_offset)``.

        ``last_n`` is the initial-read optimization: when both offsets
        are 0 and ``last_n`` is set, only the last N lines are
        transferred (via ``tail -n N`` on the remote) and the returned
        offsets point at end-of-file. This avoids shipping a multi-GB
        log across SSH when the user only wants the tail.

        Ensures the SSH connection is live before reading — callers that
        reach this method via a fresh :class:`SlurmSSHClient` (e.g. CLI
        ``srunx logs --profile foo``, which builds the client via
        :func:`srunx.transport.registry._build_ssh_handle` without the
        Web app's startup connect) would otherwise hit
        ``SSH client is not connected`` on the first call.
        """
        with self._io_lock:
            self._ensure_connected()
            return self._client.logs.get_job_output(
                str(job_id),
                job_name=job_name,
                stdout_offset=stdout_offset,
                stderr_offset=stderr_offset,
                last_n=last_n,
            )

    def get_job_status(self, job_id: int) -> str:
        """Get job status string.

        Legacy alias retained for callers that want just the raw SLURM
        state string (``mcp`` and the client's own
        :meth:`_monitor_until_terminal` loop). New code should call
        :meth:`status` which returns a full :class:`BaseJob` snapshot
        conforming to
        :class:`~srunx.slurm.protocols.JobOperations`.
        """
        with self._io_lock:
            self._ensure_connected()
            return self._client.slurm.get_job_status(str(job_id))

    def get_job_output_detailed(
        self,
        job_id: int | str,
        job_name: str | None = None,
        skip_content: bool = False,
    ) -> dict[str, str | list[str] | None]:
        """Return log-file metadata (and optionally content) for ``job_id``.

        Signature + return shape match :meth:`srunx.slurm.local.Slurm.get_job_output_detailed`
        so ``SSHWorkflowJobExecutor`` satisfies
        :class:`WorkflowJobExecutor` transparently.

        ``skip_content=True`` suppresses file content reads so callers that
        only want the primary-log path pay only the ``find`` round-trips.
        """
        with self._io_lock:
            self._ensure_connected()
            info = self._client.logs.get_job_output_detailed(
                str(job_id), job_name=job_name
            )
        if skip_content:
            # Preserve the list[str] / None / str shape expected by callers.
            info["output"] = ""
            info["error"] = ""
        return info

    # ── JobOperations surface ────────────────
    #
    # These methods align SlurmSSHClient with
    # :class:`srunx.slurm.protocols.JobOperations` so the Web UI,
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
        (``transport_type='ssh'``, ``profile_name=<this client's
        profile>``, ``scheduler_key='ssh:<profile>'``) triple so the
        poller can look the job up under the right transport. DB writes
        are best-effort and never mask an sbatch success.
        """
        import tempfile as _tempfile

        import paramiko

        from srunx.common.exceptions import (
            SubmissionError,
            TransportAuthError,
            TransportConnectionError,
            TransportTimeoutError,
        )
        from srunx.domain import (
            Job,
            JobStatus,
            ShellJob,
        )
        from srunx.runtime.rendering import (
            normalize_job_for_submission,
            render_job_script,
            render_shell_job_script,
        )
        from srunx.runtime.templates import get_template_path

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
                result = self._client.slurm.submit_sbatch_job(
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

        # Record submission with SSH transport metadata. The client
        # carries its ``submission_source`` as mutable state set by the
        # transport registry (``_build_ssh_handle``) — the Web path
        # leaves the default ``'web'``, the CLI wrapper passes
        # ``'cli'``, MCP passes ``'mcp'``. This avoids widening the
        # JobOperations signature with a kwarg that would
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

        Raises :class:`~srunx.common.exceptions.JobNotFoundError` when ``scancel``
        reports the job is missing, :class:`TransportError` subclasses
        for SSH-layer failures. The legacy :meth:`cancel_job` API is
        preserved below as a no-op alias for backwards compat.
        """

        import paramiko

        from srunx.common.exceptions import (
            JobNotFoundError,
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
            # ids; surface that as JobNotFoundError so callers can handle it as
            # a user-level condition rather than a transport failure.
            msg = str(exc).lower()
            if "invalid job id" in msg or "invalid job specification" in msg:
                raise JobNotFoundError(
                    f"Job {job_id} not found on remote cluster"
                ) from exc
            raise RemoteCommandError(str(exc)) from exc

    def status(self, job_id: int) -> BaseJob:
        """Return a snapshot :class:`BaseJob` for *job_id*.

        Uses :meth:`queue_by_ids` (already Protocol-compliant) so both
        active and terminal jobs resolve through the same code path as
        the notification poller. Raises
        :class:`~srunx.common.exceptions.JobNotFoundError` when SLURM has no record
        of ``job_id``.

        The returned :class:`BaseJob` is a static snapshot — per the
        :class:`JobOperations.status` contract it must NOT
        trigger a lazy ``sacct`` refresh on ``.status`` access (a local
        ``sacct`` probe against an SSH-only job id would either miss
        entirely or return a misleading result). ``_last_refresh`` is
        parked in the far future so ``BaseJob.status`` observes the
        snapshot verbatim.
        """
        import time as _time

        from srunx.common.exceptions import JobNotFoundError
        from srunx.domain import BaseJob, JobStatus

        info_map = self.queue_by_ids([int(job_id)])
        info = info_map.get(int(job_id))
        if info is None:
            raise JobNotFoundError(f"Job {job_id} not found on remote cluster")

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
        # job id. See :class:`JobOperations.status` contract.
        job._last_refresh = _time.time() + 10**9
        return job

    def queue(self, user: str | None = None) -> list[BaseJob]:
        """List *active* jobs (all users by default).

        Adapts :meth:`_list_active_jobs` (squeue only, no sacct merge)
        into Pydantic :class:`BaseJob` objects so the return type
        matches :class:`~srunx.slurm.protocols.JobOperations.queue` and
        the CLI ``srunx squeue`` output matches native SLURM
        ``squeue`` semantics (active jobs only — finished jobs are the
        domain of ``srunx history``).

        ``user=None`` shows **all users' jobs**, matching native
        ``squeue`` and the local :meth:`~srunx.slurm.local.Slurm.queue`.
        Pass a username explicitly (e.g. from ``-u`` / ``--user``) to
        filter to that user.
        """
        from srunx.domain import BaseJob, JobStatus

        raw_entries, _ = self._list_active_jobs(user=user)
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
                user=entry.get("user"),
                nodes=entry.get("nodes"),
                cpus=entry.get("cpus"),
                gpus=entry.get("gpus"),
                nodelist=entry.get("nodelist") or None,
                elapsed_time=entry.get("elapsed_time"),
                time_limit=entry.get("time_limit"),
            )
            job.status = status_enum
            out.append(job)
        return out

    def tail_log_incremental(
        self,
        job_id: int,
        stdout_offset: int = 0,
        stderr_offset: int = 0,
        last_n: int | None = None,
    ) -> LogChunk:
        """Return new log content since the given byte offsets.

        Thin wrapper around :meth:`get_job_output` which already returns
        ``(stdout, stderr, new_stdout_offset, new_stderr_offset)``. Pure
        function: no stdout writes, no blocking. Callers that want
        ``tail -f`` semantics poll this method in a loop.

        ``last_n`` honours the Protocol hint — applied by the underlying
        ``RemoteLogReader`` when both offsets are 0 so a ``tail -f -n N``
        kicks off with only the tail of a large log on the wire.
        """

        import paramiko

        from srunx.common.exceptions import (
            TransportAuthError,
            TransportConnectionError,
            TransportTimeoutError,
        )
        from srunx.slurm.protocols import LogChunk

        try:
            stdout, stderr, new_stdout_offset, new_stderr_offset = self.get_job_output(
                int(job_id),
                stdout_offset=stdout_offset,
                stderr_offset=stderr_offset,
                last_n=last_n,
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

    # ── Workflow executor surface ────────────────

    def run(
        self,
        job: RunnableJobType,
        *,
        workflow_name: str | None = None,
        workflow_run_id: int | None = None,
        submission_context: SubmissionRenderContext | None = None,
    ) -> RunnableJobType:
        """Submit *job* over SSH and block until it reaches a terminal status.

        Mirrors :meth:`srunx.slurm.local.Slurm.run` on the sweep/web path:

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

        IN_PLACE submission (skip the temp upload, run the user's
        mount-resident script verbatim) is gated on
        ``submission_context.allow_in_place`` so callers that do NOT
        hold the per-(profile, mount) sync lock cannot race a
        concurrent rsync. CLI workflow runs flip the flag inside
        :func:`srunx.cli.workflow.mounts._hold_workflow_mounts`; Web /
        MCP paths leave it ``False`` and keep the safe temp-upload
        behaviour. Closes Codex blocker #3 on PR #141.
        """
        allow_in_place = (
            submission_context.allow_in_place
            if submission_context is not None
            else False
        )
        # Inline imports keep the module import cost flat and mirror the
        # pattern used in ``srunx.slurm.local.Slurm`` (e.g. ``record_submission_from_job``).
        from srunx.domain import (
            Job,
            JobStatus,
            ShellJob,
        )
        from srunx.runtime.rendering import (
            normalize_job_for_submission,
            render_job_script,
            render_shell_job_script,
        )
        from srunx.runtime.templates import get_template_path

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

            # --- 1. Render script locally + decide IN_PLACE vs TEMP_UPLOAD ---
            #
            # Phase 2 (#135): when the source ShellJob lives under a
            # configured mount AND its rendered bytes equal the source
            # bytes (no Jinja substitution happened), we can skip the
            # temp-upload and run the user-managed file in place via
            # ``submit_remote_sbatch_file``. This preserves the user's
            # ``#SBATCH`` directives and avoids the ``-o`` injection
            # that ``submit_sbatch_job`` does not, but the legacy
            # tempfile path performs.
            import tempfile as _tempfile

            from srunx.runtime.submission_plan import (
                render_matches_source,
                resolve_mount_for_path,
                translate_local_to_remote,
            )

            in_place_remote_path: str | None = None
            in_place_submit_cwd: str | None = None

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
                    # IN_PLACE eligibility check — only attempted when
                    # the caller passed ``allow_in_place=True``,
                    # confirming they're holding the per-(profile,
                    # mount) sync lock for the lifetime of this call.
                    # Without that lock a concurrent rsync could swap
                    # the bytes between our render check and the
                    # cluster's sbatch read. Codex blocker #3 on
                    # PR #141.
                    if allow_in_place and self._mounts:
                        source_path: Path | None
                        try:
                            source_path = Path(job.script_path)
                        except (OSError, ValueError):
                            source_path = None
                        if source_path is not None and source_path.exists():
                            # Build a duck-typed "profile" carrying just
                            # the mounts; ``resolve_mount_for_path`` only
                            # reads ``.mounts``.
                            mount = resolve_mount_for_path(
                                source_path,
                                _MountsOnly(self._mounts),  # type: ignore[arg-type]
                            )
                            if mount is not None and render_matches_source(
                                Path(script_path), source_path
                            ):
                                # Defence-in-depth (#143): when the
                                # caller declared a locked mount-set,
                                # refuse IN_PLACE for any mount outside
                                # it. Sweep mount aggregation should
                                # already cover every cell; if it
                                # didn't, a silent race against rsync
                                # is the worst outcome — fail loud.
                                # Empty ``locked_mount_names`` keeps
                                # every pre-#143 caller working
                                # unchanged.
                                locked = (
                                    submission_context.locked_mount_names
                                    if submission_context is not None
                                    else ()
                                )
                                if locked and mount.name not in locked:
                                    raise RuntimeError(
                                        f"IN_PLACE rejected: ShellJob "
                                        f"'{job.name}' resolves to mount "
                                        f"'{mount.name}' which is not in the "
                                        f"locked mount set {sorted(locked)!r}. "
                                        "This indicates a sweep cell escaped "
                                        "the per-cell mount aggregation; "
                                        "please file an srunx bug."
                                    )
                                in_place_remote_path = translate_local_to_remote(
                                    source_path, mount
                                )
                                # Prefer the script's own directory on
                                # the remote so relative ``#SBATCH
                                # --output=`` paths resolve where the
                                # user expects.
                                in_place_submit_cwd = translate_local_to_remote(
                                    source_path.parent, mount
                                )

                with open(script_path, encoding="utf-8") as f:
                    script_content = f.read()

            # --- 2. Submit via SSH ---
            if in_place_remote_path is not None:
                result = self._client.slurm.submit_remote_sbatch_file(
                    in_place_remote_path,
                    submit_cwd=in_place_submit_cwd,
                    job_name=job.name,
                )
            else:
                result = self._client.slurm.submit_sbatch_job(
                    script_content, job_name=job.name
                )
            if result is None or not result.job_id:
                raise RuntimeError(f"Failed to submit job '{job.name}' via SSH")
            job_id = int(result.job_id)
            job.job_id = job_id
            job.status = JobStatus.PENDING

        logger.debug(f"Submitted job '{job.name}' via SSH with ID {job_id}")

        # --- 3. Record submission in state DB (best-effort) ---
        # When the client knows which SSH profile it represents, record
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
                # Callback failures are observability-side only; the
                # job is in SLURM. Mirror :meth:`Slurm.run` behaviour.
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
                # Same callback-failure policy as the on_job_submitted hook.
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
        uniform with the rest of the client.

        ``timeout`` caps the total wait (wall-clock seconds); on expiry
        raises :class:`SSHMonitorTimeoutError` so the sweep orchestrator's
        cell-failure path records the cell as failed without tearing down
        the pooled client's SSH session. ``None`` waits indefinitely.
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

    # ── DB recording helpers (delegated to _ssh_recording) ──

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
        # ``transport_type`` / ``submission_source`` are typed plain ``str``
        # here even though the underlying recorder expects ``Literal``s
        # because callers thread in ``self.submission_source`` (also plain
        # ``str``), which can carry values outside the ``SubmissionSource``
        # Literal (notably ``"mcp"`` from the MCP transport registry).
        # Casting at this single boundary keeps the static type clean
        # without lying to the recorder.
        _record_job_submission_impl(
            job,
            workflow_name=workflow_name,
            workflow_run_id=workflow_run_id,
            transport_type=transport_type,  # type: ignore[arg-type]
            profile_name=profile_name,
            scheduler_key=scheduler_key,
            submission_source=submission_source,  # type: ignore[arg-type]
        )

    def _record_completion_safe(self, job_id: int, status: JobStatus) -> None:
        _record_completion_safe_impl(job_id, status, scheduler_key=self.scheduler_key)

    # ── Resource Operations (delegated to _ssh_resources) ───

    def get_resources(self, partition: str | None = None) -> list[dict[str, Any]]:
        return _list_partition_resources_impl(self, partition)

    def get_cluster_snapshot(self) -> dict[str, Any]:
        return _cluster_snapshot_impl(self)

    def _get_partition_resources(self, partition: str) -> dict[str, Any]:
        from srunx.slurm.clients._ssh_resources import partition_resources

        return partition_resources(self, partition)
