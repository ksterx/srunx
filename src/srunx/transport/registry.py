"""Transport resolution + registry.

This module is the single entry point CLI commands use to pick between
local SLURM and an SSH-backed cluster. Higher layers call
:func:`resolve_transport` (context manager) and receive a
:class:`ResolvedTransport` that exposes the same
:class:`~srunx.client_protocol.JobOperationsProtocol` / queue client /
executor factory regardless of which transport was selected.

Resolution priority (see REQ-1):

    1. ``--profile <name>``
    2. ``--local``
    3. ``$SRUNX_SSH_PROFILE``
    4. local fallback (silent, preserves AC-10.2)

Banner emission (REQ-7): explicit sources print a one-line banner to
stderr; the default path stays silent so existing scripts that rely on
byte-exact CLI output keep working.

The SSH-related imports (``SlurmSSHAdapter``, ``SlurmSSHExecutorPool``,
``SubmissionRenderContext``, ``ConfigManager``) are gated inside
:func:`_build_ssh_handle` so the local fallback path never pays the
paramiko import cost (R-3).

Manual verification cheatsheet:

    $ srunx submit echo hi                # banner suppressed (default)
    $ srunx submit --local echo hi        # banner: transport: local (from --local)
    $ srunx submit --profile foo echo hi  # banner: transport: ssh:foo (from --profile)
    $ SRUNX_SSH_PROFILE=foo srunx list    # banner: transport: ssh:foo (from env)
    $ srunx list --quiet                  # banner suppressed for any source
"""

from __future__ import annotations

import os
import sys
from collections.abc import Callable, Iterator
from contextlib import contextmanager, nullcontext
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal

import typer
from rich.console import Console

from srunx.client import Slurm
from srunx.client_protocol import (
    JobOperationsProtocol,
    SlurmClientProtocol,
    WorkflowJobExecutorFactory,
)
from srunx.exceptions import TransportError
from srunx.logging import get_logger

if TYPE_CHECKING:
    import sqlite3

    from srunx.rendering import SubmissionRenderContext
    from srunx.ssh.core.config import ServerProfile

logger = get_logger(__name__)


TransportSource = Literal["--profile", "--local", "env", "default"]


@dataclass(frozen=True)
class TransportHandle:
    """Resolved transport with all Protocol clients attached.

    Shared shape between the CLI resolver (:func:`resolve_transport`) and
    the long-lived poller registry (:class:`TransportRegistry`). Every
    caller needs the same set of bindings — job ops, queue client,
    executor factory, optional submission render context — so we collect
    them in one immutable record.

    Attributes:
        scheduler_key: ``"local"`` or ``"ssh:<profile>"``. The DB axis
            used to group watches / jobs across clusters (REQ-8).
        profile_name: ``None`` for local transport; the SSH profile name
            for ``ssh:*`` transports.
        transport_type: ``"local"`` or ``"ssh"``. Matches the
            ``jobs.transport_type`` column domain.
        job_ops: CLI-facing job operations (submit / cancel / status /
            queue / tail_log_incremental).
        queue_client: Poller-facing batch query client.
        executor_factory: Context-manager factory for
            :class:`~srunx.client_protocol.WorkflowJobExecutorProtocol`.
            ``None`` is not returned — local uses a
            ``nullcontext``-wrapped singleton, SSH returns a pool's
            ``lease`` method.
        submission_context: Mount-aware render context for SSH; ``None``
            for local (no mount translation).
    """

    scheduler_key: str
    profile_name: str | None
    transport_type: Literal["local", "ssh"]
    job_ops: JobOperationsProtocol
    queue_client: SlurmClientProtocol
    executor_factory: WorkflowJobExecutorFactory | None
    submission_context: SubmissionRenderContext | None


@dataclass(frozen=True)
class ResolvedTransport:
    """CLI-level transport resolution result.

    Wraps a :class:`TransportHandle` with banner metadata (``label`` +
    ``source``) so the CLI layer can both drive the resolved transport
    and report which flag/env/default produced it.
    """

    label: str
    source: TransportSource
    handle: TransportHandle

    # Convenience shortcuts to the underlying handle so CLI call sites
    # don't need to reach through ``.handle.*`` for every attribute.
    @property
    def scheduler_key(self) -> str:
        return self.handle.scheduler_key

    @property
    def profile_name(self) -> str | None:
        return self.handle.profile_name

    @property
    def transport_type(self) -> Literal["local", "ssh"]:
        return self.handle.transport_type

    @property
    def job_ops(self) -> JobOperationsProtocol:
        return self.handle.job_ops

    @property
    def queue_client(self) -> SlurmClientProtocol:
        return self.handle.queue_client

    @property
    def executor_factory(self) -> WorkflowJobExecutorFactory | None:
        return self.handle.executor_factory

    @property
    def submission_context(self) -> SubmissionRenderContext | None:
        return self.handle.submission_context


def _emit_banner(resolved: ResolvedTransport, quiet: bool) -> None:
    """Emit the one-line transport banner on stderr.

    REQ-7 / AC-10.2: the default source (no flag, no env) stays silent
    so existing scripts that diff stderr byte-for-byte keep passing.
    ``quiet=True`` suppresses the banner even for explicit sources.
    """
    if quiet or resolved.source == "default":
        return
    Console(file=sys.stderr).print(
        f"[dim]→ transport: {resolved.label} (from {resolved.source})[/dim]"
    )


def _build_local_handle(slurm: Slurm | None = None) -> TransportHandle:
    """Build a :class:`TransportHandle` for local SLURM.

    Reuses *slurm* when provided (keeps singleton semantics inside a
    single CLI command) or mints a fresh :class:`Slurm` instance. The
    executor factory wraps the shared client in a ``nullcontext`` so the
    signature matches :data:`WorkflowJobExecutorFactory` — local
    submission has no pool teardown to do.
    """
    local = slurm or Slurm()

    def factory() -> Any:
        return nullcontext(local)

    return TransportHandle(
        scheduler_key="local",
        profile_name=None,
        transport_type="local",
        job_ops=local,
        queue_client=local,
        executor_factory=factory,
        submission_context=None,
    )


def _build_ssh_handle(
    profile_name: str,
) -> tuple[TransportHandle, Any]:
    """Build an SSH :class:`TransportHandle` and its backing executor pool.

    Imports are local so ``SlurmSSHAdapter`` / paramiko / pool module
    costs are never paid by CLI invocations that stay on local SLURM
    (R-3 performance requirement).

    Returns:
        A ``(handle, pool)`` tuple. The caller is responsible for closing
        the pool when the handle goes out of scope.

    Raises:
        TransportError: If the SSH profile is unknown or the adapter
            factory rejects the configuration.
    """
    # Conditional imports — see module docstring.
    from srunx.rendering import SubmissionRenderContext
    from srunx.ssh.core.config import ConfigManager
    from srunx.web.ssh_adapter import SlurmSSHAdapter, SlurmSSHAdapterSpec
    from srunx.web.ssh_executor import SlurmSSHExecutorPool

    cm = ConfigManager()
    profile = cm.get_profile(profile_name)
    if profile is None:
        raise TransportError(
            f"SSH profile '{profile_name}' not found. "
            "Configure via 'srunx ssh profile add' or check "
            "'srunx ssh profile list'."
        )

    try:
        adapter = SlurmSSHAdapter(profile_name=profile_name)
    except ValueError as exc:
        raise TransportError(str(exc)) from exc

    # Build the pool off the adapter's own connection spec so pooled
    # clones inherit the exact same resolved hostname / identity file /
    # proxy_jump / env_vars the singleton adapter uses.
    spec: SlurmSSHAdapterSpec = adapter.connection_spec
    pool = SlurmSSHExecutorPool(spec, callbacks=None, size=2)

    # SubmissionRenderContext carries mount information for path
    # translation. We only construct it when the profile actually
    # declares mounts; otherwise downstream code should render paths
    # verbatim.
    mounts = tuple(profile.mounts) if profile.mounts else ()
    render_context: SubmissionRenderContext | None = None
    if mounts:
        render_context = SubmissionRenderContext(
            mount_name=None,
            mounts=mounts,
            default_work_dir=None,
        )

    handle = TransportHandle(
        scheduler_key=f"ssh:{profile_name}",
        profile_name=profile_name,
        transport_type="ssh",
        job_ops=adapter,
        queue_client=adapter,
        executor_factory=pool.lease,
        submission_context=render_context,
    )
    return handle, pool


@contextmanager
def resolve_transport(
    *,
    profile: str | None = None,
    local: bool = False,
    quiet: bool = False,
    banner: bool = True,
) -> Iterator[ResolvedTransport]:
    """Resolve transport for one CLI invocation.

    Resolution order (REQ-1):

        1. ``--profile <name>``
        2. ``--local``
        3. ``$SRUNX_SSH_PROFILE``
        4. local fallback (silent)

    Args:
        profile: Value of ``--profile`` (explicit SSH profile name).
        local: Value of ``--local`` (force local transport, overriding
            ``$SRUNX_SSH_PROFILE``).
        quiet: Suppress the stderr transport banner even for explicit
            sources.
        banner: Emit the one-line banner. Set ``False`` for tests or
            library-style callers that don't want any stderr output.

    Yields:
        A :class:`ResolvedTransport` for the duration of the ``with``
        block. SSH pools are closed on exit.

    Raises:
        typer.BadParameter: When ``--profile`` and ``--local`` are both
            set (REQ-1, AC-1.2).
        TransportError: When an explicit / env-selected SSH profile is
            unknown or the adapter factory rejects it.
    """
    if profile and local:
        raise typer.BadParameter(
            "--profile and --local cannot be used together.",
            param_hint="--profile / --local",
        )

    env_profile = os.environ.get("SRUNX_SSH_PROFILE")
    pool: Any = None
    handle: TransportHandle
    source: TransportSource

    if profile:
        source = "--profile"
        handle, pool = _build_ssh_handle(profile)
    elif local:
        source = "--local"
        handle = _build_local_handle()
    elif env_profile:
        source = "env"
        handle, pool = _build_ssh_handle(env_profile)
    else:
        source = "default"
        handle = _build_local_handle()

    resolved = ResolvedTransport(
        label=handle.scheduler_key,
        source=source,
        handle=handle,
    )

    if banner:
        _emit_banner(resolved, quiet)

    try:
        yield resolved
    finally:
        if pool is not None:
            try:
                pool.close()
            except Exception as exc:  # noqa: BLE001 — best-effort cleanup
                logger.debug("Pool close failed (non-fatal): %s", exc)


class TransportRegistry:
    """Resolve ``scheduler_key`` values to :class:`TransportHandle` instances.

    Two lifecycles share this class:

    * CLI: one instance per command, :meth:`close` at the end.
    * Web app / poller lifespan: one instance per process, :meth:`close`
      on shutdown.

    The registry caches handles by ``scheduler_key`` and tracks any SSH
    executor pools it has created so :meth:`close` can drain them
    uniformly.
    """

    def __init__(
        self,
        *,
        local_client: Slurm | None = None,
        profile_loader: Callable[[str], ServerProfile | None] | None = None,
    ) -> None:
        self._local_client = local_client or Slurm()
        if profile_loader is None:
            # Match _build_ssh_handle's default path — use ConfigManager.
            # We import inside __init__ to keep the module import graph
            # light for callers that inject their own profile_loader
            # (common in tests).
            from srunx.ssh.core.config import ConfigManager

            cm = ConfigManager()
            profile_loader = cm.get_profile
        self._profile_loader = profile_loader
        self._cache: dict[str, TransportHandle] = {}
        self._pools: list[Any] = []

    def resolve(self, scheduler_key: str) -> TransportHandle | None:
        """Resolve ``scheduler_key`` to a :class:`TransportHandle`.

        Returns ``None`` for unknown SSH profiles or malformed keys so
        the poller can log a warning and skip the affected group
        without crashing the whole cycle (AC-8.5).
        """
        if scheduler_key in self._cache:
            return self._cache[scheduler_key]

        if scheduler_key == "local":
            handle = _build_local_handle(self._local_client)
            self._cache[scheduler_key] = handle
            return handle

        if scheduler_key.startswith("ssh:"):
            profile_name = scheduler_key[4:]
            if not profile_name or self._profile_loader(profile_name) is None:
                return None
            try:
                handle, pool = _build_ssh_handle(profile_name)
            except TransportError as exc:
                logger.warning(
                    "Failed to build SSH transport %r: %s", scheduler_key, exc
                )
                return None
            self._cache[scheduler_key] = handle
            if pool is not None:
                self._pools.append(pool)
            return handle

        return None

    def known_scheduler_keys(self, db_connection: sqlite3.Connection) -> set[str]:
        """Return every distinct ``scheduler_key`` currently persisted.

        Poller group-by entry point (REQ-8): the V5 schema's
        ``jobs.scheduler_key`` column is the authoritative axis. Unknown
        keys in the result set are handed back unfiltered — it's the
        caller's job to decide whether to warn+skip on an unresolvable
        one.
        """
        rows = db_connection.execute(
            "SELECT DISTINCT scheduler_key FROM jobs"
        ).fetchall()
        return {r[0] for r in rows if r[0]}

    def close(self) -> None:
        """Release every cached SSH pool. Idempotent."""
        for pool in self._pools:
            try:
                pool.close()
            except Exception as exc:  # noqa: BLE001 — best-effort cleanup
                logger.debug("Pool close failed (non-fatal): %s", exc)
        self._pools.clear()
        self._cache.clear()
