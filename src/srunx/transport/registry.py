"""Transport resolution + registry.

This module is the single entry point CLI commands use to pick between
local SLURM and an SSH-backed cluster. Higher layers call
:func:`resolve_transport` (context manager) and receive a
:class:`ResolvedTransport` that exposes the same
:class:`~srunx.slurm.protocols.JobOperations` / queue client /
executor factory regardless of which transport was selected.

Resolution priority (see REQ-1):

    1. ``--profile <name>``
    2. ``--local``
    3. ``$SRUNX_SSH_PROFILE``
    4. local fallback (silent, preserves AC-10.2)

Banner emission (REQ-7): explicit sources print a one-line banner to
stderr; the default path stays silent so existing scripts that rely on
byte-exact CLI output keep working.

The SSH-related imports (``SlurmSSHClient``, ``SlurmSSHExecutorPool``,
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
import threading
from collections.abc import Callable, Iterator, Sequence
from contextlib import contextmanager, nullcontext
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal

import typer
from rich.console import Console

from srunx.common.exceptions import TransportError
from srunx.common.logging import get_logger
from srunx.slurm.local import Slurm
from srunx.slurm.protocols import (
    Client,
    JobOperations,
    WorkflowJobExecutorFactory,
)

if TYPE_CHECKING:
    import sqlite3

    from srunx.callbacks import Callback
    from srunx.runtime.rendering import SubmissionRenderContext
    from srunx.ssh.core.config import ServerProfile

logger = get_logger(__name__)


TransportSource = Literal["--profile", "--local", "env", "current-profile", "default"]


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
            :class:`~srunx.slurm.protocols.WorkflowJobExecutor`.
            ``None`` is not returned — local uses a
            ``nullcontext``-wrapped singleton, SSH returns a pool's
            ``lease`` method.
        submission_context: Mount-aware render context for SSH; ``None``
            for local (no mount translation).
    """

    scheduler_key: str
    profile_name: str | None
    transport_type: Literal["local", "ssh"]
    job_ops: JobOperations
    queue_client: Client
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
    def job_ops(self) -> JobOperations:
        return self.handle.job_ops

    @property
    def queue_client(self) -> Client:
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
    emit_transport_banner(label=resolved.label, source=resolved.source, quiet=quiet)


def emit_transport_banner(
    *,
    label: str,
    source: TransportSource,
    quiet: bool,
) -> None:
    """Emit the transport banner without building a full handle.

    Used by commands that only need banner + conflict detection (e.g.
    ``srunx monitor resources``, which is local-only in Phase 5b).
    Keeps the SF7 short-circuit path byte-for-byte identical to the
    full :func:`resolve_transport` output so scripts that diff stderr
    don't see a regression when we skip the SSH handle build.

    REQ-7 / AC-10.2: ``source='default'`` stays silent and ``quiet=True``
    suppresses the banner even for explicit sources.
    """
    if quiet or source == "default":
        return
    # Construct a fresh Console each call so the emitter picks up the
    # *current* ``sys.stderr``. pytest's capture swaps ``sys.stderr``
    # between tests, and a module-level cached Console would hold a
    # stale reference and raise "I/O operation on closed file" on the
    # second invocation. Construction is cheap (~µs) and banner
    # emission is not on any hot path.
    #
    # Rich markup: a colored dot flags transport kind (cyan for SSH,
    # yellow for local), the connection target is the headline, the
    # profile name + decision source trail in dim so "where am I
    # running" reads at a glance without stealing focus.
    is_ssh = label.startswith("ssh:")
    dot_color = "cyan" if is_ssh else "yellow"
    source_display = {
        "--profile": "via --profile",
        "--local": "via --local",
        "env": "via $SRUNX_SSH_PROFILE",
        "current-profile": "via current profile",
    }.get(source, f"via {source}")
    if is_ssh:
        body = _format_ssh_banner_body(
            profile_name=label.removeprefix("ssh:"),
            source_display=source_display,
        )
    else:
        body = f"[bold]Local SLURM[/bold]  [dim italic]({source_display})[/dim italic]"
    Console(file=sys.stderr).print(f"[{dot_color}]●[/{dot_color}]  {body}")


def _format_ssh_banner_body(*, profile_name: str, source_display: str) -> str:
    """Build the SSH half of the transport banner.

    Shows ``user@host`` (plus ``:port`` when non-default and
    ``via <proxy>`` when ``proxy_jump`` is set) as the headline so the
    banner answers "which machine am I reaching" without forcing the
    user to remember what ``<profile_name>`` maps to. Profile name and
    decision source are kept as parenthetical metadata.

    Falls back to ``SSH profile: <name>`` when the profile lookup
    fails (config file missing, profile removed, import error) — the
    banner must never raise, even from a degraded state.
    """
    profile = _lookup_profile_silently(profile_name)
    if profile is None:
        return (
            f"[bold]SSH profile: {profile_name}[/bold]  "
            f"[dim italic]({source_display})[/dim italic]"
        )
    target = f"{profile.username}@{profile.hostname}"
    if profile.port != 22:
        target += f":{profile.port}"
    if profile.proxy_jump:
        target += f" via {profile.proxy_jump}"
    return (
        f"[bold]Connected to[/bold] [cyan]{target}[/cyan]  "
        f"[dim italic](profile: {profile_name} · {source_display})[/dim italic]"
    )


def _lookup_profile_silently(name: str) -> ServerProfile | None:
    """Return the :class:`ServerProfile` for *name*, or None on any failure.

    Used by the banner emitter, which must never raise — a missing
    config file or a profile that was deleted between resolution and
    banner emission should degrade to the fallback label, not crash
    the CLI.
    """
    try:
        from srunx.ssh.core.config import ConfigManager
    except ImportError:
        return None
    try:
        return ConfigManager().get_profile(name)
    except Exception as exc:  # noqa: BLE001 — defensive
        logger.debug("Could not load SSH profile %r for banner: %s", name, exc)
        return None


def _current_profile_name() -> str | None:
    """Return the active SSH profile set via ``srunx ssh profile set``, or None.

    Respects :attr:`srunx.common.config.CliTransportConfig.use_current_profile` —
    when the user has opted out (``cli.use_current_profile = false``), this
    function returns ``None`` so ``resolve_transport`` falls straight
    through to local.

    Any failure (config file missing, ConfigManager import error) is
    swallowed and treated as "no current profile" — the resolver must
    never raise from this path.
    """
    try:
        from srunx.common.config import get_config
        from srunx.ssh.core.config import ConfigManager
    except ImportError:
        return None
    try:
        if not get_config().cli.use_current_profile:
            return None
        name = ConfigManager().get_current_profile_name()
    except Exception as exc:  # noqa: BLE001 — defensive
        logger.debug("Could not read current SSH profile: %s", exc)
        return None
    if not name:
        return None
    return name.strip() or None


def _resolve_source_and_profile(
    *, profile: str | None, local: bool
) -> tuple[TransportSource, str | None]:
    """Return the resolved ``(source, profile_name)`` pair.

    Shared core between :func:`peek_scheduler_key`,
    :func:`resolve_transport_source`, and :func:`resolve_transport` so the
    5-way priority ladder lives in one place.

    Priority:

        1. ``--profile <name>``
        2. ``--local``
        3. ``$SRUNX_SSH_PROFILE``
        4. active SSH profile (``srunx ssh profile set``) when
           ``cli.use_current_profile`` is True (default)
        5. local fallback (silent)

    Raises :class:`typer.BadParameter` on the same ``--profile`` +
    ``--local`` conflict so every entry point fails consistently.
    """
    profile = profile.strip() if profile else profile
    if profile == "":
        raise typer.BadParameter(
            "--profile cannot be empty or whitespace.",
            param_hint="--profile",
        )
    if profile and local:
        raise typer.BadParameter(
            "--profile and --local cannot be used together.",
            param_hint="--profile / --local",
        )
    if profile:
        return "--profile", profile
    if local:
        return "--local", None
    env_profile = os.environ.get("SRUNX_SSH_PROFILE", "").strip()
    if env_profile:
        return "env", env_profile
    current = _current_profile_name()
    if current:
        return "current-profile", current
    return "default", None


def resolve_transport_source(
    *, profile: str | None = None, local: bool = False
) -> TransportSource:
    """Return the :data:`TransportSource` ``resolve_transport`` would pick.

    Pure helper that mirrors the source-detection limb of
    :func:`resolve_transport` so callers using
    :func:`emit_transport_banner` directly don't have to duplicate the
    precedence rules. Raises :class:`typer.BadParameter` on the same
    ``--profile`` + ``--local`` conflict.
    """
    source, _ = _resolve_source_and_profile(profile=profile, local=local)
    return source


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


def _resolve_submission_context(
    *,
    profile_name: str,
    profile_mounts: Sequence[Any] | None,
    mount_name: str | None,
) -> SubmissionRenderContext | None:
    """Decide the ``SubmissionRenderContext`` for an SSH profile.

    Split out of :func:`_build_ssh_handle` so the mount auto-selection
    policy lives in one place and can be tested independently.

    Returns ``None`` when the profile has no mounts at all (no
    translation possible). Otherwise returns a context with
    ``mount_name`` set to either the caller's explicit choice, the
    single mount's name (auto-selection), or ``None`` when the profile
    declares multiple mounts and the caller did not pick one (logs a
    warning so the silent no-translation fallback is visible).
    """
    from srunx.runtime.rendering import SubmissionRenderContext

    mounts = tuple(profile_mounts) if profile_mounts else ()
    if not mounts:
        return None

    resolved_mount_name = mount_name
    if resolved_mount_name is None:
        if len(mounts) == 1:
            resolved_mount_name = mounts[0].name
        else:
            logger.warning(
                "SSH profile %r declares %d mounts; no mount selected "
                "so path translation is disabled. Pass mount_name "
                "explicitly to enable translation.",
                profile_name,
                len(mounts),
            )
    return SubmissionRenderContext(
        mount_name=resolved_mount_name,
        mounts=mounts,
        default_work_dir=None,
    )


def _build_ssh_handle(
    profile_name: str,
    *,
    submission_source: str,
    callbacks: Sequence[Callback] | None = None,
    mount_name: str | None = None,
    pool_size: int = 2,
) -> tuple[TransportHandle, Any]:
    """Build an SSH :class:`TransportHandle` and its backing executor pool.

    Imports are local so ``SlurmSSHClient`` / paramiko / pool module
    costs are never paid by CLI invocations that stay on local SLURM
    (R-3 performance requirement).

    Args:
        profile_name: Name of the SSH profile to resolve.
        callbacks: Optional callbacks to attach to both the singleton
            adapter (used for ad-hoc ``submit`` / ``status`` / ``queue``
            ops) and every pooled clone (used by the sweep / workflow
            execution path). Defaults to ``None`` so routes that don't
            run through :class:`NotificationWatchCallback` don't pay any
            callback cost.
        submission_source: Origin tag recorded on the ``jobs`` row for
            every job submitted through this handle. Routers leave the
            default ``'web'``; CLI passes ``'cli'``; MCP passes ``'mcp'``.
        mount_name: Explicit mount selection for path translation. When
            ``None`` and the profile declares exactly one mount we
            auto-select it so ``flow run --profile`` gets mount
            translation out of the box. Multi-mount profiles with no
            explicit ``mount_name`` fall back to "no translation" and
            emit a warning.
        pool_size: Number of pooled SSH adapters. Default ``2`` for
            single-shot CLI / MCP. Long-lived callers (Web app lifespan)
            should pass a higher value (e.g. 8) to handle concurrent
            request traffic.

    Returns:
        A ``(handle, pool)`` tuple. The caller is responsible for closing
        the pool when the handle goes out of scope.

    Raises:
        TransportError: If the SSH profile is unknown or the adapter
            factory rejects the configuration.
    """
    # Conditional imports — see module docstring.
    from srunx.slurm.clients.ssh import SlurmSSHClient, SlurmSSHClientSpec
    from srunx.slurm.ssh_executor import SlurmSSHExecutorPool
    from srunx.ssh.core.config import ConfigManager

    cm = ConfigManager()
    profile = cm.get_profile(profile_name)
    if profile is None:
        raise TransportError(
            f"SSH profile '{profile_name}' not found. "
            "Configure via 'srunx ssh profile add' or check "
            "'srunx ssh profile list'."
        )

    try:
        adapter = SlurmSSHClient(
            profile_name=profile_name,
            callbacks=callbacks,
            submission_source=submission_source,
        )
    except ValueError as exc:
        raise TransportError(str(exc)) from exc

    # Build the pool off the adapter's own connection spec so pooled
    # clones inherit the exact same resolved hostname / identity file /
    # proxy_jump / env_vars the singleton adapter uses. Pooled clones
    # also pick up the same callback list so per-cell jobs in the
    # workflow / sweep path fire ``on_job_submitted`` (including
    # :class:`NotificationWatchCallback`) with the adapter's
    # ``scheduler_key`` already bound.
    spec: SlurmSSHClientSpec = adapter.connection_spec
    pool = SlurmSSHExecutorPool(
        spec,
        callbacks=callbacks,
        size=pool_size,
        submission_source=submission_source,
    )

    # Any failure between pool construction and handle return would
    # orphan the pool's SSH capacity. Wrap the remaining work so the
    # pool is drained before the exception propagates (Fix F8).
    try:
        render_context = _resolve_submission_context(
            profile_name=profile_name,
            profile_mounts=profile.mounts,
            mount_name=mount_name,
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
    except Exception:
        # Best-effort: drop the pool before the caller's ``finally``
        # clause has a chance to run (it only fires when the CM actually
        # yielded a ResolvedTransport, which we never reach here).
        try:
            pool.close()
        except Exception as close_exc:  # noqa: BLE001 — best-effort cleanup
            logger.debug(
                "Pool close during orphan cleanup failed (non-fatal): %s",
                close_exc,
            )
        raise


def peek_scheduler_key(*, profile: str | None = None, local: bool = False) -> str:
    """Return the scheduler_key ``resolve_transport`` would pick.

    Pure function with no side effects — used by callers that need to
    bind callback state (e.g. ``NotificationWatchCallback.scheduler_key``)
    before entering the transport context manager. Resolution order
    matches :func:`resolve_transport` including the current-profile
    fallback (see :func:`_resolve_source_and_profile`).

    Raises :class:`typer.BadParameter` on the same ``--profile`` +
    ``--local`` conflict so callers fail consistently whether they
    peek first or go straight to :func:`resolve_transport`.
    """
    _, resolved_profile = _resolve_source_and_profile(profile=profile, local=local)
    return f"ssh:{resolved_profile}" if resolved_profile else "local"


def _build_transport_label(handle: TransportHandle) -> str:
    """Return the banner label for *handle*.

    Spec AC-7.3 prescribes the ``scheduler_key`` grammar (``local`` /
    ``ssh:<profile>``) as the banner text so the same string callers see
    on stderr matches what they'd see in DB rows / watch targets. Using
    ``handle.scheduler_key`` directly is the simplest way to keep those
    two surfaces aligned.
    """
    return handle.scheduler_key


@contextmanager
def resolve_transport(
    *,
    profile: str | None = None,
    local: bool = False,
    quiet: bool = False,
    banner: bool = True,
    callbacks: Sequence[Callback] | None = None,
    submission_source: str = "cli",
    mount_name: str | None = None,
    pool_size: int = 2,
) -> Iterator[ResolvedTransport]:
    """Resolve transport for one CLI invocation.

    Resolution order (REQ-1, Phase 2 extended):

        1. ``--profile <name>``
        2. ``--local``
        3. ``$SRUNX_SSH_PROFILE``
        4. active SSH profile (``srunx ssh profile set``) when
           ``cli.use_current_profile`` is True (default)
        5. local fallback (silent)

    Args:
        profile: Value of ``--profile`` (explicit SSH profile name).
        local: Value of ``--local`` (force local transport, overriding
            ``$SRUNX_SSH_PROFILE``).
        quiet: Suppress the stderr transport banner even for explicit
            sources.
        banner: Emit the one-line banner. Set ``False`` for tests or
            library-style callers that don't want any stderr output.
        callbacks: Optional callbacks to forward into the SSH adapter
            singleton and every pooled clone. Non-SSH paths ignore
            ``callbacks`` (local ``Slurm`` callbacks are wired at the
            ``Slurm`` callsite, not here).
        submission_source: Origin tag for ``jobs.submission_source``.
            Defaults to ``'cli'`` which is correct for every CLI entry
            point; the value is a no-op on the local path and is passed
            through to :class:`SlurmSSHClient` on SSH.
        mount_name: Explicit mount selection forwarded to the SSH
            handle builder for path translation. ``None`` triggers
            single-mount auto-selection.
        pool_size: Pool size forwarded to the SSH executor pool. Default
            ``2`` matches single-shot CLI usage; long-lived callers pass
            a larger value.

    Yields:
        A :class:`ResolvedTransport` for the duration of the ``with``
        block. SSH pools are closed on exit.

    Raises:
        typer.BadParameter: When ``--profile`` and ``--local`` are both
            set (REQ-1, AC-1.2).
        TransportError: When an explicit / env-selected SSH profile is
            unknown or the adapter factory rejects it.
    """
    source, resolved_profile = _resolve_source_and_profile(profile=profile, local=local)

    pool: Any = None
    handle: TransportHandle

    if resolved_profile is not None:
        handle, pool = _build_ssh_handle(
            resolved_profile,
            callbacks=callbacks,
            submission_source=submission_source,
            mount_name=mount_name,
            pool_size=pool_size,
        )
    else:
        handle = _build_local_handle()

    resolved = ResolvedTransport(
        label=_build_transport_label(handle),
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
        submission_source: str = "web",
    ) -> None:
        self._local_client = local_client or Slurm()
        # Origin tag the registry attaches to every SSH handle it builds.
        # Web app lifespan uses the default ``"web"``; CLI-scoped registries
        # (rare today) could construct with ``"cli"``. The value flows into
        # ``_build_ssh_handle`` so ``jobs.submission_source`` is recorded
        # with the correct caller identity.
        self._submission_source = submission_source
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
        # ``_lock`` guards ``_cache`` and ``_pools``. The lock is only
        # held around map/list mutations and cheap lookups — SSH
        # ``_build_ssh_handle`` runs outside the lock so a paramiko
        # connect does not block concurrent resolves (F6).
        self._lock = threading.Lock()

    def _disconnect_handle_quietly(self, handle: TransportHandle) -> None:
        """Best-effort disconnect of a cached SSH adapter (F1)."""
        if handle.transport_type != "ssh":
            return
        disconnect = getattr(handle.job_ops, "disconnect", None)
        if disconnect is None:
            return
        try:
            disconnect()
        except Exception as exc:  # noqa: BLE001 — best-effort cleanup
            logger.debug("Adapter disconnect failed (non-fatal): %s", exc)

    def resolve(self, scheduler_key: str) -> TransportHandle | None:
        """Resolve ``scheduler_key`` to a :class:`TransportHandle`.

        Returns ``None`` for unknown SSH profiles or malformed keys so
        the poller can log a warning and skip the affected group
        without crashing the whole cycle (AC-8.5).

        On cache hit for an ``ssh:<profile>`` key, re-validate the
        profile still exists. A profile can be deleted between the
        first resolve and a later one; returning a stale handle would
        mis-route subsequent watches. If the profile has disappeared,
        invalidate the cache entry and return ``None`` (F1).
        """
        # Cache hit path: hold the lock only long enough to read the
        # entry and (on SSH) invalidate it when the profile is gone.
        with self._lock:
            cached = self._cache.get(scheduler_key)
        if cached is not None:
            if scheduler_key.startswith("ssh:"):
                profile_name = scheduler_key[4:]
                if self._profile_loader(profile_name) is None:
                    # Profile deleted — evict under the lock, then
                    # disconnect outside to avoid holding the lock
                    # during blocking I/O.
                    with self._lock:
                        stale = self._cache.pop(scheduler_key, None)
                    if stale is not None:
                        self._disconnect_handle_quietly(stale)
                    return None
            return cached

        # Build path — run the actual construction outside the lock so
        # paramiko connects / profile reads do not serialise concurrent
        # resolves of *different* scheduler keys.
        if scheduler_key == "local":
            built: TransportHandle | None = _build_local_handle(self._local_client)
            pool: Any = None
        elif scheduler_key.startswith("ssh:"):
            profile_name = scheduler_key[4:]
            if not profile_name or self._profile_loader(profile_name) is None:
                return None
            try:
                built, pool = _build_ssh_handle(
                    profile_name,
                    submission_source=self._submission_source,
                )
            except TransportError as exc:
                logger.warning(
                    "Failed to build SSH transport %r: %s", scheduler_key, exc
                )
                return None
        else:
            return None

        # Insert-or-return-existing under the lock. A concurrent thread
        # may have beaten us to the cache; if so, drop our freshly built
        # handle (and pool) and hand the caller the shared one.
        assert built is not None  # narrows type from above (local/ssh both built)
        with self._lock:
            existing = self._cache.get(scheduler_key)
            if existing is not None:
                # Another thread won — drop our pool before returning.
                discard_pool = pool
                discard_handle: TransportHandle | None = built
            else:
                self._cache[scheduler_key] = built
                if pool is not None:
                    self._pools.append(pool)
                discard_pool = None
                discard_handle = None

        if discard_pool is not None:
            try:
                discard_pool.close()
            except Exception as exc:  # noqa: BLE001
                logger.debug(
                    "Pool close during concurrent-resolve cleanup failed: %s", exc
                )
        if discard_handle is not None:
            self._disconnect_handle_quietly(discard_handle)

        return existing if existing is not None else built

    def register_handle(self, handle: TransportHandle) -> None:
        """Seed the cache with a pre-built :class:`TransportHandle`.

        Used by the Web app lifespan to reuse the already-connected
        startup adapter instead of re-opening a second SSH session
        (F11). If the slot is already occupied, the incoming handle is
        silently dropped — callers should treat this as "best effort
        seeding, never replace" semantics.
        """
        with self._lock:
            self._cache.setdefault(handle.scheduler_key, handle)

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
        """Release every cached SSH pool + disconnect SSH adapters.

        Idempotent. Pool closes and adapter disconnects run outside the
        lock to avoid holding it during blocking I/O; the lock is only
        held long enough to snapshot + clear the internal state.
        """
        with self._lock:
            handles = list(self._cache.values())
            pools = list(self._pools)
            self._pools.clear()
            self._cache.clear()

        for handle in handles:
            self._disconnect_handle_quietly(handle)
        for pool in pools:
            try:
                pool.close()
            except Exception as exc:  # noqa: BLE001 — best-effort cleanup
                logger.debug("Pool close failed (non-fatal): %s", exc)
