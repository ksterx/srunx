"""Transport resolution + registry.

This module is the single entry point CLI commands use to pick between
local SLURM and an SSH-backed cluster. Higher layers call
:func:`resolve_transport` (context manager) and receive a
:class:`ResolvedTransport` that exposes the same
:class:`~srunx.slurm.protocols.JobOperations` / queue client /
executor factory regardless of which transport was selected.

Cluster selection (design §5): ``--cluster`` > ``$SRUNX_CLUSTER`` >
project ``cluster`` > user ``default_cluster`` > error. There is no
implicit local fallback; a local cluster is one with
``transport = "local"``.

The SSH-related imports (``SlurmSSHClient``, ``SlurmSSHExecutorPool``,
``SubmissionRenderContext``, ``ConfigManager``) are gated inside
:func:`_build_ssh_handle` so the local fallback path never pays the
paramiko import cost (R-3).

"""

from __future__ import annotations

import sys
import threading
from collections.abc import Callable, Iterator, Sequence
from contextlib import contextmanager, nullcontext
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal

from rich.console import Console

from srunx.common.exceptions import TransportError, TransportSelectionError
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


TransportSource = Literal["--cluster", "env", "project", "config", "history"]


@dataclass(frozen=True)
class TransportPolicy:
    """Whether implicit cluster selection is honoured.

    The CLI honours ``$SRUNX_CLUSTER``, the project's ``cluster`` and the
    user's ``default_cluster``. API surfaces (MCP) must not pick a cluster
    from ambient state, so they pass ``allow_implicit=False`` and require
    an explicit cluster.
    """

    allow_implicit: bool = True


DEFAULT_POLICY = TransportPolicy()
"""CLI-execution default: both implicit rungs honoured (legacy behaviour)."""


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
    cluster_name: str | None = None


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
    def cluster_name(self) -> str | None:
        return self.handle.cluster_name

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
    emit_transport_banner(
        label=resolved.label,
        source=resolved.source,
        quiet=quiet,
        transport=resolved.transport_type,
    )


def emit_transport_banner(
    *,
    label: str,
    source: TransportSource,
    quiet: bool,
    transport: str = "local",
) -> None:
    """Print a one-line "where does this run" banner on stderr.

    Always shown (cluster selection is never silent) unless ``quiet``.
    A fresh Console per call picks up the current ``sys.stderr``.
    """
    if quiet:
        return
    source_display = {
        "--cluster": "via --cluster",
        "env": "via $SRUNX_CLUSTER",
        "project": "via .srunx/config.toml",
        "config": "via default_cluster",
        "history": "via job history",
    }.get(source, f"via {source}")
    if transport == "ssh":
        dot_color = "cyan"
        body = _format_ssh_banner_body(
            profile_name=label, source_display=source_display
        )
    else:
        dot_color = "yellow"
        body = (
            f"[bold]Local SLURM[/bold]  "
            f"[dim italic](cluster: {label} · {source_display})[/dim italic]"
        )
    Console(file=sys.stderr).print(f"[{dot_color}]●[/{dot_color}]  {body}")


def _format_ssh_banner_body(*, profile_name: str, source_display: str) -> str:
    """Build the SSH half of the transport banner.

    Shows ``user@host`` (plus ``:port`` when non-default and
    ``via <proxy>`` when ``proxy_jump`` is set) as the headline so the
    banner answers "which machine am I reaching" without forcing the
    user to remember what ``<profile_name>`` maps to. Profile name and
    decision source are kept as parenthetical metadata.

    When the profile is an ``ssh_host`` alias (``username``/``hostname``
    blank because the real connection info lives in ``~/.ssh/config``),
    fall through the same resolution path the SSH connection layer
    uses: parse ``~/.ssh/config`` via :func:`get_ssh_config_host` and
    render the resolved ``user@host[:port]``. Without this fallback the
    banner emits ``@`` for alias-only profiles even though the
    underlying connection succeeds.

    Falls back to ``SSH profile: <name>`` when the profile lookup
    fails (config file missing, profile removed, import error) — the
    banner must never raise, even from a degraded state.
    """
    profile = _lookup_profile_silently(profile_name)
    if profile is None:
        return (
            f"[bold]Cluster: {profile_name}[/bold]  "
            f"[dim italic]({source_display})[/dim italic]"
        )

    username = profile.username
    hostname = profile.hostname
    port = profile.port
    if (not username or not hostname) and profile.ssh_host:
        try:
            from srunx.ssh.core.ssh_config import get_ssh_config_host

            resolved = get_ssh_config_host(profile.ssh_host)
        except Exception as exc:  # noqa: BLE001 — banner must never raise
            logger.debug(
                "ssh_config lookup for alias {!r} failed: {}", profile.ssh_host, exc
            )
            resolved = None
        if resolved is not None:
            username = username or resolved.user
            hostname = hostname or resolved.hostname
            # ServerProfile.port defaults to 22; only override when the
            # profile hasn't been explicitly customised and the ssh_config
            # Host block specifies a non-default port.
            if profile.port == 22 and resolved.port != 22:
                port = resolved.port

    if username and hostname:
        target = f"{username}@{hostname}"
        if port != 22:
            target += f":{port}"
    elif profile.ssh_host:
        # ssh_config に該当 Host が無くても alias 名は手掛かりになる
        target = profile.ssh_host
    else:
        return (
            f"[bold]Cluster: {profile_name}[/bold]  "
            f"[dim italic]({source_display})[/dim italic]"
        )

    if profile.proxy_jump:
        target += f" via {profile.proxy_jump}"
    return (
        f"[bold]Connected to[/bold] [cyan]{target}[/cyan]  "
        f"[dim italic](cluster: {profile_name} · {source_display})[/dim italic]"
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
        logger.debug("Could not load SSH profile {!r} for banner: {}", name, exc)
        return None


def scheduler_key_for(cluster: str, transport: str) -> str:
    """DB scheduler key of *cluster* (``local`` or ``ssh:<name>``).

    The key encodes the transport kind so pollers can route without a
    config lookup; together with ``job_id`` it is the job ref of design §8
    (at most one local cluster exists, so ``local`` stays unambiguous).
    """
    return "local" if transport == "local" else f"ssh:{cluster}"


def _resolve_source_and_cluster(
    *,
    cluster: str | None,
    policy: TransportPolicy = DEFAULT_POLICY,
) -> tuple[TransportSource, str, Literal["local", "ssh"]]:
    """Return ``(source, cluster name, transport kind)`` (design §5).

    ``--cluster`` > ``$SRUNX_CLUSTER`` > project ``cluster`` >
    ``default_cluster``; never an implicit local fallback. Implicit rungs
    are skipped when ``policy.allow_implicit`` is False (MCP / Web).

    Raises:
        TransportSelectionError: empty ``--cluster``, no cluster selected,
            unknown cluster, or invalid configuration.
    """
    from srunx.common.config import ConfigError, load_settings

    cluster = cluster.strip() if cluster else cluster
    if cluster == "":
        raise TransportSelectionError(
            "--cluster cannot be empty or whitespace.", param_hint="--cluster"
        )
    try:
        settings = load_settings(reload=True)
        if cluster:
            source: TransportSource = "--cluster"
            name = cluster
        elif not policy.allow_implicit:
            raise TransportSelectionError(
                "A cluster must be specified explicitly.", param_hint="cluster"
            )
        else:
            found, origin = settings.cluster_source()
            if found is None:
                settings.resolve_cluster(None)  # raises with guidance
            assert found is not None
            name = found
            source = (
                "env"
                if origin.startswith("$")
                else (
                    "project"
                    if settings.project_path and origin == str(settings.project_path)
                    else "config"
                )
            )
        cfg = settings.cluster(name)
    except ConfigError as exc:
        raise TransportSelectionError(str(exc), param_hint="--cluster") from exc
    return source, name, cfg.transport


def resolve_transport_source(
    *,
    cluster: str | None = None,
    policy: TransportPolicy = DEFAULT_POLICY,
) -> TransportSource:
    """Return the :data:`TransportSource` ``resolve_transport`` would pick."""
    source, _, _ = _resolve_source_and_cluster(cluster=cluster, policy=policy)
    return source


def _build_local_handle(
    slurm: Slurm | None = None, cluster_name: str | None = None
) -> TransportHandle:
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
        cluster_name=cluster_name,
    )


def _select_mount_for_cwd(mounts: Sequence[Any]) -> Any | None:
    """Return the mount owning the current working directory, if any.

    Returns ``None`` when the cwd sits outside every mount, or when the
    cwd cannot be read at all (it was deleted underneath the process).
    """
    from srunx.runtime.submission_plan import resolve_mount_among

    try:
        cwd = Path.cwd()
    except OSError:
        return None
    return resolve_mount_among(cwd, mounts)


def _resolve_submission_context(
    *,
    profile_name: str,
    profile_mounts: Sequence[Any] | None,
    mount_name: str | None,
    allow_cwd_mount: bool = False,
) -> SubmissionRenderContext | None:
    """Decide the ``SubmissionRenderContext`` for an SSH profile.

    Split out of :func:`_build_ssh_handle` so the mount auto-selection
    policy lives in one place and can be tested independently.

    Returns ``None`` when the profile has no mounts at all (no
    translation possible). Otherwise returns a context with
    ``mount_name`` set to either the caller's explicit choice, the
    single mount's name, the mount that owns the current working
    directory (only when *allow_cwd_mount*), or ``None`` when none of
    those apply.

    The cwd fallback exists because the CLI has no ``--mount`` flag —
    ``mount_name`` is only ever passed by the Web and MCP surfaces, so
    without it a multi-mount profile could never get path translation
    from the CLI at all. Mount ``local`` roots are unique per profile
    (enforced in ``ConfigManager.add_profile_mount``) and matching is
    longest-prefix, so the cwd picks out at most one mount.

    It is opt-in because cwd only carries user intent for a one-shot
    CLI process. For a long-lived server (Web app, MCP) the cwd is
    wherever the process happened to be launched, so honouring it there
    would make identical requests render different remote paths on two
    machines.
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
            cwd_mount = _select_mount_for_cwd(mounts) if allow_cwd_mount else None
            if cwd_mount is not None:
                resolved_mount_name = cwd_mount.name
            else:
                # Not a warning: read-only commands (squeue / sinfo /
                # tail) build a handle too and never translate a path,
                # so this is only interesting when debugging a
                # submission that skipped translation.
                logger.debug(
                    "SSH profile {!r} declares {} mounts and none was "
                    "selected (cwd fallback {}); path translation is "
                    "disabled.",
                    profile_name,
                    len(mounts),
                    "missed" if allow_cwd_mount else "off",
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
    allow_cwd_mount: bool = False,
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
            auto-select it so ``flow run --cluster`` gets mount
            translation out of the box. Multi-mount profiles fall back
            to ``allow_cwd_mount``, then to "no translation".
        allow_cwd_mount: Let a multi-mount profile fall back to the
            mount owning the current working directory. CLI-only —
            see :func:`_resolve_submission_context`.
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
            f"Cluster '{profile_name}' not found or not an SSH cluster. "
            "Check 'srunx cluster list'."
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
    # proxy_jump / profile_name the singleton adapter uses. Pooled clones
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
            allow_cwd_mount=allow_cwd_mount,
        )

        handle = TransportHandle(
            scheduler_key=f"ssh:{profile_name}",
            profile_name=profile_name,
            transport_type="ssh",
            job_ops=adapter,
            queue_client=adapter,
            executor_factory=pool.lease,
            submission_context=render_context,
            cluster_name=profile_name,
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
                "Pool close during orphan cleanup failed (non-fatal): {}",
                close_exc,
            )
        raise


def peek_scheduler_key(
    *,
    cluster: str | None = None,
    policy: TransportPolicy = DEFAULT_POLICY,
) -> str:
    """Return the scheduler_key ``resolve_transport`` would pick (no side effects)."""
    _, name, transport = _resolve_source_and_cluster(cluster=cluster, policy=policy)
    return scheduler_key_for(name, transport)


def resolve_cluster_name(
    *,
    cluster: str | None = None,
    policy: TransportPolicy = DEFAULT_POLICY,
) -> tuple[str, Literal["local", "ssh"]]:
    """Return ``(cluster name, transport kind)`` ``resolve_transport`` would pick."""
    _, name, transport = _resolve_source_and_cluster(cluster=cluster, policy=policy)
    return name, transport


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
    cluster: str | None = None,
    source_override: TransportSource | None = None,
    quiet: bool = False,
    banner: bool = True,
    callbacks: Sequence[Callback] | None = None,
    submission_source: str = "cli",
    mount_name: str | None = None,
    allow_cwd_mount: bool = False,
    pool_size: int = 2,
    policy: TransportPolicy = DEFAULT_POLICY,
) -> Iterator[ResolvedTransport]:
    """Resolve the target cluster and yield its transport for one invocation.

    Cluster selection follows design §5 (see
    :func:`_resolve_source_and_cluster`). SSH pools are closed on exit.

    Raises:
        TransportSelectionError: no / unknown cluster or invalid config.
        TransportError: the SSH adapter could not be built.
    """
    source, name, transport = _resolve_source_and_cluster(
        cluster=cluster, policy=policy
    )

    pool: Any = None
    handle: TransportHandle

    if transport == "ssh":
        handle, pool = _build_ssh_handle(
            name,
            callbacks=callbacks,
            submission_source=submission_source,
            mount_name=mount_name,
            allow_cwd_mount=allow_cwd_mount,
            pool_size=pool_size,
        )
    else:
        handle = _build_local_handle(cluster_name=name)

    resolved = ResolvedTransport(
        label=name,
        source=source_override or source,
        handle=handle,
    )

    if banner:
        _emit_banner(resolved, quiet)

    from srunx.common.config import activate_config, active_config, load_settings

    current = active_config()
    config = (
        current
        if current is not None and current.cluster == name
        else load_settings().effective(name)
    )
    try:
        with activate_config(config):
            yield resolved
    finally:
        if pool is not None:
            try:
                pool.close()
            except Exception as exc:  # noqa: BLE001 — best-effort cleanup
                logger.debug("Pool close failed (non-fatal): {}", exc)


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
            logger.debug("Adapter disconnect failed (non-fatal): {}", exc)

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
                    "Failed to build SSH transport {!r}: {}", scheduler_key, exc
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
                    "Pool close during concurrent-resolve cleanup failed: {}", exc
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
                logger.debug("Pool close failed (non-fatal): {}", exc)
