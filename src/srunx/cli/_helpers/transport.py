"""CLI-boundary transport resolvers.

The pure resolver in :mod:`srunx.transport.registry` is framework-neutral: a
bad or missing ``--cluster`` raises
:class:`~srunx.common.exceptions.TransportSelectionError` (a plain
``ValueError``) rather than a Typer/Click exception, so the same resolver can
back the CLI, MCP, and the web app without any of them dragging in the others'
error framework.

The CLI, though, wants Typer's standard bad-flag UX: exit code 2, a clean
one-line message, and the offending flag name. This module is the *single*
place that maps ``TransportSelectionError`` -> ``typer.BadParameter``. Every
CLI entry point imports the resolvers from here instead of calling the
registry directly; the function names match the registry's so call sites are
unchanged.

These wrappers add nothing but that translation — resolution logic stays in
the registry.
"""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from typing import TYPE_CHECKING

import typer

from srunx.common.exceptions import TransportSelectionError
from srunx.transport import registry
from srunx.transport.registry import (
    DEFAULT_POLICY,
    ResolvedTransport,
    TransportPolicy,
    TransportSource,
)

if TYPE_CHECKING:
    from srunx.callbacks import Callback

__all__ = [
    "resolve_cluster_name",
    "resolve_transport",
    "peek_scheduler_key",
    "resolve_transport_source",
]


def _as_bad_parameter(exc: TransportSelectionError) -> typer.BadParameter:
    """Translate a neutral selection error into Typer's bad-flag exception."""
    return typer.BadParameter(str(exc), param_hint=exc.param_hint)


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
    allow_cwd_mount: bool = True,
    pool_size: int = 2,
    policy: TransportPolicy = DEFAULT_POLICY,
) -> Iterator[ResolvedTransport]:
    """:func:`srunx.transport.registry.resolve_transport` for CLI callers.

    Two differences from the registry function:

    * A :class:`TransportSelectionError` raised while resolving the
      transport is re-raised as ``typer.BadParameter`` so Typer renders
      it as a normal flag error instead of a traceback.
    * ``allow_cwd_mount`` defaults to ``True``. The CLI has no
      ``--mount`` flag, and a one-shot CLI process's cwd is a direct
      expression of user intent — unlike a long-lived Web / MCP server,
      whose cwd is an accident of how it was launched.
    """
    try:
        with registry.resolve_transport(
            cluster=cluster,
            source_override=source_override,
            quiet=quiet,
            banner=banner,
            callbacks=callbacks,
            submission_source=submission_source,
            mount_name=mount_name,
            allow_cwd_mount=allow_cwd_mount,
            pool_size=pool_size,
            policy=policy,
        ) as resolved:
            yield resolved
    except TransportSelectionError as exc:
        raise _as_bad_parameter(exc) from exc


def peek_scheduler_key(
    *,
    cluster: str | None = None,
    policy: TransportPolicy = DEFAULT_POLICY,
) -> str:
    """:func:`srunx.transport.registry.peek_scheduler_key`, BadParameter-translated."""
    try:
        return registry.peek_scheduler_key(cluster=cluster, policy=policy)
    except TransportSelectionError as exc:
        raise _as_bad_parameter(exc) from exc


def resolve_transport_source(
    *,
    cluster: str | None = None,
    policy: TransportPolicy = DEFAULT_POLICY,
) -> TransportSource:
    """:func:`srunx.transport.registry.resolve_transport_source`, BadParameter-translated."""
    try:
        return registry.resolve_transport_source(cluster=cluster, policy=policy)
    except TransportSelectionError as exc:
        raise _as_bad_parameter(exc) from exc


def resolve_cluster_name(
    *,
    cluster: str | None = None,
    policy: TransportPolicy = DEFAULT_POLICY,
) -> tuple[str, str]:
    """:func:`srunx.transport.registry.resolve_cluster_name`, BadParameter-translated."""
    try:
        return registry.resolve_cluster_name(cluster=cluster, policy=policy)
    except TransportSelectionError as exc:
        raise _as_bad_parameter(exc) from exc


def cluster_for_job(job_id: int, cluster: str | None) -> str:
    return job_target(job_id, cluster)[0]


def job_target(job_id: int, cluster: str | None) -> tuple[str, TransportSource | None]:
    """``(cluster, banner source)`` for a job-specific command.

    The source is ``"history"`` when the cluster came from srunx's job
    history, ``None`` when the caller passed ``--cluster``.
    """
    if cluster:
        return cluster, None
    return _cluster_from_history(job_id), "history"


def _cluster_from_history(job_id: int) -> str:
    """Pick the cluster a job-specific command targets (design §8).

    An explicit ``--cluster`` always wins. Otherwise the job is looked up
    in srunx's history: exactly one match selects that cluster; zero or
    several matches are an error asking for ``--cluster`` (a job ID alone
    is not unique across clusters).
    """
    from srunx.common.config import ConfigError, load_settings
    from srunx.observability.storage.connection import initialized_connection

    try:
        with initialized_connection() as conn:
            rows = conn.execute(
                "SELECT DISTINCT scheduler_key FROM jobs WHERE job_id = ?",
                (job_id,),
            ).fetchall()
    except Exception:  # noqa: BLE001 — an unreadable history is "no match"
        rows = []
    keys = sorted({r[0] for r in rows if r[0]})
    if len(keys) != 1:
        reason = (
            "is not in srunx history"
            if not keys
            else f"exists on several clusters ({', '.join(keys)})"
        )
        raise typer.BadParameter(
            f"Job {job_id} {reason}; pass --cluster.", param_hint="--cluster"
        )
    key = keys[0]
    if key.startswith("ssh:"):
        return key[4:]
    try:
        settings = load_settings(reload=True)
    except ConfigError as exc:
        raise typer.BadParameter(str(exc), param_hint="--cluster") from exc
    for name, cfg in settings.clusters.items():
        if cfg.transport == "local":
            return name
    raise typer.BadParameter(
        f"Job {job_id} ran on the local cluster, but no cluster with "
        "transport = 'local' is configured; pass --cluster.",
        param_hint="--cluster",
    )
