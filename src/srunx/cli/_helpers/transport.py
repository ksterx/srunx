"""CLI-boundary transport resolvers.

The pure resolver in :mod:`srunx.transport.registry` is framework-neutral: a
bad ``--profile`` / ``--local`` combination raises
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
    profile: str | None = None,
    local: bool = False,
    quiet: bool = False,
    banner: bool = True,
    callbacks: Sequence[Callback] | None = None,
    submission_source: str = "cli",
    mount_name: str | None = None,
    pool_size: int = 2,
    policy: TransportPolicy = DEFAULT_POLICY,
) -> Iterator[ResolvedTransport]:
    """:func:`srunx.transport.registry.resolve_transport` for CLI callers.

    Identical behaviour, except a :class:`TransportSelectionError` raised
    while resolving the transport is re-raised as ``typer.BadParameter`` so
    Typer renders it as a normal flag error instead of a traceback.
    """
    try:
        with registry.resolve_transport(
            profile=profile,
            local=local,
            quiet=quiet,
            banner=banner,
            callbacks=callbacks,
            submission_source=submission_source,
            mount_name=mount_name,
            pool_size=pool_size,
            policy=policy,
        ) as resolved:
            yield resolved
    except TransportSelectionError as exc:
        raise _as_bad_parameter(exc) from exc


def peek_scheduler_key(
    *,
    profile: str | None = None,
    local: bool = False,
    policy: TransportPolicy = DEFAULT_POLICY,
) -> str:
    """:func:`srunx.transport.registry.peek_scheduler_key`, BadParameter-translated."""
    try:
        return registry.peek_scheduler_key(profile=profile, local=local, policy=policy)
    except TransportSelectionError as exc:
        raise _as_bad_parameter(exc) from exc


def resolve_transport_source(
    *,
    profile: str | None = None,
    local: bool = False,
    policy: TransportPolicy = DEFAULT_POLICY,
) -> TransportSource:
    """:func:`srunx.transport.registry.resolve_transport_source`, BadParameter-translated."""
    try:
        return registry.resolve_transport_source(
            profile=profile, local=local, policy=policy
        )
    except TransportSelectionError as exc:
        raise _as_bad_parameter(exc) from exc
