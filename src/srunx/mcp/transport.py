"""Transport selection for MCP tools.

MCP is an API surface reachable by any agent session, so — unlike the CLI —
it must never resolve a remote cluster from *ambient* machine state. A job
must not land on someone's prod cluster because ``$SRUNX_SSH_PROFILE`` happens
to be exported in the server's environment, or because someone once ran
``srunx ssh use``.

Every cluster-acting MCP tool therefore takes a single ``transport`` string
and resolves it through this module, which drives the shared
:func:`srunx.transport.resolve_transport` with a policy that disables both
implicit ladder rungs (env var + current-profile). The only way to reach SSH
from MCP is to pass an explicit profile name.

    transport=None        -> local SLURM
    transport="local"     -> local SLURM (explicit)
    transport="<profile>" -> ssh:<profile>
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager

from srunx.common.exceptions import TransportSelectionError
from srunx.transport import (
    ResolvedTransport,
    TransportPolicy,
    resolve_transport,
)

# API-surface policy: ignore env vars and the active SSH profile entirely.
# Only an explicit ``transport`` argument can select a remote.
MCP_POLICY = TransportPolicy(allow_env=False, allow_current_profile=False)


def parse_transport(transport: str | None) -> tuple[str | None, bool]:
    """Map the MCP ``transport`` argument to ``(profile, local)`` kwargs.

    Returns the pair :func:`resolve_transport` accepts:

    * ``None``        -> ``(None, False)`` — no explicit selection; with the
      MCP policy (env + current off) this resolves to local SLURM.
    * ``"local"``     -> ``(None, True)`` — force local.
    * ``"<profile>"`` -> ``(profile, False)`` — that SSH profile.

    Raises :class:`~srunx.common.exceptions.TransportSelectionError` on an
    empty / whitespace string so a typo can't silently fall through to local.
    """
    if transport is None:
        return None, False
    name = transport.strip()
    if not name:
        raise TransportSelectionError(
            "transport must be a profile name, 'local', or omitted — not empty."
        )
    if name == "local":
        return None, True
    return name, False


@contextmanager
def mcp_transport(
    transport: str | None,
    *,
    mount_name: str | None = None,
) -> Iterator[ResolvedTransport]:
    """Resolve transport for one MCP tool call.

    Thin wrapper over :func:`resolve_transport` that pins the MCP policy
    (no env / no current-profile), suppresses the stderr banner (MCP has no
    human console), and tags submissions with ``submission_source='mcp'``.

    Args:
        transport: The tool's ``transport`` argument (see module docstring).
        mount_name: Explicit mount for path translation (``run_workflow``);
            ``None`` lets a single-mount profile auto-select.
    """
    profile, local = parse_transport(transport)
    with resolve_transport(
        profile=profile,
        local=local,
        banner=False,
        submission_source="mcp",
        mount_name=mount_name,
        policy=MCP_POLICY,
    ) as resolved:
        yield resolved
