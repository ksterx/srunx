"""Cluster selection for MCP tools.

MCP is an API surface reachable by any agent session, so — unlike the CLI —
it never picks a cluster from ambient state (``$SRUNX_CLUSTER``, the
project's ``cluster``, ``default_cluster``). Every cluster-acting tool takes
an explicit ``cluster`` name; tools that touch project mounts also take an
explicit ``project`` root instead of guessing from the server's cwd.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager

from srunx.common.config import project_scope
from srunx.common.logging import get_logger
from srunx.transport import (
    ResolvedTransport,
    TransportPolicy,
    resolve_transport,
)

logger = get_logger(__name__)

MCP_POLICY = TransportPolicy(allow_implicit=False)


@contextmanager
def mcp_transport(
    cluster: str | None,
    *,
    project: str | None = None,
    mount_name: str | None = None,
) -> Iterator[ResolvedTransport]:
    """Resolve transport for one MCP tool call.

    Thin wrapper over :func:`resolve_transport` that pins the MCP policy
    (explicit cluster only), suppresses the stderr banner (MCP has no
    human console), and tags submissions with ``submission_source='mcp'``.

    Args:
        cluster: The tool's ``cluster`` argument (required).
        project: Project root whose mounts apply (``None``: no project).
        mount_name: Explicit mount for path translation (``run_workflow``);
            ``None`` lets a single-mount project auto-select.
    """
    with (
        project_scope(project),
        resolve_transport(
            cluster=cluster,
            banner=False,
            submission_source="mcp",
            mount_name=mount_name,
            policy=MCP_POLICY,
        ) as resolved,
    ):
        try:
            yield resolved
        finally:
            # MCP runs as a long-lived server, so a single-shot tool call
            # must not leave its SSH session dangling for GC to reap (the
            # CLI gets away with it because its process exits). resolve_transport's
            # own finally only closes the executor pool, not the singleton
            # adapter that backs ``job_ops``, so disconnect it here.
            if resolved.transport_type == "ssh":
                disconnect = getattr(resolved.job_ops, "disconnect", None)
                if callable(disconnect):
                    try:
                        disconnect()
                    except Exception as exc:  # noqa: BLE001 — best-effort cleanup
                        logger.debug(
                            "MCP SSH adapter disconnect failed (non-fatal): {}", exc
                        )
