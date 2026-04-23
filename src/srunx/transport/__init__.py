"""Transport resolution layer for CLI unification.

``resolve_transport()`` picks between local SLURM and SSH (per ``--profile`` /
``$SRUNX_SSH_PROFILE``) and hands the CLI a uniform ``ResolvedTransport``
carrying ``JobOperations`` + ``WorkflowJobExecutorFactory`` + optional
``SubmissionRenderContext``.

See ``specs/cli-transport-unification/{spec,plan}.md`` REQ-1 / REQ-7 / REQ-8
for the full resolution contract.
"""

from srunx.transport.registry import (
    ResolvedTransport,
    TransportHandle,
    TransportRegistry,
    emit_transport_banner,
    peek_scheduler_key,
    resolve_transport,
    resolve_transport_source,
)

__all__ = [
    "ResolvedTransport",
    "TransportHandle",
    "TransportRegistry",
    "emit_transport_banner",
    "peek_scheduler_key",
    "resolve_transport",
    "resolve_transport_source",
]
