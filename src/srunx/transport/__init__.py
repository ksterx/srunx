"""Transport resolution layer for CLI unification.

``resolve_transport()`` picks the target cluster (``--cluster`` /
``$SRUNX_CLUSTER`` / project / ``default_cluster``) and hands the CLI a uniform ``ResolvedTransport``
carrying ``JobOperations`` + ``WorkflowJobExecutorFactory`` + optional
``SubmissionRenderContext``.
"""

from srunx.transport.registry import (
    DEFAULT_POLICY,
    ResolvedTransport,
    TransportHandle,
    TransportPolicy,
    TransportRegistry,
    emit_transport_banner,
    peek_scheduler_key,
    resolve_transport,
    resolve_transport_source,
)

__all__ = [
    "DEFAULT_POLICY",
    "ResolvedTransport",
    "TransportHandle",
    "TransportPolicy",
    "TransportRegistry",
    "emit_transport_banner",
    "peek_scheduler_key",
    "resolve_transport",
    "resolve_transport_source",
]
