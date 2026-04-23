"""Observability layer — state persistence, notifications, monitoring.

Exposes concrete :class:`~srunx.runtime.lifecycle.JobLifecycleSink`
implementations (:class:`DBRecorderSink`, :class:`CallbackSink`) that the
slurm/ clients consume via the protocol, plus downstream pieces
(pollers, notifications, storage) that Phase 8 (#164) will relocate
into this package.
"""

from srunx.observability.callbacks import CallbackSink
from srunx.observability.recorder import DBRecorderSink

__all__ = ["CallbackSink", "DBRecorderSink"]
