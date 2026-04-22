"""Bounded connection pool + SSH executor for Web sweep dispatch.

Step 4 of the Phase 2 SSH sweep integration. Provides:

* :class:`SSHWorkflowJobExecutor` — a protocol-conforming wrapper around
  a single :class:`~srunx.web.ssh_adapter.SlurmSSHAdapter` lease.
* :class:`SlurmSSHExecutorPool` — a thread-safe bounded pool of pre-built
  adapters for concurrent sweep cells, exposing a context-manager factory
  that satisfies :data:`~srunx.client_protocol.WorkflowJobExecutorFactory`.

The pool keeps at most ``size`` SSH sessions open to the cluster and
discards adapters whose paramiko transport dropped, so a sweep of N
cells never fans out into N bespoke SSH connections.
"""

from __future__ import annotations

import queue
import threading
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from typing import TYPE_CHECKING

from srunx.client_protocol import WorkflowJobExecutorProtocol
from srunx.logging import get_logger
from srunx.web.ssh_adapter import SlurmSSHAdapter, SlurmSSHAdapterSpec

if TYPE_CHECKING:
    from srunx.callbacks import Callback
    from srunx.models import RunnableJobType
    from srunx.rendering import SubmissionRenderContext

logger = get_logger(__name__)


class SSHWorkflowJobExecutor:
    """Thin :class:`WorkflowJobExecutorProtocol` wrapper over a single adapter.

    Yielded by :meth:`SlurmSSHExecutorPool.lease` for the lifetime of a
    single sweep cell's ``run`` + optional log retrieval. The underlying
    :class:`SlurmSSHAdapter` is owned by the pool; this wrapper forbids
    closing it.
    """

    def __init__(self, adapter: SlurmSSHAdapter) -> None:
        self._adapter = adapter

    def run(
        self,
        job: RunnableJobType,
        *,
        workflow_name: str | None = None,
        workflow_run_id: int | None = None,
        submission_context: SubmissionRenderContext | None = None,
    ) -> RunnableJobType:
        """Delegate to :meth:`SlurmSSHAdapter.run`.

        ``submission_context`` is forwarded so mount-aware path
        translation happens inside the adapter just before render (see
        :meth:`SlurmSSHAdapter.run`).
        """
        return self._adapter.run(
            job,
            workflow_name=workflow_name,
            workflow_run_id=workflow_run_id,
            submission_context=submission_context,
        )

    def get_job_output_detailed(
        self,
        job_id: int | str,
        job_name: str | None = None,
        skip_content: bool = False,
    ) -> dict[str, str | list[str] | None]:
        """Delegate to :meth:`SlurmSSHAdapter.get_job_output_detailed`."""
        return self._adapter.get_job_output_detailed(
            job_id, job_name=job_name, skip_content=skip_content
        )


class SlurmSSHExecutorPool:
    """Bounded pool of :class:`SlurmSSHAdapter` clones for concurrent leases.

    Each lease checked out via :meth:`lease` yields a protocol-conforming
    executor. On release the underlying adapter is either returned to the
    free queue (if healthy) or discarded (if the transport dropped), so a
    broken connection never poisons subsequent sweep cells.

    Construction is cheap (no SSH I/O); adapters are built lazily on first
    :meth:`lease`. The pool is thread-safe: concurrent leases beyond
    ``size`` block on the free queue rather than minting extra sessions.

    Example::

        pool = SlurmSSHExecutorPool(adapter.connection_spec, size=8)
        try:
            runner = WorkflowRunner(workflow, executor_factory=pool.lease)
            runner.run()
        finally:
            pool.close()
    """

    def __init__(
        self,
        spec: SlurmSSHAdapterSpec,
        *,
        callbacks: Sequence[Callback] | None = None,
        size: int = 8,
        submission_source: str = "web",
    ) -> None:
        if size <= 0:
            raise ValueError(f"Pool size must be positive, got {size}")
        self._spec = spec
        self._callbacks: list[Callback] = list(callbacks) if callbacks else []
        self._size = size
        # Propagated into each cloned adapter's ``submission_source`` so
        # per-cell sweep jobs record the correct transport origin in
        # ``jobs.submission_source`` (see review fix #7).
        self._submission_source = submission_source
        # Unbounded internal queue so release is always non-blocking; we
        # gate creation via ``_created`` so ``_free`` never holds more than
        # ``size`` adapters.
        self._free: queue.Queue[SlurmSSHAdapter] = queue.Queue()
        self._created = 0
        self._closed = False
        self._lock = threading.Lock()

    @property
    def size(self) -> int:
        """Maximum number of concurrent adapters."""
        return self._size

    def _build_adapter(self) -> SlurmSSHAdapter:
        """Mint a fresh adapter from the pool's connection spec.

        Does not connect eagerly — :meth:`SlurmSSHAdapter.run` and sibling
        SSH I/O methods call ``_ensure_connected`` on their first use.
        """
        return SlurmSSHAdapter.from_spec(
            self._spec,
            callbacks=self._callbacks,
            submission_source=self._submission_source,
        )

    def _acquire(self, timeout: float | None = 30.0) -> SlurmSSHAdapter:
        """Pop a free adapter, build a new one, or block up to ``timeout``."""
        if self._closed:
            raise RuntimeError("SlurmSSHExecutorPool is closed")

        # Fast path: pick up a free adapter.
        try:
            return self._free.get_nowait()
        except queue.Empty:
            pass

        # Slow path: build a new adapter if under the cap.
        with self._lock:
            if self._closed:
                raise RuntimeError("SlurmSSHExecutorPool is closed")
            if self._created < self._size:
                adapter = self._build_adapter()
                self._created += 1
                return adapter

        # Pool is at capacity — block until a lease is released.
        try:
            return self._free.get(timeout=timeout)
        except queue.Empty as exc:
            raise TimeoutError(
                f"Timed out after {timeout}s waiting for a pooled SSH adapter"
            ) from exc

    def _release(self, adapter: SlurmSSHAdapter) -> None:
        """Return ``adapter`` to the pool, or discard it if broken.

        The pool being closed implies the caller should disconnect too —
        drop the adapter in that case to honour the close contract.
        """
        if self._closed:
            self._disconnect_safely(adapter)
            with self._lock:
                self._created = max(0, self._created - 1)
            return

        try:
            healthy = adapter.is_connected
        except Exception:  # noqa: BLE001
            healthy = False

        if healthy:
            self._free.put_nowait(adapter)
        else:
            self._disconnect_safely(adapter)
            with self._lock:
                self._created = max(0, self._created - 1)

    @staticmethod
    def _disconnect_safely(adapter: SlurmSSHAdapter) -> None:
        """Call ``adapter.disconnect`` and swallow any errors."""
        try:
            adapter.disconnect()
        except Exception as exc:  # noqa: BLE001
            logger.debug(f"Adapter disconnect failed: {exc}")

    @contextmanager
    def lease(self) -> Iterator[WorkflowJobExecutorProtocol]:
        """Context manager yielding a pooled executor.

        Signature matches :data:`WorkflowJobExecutorFactory`, so this
        method can be passed directly as the runner's factory kwarg::

            runner = WorkflowRunner(workflow, executor_factory=pool.lease)
        """
        adapter = self._acquire()
        try:
            yield SSHWorkflowJobExecutor(adapter)
        finally:
            self._release(adapter)

    def close(self) -> None:
        """Drain + disconnect every pooled adapter. Idempotent."""
        with self._lock:
            if self._closed:
                return
            self._closed = True

        while True:
            try:
                adapter = self._free.get_nowait()
            except queue.Empty:
                break
            self._disconnect_safely(adapter)

        with self._lock:
            self._created = 0

    def __enter__(self) -> SlurmSSHExecutorPool:
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()
