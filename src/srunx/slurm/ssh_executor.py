"""Bounded connection pool + SSH executor for Web sweep dispatch.

Step 4 of the Phase 2 SSH sweep integration. Provides:

* :class:`SSHWorkflowJobExecutor` — a protocol-conforming wrapper around
  a single :class:`~srunx.slurm.clients.ssh.SlurmSSHClient` lease.
* :class:`SlurmSSHExecutorPool` — a thread-safe bounded pool of pre-built
  clients for concurrent sweep cells, exposing a context-manager factory
  that satisfies :data:`~srunx.slurm.protocols.WorkflowJobExecutorFactory`.

The pool keeps at most ``size`` SSH sessions open to the cluster and
discards clients whose paramiko transport dropped, so a sweep of N
cells never fans out into N bespoke SSH connections.
"""

from __future__ import annotations

import queue
import threading
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from typing import TYPE_CHECKING

from srunx.common.logging import get_logger
from srunx.slurm.clients.ssh import SlurmSSHClient, SlurmSSHClientSpec
from srunx.slurm.protocols import WorkflowJobExecutor

if TYPE_CHECKING:
    from srunx.callbacks import Callback
    from srunx.domain import RunnableJobType
    from srunx.runtime.rendering import SubmissionRenderContext

logger = get_logger(__name__)


class SSHWorkflowJobExecutor:
    """Thin :class:`WorkflowJobExecutor` wrapper over a single client.

    Yielded by :meth:`SlurmSSHExecutorPool.lease` for the lifetime of a
    single sweep cell's ``run`` + optional log retrieval. The underlying
    :class:`SlurmSSHClient` is owned by the pool; this wrapper forbids
    closing it.
    """

    def __init__(self, client: SlurmSSHClient) -> None:
        self._client = client

    def run(
        self,
        job: RunnableJobType,
        *,
        workflow_name: str | None = None,
        workflow_run_id: int | None = None,
        submission_context: SubmissionRenderContext | None = None,
    ) -> RunnableJobType:
        """Delegate to :meth:`SlurmSSHClient.run`.

        ``submission_context`` is forwarded so mount-aware path
        translation happens inside the client just before render (see
        :meth:`SlurmSSHClient.run`).
        """
        return self._client.run(
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
        """Delegate to :meth:`SlurmSSHClient.get_job_output_detailed`."""
        return self._client.get_job_output_detailed(
            job_id, job_name=job_name, skip_content=skip_content
        )


class SlurmSSHExecutorPool:
    """Bounded pool of :class:`SlurmSSHClient` clones for concurrent leases.

    Each lease checked out via :meth:`lease` yields a protocol-conforming
    executor. On release the underlying client is either returned to the
    free queue (if healthy) or discarded (if the transport dropped), so a
    broken connection never poisons subsequent sweep cells.

    Construction is cheap (no SSH I/O); clients are built lazily on first
    :meth:`lease`. The pool is thread-safe: concurrent leases beyond
    ``size`` block on the free queue rather than minting extra sessions.

    Example::

        pool = SlurmSSHExecutorPool(client.connection_spec, size=8)
        try:
            runner = WorkflowRunner(workflow, executor_factory=pool.lease)
            runner.run()
        finally:
            pool.close()
    """

    def __init__(
        self,
        spec: SlurmSSHClientSpec,
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
        # Propagated into each cloned client's ``submission_source`` so
        # per-cell sweep jobs record the correct transport origin in
        # ``jobs.submission_source`` (see review fix #7).
        self._submission_source = submission_source
        # Unbounded internal queue so release is always non-blocking; we
        # gate creation via ``_created`` so ``_free`` never holds more than
        # ``size`` clients.
        self._free: queue.Queue[SlurmSSHClient] = queue.Queue()
        self._created = 0
        self._closed = False
        self._lock = threading.Lock()

    @property
    def size(self) -> int:
        """Maximum number of concurrent clients."""
        return self._size

    def _build_client(self) -> SlurmSSHClient:
        """Mint a fresh client from the pool's connection spec.

        Does not connect eagerly — :meth:`SlurmSSHClient.run` and sibling
        SSH I/O methods call ``_ensure_connected`` on their first use.
        """
        return SlurmSSHClient.from_spec(
            self._spec,
            callbacks=self._callbacks,
            submission_source=self._submission_source,
        )

    def _acquire(self, timeout: float | None = 30.0) -> SlurmSSHClient:
        """Pop a free client, build a new one, or block up to ``timeout``."""
        if self._closed:
            raise RuntimeError("SlurmSSHExecutorPool is closed")

        # Fast path: pick up a free client.
        try:
            return self._free.get_nowait()
        except queue.Empty:
            pass

        # Slow path: build a new client if under the cap.
        with self._lock:
            if self._closed:
                raise RuntimeError("SlurmSSHExecutorPool is closed")
            if self._created < self._size:
                client = self._build_client()
                self._created += 1
                return client

        # Pool is at capacity — block until a lease is released.
        try:
            return self._free.get(timeout=timeout)
        except queue.Empty as exc:
            raise TimeoutError(
                f"Timed out after {timeout}s waiting for a pooled SSH client"
            ) from exc

    def _release(self, client: SlurmSSHClient) -> None:
        """Return ``client`` to the pool, or discard it if broken.

        The pool being closed implies the caller should disconnect too —
        drop the client in that case to honour the close contract.
        """
        if self._closed:
            self._disconnect_safely(client)
            with self._lock:
                self._created = max(0, self._created - 1)
            return

        try:
            healthy = client.is_connected
        except Exception:  # noqa: BLE001
            healthy = False

        if healthy:
            self._free.put_nowait(client)
        else:
            self._disconnect_safely(client)
            with self._lock:
                self._created = max(0, self._created - 1)

    @staticmethod
    def _disconnect_safely(client: SlurmSSHClient) -> None:
        """Call ``client.disconnect`` and swallow any errors."""
        try:
            client.disconnect()
        except Exception as exc:  # noqa: BLE001
            logger.debug(f"Client disconnect failed: {exc}")

    @contextmanager
    def lease(self) -> Iterator[WorkflowJobExecutor]:
        """Context manager yielding a pooled executor.

        Signature matches :data:`WorkflowJobExecutorFactory`, so this
        method can be passed directly as the runner's factory kwarg::

            runner = WorkflowRunner(workflow, executor_factory=pool.lease)
        """
        client = self._acquire()
        try:
            yield SSHWorkflowJobExecutor(client)
        finally:
            self._release(client)

    def close(self) -> None:
        """Drain + disconnect every pooled client. Idempotent."""
        with self._lock:
            if self._closed:
                return
            self._closed = True

        while True:
            try:
                client = self._free.get_nowait()
            except queue.Empty:
                break
            self._disconnect_safely(client)

        with self._lock:
            self._created = 0

    def __enter__(self) -> SlurmSSHExecutorPool:
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()
