"""Regression tests for the post-implementation review fixes.

Seven MUST-FIX findings were raised by Codex + code-reviewer against the
CLI transport unification feature branch. These tests pin the behaviour
each fix restores so subsequent refactors can't silently regress.

Each ``Test*`` class corresponds to one review finding (see the PR
description). The tests are intentionally small and depend only on
public module surfaces or documented private seams (``_build_ssh_handle``
is already monkey-patched by the existing transport test suite, so
adding one more patch point carries zero coupling cost).
"""

from __future__ import annotations

import sqlite3
import time
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from srunx.callbacks import NotificationWatchCallback
from srunx.client_protocol import JobStatusInfo
from srunx.db.connection import open_connection
from srunx.db.migrations import (
    MIGRATIONS,
    _apply_fk_off_migration,
    _apply_tx_migration,
)
from srunx.db.repositories.job_state_transitions import (
    JobStateTransitionRepository,
)
from srunx.db.repositories.jobs import JobRepository
from srunx.db.repositories.watches import WatchRepository
from srunx.pollers.active_watch_poller import ActiveWatchPoller
from srunx.transport import (
    TransportHandle,
    peek_scheduler_key,
    resolve_transport,
)

# ---------------------------------------------------------------------------
# Fix #1: Poller threads scheduler_key into every repo call
# ---------------------------------------------------------------------------


class _StubQueueClient:
    """Minimal ``SlurmClientProtocol`` stub."""

    def __init__(self, responses: dict[int, JobStatusInfo]) -> None:
        self.responses = responses
        self.calls: list[list[int]] = []

    def queue_by_ids(self, job_ids: list[int]) -> dict[int, JobStatusInfo]:
        self.calls.append(list(job_ids))
        return {jid: self.responses[jid] for jid in job_ids if jid in self.responses}


class _StubRegistry:
    def __init__(self, clients: dict[str, _StubQueueClient]) -> None:
        self._clients = clients

    def resolve(self, scheduler_key: str):  # type: ignore[no-untyped-def]
        client = self._clients.get(scheduler_key)
        if client is None:
            return None

        class _Handle:
            def __init__(self, qc: _StubQueueClient) -> None:
                self.queue_client = qc
                self.scheduler_key = scheduler_key

        return _Handle(client)


class TestPollerThreadsSchedulerKey:
    """Fix #1: SSH-transport watches must not be joined against scheduler_key='local'."""

    def test_ssh_watch_transitions_fire(
        self, tmp_srunx_db: tuple[sqlite3.Connection, Path]
    ) -> None:
        """A poller cycle for ``ssh:dgx`` must insert the transition under
        the matching scheduler_key, not under the default ``'local'``."""
        import anyio

        conn, db_path = tmp_srunx_db

        # Seed an SSH-recorded job + baseline PENDING transition under
        # scheduler_key='ssh:dgx'.
        JobRepository(conn).record_submission(
            job_id=500,
            name="ssh_job",
            status="PENDING",
            submission_source="web",
            transport_type="ssh",
            profile_name="dgx",
            scheduler_key="ssh:dgx",
        )
        JobStateTransitionRepository(conn).insert(
            job_id=500,
            from_status=None,
            to_status="PENDING",
            source="webhook",
            scheduler_key="ssh:dgx",
        )
        WatchRepository(conn).create(kind="job", target_ref="job:ssh:dgx:500")

        ssh = _StubQueueClient({500: JobStatusInfo(status="RUNNING")})
        registry = _StubRegistry({"ssh:dgx": ssh})

        poller = ActiveWatchPoller(registry=registry, db_path=db_path)  # type: ignore[arg-type]
        anyio.run(poller.run_cycle)

        # The poller must have recorded PENDING→RUNNING against the SSH
        # scheduler_key (not against 'local'). If #1 regressed, the
        # latest_for_job('local', 500) lookup inside the poller would
        # return None and no transition would be written.
        latest = JobStateTransitionRepository(conn).latest_for_job(
            500, scheduler_key="ssh:dgx"
        )
        assert latest is not None, (
            "SSH transition lost — poller joined on scheduler_key='local'"
        )
        assert latest.to_status == "RUNNING"

        # And the jobs.status update targeted the SSH row.
        job_row = JobRepository(conn).get(500, scheduler_key="ssh:dgx")
        assert job_row is not None
        assert job_row.status == "RUNNING"


# ---------------------------------------------------------------------------
# Fix #4: V5 migration only force-closes job watches
# ---------------------------------------------------------------------------


class TestV5MigrationPreservesNonJobWatches:
    """Fix #4: workflow_run / sweep_run / resource watches survive V5."""

    def _apply_through_v4(self, db_path: Path) -> sqlite3.Connection:
        conn = open_connection(db_path)
        for mig in MIGRATIONS[:-1]:  # everything except V5
            if mig.requires_fk_off:
                _apply_fk_off_migration(conn, mig)
            else:
                _apply_tx_migration(conn, mig)
        return conn

    def test_workflow_run_watch_survives_v5(self, tmp_path: Path) -> None:
        conn = self._apply_through_v4(tmp_path / "db.sqlite")
        try:
            conn.execute(
                "INSERT INTO watches (kind, target_ref, created_at) "
                "VALUES ('workflow_run', 'workflow_run:77', '2026-04-22T00:00:00Z')"
            )
            conn.execute(
                "INSERT INTO watches (kind, target_ref, created_at) "
                "VALUES ('sweep_run', 'sweep_run:42', '2026-04-22T00:00:00Z')"
            )
            conn.execute(
                "INSERT INTO watches (kind, target_ref, created_at) "
                "VALUES ('resource_threshold', 'resource:gpu:4', "
                "'2026-04-22T00:00:00Z')"
            )
            conn.execute(
                "INSERT INTO watches (kind, target_ref, created_at) "
                "VALUES ('job', 'job:999', '2026-04-22T00:00:00Z')"
            )
            conn.commit()

            v5 = MIGRATIONS[-1]
            assert v5.name == "v5_transport_scheduler_key"
            _apply_fk_off_migration(conn, v5)

            rows = conn.execute(
                "SELECT kind, closed_at FROM watches ORDER BY kind"
            ).fetchall()
        finally:
            conn.close()

        by_kind = {r["kind"]: r["closed_at"] for r in rows}
        assert by_kind["workflow_run"] is None, "V5 must not close workflow_run watches"
        assert by_kind["sweep_run"] is None, "V5 must not close sweep_run watches"
        assert by_kind["resource_threshold"] is None, (
            "V5 must not close resource_threshold watches"
        )
        # Only the job watch should have been force-closed.
        assert by_kind["job"] is not None, (
            "V5 must force-close pre-migration job watches "
            "(they carry ambiguous transport provenance)"
        )


# ---------------------------------------------------------------------------
# Fix #6: SlurmSSHAdapter.status() returns a non-refreshing BaseJob
# ---------------------------------------------------------------------------


class TestStatusSnapshotDoesNotRefresh:
    """Fix #6: Protocol contract — status() snapshot must not re-query SLURM."""

    def test_status_returns_parked_refresh_clock(self) -> None:
        from unittest.mock import patch as _patch

        from srunx.web.ssh_adapter import SlurmSSHAdapter

        # Build an adapter bypassing __init__ so we don't need real SSH.
        adapter = object.__new__(SlurmSSHAdapter)
        adapter._io_lock = __import__("threading").RLock()
        adapter._client = MagicMock()
        adapter.callbacks = []
        adapter._profile_name = "dgx"
        adapter._hostname = "testhost"
        adapter._username = "tester"
        adapter._key_filename = None
        adapter._port = 22
        adapter._proxy_jump = None
        adapter._env_vars = {}
        adapter._mounts = ()
        adapter.submission_source = "cli"

        # Stub queue_by_ids so status() gets a deterministic reply.
        def _stub_queue(job_ids):
            return {job_ids[0]: JobStatusInfo(status="RUNNING")}

        adapter.queue_by_ids = _stub_queue  # type: ignore[method-assign]

        # Any lazy refresh would invoke subprocess.run inside BaseJob.refresh.
        with _patch("srunx.models.subprocess.run") as sub_run:
            job = adapter.status(12345)
            # Touch .status AFTER the static _REFRESH_INTERVAL has long
            # elapsed — a non-parked _last_refresh would fire sacct here.
            time.sleep(0.01)
            _ = job.status
            _ = job.status

        sub_run.assert_not_called()
        # And the parked clock is far in the future.
        assert job._last_refresh > time.time() + 10**6


# ---------------------------------------------------------------------------
# Fix #5: resolve_transport forwards callbacks into the SSH pool
# ---------------------------------------------------------------------------


class TestResolveTransportForwardsCallbacks:
    """Fix #5: ``resolve_transport(callbacks=...)`` lands on SSH pool + adapter."""

    def test_callbacks_passed_to_build_ssh_handle(self, monkeypatch) -> None:
        """The forwarded callbacks list reaches ``_build_ssh_handle``."""
        monkeypatch.delenv("SRUNX_SSH_PROFILE", raising=False)

        cb = NotificationWatchCallback(
            endpoint_name="ep",
            preset="terminal",
            scheduler_key="ssh:foo",
        )

        fake_handle = TransportHandle(
            scheduler_key="ssh:foo",
            profile_name="foo",
            transport_type="ssh",
            job_ops=MagicMock(),
            queue_client=MagicMock(),
            executor_factory=MagicMock(),
            submission_context=None,
        )

        with patch(
            "srunx.transport.registry._build_ssh_handle",
            return_value=(fake_handle, None),
        ) as build:
            with resolve_transport(profile="foo", callbacks=[cb], banner=False):
                pass

            build.assert_called_once()
            args, kwargs = build.call_args
            assert args == ("foo",)
            assert kwargs.get("callbacks") == [cb]
            assert kwargs.get("submission_source") == "cli"


class TestNotificationWatchCallbackSchedulerKey:
    """Fix #5: NotificationWatchCallback threads scheduler_key into attach."""

    def test_callback_passes_scheduler_key_to_attach(self) -> None:
        cb = NotificationWatchCallback(
            endpoint_name="ep",
            preset="terminal",
            scheduler_key="ssh:dgx",
        )

        captured: dict[str, object] = {}

        def _fake_attach(**kwargs: object) -> int | None:
            captured.update(kwargs)
            return 1

        job = MagicMock()
        job.job_id = 123

        with patch(
            "srunx.cli.notification_setup.attach_notification_watch",
            side_effect=_fake_attach,
        ):
            cb.on_job_submitted(job)

        assert captured.get("scheduler_key") == "ssh:dgx"
        assert captured.get("job_id") == 123


# ---------------------------------------------------------------------------
# Fix #7: submission_source is pluggable per-handle
# ---------------------------------------------------------------------------


class TestPeekSchedulerKey:
    """Helper used by CLI callers to bind callbacks before resolve_transport."""

    def test_profile_wins(self, monkeypatch) -> None:
        monkeypatch.delenv("SRUNX_SSH_PROFILE", raising=False)
        assert peek_scheduler_key(profile="foo") == "ssh:foo"

    def test_local_wins_over_env(self, monkeypatch) -> None:
        monkeypatch.setenv("SRUNX_SSH_PROFILE", "envprof")
        assert peek_scheduler_key(local=True) == "local"

    def test_env_when_no_flags(self, monkeypatch) -> None:
        monkeypatch.setenv("SRUNX_SSH_PROFILE", "envprof")
        assert peek_scheduler_key() == "ssh:envprof"

    def test_default_is_local(self, monkeypatch) -> None:
        monkeypatch.delenv("SRUNX_SSH_PROFILE", raising=False)
        assert peek_scheduler_key() == "local"

    def test_profile_local_conflict_raises(self, monkeypatch) -> None:
        import typer

        monkeypatch.delenv("SRUNX_SSH_PROFILE", raising=False)
        with pytest.raises(typer.BadParameter):
            peek_scheduler_key(profile="foo", local=True)


class TestSubmissionSourcePerHandle:
    """Fix #7: ``submission_source`` defaults can be overridden at handle build."""

    def test_cli_submission_source_lands_on_adapter(self, monkeypatch) -> None:
        """``resolve_transport`` passes ``submission_source`` into
        ``_build_ssh_handle`` kwargs; the adapter records the origin tag."""
        monkeypatch.delenv("SRUNX_SSH_PROFILE", raising=False)

        fake_handle = TransportHandle(
            scheduler_key="ssh:foo",
            profile_name="foo",
            transport_type="ssh",
            job_ops=MagicMock(),
            queue_client=MagicMock(),
            executor_factory=MagicMock(),
            submission_context=None,
        )

        with patch(
            "srunx.transport.registry._build_ssh_handle",
            return_value=(fake_handle, None),
        ) as build:
            with resolve_transport(
                profile="foo",
                submission_source="mcp",
                banner=False,
            ):
                pass

        build.assert_called_once()
        _, kwargs = build.call_args
        assert kwargs.get("submission_source") == "mcp"


# ---------------------------------------------------------------------------
# Phase-7 review fixes (SHOULD / NIT)
# ---------------------------------------------------------------------------


class TestRegistryCacheInvalidation:
    """F1: TransportRegistry invalidates SSH cache entries when profile is removed."""

    def test_profile_deletion_invalidates_cache(self) -> None:
        """After a profile is deleted, a cache hit must re-check and return None."""
        from srunx.transport.registry import TransportRegistry

        profiles: dict[str, MagicMock] = {"foo": MagicMock(mounts=[])}

        def loader(name: str):
            return profiles.get(name)

        reg = TransportRegistry(profile_loader=loader)

        fake_handle = TransportHandle(
            scheduler_key="ssh:foo",
            profile_name="foo",
            transport_type="ssh",
            job_ops=MagicMock(),
            queue_client=MagicMock(),
            executor_factory=MagicMock(),
            submission_context=None,
        )
        fake_pool = MagicMock()
        with patch(
            "srunx.transport.registry._build_ssh_handle",
            return_value=(fake_handle, fake_pool),
        ):
            first = reg.resolve("ssh:foo")
            assert first is fake_handle

        # Simulate the admin deleting the profile.
        del profiles["foo"]

        # Cache hit path must re-validate the profile and return None.
        assert reg.resolve("ssh:foo") is None
        # Stale adapter's disconnect() must have been called.
        fake_handle.job_ops.disconnect.assert_called_once()  # type: ignore[attr-defined]
        reg.close()

    def test_close_disconnects_ssh_adapters(self) -> None:
        """F1: ``close()`` must call ``disconnect()`` on cached SSH adapters."""
        from srunx.transport.registry import TransportRegistry

        fake_handle = TransportHandle(
            scheduler_key="ssh:foo",
            profile_name="foo",
            transport_type="ssh",
            job_ops=MagicMock(),
            queue_client=MagicMock(),
            executor_factory=MagicMock(),
            submission_context=None,
        )
        reg = TransportRegistry(profile_loader=lambda name: MagicMock(mounts=[]))
        reg.register_handle(fake_handle)
        reg.close()
        fake_handle.job_ops.disconnect.assert_called_once()  # type: ignore[attr-defined]


class TestParseTargetRefHardening:
    """F3: ``_parse_target_ref`` rejects pathological inputs."""

    def test_rejects_zero_job_id(self) -> None:
        from srunx.pollers.active_watch_poller import _parse_target_ref

        assert _parse_target_ref("job:local:0", "job") is None

    def test_rejects_negative_job_id(self) -> None:
        from srunx.pollers.active_watch_poller import _parse_target_ref

        # Leading ``-`` fails the isdigit() check before int() is called.
        assert _parse_target_ref("job:local:-1", "job") is None

    def test_rejects_overflow_job_id(self) -> None:
        from srunx.pollers.active_watch_poller import _parse_target_ref

        # 13-digit tail exceeds _MAX_JOB_ID_DIGITS cap.
        assert _parse_target_ref("job:local:1234567890123", "job") is None

    def test_rejects_nul_byte_in_profile(self) -> None:
        from srunx.pollers.active_watch_poller import _parse_target_ref

        assert _parse_target_ref("job:ssh:pro\x00file:1", "job") is None

    def test_rejects_oversized_profile(self) -> None:
        from srunx.pollers.active_watch_poller import _parse_target_ref

        long_name = "a" * 65  # just past the 64-char cap
        assert _parse_target_ref(f"job:ssh:{long_name}:1", "job") is None

    def test_accepts_valid_local(self) -> None:
        from srunx.pollers.active_watch_poller import _parse_target_ref

        assert _parse_target_ref("job:local:42", "job") == ("local", 42)

    def test_accepts_valid_ssh(self) -> None:
        from srunx.pollers.active_watch_poller import _parse_target_ref

        assert _parse_target_ref("job:ssh:my-cluster_01:42", "job") == (
            "ssh:my-cluster_01",
            42,
        )


class TestValidateProfileName:
    """F4: ``validate_profile_name`` rejects invalid profile names."""

    def test_accepts_valid_names(self) -> None:
        from srunx.ssh.core.config import validate_profile_name

        # None of these should raise.
        for name in ("foo", "my-cluster_01", "dgx.prod", "a", "a" * 64):
            validate_profile_name(name)

    def test_rejects_colon(self) -> None:
        from srunx.ssh.core.config import validate_profile_name

        with pytest.raises(ValueError, match="Invalid profile name"):
            validate_profile_name("bad:name")

    def test_rejects_nul_byte(self) -> None:
        from srunx.ssh.core.config import validate_profile_name

        with pytest.raises(ValueError, match="Invalid profile name"):
            validate_profile_name("bad\x00name")

    def test_rejects_path_separator(self) -> None:
        from srunx.ssh.core.config import validate_profile_name

        with pytest.raises(ValueError, match="Invalid profile name"):
            validate_profile_name("some/path")

    def test_rejects_too_long(self) -> None:
        from srunx.ssh.core.config import validate_profile_name

        with pytest.raises(ValueError, match="Invalid profile name"):
            validate_profile_name("a" * 65)

    def test_rejects_empty(self) -> None:
        from srunx.ssh.core.config import validate_profile_name

        with pytest.raises(ValueError, match="Invalid profile name"):
            validate_profile_name("")


class TestRegistryThreadSafety:
    """F6: Concurrent resolves of the same key build only one handle."""

    def test_concurrent_resolve_converges_on_one_handle(self) -> None:
        """Two threads hitting the same ``ssh:<profile>`` key must
        converge on a single TransportHandle even though the build
        path runs outside the cache lock."""
        import threading
        import time

        from srunx.transport.registry import TransportRegistry

        built_pools: list[MagicMock] = []
        pool_lock = threading.Lock()

        def slow_build(profile_name, *args, **kwargs):
            # Simulate paramiko connect latency so both threads race
            # through the build path.
            time.sleep(0.05)
            handle = TransportHandle(
                scheduler_key=f"ssh:{profile_name}",
                profile_name=profile_name,
                transport_type="ssh",
                job_ops=MagicMock(),
                queue_client=MagicMock(),
                executor_factory=MagicMock(),
                submission_context=None,
            )
            pool = MagicMock()
            with pool_lock:
                built_pools.append(pool)
            return (handle, pool)

        reg = TransportRegistry(profile_loader=lambda name: MagicMock(mounts=[]))
        results: list[TransportHandle | None] = [None, None]

        def worker(idx: int) -> None:
            results[idx] = reg.resolve("ssh:foo")

        with patch(
            "srunx.transport.registry._build_ssh_handle",
            side_effect=slow_build,
        ):
            t1 = threading.Thread(target=worker, args=(0,))
            t2 = threading.Thread(target=worker, args=(1,))
            t1.start()
            t2.start()
            t1.join()
            t2.join()

        # Both threads must observe the same cached handle.
        assert results[0] is not None
        assert results[1] is results[0]

        # If both threads raced past the cache miss, the losing
        # thread's pool must have been closed (orphan cleanup) so only
        # the cached one stays live.
        if len(built_pools) > 1:
            closed_pools = [p for p in built_pools if p.close.called]
            assert len(closed_pools) == len(built_pools) - 1, (
                "concurrent-resolve cleanup must close all but one pool"
            )

        reg.close()


class TestPoolOrphanProtection:
    """F8: A failure after ``SlurmSSHExecutorPool(...)`` must close the pool."""

    def test_pool_closed_on_render_context_failure(self) -> None:
        from srunx.transport.registry import _build_ssh_handle

        # Use a simple object for the mount so ``mounts[0].name`` picks
        # up a plain string rather than a mock's auto-name.
        class _FakeMount:
            def __init__(self, name: str) -> None:
                self.name = name

        fake_profile = MagicMock()
        fake_profile.mounts = [_FakeMount("m1")]

        fake_cm = MagicMock()
        fake_cm.get_profile.return_value = fake_profile

        fake_pool = MagicMock()

        # SubmissionRenderContext is frozen so we can't easily force it
        # to throw at construction; instead patch it into a function
        # that raises. The registry's orphan guard must catch the
        # exception, close the pool, and re-raise.
        with (
            patch("srunx.ssh.core.config.ConfigManager", return_value=fake_cm),
            patch(
                "srunx.web.ssh_adapter.SlurmSSHAdapter",
                return_value=MagicMock(connection_spec=MagicMock()),
            ),
            patch(
                "srunx.web.ssh_executor.SlurmSSHExecutorPool",
                return_value=fake_pool,
            ),
            patch(
                "srunx.rendering.SubmissionRenderContext",
                side_effect=RuntimeError("render setup blew up"),
            ),
            pytest.raises(RuntimeError, match="render setup blew up"),
        ):
            _build_ssh_handle("foo")

        fake_pool.close.assert_called_once()


class TestBannerLabelIsHumanReadable:
    """F10: ``ResolvedTransport.label`` prefers a human-friendly rendering."""

    def test_local_label_is_friendly(self, monkeypatch) -> None:
        from srunx.transport import resolve_transport

        monkeypatch.delenv("SRUNX_SSH_PROFILE", raising=False)
        with resolve_transport(local=True, banner=False) as rt:
            assert rt.label == "local SLURM"
            # scheduler_key stays machine-parseable.
            assert rt.scheduler_key == "local"

    def test_ssh_label_is_friendly(self, monkeypatch) -> None:
        from srunx.transport import resolve_transport

        monkeypatch.delenv("SRUNX_SSH_PROFILE", raising=False)
        fake_handle = TransportHandle(
            scheduler_key="ssh:dgx",
            profile_name="dgx",
            transport_type="ssh",
            job_ops=MagicMock(),
            queue_client=MagicMock(),
            executor_factory=MagicMock(),
            submission_context=None,
        )
        with patch(
            "srunx.transport.registry._build_ssh_handle",
            return_value=(fake_handle, None),
        ):
            with resolve_transport(profile="dgx", banner=False) as rt:
                assert rt.label == "SSH: dgx"
                assert rt.scheduler_key == "ssh:dgx"


# ``tmp_srunx_db`` is provided by tests/conftest.py — no local override needed.
