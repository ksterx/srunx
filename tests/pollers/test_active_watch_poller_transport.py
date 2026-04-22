"""Phase 6: ActiveWatchPoller transport-aware tests.

Exercise the V5 ``target_ref`` parser and the scheduler_key group-by
loop so that watches belonging to different transports (local vs
``ssh:<profile>``) each hit the matching ``queue_by_ids`` endpoint,
and that unknown scheduler_keys degrade gracefully (AC-8.5).
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from srunx.client_protocol import JobStatusInfo
from srunx.db.repositories.job_state_transitions import (
    JobStateTransitionRepository,
)
from srunx.db.repositories.jobs import JobRepository
from srunx.db.repositories.watches import WatchRepository
from srunx.pollers.active_watch_poller import (
    ActiveWatchPoller,
    _parse_target_ref,
)

# ---------------------------------------------------------------------------
# _parse_target_ref
# ---------------------------------------------------------------------------


class TestParseTargetRef:
    """AC-8.2 / AC-8.3 / AC-8.4 — V5 grammar parser."""

    def test_local_form(self) -> None:
        # AC-8.2
        assert _parse_target_ref("job:local:12345", "job") == ("local", 12345)

    def test_ssh_form(self) -> None:
        # AC-8.3
        assert _parse_target_ref("job:ssh:dgx:12345", "job") == ("ssh:dgx", 12345)

    def test_ssh_form_with_hyphens_in_profile(self) -> None:
        # Profile names may contain hyphens / underscores (just not colons).
        assert _parse_target_ref("job:ssh:my-cluster_01:7", "job") == (
            "ssh:my-cluster_01",
            7,
        )

    def test_legacy_2segment_returns_none(self) -> None:
        # AC-8.4 — after V5 migration these rows should not exist.
        assert _parse_target_ref("job:12345", "job") is None

    def test_wrong_kind_returns_none(self) -> None:
        assert _parse_target_ref("job:local:12345", "workflow_run") is None

    def test_empty_string_returns_none(self) -> None:
        assert _parse_target_ref("", "job") is None

    def test_non_int_tail_returns_none(self) -> None:
        assert _parse_target_ref("job:local:notanint", "job") is None

    def test_malformed_unknown_middle_returns_none(self) -> None:
        assert _parse_target_ref("job:foo:42", "job") is None

    def test_ssh_missing_profile_returns_none(self) -> None:
        # ``job:ssh::42`` has an empty profile segment.
        assert _parse_target_ref("job:ssh::42", "job") is None

    def test_ssh_too_many_segments_returns_none(self) -> None:
        # ``job:ssh:profile:extra:42`` has more than one profile segment;
        # profile names are guaranteed not to contain ``:`` so we reject
        # the ref rather than guess at the split.
        assert _parse_target_ref("job:ssh:profile:extra:42", "job") is None


# ---------------------------------------------------------------------------
# Transport-aware run_cycle
# ---------------------------------------------------------------------------


class _StubQueueClient:
    """Stub implementing ``SlurmClientProtocol.queue_by_ids`` only."""

    def __init__(self, responses: dict[int, JobStatusInfo]) -> None:
        self.responses = responses
        self.calls: list[list[int]] = []

    def queue_by_ids(self, job_ids: list[int]) -> dict[int, JobStatusInfo]:
        self.calls.append(list(job_ids))
        return {jid: self.responses[jid] for jid in job_ids if jid in self.responses}


class _StubRegistry:
    """Tiny ``TransportRegistry`` stand-in — resolve by lookup table."""

    def __init__(self, clients: dict[str, _StubQueueClient | None]) -> None:
        self._clients = clients
        self.resolve_calls: list[str] = []

    def resolve(self, scheduler_key: str):  # type: ignore[no-untyped-def]
        self.resolve_calls.append(scheduler_key)
        client = self._clients.get(scheduler_key)
        if client is None:
            return None

        class _Handle:
            def __init__(self, qc: _StubQueueClient) -> None:
                self.queue_client = qc
                self.job_ops = qc
                self.scheduler_key = scheduler_key

        return _Handle(client)


def _seed_job(conn: sqlite3.Connection, job_id: int, status: str = "PENDING") -> int:
    return JobRepository(conn).record_submission(
        job_id=job_id,
        name=f"job_{job_id}",
        status=status,
        submission_source="web",
    )


def _seed_watch(conn: sqlite3.Connection, target_ref: str) -> int:
    return WatchRepository(conn).create(kind="job", target_ref=target_ref)


def _seed_pending_transition(conn: sqlite3.Connection, job_id: int, status: str) -> int:
    return JobStateTransitionRepository(conn).insert(
        job_id=job_id,
        from_status=None,
        to_status=status,
        source="poller",
    )


class TestRegistryGroupBy:
    """AC-8.1 / AC-8.5 — registry-driven group-by behaviour."""

    def test_multiple_scheduler_keys_each_queried_once(
        self,
        tmp_srunx_db: tuple[sqlite3.Connection, Path],
    ) -> None:
        # AC-8.1: watches spanning local + ssh:dgx produce two
        # queue_by_ids calls, one per transport group.
        import anyio

        conn, db_path = tmp_srunx_db

        _seed_job(conn, 100, "PENDING")
        _seed_job(conn, 200, "PENDING")
        _seed_watch(conn, "job:local:100")
        _seed_watch(conn, "job:ssh:dgx:200")
        _seed_pending_transition(conn, 100, "PENDING")
        _seed_pending_transition(conn, 200, "PENDING")

        local = _StubQueueClient({100: JobStatusInfo(status="RUNNING")})
        ssh = _StubQueueClient({200: JobStatusInfo(status="RUNNING")})
        registry = _StubRegistry({"local": local, "ssh:dgx": ssh})

        poller = ActiveWatchPoller(registry=registry, db_path=db_path)  # type: ignore[arg-type]
        anyio.run(poller.run_cycle)

        assert local.calls == [[100]]
        assert ssh.calls == [[200]]
        # Both scheduler_keys were resolved at least once.
        assert "local" in registry.resolve_calls
        assert "ssh:dgx" in registry.resolve_calls

    def test_unknown_scheduler_key_warned_and_skipped(
        self,
        tmp_srunx_db: tuple[sqlite3.Connection, Path],
    ) -> None:
        # AC-8.5: watches whose profile has been deleted stay open;
        # the poller logs a warning and keeps processing other groups.
        import anyio

        conn, db_path = tmp_srunx_db

        _seed_job(conn, 10, "PENDING")
        _seed_job(conn, 20, "PENDING")
        _seed_watch(conn, "job:local:10")
        _seed_watch(conn, "job:ssh:ghost:20")
        _seed_pending_transition(conn, 10, "PENDING")
        _seed_pending_transition(conn, 20, "PENDING")

        local = _StubQueueClient({10: JobStatusInfo(status="RUNNING")})
        # ``ssh:ghost`` is intentionally missing — registry.resolve returns None.
        registry = _StubRegistry({"local": local, "ssh:ghost": None})

        poller = ActiveWatchPoller(registry=registry, db_path=db_path)  # type: ignore[arg-type]
        # Should not raise: the ssh:ghost group is skipped, local is processed.
        anyio.run(poller.run_cycle)

        assert local.calls == [[10]]

    def test_empty_cycle_is_noop(
        self,
        tmp_srunx_db: tuple[sqlite3.Connection, Path],
    ) -> None:
        import anyio

        _conn, db_path = tmp_srunx_db
        registry = _StubRegistry({"local": _StubQueueClient({})})
        poller = ActiveWatchPoller(registry=registry, db_path=db_path)  # type: ignore[arg-type]
        anyio.run(poller.run_cycle)


class TestBackCompatConstructor:
    """Constructor requires at least one of ``registry`` / ``slurm_client``."""

    def test_requires_registry_or_slurm_client(self) -> None:
        with pytest.raises(ValueError):
            ActiveWatchPoller()

    def test_legacy_slurm_client_still_accepted(
        self,
        tmp_srunx_db: tuple[sqlite3.Connection, Path],
    ) -> None:
        # Pre-Phase-6 call shape: positional ``slurm_client``. The
        # existing poller tests rely on this so we must not break it.
        _conn, db_path = tmp_srunx_db
        stub = _StubQueueClient({})
        # Positional arg should still work.
        poller = ActiveWatchPoller(stub, db_path=db_path)
        assert poller is not None
