"""Tests for ``srunx.observability.storage.cli_helpers`` — the CLI-side state-DB bridge.

Focused on the ``scheduler_key`` push-down that ``srunx history``
relies on: CLI history queries must scope to the resolved transport
so cross-cluster jobs don't bleed into each other's listings.
"""

from __future__ import annotations

import pytest

from srunx.domain import Job, JobEnvironment, JobResource
from srunx.observability.storage.cli_helpers import (
    create_cli_workflow_run,
    list_recent_jobs,
    record_submission_from_job,
)


def _record_ssh_job(
    job: Job,
    *,
    profile: str,
    workflow_run_id: int | None = None,
) -> None:
    """Record a job under the SSH transport triple.

    JobRepository validates ``(transport_type, profile_name,
    scheduler_key)`` together so callers can't drift one without the
    others — this helper bundles the trio for SSH-tagged tests.
    """
    record_submission_from_job(
        job,
        transport_type="ssh",
        profile_name=profile,
        scheduler_key=f"ssh:{profile}",
        workflow_run_id=workflow_run_id,
    )


class TestSchedulerKeyFilter:
    """SSH-parity for ``srunx history``: scheduler_key push-down.

    The CLI passes ``scheduler_key="ssh:<profile>"`` (or ``"local"``)
    into the helpers; the SQL ``WHERE`` filter must scope rows to that
    transport so cross-cluster jobs don't bleed into each other's
    history. ``scheduler_key=None`` keeps the legacy behaviour (every
    transport).
    """

    def test_list_recent_jobs_filters_by_scheduler_key(self, _isolated_db):
        record_submission_from_job(_make_job("local_job", 10, gpus=2))
        _record_ssh_job(_make_job("dgx_job", 11, gpus=4), profile="dgx")
        _record_ssh_job(_make_job("aws_job", 12, gpus=8), profile="aws")

        local_jobs = list_recent_jobs(scheduler_key="local")
        assert {j["job_name"] for j in local_jobs} == {"local_job"}

        dgx_jobs = list_recent_jobs(scheduler_key="ssh:dgx")
        assert {j["job_name"] for j in dgx_jobs} == {"dgx_job"}

        # No filter → every transport surfaces (legacy Web UI behaviour).
        all_jobs = list_recent_jobs()
        assert {j["job_name"] for j in all_jobs} == {
            "local_job",
            "dgx_job",
            "aws_job",
        }

    def test_list_recent_jobs_id_filter_respects_scheduler_key(self, _isolated_db):
        """``-j <id>`` push-down still scopes to the requested transport.

        Without the AND-clause, ``srunx history -j 100 --profile dgx``
        would happily return a local job that happens to share the
        SLURM ``job_id`` (different clusters reuse the int counter).
        """
        record_submission_from_job(_make_job("local_100", 100))
        _record_ssh_job(_make_job("dgx_100", 100), profile="dgx")

        # No scope — both rows surface (composite uniqueness on
        # (scheduler_key, job_id) means both can coexist).
        both = list_recent_jobs(job_ids=[100])
        assert {j["job_name"] for j in both} == {"local_100", "dgx_100"}

        only_ssh = list_recent_jobs(job_ids=[100], scheduler_key="ssh:dgx")
        assert {j["job_name"] for j in only_ssh} == {"dgx_100"}


@pytest.fixture
def _isolated_db(tmp_path, monkeypatch):
    """Redirect the state DB to a per-test tmp dir."""
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path))
    # Clear any cached config so the fresh XDG_CONFIG_HOME applies.
    import srunx.common.config

    srunx.common.config._config = None
    yield
    srunx.common.config._config = None


def _make_job(name: str, job_id: int, gpus: int = 0) -> Job:
    return Job(
        name=name,
        command=["echo", name],
        resources=JobResource(nodes=1, gpus_per_node=gpus),
        environment=JobEnvironment(),
        job_id=job_id,
    )


class TestCreateCliWorkflowRun:
    def test_returns_new_id(self, _isolated_db):
        run_id = create_cli_workflow_run(workflow_name="pipeline")
        assert isinstance(run_id, int)
        assert run_id > 0

    def test_returns_none_on_db_failure(self, _isolated_db, monkeypatch):
        """Best-effort contract — any DB error returns None, not raises."""
        import srunx.observability.storage.cli_helpers as cli_helpers

        def boom(*a, **kw):
            raise RuntimeError("disk full")

        monkeypatch.setattr(cli_helpers, "init_db", boom, raising=False)
        # The import-time binding is inside the function body, so we
        # patch the concrete symbol the function imports.
        import srunx.observability.storage.connection as connection_mod

        monkeypatch.setattr(connection_mod, "init_db", boom)
        assert create_cli_workflow_run(workflow_name="pipeline") is None
