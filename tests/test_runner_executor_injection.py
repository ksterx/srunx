"""Tests for ``WorkflowRunner``'s injectable executor factory.

Phase 2 Step 1 of the SSH sweep integration. Verifies that:

* Default (``executor_factory=None``) preserves the legacy shape: the
  runner still uses ``self.slurm`` under the hood so existing
  ``@patch("srunx.runner.Slurm")`` tests keep working.
* A custom factory is leased as a context manager, its ``run`` /
  ``get_job_output_detailed`` methods are invoked on the yielded
  executor, and the CM's ``__exit__`` runs after each use.
"""

from __future__ import annotations

from contextlib import contextmanager
from typing import Any
from unittest.mock import Mock, patch

from srunx.models import Job, JobEnvironment, JobStatus, Workflow
from srunx.runner import WorkflowRunner


class _FakeExecutor:
    """Minimal :class:`WorkflowJobExecutorProtocol` stand-in for tests."""

    def __init__(self) -> None:
        self.run_calls: list[tuple[Any, dict[str, Any]]] = []
        self.log_calls: list[tuple[Any, Any]] = []

    def run(
        self,
        job: Any,
        *,
        workflow_name: str | None = None,
        workflow_run_id: int | None = None,
    ) -> Any:
        self.run_calls.append(
            (job, {"workflow_name": workflow_name, "workflow_run_id": workflow_run_id})
        )
        job.status = JobStatus.COMPLETED
        return job

    def get_job_output_detailed(
        self,
        job_id: int | str,
        job_name: str | None = None,
        skip_content: bool = False,
    ) -> dict[str, str | list[str] | None]:
        self.log_calls.append((job_id, job_name))
        return {"found_files": [], "output": "", "error": "", "searched_dirs": []}


def _make_workflow() -> tuple[Workflow, Job]:
    job = Job(
        name="test_job",
        command=["echo", "hello"],
        environment=JobEnvironment(conda="env"),
    )
    job._status = JobStatus.PENDING
    return Workflow(name="test", jobs=[job]), job


@patch("srunx.runner._transition_workflow_run")
@patch("srunx.db.cli_helpers.create_cli_workflow_run")
@patch("srunx.runner.Slurm")
def test_default_factory_uses_self_slurm(
    mock_slurm_class: Mock,
    mock_create: Mock,
    _mock_transition: Mock,
) -> None:
    """With ``executor_factory=None`` the runner still talks to ``self.slurm``.

    This is the backward-compat anchor: every existing test that patches
    ``srunx.runner.Slurm`` depends on this behaviour.
    """
    mock_slurm = Mock()
    mock_slurm_class.return_value = mock_slurm
    mock_create.return_value = 123

    workflow, job = _make_workflow()

    def mock_run(j: Any, **kwargs: Any) -> Any:
        j.status = JobStatus.COMPLETED
        return j

    mock_slurm.run.side_effect = mock_run

    runner = WorkflowRunner(workflow)
    assert runner._executor_factory is None  # default

    results = runner.run()

    assert "test_job" in results
    mock_slurm.run.assert_called_once()
    call_kwargs = mock_slurm.run.call_args.kwargs
    assert call_kwargs["workflow_name"] == "test"
    assert call_kwargs["workflow_run_id"] == 123


@patch("srunx.runner._transition_workflow_run")
@patch("srunx.db.cli_helpers.create_cli_workflow_run")
@patch("srunx.runner.Slurm")
def test_custom_factory_is_leased_and_invoked(
    _mock_slurm_class: Mock,
    mock_create: Mock,
    _mock_transition: Mock,
) -> None:
    """A user-supplied factory is leased and its executor receives the run call.

    The legacy ``self.slurm`` must NOT be called because the factory
    takes over.
    """
    mock_create.return_value = 777

    fake = _FakeExecutor()
    enter_count = 0
    exit_count = 0

    @contextmanager
    def factory():  # noqa: ANN202
        nonlocal enter_count, exit_count
        enter_count += 1
        try:
            yield fake
        finally:
            exit_count += 1

    workflow, job = _make_workflow()
    runner = WorkflowRunner(workflow, executor_factory=factory)

    results = runner.run()

    assert "test_job" in results
    assert len(fake.run_calls) == 1
    submitted_job, kwargs = fake.run_calls[0]
    assert submitted_job is job
    assert kwargs == {"workflow_name": "test", "workflow_run_id": 777}

    # Context manager lifecycle: every lease must be paired with a release.
    assert enter_count == exit_count == 1


@patch("srunx.runner._transition_workflow_run")
@patch("srunx.db.cli_helpers.create_cli_workflow_run")
@patch("srunx.runner.Slurm")
def test_custom_factory_is_used_for_log_retrieval_on_failure(
    _mock_slurm_class: Mock,
    mock_create: Mock,
    _mock_transition: Mock,
) -> None:
    """``get_job_output_detailed`` also flows through the factory.

    The runner calls it on job failure to surface SLURM logs; the
    injected executor — not ``self.slurm`` — must receive that call.
    """
    mock_create.return_value = 42

    fake = _FakeExecutor()

    def failing_run(
        job: Any,
        *,
        workflow_name: str | None = None,
        workflow_run_id: int | None = None,
    ) -> Any:
        job.job_id = 5555
        job.status = JobStatus.FAILED
        return job

    fake.run = failing_run  # type: ignore[method-assign]

    enter_count = 0
    exit_count = 0

    @contextmanager
    def factory():  # noqa: ANN202
        nonlocal enter_count, exit_count
        enter_count += 1
        try:
            yield fake
        finally:
            exit_count += 1

    workflow, _job = _make_workflow()
    runner = WorkflowRunner(workflow, executor_factory=factory)

    try:
        runner.run()
    except RuntimeError:
        # Expected — job failed.
        pass

    # One lease for run, one for log retrieval on failure.
    assert enter_count == exit_count >= 1
    assert len(fake.log_calls) >= 1
    job_id, job_name = fake.log_calls[0]
    assert job_id == 5555
    assert job_name == "test_job"


def test_from_yaml_passes_executor_factory_through(tmp_path: Any) -> None:
    """``from_yaml`` forwards ``executor_factory`` to ``__init__``."""
    yaml_path = tmp_path / "wf.yaml"
    yaml_path.write_text(
        """
name: passthrough
jobs:
  - name: only
    command: ["echo", "hi"]
""".strip()
    )

    @contextmanager
    def factory():  # noqa: ANN202
        # Never actually invoked by this test — we only check wiring.
        yield _FakeExecutor()

    runner = WorkflowRunner.from_yaml(yaml_path, executor_factory=factory)

    assert runner._executor_factory is factory
