"""Tests for :class:`srunx.observability.storage.repositories.workflow_runs.WorkflowRunRepository`."""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from datetime import datetime
from pathlib import Path

import pytest

from srunx.observability.storage.connection import open_connection
from srunx.observability.storage.migrations import apply_migrations
from srunx.observability.storage.repositories.workflow_runs import WorkflowRunRepository


@pytest.fixture
def conn(tmp_path: Path) -> Iterator[sqlite3.Connection]:
    db = tmp_path / "srunx.observability.storage"
    connection = open_connection(db)
    apply_migrations(connection)
    try:
        yield connection
    finally:
        connection.close()


@pytest.fixture
def repo(conn: sqlite3.Connection) -> WorkflowRunRepository:
    return WorkflowRunRepository(conn)


def test_create_returns_positive_id(repo: WorkflowRunRepository) -> None:
    run_id = repo.create("ml_pipeline", "/tmp/wf.yaml", {"model": "resnet"}, "cli")
    assert run_id > 0


def test_create_sets_pending_status(repo: WorkflowRunRepository) -> None:
    run_id = repo.create("wf", None, None, "web")
    run = repo.get(run_id)
    assert run is not None
    assert run.status == "pending"
    assert run.workflow_name == "wf"
    assert run.workflow_yaml_path is None
    assert run.args is None
    assert run.triggered_by == "web"
    assert isinstance(run.started_at, datetime)
    assert run.completed_at is None


def test_create_serializes_args_dict(repo: WorkflowRunRepository) -> None:
    run_id = repo.create("wf", None, {"x": 1, "y": "z"}, "cli")
    run = repo.get(run_id)
    assert run is not None
    assert run.args == {"x": 1, "y": "z"}


def test_get_missing_returns_none(repo: WorkflowRunRepository) -> None:
    assert repo.get(9999) is None


def test_list_orders_by_started_at_desc(repo: WorkflowRunRepository) -> None:
    first = repo.create("a", None, None, "cli")
    second = repo.create("b", None, None, "cli")
    third = repo.create("c", None, None, "cli")

    runs = repo.list_all()
    assert [r.id for r in runs] == [third, second, first]


def test_list_empty(repo: WorkflowRunRepository) -> None:
    assert repo.list_all() == []


def test_list_incomplete_filters_by_status(repo: WorkflowRunRepository) -> None:
    a = repo.create("a", None, None, "cli")  # pending
    b = repo.create("b", None, None, "cli")  # will go running
    c = repo.create("c", None, None, "cli")  # will go completed
    d = repo.create("d", None, None, "cli")  # will go failed

    repo.update_status(b, "running")
    repo.update_status(c, "completed", completed_at="2026-04-18T00:00:00Z")
    repo.update_status(d, "failed", error="boom")

    incomplete_ids = {r.id for r in repo.list_incomplete()}
    assert incomplete_ids == {a, b}


def test_update_status_returns_true_on_match(repo: WorkflowRunRepository) -> None:
    run_id = repo.create("wf", None, None, "cli")
    assert repo.update_status(run_id, "running") is True


def test_update_status_returns_false_on_missing(
    repo: WorkflowRunRepository,
) -> None:
    assert repo.update_status(9999, "running") is False


def test_update_status_sets_error_and_completed_at(
    repo: WorkflowRunRepository,
) -> None:
    run_id = repo.create("wf", None, None, "cli")
    completed = "2026-04-18T12:34:56.000Z"
    repo.update_status(run_id, "failed", error="kaboom", completed_at=completed)

    run = repo.get(run_id)
    assert run is not None
    assert run.status == "failed"
    assert run.error == "kaboom"
    assert isinstance(run.completed_at, datetime)
    assert run.completed_at.year == 2026


def test_update_status_only_status(repo: WorkflowRunRepository) -> None:
    run_id = repo.create("wf", None, None, "cli")
    repo.update_status(run_id, "running")
    run = repo.get(run_id)
    assert run is not None
    assert run.status == "running"
    assert run.error is None
    assert run.completed_at is None
