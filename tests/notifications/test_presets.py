"""Truth-table coverage for :func:`srunx.notifications.presets.should_deliver`."""

from __future__ import annotations

import pytest
from srunx.notifications.presets import should_deliver

TERMINAL_JOB_STATUSES = [
    "COMPLETED",
    "FAILED",
    "CANCELLED",
    "TIMEOUT",
    "NODE_FAIL",
    "PREEMPTED",
    "OUT_OF_MEMORY",
]
NON_TERMINAL_JOB_STATUSES = ["PENDING", "RUNNING", "CONFIGURING"]

TERMINAL_WR_STATUSES = ["completed", "failed", "cancelled"]
NON_TERMINAL_WR_STATUSES = ["pending", "running"]


class TestPresetTerminal:
    """``preset='terminal'`` only fires for terminal status_changed events."""

    @pytest.mark.parametrize("status", TERMINAL_JOB_STATUSES)
    def test_job_terminal_fires(self, status: str) -> None:
        assert should_deliver("terminal", "job.status_changed", status) is True

    @pytest.mark.parametrize("status", NON_TERMINAL_JOB_STATUSES)
    def test_job_non_terminal_skipped(self, status: str) -> None:
        assert should_deliver("terminal", "job.status_changed", status) is False

    @pytest.mark.parametrize("status", TERMINAL_WR_STATUSES)
    def test_workflow_terminal_fires(self, status: str) -> None:
        assert should_deliver("terminal", "workflow_run.status_changed", status) is True

    @pytest.mark.parametrize("status", NON_TERMINAL_WR_STATUSES)
    def test_workflow_non_terminal_skipped(self, status: str) -> None:
        assert (
            should_deliver("terminal", "workflow_run.status_changed", status) is False
        )

    def test_job_submitted_skipped(self) -> None:
        assert should_deliver("terminal", "job.submitted", None) is False

    def test_resource_threshold_skipped(self) -> None:
        assert should_deliver("terminal", "resource.threshold_crossed", None) is False

    def test_scheduled_report_skipped(self) -> None:
        assert should_deliver("terminal", "scheduled_report.due", None) is False


class TestPresetRunningAndTerminal:
    """``preset='running_and_terminal'`` adds RUNNING / running on top of terminal."""

    def test_job_running_fires(self) -> None:
        assert (
            should_deliver("running_and_terminal", "job.status_changed", "RUNNING")
            is True
        )

    def test_workflow_running_fires(self) -> None:
        assert (
            should_deliver(
                "running_and_terminal", "workflow_run.status_changed", "running"
            )
            is True
        )

    @pytest.mark.parametrize("status", TERMINAL_JOB_STATUSES)
    def test_job_terminal_still_fires(self, status: str) -> None:
        assert (
            should_deliver("running_and_terminal", "job.status_changed", status) is True
        )

    @pytest.mark.parametrize("status", TERMINAL_WR_STATUSES)
    def test_workflow_terminal_still_fires(self, status: str) -> None:
        assert (
            should_deliver(
                "running_and_terminal", "workflow_run.status_changed", status
            )
            is True
        )

    def test_job_pending_skipped(self) -> None:
        assert (
            should_deliver("running_and_terminal", "job.status_changed", "PENDING")
            is False
        )

    def test_workflow_pending_skipped(self) -> None:
        assert (
            should_deliver(
                "running_and_terminal", "workflow_run.status_changed", "pending"
            )
            is False
        )

    def test_job_submitted_skipped(self) -> None:
        assert should_deliver("running_and_terminal", "job.submitted", None) is False

    def test_resource_threshold_skipped(self) -> None:
        assert (
            should_deliver("running_and_terminal", "resource.threshold_crossed", None)
            is False
        )


class TestPresetAll:
    """``preset='all'`` fires for every event kind."""

    def test_job_submitted(self) -> None:
        assert should_deliver("all", "job.submitted", None) is True

    @pytest.mark.parametrize(
        "status", TERMINAL_JOB_STATUSES + NON_TERMINAL_JOB_STATUSES
    )
    def test_job_status_changed(self, status: str) -> None:
        assert should_deliver("all", "job.status_changed", status) is True

    @pytest.mark.parametrize("status", TERMINAL_WR_STATUSES + NON_TERMINAL_WR_STATUSES)
    def test_workflow_status_changed(self, status: str) -> None:
        assert should_deliver("all", "workflow_run.status_changed", status) is True

    def test_resource_threshold_crossed(self) -> None:
        assert should_deliver("all", "resource.threshold_crossed", None) is True

    def test_scheduled_report_due(self) -> None:
        assert should_deliver("all", "scheduled_report.due", None) is True


class TestPresetDigest:
    """``preset='digest'`` is a Phase 2 placeholder — always False in Phase 1."""

    @pytest.mark.parametrize(
        "event_kind,to_status",
        [
            ("job.submitted", None),
            ("job.status_changed", "COMPLETED"),
            ("job.status_changed", "RUNNING"),
            ("workflow_run.status_changed", "completed"),
            ("resource.threshold_crossed", None),
            ("scheduled_report.due", None),
        ],
    )
    def test_always_false(self, event_kind: str, to_status: str | None) -> None:
        assert should_deliver("digest", event_kind, to_status) is False
