"""Tests for ScheduledReporter._get_historical_counts (L9).

Coverage split:

- ``TestHistoricalCountsDbFirst`` — new P2-6 path: state DB via
  ``JobRepository.count_by_status_in_range``; user-less calls prefer
  this over the sacct shell-out.
- ``TestHistoricalCountsSacctFallback`` — legacy sacct parsing, still
  active when ``user`` is given or the DB call fails. Each test here
  stubs ``_db_historical_counts`` to ``None`` so the fallback runs.
- ``TestParseTimeframeToDelta`` — the new timeframe parser the DB
  path relies on.
"""

from datetime import timedelta
from unittest.mock import MagicMock, patch

import pytest

from srunx.monitor.report_types import ReportConfig
from srunx.monitor.scheduler import ScheduledReporter


def _make_reporter(timeframe: str = "24h") -> ScheduledReporter:
    config = ReportConfig(schedule="1h", timeframe=timeframe)
    return ScheduledReporter(
        client=MagicMock(),
        callback=MagicMock(),
        config=config,
    )


class TestHistoricalCountsDbFirst:
    """Preferred DB path introduced in P2-6 #C."""

    def test_db_path_returns_counts_when_rows_exist(self, tmp_srunx_db, monkeypatch):
        from srunx.db.repositories.base import now_iso
        from srunx.db.repositories.jobs import JobRepository

        conn, _db_path = tmp_srunx_db
        repo = JobRepository(conn)

        # Seed three rows inside the 24h window — submitted_at = now.
        ts = now_iso()
        for i, status in enumerate(["COMPLETED", "COMPLETED", "FAILED"], start=1000):
            repo.record_submission(
                job_id=i,
                name=f"job_{i}",
                status=status,
                submission_source="cli",
                submitted_at=ts,
            )
        # One CANCELLED row.
        repo.record_submission(
            job_id=2000,
            name="cancelled_job",
            status="CANCELLED",
            submission_source="cli",
            submitted_at=ts,
        )

        reporter = _make_reporter()
        assert reporter._get_historical_counts() == (2, 1, 1)

    def test_db_path_excludes_rows_outside_timeframe(self, tmp_srunx_db, monkeypatch):
        from datetime import UTC, datetime, timedelta

        from srunx.db.repositories.jobs import JobRepository

        conn, _db_path = tmp_srunx_db
        repo = JobRepository(conn)

        # Row submitted 30 hours ago — outside the 24h window.
        stale = (
            (datetime.now(UTC) - timedelta(hours=30))
            .isoformat(timespec="milliseconds")
            .replace("+00:00", "Z")
        )
        repo.record_submission(
            job_id=3000,
            name="stale",
            status="COMPLETED",
            submission_source="cli",
            submitted_at=stale,
        )

        reporter = _make_reporter(timeframe="24h")
        assert reporter._get_historical_counts() == (0, 0, 0)

    def test_db_path_skipped_when_user_given(self, tmp_srunx_db):
        """``user`` forces sacct — the state DB has no user column."""
        reporter = _make_reporter()

        # _db_historical_counts should NOT be consulted for user-scoped
        # calls. Make it explode if it is.
        reporter._db_historical_counts = MagicMock(  # type: ignore[method-assign]
            side_effect=AssertionError("DB path must not run for user-scoped call")
        )

        with patch(
            "subprocess.run",
            return_value=MagicMock(returncode=0, stdout="COMPLETED\n"),
        ):
            reporter._get_historical_counts(user="alice")


class TestHistoricalCountsSacctFallback:
    """Legacy sacct path — still active when ``user`` is set or DB fails."""

    def _stub_db_path_to_miss(self, reporter: ScheduledReporter) -> None:
        reporter._db_historical_counts = MagicMock(return_value=None)  # type: ignore[method-assign]

    @patch("subprocess.run")
    def test_parses_completed_failed_cancelled(self, mock_run):
        mock_run.return_value = MagicMock(
            returncode=0,
            stdout="COMPLETED\nCOMPLETED\nFAILED\nCANCELLED by 1000\nCOMPLETED\n",
        )
        reporter = _make_reporter()
        self._stub_db_path_to_miss(reporter)
        c, f, ca = reporter._get_historical_counts()
        assert c == 3
        assert f == 1
        assert ca == 1

    @patch("subprocess.run")
    def test_handles_empty_output(self, mock_run):
        mock_run.return_value = MagicMock(returncode=0, stdout="")
        reporter = _make_reporter()
        self._stub_db_path_to_miss(reporter)
        assert reporter._get_historical_counts() == (0, 0, 0)

    @patch("subprocess.run")
    def test_handles_sacct_failure(self, mock_run):
        mock_run.return_value = MagicMock(returncode=1, stdout="")
        reporter = _make_reporter()
        self._stub_db_path_to_miss(reporter)
        assert reporter._get_historical_counts() == (0, 0, 0)

    @patch("subprocess.run", side_effect=FileNotFoundError)
    def test_handles_sacct_not_found(self, _mock_run):
        reporter = _make_reporter()
        self._stub_db_path_to_miss(reporter)
        assert reporter._get_historical_counts() == (0, 0, 0)

    @patch("subprocess.run")
    def test_passes_user_filter(self, mock_run):
        mock_run.return_value = MagicMock(returncode=0, stdout="COMPLETED\n")
        reporter = _make_reporter()
        # user-scoped calls always hit sacct — no stub needed.
        reporter._get_historical_counts(user="testuser")
        cmd = mock_run.call_args[0][0]
        assert "--user" in cmd
        assert "testuser" in cmd

    @patch("subprocess.run")
    def test_uses_config_timeframe(self, mock_run):
        mock_run.return_value = MagicMock(returncode=0, stdout="")
        reporter = _make_reporter(timeframe="48h")
        self._stub_db_path_to_miss(reporter)
        reporter._get_historical_counts()
        cmd = mock_run.call_args[0][0]
        assert "now-48h" in cmd


class TestParseTimeframeToDelta:
    """Parser backing the DB path's timeframe→timedelta conversion."""

    def test_hours(self):
        assert ScheduledReporter._parse_timeframe_to_delta("24h") == timedelta(hours=24)

    def test_minutes(self):
        assert ScheduledReporter._parse_timeframe_to_delta("30m") == timedelta(
            minutes=30
        )

    def test_days(self):
        assert ScheduledReporter._parse_timeframe_to_delta("7d") == timedelta(days=7)

    def test_seconds(self):
        assert ScheduledReporter._parse_timeframe_to_delta("90s") == timedelta(
            seconds=90
        )

    @pytest.mark.parametrize("bad", ["", "24", "h24", "24x", "1.5h", "--1h"])
    def test_rejects_invalid(self, bad):
        with pytest.raises(ValueError):
            ScheduledReporter._parse_timeframe_to_delta(bad)
