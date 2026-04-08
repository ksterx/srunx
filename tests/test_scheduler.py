"""Tests for ScheduledReporter._get_historical_counts (L9)."""

from unittest.mock import MagicMock, patch

from srunx.monitor.report_types import ReportConfig
from srunx.monitor.scheduler import ScheduledReporter


def _make_reporter(timeframe: str = "24h") -> ScheduledReporter:
    config = ReportConfig(schedule="1h", timeframe=timeframe)
    return ScheduledReporter(
        client=MagicMock(),
        callback=MagicMock(),
        config=config,
    )


class TestGetHistoricalCounts:
    """Test sacct-based historical job counting."""

    @patch("subprocess.run")
    def test_parses_completed_failed_cancelled(self, mock_run):
        mock_run.return_value = MagicMock(
            returncode=0,
            stdout="COMPLETED\nCOMPLETED\nFAILED\nCANCELLED by 1000\nCOMPLETED\n",
        )
        reporter = _make_reporter()
        c, f, ca = reporter._get_historical_counts()
        assert c == 3
        assert f == 1
        assert ca == 1

    @patch("subprocess.run")
    def test_handles_empty_output(self, mock_run):
        mock_run.return_value = MagicMock(returncode=0, stdout="")
        reporter = _make_reporter()
        assert reporter._get_historical_counts() == (0, 0, 0)

    @patch("subprocess.run")
    def test_handles_sacct_failure(self, mock_run):
        mock_run.return_value = MagicMock(returncode=1, stdout="")
        reporter = _make_reporter()
        assert reporter._get_historical_counts() == (0, 0, 0)

    @patch("subprocess.run", side_effect=FileNotFoundError)
    def test_handles_sacct_not_found(self, _mock_run):
        reporter = _make_reporter()
        assert reporter._get_historical_counts() == (0, 0, 0)

    @patch("subprocess.run")
    def test_passes_user_filter(self, mock_run):
        mock_run.return_value = MagicMock(returncode=0, stdout="COMPLETED\n")
        reporter = _make_reporter()
        reporter._get_historical_counts(user="testuser")
        cmd = mock_run.call_args[0][0]
        assert "--user" in cmd
        assert "testuser" in cmd

    @patch("subprocess.run")
    def test_uses_config_timeframe(self, mock_run):
        mock_run.return_value = MagicMock(returncode=0, stdout="")
        reporter = _make_reporter(timeframe="48h")
        reporter._get_historical_counts()
        cmd = mock_run.call_args[0][0]
        assert "now-48h" in cmd
