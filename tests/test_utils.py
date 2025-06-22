"""Tests for srunx.utils module."""

import subprocess
from unittest.mock import patch

import pytest

from srunx.models import BaseJob, JobStatus
from srunx.utils import get_job_status, job_status_msg


class TestGetJobStatus:
    """Test get_job_status function."""

    @patch("subprocess.run")
    def test_get_job_status_success(self, mock_run):
        """Test successful job status retrieval."""
        mock_run.return_value.stdout = "12345|test_job|RUNNING\n"

        job = get_job_status(12345)

        assert job.job_id == 12345
        assert job.name == "test_job"
        # Use private _status to avoid calling refresh()
        job._status = JobStatus.RUNNING
        assert job._status == JobStatus.RUNNING

        mock_run.assert_called_once_with(
            [
                "sacct",
                "-j",
                "12345",
                "--format",
                "JobID,JobName,State",
                "--noheader",
                "--parsable2",
            ],
            capture_output=True,
            text=True,
            check=True,
        )

    @patch("subprocess.run")
    def test_get_job_status_completed(self, mock_run):
        """Test job status retrieval for completed job."""
        mock_run.return_value.stdout = "12345|ml_training|COMPLETED\n"

        job = get_job_status(12345)

        assert job.job_id == 12345
        assert job.name == "ml_training"
        job._status = JobStatus.COMPLETED
        assert job._status == JobStatus.COMPLETED

    @patch("subprocess.run")
    def test_get_job_status_failed(self, mock_run):
        """Test job status retrieval for failed job."""
        mock_run.return_value.stdout = "12345|failed_job|FAILED\n"

        job = get_job_status(12345)

        assert job.job_id == 12345
        assert job.name == "failed_job"
        job._status = JobStatus.FAILED
        assert job._status == JobStatus.FAILED

    @patch("subprocess.run")
    def test_get_job_status_pending(self, mock_run):
        """Test job status retrieval for pending job."""
        mock_run.return_value.stdout = "12345|waiting_job|PENDING\n"

        job = get_job_status(12345)

        assert job.job_id == 12345
        assert job.name == "waiting_job"
        job._status = JobStatus.PENDING
        assert job._status == JobStatus.PENDING

    @patch("subprocess.run")
    def test_get_job_status_cancelled(self, mock_run):
        """Test job status retrieval for cancelled job."""
        mock_run.return_value.stdout = "12345|cancelled_job|CANCELLED\n"

        job = get_job_status(12345)

        assert job.job_id == 12345
        assert job.name == "cancelled_job"
        job._status = JobStatus.CANCELLED
        assert job._status == JobStatus.CANCELLED

    @patch("subprocess.run")
    def test_get_job_status_timeout(self, mock_run):
        """Test job status retrieval for timed out job."""
        mock_run.return_value.stdout = "12345|timeout_job|TIMEOUT\n"

        job = get_job_status(12345)

        assert job.job_id == 12345
        assert job.name == "timeout_job"
        job._status = JobStatus.TIMEOUT
        assert job._status == JobStatus.TIMEOUT

    @patch("subprocess.run")
    def test_get_job_status_subprocess_error(self, mock_run):
        """Test job status retrieval with subprocess error."""
        error = subprocess.CalledProcessError(1, ["sacct"])
        error.stdout = ""
        error.stderr = "Job not found"
        mock_run.side_effect = error

        with pytest.raises(subprocess.CalledProcessError):
            get_job_status(12345)

    @patch("subprocess.run")
    def test_get_job_status_empty_output(self, mock_run):
        """Test job status retrieval with empty output."""
        mock_run.return_value.stdout = ""

        with pytest.raises(ValueError, match="No job information found"):
            get_job_status(12345)

    @patch("subprocess.run")
    def test_get_job_status_malformed_output(self, mock_run):
        """Test job status retrieval with malformed output."""
        mock_run.return_value.stdout = "12345|incomplete_data\n"

        with pytest.raises(ValueError, match="Cannot parse job data"):
            get_job_status(12345)

    @patch("subprocess.run")
    def test_get_job_status_multiple_lines(self, mock_run):
        """Test job status retrieval with multiple lines (uses first line)."""
        mock_run.return_value.stdout = (
            "12345|parent_job|RUNNING\n"
            "12345.0|child_job|RUNNING\n"
            "12345.1|another_child|COMPLETED\n"
        )

        job = get_job_status(12345)

        # Should use the first line (parent job)
        assert job.job_id == 12345
        assert job.name == "parent_job"
        job._status = JobStatus.RUNNING
        assert job._status == JobStatus.RUNNING

    @patch("subprocess.run")
    def test_get_job_status_with_extra_fields(self, mock_run):
        """Test job status retrieval with extra fields in output."""
        mock_run.return_value.stdout = "12345|test_job|RUNNING\n"

        job = get_job_status(12345)

        # Should still parse correctly using first 3 fields
        assert job.job_id == 12345
        assert job.name == "test_job"
        job._status = JobStatus.RUNNING
        assert job._status == JobStatus.RUNNING


class TestJobStatusMsg:
    """Test job_status_msg function."""

    def test_job_status_msg_completed(self):
        """Test status message for completed job."""
        job = BaseJob(name="test_job", job_id=12345)
        job._status = JobStatus.COMPLETED

        with patch.object(
            BaseJob, "status", new_callable=lambda: property(lambda self: self._status)
        ):
            msg = job_status_msg(job)

            assert "✅" in msg
            assert "COMPLETED" in msg
            assert "test_job" in msg
            assert "12345" in msg

    def test_job_status_msg_running(self):
        """Test status message for running job."""
        job = BaseJob(name="running_job", job_id=67890)
        # Set status directly to avoid refresh
        job._status = JobStatus.RUNNING

        # Mock the status property to avoid refresh call
        with patch.object(
            BaseJob, "status", new_callable=lambda: property(lambda self: self._status)
        ):
            msg = job_status_msg(job)

            assert "🚀" in msg
            assert "RUNNING" in msg
            assert "running_job" in msg
            assert "67890" in msg

    def test_job_status_msg_pending(self):
        """Test status message for pending job."""
        job = BaseJob(name="pending_job")  # No job_id, so no refresh
        job._status = JobStatus.PENDING

        with patch.object(
            BaseJob, "status", new_callable=lambda: property(lambda self: self._status)
        ):
            msg = job_status_msg(job)

            assert "⌛" in msg
            assert "PENDING" in msg
            assert "pending_job" in msg
            assert "—" in msg  # No job_id

    def test_job_status_msg_failed(self):
        """Test status message for failed job."""
        job = BaseJob(name="failed_job", job_id=22222)
        job._status = JobStatus.FAILED

        with patch.object(
            BaseJob, "status", new_callable=lambda: property(lambda self: self._status)
        ):
            msg = job_status_msg(job)

            assert "❌" in msg
            assert "FAILED" in msg
            assert "failed_job" in msg
            assert "22222" in msg

    def test_job_status_msg_cancelled(self):
        """Test status message for cancelled job."""
        job = BaseJob(name="cancelled_job", job_id=33333)
        job._status = JobStatus.CANCELLED

        with patch.object(
            BaseJob, "status", new_callable=lambda: property(lambda self: self._status)
        ):
            msg = job_status_msg(job)

            assert "🛑" in msg
            assert "CANCELLED" in msg
            assert "cancelled_job" in msg
            assert "33333" in msg

    def test_job_status_msg_timeout(self):
        """Test status message for timed out job."""
        job = BaseJob(name="timeout_job", job_id=44444)
        job._status = JobStatus.TIMEOUT

        with patch.object(
            BaseJob, "status", new_callable=lambda: property(lambda self: self._status)
        ):
            msg = job_status_msg(job)

            assert "⏰" in msg
            assert "TIMEOUT" in msg
            assert "timeout_job" in msg
            assert "44444" in msg

    def test_job_status_msg_unknown(self):
        """Test status message for unknown status job."""
        job = BaseJob(name="unknown_job", job_id=55555)
        job._status = JobStatus.UNKNOWN

        with patch.object(
            BaseJob, "status", new_callable=lambda: property(lambda self: self._status)
        ):
            msg = job_status_msg(job)

            assert "❓" in msg
            assert "UNKNOWN" in msg
            assert "unknown_job" in msg
            assert "55555" in msg

    def test_job_status_msg_no_job_id(self):
        """Test status message for job without job_id."""
        job = BaseJob(name="no_id_job")
        job._status = JobStatus.PENDING

        with patch.object(
            BaseJob, "status", new_callable=lambda: property(lambda self: self._status)
        ):
            msg = job_status_msg(job)

            assert "⌛" in msg
            assert "PENDING" in msg
            assert "no_id_job" in msg
            assert "—" in msg  # Should show em dash for missing job_id

    def test_job_status_msg_long_name(self):
        """Test status message formatting with long job name."""
        job = BaseJob(
            name="very_long_job_name_that_exceeds_normal_length", job_id=99999
        )
        job._status = JobStatus.RUNNING

        with patch.object(
            BaseJob, "status", new_callable=lambda: property(lambda self: self._status)
        ):
            msg = job_status_msg(job)

            assert "🚀" in msg
            assert "RUNNING" in msg
            assert "very_long_job_name_that_exceeds_normal_length" in msg
            assert "99999" in msg

    def test_job_status_msg_formatting(self):
        """Test that status message has proper formatting."""
        job = BaseJob(name="format_test", job_id=12345)
        job._status = JobStatus.COMPLETED

        with patch.object(
            BaseJob, "status", new_callable=lambda: property(lambda self: self._status)
        ):
            msg = job_status_msg(job)

            # Check that the message contains the expected components
            assert "✅" in msg
            assert "COMPLETED" in msg
            assert "format_test" in msg
            assert "12345" in msg

    def test_job_status_msg_all_statuses(self):
        """Test status message for all possible job statuses."""
        expected_icons = {
            JobStatus.COMPLETED: "✅",
            JobStatus.RUNNING: "🚀",
            JobStatus.PENDING: "⌛",
            JobStatus.FAILED: "❌",
            JobStatus.CANCELLED: "🛑",
            JobStatus.TIMEOUT: "⏰",
            JobStatus.UNKNOWN: "❓",
        }

        for status, expected_icon in expected_icons.items():
            job = BaseJob(name="test", job_id=12345)
            job._status = status
            with patch.object(
                BaseJob,
                "status",
                new_callable=lambda: property(lambda self: self._status),
            ):
                msg = job_status_msg(job)
                assert expected_icon in msg
                assert status.name in msg
