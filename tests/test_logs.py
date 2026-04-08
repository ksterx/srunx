"""Tests for log streaming functionality."""

from unittest.mock import patch

import pytest

from srunx.client import Slurm
from srunx.models import Job, JobEnvironment, JobResource, JobStatus


class TestLogStreaming:
    """Test log streaming functionality for local SLURM."""

    def test_tail_log_static_mode(self, tmp_path, monkeypatch):
        """Test static log display mode."""
        # Create mock log file
        log_file = tmp_path / "job_123.log"
        log_file.write_text("Line 1\nLine 2\nLine 3\n")

        # Mock get_job_output_detailed to return our test file
        def mock_get_job_output_detailed(
            self, job_id, job_name=None, skip_content=False
        ):
            return {
                "found_files": [str(log_file)],
                "primary_log": str(log_file),
                "output": "" if skip_content else log_file.read_text(),
                "error": "",
                "slurm_log_dir": str(tmp_path),
                "searched_dirs": [str(tmp_path)],
            }

        monkeypatch.setattr(
            Slurm, "get_job_output_detailed", mock_get_job_output_detailed
        )

        client = Slurm()

        # Mock console for testing - patch where it's imported
        with patch("rich.console.Console") as MockConsole:
            mock_console = MockConsole.return_value

            # Test without last_n
            client.tail_log(job_id=123, job_name="test_job", follow=False)

            # Verify console.print was called
            assert mock_console.print.called

    def test_tail_log_with_last_n(self, tmp_path, monkeypatch):
        """Test static log display with last N lines."""
        # Create mock log file with multiple lines
        log_content = "\n".join([f"Line {i}" for i in range(1, 101)])
        log_file = tmp_path / "job_456.log"
        log_file.write_text(log_content)

        # Mock get_job_output_detailed
        def mock_get_job_output_detailed(
            self, job_id, job_name=None, skip_content=False
        ):
            return {
                "found_files": [str(log_file)],
                "primary_log": str(log_file),
                "output": "" if skip_content else log_file.read_text(),
                "error": "",
                "slurm_log_dir": str(tmp_path),
                "searched_dirs": [str(tmp_path)],
            }

        monkeypatch.setattr(
            Slurm, "get_job_output_detailed", mock_get_job_output_detailed
        )

        client = Slurm()

        with patch("rich.console.Console") as MockConsole:
            mock_console = MockConsole.return_value

            # Test with last_n=10
            client.tail_log(job_id=456, job_name="test_job", follow=False, last_n=10)

            assert mock_console.print.called

    def test_tail_log_no_file_found(self, monkeypatch):
        """Test log display when no log file is found."""

        # Mock get_job_output_detailed to return empty results
        def mock_get_job_output_detailed(
            self, job_id, job_name=None, skip_content=False
        ):
            return {
                "found_files": [],
                "primary_log": None,
                "output": "",
                "error": "",
                "slurm_log_dir": None,
                "searched_dirs": ["/tmp", "./"],
            }

        monkeypatch.setattr(
            Slurm, "get_job_output_detailed", mock_get_job_output_detailed
        )

        client = Slurm()

        with patch("rich.console.Console") as MockConsole:
            mock_console = MockConsole.return_value

            # Test when no log file found
            client.tail_log(job_id=789, job_name="missing_job", follow=False)

            # Verify error message was printed
            assert mock_console.print.called


class TestTemplateManagement:
    """Test template management functionality."""

    def test_list_templates(self):
        """Test listing available templates."""
        from srunx.template import list_templates

        templates = list_templates()

        assert len(templates) == 1
        assert templates[0]["name"] == "base"

    def test_get_template_path(self):
        """Test getting template path."""
        from srunx.template import get_template_path

        # Test valid template
        path = get_template_path("base")
        assert path.endswith("base.slurm.jinja")

        # Test invalid template
        with pytest.raises(ValueError):
            get_template_path("nonexistent-template")

    def test_get_template_info(self):
        """Test getting template information."""
        from srunx.template import get_template_info

        info = get_template_info("base")

        assert info["name"] == "base"
        assert "description" in info
        assert "use_case" in info
        assert "path" in info


class TestJobHistory:
    """Test job history tracking functionality."""

    def test_job_history_record(self, tmp_path):
        """Test recording job to history."""
        from srunx.history import JobHistory

        db_path = tmp_path / "test_history.db"
        history = JobHistory(db_path=db_path)

        # Create a test job
        job = Job(
            name="test_job",
            command=["python", "train.py"],
            resources=JobResource(nodes=1, gpus_per_node=1),
            environment=JobEnvironment(),
        )
        job.job_id = 12345
        job.status = JobStatus.PENDING

        # Record job
        history.record_job(job)

        # Verify job was recorded
        recent_jobs = history.get_recent_jobs(limit=10)
        assert len(recent_jobs) > 0
        assert recent_jobs[0]["job_id"] == 12345
        assert recent_jobs[0]["job_name"] == "test_job"

    def test_job_history_update_completion(self, tmp_path):
        """Test updating job completion status."""
        from datetime import datetime

        from srunx.history import JobHistory

        db_path = tmp_path / "test_history.db"
        history = JobHistory(db_path=db_path)

        # Create and record a test job
        job = Job(
            name="completion_test",
            command=["python", "script.py"],
            resources=JobResource(nodes=1),
            environment=JobEnvironment(),
        )
        job.job_id = 99999
        job.status = JobStatus.PENDING

        history.record_job(job)

        # Update completion
        history.update_job_completion(
            job_id=99999, status=JobStatus.COMPLETED, completed_at=datetime.now()
        )

        # Verify completion was updated
        recent_jobs = history.get_recent_jobs(limit=10)
        completed_job = next((j for j in recent_jobs if j["job_id"] == 99999), None)

        assert completed_job is not None
        assert completed_job["status"] == "COMPLETED"
        assert completed_job["completed_at"] is not None
        assert completed_job["duration_seconds"] is not None

    def test_job_history_stats(self, tmp_path):
        """Test job statistics generation."""
        from srunx.history import JobHistory

        db_path = tmp_path / "test_history.db"
        history = JobHistory(db_path=db_path)

        # Record multiple jobs
        for i in range(5):
            job = Job(
                name=f"stats_test_{i}",
                command=["python", "script.py"],
                resources=JobResource(nodes=1, gpus_per_node=1),
                environment=JobEnvironment(),
            )
            job.job_id = 20000 + i
            job.status = JobStatus.PENDING

            history.record_job(job)

        # Get stats
        stats = history.get_job_stats()

        assert stats["total_jobs"] >= 5
        assert isinstance(stats["jobs_by_status"], dict)

    def test_job_history_stats_with_date_filter(self, tmp_path):
        """Test job statistics with date range filtering."""

        from srunx.history import JobHistory

        db_path = tmp_path / "test_history_dates.db"
        history = JobHistory(db_path=db_path)

        # Record jobs with different dates via direct SQL
        import sqlite3

        with sqlite3.connect(db_path) as conn:
            conn.execute(
                "INSERT INTO jobs (job_id, job_name, status, submitted_at) VALUES (?, ?, ?, ?)",
                (1001, "old_job", "COMPLETED", "2025-01-15T10:00:00"),
            )
            conn.execute(
                "INSERT INTO jobs (job_id, job_name, status, submitted_at) VALUES (?, ?, ?, ?)",
                (1002, "recent_job", "COMPLETED", "2025-06-15T10:00:00"),
            )
            conn.execute(
                "INSERT INTO jobs (job_id, job_name, status, submitted_at) VALUES (?, ?, ?, ?)",
                (1003, "latest_job", "FAILED", "2025-12-01T10:00:00"),
            )
            conn.commit()

        # Filter by date range
        stats = history.get_job_stats(from_date="2025-06-01", to_date="2025-12-31")
        assert stats["total_jobs"] == 2  # recent_job and latest_job

        # Filter only from_date
        stats = history.get_job_stats(from_date="2025-12-01")
        assert stats["total_jobs"] == 1  # latest_job only

        # Filter only to_date (should include all before end of day)
        stats = history.get_job_stats(to_date="2025-01-15")
        assert stats["total_jobs"] == 1  # old_job only

    def test_job_history_workflow_stats(self, tmp_path):
        """Test workflow statistics with correct labels."""
        from srunx.history import JobHistory

        db_path = tmp_path / "test_workflow_stats.db"
        history = JobHistory(db_path=db_path)

        # Record jobs in a workflow
        import sqlite3

        with sqlite3.connect(db_path) as conn:
            conn.execute(
                "INSERT INTO jobs (job_id, job_name, status, submitted_at, workflow_name, duration_seconds) VALUES (?, ?, ?, ?, ?, ?)",
                (
                    2001,
                    "wf_step1",
                    "COMPLETED",
                    "2025-06-01T10:00:00",
                    "ml_pipeline",
                    120.0,
                ),
            )
            conn.execute(
                "INSERT INTO jobs (job_id, job_name, status, submitted_at, workflow_name, duration_seconds) VALUES (?, ?, ?, ?, ?, ?)",
                (
                    2002,
                    "wf_step2",
                    "COMPLETED",
                    "2025-06-01T11:00:00",
                    "ml_pipeline",
                    300.0,
                ),
            )
            conn.execute(
                "INSERT INTO jobs (job_id, job_name, status, submitted_at, workflow_name) VALUES (?, ?, ?, ?, ?)",
                (2003, "wf_step3", "RUNNING", "2025-06-01T12:00:00", "ml_pipeline"),
            )
            conn.commit()

        stats = history.get_workflow_stats("ml_pipeline")
        assert stats["total_jobs"] == 3
        assert (
            stats["avg_duration_seconds"] == 210.0
        )  # (120 + 300) / 2, excluding RUNNING
        assert stats["first_submitted"] == "2025-06-01T10:00:00"
        assert stats["last_submitted"] == "2025-06-01T12:00:00"

    def test_job_history_record_uses_private_status(self, tmp_path):
        """Test that record_job uses _status (private attr) not status property."""
        from srunx.history import JobHistory

        db_path = tmp_path / "test_status.db"
        history = JobHistory(db_path=db_path)

        job = Job(
            name="status_test",
            command=["python", "train.py"],
            resources=JobResource(nodes=1),
            environment=JobEnvironment(),
        )
        job.job_id = 30001
        job._status = JobStatus.PENDING

        history.record_job(job)

        recent_jobs = history.get_recent_jobs(limit=1)
        assert recent_jobs[0]["status"] == "PENDING"


class TestLastNOptimization:
    """Test that --last N reads files efficiently."""

    def test_tail_log_last_n_skips_content_read(self, tmp_path, monkeypatch):
        """Test that last_n mode skips full content read via skip_content."""
        log_file = tmp_path / "job_789.log"
        log_file.write_text("\n".join([f"Line {i}" for i in range(1, 51)]))

        skip_content_called_with = []

        def mock_get_job_output_detailed(
            self, job_id, job_name=None, skip_content=False
        ):
            skip_content_called_with.append(skip_content)
            return {
                "found_files": [str(log_file)],
                "primary_log": str(log_file),
                "output": "" if skip_content else log_file.read_text(),
                "error": "",
                "slurm_log_dir": str(tmp_path),
                "searched_dirs": [str(tmp_path)],
            }

        monkeypatch.setattr(
            Slurm, "get_job_output_detailed", mock_get_job_output_detailed
        )
        client = Slurm()

        with patch("rich.console.Console") as MockConsole:
            mock_console = MockConsole.return_value
            client.tail_log(job_id=789, follow=False, last_n=5)

        # Should have called with skip_content=True
        assert skip_content_called_with == [True]

    def test_tail_log_last_n_reads_correct_lines(self, tmp_path, monkeypatch):
        """Test that last_n reads exactly the last N lines."""
        lines = [f"Line {i}\n" for i in range(1, 101)]
        log_file = tmp_path / "job_100.log"
        log_file.write_text("".join(lines))

        def mock_get_job_output_detailed(
            self, job_id, job_name=None, skip_content=False
        ):
            return {
                "found_files": [str(log_file)],
                "primary_log": str(log_file),
                "output": "",
                "error": "",
                "slurm_log_dir": str(tmp_path),
                "searched_dirs": [str(tmp_path)],
            }

        monkeypatch.setattr(
            Slurm, "get_job_output_detailed", mock_get_job_output_detailed
        )
        client = Slurm()

        printed_texts = []
        with patch("rich.console.Console") as MockConsole:
            mock_console = MockConsole.return_value
            mock_console.print.side_effect = lambda *args, **kwargs: (
                printed_texts.append(args[0] if args else "")
            )
            client.tail_log(job_id=100, follow=False, last_n=3)

        # The second print call (after the file path) should contain last 3 lines
        output_text = printed_texts[1]  # index 0 is the file path line
        assert "Line 98" in output_text
        assert "Line 99" in output_text
        assert "Line 100" in output_text
        assert "Line 97" not in output_text

    def test_tail_log_static_no_last_n_reads_full_content(self, tmp_path, monkeypatch):
        """Test that static mode without last_n reads full content."""
        log_file = tmp_path / "job_200.log"
        log_file.write_text("Full content here\n")

        skip_content_called_with = []

        def mock_get_job_output_detailed(
            self, job_id, job_name=None, skip_content=False
        ):
            skip_content_called_with.append(skip_content)
            return {
                "found_files": [str(log_file)],
                "primary_log": str(log_file),
                "output": "" if skip_content else log_file.read_text(),
                "error": "",
                "slurm_log_dir": str(tmp_path),
                "searched_dirs": [str(tmp_path)],
            }

        monkeypatch.setattr(
            Slurm, "get_job_output_detailed", mock_get_job_output_detailed
        )
        client = Slurm()

        with patch("rich.console.Console") as MockConsole:
            mock_console = MockConsole.return_value
            client.tail_log(job_id=200, follow=False)

        # Should have called with skip_content=False
        assert skip_content_called_with == [False]


class TestJobMonitorHistoryIntegration:
    """Test that JobMonitor updates history on terminal states."""

    def test_notify_transition_updates_history_on_completion(self):
        """Test that _notify_transition updates history for completed jobs."""
        from unittest.mock import MagicMock

        from srunx.monitor.job_monitor import JobMonitor

        monitor = JobMonitor(job_ids=[123])
        monitor.callbacks = []

        job = Job(name="hist_test", job_id=123, command=["test"])
        job._status = JobStatus.COMPLETED

        with patch("srunx.history.get_history") as mock_get_history:
            mock_history = MagicMock()
            mock_get_history.return_value = mock_history

            monitor._notify_transition(job, JobStatus.COMPLETED)

            mock_history.update_job_completion.assert_called_once_with(
                123, JobStatus.COMPLETED
            )

    def test_notify_transition_updates_history_on_failure(self):
        """Test that _notify_transition updates history for failed jobs."""
        from unittest.mock import MagicMock

        from srunx.monitor.job_monitor import JobMonitor

        monitor = JobMonitor(job_ids=[456])
        monitor.callbacks = []

        job = Job(name="fail_test", job_id=456, command=["test"])
        job._status = JobStatus.FAILED

        with patch("srunx.history.get_history") as mock_get_history:
            mock_history = MagicMock()
            mock_get_history.return_value = mock_history

            monitor._notify_transition(job, JobStatus.FAILED)

            mock_history.update_job_completion.assert_called_once_with(
                456, JobStatus.FAILED
            )

    def test_notify_transition_skips_history_for_running(self):
        """Test that _notify_transition does NOT update history for non-terminal states."""
        from unittest.mock import MagicMock

        from srunx.monitor.job_monitor import JobMonitor

        monitor = JobMonitor(job_ids=[789])
        callback = MagicMock()
        monitor.callbacks = [callback]

        job = Job(name="run_test", job_id=789, command=["test"])
        job._status = JobStatus.RUNNING

        with patch("srunx.history.get_history") as mock_get_history:
            monitor._notify_transition(job, JobStatus.RUNNING)

            # History should NOT be called for RUNNING status
            mock_get_history.assert_not_called()

        # But callback should still be called
        callback.on_job_running.assert_called_once_with(job)

    def test_notify_transition_handles_history_error(self):
        """Test that history errors don't break callback notifications."""
        from unittest.mock import MagicMock

        from srunx.monitor.job_monitor import JobMonitor

        monitor = JobMonitor(job_ids=[101])
        callback = MagicMock()
        monitor.callbacks = [callback]

        job = Job(name="err_test", job_id=101, command=["test"])
        job._status = JobStatus.COMPLETED

        with patch("srunx.history.get_history") as mock_get_history:
            mock_get_history.side_effect = Exception("DB error")

            # Should not raise
            monitor._notify_transition(job, JobStatus.COMPLETED)

        # Callback should still be called despite history error
        callback.on_job_completed.assert_called_once_with(job)


@pytest.mark.skip(reason="SSH tests require actual SSH connection")
class TestSSHLogStreaming:
    """Test SSH log streaming functionality (integration tests)."""

    def test_ssh_tail_log_static(self):
        """Test SSH static log display."""

        # This would require actual SSH connection
        # Skipped for unit testing
        pass

    def test_ssh_tail_log_follow(self):
        """Test SSH real-time log streaming."""

        # This would require actual SSH connection
        # Skipped for unit testing
        pass
