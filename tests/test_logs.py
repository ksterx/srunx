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
        def mock_get_job_output_detailed(self, job_id, job_name=None):
            return {
                "found_files": [str(log_file)],
                "primary_log": str(log_file),
                "output": log_file.read_text(),
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
        def mock_get_job_output_detailed(self, job_id, job_name=None):
            return {
                "found_files": [str(log_file)],
                "primary_log": str(log_file),
                "output": log_file.read_text(),
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
        def mock_get_job_output_detailed(self, job_id, job_name=None):
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

        assert len(templates) > 0
        assert any(t["name"] == "pytorch-ddp" for t in templates)
        assert any(t["name"] == "tensorflow-multiworker" for t in templates)
        assert any(t["name"] == "horovod" for t in templates)

    def test_get_template_path(self):
        """Test getting template path."""
        from srunx.template import get_template_path

        # Test valid template
        path = get_template_path("pytorch-ddp")
        assert path.endswith("pytorch_ddp.slurm.jinja")

        # Test invalid template
        with pytest.raises(ValueError):
            get_template_path("nonexistent-template")

    def test_get_template_info(self):
        """Test getting template information."""
        from srunx.template import get_template_info

        info = get_template_info("pytorch-ddp")

        assert info["name"] == "pytorch-ddp"
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
