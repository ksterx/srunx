"""Tests for srunx.cli module."""

import argparse
import os
from unittest.mock import Mock, patch

from srunx.cli.main import (
    _parse_env_vars,
    cmd_cancel,
    cmd_queue,
    cmd_status,
    cmd_submit,
    create_cancel_parser,
    create_job_parser,
    create_main_parser,
    create_queue_parser,
    create_status_parser,
)
from srunx.models import JobStatus


class TestParsers:
    """Test argument parsers."""

    def test_create_job_parser(self):
        """Test job parser creation."""
        parser = create_job_parser()

        # Test required command argument
        args = parser.parse_args(["python", "script.py"])
        assert args.command == ["python", "script.py"]

        # Test default values
        assert args.name == "job"
        assert args.nodes == 1
        assert args.gpus_per_node == 0

    def test_create_job_parser_with_options(self):
        """Test job parser with various options."""
        parser = create_job_parser()

        args = parser.parse_args(
            [
                "python",
                "train.py",
                "--name",
                "ml_training",
                "--nodes",
                "2",
                "--gpus-per-node",
                "1",
                "--memory",
                "32GB",
                "--time",
                "2:00:00",
                "--conda",
                "ml_env",
            ]
        )

        assert args.command == ["python", "train.py"]
        assert args.name == "ml_training"
        assert args.nodes == 2
        assert args.gpus_per_node == 1
        assert args.memory == "32GB"
        assert args.time == "2:00:00"
        assert args.conda == "ml_env"

    def test_create_status_parser(self):
        """Test status parser creation."""
        parser = create_status_parser()

        args = parser.parse_args(["12345"])
        assert args.job_id == 12345

    def test_create_queue_parser(self):
        """Test queue parser creation."""
        parser = create_queue_parser()

        # Test without user
        args = parser.parse_args([])
        assert args.user is None

        # Test with user
        args = parser.parse_args(["--user", "testuser"])
        assert args.user == "testuser"

    def test_create_cancel_parser(self):
        """Test cancel parser creation."""
        parser = create_cancel_parser()

        args = parser.parse_args(["12345"])
        assert args.job_id == 12345

    def test_create_main_parser(self):
        """Test main parser creation."""
        parser = create_main_parser()

        # Test submit subcommand
        args = parser.parse_args(["submit", "python", "script.py"])
        # The subcommand name is stored in 'command'
        assert hasattr(args, "command")
        # The function is set by the subparser
        assert hasattr(args, "func")
        # The actual command line arguments are stored directly in args
        # after being processed by the copied job parser

        # Test status subcommand
        args = parser.parse_args(["status", "12345"])
        assert hasattr(args, "command")
        assert args.job_id == 12345

        # Test queue subcommand
        args = parser.parse_args(["queue"])
        assert hasattr(args, "command")

        # Test cancel subcommand
        args = parser.parse_args(["cancel", "12345"])
        assert hasattr(args, "command")
        assert args.job_id == 12345


class TestEnvVarsParser:
    """Test environment variables parser."""

    def test_parse_env_vars_empty(self):
        """Test parsing empty env vars list."""
        result = _parse_env_vars(None)
        assert result == {}

        result = _parse_env_vars([])
        assert result == {}

    def test_parse_env_vars_valid(self):
        """Test parsing valid env vars."""
        env_vars = ["KEY1=value1", "KEY2=value2", "PATH=/usr/bin:/bin"]
        result = _parse_env_vars(env_vars)

        assert result == {"KEY1": "value1", "KEY2": "value2", "PATH": "/usr/bin:/bin"}

    def test_parse_env_vars_with_equals_in_value(self):
        """Test parsing env vars with equals sign in value."""
        env_vars = ["DATABASE_URL=postgresql://user:pass=123@host:5432/db"]
        result = _parse_env_vars(env_vars)

        assert result == {"DATABASE_URL": "postgresql://user:pass=123@host:5432/db"}

    @patch("srunx.cli.main.logger")
    def test_parse_env_vars_invalid_format(self, mock_logger):
        """Test parsing invalid env vars format."""
        env_vars = ["INVALID_FORMAT", "VALID=value"]
        result = _parse_env_vars(env_vars)

        assert result == {"VALID": "value"}
        mock_logger.warning.assert_called_once()


class TestCommandHandlers:
    """Test CLI command handlers."""

    @patch("srunx.cli.main.Slurm")
    def test_cmd_submit_basic(self, mock_slurm_class):
        """Test basic job submission command."""
        mock_slurm = Mock()
        mock_slurm_class.return_value = mock_slurm

        # Mock submitted job
        mock_job = Mock()
        mock_job.job_id = 12345
        mock_job.name = "test_job"
        mock_slurm.submit.return_value = mock_job

        # Create args
        args = argparse.Namespace(
            command=["python", "script.py"],
            name="test_job",
            nodes=1,
            gpus_per_node=0,
            ntasks_per_node=1,
            cpus_per_task=1,
            memory=None,
            time=None,
            conda="test_env",
            venv=None,
            sqsh=None,
            env_vars=None,
            log_dir="logs",
            work_dir=None,
            slack=False,
            verbose=False,
            wait=False,
        )

        # Should not raise exception
        cmd_submit(args)

        mock_slurm.submit.assert_called_once()

    @patch("srunx.cli.main.Slurm")
    @patch.dict(os.environ, {"SLACK_WEBHOOK_URL": "https://hooks.slack.com/test"})
    def test_cmd_submit_with_slack(self, mock_slurm_class):
        """Test job submission with Slack notifications."""
        mock_slurm = Mock()
        mock_slurm_class.return_value = mock_slurm

        mock_job = Mock()
        mock_job.job_id = 12345
        mock_job.name = "test_job"
        mock_slurm.submit.return_value = mock_job

        args = argparse.Namespace(
            command=["python", "script.py"],
            name="test_job",
            nodes=1,
            gpus_per_node=0,
            ntasks_per_node=1,
            cpus_per_task=1,
            memory=None,
            time=None,
            conda="test_env",
            venv=None,
            sqsh=None,
            env_vars=None,
            log_dir="logs",
            work_dir=None,
            slack=True,
            verbose=False,
            wait=False,
        )

        cmd_submit(args)

        # Check that Slurm was initialized with callbacks
        mock_slurm_class.assert_called_once()
        call_kwargs = mock_slurm_class.call_args.kwargs
        assert "callbacks" in call_kwargs
        assert len(call_kwargs["callbacks"]) == 1

    @patch("srunx.cli.main.Slurm")
    def test_cmd_submit_with_wait(self, mock_slurm_class):
        """Test job submission with wait option."""
        mock_slurm = Mock()
        mock_slurm_class.return_value = mock_slurm

        mock_submitted_job = Mock()
        mock_submitted_job.job_id = 12345
        mock_submitted_job.name = "test_job"
        mock_slurm.submit.return_value = mock_submitted_job

        mock_completed_job = Mock()
        mock_completed_job.status = JobStatus.COMPLETED
        mock_slurm.monitor.return_value = mock_completed_job

        args = argparse.Namespace(
            command=["python", "script.py"],
            name="test_job",
            nodes=1,
            gpus_per_node=0,
            ntasks_per_node=1,
            cpus_per_task=1,
            memory=None,
            time=None,
            conda="test_env",
            venv=None,
            sqsh=None,
            env_vars=None,
            log_dir="logs",
            work_dir=None,
            slack=False,
            verbose=False,
            wait=True,
            poll_interval=5,
        )

        cmd_submit(args)

        mock_slurm.submit.assert_called_once()
        mock_slurm.monitor.assert_called_once()

    @patch("srunx.cli.main.Slurm")
    @patch("srunx.cli.main.sys.exit")
    def test_cmd_submit_failure(self, mock_exit, mock_slurm_class):
        """Test job submission failure handling."""
        mock_slurm = Mock()
        mock_slurm_class.return_value = mock_slurm
        mock_slurm.submit.side_effect = Exception("Submission failed")

        args = argparse.Namespace(
            command=["python", "script.py"],
            name="test_job",
            nodes=1,
            gpus_per_node=0,
            ntasks_per_node=1,
            cpus_per_task=1,
            memory=None,
            time=None,
            conda="test_env",
            venv=None,
            sqsh=None,
            env_vars=None,
            log_dir="logs",
            work_dir=None,
            slack=False,
            verbose=False,
            wait=False,
        )

        cmd_submit(args)

        mock_exit.assert_called_once_with(1)

    @patch("srunx.cli.main.Slurm")
    def test_cmd_status(self, mock_slurm_class):
        """Test job status command."""
        mock_slurm = Mock()
        mock_slurm_class.return_value = mock_slurm

        mock_job = Mock()
        mock_job.job_id = 12345
        mock_job.name = "test_job"
        mock_job.status = JobStatus.RUNNING
        mock_slurm.retrieve.return_value = mock_job

        args = argparse.Namespace(job_id=12345)

        # Should not raise exception
        cmd_status(args)

        mock_slurm.retrieve.assert_called_once_with(12345)

    @patch("srunx.cli.main.Slurm")
    @patch("srunx.cli.main.sys.exit")
    def test_cmd_status_failure(self, mock_exit, mock_slurm_class):
        """Test job status command failure."""
        mock_slurm = Mock()
        mock_slurm_class.return_value = mock_slurm
        mock_slurm.retrieve.side_effect = Exception("Job not found")

        args = argparse.Namespace(job_id=12345)

        cmd_status(args)

        mock_exit.assert_called_once_with(1)

    @patch("srunx.cli.main.Slurm")
    def test_cmd_queue_empty(self, mock_slurm_class):
        """Test queue command with empty queue."""
        mock_slurm = Mock()
        mock_slurm_class.return_value = mock_slurm
        mock_slurm.queue.return_value = []

        args = argparse.Namespace(user=None)

        cmd_queue(args)

        mock_slurm.queue.assert_called_once_with(None)

    @patch("srunx.cli.main.Slurm")
    def test_cmd_queue_with_jobs(self, mock_slurm_class):
        """Test queue command with jobs."""
        mock_slurm = Mock()
        mock_slurm_class.return_value = mock_slurm

        mock_job1 = Mock()
        mock_job1.job_id = 12345
        mock_job1.name = "job1"
        mock_job1.status = JobStatus.RUNNING

        mock_job2 = Mock()
        mock_job2.job_id = 12346
        mock_job2.name = "job2"
        mock_job2.status = JobStatus.PENDING

        mock_slurm.queue.return_value = [mock_job1, mock_job2]

        args = argparse.Namespace(user="testuser")

        cmd_queue(args)

        mock_slurm.queue.assert_called_once_with("testuser")

    @patch("srunx.cli.main.Slurm")
    def test_cmd_cancel(self, mock_slurm_class):
        """Test job cancellation command."""
        mock_slurm = Mock()
        mock_slurm_class.return_value = mock_slurm

        args = argparse.Namespace(job_id=12345)

        cmd_cancel(args)

        mock_slurm.cancel.assert_called_once_with(12345)

    @patch("srunx.cli.main.Slurm")
    @patch("srunx.cli.main.sys.exit")
    def test_cmd_cancel_failure(self, mock_exit, mock_slurm_class):
        """Test job cancellation failure."""
        mock_slurm = Mock()
        mock_slurm_class.return_value = mock_slurm
        mock_slurm.cancel.side_effect = Exception("Cancel failed")

        args = argparse.Namespace(job_id=12345)

        cmd_cancel(args)

        mock_exit.assert_called_once_with(1)
