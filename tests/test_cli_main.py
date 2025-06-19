"""Tests for srunx.cli.main module."""

import argparse
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
    main,
)
from srunx.models import Job, JobEnvironment, JobStatus


class TestParserCreation:
    """Test argument parser creation functions."""

    def test_create_job_parser(self) -> None:
        """Test create_job_parser function."""
        parser = create_job_parser()

        assert isinstance(parser, argparse.ArgumentParser)

        # Test parsing with required arguments
        args = parser.parse_args(["python", "script.py"])
        assert args.command == ["python", "script.py"]
        assert args.name == "job"
        assert args.nodes == 1

    def test_create_job_parser_with_options(self) -> None:
        """Test create_job_parser with various options."""
        parser = create_job_parser()

        args = parser.parse_args(
            [
                "python",
                "script.py",
                "--name",
                "test_job",
                "--nodes",
                "2",
                "--gpus-per-node",
                "1",
                "--conda",
                "ml_env",
                "--memory",
                "32GB",
                "--time",
                "2:00:00",
                "--wait",
            ]
        )

        assert args.command == ["python", "script.py"]
        assert args.name == "test_job"
        assert args.nodes == 2
        assert args.gpus_per_node == 1
        assert args.conda == "ml_env"
        assert args.memory == "32GB"
        assert args.time == "2:00:00"
        assert args.wait is True

    def test_create_status_parser(self) -> None:
        """Test create_status_parser function."""
        parser = create_status_parser()

        args = parser.parse_args(["12345"])
        assert args.job_id == 12345

    def test_create_queue_parser(self) -> None:
        """Test create_queue_parser function."""
        parser = create_queue_parser()

        # Test without user
        args = parser.parse_args([])
        assert not hasattr(args, "user") or args.user is None

        # Test with user
        args = parser.parse_args(["--user", "testuser"])
        assert args.user == "testuser"

    def test_create_cancel_parser(self) -> None:
        """Test create_cancel_parser function."""
        parser = create_cancel_parser()

        args = parser.parse_args(["54321"])
        assert args.job_id == 54321

    def test_create_main_parser(self) -> None:
        """Test create_main_parser function."""
        parser = create_main_parser()

        # Test submit subcommand
        args = parser.parse_args(["submit", "python", "script.py"])
        assert hasattr(args, "command") and args.command == ["python", "script.py"]
        assert hasattr(args, "func")

        # Test status subcommand - need to get the subcommand name from dest
        args = parser.parse_args(["status", "12345"])
        # The subparser stores the command name in the dest attribute
        subparsers_action = [
            action for action in parser._actions if action.dest == "command"
        ][0]
        assert hasattr(args, subparsers_action.dest)
        assert args.job_id == 12345

        # Test queue subcommand
        args = parser.parse_args(["queue"])
        # Just verify it parses without error

        # Test cancel subcommand
        args = parser.parse_args(["cancel", "12345"])
        assert args.job_id == 12345


class TestUtilityFunctions:
    """Test utility functions."""

    def test_parse_env_vars_empty(self) -> None:
        """Test _parse_env_vars with empty input."""
        result = _parse_env_vars(None)
        assert result == {}

        result = _parse_env_vars([])
        assert result == {}

    def test_parse_env_vars_valid(self) -> None:
        """Test _parse_env_vars with valid input."""
        env_vars = ["KEY1=value1", "KEY2=value2", "PATH=/custom/path"]
        result = _parse_env_vars(env_vars)

        expected = {
            "KEY1": "value1",
            "KEY2": "value2",
            "PATH": "/custom/path",
        }
        assert result == expected

    def test_parse_env_vars_with_equals_in_value(self) -> None:
        """Test _parse_env_vars with equals sign in value."""
        env_vars = ["CONFIG=key=value", "URL=http://example.com"]
        result = _parse_env_vars(env_vars)

        expected = {
            "CONFIG": "key=value",
            "URL": "http://example.com",
        }
        assert result == expected

    @patch("srunx.cli.main.logger")
    def test_parse_env_vars_invalid_format(self, mock_logger: Mock) -> None:
        """Test _parse_env_vars with invalid format."""
        env_vars = ["VALID=value", "INVALID_NO_EQUALS", "ANOTHER=valid"]
        result = _parse_env_vars(env_vars)

        expected = {
            "VALID": "value",
            "ANOTHER": "valid",
        }
        assert result == expected
        mock_logger.warning.assert_called_once()


class TestCommandFunctions:
    """Test command handling functions."""

    @patch("srunx.cli.main.Slurm")
    def test_cmd_submit_basic(self, mock_client_class: Mock) -> None:
        """Test cmd_submit with basic arguments."""
        # Setup
        mock_client = Mock()
        submitted_job = Job(
            name="test_job",
            command=["python", "script.py"],
            environment=JobEnvironment(conda="test_env"),
            job_id=12345,
            status=JobStatus.PENDING,
        )
        mock_client.run.return_value = submitted_job
        mock_client_class.return_value = mock_client

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
            log_dir="/var/log/slurm",
            work_dir=None,
            template=None,
            wait=False,
            slack=False,
            verbose=False,
        )

        # Execute
        cmd_submit(args)

        # Verify
        mock_client.run.assert_called_once()
        job_arg = mock_client.run.call_args[0][0]
        assert job_arg.name == "test_job"
        assert job_arg.command == ["python", "script.py"]
        assert job_arg.environment.conda == "test_env"

    @patch("srunx.cli.main.Slurm")
    def test_cmd_submit_with_wait(self, mock_client_class: Mock) -> None:
        """Test cmd_submit with wait option."""
        # Setup
        mock_client = Mock()
        submitted_job = Job(
            name="wait_job",
            command=["python", "script.py"],
            environment=JobEnvironment(conda="test_env"),
            job_id=12345,
            status=JobStatus.PENDING,
        )
        completed_job = Job(
            name="wait_job",
            command=["python", "script.py"],
            environment=JobEnvironment(conda="test_env"),
            job_id=12345,
            status=JobStatus.COMPLETED,
        )
        mock_client.run.return_value = submitted_job
        mock_client.monitor.return_value = completed_job
        mock_client_class.return_value = mock_client

        args = argparse.Namespace(
            command=["python", "script.py"],
            name="wait_job",
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
            log_dir="/var/log/slurm",
            work_dir=None,
            template=None,
            wait=True,
            poll_interval=30,
            slack=False,
            verbose=False,
        )

        # Execute
        cmd_submit(args)

        # Verify
        mock_client.run.assert_called_once()
        mock_client.monitor.assert_called_once_with(submitted_job, poll_interval=30)

    @patch("srunx.cli.main.Slurm")
    @patch("srunx.cli.main.sys.exit")
    def test_cmd_submit_error(self, mock_exit: Mock, mock_client_class: Mock) -> None:
        """Test cmd_submit with submission error."""
        # Setup
        mock_client = Mock()
        mock_client.run.side_effect = Exception("Submission failed")
        mock_client_class.return_value = mock_client

        args = argparse.Namespace(
            command=["python", "script.py"],
            name="error_job",
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
            log_dir="/var/log/slurm",
            work_dir=None,
            template=None,
            wait=False,
        )

        # Execute
        cmd_submit(args)

        # Verify
        mock_exit.assert_called_once_with(1)

    @patch("srunx.cli.main.Slurm")
    def test_cmd_status_success(self, mock_client_class: Mock) -> None:
        """Test cmd_status with successful status query."""
        # Setup
        mock_client = Mock()
        job = Job(
            name="status_job",
            command=["unknown"],
            environment=JobEnvironment(conda="test_env"),
            job_id=12345,
            status=JobStatus.RUNNING,
        )
        mock_client.retrieve.return_value = job
        mock_client_class.return_value = mock_client

        args = argparse.Namespace(job_id=12345)

        # Execute
        cmd_status(args)

        # Verify
        mock_client.retrieve.assert_called_once_with(12345)

    @patch("srunx.cli.main.Slurm")
    @patch("srunx.cli.main.sys.exit")
    def test_cmd_status_error(self, mock_exit: Mock, mock_client_class: Mock) -> None:
        """Test cmd_status with error."""
        # Setup
        mock_client = Mock()
        mock_client.retrieve.side_effect = Exception("Status query failed")
        mock_client_class.return_value = mock_client

        args = argparse.Namespace(job_id=99999)

        # Execute
        cmd_status(args)

        # Verify
        mock_exit.assert_called_once_with(1)

    @patch("srunx.cli.main.Slurm")
    def test_cmd_queue_success(self, mock_client_class: Mock) -> None:
        """Test cmd_queue with successful job listing."""
        # Setup
        mock_client = Mock()
        jobs = [
            Job(
                name="job1",
                command=["unknown"],
                environment=JobEnvironment(conda="env1"),
                job_id=12345,
                status=JobStatus.RUNNING,
            ),
            Job(
                name="job2",
                command=["unknown"],
                environment=JobEnvironment(conda="env2"),
                job_id=54321,
                status=JobStatus.PENDING,
            ),
        ]
        mock_client.queue.return_value = jobs
        mock_client_class.return_value = mock_client

        args = argparse.Namespace(user=None)

        # Execute
        cmd_queue(args)

        # Verify
        mock_client.queue.assert_called_once_with(None)

    @patch("srunx.cli.main.Slurm")
    def test_cmd_queue_with_user(self, mock_client_class: Mock) -> None:
        """Test cmd_queue with specific user."""
        # Setup
        mock_client = Mock()
        mock_client.queue.return_value = []
        mock_client_class.return_value = mock_client

        args = argparse.Namespace(user="testuser")

        # Execute
        cmd_queue(args)

        # Verify
        mock_client.queue.assert_called_once_with("testuser")

    @patch("srunx.cli.main.Slurm")
    def test_cmd_queue_empty(self, mock_client_class: Mock) -> None:
        """Test cmd_queue with no jobs."""
        # Setup
        mock_client = Mock()
        mock_client.queue.return_value = []
        mock_client_class.return_value = mock_client

        args = argparse.Namespace(user=None)

        # Execute
        cmd_queue(args)

        # Verify
        mock_client.queue.assert_called_once_with(None)

    @patch("srunx.cli.main.Slurm")
    @patch("srunx.cli.main.sys.exit")
    def test_cmd_queue_error(self, mock_exit: Mock, mock_client_class: Mock) -> None:
        """Test cmd_queue with error."""
        # Setup
        mock_client = Mock()
        mock_client.queue.side_effect = Exception("Queue failed")
        mock_client_class.return_value = mock_client

        args = argparse.Namespace(user=None)

        # Execute
        cmd_queue(args)

        # Verify
        mock_exit.assert_called_once_with(1)

    @patch("srunx.cli.main.Slurm")
    def test_cmd_cancel_success(self, mock_client_class: Mock) -> None:
        """Test cmd_cancel with successful cancellation."""
        # Setup
        mock_client = Mock()
        mock_client_class.return_value = mock_client

        args = argparse.Namespace(job_id=12345)

        # Execute
        cmd_cancel(args)

        # Verify
        mock_client.cancel.assert_called_once_with(12345)

    @patch("srunx.cli.main.Slurm")
    @patch("srunx.cli.main.sys.exit")
    def test_cmd_cancel_error(self, mock_exit: Mock, mock_client_class: Mock) -> None:
        """Test cmd_cancel with error."""
        # Setup
        mock_client = Mock()
        mock_client.cancel.side_effect = Exception("Cancel failed")
        mock_client_class.return_value = mock_client

        args = argparse.Namespace(job_id=12345)

        # Execute
        cmd_cancel(args)

        # Verify
        mock_exit.assert_called_once_with(1)


class TestMainFunction:
    """Test main function."""

    @patch("srunx.cli.main.configure_cli_logging")
    @patch("srunx.cli.main.create_main_parser")
    def test_main_with_subcommand(
        self, mock_create_parser: Mock, mock_configure: Mock
    ) -> None:
        """Test main function with subcommand."""
        # Setup
        mock_parser = Mock()
        mock_args = Mock()
        mock_args.func = Mock()
        mock_args.log_level = "INFO"
        mock_args.quiet = False
        mock_parser.parse_args.return_value = mock_args
        mock_create_parser.return_value = mock_parser

        # Execute
        with patch("sys.argv", ["srunx", "submit", "echo", "test"]):
            main()

        # Verify
        mock_configure.assert_called_once_with(level="INFO", quiet=False)
        mock_args.func.assert_called_once_with(mock_args)

    @patch("srunx.cli.main.configure_cli_logging")
    @patch("srunx.cli.main.create_main_parser")
    @patch("srunx.cli.main.create_job_parser")
    @patch("srunx.cli.main.cmd_submit")
    @patch("srunx.cli.main.sys.exit")
    def test_main_backward_compatibility(
        self,
        mock_exit: Mock,
        mock_cmd_submit: Mock,
        mock_create_job_parser: Mock,
        mock_create_parser: Mock,
        mock_configure: Mock,
    ) -> None:
        """Test main function backward compatibility."""
        # Setup
        mock_main_parser = Mock()
        mock_args = Mock()
        del mock_args.func  # No func attribute means no subcommand
        mock_main_parser.parse_args.return_value = mock_args
        mock_create_parser.return_value = mock_main_parser

        mock_job_parser = Mock()
        mock_job_args = Mock()
        mock_job_parser.parse_args.return_value = mock_job_args
        mock_create_job_parser.return_value = mock_job_parser

        # Execute
        with patch("sys.argv", ["srunx", "echo", "test"]):
            main()

        # Verify
        mock_cmd_submit.assert_called_once_with(mock_job_args)

    @patch("srunx.cli.main.configure_cli_logging")
    @patch("srunx.cli.main.create_main_parser")
    @patch("srunx.cli.main.create_job_parser")
    @patch("srunx.cli.main.sys.exit")
    def test_main_backward_compatibility_error(
        self,
        mock_exit: Mock,
        mock_create_job_parser: Mock,
        mock_create_parser: Mock,
        mock_configure: Mock,
    ) -> None:
        """Test main function backward compatibility with parsing error."""
        # Setup
        mock_main_parser = Mock()
        mock_args = Mock()
        del mock_args.func  # No func attribute
        mock_main_parser.parse_args.return_value = mock_args
        mock_create_parser.return_value = mock_main_parser

        mock_job_parser = Mock()
        mock_job_parser.parse_args.side_effect = SystemExit(2)  # Parsing error
        mock_create_job_parser.return_value = mock_job_parser

        # Execute
        with patch("sys.argv", ["srunx"]):
            main()

        # Verify
        mock_exit.assert_called_once_with(1)
        mock_main_parser.print_help.assert_called_once()


class TestIntegration:
    """Test integration scenarios."""

    def test_submit_command_with_complex_args(self) -> None:
        """Test submit command with complex argument combinations."""
        parser = create_main_parser()

        # The command arguments need to be quoted properly since they contain arguments
        args = parser.parse_args(
            [
                "submit",
                "python",
                "train.py",  # Don't include --epochs 100 as that's part of the python command
                "--name",
                "ml_training",
                "--nodes",
                "2",
                "--gpus-per-node",
                "2",
                "--memory",
                "64GB",
                "--time",
                "12:00:00",
                "--conda",
                "ml_env",
                "--env",
                "CUDA_VISIBLE_DEVICES=0,1",
                "--env",
                "OMP_NUM_THREADS=8",
                "--wait",
                "--poll-interval",
                "60",
            ]
        )

        # The 'command' attribute in submit context refers to the command to run
        assert hasattr(args, "command") and args.command == ["python", "train.py"]
        assert args.name == "ml_training"
        assert args.nodes == 2
        assert args.gpus_per_node == 2
        assert args.memory == "64GB"
        assert args.time == "12:00:00"
        assert args.conda == "ml_env"
        assert args.env_vars == ["CUDA_VISIBLE_DEVICES=0,1", "OMP_NUM_THREADS=8"]
        assert args.wait is True
        assert args.poll_interval == 60

    def test_all_subcommands_parsing(self) -> None:
        """Test that all subcommands parse correctly."""
        parser = create_main_parser()

        # Submit - the command field contains the actual command to run
        args = parser.parse_args(["submit", "echo", "test"])
        assert args.command == ["echo", "test"]

        # Status
        args = parser.parse_args(["status", "12345"])
        assert args.job_id == 12345

        # Queue
        args = parser.parse_args(["queue", "--user", "testuser"])
        assert args.user == "testuser"

        # Cancel
        args = parser.parse_args(["cancel", "54321"])
        assert args.job_id == 54321
