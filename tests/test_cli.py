"""Tests for srunx.cli module."""

import re
from unittest.mock import Mock, patch

import pytest
from typer.testing import CliRunner

from srunx.cli.main import _parse_env_vars, app


def strip_ansi_codes(text: str) -> str:
    """Strip ANSI escape sequences from text."""
    ansi_escape = re.compile(r"\x1b\[[0-9;]*m")
    return ansi_escape.sub("", text)


class TestHelperFunctions:
    """Test helper functions."""

    def test_parse_env_vars(self):
        """Test parsing environment variables."""
        # Test empty input
        assert _parse_env_vars(None) == {}
        assert _parse_env_vars([]) == {}

        # Test single variable
        result = _parse_env_vars(["KEY=value"])
        assert result == {"KEY": "value"}

        # Test multiple variables
        result = _parse_env_vars(["KEY1=value1", "KEY2=value2"])
        assert result == {"KEY1": "value1", "KEY2": "value2"}

        # Test variable with equals in value
        result = _parse_env_vars(["PATH=/bin:/usr/bin"])
        assert result == {"PATH": "/bin:/usr/bin"}

        # Test invalid format
        with pytest.raises(ValueError, match="Invalid environment variable format"):
            _parse_env_vars(["INVALID_FORMAT"])


class TestTyperCLI:
    """Test Typer CLI commands."""

    def setup_method(self):
        """Setup test environment."""
        self.runner = CliRunner()

    def test_help_command(self):
        """Test main help command."""
        result = self.runner.invoke(app, ["--help"])
        assert result.exit_code == 0
        assert "Python library for SLURM job management" in result.stdout
        assert "submit" in result.stdout
        assert "status" in result.stdout
        assert "list" in result.stdout
        assert "cancel" in result.stdout
        assert "flow" in result.stdout
        assert "config" in result.stdout

    def test_submit_help(self):
        """Test submit command help."""
        result = self.runner.invoke(app, ["submit", "--help"])
        assert result.exit_code == 0
        clean_output = strip_ansi_codes(result.stdout)
        assert "Submit a SLURM job" in clean_output
        assert "--name" in clean_output
        assert "--nodes" in clean_output
        assert "--gpus-per-node" in clean_output

    def test_status_help(self):
        """Test status command help."""
        result = self.runner.invoke(app, ["status", "--help"])
        assert result.exit_code == 0
        assert "Check job status" in result.stdout
        assert "job_id" in result.stdout

    def test_list_help(self):
        """Test list command help."""
        result = self.runner.invoke(app, ["list", "--help"])
        assert result.exit_code == 0
        assert "List user's jobs in the queue" in result.stdout

    def test_cancel_help(self):
        """Test cancel command help."""
        result = self.runner.invoke(app, ["cancel", "--help"])
        assert result.exit_code == 0
        assert "Cancel a running job" in result.stdout
        assert "job_id" in result.stdout

    def test_flow_help(self):
        """Test flow command help."""
        result = self.runner.invoke(app, ["flow", "--help"])
        assert result.exit_code == 0
        assert "Workflow management" in result.stdout
        assert "run" in result.stdout
        assert "validate" in result.stdout

    def test_flow_run_help(self):
        """Test flow run command help includes debug option."""
        result = self.runner.invoke(app, ["flow", "run", "--help"])
        assert result.exit_code == 0
        clean_output = strip_ansi_codes(result.stdout)
        assert "Execute workflow from YAML file" in clean_output
        assert "--debug" in clean_output
        assert "Show rendered SLURM scripts for each job" in clean_output

    def test_config_help(self):
        """Test config command help."""
        result = self.runner.invoke(app, ["config", "--help"])
        assert result.exit_code == 0
        assert "Configuration management" in result.stdout
        assert "show" in result.stdout
        assert "paths" in result.stdout
        assert "init" in result.stdout

    @patch("srunx.cli.main.Slurm")
    @patch("srunx.cli.main.get_config")
    def test_submit_command_basic(self, mock_get_config, mock_slurm_class):
        """Test basic submit command."""
        # Mock config
        mock_config = Mock()
        mock_config.log_dir = "logs"
        mock_config.work_dir = None
        mock_get_config.return_value = mock_config

        # Mock Slurm client
        mock_slurm = Mock()
        mock_job = Mock()
        mock_job.job_id = 12345
        mock_job.name = "test_job"
        mock_job.command = ["python", "script.py"]
        mock_slurm.submit.return_value = mock_job
        mock_slurm_class.return_value = mock_slurm

        result = self.runner.invoke(
            app, ["submit", "python", "script.py", "--name", "test_job"]
        )

        assert result.exit_code == 0
        assert "Job submitted successfully: 12345" in result.stdout
        mock_slurm.submit.assert_called_once()

    @patch("srunx.cli.main.Slurm")
    def test_status_command(self, mock_slurm_class):
        """Test status command."""
        # Mock Slurm client
        mock_slurm = Mock()
        mock_job = Mock()
        mock_job.job_id = 12345
        mock_job.name = "test_job"
        mock_job.command = ["python", "script.py"]
        mock_job.status = Mock()
        mock_job.status.name = "RUNNING"
        mock_slurm.retrieve.return_value = mock_job
        mock_slurm_class.return_value = mock_slurm

        result = self.runner.invoke(app, ["status", "12345"])

        assert result.exit_code == 0
        assert "Job ID: 12345" in result.stdout
        assert "Status: RUNNING" in result.stdout
        mock_slurm.retrieve.assert_called_once_with(12345)

    @patch("srunx.cli.main.Slurm")
    def test_cancel_command(self, mock_slurm_class):
        """Test cancel command."""
        # Mock Slurm client
        mock_slurm = Mock()
        mock_slurm.cancel.return_value = True
        mock_slurm_class.return_value = mock_slurm

        result = self.runner.invoke(app, ["cancel", "12345"])

        assert result.exit_code == 0
        assert "Job 12345 cancelled successfully" in result.stdout
        mock_slurm.cancel.assert_called_once_with(12345)

    @patch("srunx.cli.main.Slurm")
    def test_list_command_empty(self, mock_slurm_class):
        """Test list command with empty queue."""
        # Mock Slurm client
        mock_slurm = Mock()
        mock_slurm.queue.return_value = []
        mock_slurm_class.return_value = mock_slurm

        result = self.runner.invoke(app, ["list"])

        assert result.exit_code == 0
        assert "No jobs in queue" in result.stdout
        mock_slurm.queue.assert_called_once()

    @patch("srunx.cli.main.get_config")
    def test_config_show_command(self, mock_get_config):
        """Test config show command."""
        # Mock config
        mock_config = Mock()
        mock_config.log_dir = "logs"
        mock_config.work_dir = "/tmp"
        mock_config.resources = Mock()
        mock_config.resources.nodes = 1
        mock_config.resources.gpus_per_node = 0
        mock_config.resources.ntasks_per_node = 1
        mock_config.resources.cpus_per_task = 1
        mock_config.resources.memory_per_node = None
        mock_config.resources.time_limit = None
        mock_config.resources.partition = None
        mock_config.environment = Mock()
        mock_config.environment.conda = None
        mock_config.environment.venv = None
        mock_config.environment.container = None
        mock_get_config.return_value = mock_config

        result = self.runner.invoke(app, ["config", "show"])

        assert result.exit_code == 0
        assert "srunx Configuration" in result.stdout
        mock_get_config.assert_called_once()

    @patch("srunx.cli.main.get_config_paths")
    def test_config_paths_command(self, mock_get_config_paths):
        """Test config paths command."""
        from pathlib import Path

        # Mock paths
        mock_paths = [Path("/home/user/.config/srunx/config.toml")]
        mock_get_config_paths.return_value = mock_paths

        result = self.runner.invoke(app, ["config", "paths"])

        assert result.exit_code == 0
        assert "Configuration file paths" in result.stdout
        mock_get_config_paths.assert_called_once()

    def test_submit_missing_command(self):
        """Test submit command without required command argument."""
        result = self.runner.invoke(app, ["submit"])
        assert result.exit_code == 2  # Typer error exit code
        assert "Missing argument" in result.stderr

    def test_status_missing_job_id(self):
        """Test status command without required job ID."""
        result = self.runner.invoke(app, ["status"])
        assert result.exit_code == 2  # Typer error exit code
        assert "Missing argument" in result.stderr

    def test_cancel_missing_job_id(self):
        """Test cancel command without required job ID."""
        result = self.runner.invoke(app, ["cancel"])
        assert result.exit_code == 2  # Typer error exit code
        assert "Missing argument" in result.stderr
