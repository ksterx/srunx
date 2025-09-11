"""Tests for srunx workflow CLI execution control features."""

import tempfile
from pathlib import Path
from unittest.mock import Mock, patch

import yaml  # type: ignore
from typer.testing import CliRunner

from srunx.cli.main import app as main_app


class TestWorkflowExecutionControl:
    """Test workflow execution control CLI features."""

    def setup_method(self):
        """Setup test environment."""
        self.runner = CliRunner()

    def create_test_workflow(self, temp_dir: Path) -> Path:
        """Create a test workflow YAML file."""
        yaml_content = {
            "name": "test_workflow",
            "jobs": [
                {
                    "name": "job1",
                    "command": ["echo", "1"],
                    "environment": {"conda": "env"},
                },
                {
                    "name": "job2",
                    "command": ["echo", "2"],
                    "environment": {"conda": "env"},
                    "depends_on": ["job1"],
                },
                {
                    "name": "job3",
                    "command": ["echo", "3"],
                    "environment": {"conda": "env"},
                    "depends_on": ["job2"],
                },
            ],
        }

        yaml_path = temp_dir / "test_workflow.yaml"
        with open(yaml_path, "w") as f:
            yaml.dump(yaml_content, f)
        return yaml_path

    def test_workflow_run_help_includes_new_options(self):
        """Test that workflow run help includes new execution control options."""
        result = self.runner.invoke(main_app, ["flow", "run", "--help"])
        assert result.exit_code == 0

        # Check for new options in help
        assert "--from" in result.stdout
        assert "--to" in result.stdout
        assert "--job" in result.stdout
        assert "Start execution from this job" in result.stdout
        assert "Stop execution at this job" in result.stdout
        assert "Execute only this specific job" in result.stdout

    @patch("srunx.cli.main.WorkflowRunner")
    def test_workflow_run_with_from_option(self, mock_runner_class):
        """Test workflow run with --from option."""
        mock_runner = Mock()
        mock_runner_class.from_yaml.return_value = mock_runner
        mock_runner.workflow.validate.return_value = None
        mock_runner.run.return_value = {"job2": Mock(), "job3": Mock()}

        with tempfile.TemporaryDirectory() as temp_dir:
            yaml_path = self.create_test_workflow(Path(temp_dir))

            result = self.runner.invoke(
                main_app, ["flow", "run", str(yaml_path), "--from", "job2"]
            )

            assert result.exit_code == 0
            mock_runner.run.assert_called_once_with(
                from_job="job2", to_job=None, single_job=None
            )

    @patch("srunx.cli.main.WorkflowRunner")
    def test_workflow_run_with_to_option(self, mock_runner_class):
        """Test workflow run with --to option."""
        mock_runner = Mock()
        mock_runner_class.from_yaml.return_value = mock_runner
        mock_runner.workflow.validate.return_value = None
        mock_runner.run.return_value = {"job1": Mock(), "job2": Mock()}

        with tempfile.TemporaryDirectory() as temp_dir:
            yaml_path = self.create_test_workflow(Path(temp_dir))

            result = self.runner.invoke(
                main_app, ["flow", "run", str(yaml_path), "--to", "job2"]
            )

            assert result.exit_code == 0
            mock_runner.run.assert_called_once_with(
                from_job=None, to_job="job2", single_job=None
            )

    @patch("srunx.cli.main.WorkflowRunner")
    def test_workflow_run_with_job_option(self, mock_runner_class):
        """Test workflow run with --job option."""
        mock_runner = Mock()
        mock_runner_class.from_yaml.return_value = mock_runner
        mock_runner.workflow.validate.return_value = None
        mock_runner.run.return_value = {"job2": Mock()}

        with tempfile.TemporaryDirectory() as temp_dir:
            yaml_path = self.create_test_workflow(Path(temp_dir))

            result = self.runner.invoke(
                main_app, ["flow", "run", str(yaml_path), "--job", "job2"]
            )

            assert result.exit_code == 0
            mock_runner.run.assert_called_once_with(
                from_job=None, to_job=None, single_job="job2"
            )

    @patch("srunx.cli.main.WorkflowRunner")
    def test_workflow_run_with_from_and_to_options(self, mock_runner_class):
        """Test workflow run with both --from and --to options."""
        mock_runner = Mock()
        mock_runner_class.from_yaml.return_value = mock_runner
        mock_runner.workflow.validate.return_value = None
        mock_runner.run.return_value = {"job2": Mock(), "job3": Mock()}

        with tempfile.TemporaryDirectory() as temp_dir:
            yaml_path = self.create_test_workflow(Path(temp_dir))

            result = self.runner.invoke(
                main_app,
                ["flow", "run", str(yaml_path), "--from", "job2", "--to", "job3"],
            )

            assert result.exit_code == 0
            mock_runner.run.assert_called_once_with(
                from_job="job2", to_job="job3", single_job=None
            )

    def test_workflow_run_job_with_from_to_conflict(self):
        """Test that --job conflicts with --from and --to options."""
        with tempfile.TemporaryDirectory() as temp_dir:
            yaml_path = self.create_test_workflow(Path(temp_dir))

            # Test --job with --from
            result = self.runner.invoke(
                main_app,
                ["flow", "run", str(yaml_path), "--job", "job2", "--from", "job1"],
            )
            assert result.exit_code == 1
            # Error messages are typically logged, check output or stderr
            output_text = result.stdout + (
                result.stderr if hasattr(result, "stderr") else ""
            )
            assert (
                "Cannot use --job with --from or --to options" in output_text
                or "Cannot use --job" in str(result.exception)
            )

            # Test --job with --to
            result = self.runner.invoke(
                main_app,
                ["flow", "run", str(yaml_path), "--job", "job2", "--to", "job3"],
            )
            assert result.exit_code == 1
            output_text = result.stdout + (
                result.stderr if hasattr(result, "stderr") else ""
            )
            assert (
                "Cannot use --job with --from or --to options" in output_text
                or "Cannot use --job" in str(result.exception)
            )

            # Test --job with both --from and --to
            result = self.runner.invoke(
                main_app,
                [
                    "flow",
                    "run",
                    str(yaml_path),
                    "--job",
                    "job2",
                    "--from",
                    "job1",
                    "--to",
                    "job3",
                ],
            )
            assert result.exit_code == 1
            output_text = result.stdout + (
                result.stderr if hasattr(result, "stderr") else ""
            )
            assert (
                "Cannot use --job with --from or --to options" in output_text
                or "Cannot use --job" in str(result.exception)
            )

    @patch("srunx.cli.main.WorkflowRunner")
    def test_workflow_dry_run_with_execution_options(self, mock_runner_class):
        """Test workflow dry run shows execution plan with new options."""
        mock_runner = Mock()
        mock_runner_class.from_yaml.return_value = mock_runner
        mock_runner.workflow.name = "test_workflow"
        mock_runner.workflow.validate.return_value = None

        # Create mock jobs - need to properly mock isinstance checks
        from srunx.models import Job, JobEnvironment

        mock_job = Job(name="job2", command=["echo", "2"], environment=JobEnvironment())
        mock_runner._get_jobs_to_execute.return_value = [mock_job]

        with tempfile.TemporaryDirectory() as temp_dir:
            yaml_path = self.create_test_workflow(Path(temp_dir))

            # Test dry run with --job
            result = self.runner.invoke(
                main_app, ["flow", "run", str(yaml_path), "--job", "job2", "--dry-run"]
            )

            assert result.exit_code == 0
            assert "Executing single job: job2" in result.stdout
            assert "job2: echo 2" in result.stdout
            mock_runner._get_jobs_to_execute.assert_called_with(None, None, "job2")

    @patch("srunx.cli.main.WorkflowRunner")
    def test_workflow_dry_run_with_from_to_options(self, mock_runner_class):
        """Test workflow dry run shows execution range with --from and --to options."""
        mock_runner = Mock()
        mock_runner_class.from_yaml.return_value = mock_runner
        mock_runner.workflow.name = "test_workflow"
        mock_runner.workflow.validate.return_value = None

        # Create mock jobs - need to properly mock isinstance checks
        from srunx.models import Job, JobEnvironment

        mock_job1 = Job(
            name="job2", command=["echo", "2"], environment=JobEnvironment()
        )
        mock_job2 = Job(
            name="job3", command=["echo", "3"], environment=JobEnvironment()
        )
        mock_runner._get_jobs_to_execute.return_value = [mock_job1, mock_job2]

        with tempfile.TemporaryDirectory() as temp_dir:
            yaml_path = self.create_test_workflow(Path(temp_dir))

            # Test dry run with --from and --to
            result = self.runner.invoke(
                main_app,
                [
                    "flow",
                    "run",
                    str(yaml_path),
                    "--from",
                    "job2",
                    "--to",
                    "job3",
                    "--dry-run",
                ],
            )

            assert result.exit_code == 0
            assert "Executing jobs from job2 to job3: 2 jobs" in result.stdout
            assert "job2: echo 2" in result.stdout
            assert "job3: echo 3" in result.stdout
            mock_runner._get_jobs_to_execute.assert_called_with("job2", "job3", None)
