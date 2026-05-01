"""Tests for srunx.cli module."""

import re
from unittest.mock import Mock, patch

import pytest
from typer.testing import CliRunner

from srunx.cli._helpers.sbatch_helpers import _parse_env_vars
from srunx.cli.main import app


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
        """Test main help command lists the SLURM-aligned subcommands."""
        result = self.runner.invoke(app, ["--help"])
        assert result.exit_code == 0
        assert "Python library for SLURM job management" in result.stdout
        assert "sbatch" in result.stdout
        assert "squeue" in result.stdout
        assert "scancel" in result.stdout
        assert "sinfo" in result.stdout
        assert "history" in result.stdout
        assert "tail" in result.stdout
        assert "watch" in result.stdout
        assert "flow" in result.stdout
        assert "config" in result.stdout

    def test_sbatch_help(self):
        """Test sbatch command help advertises SLURM-aligned flags."""
        result = self.runner.invoke(app, ["sbatch", "--help"])
        assert result.exit_code == 0
        clean_output = strip_ansi_codes(result.stdout)
        assert "Submit a SLURM job" in clean_output
        assert "--wrap" in clean_output
        assert "--nodes" in clean_output
        assert "--gpus-per-node" in clean_output
        assert "--gres" in clean_output
        # SLURM short flags must appear too: -J (name), -D (chdir),
        # -N (nodes), -p (partition), -t (time), -c (cpus-per-task),
        # -w (nodelist).
        assert "-J" in clean_output
        assert "-N" in clean_output
        assert "-p" in clean_output

    def test_squeue_help(self):
        """Test squeue command help."""
        result = self.runner.invoke(app, ["squeue", "--help"])
        assert result.exit_code == 0
        assert "List active jobs" in result.stdout

    def test_scancel_help(self):
        """Test scancel command help."""
        result = self.runner.invoke(app, ["scancel", "--help"])
        assert result.exit_code == 0
        assert "Cancel a running job" in result.stdout
        assert "job_id" in result.stdout

    def test_flow_help(self):
        """Test flow command help."""
        result = self.runner.invoke(app, ["flow", "--help"])
        assert result.exit_code == 0
        assert "Workflow management" in result.stdout
        assert "run" in result.stdout

    def test_flow_run_help(self):
        """Test flow run command help includes debug + validate + sync options.

        Use loose substring checks because Typer wraps long help lines
        across the terminal width — the longer help text PR #135 added
        for ``--sync`` pushed the ``--debug`` description below the
        wrap point. Asserting the flag *names* exist (which is what
        users grep for) is the contract we actually care about.
        """
        result = self.runner.invoke(app, ["flow", "run", "--help"])
        assert result.exit_code == 0
        clean_output = strip_ansi_codes(result.stdout)
        assert "Execute workflow from YAML file" in clean_output
        assert "--debug" in clean_output
        assert "--validate" in clean_output
        assert "--sync" in clean_output
        assert "--no-sync" in clean_output

    def test_flow_run_debug_flag_threads_through(self, tmp_path):
        """Regression for I6: ``srunx flow run --debug`` must forward debug=True.

        Prior to the fix, ``flow run`` parsed ``--debug`` but did not
        pass it to :func:`srunx.cli.workflow._execute_workflow`, so
        :class:`DebugCallback` never fired.
        """
        yaml_path = tmp_path / "wf.yaml"
        yaml_path.write_text(
            'name: dbg\njobs:\n  - name: a\n    command: ["echo", "hi"]\n'
        )
        with patch("srunx.cli.workflow._execute_workflow") as mock_exec:
            result = self.runner.invoke(app, ["flow", "run", "--debug", str(yaml_path)])
        assert result.exit_code == 0, result.stdout
        mock_exec.assert_called_once()
        assert mock_exec.call_args.kwargs["debug"] is True

    def test_config_help(self):
        """Test config command help."""
        result = self.runner.invoke(app, ["config", "--help"])
        assert result.exit_code == 0
        assert "Configuration management" in result.stdout
        assert "show" in result.stdout
        assert "paths" in result.stdout
        assert "init" in result.stdout

    @patch("srunx.slurm.local.Slurm")
    @patch("srunx.cli.commands.jobs.sbatch.get_config")
    def test_sbatch_wrap_basic(self, mock_get_config, mock_slurm_class):
        """``srunx sbatch --wrap "cmd"`` submits a Job that runs cmd via bash -c.

        Real ``sbatch --wrap`` invokes ``/bin/sh -c "<cmd>"`` on the
        compute node, so shell operators (``&&`` / ``|`` / ``>``)
        evaluate there. srunx mirrors that by stuffing the wrap
        string into ``["bash", "-c", "<cmd>"]`` so the rendered
        sbatch script ends up running ``srun bash -c '<cmd>'``.
        Closes #138.
        """
        mock_config = Mock()
        mock_config.log_dir = "logs"
        mock_config.work_dir = None
        mock_get_config.return_value = mock_config

        mock_slurm = Mock()
        mock_job = Mock()
        mock_job.job_id = 12345
        mock_job.name = "test_job"
        mock_job.command = ["bash", "-c", "python script.py"]
        mock_slurm.submit.return_value = mock_job
        mock_slurm_class.return_value = mock_slurm

        result = self.runner.invoke(
            app,
            ["sbatch", "--wrap", "python script.py", "--name", "test_job"],
        )

        assert result.exit_code == 0, result.stdout
        assert "Job submitted successfully: 12345" in result.stdout
        mock_slurm.submit.assert_called_once()
        submitted_job = mock_slurm.submit.call_args[0][0]
        # --wrap is wrapped via bash -c so shell operators stay on
        # the compute node (real sbatch parity).
        assert submitted_job.command == ["bash", "-c", "python script.py"]

    @patch("srunx.slurm.local.Slurm")
    @patch("srunx.cli.commands.jobs.sbatch.get_config")
    def test_sbatch_wrap_preserves_shell_operators(
        self, mock_get_config, mock_slurm_class
    ):
        """``--wrap "cmd1 && cmd2"`` runs both commands on the compute node.

        Regression for #138: the old implementation passed the wrap
        string straight to Job.command, which the template rendered
        as ``srun cmd1 && cmd2``. ``cmd2`` would silently run on the
        submitting host, not the compute node.
        """
        mock_config = Mock()
        mock_config.log_dir = "logs"
        mock_config.work_dir = None
        mock_get_config.return_value = mock_config

        mock_slurm = Mock()
        mock_job = Mock(job_id=1, name="j", command=[])
        mock_slurm.submit.return_value = mock_job
        mock_slurm_class.return_value = mock_slurm

        result = self.runner.invoke(
            app,
            [
                "sbatch",
                "--wrap",
                "module load cuda && python train.py",
            ],
        )

        assert result.exit_code == 0, result.stdout
        submitted_job = mock_slurm.submit.call_args[0][0]
        # The whole shell pipeline must live inside bash -c.
        assert submitted_job.command == [
            "bash",
            "-c",
            "module load cuda && python train.py",
        ]

    def test_wrap_renders_to_srun_bash_c(self) -> None:
        """End-to-end render check: shell operators stay quoted in bash -c.

        Renders the default template against a Job built the way the
        CLI builds it for ``--wrap`` and asserts the output line
        contains ``srun bash -c '...'``. Goes through the actual
        :func:`render_job_script` pipeline so any future template
        change keeps wrap parity. Closes #138.
        """
        import tempfile

        from srunx.domain import (
            Job,
            JobEnvironment,
            JobResource,
        )
        from srunx.runtime.rendering import render_job_script
        from srunx.runtime.templates import get_template_path

        job = Job(
            name="wrap-parity",
            command=["bash", "-c", "module load cuda && python train.py"],
            resources=JobResource(),
            environment=JobEnvironment(),
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            script_path = render_job_script(get_template_path("base"), job, tmpdir)
            content = open(script_path).read()

        # The whole pipeline must sit inside bash -c '...'. ``shlex.join``
        # uses single quotes for safety.
        assert "srun bash -c 'module load cuda && python train.py'" in content, (
            "wrap parity broken: shell operators must stay inside bash -c "
            "so they evaluate on the compute node, not the submitting host. "
            "See #138."
        )

    @patch("srunx.slurm.local.Slurm")
    @patch("srunx.cli.commands.jobs.sbatch.get_config")
    def test_sbatch_positional_script(
        self, mock_get_config, mock_slurm_class, tmp_path
    ):
        """``srunx sbatch <script>`` submits a ShellJob referencing the file."""
        mock_config = Mock()
        mock_config.log_dir = "logs"
        mock_config.work_dir = None
        mock_get_config.return_value = mock_config

        script_path = tmp_path / "run.sh"
        script_path.write_text("#!/bin/bash\necho hi\n")

        from srunx.domain import ShellJob

        mock_slurm = Mock()
        mock_job = Mock(spec=ShellJob)
        mock_job.job_id = 12345
        mock_job.name = "test_job"
        mock_job.script_path = str(script_path)
        mock_slurm.submit.return_value = mock_job
        mock_slurm_class.return_value = mock_slurm

        result = self.runner.invoke(
            app, ["sbatch", str(script_path), "--name", "test_job"]
        )

        assert result.exit_code == 0, result.stdout
        assert "Job submitted successfully: 12345" in result.stdout
        submitted_job = mock_slurm.submit.call_args[0][0]
        assert isinstance(submitted_job, ShellJob)
        assert submitted_job.script_path == str(script_path)

    def test_sbatch_script_and_wrap_mutex(self, tmp_path):
        """Positional script and --wrap are mutually exclusive."""
        script_path = tmp_path / "run.sh"
        script_path.write_text("#!/bin/bash\necho hi\n")
        result = self.runner.invoke(
            app,
            ["sbatch", str(script_path), "--wrap", "python script.py"],
        )
        assert result.exit_code != 0
        combined = (result.stdout or "") + (result.stderr or "")
        assert "mutually exclusive" in combined.lower()

    @patch("srunx.slurm.local.Slurm")
    @patch("srunx.cli.commands.jobs.sbatch.get_config")
    def test_sbatch_gres_sets_gpus_per_node(self, mock_get_config, mock_slurm_class):
        """``--gres=gpu:4`` overrides ``--gpus-per-node`` (sbatch parity)."""
        mock_config = Mock()
        mock_config.log_dir = "logs"
        mock_config.work_dir = None
        mock_get_config.return_value = mock_config

        mock_slurm = Mock()
        mock_job = Mock()
        mock_job.job_id = 1
        mock_job.name = "j"
        mock_job.command = "python x.py"
        mock_slurm.submit.return_value = mock_job
        mock_slurm_class.return_value = mock_slurm

        result = self.runner.invoke(
            app, ["sbatch", "--wrap", "python x.py", "--gres", "gpu:4"]
        )

        assert result.exit_code == 0, result.stdout
        submitted_job = mock_slurm.submit.call_args[0][0]
        assert submitted_job.resources.gpus_per_node == 4

    @patch("srunx.slurm.local.Slurm")
    def test_scancel_command(self, mock_slurm_class):
        """Test scancel command."""
        # Mock Slurm client
        mock_slurm = Mock()
        mock_slurm.cancel.return_value = True
        mock_slurm_class.return_value = mock_slurm

        result = self.runner.invoke(app, ["scancel", "12345"])

        assert result.exit_code == 0
        assert "Job 12345 cancelled successfully" in result.stdout
        mock_slurm.cancel.assert_called_once_with(12345)

    @patch("srunx.slurm.local.Slurm")
    def test_squeue_command_empty(self, mock_slurm_class):
        """Test squeue command with empty queue."""
        # Mock Slurm client
        mock_slurm = Mock()
        mock_slurm.queue.return_value = []
        mock_slurm_class.return_value = mock_slurm

        result = self.runner.invoke(app, ["squeue"])

        assert result.exit_code == 0
        assert "No jobs in queue" in result.stdout
        mock_slurm.queue.assert_called_once()

    @patch("srunx.cli.commands.config.get_config")
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
        mock_get_config.assert_called_once()

    @patch("srunx.cli.commands.config.get_config_paths")
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

    def test_sbatch_missing_job_source(self):
        """sbatch requires either a positional script or --wrap."""
        result = self.runner.invoke(app, ["sbatch"])
        assert result.exit_code != 0
        combined = (result.stdout or "") + (result.stderr or "")
        assert "job source" in combined.lower() or "wrap" in combined.lower()

    def test_scancel_missing_job_id(self):
        """Test scancel command without required job ID."""
        result = self.runner.invoke(app, ["scancel"])
        assert result.exit_code == 2  # Typer error exit code
        assert "Missing argument" in result.stderr


class TestParseContainerArgs:
    """Test _parse_container_args() function (T6.3)."""

    def test_parse_simple_image(self):
        """Test parsing a simple image path."""
        from srunx.cli._helpers.sbatch_helpers import _parse_container_args

        result = _parse_container_args("pytorch/pytorch:latest")
        assert result is not None
        assert result.image == "pytorch/pytorch:latest"
        assert result.runtime == "pyxis"  # default

    def test_parse_none_returns_none(self):
        """Test parsing None returns None."""
        from srunx.cli._helpers.sbatch_helpers import _parse_container_args

        result = _parse_container_args(None)
        assert result is None

    def test_parse_empty_string_returns_none(self):
        """Test parsing empty string returns None."""
        from srunx.cli._helpers.sbatch_helpers import _parse_container_args

        result = _parse_container_args("")
        assert result is None

    def test_parse_runtime_apptainer(self):
        """Test parsing with runtime=apptainer."""
        from srunx.cli._helpers.sbatch_helpers import _parse_container_args

        result = _parse_container_args("image=test.sif,runtime=apptainer,nv=true")
        assert result is not None
        assert result.image == "test.sif"
        assert result.runtime == "apptainer"
        assert result.nv is True

    def test_parse_bind_alias(self):
        """Test parsing with bind= alias for mounts."""
        from srunx.cli._helpers.sbatch_helpers import _parse_container_args

        result = _parse_container_args(
            "image=test.sif,bind=/data:/data;/scratch:/scratch,runtime=apptainer"
        )
        assert result is not None
        assert result.mounts == ["/data:/data", "/scratch:/scratch"]

    def test_parse_mounts_key(self):
        """Test parsing with mounts= key."""
        from srunx.cli._helpers.sbatch_helpers import _parse_container_args

        result = _parse_container_args(
            "image=test.sif,mounts=/data:/data;/scratch:/scratch"
        )
        assert result is not None
        assert result.mounts == ["/data:/data", "/scratch:/scratch"]

    def test_parse_apptainer_options(self):
        """Test parsing all Apptainer-specific options."""
        from srunx.cli._helpers.sbatch_helpers import _parse_container_args

        result = _parse_container_args(
            "image=test.sif,runtime=apptainer,nv=true,rocm=true,"
            "cleanenv=true,fakeroot=true,writable_tmpfs=true,"
            "overlay=/overlay.img"
        )
        assert result is not None
        assert result.runtime == "apptainer"
        assert result.nv is True
        assert result.rocm is True
        assert result.cleanenv is True
        assert result.fakeroot is True
        assert result.writable_tmpfs is True
        assert result.overlay == "/overlay.img"

    def test_parse_env_in_container_args(self):
        """Test parsing env key=value pairs in container args."""
        from srunx.cli._helpers.sbatch_helpers import _parse_container_args

        result = _parse_container_args(
            "image=test.sif,runtime=apptainer,env=KEY1=VAL1;KEY2=VAL2"
        )
        assert result is not None
        assert result.env == {"KEY1": "VAL1", "KEY2": "VAL2"}

    def test_parse_workdir(self):
        """Test parsing workdir."""
        from srunx.cli._helpers.sbatch_helpers import _parse_container_args

        result = _parse_container_args("image=test.sif,workdir=/workspace")
        assert result is not None
        assert result.workdir == "/workspace"

    def test_parse_bare_runtime_only(self):
        """Test parsing a bare runtime=apptainer without image."""
        from srunx.cli._helpers.sbatch_helpers import _parse_container_args

        result = _parse_container_args("runtime=apptainer")
        assert result is not None
        assert result.runtime == "apptainer"
        assert result.image is None


class TestSbatchTemplateOption:
    """Test that sbatch exposes container/template options (T6.3)."""

    def setup_method(self):
        """Setup test environment."""
        self.runner = CliRunner()

    def test_sbatch_help_has_container_runtime_option(self):
        """Test that sbatch help shows --container-runtime / --no-container."""
        result = self.runner.invoke(app, ["sbatch", "--help"])
        assert result.exit_code == 0
        clean_output = strip_ansi_codes(result.stdout)
        assert "--container-runtime" in clean_output
        assert "--no-container" in clean_output

    def test_sbatch_help_has_template_option(self):
        """Test that sbatch help shows --template (replaces template apply)."""
        result = self.runner.invoke(app, ["sbatch", "--help"])
        assert result.exit_code == 0
        clean_output = strip_ansi_codes(result.stdout)
        assert "--template" in clean_output


class TestNoContainerFlag:
    """Test --no-container flag suppresses config defaults (T6.7, AC-15)."""

    def setup_method(self):
        """Setup test environment."""
        self.runner = CliRunner()

    @patch("srunx.slurm.local.Slurm")
    @patch("srunx.cli.commands.jobs.sbatch.get_config")
    def test_no_container_suppresses_config_default_on_sbatch(
        self, mock_get_config, mock_slurm_class
    ):
        """Test --no-container on sbatch suppresses config default container."""
        from srunx.common.config import SrunxConfig

        # Config has a default container (use model_validate to avoid Pydantic
        # class identity issues when tests run together)
        mock_config = SrunxConfig.model_validate(
            {
                "environment": {
                    "container": {"image": "default-image:latest", "runtime": "pyxis"}
                }
            }
        )
        mock_get_config.return_value = mock_config

        # Mock Slurm client
        mock_slurm = Mock()
        mock_job = Mock()
        mock_job.job_id = 99999
        mock_job.name = "test"
        mock_job.command = "echo hello"
        mock_slurm.submit.return_value = mock_job
        mock_slurm_class.return_value = mock_slurm

        result = self.runner.invoke(
            app, ["sbatch", "--wrap", "echo hello", "--no-container"]
        )

        assert result.exit_code == 0, result.stdout
        # Verify the submitted job has no container
        submitted_job = mock_slurm.submit.call_args[0][0]
        assert submitted_job.environment.container is None

    @patch("srunx.slurm.local.Slurm")
    @patch("srunx.cli.commands.jobs.sbatch.get_config")
    def test_container_runtime_override_on_sbatch(
        self, mock_get_config, mock_slurm_class
    ):
        """Test --container-runtime without --container overrides config default (T6.9)."""
        from srunx.common.config import SrunxConfig

        # Config has a default pyxis container (use model_validate to avoid Pydantic
        # class identity issues when tests run together)
        mock_config = SrunxConfig.model_validate(
            {
                "environment": {
                    "container": {"image": "default-image:latest", "runtime": "pyxis"}
                }
            }
        )
        mock_get_config.return_value = mock_config

        # Mock Slurm client
        mock_slurm = Mock()
        mock_job = Mock()
        mock_job.job_id = 99999
        mock_job.name = "test"
        mock_job.command = "echo hello"
        mock_slurm.submit.return_value = mock_job
        mock_slurm_class.return_value = mock_slurm

        result = self.runner.invoke(
            app,
            ["sbatch", "--wrap", "echo hello", "--container-runtime", "apptainer"],
        )

        assert result.exit_code == 0, result.stdout
        submitted_job = mock_slurm.submit.call_args[0][0]
        assert submitted_job.environment.container is not None
        assert submitted_job.environment.container.runtime == "apptainer"
        # Image should be preserved from config default
        assert submitted_job.environment.container.image == "default-image:latest"
