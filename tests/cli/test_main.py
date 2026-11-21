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
        pass it to :func:`srunx.cli.workflow.orchestrator._execute_workflow`, so
        :class:`DebugCallback` never fired.
        """
        yaml_path = tmp_path / "wf.yaml"
        yaml_path.write_text(
            'name: dbg\njobs:\n  - name: a\n    command: ["echo", "hi"]\n'
        )
        with patch("srunx.cli.workflow.orchestrator._execute_workflow") as mock_exec:
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
    def test_sbatch_wrap_basic(self, mock_slurm_class):
        """``srunx sbatch --wrap "cmd"`` submits a Job that runs cmd via bash -c.

        Real ``sbatch --wrap`` invokes ``/bin/sh -c "<cmd>"`` on the
        compute node, so shell operators (``&&`` / ``|`` / ``>``)
        evaluate there. srunx mirrors that by stuffing the wrap
        string into ``["bash", "-c", "<cmd>"]`` so the rendered
        sbatch script ends up running ``srun bash -c '<cmd>'``.
        Closes #138.
        """

        mock_slurm = Mock()
        mock_job = Mock()
        mock_job.job_id = 12345
        mock_job.name = "test_job"
        mock_job.command = ["bash", "-c", "python script.py"]
        mock_slurm.submit.return_value = mock_job
        mock_slurm_class.return_value = mock_slurm

        result = self.runner.invoke(
            app,
            ["sbatch", "--wrap", "python script.py", "--job-name", "test_job"],
        )

        assert result.exit_code == 0, result.stdout
        assert "Job submitted successfully: 12345" in result.stdout
        mock_slurm.submit.assert_called_once()
        submitted_job = mock_slurm.submit.call_args[0][0]
        # --wrap is wrapped via bash -c so shell operators stay on
        # the compute node (real sbatch parity).
        assert submitted_job.command == ["bash", "-c", "python script.py"]

    @patch("srunx.slurm.local.Slurm")
    def test_sbatch_wrap_preserves_shell_operators(self, mock_slurm_class):
        """``--wrap "cmd1 && cmd2"`` runs both commands on the compute node.

        Regression for #138: the old implementation passed the wrap
        string straight to Job.command, which the template rendered
        as ``srun cmd1 && cmd2``. ``cmd2`` would silently run on the
        submitting host, not the compute node.
        """

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
    def test_sbatch_positional_script(self, mock_slurm_class, tmp_path):
        """``srunx sbatch <script>`` submits a ShellJob referencing the file."""

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
            app, ["sbatch", "--job-name", "test_job", str(script_path)]
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
            ["sbatch", "--wrap", "python script.py", str(script_path)],
        )
        assert result.exit_code != 0
        combined = (result.stdout or "") + (result.stderr or "")
        assert "mutually exclusive" in combined.lower()

    @patch("srunx.slurm.local.Slurm")
    def test_sbatch_gres_passed_through(self, mock_slurm_class):
        """``--gres=gpu:4`` reaches sbatch unchanged."""

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
        # --gres is passed through verbatim (no conversion, design §6).
        kwargs = mock_slurm.submit.call_args.kwargs
        assert kwargs["extra_sbatch_args"] == ["--gres=gpu:4"]

    @patch("srunx.slurm.local.Slurm")
    def test_scancel_command(self, mock_slurm_class):
        """Test scancel command."""
        # Mock Slurm client
        mock_slurm = Mock()
        mock_slurm.cancel.return_value = True
        mock_slurm_class.return_value = mock_slurm

        result = self.runner.invoke(app, ["scancel", "12345", "--cluster", "local"])

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

    def test_config_show_command(self):
        """``config show`` prints effective values with their sources."""
        from tests.conftest import append_user_config

        append_user_config('[defaults.resources]\npartition = "gpu"')
        result = self.runner.invoke(app, ["config", "show"])
        assert result.exit_code == 0, result.output
        assert "Cluster: local" in result.stdout
        assert "resources.partition" in result.stdout
        assert "'gpu'" in result.stdout
        assert "built-in" in result.stdout

    def test_config_paths_command(self):
        from srunx.common import paths

        result = self.runner.invoke(app, ["config", "paths"])
        assert result.exit_code == 0
        assert str(paths.user_config_path()) in result.stdout
        assert "state" in result.stdout

    def test_config_init_writes_valid_user_config(self):
        from srunx.common import paths
        from srunx.common.config import clear_settings_cache, load_settings

        path = paths.user_config_path()
        path.unlink()
        result = self.runner.invoke(app, ["config", "init"])
        assert result.exit_code == 0, result.output
        assert path.exists()
        clear_settings_cache()
        assert "local" in load_settings().clusters
        assert oct(path.stat().st_mode & 0o777) == "0o600"
        assert oct(path.parent.stat().st_mode & 0o777) == "0o700"

    def test_config_init_does_not_overwrite_without_force(self):
        from srunx.common import paths

        before = paths.user_config_path().read_text()
        result = self.runner.invoke(app, ["config", "init"])
        assert result.exit_code == 0
        assert "already exists" in result.stdout
        assert paths.user_config_path().read_text() == before

    def test_config_set_delete_unset(self):
        from srunx.common import paths
        from srunx.common.config_store import ConfigStore

        r = self.runner.invoke(
            app, ["config", "set", "resources.partition", "gpu", "--cluster", "local"]
        )
        assert r.exit_code == 0, r.output
        data = ConfigStore(paths.user_config_path()).read()
        assert data["clusters"]["local"]["defaults"]["resources"]["partition"] == "gpu"
        r = self.runner.invoke(app, ["config", "set", "resources.nodes", "4"])
        assert r.exit_code == 0, r.output
        assert (
            ConfigStore(paths.user_config_path()).read()["defaults"]["resources"][
                "nodes"
            ]
            == 4
        )
        r = self.runner.invoke(app, ["config", "unset", "resources.time_limit"])
        assert r.exit_code == 0, r.output
        assert ConfigStore(paths.user_config_path()).read()["defaults"]["resources"][
            "unset"
        ] == ["time_limit"]
        r = self.runner.invoke(app, ["config", "delete", "resources.nodes"])
        assert r.exit_code == 0, r.output
        assert (
            "nodes"
            not in ConfigStore(paths.user_config_path()).read()["defaults"]["resources"]
        )

    def test_config_set_rejects_invalid_value(self):
        r = self.runner.invoke(app, ["config", "set", "resources.nodes", "0"])
        assert r.exit_code == 1

    def test_config_init_project_and_project_set(self, tmp_path, monkeypatch):
        (tmp_path / ".git").mkdir()
        monkeypatch.chdir(tmp_path)
        r = self.runner.invoke(app, ["config", "init-project", "--name", "demo"])
        assert r.exit_code == 0, r.output
        r = self.runner.invoke(app, ["config", "set", "cluster", "local", "--project"])
        assert r.exit_code == 0, r.output
        r = self.runner.invoke(
            app,
            ["config", "set", "remote_dir", "/r/demo", "--cluster", "dgx", "--project"],
        )
        assert r.exit_code == 0, r.output
        text = (tmp_path / ".srunx" / "config.toml").read_text()
        assert 'name = "demo"' in text
        assert 'cluster = "local"' in text
        assert 'remote_dir = "/r/demo"' in text

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
    def test_no_container_suppresses_config_default_on_sbatch(self, mock_slurm_class):
        """Test --no-container on sbatch suppresses config default container."""
        from tests.conftest import append_user_config

        append_user_config(
            '[defaults.environment.container]\nimage = "default-image:latest"\n'
            'runtime = "pyxis"'
        )

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
    def test_container_runtime_override_on_sbatch(self, mock_slurm_class):
        """Test --container-runtime without --container overrides config default (T6.9)."""
        from tests.conftest import append_user_config

        append_user_config(
            '[defaults.environment.container]\nimage = "default-image:latest"\n'
            'runtime = "pyxis"'
        )

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


class TestVersion:
    """``srunx --version`` / ``-v`` prints the installed version and exits."""

    def test_version_long_flag(self):
        from srunx import __version__

        runner = CliRunner()
        result = runner.invoke(app, ["--version"])
        assert result.exit_code == 0
        assert __version__ in result.stdout
        assert "srunx" in result.stdout

    def test_version_short_flag(self):
        from srunx import __version__

        runner = CliRunner()
        result = runner.invoke(app, ["-v"])
        assert result.exit_code == 0
        assert __version__ in result.stdout

    def test_version_is_eager(self):
        """``-v`` short-circuits before any subcommand runs."""
        runner = CliRunner()
        result = runner.invoke(app, ["-v", "squeue"])
        assert result.exit_code == 0
        from srunx import __version__

        assert __version__ in result.stdout
