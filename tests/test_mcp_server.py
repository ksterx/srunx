"""Tests for srunx.mcp.server module."""

from unittest.mock import MagicMock, patch

import pytest
import yaml

from srunx.mcp.server import (
    _err,
    _get_ssh_client,
    _job_to_dict,
    _ok,
    _validate_job_id,
    _validate_partition,
    cancel_job,
    create_workflow,
    get_config,
    get_job_logs,
    get_job_status,
    get_resources,
    list_jobs,
    list_ssh_profiles,
    list_workflows,
    submit_job,
    validate_workflow,
)

# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------


class TestOk:
    """Test _ok helper."""

    def test_ok_no_data(self):
        result = _ok()
        assert result == {"success": True}

    def test_ok_with_data(self):
        result = _ok(data={"key": "value"})
        assert result == {"success": True, "data": {"key": "value"}}

    def test_ok_with_kwargs(self):
        result = _ok(job_id="123", name="test")
        assert result == {"success": True, "job_id": "123", "name": "test"}

    def test_ok_with_data_and_kwargs(self):
        result = _ok(data=[1, 2], extra="info")
        assert result == {"success": True, "data": [1, 2], "extra": "info"}

    def test_ok_none_data_excluded(self):
        result = _ok(data=None)
        assert "data" not in result
        assert result == {"success": True}


class TestErr:
    """Test _err helper."""

    def test_err_basic(self):
        result = _err("something went wrong")
        assert result == {"success": False, "error": "something went wrong"}

    def test_err_empty_message(self):
        result = _err("")
        assert result == {"success": False, "error": ""}


# ---------------------------------------------------------------------------
# Input validation
# ---------------------------------------------------------------------------


class TestValidateJobId:
    """Test _validate_job_id."""

    def test_valid_simple_id(self):
        assert _validate_job_id("12345") == "12345"

    def test_valid_array_job_id(self):
        assert _validate_job_id("12345_1") == "12345_1"

    def test_invalid_alpha(self):
        with pytest.raises(ValueError, match="Invalid job ID"):
            _validate_job_id("abc")

    def test_invalid_empty(self):
        with pytest.raises(ValueError, match="Invalid job ID"):
            _validate_job_id("")

    def test_invalid_special_chars(self):
        with pytest.raises(ValueError, match="Invalid job ID"):
            _validate_job_id("123; rm -rf /")

    def test_invalid_spaces(self):
        with pytest.raises(ValueError, match="Invalid job ID"):
            _validate_job_id("123 456")

    def test_invalid_negative(self):
        with pytest.raises(ValueError, match="Invalid job ID"):
            _validate_job_id("-1")

    def test_invalid_double_underscore(self):
        with pytest.raises(ValueError, match="Invalid job ID"):
            _validate_job_id("123_1_2")


class TestValidatePartition:
    """Test _validate_partition."""

    def test_valid_alphanumeric(self):
        assert _validate_partition("gpu") == "gpu"

    def test_valid_with_underscore(self):
        assert _validate_partition("gpu_high") == "gpu_high"

    def test_valid_with_hyphen(self):
        assert _validate_partition("gpu-high") == "gpu-high"

    def test_valid_mixed(self):
        assert _validate_partition("gpu_A100-2") == "gpu_A100-2"

    def test_invalid_semicolon(self):
        with pytest.raises(ValueError, match="Invalid partition name"):
            _validate_partition("gpu; echo hacked")

    def test_invalid_spaces(self):
        with pytest.raises(ValueError, match="Invalid partition name"):
            _validate_partition("gpu high")

    def test_invalid_empty(self):
        with pytest.raises(ValueError, match="Invalid partition name"):
            _validate_partition("")


# ---------------------------------------------------------------------------
# _job_to_dict
# ---------------------------------------------------------------------------


class TestJobToDict:
    """Test _job_to_dict helper."""

    def test_basic_job(self):
        job = MagicMock()
        job.name = "test_job"
        job.job_id = "12345"
        job._status.value = "RUNNING"
        job.command = ["python", "train.py"]
        job.partition = "gpu"
        job.user = "testuser"
        job.elapsed_time = "00:10:00"
        job.nodes = 1
        job.nodelist = "node001"
        job.cpus = 4
        job.gpus = 1
        # No script_path
        del job.script_path

        result = _job_to_dict(job)
        assert result["name"] == "test_job"
        assert result["job_id"] == "12345"
        assert result["status"] == "RUNNING"
        assert result["command"] == "python train.py"
        assert result["partition"] == "gpu"
        assert result["user"] == "testuser"
        assert result["gpus"] == 1

    def test_job_with_string_command(self):
        job = MagicMock()
        job.name = "test"
        job.job_id = "1"
        job._status.value = "PENDING"
        job.command = "echo hello"
        del job.script_path
        # Set optional fields to None so they don't appear
        job.partition = None
        job.user = None
        job.elapsed_time = None
        job.nodes = None
        job.nodelist = None
        job.cpus = None
        job.gpus = None

        result = _job_to_dict(job)
        assert result["command"] == "echo hello"

    def test_shell_job(self):
        job = MagicMock()
        job.name = "shell_job"
        job.job_id = "999"
        job._status.value = "COMPLETED"
        job.script_path = "/path/to/script.sh"
        del job.command
        job.partition = None
        job.user = None
        job.elapsed_time = None
        job.nodes = None
        job.nodelist = None
        job.cpus = None
        job.gpus = None

        result = _job_to_dict(job)
        assert result["script_path"] == "/path/to/script.sh"
        assert "command" not in result

    def test_job_without_status_attr(self):
        job = MagicMock(spec=[])
        job.name = "no_status"
        job.job_id = "111"
        # No _status attribute
        result = _job_to_dict(job)
        assert result["status"] == "UNKNOWN"


# ---------------------------------------------------------------------------
# _get_ssh_client
# ---------------------------------------------------------------------------


class TestGetSshClient:
    """Test _get_ssh_client helper."""

    @patch("srunx.ssh.core.config.ConfigManager")
    def test_no_active_profile_raises(self, mock_cm_cls):
        mock_cm = MagicMock()
        mock_cm.get_current_profile_name.return_value = None
        mock_cm_cls.return_value = mock_cm

        with pytest.raises(RuntimeError, match="No active SSH profile"):
            _get_ssh_client()

    @patch("srunx.ssh.core.config.ConfigManager")
    def test_profile_not_found_raises(self, mock_cm_cls):
        mock_cm = MagicMock()
        mock_cm.get_current_profile_name.return_value = "missing"
        mock_cm.get_profile.return_value = None
        mock_cm_cls.return_value = mock_cm

        with pytest.raises(RuntimeError, match="SSH profile 'missing' not found"):
            _get_ssh_client()

    @patch("srunx.ssh.core.client.SSHSlurmClient")
    @patch("srunx.ssh.core.ssh_config.SSHConfigParser")
    @patch("srunx.ssh.core.config.ConfigManager")
    def test_ssh_host_profile(self, mock_cm_cls, mock_parser_cls, mock_client_cls):
        profile = MagicMock()
        profile.ssh_host = "myhost"
        profile.env_vars = {"KEY": "VAL"}

        mock_cm = MagicMock()
        mock_cm.get_current_profile_name.return_value = "prod"
        mock_cm.get_profile.return_value = profile
        mock_cm_cls.return_value = mock_cm

        ssh_host = MagicMock()
        ssh_host.hostname = "real.host.com"
        ssh_host.user = "admin"
        ssh_host.identity_file = "/home/.ssh/id_rsa"
        ssh_host.port = 2222
        ssh_host.proxy_jump = None
        mock_parser = MagicMock()
        mock_parser.get_host.return_value = ssh_host
        mock_parser_cls.return_value = mock_parser

        client = _get_ssh_client()
        mock_client_cls.assert_called_once_with(
            hostname="real.host.com",
            username="admin",
            key_filename="/home/.ssh/id_rsa",
            port=2222,
            proxy_jump=None,
            env_vars={"KEY": "VAL"},
        )
        assert client is mock_client_cls.return_value

    @patch("srunx.ssh.core.client.SSHSlurmClient")
    @patch("srunx.ssh.core.ssh_config.SSHConfigParser")
    @patch("srunx.ssh.core.config.ConfigManager")
    def test_direct_hostname_profile(
        self, mock_cm_cls, mock_parser_cls, mock_client_cls
    ):
        profile = MagicMock()
        profile.ssh_host = None
        profile.hostname = "direct.host.com"
        profile.username = "user1"
        profile.key_filename = "/path/key"
        profile.port = 22
        profile.proxy_jump = None
        profile.env_vars = None

        mock_cm = MagicMock()
        mock_cm.get_current_profile_name.return_value = "dev"
        mock_cm.get_profile.return_value = profile
        mock_cm_cls.return_value = mock_cm

        mock_parser = MagicMock()
        mock_parser.get_host.return_value = None
        mock_parser_cls.return_value = mock_parser

        client = _get_ssh_client()
        mock_client_cls.assert_called_once_with(
            hostname="direct.host.com",
            username="user1",
            key_filename="/path/key",
            port=22,
            proxy_jump=None,
            env_vars=None,
        )


# ---------------------------------------------------------------------------
# Tool functions
# ---------------------------------------------------------------------------


class TestSubmitJob:
    """Test submit_job tool."""

    @patch("srunx.client.Slurm")
    def test_submit_local_success(self, mock_slurm_cls):
        mock_slurm = MagicMock()
        mock_job = MagicMock()
        mock_job.job_id = "12345"
        mock_job.name = "test_job"
        mock_job._status.value = "PENDING"
        mock_slurm.submit.return_value = mock_job
        mock_slurm_cls.return_value = mock_slurm

        result = submit_job(command="python train.py", name="test_job")
        assert result["success"] is True
        assert result["job_id"] == "12345"
        assert result["name"] == "test_job"

    def test_submit_ssh_requires_work_dir(self):
        result = submit_job(command="python train.py", use_ssh=True)
        assert result["success"] is False
        assert "work_dir is required" in result["error"]

    @patch("srunx.mcp.server._get_ssh_client")
    @patch(
        "jinja2.Template",
        return_value=MagicMock(render=MagicMock(return_value="#!/bin/bash\necho hi")),
    )
    def test_submit_ssh_success(self, _mock_tpl, mock_get_client):
        mock_client = MagicMock()
        mock_returned_job = MagicMock()
        mock_returned_job.job_id = "99999"
        mock_returned_job.name = "ssh_job"
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client.submit_sbatch_job.return_value = mock_returned_job
        mock_get_client.return_value = mock_client

        result = submit_job(
            command="python train.py",
            name="ssh_job",
            use_ssh=True,
            work_dir="/remote/workdir",
        )
        assert result["success"] is True
        assert result["job_id"] == "99999"

    @patch("srunx.mcp.server._get_ssh_client")
    @patch(
        "jinja2.Template",
        return_value=MagicMock(render=MagicMock(return_value="#!/bin/bash")),
    )
    def test_submit_ssh_returns_none(self, _mock_tpl, mock_get_client):
        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client.submit_sbatch_job.return_value = None
        mock_get_client.return_value = mock_client

        result = submit_job(
            command="python train.py",
            use_ssh=True,
            work_dir="/remote/workdir",
        )
        assert result["success"] is False
        assert "SSH job submission failed" in result["error"]

    def test_submit_catches_exception(self):
        with patch(
            "srunx.client.Slurm", side_effect=RuntimeError("slurm not available")
        ):
            result = submit_job(command="echo hi")
            assert result["success"] is False
            assert "slurm not available" in result["error"]

    @patch("srunx.mcp.server._get_ssh_client")
    def test_submit_ssh_real_template_render(self, mock_get_client):
        """Regression test for #117: real template render must succeed."""
        mock_client = MagicMock()
        mock_returned_job = MagicMock()
        mock_returned_job.job_id = "42"
        mock_returned_job.name = "ssh_job"
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client.submit_sbatch_job.return_value = mock_returned_job
        mock_get_client.return_value = mock_client

        result = submit_job(
            command="echo hi",
            name="ssh_job",
            use_ssh=True,
            work_dir="/remote/workdir",
        )
        assert result["success"] is True, result
        script_content = mock_client.submit_sbatch_job.call_args[0][0]
        assert "#SBATCH --job-name=ssh_job" in script_content
        assert "SRUNX_OUTPUTS_DIR" not in script_content


class TestListJobs:
    """Test list_jobs tool."""

    @patch("srunx.client.Slurm")
    def test_list_local(self, mock_slurm_cls):
        mock_slurm = MagicMock()
        job1 = MagicMock()
        job1.name = "job1"
        job1.job_id = "1"
        job1._status.value = "RUNNING"
        job1.command = "echo"
        job1.partition = None
        job1.user = None
        job1.elapsed_time = None
        job1.nodes = None
        job1.nodelist = None
        job1.cpus = None
        job1.gpus = None
        # Ensure script_path is not present
        del job1.script_path

        mock_slurm.queue.return_value = [job1]
        mock_slurm_cls.return_value = mock_slurm

        result = list_jobs(use_ssh=False)
        assert result["success"] is True
        assert result["count"] == 1
        assert result["jobs"][0]["name"] == "job1"

    @patch("srunx.mcp.server._get_ssh_client")
    def test_list_ssh(self, mock_get_client):
        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client._execute_slurm_command.return_value = (
            "12345     gpu       train         user1  RUNNING   0:05:00  1:00:00      1 node001 gpu:1\n",
            "",
            0,
        )
        mock_get_client.return_value = mock_client

        result = list_jobs(use_ssh=True)
        assert result["success"] is True
        assert result["count"] == 1
        assert result["jobs"][0]["job_id"] == "12345"
        assert result["jobs"][0]["status"] == "RUNNING"

    @patch("srunx.mcp.server._get_ssh_client")
    def test_list_ssh_squeue_fails(self, mock_get_client):
        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client._execute_slurm_command.return_value = ("", "error msg", 1)
        mock_get_client.return_value = mock_client

        result = list_jobs(use_ssh=True)
        assert result["success"] is False
        assert "squeue failed" in result["error"]

    def test_list_catches_exception(self):
        with patch("srunx.client.Slurm", side_effect=RuntimeError("no slurm")):
            result = list_jobs()
            assert result["success"] is False


class TestGetJobStatus:
    """Test get_job_status tool."""

    def test_invalid_job_id(self):
        result = get_job_status(job_id="abc")
        assert result["success"] is False
        assert "Invalid job ID" in result["error"]

    @patch("srunx.client.Slurm")
    def test_local_calls_retrieve(self, mock_slurm_cls):
        """Local get_job_status calls Slurm.retrieve with int job_id."""
        mock_slurm_cls.retrieve.side_effect = ValueError("test")
        result = get_job_status(job_id="12345")
        mock_slurm_cls.retrieve.assert_called_once_with(12345)
        assert result["success"] is False

    @patch("srunx.mcp.server._get_ssh_client")
    def test_ssh_success(self, mock_get_client):
        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client.get_job_status.return_value = "COMPLETED"
        mock_get_client.return_value = mock_client

        result = get_job_status(job_id="12345", use_ssh=True)
        assert result["success"] is True
        assert result["status"] == "COMPLETED"

    @patch("srunx.mcp.server._get_ssh_client")
    def test_ssh_not_found(self, mock_get_client):
        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client.get_job_status.return_value = "NOT_FOUND"
        mock_get_client.return_value = mock_client

        result = get_job_status(job_id="99999", use_ssh=True)
        assert result["success"] is False
        assert "NOT_FOUND" in result["error"]

    def test_injection_attempt(self):
        result = get_job_status(job_id="123; rm -rf /")
        assert result["success"] is False
        assert "Invalid job ID" in result["error"]


class TestCancelJob:
    """Test cancel_job tool."""

    def test_invalid_job_id(self):
        result = cancel_job(job_id="not_a_number")
        assert result["success"] is False
        assert "Invalid job ID" in result["error"]

    @patch("srunx.client.Slurm")
    def test_local_cancel(self, mock_slurm_cls):
        mock_slurm = MagicMock()
        mock_slurm_cls.return_value = mock_slurm

        result = cancel_job(job_id="12345")
        assert result["success"] is True
        assert result["message"] == "Job cancelled"
        mock_slurm.cancel.assert_called_once_with(12345)

    @patch("srunx.mcp.server._get_ssh_client")
    def test_ssh_cancel(self, mock_get_client):
        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client._execute_slurm_command.return_value = ("", "", 0)
        mock_get_client.return_value = mock_client

        result = cancel_job(job_id="12345", use_ssh=True)
        assert result["success"] is True
        assert result["message"] == "Job cancelled"

    @patch("srunx.mcp.server._get_ssh_client")
    def test_ssh_cancel_fails(self, mock_get_client):
        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client._execute_slurm_command.return_value = ("", "permission denied", 1)
        mock_get_client.return_value = mock_client

        result = cancel_job(job_id="12345", use_ssh=True)
        assert result["success"] is False
        assert "scancel failed" in result["error"]


class TestGetJobLogs:
    """Test get_job_logs tool."""

    def test_invalid_job_id(self):
        result = get_job_logs(job_id="bad_id")
        assert result["success"] is False
        assert "Invalid job ID" in result["error"]

    @patch("srunx.client.Slurm")
    def test_local_logs(self, mock_slurm_cls):
        mock_slurm = MagicMock()
        mock_slurm.get_job_output_detailed.return_value = {
            "output": "training started\n",
            "error": "",
            "found_files": ["logs/job-12345.out"],
        }
        mock_slurm_cls.return_value = mock_slurm

        result = get_job_logs(job_id="12345")
        assert result["success"] is True
        assert result["stdout"] == "training started\n"
        assert result["log_files"] == ["logs/job-12345.out"]

    @patch("srunx.mcp.server._get_ssh_client")
    def test_ssh_logs(self, mock_get_client):
        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client.get_job_output.return_value = (
            "stdout content",
            "stderr content",
            100,
            50,
        )
        mock_get_client.return_value = mock_client

        result = get_job_logs(job_id="12345", use_ssh=True)
        assert result["success"] is True
        assert result["stdout"] == "stdout content"
        assert result["stderr"] == "stderr content"

    @patch("srunx.mcp.server._get_ssh_client")
    def test_ssh_logs_no_output(self, mock_get_client):
        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client.get_job_output.return_value = ("", "", 0, 0)
        mock_get_client.return_value = mock_client

        result = get_job_logs(job_id="12345", use_ssh=True)
        assert result["success"] is False
        assert "No logs found" in result["error"]


class TestGetResources:
    """Test get_resources tool."""

    def test_invalid_partition(self):
        result = get_resources(partition="gpu; whoami")
        assert result["success"] is False
        assert "Invalid partition name" in result["error"]

    @patch("srunx.monitor.resource_monitor.ResourceMonitor")
    def test_local_resources(self, mock_monitor_cls):
        mock_monitor = MagicMock()
        snapshot = MagicMock()
        snapshot.partition = "gpu"
        snapshot.total_gpus = 8
        snapshot.gpus_in_use = 3
        snapshot.gpus_available = 5
        snapshot.gpu_utilization = 0.375
        snapshot.jobs_running = 2
        snapshot.nodes_total = 4
        snapshot.nodes_idle = 2
        snapshot.nodes_down = 0
        mock_monitor.get_partition_resources.return_value = snapshot
        mock_monitor_cls.return_value = mock_monitor

        result = get_resources(partition="gpu")
        assert result["success"] is True
        assert result["total_gpus"] == 8
        assert result["gpus_available"] == 5
        assert result["gpu_utilization"] == 0.375

    @patch("srunx.mcp.server._get_ssh_client")
    def test_ssh_resources(self, mock_get_client):
        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client._execute_slurm_command.return_value = (
            "node001 gpu:4 idle gpu*\n",
            "",
            0,
        )
        mock_get_client.return_value = mock_client

        result = get_resources(use_ssh=True)
        assert result["success"] is True
        assert "node001" in result["raw_output"]

    @patch("srunx.mcp.server._get_ssh_client")
    def test_ssh_resources_sinfo_fails(self, mock_get_client):
        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client._execute_slurm_command.return_value = ("", "sinfo error", 1)
        mock_get_client.return_value = mock_client

        result = get_resources(use_ssh=True)
        assert result["success"] is False
        assert "sinfo failed" in result["error"]


class TestValidateWorkflow:
    """Test validate_workflow tool."""

    def test_valid_workflow(self, tmp_path):
        workflow = {
            "name": "test_pipeline",
            "jobs": [
                {"name": "preprocess", "command": ["python", "preprocess.py"]},
                {
                    "name": "train",
                    "command": ["python", "train.py"],
                    "depends_on": ["preprocess"],
                },
            ],
        }
        yaml_file = tmp_path / "workflow.yaml"
        with open(yaml_file, "w") as f:
            yaml.dump(workflow, f)

        result = validate_workflow(str(yaml_file))
        assert result["success"] is True
        assert result["valid"] is True
        assert result["name"] == "test_pipeline"
        assert result["job_count"] == 2

    def test_nonexistent_file(self):
        result = validate_workflow("/nonexistent/path/workflow.yaml")
        assert result["success"] is False

    def test_invalid_yaml_structure(self, tmp_path):
        yaml_file = tmp_path / "bad.yaml"
        yaml_file.write_text("not: a: valid: workflow\n")

        result = validate_workflow(str(yaml_file))
        assert result["success"] is False

    def test_circular_dependency(self, tmp_path):
        workflow = {
            "name": "circular",
            "jobs": [
                {
                    "name": "a",
                    "command": ["echo", "a"],
                    "depends_on": ["b"],
                },
                {
                    "name": "b",
                    "command": ["echo", "b"],
                    "depends_on": ["a"],
                },
            ],
        }
        yaml_file = tmp_path / "circular.yaml"
        with open(yaml_file, "w") as f:
            yaml.dump(workflow, f)

        result = validate_workflow(str(yaml_file))
        assert result["success"] is False


class TestCreateWorkflow:
    """Test create_workflow tool."""

    def test_create_basic_workflow(self, tmp_path):
        output = str(tmp_path / "out.yaml")
        result = create_workflow(
            name="my_flow",
            jobs=[
                {"name": "step1", "command": ["echo", "hello"]},
                {
                    "name": "step2",
                    "command": ["echo", "world"],
                    "depends_on": ["step1"],
                },
            ],
            output_path=output,
        )
        assert result["success"] is True
        assert result["name"] == "my_flow"
        assert result["job_count"] == 2

        # Verify file contents
        with open(output) as f:
            data = yaml.safe_load(f)
        assert data["name"] == "my_flow"
        assert len(data["jobs"]) == 2

    def test_python_arg_rejected(self, tmp_path):
        output = str(tmp_path / "out.yaml")
        result = create_workflow(
            name="my_flow",
            jobs=[{"name": "step1", "command": ["echo", "hello"]}],
            output_path=output,
            args={"bad_var": "python: import os; os.system('rm -rf /')"},
        )
        assert result["success"] is False
        assert "python:" in result["error"].lower()

    def test_python_arg_rejected_case_insensitive(self, tmp_path):
        output = str(tmp_path / "out.yaml")
        result = create_workflow(
            name="my_flow",
            jobs=[{"name": "step1", "command": ["echo", "hello"]}],
            output_path=output,
            args={"bad_var": "Python: some expression"},
        )
        assert result["success"] is False
        assert "python:" in result["error"].lower()

    def test_missing_name_in_job(self, tmp_path):
        output = str(tmp_path / "out.yaml")
        result = create_workflow(
            name="my_flow",
            jobs=[{"command": ["echo", "hello"]}],
            output_path=output,
            args={"some_var": "value"},
        )
        assert result["success"] is False
        assert "name" in result["error"].lower()

    def test_missing_command_in_job(self, tmp_path):
        output = str(tmp_path / "out.yaml")
        result = create_workflow(
            name="my_flow",
            jobs=[{"name": "step1"}],
            output_path=output,
            args={"some_var": "value"},
        )
        assert result["success"] is False
        assert (
            "command" in result["error"].lower()
            or "script_path" in result["error"].lower()
        )

    def test_with_args_and_default_project(self, tmp_path):
        output = str(tmp_path / "out.yaml")
        result = create_workflow(
            name="templated_flow",
            jobs=[
                {"name": "step1", "command": ["echo", "{{ base_dir }}"]},
            ],
            output_path=output,
            args={"base_dir": "/data/exp"},
            default_project="myproject",
        )
        assert result["success"] is True

        with open(output) as f:
            data = yaml.safe_load(f)
        assert data["args"]["base_dir"] == "/data/exp"
        assert data["default_project"] == "myproject"


class TestGetConfig:
    """Test get_config tool."""

    @patch("srunx.config.get_config")
    def test_returns_config(self, mock_get_cfg):
        mock_config = MagicMock()
        mock_config.resources.model_dump.return_value = {
            "nodes": 1,
            "gpus_per_node": 0,
        }
        mock_config.environment.conda = "ml_env"
        mock_config.environment.venv = None
        mock_config.environment.env_vars = {}
        mock_config.log_dir = "logs"
        mock_config.work_dir = None
        mock_get_cfg.return_value = mock_config

        result = get_config()
        assert result["success"] is True
        assert result["resources"]["nodes"] == 1
        assert result["environment"]["conda"] == "ml_env"
        assert result["log_dir"] == "logs"

    def test_catches_exception(self):
        with patch(
            "srunx.config.get_config",
            side_effect=RuntimeError("config broken"),
        ):
            result = get_config()
            assert result["success"] is False
            assert "config broken" in result["error"]


class TestListSshProfiles:
    """Test list_ssh_profiles tool."""

    @patch("srunx.ssh.core.config.ConfigManager")
    def test_returns_profiles(self, mock_cm_cls):
        mock_cm = MagicMock()
        mock_profile = MagicMock()
        mock_profile.hostname = "dgx.example.com"
        mock_profile.username = "researcher"
        mock_profile.port = 22
        mock_profile.description = "DGX server"
        mock_mount = MagicMock()
        mock_mount.name = "project"
        mock_mount.local = "/home/user/project"
        mock_mount.remote = "/remote/project"
        mock_profile.mounts = [mock_mount]

        mock_cm.list_profiles.return_value = {"dgx": mock_profile}
        mock_cm.get_current_profile_name.return_value = "dgx"
        mock_cm_cls.return_value = mock_cm

        result = list_ssh_profiles()
        assert result["success"] is True
        assert result["count"] == 1
        assert result["current"] == "dgx"
        assert result["profiles"][0]["name"] == "dgx"
        assert result["profiles"][0]["hostname"] == "dgx.example.com"
        assert result["profiles"][0]["is_current"] is True
        assert len(result["profiles"][0]["mounts"]) == 1

    @patch("srunx.ssh.core.config.ConfigManager")
    def test_empty_profiles(self, mock_cm_cls):
        mock_cm = MagicMock()
        mock_cm.list_profiles.return_value = {}
        mock_cm.get_current_profile_name.return_value = None
        mock_cm_cls.return_value = mock_cm

        result = list_ssh_profiles()
        assert result["success"] is True
        assert result["count"] == 0
        assert result["profiles"] == []

    def test_catches_exception(self):
        with patch(
            "srunx.ssh.core.config.ConfigManager",
            side_effect=RuntimeError("config error"),
        ):
            result = list_ssh_profiles()
            assert result["success"] is False
            assert "config error" in result["error"]


class TestListWorkflows:
    """Test list_workflows tool."""

    def test_finds_workflows(self, tmp_path):
        workflow = {"name": "my_flow", "jobs": [{"name": "step1", "command": ["echo"]}]}
        wf_file = tmp_path / "flow.yaml"
        with open(wf_file, "w") as f:
            yaml.dump(workflow, f)

        # Non-workflow yaml should be skipped
        other_file = tmp_path / "config.yaml"
        with open(other_file, "w") as f:
            yaml.dump({"key": "value"}, f)

        result = list_workflows(directory=str(tmp_path))
        assert result["success"] is True
        assert result["count"] == 1
        assert result["workflows"][0]["name"] == "my_flow"

    def test_skips_hidden_dirs(self, tmp_path):
        hidden = tmp_path / ".hidden"
        hidden.mkdir()
        wf_file = hidden / "flow.yaml"
        with open(wf_file, "w") as f:
            yaml.dump(
                {"name": "hidden", "jobs": [{"name": "a", "command": ["echo"]}]}, f
            )

        result = list_workflows(directory=str(tmp_path))
        assert result["success"] is True
        assert result["count"] == 0

    def test_empty_directory(self, tmp_path):
        result = list_workflows(directory=str(tmp_path))
        assert result["success"] is True
        assert result["count"] == 0

    def test_nonexistent_directory(self):
        result = list_workflows(directory="/nonexistent/dir/abc123")
        assert result["success"] is True
        assert result["count"] == 0
