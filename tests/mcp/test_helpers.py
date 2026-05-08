"""Tests for srunx.mcp.helpers."""

from unittest.mock import MagicMock, patch

import pytest

from srunx.mcp.helpers import (
    err,
    get_ssh_client,
    job_to_dict,
    ok,
    validate_job_id,
    validate_partition,
)


class TestOk:
    """Test ok helper."""

    def test_ok_no_data(self):
        result = ok()
        assert result == {"success": True}

    def test_ok_with_data(self):
        result = ok(data={"key": "value"})
        assert result == {"success": True, "data": {"key": "value"}}

    def test_ok_with_kwargs(self):
        result = ok(job_id="123", name="test")
        assert result == {"success": True, "job_id": "123", "name": "test"}

    def test_ok_with_data_and_kwargs(self):
        result = ok(data=[1, 2], extra="info")
        assert result == {"success": True, "data": [1, 2], "extra": "info"}

    def test_ok_none_data_excluded(self):
        result = ok(data=None)
        assert "data" not in result
        assert result == {"success": True}


class TestErr:
    """Test err helper."""

    def test_err_basic(self):
        result = err("something went wrong")
        assert result == {"success": False, "error": "something went wrong"}

    def test_err_empty_message(self):
        result = err("")
        assert result == {"success": False, "error": ""}


class TestValidateJobId:
    """Test validate_job_id."""

    def test_valid_simple_id(self):
        assert validate_job_id("12345") == "12345"

    def test_valid_array_job_id(self):
        assert validate_job_id("12345_1") == "12345_1"

    def test_invalid_alpha(self):
        with pytest.raises(ValueError, match="Invalid job ID"):
            validate_job_id("abc")

    def test_invalid_empty(self):
        with pytest.raises(ValueError, match="Invalid job ID"):
            validate_job_id("")

    def test_invalid_special_chars(self):
        with pytest.raises(ValueError, match="Invalid job ID"):
            validate_job_id("123; rm -rf /")

    def test_invalid_spaces(self):
        with pytest.raises(ValueError, match="Invalid job ID"):
            validate_job_id("123 456")

    def test_invalid_negative(self):
        with pytest.raises(ValueError, match="Invalid job ID"):
            validate_job_id("-1")

    def test_invalid_double_underscore(self):
        with pytest.raises(ValueError, match="Invalid job ID"):
            validate_job_id("123_1_2")


class TestValidatePartition:
    """Test validate_partition."""

    def test_valid_alphanumeric(self):
        assert validate_partition("gpu") == "gpu"

    def test_valid_with_underscore(self):
        assert validate_partition("gpu_high") == "gpu_high"

    def test_valid_with_hyphen(self):
        assert validate_partition("gpu-high") == "gpu-high"

    def test_valid_mixed(self):
        assert validate_partition("gpu_A100-2") == "gpu_A100-2"

    def test_invalid_semicolon(self):
        with pytest.raises(ValueError, match="Invalid partition name"):
            validate_partition("gpu; echo hacked")

    def test_invalid_spaces(self):
        with pytest.raises(ValueError, match="Invalid partition name"):
            validate_partition("gpu high")

    def test_invalid_empty(self):
        with pytest.raises(ValueError, match="Invalid partition name"):
            validate_partition("")


class TestJobToDict:
    """Test job_to_dict helper."""

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
        del job.script_path

        result = job_to_dict(job)
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
        job.partition = None
        job.user = None
        job.elapsed_time = None
        job.nodes = None
        job.nodelist = None
        job.cpus = None
        job.gpus = None

        result = job_to_dict(job)
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

        result = job_to_dict(job)
        assert result["script_path"] == "/path/to/script.sh"
        assert "command" not in result

    def test_job_without_status_attr(self):
        job = MagicMock(spec=[])
        job.name = "no_status"
        job.job_id = "111"
        result = job_to_dict(job)
        assert result["status"] == "UNKNOWN"


class TestGetSshClient:
    """Test get_ssh_client helper."""

    @patch("srunx.ssh.core.config.ConfigManager")
    def test_no_active_profile_raises(self, mock_cm_cls):
        mock_cm = MagicMock()
        mock_cm.get_current_profile_name.return_value = None
        mock_cm_cls.return_value = mock_cm

        with pytest.raises(RuntimeError, match="No active SSH profile"):
            get_ssh_client()

    @patch("srunx.ssh.core.config.ConfigManager")
    def test_profile_not_found_raises(self, mock_cm_cls):
        mock_cm = MagicMock()
        mock_cm.get_current_profile_name.return_value = "missing"
        mock_cm.get_profile.return_value = None
        mock_cm_cls.return_value = mock_cm

        with pytest.raises(RuntimeError, match="SSH profile 'missing' not found"):
            get_ssh_client()

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

        client = get_ssh_client()
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

        get_ssh_client()
        mock_client_cls.assert_called_once_with(
            hostname="direct.host.com",
            username="user1",
            key_filename="/path/key",
            port=22,
            proxy_jump=None,
            env_vars=None,
        )
