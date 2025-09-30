import os
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

import pytest

from srunx.ssh.core.client import SlurmJob, SSHSlurmClient


class TestSlurmJob:
    def test_create_basic_job(self):
        job = SlurmJob(job_id="12345", name="test_job")

        assert job.job_id == "12345"
        assert job.name == "test_job"
        assert job.status == "UNKNOWN"
        assert job.output_file is None
        assert job.error_file is None
        assert job.script_path is None
        assert job.is_local_script is False
        assert job._cleanup is False

    def test_create_full_job(self):
        job = SlurmJob(
            job_id="67890",
            name="ml_training",
            status="RUNNING",
            output_file="/path/to/output.out",
            error_file="/path/to/error.err",
            script_path="/path/to/script.sh",
            is_local_script=True,
            _cleanup=True,
        )

        assert job.job_id == "67890"
        assert job.name == "ml_training"
        assert job.status == "RUNNING"
        assert job.output_file == "/path/to/output.out"
        assert job.error_file == "/path/to/error.err"
        assert job.script_path == "/path/to/script.sh"
        assert job.is_local_script is True
        assert job._cleanup is True


class TestSSHSlurmClient:
    @pytest.fixture
    def mock_ssh_client(self):
        """Create a mock SSH client"""
        client = SSHSlurmClient(
            hostname="test.example.com", username="testuser", key_filename="/test/key"
        )
        client.ssh_client = Mock()
        client.sftp_client = Mock()
        client.proxy_client = None
        return client

    def test_init_basic(self):
        client = SSHSlurmClient(hostname="test.example.com", username="testuser")

        assert client.hostname == "test.example.com"
        assert client.username == "testuser"
        assert client.password is None
        assert client.key_filename is None
        assert client.port == 22
        assert client.proxy_jump is None
        assert client.ssh_config_path is None

    def test_init_with_all_params(self):
        client = SSHSlurmClient(
            hostname="dgx.example.com",
            username="researcher",
            password="secret",
            key_filename="/home/user/.ssh/dgx_key",
            port=2222,
            proxy_jump="bastion.com",
            ssh_config_path="/custom/ssh/config",
            env_vars={"CUDA_VISIBLE_DEVICES": "0,1"},
            verbose=True,
        )

        assert client.hostname == "dgx.example.com"
        assert client.username == "researcher"
        assert client.password == "secret"
        assert client.key_filename == "/home/user/.ssh/dgx_key"
        assert client.port == 2222
        assert client.proxy_jump == "bastion.com"
        assert client.ssh_config_path == "/custom/ssh/config"

    def test_handle_slurm_error_command_not_found(self, mock_ssh_client):
        with patch.object(mock_ssh_client, "logger") as mock_logger:
            mock_ssh_client._handle_slurm_error(
                "sbatch", "sbatch: command not found", 127
            )

            assert mock_logger.error.call_count >= 2
            # Check that helpful error message about SLURM installation is provided
            error_calls = [call[0][0] for call in mock_logger.error.call_args_list]
            assert any("SLURM commands not found" in msg for msg in error_calls)

    def test_handle_slurm_error_permission_denied(self, mock_ssh_client):
        with patch.object(mock_ssh_client, "logger") as mock_logger:
            mock_ssh_client._handle_slurm_error("sbatch", "Permission denied", 1)

            error_calls = [call[0][0] for call in mock_logger.error.call_args_list]
            assert any("Permission denied" in msg for msg in error_calls)

    def test_handle_slurm_error_invalid_partition(self, mock_ssh_client):
        with patch.object(mock_ssh_client, "logger") as mock_logger:
            mock_ssh_client._handle_slurm_error(
                "sbatch", "Invalid partition specified", 1
            )

            error_calls = [call[0][0] for call in mock_logger.error.call_args_list]
            assert any("Invalid partition" in msg for msg in error_calls)

    def test_execute_with_environment(self, mock_ssh_client):
        mock_ssh_client.execute_command = Mock(return_value=("output", "error", 0))

        result = mock_ssh_client._execute_with_environment("echo hello")

        mock_ssh_client.execute_command.assert_called_once_with(
            'bash -l -c "echo hello"'
        )
        assert result == ("output", "error", 0)

    def test_context_manager_success(self):
        client = SSHSlurmClient(hostname="test.example.com", username="testuser")

        with patch.object(client, "connect", return_value=True):
            with patch.object(client, "disconnect"):
                with client as ctx:
                    assert ctx is client

    def test_context_manager_connection_failure(self):
        client = SSHSlurmClient(hostname="test.example.com", username="testuser")

        with patch.object(client, "connect", return_value=False):
            with pytest.raises(
                ConnectionError, match="Failed to establish SSH connection"
            ):
                with client:
                    pass

    def test_context_manager_disconnect_on_exit(self):
        client = SSHSlurmClient(hostname="test.example.com", username="testuser")

        with patch.object(client, "connect", return_value=True):
            with patch.object(client, "disconnect") as mock_disconnect:
                with client:
                    pass
                mock_disconnect.assert_called_once()

    def test_write_remote_file_success(self, mock_ssh_client):
        mock_file = MagicMock()
        mock_ssh_client.sftp_client.open.return_value.__enter__ = Mock(
            return_value=mock_file
        )
        mock_ssh_client.sftp_client.open.return_value.__exit__ = Mock(return_value=None)

        mock_ssh_client._write_remote_file("/remote/path/file.txt", "content")

        mock_ssh_client.sftp_client.open.assert_called_once_with(
            "/remote/path/file.txt", "w"
        )
        mock_file.write.assert_called_once_with("content")

    def test_write_remote_file_no_connection(self, mock_ssh_client):
        mock_ssh_client.sftp_client = None

        with pytest.raises(ConnectionError, match="SFTP client is not connected"):
            mock_ssh_client._write_remote_file("/remote/path/file.txt", "content")

    def test_upload_file_local(self, mock_ssh_client):
        # Create a temporary real file for testing
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".sh", delete=False
        ) as temp_file:
            temp_file.write("#!/bin/bash\necho 'test'")
            temp_path = temp_file.name

        try:
            # Mock SFTP upload
            mock_ssh_client.sftp_client.put = Mock()
            mock_ssh_client.execute_command = Mock(return_value=("", "", 0))

            result = mock_ssh_client.upload_file(temp_path)

            assert result.startswith("/tmp/srunx/")
            # Check that the base filename (without directory) is in the result
            filename_base = Path(temp_path).stem  # filename without extension
            assert filename_base in result
            mock_ssh_client.sftp_client.put.assert_called_once()
        finally:
            os.unlink(temp_path)

    def test_get_job_status_completed(self, mock_ssh_client):
        # Mock the sacct command to return completed status
        mock_ssh_client._execute_slurm_command = Mock(
            return_value=("12345 COMPLETED", "", 0)
        )

        status = mock_ssh_client.get_job_status("12345")

        assert status == "COMPLETED"

    def test_get_job_status_not_found(self, mock_ssh_client):
        mock_ssh_client._execute_slurm_command = Mock(
            return_value=("", "Job not found", 1)
        )

        status = mock_ssh_client.get_job_status("99999")

        assert status == "NOT_FOUND"

    def test_monitor_job_completion(self, mock_ssh_client):
        job = SlurmJob(job_id="12345", name="test_job")

        # Mock job completion after 2 polls
        mock_ssh_client.get_job_status = Mock(side_effect=["RUNNING", "COMPLETED"])

        with patch("time.sleep"):
            result = mock_ssh_client.monitor_job(job, poll_interval=1)

        assert result.status == "COMPLETED"
        assert mock_ssh_client.get_job_status.call_count == 2

    def test_monitor_job_timeout(self, mock_ssh_client):
        job = SlurmJob(job_id="12345", name="test_job")

        # Mock job that never completes
        mock_ssh_client.get_job_status = Mock(return_value="RUNNING")

        with patch("time.sleep"):
            with patch("time.time", side_effect=[0, 1, 2, 3]):  # Simulate time passing
                result = mock_ssh_client.monitor_job(job, poll_interval=1, timeout=2)

        assert result.status == "TIMEOUT"

    def test_submit_sbatch_job_success(self, mock_ssh_client):
        script_content = "#!/bin/bash\necho 'Hello World'"

        # Mock all necessary methods
        mock_ssh_client._write_remote_file = Mock()
        mock_ssh_client.execute_command = Mock(return_value=("", "", 0))
        mock_ssh_client.validate_remote_script = Mock(return_value=(True, ""))
        mock_ssh_client._get_slurm_command = Mock(return_value="sbatch")
        mock_ssh_client._execute_slurm_command = Mock(
            return_value=("Submitted batch job 12345", "", 0)
        )

        job = mock_ssh_client.submit_sbatch_job(script_content, job_name="test_job")

        assert job is not None
        assert job.job_id == "12345"
        assert job.name == "test_job"

    def test_submit_sbatch_job_failure(self, mock_ssh_client):
        script_content = "#!/bin/bash\necho 'Hello World'"

        # Mock all necessary methods for failure case
        mock_ssh_client._write_remote_file = Mock()
        mock_ssh_client.execute_command = Mock(return_value=("", "", 0))
        mock_ssh_client.validate_remote_script = Mock(return_value=(True, ""))
        mock_ssh_client._get_slurm_command = Mock(return_value="sbatch")
        mock_ssh_client._execute_slurm_command = Mock(
            return_value=("", "sbatch: error: invalid option", 1)
        )

        job = mock_ssh_client.submit_sbatch_job(script_content, job_name="test_job")

        assert job is None

    def test_cleanup_job_files(self, mock_ssh_client):
        job = SlurmJob(
            job_id="12345",
            name="test_job",
            script_path="/tmp/srunx/test_script.sh",
            is_local_script=True,
            _cleanup=True,
        )

        mock_ssh_client.cleanup_file = Mock()

        mock_ssh_client.cleanup_job_files(job)

        mock_ssh_client.cleanup_file.assert_called_once_with(
            "/tmp/srunx/test_script.sh"
        )

    def test_cleanup_job_files_no_cleanup(self, mock_ssh_client):
        job = SlurmJob(
            job_id="12345",
            name="test_job",
            script_path="/tmp/srunx/test_script.sh",
            _cleanup=False,
        )

        mock_ssh_client.cleanup_file = Mock()

        mock_ssh_client.cleanup_job_files(job)

        mock_ssh_client.cleanup_file.assert_not_called()
