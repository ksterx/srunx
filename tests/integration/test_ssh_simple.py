"""Simplified integration tests for SSH functionality."""

import tempfile
from pathlib import Path
from unittest.mock import Mock, patch

from srunx.ssh.core.client import SlurmJob, SSHSlurmClient
from srunx.ssh.core.config import ConfigManager, ServerProfile


class TestSimpleSSHIntegration:
    """Test basic SSH integration scenarios."""

    def test_client_initialization_and_context_manager(self):
        """Test basic client setup and context manager."""
        client = SSHSlurmClient(
            hostname="test.example.com", username="testuser", key_filename="/test/key"
        )

        assert client.hostname == "test.example.com"
        assert client.username == "testuser"
        assert client.key_filename == "/test/key"

        # Test with mock connection
        with patch.object(client, "connect", return_value=True):
            with patch.object(client, "disconnect"):
                mock_ssh = Mock()
                mock_sftp = Mock()
                client.ssh_client = mock_ssh
                client.sftp_client = mock_sftp

                with client as ctx:
                    assert ctx is client
                    assert client.ssh_client == mock_ssh
                    assert client.sftp_client == mock_sftp

    def test_profile_creation_and_usage(self):
        """Test creating and using SSH profiles."""
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".json", delete=False
        ) as config_file:
            config_path = config_file.name

        try:
            config_manager = ConfigManager(config_path)

            # Create a profile
            profile = ServerProfile(
                hostname="dgx.example.com",
                username="researcher",
                key_filename="/home/user/.ssh/dgx_key",
                port=22,
                description="Test DGX server",
                env_vars={"CUDA_VISIBLE_DEVICES": "0,1,2,3"},
            )

            config_manager.add_profile("dgx_test", profile)

            # Verify profile was saved
            loaded_profile = config_manager.get_profile("dgx_test")
            assert loaded_profile is not None
            assert loaded_profile.hostname == "dgx.example.com"
            assert loaded_profile.username == "researcher"
            assert loaded_profile.env_vars["CUDA_VISIBLE_DEVICES"] == "0,1,2,3"

            # Test profile listing
            profiles = config_manager.list_profiles()
            assert "dgx_test" in profiles
            assert len(profiles) == 1

        finally:
            Path(config_path).unlink(missing_ok=True)

    def test_job_submission_workflow(self):
        """Test basic job submission workflow."""
        client = SSHSlurmClient(hostname="test.example.com", username="testuser")

        # Mock all required components
        client.ssh_client = Mock()
        client.sftp_client = Mock()

        # Mock file operations
        client._write_remote_file = Mock()
        client.execute_command = Mock(return_value=("", "", 0))
        client.validate_remote_script = Mock(return_value=(True, ""))
        client._get_slurm_command = Mock(return_value="sbatch")
        client._execute_slurm_command = Mock(
            return_value=("Submitted batch job 12345", "", 0)
        )

        # Submit a job
        script_content = """#!/bin/bash
#SBATCH --job-name=test_job
echo "Hello from SLURM!"
"""

        job = client.submit_sbatch_job(script_content, job_name="test_job")

        assert job is not None
        assert job.job_id == "12345"
        assert job.name == "test_job"

        # Verify methods were called
        client._write_remote_file.assert_called_once()
        client.execute_command.assert_called()
        client._execute_slurm_command.assert_called_once()

    def test_job_status_monitoring(self):
        """Test job status monitoring functionality."""
        client = SSHSlurmClient(hostname="test.example.com", username="testuser")

        # Mock status progression
        status_sequence = ["PENDING", "RUNNING", "COMPLETED"]

        def mock_execute_slurm_command(cmd):
            if not hasattr(mock_execute_slurm_command, "call_count"):
                mock_execute_slurm_command.call_count = 0

            status = status_sequence[
                min(mock_execute_slurm_command.call_count, len(status_sequence) - 1)
            ]
            mock_execute_slurm_command.call_count += 1

            return (f"12345 {status}", "", 0)

        client._execute_slurm_command = mock_execute_slurm_command

        # Create a job for monitoring
        job = SlurmJob(job_id="12345", name="test_job")

        # Test individual status checks
        status1 = client.get_job_status("12345")
        assert status1 == "PENDING"

        status2 = client.get_job_status("12345")
        assert status2 == "RUNNING"

        status3 = client.get_job_status("12345")
        assert status3 == "COMPLETED"

    def test_error_handling_scenarios(self):
        """Test various error handling scenarios."""
        client = SSHSlurmClient(hostname="test.example.com", username="testuser")

        # Test SLURM command not found
        client._execute_slurm_command = Mock(
            return_value=("", "sbatch: command not found", 127)
        )

        with patch.object(client, "logger") as mock_logger:
            client._handle_slurm_error("sbatch", "sbatch: command not found", 127)

            # Should log helpful error messages
            assert mock_logger.error.call_count >= 2
            error_messages = [call[0][0] for call in mock_logger.error.call_args_list]
            assert any("SLURM commands not found" in msg for msg in error_messages)

    def test_file_upload_basic(self):
        """Test basic file upload functionality."""
        client = SSHSlurmClient(hostname="test.example.com", username="testuser")

        # Mock SFTP client
        client.sftp_client = Mock()
        client.sftp_client.put = Mock()
        client.execute_command = Mock(return_value=("", "", 0))

        # Create temporary test file
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".py", delete=False
        ) as test_file:
            test_file.write("print('Hello, World!')")
            test_path = test_file.name

        try:
            remote_path = client.upload_file(test_path)

            # Verify upload was attempted
            assert remote_path.startswith("/tmp/srunx/")
            assert Path(test_path).stem in remote_path
            client.sftp_client.put.assert_called_once()

        finally:
            Path(test_path).unlink()

    def test_environment_variables_handling(self):
        """Test environment variable handling."""
        env_vars = {
            "CUDA_VISIBLE_DEVICES": "0,1,2,3",
            "WANDB_API_KEY": "test_key_12345",
            "CUSTOM_VAR": "custom_value",
        }

        client = SSHSlurmClient(
            hostname="test.example.com", username="testuser", env_vars=env_vars
        )

        env_setup = client._get_slurm_env_setup()

        # Verify environment variables are included (with proper quoting)
        assert "export CUDA_VISIBLE_DEVICES='0,1,2,3'" in env_setup
        assert "export WANDB_API_KEY='test_key_12345'" in env_setup
        assert "export CUSTOM_VAR='custom_value'" in env_setup

    def test_profile_environment_variables(self):
        """Test profile-based environment variable management."""
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".json", delete=False
        ) as config_file:
            config_path = config_file.name

        try:
            config_manager = ConfigManager(config_path)

            # Add profile with environment variables
            profile = ServerProfile(
                hostname="ml.example.com",
                username="researcher",
                key_filename="/keys/ml_key",
                env_vars={
                    "WANDB_PROJECT": "deep_learning",
                    "CUDA_VISIBLE_DEVICES": "0,1",
                },
            )

            config_manager.add_profile("ml_server", profile)

            # Test profile retrieval and environment variables
            loaded_profile = config_manager.get_profile("ml_server")
            assert loaded_profile.env_vars["WANDB_PROJECT"] == "deep_learning"
            assert loaded_profile.env_vars["CUDA_VISIBLE_DEVICES"] == "0,1"

            # Test profile update (basic functionality)
            success = config_manager.update_profile(
                "ml_server", description="Updated ML server"
            )
            assert success

            updated_profile = config_manager.get_profile("ml_server")
            assert updated_profile.description == "Updated ML server"
            assert updated_profile.env_vars["WANDB_PROJECT"] == "deep_learning"

        finally:
            Path(config_path).unlink(missing_ok=True)
