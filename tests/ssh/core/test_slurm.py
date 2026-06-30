"""Tests for srunx.ssh.core.slurm.SlurmRemoteClient.

Uses the facade as a fixture for convenience (it builds the component
graph for us) but exercises the slurm component directly via
``client.slurm.X``. Mocks target the call site:

* ``client.slurm.execute_slurm_command`` for SLURM CLI invocations.
* ``client.connection.execute_command`` for raw shell commands.
* ``client.files.write_remote_file`` / ``validate_remote_script`` for
  file ops the slurm component calls back into.
"""

import shlex
from unittest.mock import Mock, patch

import pytest

from srunx.ssh.core.client import SSHSlurmClient
from srunx.ssh.core.client_types import SlurmJob


@pytest.fixture
def client():
    """Facade with a mocked SSH transport — components share the connection."""
    c = SSHSlurmClient(
        hostname="test.example.com",
        username="testuser",
        key_filename="/test/key",
    )
    c.connection.ssh_client = Mock()
    c.connection.sftp_client = Mock()
    c.connection.proxy_client = None
    return c


class TestHandleSlurmError:
    def test_command_not_found(self, client):
        with patch.object(client.slurm, "logger") as mock_logger:
            client.slurm._handle_slurm_error("sbatch", "sbatch: command not found", 127)
            assert mock_logger.error.call_count >= 2
            error_calls = [c[0][0] for c in mock_logger.error.call_args_list]
            assert any("SLURM commands not found" in msg for msg in error_calls)

    def test_permission_denied(self, client):
        with patch.object(client.slurm, "logger") as mock_logger:
            client.slurm._handle_slurm_error("sbatch", "Permission denied", 1)
            error_calls = [c[0][0] for c in mock_logger.error.call_args_list]
            assert any("Permission denied" in msg for msg in error_calls)

    def test_invalid_partition(self, client):
        with patch.object(client.slurm, "logger") as mock_logger:
            client.slurm._handle_slurm_error("sbatch", "Invalid partition specified", 1)
            error_calls = [c[0][0] for c in mock_logger.error.call_args_list]
            assert any("Invalid partition" in msg for msg in error_calls)


class TestGetJobStatus:
    def test_completed(self, client):
        client.slurm.execute_slurm_command = Mock(
            return_value=("12345 COMPLETED", "", 0)
        )
        assert client.slurm.get_job_status("12345") == "COMPLETED"

    def test_not_found(self, client):
        client.slurm.execute_slurm_command = Mock(return_value=("", "Job not found", 1))
        assert client.slurm.get_job_status("99999") == "NOT_FOUND"


class TestMonitorJob:
    def test_completion(self, client):
        job = SlurmJob(job_id="12345", name="test_job")
        client.slurm.get_job_status = Mock(side_effect=["RUNNING", "COMPLETED"])

        with patch("time.sleep"):
            result = client.slurm.monitor_job(job, poll_interval=1)

        assert result.status == "COMPLETED"
        assert client.slurm.get_job_status.call_count == 2

    def test_timeout(self, client):
        job = SlurmJob(job_id="12345", name="test_job")
        client.slurm.get_job_status = Mock(return_value="RUNNING")

        with patch("time.sleep"):
            with patch("time.time", side_effect=[0, 1, 2, 3]):
                result = client.slurm.monitor_job(job, poll_interval=1, timeout=2)

        assert result.status == "TIMEOUT"


class TestSubmitSbatchJob:
    def test_success(self, client):
        script_content = "#!/bin/bash\necho 'Hello World'"
        client.files.write_remote_file = Mock()
        client.connection.execute_command = Mock(return_value=("", "", 0))
        client.files.validate_remote_script = Mock(return_value=(True, ""))
        client.slurm._get_slurm_command = Mock(return_value="sbatch")
        client.slurm.execute_slurm_command = Mock(
            return_value=("Submitted batch job 12345", "", 0)
        )

        job = client.slurm.submit_sbatch_job(script_content, job_name="test_job")

        assert job is not None
        assert job.job_id == "12345"
        assert job.name == "test_job"

    def test_failure(self, client):
        script_content = "#!/bin/bash\necho 'Hello World'"
        client.files.write_remote_file = Mock()
        client.connection.execute_command = Mock(return_value=("", "", 0))
        client.files.validate_remote_script = Mock(return_value=(True, ""))
        client.slurm._get_slurm_command = Mock(return_value="sbatch")
        client.slurm.execute_slurm_command = Mock(
            return_value=("", "sbatch: error: invalid option", 1)
        )

        job = client.slurm.submit_sbatch_job(script_content, job_name="test_job")
        assert job is None


class TestJobEnvPropagation:
    """AC-7: job env_vars ride into the remote env prefix + --export=ALL.

    Mocks ``connection.execute_command`` so we can inspect the *full* shell
    command (env prefix + sbatch) that gets sent to the remote — the env
    prefix is assembled in ``execute_slurm_command`` / ``_get_slurm_env_setup``,
    which we exercise for real here.
    """

    def _capture_remote_command(self, client):
        captured: dict[str, str] = {}

        def fake_execute(cmd: str):
            captured["cmd"] = cmd
            # execute_slurm_command wraps the real command in
            # ``bash -l -c '<inner>'``; the inner command's own single quotes
            # are re-escaped by that outer wrapper. Unwrap with shlex so
            # assertions see the de-escaped command actually run on the remote.
            try:
                parts = shlex.split(cmd)
                captured["inner"] = parts[-1] if parts else cmd
            except ValueError:
                captured["inner"] = cmd
            return ("Submitted batch job 12345", "", 0)

        client.connection.execute_command = Mock(side_effect=fake_execute)
        client.slurm._get_slurm_command = Mock(return_value="sbatch")
        return captured

    def test_temp_upload_path_env_prefix_and_export_all(self, client):
        client.files.write_remote_file = Mock()
        client.files.validate_remote_script = Mock(return_value=(True, ""))
        captured = self._capture_remote_command(client)

        job = client.slurm.submit_sbatch_job(
            "#!/bin/bash\necho hi",
            job_name="test_job",
            job_env_vars={"FOO": "bar"},
        )

        assert job is not None and job.job_id == "12345"
        assert "export FOO='bar'" in captured["inner"]
        assert "--export=ALL" in captured["inner"]

    def test_in_place_path_env_prefix_and_export_all(self, client):
        client.files.validate_remote_script = Mock(return_value=(True, ""))
        captured = self._capture_remote_command(client)

        job = client.slurm.submit_remote_sbatch_file(
            "/remote/run.sh",
            job_name="test_job",
            job_env_vars={"FOO": "bar"},
        )

        assert job is not None and job.job_id == "12345"
        assert "export FOO='bar'" in captured["inner"]
        assert "--export=ALL" in captured["inner"]

    def test_env_value_single_quote_escaped(self, client):
        client.files.write_remote_file = Mock()
        client.files.validate_remote_script = Mock(return_value=(True, ""))
        captured = self._capture_remote_command(client)

        client.slurm.submit_sbatch_job(
            "#!/bin/bash\necho hi",
            job_name="test_job",
            job_env_vars={"MSG": "it's working"},
        )

        # Single quote escaped as '\'' within the single-quoted value.
        assert "export MSG='it'\\''s working'" in captured["inner"]

    def test_job_env_overrides_profile_custom_env(self, client):
        client.connection.custom_env_vars = {"FOO": "profile"}
        client.files.write_remote_file = Mock()
        client.files.validate_remote_script = Mock(return_value=(True, ""))
        captured = self._capture_remote_command(client)

        client.slurm.submit_sbatch_job(
            "#!/bin/bash\necho hi",
            job_name="test_job",
            job_env_vars={"FOO": "job"},
        )

        # Job key wins ({**profile, **job}); only the job value is exported.
        assert "export FOO='job'" in captured["inner"]
        assert "export FOO='profile'" not in captured["inner"]

    def test_no_job_env_omits_export_all_temp_upload(self, client):
        """No job env → no --export=ALL, so the script's export policy wins."""
        client.files.write_remote_file = Mock()
        client.files.validate_remote_script = Mock(return_value=(True, ""))
        captured = self._capture_remote_command(client)

        client.slurm.submit_sbatch_job("#!/bin/bash\necho hi", job_name="test_job")

        assert "--export=ALL" not in captured["inner"]

    def test_no_job_env_omits_export_all_in_place(self, client):
        """In-place path must preserve the user's own #SBATCH directives."""
        client.files.validate_remote_script = Mock(return_value=(True, ""))
        captured = self._capture_remote_command(client)

        client.slurm.submit_remote_sbatch_file("/remote/run.sh", job_name="test_job")

        assert "--export=ALL" not in captured["inner"]


class TestCleanupJobFiles:
    def test_local_script_with_cleanup(self, client):
        job = SlurmJob(
            job_id="12345",
            name="test_job",
            script_path="/tmp/srunx/test_script.sh",
            is_local_script=True,
            _cleanup=True,
        )
        client.files.cleanup_file = Mock()

        client.slurm.cleanup_job_files(job)

        client.files.cleanup_file.assert_called_once_with("/tmp/srunx/test_script.sh")

    def test_no_cleanup_flag(self, client):
        job = SlurmJob(
            job_id="12345",
            name="test_job",
            script_path="/tmp/srunx/test_script.sh",
            _cleanup=False,
        )
        client.files.cleanup_file = Mock()

        client.slurm.cleanup_job_files(job)
        client.files.cleanup_file.assert_not_called()
