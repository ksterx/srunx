"""Tests for SSH component classes (standalone usage)."""

from unittest.mock import MagicMock, patch

import pytest

from srunx.ssh.core.connection import SSHConnection
from srunx.ssh.core.file_manager import RemoteFileManager
from srunx.ssh.core.log_reader import RemoteLogReader
from srunx.ssh.core.slurm import SlurmRemoteClient
from srunx.ssh.core.utils import detect_project_root, quote_shell_path, sanitize_job_id


class TestUtils:
    """Test module-level utility functions."""

    def test_quote_shell_path(self):
        assert quote_shell_path("/tmp/test") == "/tmp/test"
        assert quote_shell_path("/tmp/has space") == "'/tmp/has space'"

    def test_sanitize_job_id_int(self):
        assert sanitize_job_id(12345) == "12345"

    def test_sanitize_job_id_str(self):
        assert sanitize_job_id("12345") == "12345"

    def test_sanitize_job_id_invalid(self):
        with pytest.raises(ValueError):
            sanitize_job_id("abc")

    @patch("srunx.ssh.core.utils.subprocess.run")
    def test_detect_project_root_with_git(self, mock_run):
        mock_run.return_value = MagicMock(returncode=0, stdout="/home/user/project\n")
        assert detect_project_root() == "/home/user/project"

    @patch("srunx.ssh.core.utils.subprocess.run", side_effect=FileNotFoundError)
    def test_detect_project_root_fallback(self, _mock_run):
        result = detect_project_root()
        assert isinstance(result, str)


class TestSSHConnection:
    """Test SSHConnection standalone instantiation."""

    def test_init(self):
        conn = SSHConnection(hostname="test.example.com", username="user")
        assert conn.hostname == "test.example.com"
        assert conn.username == "user"
        assert conn.ssh_client is None
        assert conn.sftp_client is None
        assert conn.port == 22
        assert conn.verbose is False

    def test_init_custom_params(self):
        conn = SSHConnection(
            hostname="h",
            username="u",
            port=2222,
            temp_dir="/custom/tmp",
            verbose=True,
        )
        assert conn.port == 2222
        assert conn.temp_dir == "/custom/tmp"
        assert conn.verbose is True

    def test_execute_command_requires_connection(self):
        conn = SSHConnection(hostname="h", username="u")
        with pytest.raises(ConnectionError):
            conn.execute_command("echo hello")

    def test_context_manager(self):
        conn = SSHConnection(hostname="h", username="u")
        with (
            patch.object(conn, "connect"),
            patch.object(conn, "disconnect") as mock_disc,
        ):
            with conn:
                pass
            mock_disc.assert_called_once()


class TestRemoteFileManager:
    """Test RemoteFileManager standalone instantiation."""

    def test_init(self):
        conn = MagicMock(spec=SSHConnection)
        conn.hostname = "h"
        conn.username = "u"
        conn.key_filename = None
        fm = RemoteFileManager(conn)
        assert fm._conn is conn

    def test_file_exists_delegates_to_connection(self):
        conn = MagicMock(spec=SSHConnection)
        conn.hostname = "h"
        conn.username = "u"
        conn.key_filename = None
        conn.execute_command.return_value = ("exists\n", "", 0)
        fm = RemoteFileManager(conn)
        result = fm.file_exists("/tmp/test.sh")
        assert result is True
        conn.execute_command.assert_called_once()

    def test_cleanup_file_delegates(self):
        conn = MagicMock(spec=SSHConnection)
        conn.hostname = "h"
        conn.username = "u"
        conn.key_filename = None
        conn.verbose = False
        conn.execute_command.return_value = ("", "", 0)
        fm = RemoteFileManager(conn)
        fm.cleanup_file("/tmp/test.sh")
        conn.execute_command.assert_called_once()


class TestSlurmRemoteClient:
    """Test SlurmRemoteClient standalone instantiation."""

    def test_init(self):
        conn = MagicMock(spec=SSHConnection)
        fm = MagicMock(spec=RemoteFileManager)
        slurm = SlurmRemoteClient(conn, fm)
        assert slurm._conn is conn
        assert slurm._files is fm
        assert slurm._slurm_path is None

    def test_get_slurm_env_setup(self):
        conn = MagicMock(spec=SSHConnection)
        conn.custom_env_vars = {"FOO": "bar"}
        fm = MagicMock(spec=RemoteFileManager)
        slurm = SlurmRemoteClient(conn, fm)
        setup = slurm._get_slurm_env_setup()
        assert "FOO" in setup
        assert "bar" in setup

    def test_handle_slurm_error_logs_without_raising(self):
        conn = MagicMock(spec=SSHConnection)
        fm = MagicMock(spec=RemoteFileManager)
        slurm = SlurmRemoteClient(conn, fm)
        # Should not raise — only logs error details
        slurm._handle_slurm_error("sbatch", "command not found", 127)

    def test_get_job_status_uses_shared_three_stage_fallback(self):
        """sacct empty + squeue empty -> scontrol parses COMPLETED."""
        conn = MagicMock(spec=SSHConnection)
        fm = MagicMock(spec=RemoteFileManager)
        slurm = SlurmRemoteClient(conn, fm)
        scontrol_out = (
            "JobId=42 JobName=train JobState=COMPLETED Reason=None ExitCode=0:0"
        )
        responses = iter(
            [
                ("", "", 0),  # sacct empty
                ("", "", 0),  # squeue empty
                (scontrol_out, "", 0),  # scontrol hit
            ]
        )
        # Bypass the real execute path — the helper only needs the callable.
        slurm.execute_slurm_command = lambda cmd: next(responses)  # type: ignore[method-assign]
        assert slurm.get_job_status("42") == "COMPLETED"

    def test_get_job_status_rejects_invalid_job_id(self):
        conn = MagicMock(spec=SSHConnection)
        fm = MagicMock(spec=RemoteFileManager)
        slurm = SlurmRemoteClient(conn, fm)
        # If the helper touched execute_slurm_command it would raise since
        # we never stubbed it — asserting "ERROR" confirms the regex guard.
        assert slurm.get_job_status("not; an id") == "ERROR"


class TestRemoteLogReader:
    """Test RemoteLogReader standalone instantiation."""

    def test_init(self):
        conn = MagicMock(spec=SSHConnection)
        slurm = MagicMock(spec=SlurmRemoteClient)
        reader = RemoteLogReader(conn, slurm)
        assert reader._conn is conn
        assert reader._slurm is slurm
