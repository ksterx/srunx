"""Tests for srunx.ssh.core.file_manager.RemoteFileManager."""

import os
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, Mock

import pytest

from srunx.ssh.core.client import SSHSlurmClient


@pytest.fixture
def client():
    c = SSHSlurmClient(
        hostname="test.example.com",
        username="testuser",
        key_filename="/test/key",
    )
    c.connection.ssh_client = Mock()
    c.connection.sftp_client = Mock()
    c.connection.proxy_client = None
    return c


class TestWriteRemoteFile:
    def test_success(self, client):
        mock_file = MagicMock()
        client.connection.sftp_client.open.return_value.__enter__ = Mock(
            return_value=mock_file
        )
        client.connection.sftp_client.open.return_value.__exit__ = Mock(
            return_value=None
        )

        client.files.write_remote_file("/remote/path/file.txt", "content")

        client.connection.sftp_client.open.assert_called_once_with(
            "/remote/path/file.txt", "w"
        )
        mock_file.write.assert_called_once_with("content")

    def test_no_connection(self, client):
        client.connection.sftp_client = None

        with pytest.raises(ConnectionError, match="SFTP client is not connected"):
            client.files.write_remote_file("/remote/path/file.txt", "content")


class TestUploadFile:
    def test_local_file_upload(self, client):
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".sh", delete=False
        ) as temp_file:
            temp_file.write("#!/bin/bash\necho 'test'")
            temp_path = temp_file.name

        try:
            client.connection.sftp_client.put = Mock()
            client.connection.execute_command = Mock(return_value=("", "", 0))

            result = client.files.upload_file(temp_path)

            assert result.startswith("/tmp/srunx/")
            filename_base = Path(temp_path).stem
            assert filename_base in result
            client.connection.sftp_client.put.assert_called_once()
        finally:
            os.unlink(temp_path)


class TestShellQuoting:
    """File-manager methods that take a remote path must shlex-quote it."""

    def test_cleanup_file_quotes_path(self, client):
        client.connection.execute_command = Mock(return_value=("", "", 0))
        client.files.cleanup_file("/tmp/safe file; rm -rf /")
        call_args = client.connection.execute_command.call_args[0][0]
        assert call_args == "rm -f '/tmp/safe file; rm -rf /'"

    def test_file_exists_quotes_path(self, client):
        client.connection.execute_command = Mock(return_value=("exists", "", 0))
        client.files.file_exists("/tmp/test; echo pwned")
        call_args = client.connection.execute_command.call_args[0][0]
        assert "test -f '/tmp/test; echo pwned'" in call_args

    def test_validate_remote_script_quotes_path(self, client):
        import shlex

        client.connection.execute_command = Mock(
            side_effect=[
                ("readable", "", 0),
                ("executable", "", 0),
                ("100", "", 0),
            ]
        )
        client.files.file_exists = Mock(return_value=True)

        path = "/tmp/evil path; cat /etc/passwd"
        quoted = shlex.quote(path)
        client.files.validate_remote_script(path)

        # Every execute_command call must contain the shlex-quoted path.
        for call in client.connection.execute_command.call_args_list:
            cmd = call[0][0]
            assert quoted in cmd, f"Unquoted path in command: {cmd}"
