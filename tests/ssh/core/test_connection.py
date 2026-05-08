"""Tests for srunx.ssh.core.connection.SSHConnection."""

from unittest.mock import Mock

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


class TestExecuteWithEnvironment:
    def test_wraps_in_login_shell(self, client):
        client.connection.execute_command = Mock(return_value=("output", "error", 0))

        result = client.connection._execute_with_environment("echo hello")

        client.connection.execute_command.assert_called_once_with(
            "bash -l -c 'echo hello'"
        )
        assert result == ("output", "error", 0)
