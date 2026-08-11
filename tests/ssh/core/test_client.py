"""Tests for srunx.ssh.core.client.SSHSlurmClient.

After the facade-deduplication refactor, the only public API ``client.py``
exposes is the lifecycle (``connect`` / ``disconnect`` / ``__enter__`` /
``__exit__`` / ``test_connection``) plus :meth:`sync_project`. Tests for
component methods live alongside their components — see
``test_slurm.py``, ``test_file_manager.py``, ``test_log_reader.py``,
``test_connection.py``.
"""

from unittest.mock import MagicMock, patch

import pytest

from srunx.ssh.core.client import SSHSlurmClient


class TestInit:
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
            verbose=True,
        )

        assert client.hostname == "dgx.example.com"
        assert client.username == "researcher"
        assert client.password == "secret"
        assert client.key_filename == "/home/user/.ssh/dgx_key"
        assert client.port == 2222
        assert client.proxy_jump == "bastion.com"
        assert client.ssh_config_path == "/custom/ssh/config"

    def test_components_constructed(self):
        """The facade owns one instance of each component, sharing connection state."""
        client = SSHSlurmClient(hostname="t", username="u")
        # Same connection threaded into every component → mocks at
        # ``client.connection.execute_command`` propagate through
        # files / slurm / logs.
        assert client.files._conn is client.connection
        assert client.slurm._conn is client.connection
        assert client.slurm._files is client.files
        assert client.logs._conn is client.connection
        assert client.logs._slurm is client.slurm


class TestContextManager:
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


class TestSyncProjectDeleteDefault:
    """``sync_project`` must not re-introduce mirror-by-default.

    :meth:`RsyncClient.push` defaults to ``delete=False`` so that no caller
    inherits mirror semantics by accident. A wrapper that passes
    ``delete=True`` of its own accord defeats that for every one of its
    callers — which is precisely how remote-only checkpoints and job logs
    were deleted before. This pins the wrapper to the additive default.
    """

    def _client(self) -> SSHSlurmClient:
        with (
            patch("srunx.sync.rsync.shutil.which", return_value="/usr/bin/rsync"),
            patch(
                "srunx.sync.rsync.subprocess.run",
                return_value=MagicMock(stdout="--protect-args --mkpath", stderr=""),
            ),
            patch(
                "srunx.ssh.core.client.subprocess.run",
                return_value=MagicMock(returncode=0, stdout="/home/user/proj\n"),
            ),
        ):
            return SSHSlurmClient(
                hostname="server", username="user", key_filename="~/.ssh/id_rsa"
            )

    def test_additive_by_default(self):
        client = self._client()

        with patch.object(client._rsync_client, "push") as mock_push:
            mock_push.return_value = MagicMock(returncode=0, stdout="", stderr="")
            # Explicit paths: project-root detection shells out to git, which
            # the test suite's isolation fixture blocks.
            client.sync_project(local_path="/local/proj", remote_path="~/proj/")

        assert mock_push.call_args.kwargs["delete"] is False

    def test_mirror_still_available_on_request(self):
        client = self._client()

        with patch.object(client._rsync_client, "push") as mock_push:
            mock_push.return_value = MagicMock(returncode=0, stdout="", stderr="")
            client.sync_project(
                local_path="/local/proj", remote_path="~/proj/", delete=True
            )

        assert mock_push.call_args.kwargs["delete"] is True
