import socket
from unittest.mock import Mock, patch

import pytest

from srunx.ssh.core.ssh_config import SSHHost
from srunx.ssh.helpers.proxy_helper import (
    check_connectivity,
    main,
    suggest_port_forwarding,
)


class TestProxyHelper:
    def test_suggest_port_forwarding_with_proxy_jump(self, capsys):
        mock_host = SSHHost(
            hostname="dgx.example.com",
            user="researcher",
            port=22,
            identity_file="~/.ssh/dgx_key",
            proxy_jump="bastion.example.com",
        )

        with patch(
            "srunx.ssh.helpers.proxy_helper.get_ssh_config_host", return_value=mock_host
        ):
            suggest_port_forwarding("dgx1")

        captured = capsys.readouterr()
        assert "uses ProxyJump: bastion.example.com" in captured.out
        assert "SSH Port Forwarding:" in captured.out
        assert "srunx ssh profile add" in captured.out

    def test_suggest_port_forwarding_no_proxy_jump(self, capsys):
        mock_host = SSHHost(hostname="dgx.example.com", user="researcher")

        with patch(
            "srunx.ssh.helpers.proxy_helper.get_ssh_config_host", return_value=mock_host
        ):
            suggest_port_forwarding("dgx1")

        captured = capsys.readouterr()
        assert "does not use ProxyJump" in captured.out

    def test_suggest_port_forwarding_host_not_found(self, capsys):
        with patch(
            "srunx.ssh.helpers.proxy_helper.get_ssh_config_host", return_value=None
        ):
            suggest_port_forwarding("nonexistent")

        captured = capsys.readouterr()
        assert "does not use ProxyJump" in captured.out

    def test_check_connectivity_success(self):
        with patch("socket.create_connection") as mock_socket:
            mock_sock = Mock()
            mock_socket.return_value = mock_sock

            result = check_connectivity("example.com", 22)

            assert result is True
            mock_socket.assert_called_once_with(("example.com", 22), timeout=5)
            mock_sock.close.assert_called_once()

    def test_check_connectivity_failure(self):
        with patch("socket.create_connection", side_effect=socket.error):
            result = check_connectivity("unreachable.com", 22)
            assert result is False

    def test_check_connectivity_timeout(self):
        with patch("socket.create_connection", side_effect=socket.timeout):
            result = check_connectivity("slow.com", 22)
            assert result is False

    def test_main_with_proxy_jump(self, capsys):
        mock_host = SSHHost(
            hostname="dgx.example.com",
            user="researcher",
            port=22,
            identity_file="~/.ssh/dgx_key",
            proxy_jump="bastion.example.com",
        )

        test_args = ["proxy_helper.py", "dgx1"]

        with patch("sys.argv", test_args):
            with patch(
                "srunx.ssh.helpers.proxy_helper.get_ssh_config_host",
                return_value=mock_host,
            ):
                main()

        captured = capsys.readouterr()
        assert "SSH Config Analysis for 'dgx1':" in captured.out
        assert "ProxyJump: bastion.example.com" in captured.out
        assert "SSH Port Forwarding:" in captured.out

    def test_main_without_proxy_jump(self, capsys):
        mock_host = SSHHost(
            hostname="dgx.example.com",
            user="researcher",
            port=22,
            identity_file="~/.ssh/dgx_key",
        )

        test_args = ["proxy_helper.py", "dgx1"]

        with patch("sys.argv", test_args):
            with patch(
                "srunx.ssh.helpers.proxy_helper.get_ssh_config_host",
                return_value=mock_host,
            ):
                main()

        captured = capsys.readouterr()
        assert "SSH Config Analysis for 'dgx1':" in captured.out
        assert "should work directly with srunx ssh" in captured.out

    def test_main_host_not_found(self, capsys):
        test_args = ["proxy_helper.py", "nonexistent"]

        with patch("sys.argv", test_args):
            with patch(
                "srunx.ssh.helpers.proxy_helper.get_ssh_config_host", return_value=None
            ):
                with pytest.raises(SystemExit):
                    main()

        captured = capsys.readouterr()
        assert "Host 'nonexistent' not found in SSH config" in captured.err

    def test_main_with_test_connection_success(self, capsys):
        mock_host = SSHHost(
            hostname="dgx.example.com",
            user="researcher",
            port=22,
            identity_file="~/.ssh/dgx_key",
        )

        test_args = ["proxy_helper.py", "dgx1", "--test-connection"]

        with patch("sys.argv", test_args):
            with patch(
                "srunx.ssh.helpers.proxy_helper.get_ssh_config_host",
                return_value=mock_host,
            ):
                with patch(
                    "srunx.ssh.helpers.proxy_helper.check_connectivity",
                    return_value=True,
                ):
                    main()

        captured = capsys.readouterr()
        assert "Testing connectivity" in captured.out
        assert "✓ Direct connection successful" in captured.out

    def test_main_with_test_connection_failure(self, capsys):
        mock_host = SSHHost(
            hostname="dgx.example.com",
            user="researcher",
            port=22,
            identity_file="~/.ssh/dgx_key",
        )

        test_args = ["proxy_helper.py", "dgx1", "--test-connection"]

        with patch("sys.argv", test_args):
            with patch(
                "srunx.ssh.helpers.proxy_helper.get_ssh_config_host",
                return_value=mock_host,
            ):
                with patch(
                    "srunx.ssh.helpers.proxy_helper.check_connectivity",
                    return_value=False,
                ):
                    main()

        captured = capsys.readouterr()
        assert "Testing connectivity" in captured.out
        assert "✗ Direct connection failed" in captured.out

    def test_main_with_custom_ssh_config(self, capsys):
        mock_host = SSHHost(hostname="dgx.example.com", user="researcher")

        test_args = ["proxy_helper.py", "dgx1", "--ssh-config", "/custom/ssh/config"]

        with patch("sys.argv", test_args):
            with patch(
                "srunx.ssh.helpers.proxy_helper.get_ssh_config_host",
                return_value=mock_host,
            ) as mock_get:
                main()

        # Verify that custom ssh config path was passed
        mock_get.assert_called_with("dgx1", "/custom/ssh/config")
