import os
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest

from srunx.ssh.core.ssh_config import SSHConfigParser, SSHHost


class TestSSHHost:
    def test_create_basic_host(self):
        host = SSHHost(hostname="example.com")

        assert host.hostname == "example.com"
        assert host.user is None
        assert host.port is None
        assert host.identity_file is None
        assert host.proxy_command is None
        assert host.proxy_jump is None
        assert host.forward_agent is None

    def test_create_full_host(self):
        host = SSHHost(
            hostname="dgx.example.com",
            user="researcher",
            port=2222,
            identity_file="~/.ssh/dgx_key",
            proxy_command="ssh proxy-host nc %h %p",
            proxy_jump="bastion.example.com",
            forward_agent=True,
        )

        assert host.hostname == "dgx.example.com"
        assert host.user == "researcher"
        assert host.port == 2222
        assert host.identity_file == "~/.ssh/dgx_key"
        assert host.proxy_command == "ssh proxy-host nc %h %p"
        assert host.proxy_jump == "bastion.example.com"
        assert host.forward_agent is True

    def test_effective_properties(self):
        host = SSHHost(
            hostname="example.com",
            user="testuser",
            port=2222,
            identity_file="~/.ssh/test_key",
        )

        assert host.effective_hostname == "example.com"
        assert host.effective_user == "testuser"
        assert host.effective_port == 2222

    def test_effective_port_default(self):
        host = SSHHost(hostname="example.com")
        assert host.effective_port == 22

    def test_effective_identity_file_expansion(self):
        host = SSHHost(hostname="example.com", identity_file="~/.ssh/id_rsa")

        with patch.dict(os.environ, {"HOME": "/home/testuser"}):
            assert host.effective_identity_file == "/home/testuser/.ssh/id_rsa"

    def test_effective_identity_file_none(self):
        host = SSHHost(hostname="example.com")
        assert host.effective_identity_file is None


class TestSSHConfigParser:
    @pytest.fixture
    def temp_ssh_config(self):
        """Create a temporary SSH config file for testing"""
        with tempfile.NamedTemporaryFile(
            mode="w", delete=False, suffix=".ssh_config"
        ) as f:
            temp_path = f.name
        yield temp_path
        # Cleanup
        if os.path.exists(temp_path):
            os.unlink(temp_path)

    @pytest.fixture
    def sample_ssh_config(self):
        return """# Sample SSH config
Host dgx1
    HostName dgx1.example.com
    User researcher
    Port 2222
    IdentityFile ~/.ssh/dgx_key
    ForwardAgent yes

Host bastion
    HostName bastion.company.com
    User admin
    IdentityFile ~/.ssh/company_key

Host dgx-*
    HostName %h.example.com
    User researcher
    ProxyJump bastion
    IdentityFile ~/.ssh/dgx_wildcard_key

Host proxy-test
    HostName internal.example.com
    ProxyCommand ssh bastion nc %h %p

Host *
    User defaultuser
    Port 22
    IdentityFile ~/.ssh/id_rsa

# Comment at the end
"""

    def test_init_with_existing_config(self, temp_ssh_config, sample_ssh_config):
        with open(temp_ssh_config, "w") as f:
            f.write(sample_ssh_config)

        parser = SSHConfigParser(temp_ssh_config)

        assert parser.config_path == Path(temp_ssh_config)
        assert len(parser.hosts) == 5  # dgx1, bastion, dgx-*, proxy-test, *

    def test_init_with_nonexistent_config(self, temp_ssh_config):
        # Don't create the file, just use the path
        parser = SSHConfigParser(temp_ssh_config)

        assert parser.config_path == Path(temp_ssh_config)
        # Should parse as empty without raising
        assert isinstance(parser.list_hosts(), dict)
