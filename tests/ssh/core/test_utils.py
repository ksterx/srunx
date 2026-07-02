"""Tests for srunx.ssh.core.utils shell-safety helpers."""

import paramiko

from srunx.ssh.core.utils import (
    configure_host_key_verification,
    quote_shell_path,
)


class TestQuoteShellPath:
    def test_absolute_path_quoted(self):
        assert quote_shell_path("/tmp/a b") == "'/tmp/a b'"

    def test_home_path_expands_but_is_injection_safe(self):
        # $HOME must expand remotely, but a $()/backtick in the suffix must not.
        out = quote_shell_path("~/logs/$(id)")
        assert out.startswith('"$HOME/"')
        # The dangerous part sits inside single quotes → not substituted.
        assert "'logs/$(id)'" in out

    def test_home_path_no_suffix(self):
        assert quote_shell_path("~/") == "\"$HOME/\"''"


class TestHostKeyVerification:
    def test_default_policy_is_reject(self, monkeypatch):
        monkeypatch.delenv("SRUNX_SSH_HOST_KEY_POLICY", raising=False)
        client = paramiko.SSHClient()
        configure_host_key_verification(client)
        assert isinstance(client._policy, paramiko.RejectPolicy)

    def test_accept_new_policy(self, monkeypatch):
        monkeypatch.setenv("SRUNX_SSH_HOST_KEY_POLICY", "accept-new")
        client = paramiko.SSHClient()
        configure_host_key_verification(client)
        assert isinstance(client._policy, paramiko.AutoAddPolicy)

    def test_warn_policy_opt_in(self, monkeypatch):
        monkeypatch.setenv("SRUNX_SSH_HOST_KEY_POLICY", "warn")
        client = paramiko.SSHClient()
        configure_host_key_verification(client)
        assert isinstance(client._policy, paramiko.WarningPolicy)
