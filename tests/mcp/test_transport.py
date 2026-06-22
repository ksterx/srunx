"""Tests for the MCP transport selection layer (srunx.mcp.transport)."""

from __future__ import annotations

from contextlib import contextmanager
from unittest.mock import MagicMock, patch

import pytest

from srunx.common.exceptions import TransportSelectionError
from srunx.mcp.transport import MCP_POLICY, mcp_transport, parse_transport


class TestParseTransport:
    def test_none_is_local_no_force(self):
        assert parse_transport(None) == (None, False)

    def test_local_forces_local(self):
        assert parse_transport("local") == (None, True)

    def test_profile_name(self):
        assert parse_transport("dgx") == ("dgx", False)

    def test_profile_name_stripped(self):
        assert parse_transport("  dgx  ") == ("dgx", False)

    def test_empty_rejected(self):
        # A typo must not silently fall through to local.
        with pytest.raises(TransportSelectionError):
            parse_transport("")

    def test_whitespace_rejected(self):
        with pytest.raises(TransportSelectionError):
            parse_transport("   ")


class TestMcpPolicy:
    def test_policy_disables_implicit_rungs(self):
        # The whole point of the MCP policy: never resolve a remote from
        # ambient state (env var or current profile).
        assert MCP_POLICY.allow_env is False
        assert MCP_POLICY.allow_current_profile is False


class TestMcpTransport:
    def test_pins_policy_and_origin(self):
        """mcp_transport drives resolve_transport with the MCP policy,
        no banner, and the 'mcp' submission source."""
        rt = MagicMock()
        rt.transport_type = "local"

        @contextmanager
        def fake_resolve(**kwargs):
            fake_resolve.kwargs = kwargs
            yield rt

        with patch("srunx.mcp.transport.resolve_transport", fake_resolve):
            with mcp_transport("dgx") as resolved:
                assert resolved is rt
        assert fake_resolve.kwargs["profile"] == "dgx"
        assert fake_resolve.kwargs["local"] is False
        assert fake_resolve.kwargs["banner"] is False
        assert fake_resolve.kwargs["submission_source"] == "mcp"
        assert fake_resolve.kwargs["policy"] is MCP_POLICY

    def test_disconnects_ssh_adapter_on_exit(self):
        """A long-lived MCP server must not leak the SSH session."""
        rt = MagicMock()
        rt.transport_type = "ssh"

        @contextmanager
        def fake_resolve(**kwargs):
            yield rt

        with patch("srunx.mcp.transport.resolve_transport", fake_resolve):
            with mcp_transport("dgx"):
                pass
        rt.job_ops.disconnect.assert_called_once()

    def test_local_adapter_not_disconnected(self):
        rt = MagicMock()
        rt.transport_type = "local"

        @contextmanager
        def fake_resolve(**kwargs):
            yield rt

        with patch("srunx.mcp.transport.resolve_transport", fake_resolve):
            with mcp_transport(None):
                pass
        rt.job_ops.disconnect.assert_not_called()

    def test_local_passthrough(self):
        rt = MagicMock()
        rt.transport_type = "local"

        @contextmanager
        def fake_resolve(**kwargs):
            fake_resolve.kwargs = kwargs
            yield rt

        with patch("srunx.mcp.transport.resolve_transport", fake_resolve):
            with mcp_transport("local"):
                pass
        assert fake_resolve.kwargs["profile"] is None
        assert fake_resolve.kwargs["local"] is True
