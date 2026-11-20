"""Tests for the MCP transport selection layer (srunx.mcp.transport)."""

from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from srunx.common.exceptions import TransportSelectionError
from srunx.mcp.transport import MCP_POLICY, mcp_transport
from srunx.transport.registry import resolve_cluster_name


class TestMcpPolicy:
    def test_policy_disables_implicit_selection(self):
        # MCP never resolves a cluster from ambient state.
        assert MCP_POLICY.allow_implicit is False

    def test_no_explicit_cluster_is_rejected(self, monkeypatch):
        # conftest sets ``default_cluster = "local"``; MCP must ignore it,
        # and $SRUNX_CLUSTER too.
        monkeypatch.setenv("SRUNX_CLUSTER", "local")
        with pytest.raises(TransportSelectionError):
            resolve_cluster_name(cluster=None, policy=MCP_POLICY)

    def test_explicit_cluster_accepted(self):
        assert resolve_cluster_name(cluster="local", policy=MCP_POLICY) == (
            "local",
            "local",
        )

    def test_mcp_transport_without_cluster_raises(self):
        with pytest.raises(TransportSelectionError):
            with mcp_transport(None):
                pass


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
            with mcp_transport("dgx", mount_name="ml") as resolved:
                assert resolved is rt
        assert fake_resolve.kwargs["cluster"] == "dgx"
        assert fake_resolve.kwargs["mount_name"] == "ml"
        assert fake_resolve.kwargs["banner"] is False
        assert fake_resolve.kwargs["submission_source"] == "mcp"
        assert fake_resolve.kwargs["policy"] is MCP_POLICY

    def test_project_is_applied_during_resolution(self, tmp_path: Path):
        from srunx.common.config import load_settings

        root = tmp_path / "proj"
        (root / ".srunx").mkdir(parents=True)
        (root / ".srunx" / "config.toml").write_text('name = "proj"\n')
        rt = MagicMock()
        rt.transport_type = "local"
        seen: list[Path | None] = []

        @contextmanager
        def fake_resolve(**kwargs):
            seen.append(load_settings().project_root)
            yield rt

        with patch("srunx.mcp.transport.resolve_transport", fake_resolve):
            with mcp_transport("local", project=str(root)):
                pass
        assert seen == [root.resolve()]

    def test_unknown_project_rejected(self, tmp_path: Path):
        from srunx.common.config import ConfigError

        with pytest.raises(ConfigError):
            with mcp_transport("local", project=str(tmp_path / "missing")):
                pass

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
            with mcp_transport("local"):
                pass
        rt.job_ops.disconnect.assert_not_called()
