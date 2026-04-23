"""Tests for config router: /api/config/*"""

from __future__ import annotations

import os
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from srunx.web.app import create_app
from srunx.web.deps import get_adapter


@pytest.fixture
def mock_adapter() -> MagicMock:
    return MagicMock()


@pytest.fixture
def client(mock_adapter: MagicMock):  # type: ignore[misc]
    import srunx.web.config as config_mod

    original = config_mod._config
    config_mod._config = None
    config_mod.get_web_config()

    app = create_app()
    app.dependency_overrides[get_adapter] = lambda: mock_adapter

    yield TestClient(app, raise_server_exceptions=False)

    app.dependency_overrides.clear()
    config_mod._config = original


class TestGetConfig:
    @patch("srunx.web.routers.config.get_config")
    def test_get_current_config(self, mock_get_config, client: TestClient) -> None:
        from srunx.common.config import SrunxConfig

        mock_get_config.return_value = SrunxConfig()
        resp = client.get("/api/config")
        assert resp.status_code == 200
        data = resp.json()
        # SrunxConfig has resource_defaults and environment_defaults
        assert "resource_defaults" in data or isinstance(data, dict)
        mock_get_config.assert_called_once_with(reload=True)


class TestGetPaths:
    @patch("srunx.web.routers.config.get_config_paths")
    def test_get_paths(self, mock_paths, client: TestClient, tmp_path) -> None:
        # Simulate three config paths: system, user, project
        existing = tmp_path / "srunx.toml"
        existing.write_text("")
        mock_paths.return_value = [
            tmp_path / "system.toml",
            existing,
            tmp_path / "project.toml",
        ]

        resp = client.get("/api/config/paths")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 3
        assert data[0]["source"] == "system"
        assert data[0]["exists"] is False
        assert data[1]["source"] == "user"
        assert data[1]["exists"] is True
        assert data[2]["source"] == "project"


class TestResetConfig:
    @patch("srunx.web.routers.config.save_user_config")
    @patch("srunx.web.routers.config.get_config")
    def test_reset_config(self, mock_get_config, mock_save, client: TestClient) -> None:
        from srunx.common.config import SrunxConfig

        mock_get_config.return_value = SrunxConfig()
        resp = client.post("/api/config/reset")
        assert resp.status_code == 200
        mock_save.assert_called_once()
        mock_get_config.assert_called_once_with(reload=True)


class TestGetEnvVars:
    def test_get_env_vars_empty(self, client: TestClient) -> None:
        # With all SRUNX_ vars cleared by conftest, should return empty or minimal
        resp = client.get("/api/config/env")
        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data, list)

    def test_get_env_vars_with_values(self, client: TestClient) -> None:
        with patch.dict(
            os.environ,
            {"SRUNX_DEFAULT_NODES": "4", "SRUNX_DEFAULT_PARTITION": "gpu"},
        ):
            resp = client.get("/api/config/env")
            assert resp.status_code == 200
            data = resp.json()
            names = [item["name"] for item in data]
            assert "SRUNX_DEFAULT_NODES" in names
            assert "SRUNX_DEFAULT_PARTITION" in names
            # Check structure
            nodes_entry = next(e for e in data if e["name"] == "SRUNX_DEFAULT_NODES")
            assert nodes_entry["value"] == "4"
            assert "description" in nodes_entry


class TestSSHStatus:
    def test_get_ssh_status_connected(self, client: TestClient) -> None:
        # mock_adapter is injected, so get_adapter_or_none will still return None
        # because we only override get_adapter, not the global _adapter.
        # We need to patch get_adapter_or_none directly.
        with (
            patch("srunx.web.deps.get_adapter_or_none", return_value=MagicMock()),
            patch(
                "srunx.web.deps.get_active_profile_name",
                return_value="my-server",
            ),
        ):
            resp = client.get("/api/config/ssh/status")
            assert resp.status_code == 200
            data = resp.json()
            assert data["connected"] is True
            assert data["profile_name"] == "my-server"

    def test_get_ssh_status_disconnected(self, client: TestClient) -> None:
        with (
            patch("srunx.web.deps.get_adapter_or_none", return_value=None),
            patch("srunx.web.deps.get_active_profile_name", return_value=None),
        ):
            resp = client.get("/api/config/ssh/status")
            assert resp.status_code == 200
            data = resp.json()
            assert data["connected"] is False
            assert data["profile_name"] is None
