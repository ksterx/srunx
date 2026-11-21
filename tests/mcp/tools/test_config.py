"""Tests for srunx.mcp.tools.config."""

from __future__ import annotations

import os
from pathlib import Path
from unittest.mock import patch

import pytest

from srunx.common.config import clear_settings_cache
from srunx.mcp.tools.config import get_config, list_clusters

USER_CONFIG = """\
default_cluster = "local"

[defaults.resources]
nodes = 2

[clusters.local]
transport = "local"

[clusters.dgx]
transport = "ssh"
hostname = "dgx.example.com"
username = "researcher"
description = "DGX server"

[clusters.dgx.defaults.resources]
gpus_per_node = 8
"""


@pytest.fixture
def user_config() -> Path:
    path = Path(os.environ["XDG_CONFIG_HOME"]) / "srunx" / "config.toml"
    path.write_text(USER_CONFIG)
    clear_settings_cache()
    return path


@pytest.fixture
def project(tmp_path: Path) -> Path:
    root = tmp_path / "proj"
    (root / ".srunx").mkdir(parents=True)
    (root / ".srunx" / "config.toml").write_text(
        'name = "proj"\n\n'
        "[clusters.dgx]\n"
        'remote_dir = "/remote/proj"\n\n'
        "[clusters.dgx.defaults.resources]\n"
        "nodes = 4\n"
    )
    clear_settings_cache()
    return root


class TestGetConfig:
    def test_global_defaults(self, user_config: Path):
        result = get_config()
        assert result["success"] is True
        assert result["cluster"] is None
        assert result["resources"]["nodes"] == 2
        assert result["resources"]["gpus_per_node"] == 0
        assert result["log_dir"] == "logs"

    def test_cluster_defaults(self, user_config: Path):
        result = get_config(cluster="dgx")
        assert result["success"] is True
        assert result["resources"]["nodes"] == 2
        assert result["resources"]["gpus_per_node"] == 8

    def test_project_defaults(self, user_config: Path, project: Path):
        result = get_config(cluster="dgx", project=str(project))
        assert result["success"] is True
        assert result["project"] == str(project.resolve())
        assert result["resources"]["nodes"] == 4

    def test_unknown_cluster(self, user_config: Path):
        result = get_config(cluster="nope")
        assert result["success"] is False

    def test_catches_exception(self):
        with patch(
            "srunx.common.config.load_settings",
            side_effect=RuntimeError("config broken"),
        ):
            result = get_config()
            assert result["success"] is False
            assert "config broken" in result["error"]


class TestListClusters:
    def test_returns_clusters(self, user_config: Path):
        result = list_clusters()
        assert result["success"] is True
        assert result["count"] == 2
        by_name = {c["name"]: c for c in result["clusters"]}
        assert by_name["local"]["transport"] == "local"
        assert by_name["dgx"]["transport"] == "ssh"
        assert by_name["dgx"]["host"] == "dgx.example.com"
        assert by_name["dgx"]["description"] == "DGX server"
        assert "mounts" not in by_name["dgx"]

    def test_project_mounts(self, user_config: Path, project: Path):
        result = list_clusters(project=str(project))
        assert result["success"] is True
        dgx = next(c for c in result["clusters"] if c["name"] == "dgx")
        assert dgx["mounts"] == [
            {"name": "proj", "local": str(project.resolve()), "remote": "/remote/proj"}
        ]

    def test_default_only_local(self):
        result = list_clusters()
        assert result["success"] is True
        assert [c["name"] for c in result["clusters"]] == ["local"]

    def test_catches_exception(self):
        with patch(
            "srunx.common.config.load_settings",
            side_effect=RuntimeError("config error"),
        ):
            result = list_clusters()
            assert result["success"] is False
            assert "config error" in result["error"]
