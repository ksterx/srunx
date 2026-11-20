"""Tests for the config / projects / clusters routers.

Covers ``/api/config`` (effective config, file editing, paths, env),
``/api/projects`` and ``/api/clusters``, plus request-scoped selection via
``?cluster=`` / ``?project=``. Config is real TOML under the per-test
``XDG_CONFIG_HOME`` from ``tests/conftest.py``.
"""

from __future__ import annotations

import os
from pathlib import Path
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from srunx.common.config import clear_settings_cache
from srunx.web.app import create_app

USER_CONFIG = """\
default_cluster = "local"

[defaults.resources]
nodes = 1

[clusters.local]
transport = "local"

[clusters.gpu]
transport = "ssh"
hostname = "gpu.example.com"
username = "alice"

[clusters.gpu.defaults.resources]
partition = "a100"
"""


def _user_config_path() -> Path:
    return Path(os.environ["XDG_CONFIG_HOME"]) / "srunx" / "config.toml"


@pytest.fixture
def user_config() -> Path:
    path = _user_config_path()
    path.write_text(USER_CONFIG)
    clear_settings_cache()
    return path


@pytest.fixture
def project_root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    root = tmp_path / "myproj"
    (root / ".srunx").mkdir(parents=True)
    (root / ".srunx" / "config.toml").write_text(
        'name = "myproj"\n'
        'cluster = "gpu"\n\n'
        "[clusters.gpu]\n"
        'remote_dir = "/home/alice/myproj"\n\n'
        "[clusters.gpu.defaults.resources]\n"
        "nodes = 4\n"
    )
    monkeypatch.setenv("SRUNX_WEB_PROJECTS", str(root))
    clear_settings_cache()
    return root


@pytest.fixture
def client(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):  # type: ignore[misc]
    import srunx.web.config as config_mod

    # Keep cwd discovery from picking up a project outside the test.
    monkeypatch.chdir(tmp_path)
    original = config_mod._config
    config_mod._config = None

    app = create_app()
    yield TestClient(app, raise_server_exceptions=False)

    config_mod._config = original


class TestEffectiveConfig:
    def test_default_cluster(self, user_config: Path, client: TestClient) -> None:
        resp = client.get("/api/config")
        assert resp.status_code == 200
        data = resp.json()
        assert data["cluster"] == "local"
        assert data["project"] is None
        assert data["values"]["resources"]["partition"] is None
        assert "resources" in data["values"]

    def test_request_cluster(self, user_config: Path, client: TestClient) -> None:
        resp = client.get("/api/config", params={"cluster": "gpu"})
        assert resp.status_code == 200
        data = resp.json()
        assert data["cluster"] == "gpu"
        assert data["values"]["resources"]["partition"] == "a100"
        assert str(user_config) in data["sources"]["resources.partition"]

    def test_selection_is_request_scoped(
        self, user_config: Path, client: TestClient
    ) -> None:
        assert client.get("/api/config?cluster=gpu").json()["cluster"] == "gpu"
        # The next request without ?cluster= is unaffected.
        assert client.get("/api/config").json()["cluster"] == "local"

    def test_project_defaults(
        self, user_config: Path, project_root: Path, client: TestClient
    ) -> None:
        resp = client.get("/api/config", params={"project": "myproj"})
        assert resp.status_code == 200
        data = resp.json()
        # The project's ``cluster`` becomes the default selection.
        assert data["cluster"] == "gpu"
        assert data["project"] == "myproj"
        assert data["project_root"] == str(project_root.resolve())
        assert data["values"]["resources"]["nodes"] == 4

    def test_unknown_project_rejected(
        self, user_config: Path, client: TestClient
    ) -> None:
        resp = client.get("/api/config", params={"project": "ghost"})
        assert resp.status_code == 400
        assert "ghost" in resp.json()["detail"]


class TestGetPaths:
    def test_paths_without_project(self, client: TestClient) -> None:
        resp = client.get("/api/config/paths")
        assert resp.status_code == 200
        data = resp.json()
        assert [d["source"] for d in data] == ["system", "user", "state"]
        user = data[1]
        assert user["path"] == str(_user_config_path())
        assert user["exists"] is True

    def test_paths_with_project(self, project_root: Path, client: TestClient) -> None:
        resp = client.get("/api/config/paths", params={"project": "myproj"})
        data = resp.json()
        assert [d["source"] for d in data] == ["system", "user", "project", "state"]
        assert data[2]["path"].endswith(".srunx/config.toml")
        assert data[2]["exists"] is True


class TestConfigFile:
    def test_get_user_file(self, user_config: Path, client: TestClient) -> None:
        resp = client.get("/api/config/file")
        assert resp.status_code == 200
        data = resp.json()
        assert data["scope"] == "user"
        assert data["exists"] is True
        assert data["data"]["clusters"]["gpu"]["hostname"] == "gpu.example.com"
        assert data["version"]

    def test_patch_set_and_delete(self, user_config: Path, client: TestClient) -> None:
        version = client.get("/api/config/file").json()["version"]
        resp = client.patch(
            "/api/config/file",
            json={
                "scope": "user",
                "version": version,
                "set": {"clusters.gpu.defaults.resources.time_limit": "2:00:00"},
                "delete": ["defaults.resources.nodes"],
            },
        )
        assert resp.status_code == 200, resp.text
        data = resp.json()
        assert data["version"] != version
        gpu = data["data"]["clusters"]["gpu"]["defaults"]["resources"]
        assert gpu == {"partition": "a100", "time_limit": "2:00:00"}
        assert "nodes" not in data["data"]["defaults"]["resources"]
        # Comments / unrelated keys stay intact on disk.
        text = user_config.read_text()
        assert 'time_limit = "2:00:00"' in text
        assert 'hostname = "gpu.example.com"' in text

    def test_delete_after_unset_restores_inheritance(
        self, user_config: Path, client: TestClient
    ) -> None:
        key = "defaults.resources.partition"
        resp = client.patch("/api/config/file", json={"scope": "user", "unset": [key]})
        assert resp.json()["data"]["defaults"]["resources"]["unset"] == ["partition"]
        resp = client.patch("/api/config/file", json={"scope": "user", "delete": [key]})
        assert resp.status_code == 200, resp.text
        assert "unset" not in resp.json()["data"]["defaults"]["resources"]

    def test_patch_connection_drops_cached_adapter(
        self, user_config: Path, client: TestClient
    ) -> None:
        from unittest.mock import MagicMock

        from srunx.common.remote_env import RemoteEnv, _read_cache, store_remote_env
        from srunx.web import deps

        adapter = MagicMock()
        deps._adapters["gpu"] = adapter
        store_remote_env("gpu", RemoteEnv("/h", "/d", "/c"))
        try:
            resp = client.patch(
                "/api/config/file",
                json={"scope": "user", "set": {"clusters.gpu.hostname": "new"}},
            )
            assert resp.status_code == 200, resp.text
            adapter.disconnect.assert_called_once()
            assert "gpu" not in deps._adapters
            assert "gpu" not in _read_cache()
        finally:
            deps._adapters.pop("gpu", None)

    def test_patch_defaults_keeps_cached_adapter(
        self, user_config: Path, client: TestClient
    ) -> None:
        from unittest.mock import MagicMock

        from srunx.web import deps

        adapter = MagicMock()
        deps._adapters["gpu"] = adapter
        try:
            key = "clusters.gpu.defaults.resources.partition"
            resp = client.patch(
                "/api/config/file", json={"scope": "user", "set": {key: "h100"}}
            )
            assert resp.status_code == 200, resp.text
            adapter.disconnect.assert_not_called()
            assert deps._adapters["gpu"] is adapter
        finally:
            deps._adapters.pop("gpu", None)

    def test_stale_version_conflict(
        self, user_config: Path, client: TestClient
    ) -> None:
        version = client.get("/api/config/file").json()["version"]
        user_config.write_text(USER_CONFIG + "\n# edited elsewhere\n")
        resp = client.patch(
            "/api/config/file",
            json={
                "scope": "user",
                "version": version,
                "set": {"defaults.resources.nodes": 2},
            },
        )
        assert resp.status_code == 409
        assert "edited elsewhere" in user_config.read_text()
        assert "nodes = 2" not in user_config.read_text()

    def test_invalid_value_rejected(
        self, user_config: Path, client: TestClient
    ) -> None:
        before = user_config.read_text()
        resp = client.patch(
            "/api/config/file",
            json={"scope": "user", "set": {"defaults.bogus": 1}},
        )
        assert resp.status_code == 422
        assert user_config.read_text() == before

    def test_unset(self, user_config: Path, client: TestClient) -> None:
        resp = client.patch(
            "/api/config/file",
            json={
                "scope": "user",
                "unset": ["clusters.gpu.defaults.resources.partition"],
            },
        )
        assert resp.status_code == 200, resp.text
        res = resp.json()["data"]["clusters"]["gpu"]["defaults"]["resources"]
        assert "partition" not in res
        assert res["unset"] == ["partition"]
        cfg = client.get("/api/config?cluster=gpu").json()
        assert cfg["values"]["resources"]["partition"] is None

    def test_project_scope_requires_project(
        self, user_config: Path, client: TestClient
    ) -> None:
        resp = client.get("/api/config/file", params={"scope": "project"})
        assert resp.status_code == 400

    def test_project_scope_edit(
        self, user_config: Path, project_root: Path, client: TestClient
    ) -> None:
        resp = client.patch(
            "/api/config/file?project=myproj",
            json={
                "scope": "project",
                "set": {"clusters.gpu.defaults.resources.nodes": 8},
            },
        )
        assert resp.status_code == 200, resp.text
        assert "nodes = 8" in (project_root / ".srunx" / "config.toml").read_text()
        cfg = client.get("/api/config", params={"project": "myproj"}).json()
        assert cfg["values"]["resources"]["nodes"] == 8

    def test_project_scope_rejects_connection_keys(
        self, user_config: Path, project_root: Path, client: TestClient
    ) -> None:
        resp = client.patch(
            "/api/config/file?project=myproj",
            json={"scope": "project", "set": {"clusters.gpu.host": "evil"}},
        )
        assert resp.status_code == 422
        assert "evil" not in (project_root / ".srunx" / "config.toml").read_text()


class TestGetEnvVars:
    def test_empty(self, client: TestClient) -> None:
        resp = client.get("/api/config/env")
        assert resp.status_code == 200
        names = [e["name"] for e in resp.json()]
        assert "SRUNX_CLUSTER" not in names

    def test_reports_relevant_vars_only(self, client: TestClient) -> None:
        with patch.dict(
            os.environ,
            {
                "SRUNX_CLUSTER": "gpu",
                "SRUNX_WEB_TOKEN": "secret",
                # No longer a config source; not reported.
                "SRUNX_DEFAULT_NODES": "4",
            },
        ):
            data = client.get(
                "/api/config/env", headers={"Authorization": "Bearer secret"}
            ).json()
        by_name = {e["name"]: e for e in data}
        assert by_name["SRUNX_CLUSTER"]["value"] == "gpu"
        assert by_name["SRUNX_CLUSTER"]["description"]
        assert by_name["SRUNX_WEB_TOKEN"]["value"] == "********"
        assert "SRUNX_DEFAULT_NODES" not in by_name


class TestRemovedEndpoints:
    @pytest.mark.parametrize(
        ("method", "url"),
        [
            ("put", "/api/config"),
            ("post", "/api/config/reset"),
            ("get", "/api/config/ssh/status"),
            ("get", "/api/config/ssh/profiles"),
            ("post", "/api/config/ssh/profiles/p/connect"),
        ],
    )
    def test_gone(self, client: TestClient, method: str, url: str) -> None:
        resp = getattr(client, method)(url)
        # Unknown GETs fall through to the SPA's index.html; never JSON.
        assert resp.status_code in (404, 405) or not resp.headers[
            "content-type"
        ].startswith("application/json")


class TestProjects:
    def test_list(
        self, user_config: Path, project_root: Path, client: TestClient
    ) -> None:
        resp = client.get("/api/projects")
        assert resp.status_code == 200
        assert resp.json() == [
            {
                "name": "myproj",
                "root": str(project_root.resolve()),
                "config_path": str(project_root.resolve() / ".srunx" / "config.toml"),
                "cluster": "gpu",
            }
        ]

    def test_detail_mounts(
        self, user_config: Path, project_root: Path, client: TestClient
    ) -> None:
        resp = client.get("/api/projects/myproj")
        assert resp.status_code == 200
        mounts = resp.json()["mounts"]
        assert list(mounts) == ["gpu"]  # only SSH clusters
        assert mounts["gpu"][0]["name"] == "myproj"
        assert mounts["gpu"][0]["local"] == str(project_root.resolve())
        assert mounts["gpu"][0]["remote"] == "/home/alice/myproj"
        assert mounts["gpu"][0]["sync"] is True

    def test_unknown(self, client: TestClient) -> None:
        assert client.get("/api/projects/ghost").status_code == 404


class TestClusters:
    def test_list(self, user_config: Path, client: TestClient) -> None:
        resp = client.get("/api/clusters")
        assert resp.status_code == 200
        data = resp.json()
        assert data["default"] == "local"
        names = [c["name"] for c in data["clusters"]]
        assert names == ["gpu", "local"]
        gpu = data["clusters"][0]
        assert gpu["transport"] == "ssh"
        assert gpu["hostname"] == "gpu.example.com"

    def test_create_update_delete(self, user_config: Path, client: TestClient) -> None:
        resp = client.post(
            "/api/clusters",
            json={
                "name": "hpc",
                "transport": "ssh",
                "host": "hpc-login",
                "description": "HPC",
                "make_default": True,
            },
        )
        assert resp.status_code == 200, resp.text
        assert resp.json()["host"] == "hpc-login"
        text = user_config.read_text()
        assert "[clusters.hpc]" in text
        assert 'default_cluster = "hpc"' in text
        assert "\nport =" not in text.split("[clusters.hpc]")[1]

        dup = client.post("/api/clusters", json={"name": "hpc", "transport": "ssh"})
        assert dup.status_code == 409

        upd = client.put("/api/clusters/hpc", json={"description": "", "port": 2222})
        assert upd.status_code == 200, upd.text
        assert upd.json()["port"] == 2222
        assert upd.json()["description"] is None

        assert client.put("/api/clusters/ghost", json={}).status_code == 404

        resp = client.delete("/api/clusters/hpc")
        assert resp.status_code == 200
        data = client.get("/api/clusters").json()
        assert "hpc" not in [c["name"] for c in data["clusters"]]
        assert "default_cluster" not in user_config.read_text()

    def test_create_invalid_name(self, user_config: Path, client: TestClient) -> None:
        resp = client.post(
            "/api/clusters", json={"name": "bad name/x", "transport": "ssh"}
        )
        assert resp.status_code == 422

    def test_delete_unknown(self, user_config: Path, client: TestClient) -> None:
        assert client.delete("/api/clusters/ghost").status_code == 422

    def test_delete_drops_cached_adapter(
        self, user_config: Path, client: TestClient
    ) -> None:
        from unittest.mock import MagicMock

        from srunx.web import deps

        adapter = MagicMock()
        deps._adapters["gpu"] = adapter
        try:
            assert client.delete("/api/clusters/gpu").status_code == 200
            adapter.disconnect.assert_called_once()
            assert "gpu" not in deps._adapters
        finally:
            deps._adapters.pop("gpu", None)

    def test_update_drops_cached_adapter_and_remote_env(
        self, user_config: Path, client: TestClient
    ) -> None:
        from unittest.mock import MagicMock

        from srunx.common.remote_env import RemoteEnv, _read_cache, store_remote_env
        from srunx.web import deps

        adapter = MagicMock()
        deps._adapters["gpu"] = adapter
        store_remote_env("gpu", RemoteEnv("/h", "/d", "/c"))
        try:
            resp = client.put("/api/clusters/gpu", json={"description": "x"})
            assert resp.status_code == 200
            adapter.disconnect.assert_called_once()
            assert "gpu" not in deps._adapters
            assert "gpu" not in _read_cache()
        finally:
            deps._adapters.pop("gpu", None)

    def test_update_transport_to_local_and_back(
        self, user_config: Path, client: TestClient
    ) -> None:
        # A second local cluster is refused before anything is written.
        assert (
            client.put("/api/clusters/gpu", json={"transport": "local"}).status_code
            == 422
        )
        assert client.delete("/api/clusters/local").status_code == 200
        resp = client.put("/api/clusters/gpu", json={"transport": "local"})
        assert resp.status_code == 200, resp.text
        assert resp.json()["transport"] == "local"
        assert resp.json()["hostname"] is None
        resp = client.put("/api/clusters/gpu", json={"transport": "ssh", "host": "g"})
        assert resp.status_code == 200, resp.text
        assert resp.json()["host"] == "g"
        assert (
            client.put("/api/clusters/gpu", json={"transport": "x"}).status_code == 422
        )

    def test_test_local(self, user_config: Path, client: TestClient) -> None:
        with patch("shutil.which", return_value="/usr/bin/sbatch"):
            resp = client.post("/api/clusters/local/test")
        assert resp.status_code == 200
        assert resp.json()["ok"] is True
        assert resp.json()["transport"] == "local"

    def test_test_ssh(self, user_config: Path, client: TestClient) -> None:
        from unittest.mock import MagicMock

        env = MagicMock(home="/home/alice")
        with patch("srunx.common.remote_env.fetch_remote_env", return_value=env):
            resp = client.post("/api/clusters/gpu/test")
        assert resp.json() == {
            "ok": True,
            "transport": "ssh",
            "detail": "connected",
            "home": "/home/alice",
        }
        with patch("srunx.common.remote_env.fetch_remote_env", return_value=None):
            resp = client.post("/api/clusters/gpu/test")
        assert resp.json()["ok"] is False

    def test_test_unknown(self, client: TestClient) -> None:
        assert client.post("/api/clusters/ghost/test").status_code == 404


class TestAdapterSelection:
    """``get_adapter`` resolves the request's cluster to a cached adapter."""

    def test_adapter_per_cluster_cached(self, user_config: Path) -> None:
        from srunx.web import deps

        with patch("srunx.web.deps.SlurmSSHClient") as cls:
            try:
                a = deps.adapter_for("gpu")
                b = deps.adapter_for("gpu")
            finally:
                deps._adapters.clear()
        assert a is b
        cls.assert_called_once_with(profile_name="gpu", submission_source="web")

    def test_local_cluster_rejected(self, user_config: Path) -> None:
        from fastapi import HTTPException

        from srunx.web import deps

        with pytest.raises(HTTPException) as exc:
            deps.adapter_for("local")
        assert exc.value.status_code == 400

    def test_unknown_cluster(self, user_config: Path) -> None:
        from fastapi import HTTPException

        from srunx.web import deps

        with pytest.raises(HTTPException) as exc:
            deps.adapter_for("ghost")
        assert exc.value.status_code == 404

    def test_endpoint_uses_request_cluster(
        self, user_config: Path, client: TestClient
    ) -> None:
        from unittest.mock import MagicMock

        from srunx.web import deps

        adapter = MagicMock()
        adapter.list_jobs.return_value = []
        deps._adapters["gpu"] = adapter
        try:
            resp = client.get("/api/jobs", params={"cluster": "gpu"})
        finally:
            deps._adapters.pop("gpu", None)
        assert resp.status_code == 200, resp.text
        adapter.list_jobs.assert_called()

    def test_local_default_cluster_is_400(
        self, user_config: Path, client: TestClient
    ) -> None:
        resp = client.get("/api/jobs")
        assert resp.status_code == 400
        assert "local transport" in resp.json()["detail"]
