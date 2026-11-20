"""Tests for srunx.common.config: TOML layers, clusters, projects, resolution."""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from srunx.common import paths
from srunx.common.config import (
    ConfigError,
    EnvironmentDefaults,
    ResourceDefaults,
    Settings,
    SrunxConfig,
    activate_config,
    active_config,
    apply_unset,
    clear_settings_cache,
    get_config,
    load_settings,
    merge_layer,
    use_project,
)


def write_user(text: str) -> Path:
    path = paths.user_config_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)
    clear_settings_cache()
    return path


def make_project(root: Path, text: str) -> Path:
    (root / ".srunx").mkdir(parents=True, exist_ok=True)
    (root / ".srunx" / "config.toml").write_text(text)
    (root / ".git").mkdir(exist_ok=True)
    clear_settings_cache()
    return root


USER = """
default_cluster = "local"

[defaults.resources]
time_limit = "01:00:00"
nodes = 2

[clusters.local]
transport = "local"

[clusters.gpu]
transport = "ssh"
host = "gpu-login"

[clusters.gpu.defaults.resources]
partition = "gpu"
gpus_per_node = 8

[clusters.hr]
transport = "ssh"
hostname = "hr.example.com"
username = "me"
"""


class TestModels:
    def test_resource_defaults(self):
        resource = ResourceDefaults()
        assert resource.nodes == 1
        assert resource.partition is None

    def test_environment_defaults(self):
        env = EnvironmentDefaults()
        assert env.conda is None
        assert env.env_vars == {}

    def test_srunx_config_defaults(self):
        config = SrunxConfig()
        assert config.log_dir == "logs"
        assert config.sources == {}


class TestMergeLayer:
    def test_records_sources_and_nests(self):
        sources: dict[str, str] = {}
        merged = merge_layer({}, {"resources": {"nodes": 2}}, sources, "a")
        merged = merge_layer(merged, {"resources": {"partition": "gpu"}}, sources, "b")
        assert merged == {"resources": {"nodes": 2, "partition": "gpu"}}
        assert sources == {"resources.nodes": "a", "resources.partition": "b"}

    def test_unset_drops_lower_values(self):
        sources: dict[str, str] = {}
        merged = merge_layer({}, {"resources": {"partition": "gpu"}}, sources, "a")
        merged = merge_layer(
            merged, {"resources": {"unset": ["partition"]}}, sources, "b"
        )
        assert merged == {"resources": {}}
        assert sources == {}


class TestSettings:
    def test_layer_order(self, tmp_path, monkeypatch):
        write_user(USER)
        root = make_project(
            tmp_path / "proj",
            """
name = "proj"
cluster = "gpu"

[defaults.resources]
time_limit = "02:00:00"

[clusters.gpu.defaults.resources]
time_limit = "04:00:00"
""",
        )
        settings = Settings(root)
        cfg = settings.effective("gpu")
        assert cfg.resources.time_limit == "04:00:00"
        assert cfg.resources.partition == "gpu"
        assert cfg.resources.nodes == 2
        assert cfg.resources.gpus_per_node == 8
        assert cfg.resources.cpus_per_task == 1  # built-in
        assert "[clusters.gpu]" in cfg.sources["resources.time_limit"]
        assert str(root) in cfg.sources["resources.time_limit"]
        assert cfg.sources["resources.partition"].endswith("config.toml [clusters.gpu]")
        assert "resources.cpus_per_task" not in cfg.sources

        # Without the cluster, cluster layers do not apply.
        plain = settings.effective(None)
        assert plain.resources.time_limit == "02:00:00"
        assert plain.resources.partition is None

    def test_unset_in_project(self, tmp_path):
        write_user(USER)
        root = make_project(
            tmp_path / "proj",
            'name = "proj"\n[clusters.gpu.defaults.resources]\nunset = ["partition"]\n',
        )
        cfg = Settings(root).effective("gpu")
        assert cfg.resources.partition is None
        assert "resources.partition" not in cfg.sources

    def test_cluster_selection_order(self, tmp_path, monkeypatch):
        write_user(USER)
        root = make_project(tmp_path / "proj", 'name = "proj"\ncluster = "gpu"\n')
        settings = Settings(root)
        assert settings.cluster_source("hr") == ("hr", "--cluster")
        monkeypatch.setenv("SRUNX_CLUSTER", "hr")
        assert settings.cluster_source() == ("hr", "$SRUNX_CLUSTER")
        monkeypatch.delenv("SRUNX_CLUSTER")
        assert settings.cluster_source()[0] == "gpu"
        assert Settings(None, discover=False).cluster_source()[0] == "local"

    def test_no_cluster_is_an_error(self):
        write_user('[clusters.local]\ntransport = "local"\n')
        with pytest.raises(ConfigError, match="No cluster selected"):
            Settings(None, discover=False).resolve_cluster(None)

    def test_unknown_cluster_is_an_error(self):
        write_user(USER)
        with pytest.raises(ConfigError, match="Unknown cluster 'nope'"):
            Settings(None, discover=False).resolve_cluster("nope")

    def test_project_may_mention_unconfigured_clusters(self, tmp_path):
        write_user(USER)
        root = make_project(
            tmp_path / "proj",
            'name = "proj"\n[clusters.other.defaults.resources]\npartition = "x"\n',
        )
        assert Settings(root).effective("gpu").resources.partition == "gpu"

    def test_project_cannot_define_connections(self, tmp_path):
        write_user(USER)
        root = make_project(
            tmp_path / "proj",
            'name = "proj"\n[clusters.gpu]\nhost = "evil"\n',
        )
        with pytest.raises(ConfigError, match="Extra inputs are not permitted"):
            Settings(root)

    def test_project_name_required(self, tmp_path):
        write_user(USER)
        root = make_project(tmp_path / "proj", 'cluster = "gpu"\n')
        with pytest.raises(ConfigError):
            Settings(root)

    def test_invalid_toml_is_reported(self):
        write_user("this is = = not toml")
        with pytest.raises(ConfigError, match="Invalid TOML"):
            Settings(None, discover=False)

    def test_unknown_default_key_rejected(self):
        write_user("[defaults.resources]\nbogus = 1\n")
        with pytest.raises(ConfigError):
            Settings(None, discover=False)

    def test_only_one_local_cluster(self):
        write_user(
            '[clusters.a]\ntransport = "local"\n[clusters.b]\ntransport = "local"\n'
        )
        with pytest.raises(ConfigError, match="At most one cluster"):
            Settings(None, discover=False)

    def test_ssh_cluster_needs_host(self):
        write_user('[clusters.a]\ntransport = "ssh"\n')
        with pytest.raises(ConfigError, match="needs 'host'"):
            Settings(None, discover=False)

    def test_mounts(self, tmp_path):
        write_user(
            USER + '\n[projects.proj.clusters.hr]\nremote_dir = "/home/me/personal"\n'
        )
        root = make_project(
            tmp_path / "proj",
            """
name = "proj"

[clusters.gpu]
remote_dir = "/shared/proj"
exclude = ["data/"]

[[clusters.gpu.mounts]]
name = "datasets"
local = "../datasets"
remote = "/data/datasets"
sync = false

[clusters.hr]
remote_dir = "/home/x/proj"
""",
        )
        settings = Settings(root)
        mounts = settings.mounts("gpu")
        assert [m.name for m in mounts] == ["proj", "datasets"]
        assert mounts[0].local == str(root.resolve())
        assert mounts[0].remote == "/shared/proj"
        assert mounts[0].exclude == ("data/",)
        assert mounts[1].local == str((tmp_path / "datasets").resolve())
        assert mounts[1].sync is False
        # The user's personal override wins over the project file.
        assert settings.mounts("hr")[0].remote == "/home/me/personal"
        assert settings.mounts("local") == []

    def test_default_workspace_uses_remote_env(self, tmp_path):
        from srunx.common.remote_env import RemoteEnv, store_remote_env

        write_user(USER)
        root = make_project(tmp_path / "proj", 'name = "proj"\n')
        settings = Settings(root)
        assert settings.mounts("gpu", fetch_remote=False) == []
        store_remote_env("gpu", RemoteEnv("/home/u", "/home/u/.local/share", "/c"))
        (mount,) = settings.mounts("gpu", fetch_remote=False)
        assert mount.remote.startswith("/home/u/.local/share/srunx/workspaces/proj-")


class TestProjectDiscovery:
    def test_walks_up_to_git_root(self, tmp_path, monkeypatch):
        root = make_project(tmp_path / "repo", 'name = "repo"\n')
        sub = root / "a" / "b"
        sub.mkdir(parents=True)
        assert paths.find_project_root(sub) == root.resolve()

    def test_stops_at_git_root_without_config(self, tmp_path):
        outer = make_project(tmp_path / "outer", 'name = "outer"\n')
        inner = outer / "inner"
        (inner / ".git").mkdir(parents=True)
        assert paths.find_project_root(inner) is None

    def test_never_uses_home(self, tmp_path, monkeypatch):
        monkeypatch.setenv("HOME", str(tmp_path))
        make_project(tmp_path, 'name = "home"\n')
        (tmp_path / ".git").rmdir()
        sub = tmp_path / "work"
        sub.mkdir()
        assert paths.find_project_root(sub) is None

    def test_use_project_overrides_discovery(self, tmp_path, monkeypatch):
        write_user(USER)
        root = make_project(tmp_path / "p", 'name = "p"\ncluster = "gpu"\n')
        with use_project(root):
            assert load_settings().project_root == root.resolve()
        with pytest.raises(ConfigError, match="does not exist"):
            with use_project(tmp_path / "missing"):
                pass


class TestActivation:
    def test_get_config_prefers_active(self):
        write_user(USER)
        custom = SrunxConfig(log_dir="x")
        with activate_config(custom):
            assert get_config() is custom
            assert active_config() is custom
        assert active_config() is None

    def test_get_config_uses_default_cluster(self):
        write_user(USER.replace('default_cluster = "local"', 'default_cluster = "gpu"'))
        assert get_config().resources.partition == "gpu"

    def test_domain_models_use_active_config(self):
        from srunx.domain import JobResource

        write_user(USER)
        cfg = load_settings(reload=True).effective("gpu")
        with activate_config(cfg):
            assert JobResource().partition == "gpu"
            assert JobResource(partition="debug").partition == "debug"
        assert JobResource().partition is None


class TestApplyUnset:
    def test_resets_to_builtin(self):
        write_user(USER)
        cfg = load_settings(reload=True).effective("gpu")
        cleared = apply_unset(cfg, ["partition", "time"])
        assert cleared.resources.partition is None
        assert cleared.resources.time_limit is None
        assert "resources.partition" not in cleared.sources
        assert cleared.cluster == "gpu"

    def test_rejects_unknown(self):
        with pytest.raises(ConfigError):
            apply_unset(SrunxConfig(), ["nonsense"])


class TestPaths:
    def test_xdg_and_state(self, tmp_path, monkeypatch):
        monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / "c"))
        monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path / "s"))
        assert paths.user_config_path() == tmp_path / "c" / "srunx" / "config.toml"
        assert paths.db_path() == tmp_path / "s" / "srunx" / "srunx.db"
        assert paths.locks_dir() == tmp_path / "s" / "srunx" / "locks"
        assert paths.templates_dir() == tmp_path / "c" / "srunx" / "templates"

    def test_relative_xdg_ignored(self, monkeypatch):
        monkeypatch.setenv("XDG_STATE_HOME", "relative/dir")
        assert paths.default_state_dir() == Path.home() / ".local" / "state" / "srunx"

    def test_state_dir_from_user_config(self, tmp_path):
        write_user(f'state_dir = "{tmp_path / "fast"}"\n')
        assert paths.db_path() == tmp_path / "fast" / "srunx.db"

    def test_project_paths(self, tmp_path):
        assert (
            paths.project_config_path(tmp_path) == tmp_path / ".srunx" / "config.toml"
        )
        assert (
            paths.project_workflows_dir(tmp_path) == tmp_path / ".srunx" / "workflows"
        )


def test_environment_variables_do_not_set_defaults(monkeypatch):
    write_user(USER)
    monkeypatch.setenv("SRUNX_DEFAULT_PARTITION", "env")
    assert load_settings(reload=True).effective("local").resources.partition is None
    assert "SRUNX_DEFAULT_PARTITION" in os.environ


class TestReviewRegressions:
    def test_partial_container_override_is_merged(self, tmp_path):
        write_user(
            USER + '\n[defaults.environment.container]\nimage = "img.sif"\n'
            'runtime = "apptainer"\n'
        )
        root = make_project(
            tmp_path / "p", 'name = "p"\n[defaults.environment.container]\nnv = true\n'
        )
        container = Settings(root).effective("local").environment.container
        assert container.runtime == "apptainer"
        assert container.nv is True

    def test_invalid_merged_container_still_rejected(self, tmp_path):
        write_user(USER)
        root = make_project(
            tmp_path / "p", 'name = "p"\n[defaults.environment.container]\nnv = true\n'
        )
        with pytest.raises(ConfigError, match="apptainer"):
            Settings(root).effective("local")

    def test_bad_container_field_type_rejected(self):
        write_user('[defaults.environment.container]\nnv = "maybe"\n')
        with pytest.raises(ConfigError):
            Settings(None, discover=False)

    def test_duplicate_mount_names_rejected(self, tmp_path):
        write_user(USER)
        root = make_project(
            tmp_path / "p",
            'name = "p"\n[clusters.gpu]\nremote_dir = "/r/p"\n'
            '[[clusters.gpu.mounts]]\nname = "p"\nlocal = "sub"\nremote = "/r/x"\n',
        )
        with pytest.raises(ConfigError, match="must be unique"):
            Settings(root).mounts("gpu")


def test_duplicate_local_roots_rejected(tmp_path):
    write_user(USER)
    root = make_project(
        tmp_path / "p",
        'name = "p"\n[clusters.gpu]\nremote_dir = "/r/p"\n'
        '[[clusters.gpu.mounts]]\nname = "again"\nlocal = "."\nremote = "/r/other"\n',
    )
    with pytest.raises(ConfigError, match="share the local directory"):
        Settings(root).mounts("gpu")
