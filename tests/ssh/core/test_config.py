"""Tests for srunx.ssh.core.config: ConfigManager over TOML clusters + models."""

from pathlib import Path

import pytest
from pydantic import ValidationError

from srunx.common import paths
from srunx.common.config import clear_settings_cache, use_project
from srunx.ssh.core.config import ConfigManager, MountConfig, ServerProfile


def write_user(text: str) -> None:
    path = paths.user_config_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)
    clear_settings_cache()


USER = """
default_cluster = "local"

[clusters.local]
transport = "local"

[clusters.dgx]
transport = "ssh"
host = "dgx1"
description = "DGX"

[clusters.direct]
transport = "ssh"
hostname = "example.com"
username = "me"
key_filename = "~/.ssh/k"
port = 2222
"""


class TestConfigManager:
    def test_lists_only_ssh_clusters(self):
        write_user(USER)
        profiles = ConfigManager().list_profiles()
        assert set(profiles) == {"dgx", "direct"}
        assert profiles["dgx"].ssh_host == "dgx1"
        assert profiles["direct"].port == 2222
        assert profiles["direct"].key_filename == str(Path("~/.ssh/k").expanduser())

    def test_get_profile(self):
        write_user(USER)
        cm = ConfigManager()
        assert cm.get_profile("dgx").description == "DGX"
        assert cm.get_profile("local") is None
        assert cm.get_profile("missing") is None

    def test_add_update_remove_roundtrip(self):
        write_user(USER)
        cm = ConfigManager()
        cm.add_profile(
            "new", ServerProfile(hostname="h", username="u", key_filename="/k")
        )
        assert cm.get_profile("new").hostname == "h"
        text = paths.user_config_path().read_text()
        assert "[clusters.new]" in text and 'transport = "ssh"' in text
        assert cm.update_profile("new", hostname="h2", snapshot_root="/snap")
        assert cm.get_profile("new").hostname == "h2"
        assert cm.get_profile("new").snapshot_root == "/snap"
        assert cm.update_profile("new", snapshot_root=None)
        assert cm.get_profile("new").snapshot_root is None
        assert cm.remove_profile("new")
        assert cm.get_profile("new") is None
        assert not cm.remove_profile("new")
        assert not cm.update_profile("new", hostname="x")

    def test_remove_default_cluster_clears_default(self):
        write_user(USER.replace('default_cluster = "local"', 'default_cluster = "dgx"'))
        ConfigManager().remove_profile("dgx")
        assert "default_cluster" not in paths.user_config_path().read_text()

    def test_invalid_name_rejected(self):
        with pytest.raises(ValueError):
            ConfigManager().add_profile(
                "a:b", ServerProfile(hostname="h", username="u", key_filename="")
            )

    def test_mounts_come_from_project(self, tmp_path):
        write_user(USER)
        root = tmp_path / "proj"
        (root / ".srunx").mkdir(parents=True)
        (root / ".git").mkdir()
        (root / ".srunx" / "config.toml").write_text(
            'name = "proj"\n[clusters.dgx]\nremote_dir = "/r/proj"\nsync = false\n'
        )
        with use_project(root):
            (mount,) = ConfigManager().get_profile("dgx").mounts
        assert mount.name == "proj"
        assert mount.local == str(root.resolve())
        assert mount.remote == "/r/proj"
        assert mount.sync is False
        assert ConfigManager().get_profile("dgx").mounts == []


class TestMountConfigExcludePatterns:
    """Tests for MountConfig.exclude_patterns field.

    Post-refactor: ``exclude_patterns`` is stored as ``tuple[str, ...]``
    (deep immutability). JSON/YAML ``list[str]`` input is coerced to a
    tuple by a ``mode='before'`` validator; ``model_dump(mode='json')``
    round-trips back to a list.
    """

    def test_default_exclude_patterns_is_empty(self, tmp_path):
        mount = MountConfig(name="proj", local=str(tmp_path), remote="/remote/proj")
        assert mount.exclude_patterns == ()
        assert isinstance(mount.exclude_patterns, tuple)

    def test_exclude_patterns_set_on_creation(self, tmp_path):
        patterns = ["data/", "*.bin", "logs/"]
        mount = MountConfig(
            name="proj",
            local=str(tmp_path),
            remote="/remote/proj",
            exclude_patterns=patterns,
        )
        # list input is coerced to tuple (deep-immutability guarantee)
        assert mount.exclude_patterns == tuple(patterns)
        assert isinstance(mount.exclude_patterns, tuple)
        assert list(mount.exclude_patterns) == patterns

    def test_exclude_patterns_serialized_in_model_dump(self, tmp_path):
        patterns = ["data/", "*.log"]
        mount = MountConfig(
            name="proj",
            local=str(tmp_path),
            remote="/remote/proj",
            exclude_patterns=patterns,
        )
        # Native dump preserves the tuple
        dumped = mount.model_dump()
        assert dumped["exclude_patterns"] == tuple(patterns)
        # JSON mode converts tuple → list for JSON compatibility
        dumped_json = mount.model_dump(mode="json")
        assert dumped_json["exclude_patterns"] == patterns

    def test_exclude_patterns_deserialized_from_dict(self, tmp_path):
        data = {
            "name": "proj",
            "local": str(tmp_path),
            "remote": "/remote/proj",
            "exclude_patterns": ["weights/", "*.ckpt"],
        }
        mount = MountConfig.model_validate(data)
        assert mount.exclude_patterns == ("weights/", "*.ckpt")

    def test_backward_compat_missing_exclude_patterns(self, tmp_path):
        """Old config without exclude_patterns should deserialize with default ()."""
        data = {
            "name": "proj",
            "local": str(tmp_path),
            "remote": "/remote/proj",
        }
        mount = MountConfig.model_validate(data)
        assert mount.exclude_patterns == ()


class TestMountConfigImmutability:
    """Verify frozen=True + tuple exclude_patterns cannot be mutated.

    A frozen MountConfig is required so the SSH adapter's frozen
    SlurmSSHClientSpec genuinely has deep immutability (and stays
    hashable end-to-end).
    """

    def test_field_assignment_is_blocked(self, tmp_path):
        mount = MountConfig(name="proj", local=str(tmp_path), remote="/remote/proj")
        with pytest.raises(ValidationError):
            mount.local = "/other"  # type: ignore[misc]

    def test_exclude_patterns_assignment_is_blocked(self, tmp_path):
        mount = MountConfig(
            name="proj",
            local=str(tmp_path),
            remote="/remote/proj",
            exclude_patterns=["a/"],
        )
        with pytest.raises(ValidationError):
            mount.exclude_patterns = ("b/",)  # type: ignore[misc]

    def test_exclude_patterns_inplace_concat_is_blocked(self, tmp_path):
        mount = MountConfig(
            name="proj",
            local=str(tmp_path),
            remote="/remote/proj",
            exclude_patterns=["a/"],
        )
        # tuple doesn't support __iadd__ on a field, but frozen would also
        # block the rebinding that += would attempt.
        with pytest.raises((ValidationError, TypeError)):
            mount.exclude_patterns += ("b/",)  # type: ignore[misc]

    def test_exclude_patterns_tuple_has_no_mutation_methods(self, tmp_path):
        mount = MountConfig(
            name="proj",
            local=str(tmp_path),
            remote="/remote/proj",
            exclude_patterns=["a/"],
        )
        # Proves the contained sequence is a tuple, so list-style
        # mutation methods simply don't exist.
        assert not hasattr(mount.exclude_patterns, "append")
        assert not hasattr(mount.exclude_patterns, "extend")

    def test_frozen_mount_is_hashable(self, tmp_path):
        mount = MountConfig(
            name="proj",
            local=str(tmp_path),
            remote="/remote/proj",
            exclude_patterns=["a/", "b/"],
        )
        # A hashable MountConfig lets SlurmSSHClientSpec (frozen dataclass
        # containing tuple[MountConfig, ...]) be used as a dict key / set
        # member, which is the whole point of deep immutability.
        assert hash(mount) == hash(mount)
        assert {mount} == {mount}


class TestServerProfileKeyFilenameExpansion:
    def test_tilde_is_expanded(self):
        profile = ServerProfile(
            hostname="h", username="u", key_filename="~/.ssh/test_key"
        )
        assert profile.key_filename == str(Path("~/.ssh/test_key").expanduser())
        assert "~" not in profile.key_filename

    def test_absolute_path_is_unchanged(self):
        profile = ServerProfile(
            hostname="h", username="u", key_filename="/abs/path/key"
        )
        assert profile.key_filename == "/abs/path/key"

    def test_empty_string_is_unchanged(self):
        profile = ServerProfile(hostname="h", username="u", key_filename="")
        assert profile.key_filename == ""


class TestServerProfileSnapshotRoot:
    def _profile(self, **kw) -> ServerProfile:
        return ServerProfile(hostname="h", username="u", key_filename="/k", **kw)

    def test_snapshot_root_defaults_to_none(self):
        assert self._profile().snapshot_root is None

    @pytest.mark.parametrize(
        "value", ["/data/snap", "~", "~/.local/share/srunx/snapshots", "/a/b.c_d+e-f"]
    )
    def test_snapshot_root_accepts_valid(self, value):
        assert self._profile(snapshot_root=value).snapshot_root == value

    def test_snapshot_root_empty_string_becomes_none(self):
        assert self._profile(snapshot_root="").snapshot_root is None

    @pytest.mark.parametrize(
        "value",
        [
            "/data/my snap",
            "/data/it's",
            '/data/"q"',
            "/data/$HOME",
            "/data;rm",
            "/data/../etc",
            "~/../x",
            "relative/path",
            "/a/~b",
            "~user/x",
        ],
    )
    def test_snapshot_root_rejects_invalid(self, value):
        with pytest.raises(ValidationError):
            self._profile(snapshot_root=value)

    def test_snapshot_root_old_config_loads_unchanged(self):
        profile = ServerProfile.model_validate(
            {"hostname": "h", "username": "u", "key_filename": "/k"}
        )
        assert profile.snapshot_root is None
