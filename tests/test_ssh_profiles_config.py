import json
import os
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest

from srunx.ssh.core.config import ConfigManager, MountConfig, ServerProfile


@pytest.fixture
def temp_config_file(tmp_path):
    fd, path = tempfile.mkstemp(suffix=".json")
    os.close(fd)
    yield path
    if os.path.exists(path):
        os.unlink(path)


@pytest.fixture
def sample_config_data():
    return {
        "current_profile": "test-profile",
        "profiles": {
            "test-profile": {
                "hostname": "example.com",
                "username": "testuser",
                "key_filename": "/home/user/.ssh/test_key",
                "port": 22,
                "description": "Test profile",
                "ssh_host": None,
                "env_vars": {"WANDB_PROJECT": "proj"},
            },
            "dgx-profile": {
                "hostname": "dgx.example.com",
                "username": "researcher",
                "key_filename": "/home/user/.ssh/dgx_key",
                "port": 2222,
                "description": "DGX profile",
                "ssh_host": "dgx1",
                "env_vars": None,
            },
        },
    }


class TestConfigManager:
    def test_init_with_existing_config(self, temp_config_file, sample_config_data):
        # Write sample config to temp file
        with open(temp_config_file, "w") as f:
            json.dump(sample_config_data, f)

        config_manager = ConfigManager(temp_config_file)

        assert config_manager.config_path == Path(temp_config_file)
        assert config_manager.config_data == sample_config_data

    def test_init_with_nonexistent_config(self, temp_config_file):
        # Don't create the file, just use the path
        config_manager = ConfigManager(temp_config_file)

        expected_data = {"current_profile": None, "profiles": {}}
        assert config_manager.config_data == expected_data

        # Should have created the file
        assert os.path.exists(temp_config_file)

    def test_init_with_invalid_json(self, temp_config_file):
        # Write invalid JSON to file
        with open(temp_config_file, "w") as f:
            f.write("invalid json content {")

        with pytest.raises(RuntimeError, match="Failed to load config"):
            ConfigManager(temp_config_file)

    @patch("pathlib.Path.home")
    def test_default_config_path(self, mock_home, temp_config_file):
        temp_dir = Path(temp_config_file).parent
        mock_home.return_value = temp_dir

        config_manager = ConfigManager()
        expected_path = temp_dir / ".config" / "srunx" / "config.json"

        assert config_manager.config_path == expected_path

    def test_add_profile(self, temp_config_file):
        config_manager = ConfigManager(temp_config_file)

        profile = ServerProfile(
            hostname="new.example.com",
            username="newuser",
            key_filename="/home/user/.ssh/new_key",
        )

        config_manager.add_profile("new-profile", profile)

        assert "new-profile" in config_manager.config_data["profiles"]
        assert (
            config_manager.config_data["profiles"]["new-profile"]
            == profile.model_dump()
        )

        # Verify it was saved to file
        with open(temp_config_file) as f:
            saved_data = json.load(f)
        assert saved_data["profiles"]["new-profile"] == profile.model_dump()

    def test_get_profile_existing(self, temp_config_file, sample_config_data):
        with open(temp_config_file, "w") as f:
            json.dump(sample_config_data, f)

        config_manager = ConfigManager(temp_config_file)
        profile = config_manager.get_profile("test-profile")

        assert profile is not None
        assert profile.hostname == "example.com"
        assert profile.username == "testuser"

    def test_get_profile_nonexistent(self, temp_config_file):
        config_manager = ConfigManager(temp_config_file)
        profile = config_manager.get_profile("nonexistent")

        assert profile is None

    def test_list_profiles(self, temp_config_file, sample_config_data):
        with open(temp_config_file, "w") as f:
            json.dump(sample_config_data, f)

        config_manager = ConfigManager(temp_config_file)
        profiles = config_manager.list_profiles()

        assert len(profiles) == 2
        assert "test-profile" in profiles
        assert "dgx-profile" in profiles
        assert isinstance(profiles["test-profile"], ServerProfile)
        assert profiles["test-profile"].hostname == "example.com"

    def test_remove_profile_existing(self, temp_config_file, sample_config_data):
        with open(temp_config_file, "w") as f:
            json.dump(sample_config_data, f)

        config_manager = ConfigManager(temp_config_file)
        result = config_manager.remove_profile("test-profile")

        assert result is True
        assert "test-profile" not in config_manager.config_data["profiles"]
        # Should clear current profile if it was the removed one
        assert config_manager.config_data["current_profile"] is None

    def test_remove_profile_nonexistent(self, temp_config_file):
        config_manager = ConfigManager(temp_config_file)
        result = config_manager.remove_profile("nonexistent")

        assert result is False

    def test_set_current_profile_existing(self, temp_config_file, sample_config_data):
        with open(temp_config_file, "w") as f:
            json.dump(sample_config_data, f)

        config_manager = ConfigManager(temp_config_file)
        result = config_manager.set_current_profile("dgx-profile")

        assert result is True
        assert config_manager.config_data["current_profile"] == "dgx-profile"

    def test_set_current_profile_nonexistent(self, temp_config_file):
        config_manager = ConfigManager(temp_config_file)
        result = config_manager.set_current_profile("nonexistent")
        assert result is False


class TestMountConfigExcludePatterns:
    """Tests for MountConfig.exclude_patterns field."""

    def test_default_exclude_patterns_is_empty(self, tmp_path):
        mount = MountConfig(name="proj", local=str(tmp_path), remote="/remote/proj")
        assert mount.exclude_patterns == []

    def test_exclude_patterns_set_on_creation(self, tmp_path):
        patterns = ["data/", "*.bin", "logs/"]
        mount = MountConfig(
            name="proj",
            local=str(tmp_path),
            remote="/remote/proj",
            exclude_patterns=patterns,
        )
        assert mount.exclude_patterns == patterns

    def test_exclude_patterns_serialized_in_model_dump(self, tmp_path):
        patterns = ["data/", "*.log"]
        mount = MountConfig(
            name="proj",
            local=str(tmp_path),
            remote="/remote/proj",
            exclude_patterns=patterns,
        )
        dumped = mount.model_dump()
        assert dumped["exclude_patterns"] == patterns

    def test_exclude_patterns_deserialized_from_dict(self, tmp_path):
        data = {
            "name": "proj",
            "local": str(tmp_path),
            "remote": "/remote/proj",
            "exclude_patterns": ["weights/", "*.ckpt"],
        }
        mount = MountConfig.model_validate(data)
        assert mount.exclude_patterns == ["weights/", "*.ckpt"]

    def test_backward_compat_missing_exclude_patterns(self, tmp_path):
        """Old config without exclude_patterns should deserialize with default []."""
        data = {
            "name": "proj",
            "local": str(tmp_path),
            "remote": "/remote/proj",
        }
        mount = MountConfig.model_validate(data)
        assert mount.exclude_patterns == []

    def test_mount_with_excludes_persisted_via_config_manager(self, temp_config_file):
        cm = ConfigManager(temp_config_file)
        profile = ServerProfile(hostname="h", username="u", key_filename="/k")
        cm.add_profile("p", profile)

        mount = MountConfig(
            name="proj",
            local="/tmp/local",
            remote="/remote/proj",
            exclude_patterns=["data/", "*.bin"],
        )
        cm.add_profile_mount("p", mount)

        # Reload from disk
        cm2 = ConfigManager(temp_config_file)
        loaded_profile = cm2.get_profile("p")
        assert loaded_profile is not None
        assert len(loaded_profile.mounts) == 1
        assert loaded_profile.mounts[0].exclude_patterns == ["data/", "*.bin"]

    def test_mount_without_excludes_persisted_via_config_manager(
        self, temp_config_file
    ):
        cm = ConfigManager(temp_config_file)
        profile = ServerProfile(hostname="h", username="u", key_filename="/k")
        cm.add_profile("p", profile)

        mount = MountConfig(name="proj", local="/tmp/local", remote="/remote/proj")
        cm.add_profile_mount("p", mount)

        cm2 = ConfigManager(temp_config_file)
        loaded_profile = cm2.get_profile("p")
        assert loaded_profile is not None
        assert loaded_profile.mounts[0].exclude_patterns == []
