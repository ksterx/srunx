import json
import os
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest

from srunx.ssh.core.config import ConfigManager, ServerProfile


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
