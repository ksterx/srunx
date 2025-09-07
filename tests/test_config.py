"""Tests for srunx.config module."""

import json
import os
import tempfile
from pathlib import Path
from unittest.mock import patch

from srunx.config import (
    EnvironmentDefaults,
    ResourceDefaults,
    SrunxConfig,
    create_example_config,
    get_config_paths,
    load_config,
    load_config_from_env,
    load_config_from_file,
    merge_config,
    save_user_config,
)


class TestResourceDefaults:
    """Test ResourceDefaults model."""

    def test_resource_defaults(self):
        """Test ResourceDefaults with default values."""
        resource = ResourceDefaults()
        assert resource.nodes == 1
        assert resource.gpus_per_node == 0
        assert resource.ntasks_per_node == 1
        assert resource.cpus_per_task == 1
        assert resource.memory_per_node is None
        assert resource.time_limit is None
        assert resource.nodelist is None
        assert resource.partition is None

    def test_resource_defaults_custom_values(self):
        """Test ResourceDefaults with custom values."""
        resource = ResourceDefaults(
            nodes=2,
            gpus_per_node=1,
            memory_per_node="32GB",
            time_limit="2:00:00",
            partition="gpu",
        )
        assert resource.nodes == 2
        assert resource.gpus_per_node == 1
        assert resource.memory_per_node == "32GB"
        assert resource.time_limit == "2:00:00"
        assert resource.partition == "gpu"


class TestEnvironmentDefaults:
    """Test EnvironmentDefaults model."""

    def test_environment_defaults(self):
        """Test EnvironmentDefaults with default values."""
        env = EnvironmentDefaults()
        assert env.conda is None
        assert env.venv is None
        assert env.container is None
        assert env.env_vars == {}

    def test_environment_defaults_custom_values(self):
        """Test EnvironmentDefaults with custom values."""
        env = EnvironmentDefaults(
            conda="ml_env", env_vars={"CUDA_VISIBLE_DEVICES": "0"}
        )
        assert env.conda == "ml_env"
        assert env.env_vars == {"CUDA_VISIBLE_DEVICES": "0"}


class TestSrunxConfig:
    """Test SrunxConfig model."""

    def test_srunx_config_defaults(self):
        """Test SrunxConfig with default values."""
        config = SrunxConfig()
        assert config.resources.nodes == 1
        assert config.environment.conda is None
        assert config.log_dir == "logs"
        assert config.work_dir is None

    def test_srunx_config_custom_values(self):
        """Test SrunxConfig with custom values."""
        config = SrunxConfig(
            resources=ResourceDefaults(nodes=2, partition="gpu"),
            environment=EnvironmentDefaults(conda="ml_env"),
            log_dir="custom_logs",
            work_dir="/scratch/user",
        )
        assert config.resources.nodes == 2
        assert config.resources.partition == "gpu"
        assert config.environment.conda == "ml_env"
        assert config.log_dir == "custom_logs"
        assert config.work_dir == "/scratch/user"


class TestConfigLoading:
    """Test configuration loading functions."""

    def test_load_config_from_file_nonexistent(self):
        """Test loading config from nonexistent file."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            config_path = Path(tmp_dir) / "nonexistent.json"
            result = load_config_from_file(config_path)
            assert result == {}

    def test_load_config_from_file_valid(self):
        """Test loading config from valid JSON file."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            config_path = Path(tmp_dir) / "config.json"
            test_config = {
                "resources": {"nodes": 2, "partition": "gpu"},
                "log_dir": "custom_logs",
            }
            with open(config_path, "w") as f:
                json.dump(test_config, f)

            result = load_config_from_file(config_path)
            assert result == test_config

    def test_load_config_from_file_invalid_json(self):
        """Test loading config from invalid JSON file."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            config_path = Path(tmp_dir) / "invalid.json"
            with open(config_path, "w") as f:
                f.write("invalid json content")

            result = load_config_from_file(config_path)
            assert result == {}

    def test_merge_config(self):
        """Test configuration merging."""
        base = {"resources": {"nodes": 1, "gpus_per_node": 0}, "log_dir": "logs"}
        override = {
            "resources": {"nodes": 2, "partition": "gpu"},
            "work_dir": "/scratch",
        }

        result = merge_config(base, override)
        expected = {
            "resources": {"nodes": 2, "gpus_per_node": 0, "partition": "gpu"},
            "log_dir": "logs",
            "work_dir": "/scratch",
        }
        assert result == expected

    def test_load_config_from_env_empty(self):
        """Test loading config from environment with no variables set."""
        with patch.dict(os.environ, {}, clear=True):
            result = load_config_from_env()
            assert result == {}

    def test_load_config_from_env_with_values(self):
        """Test loading config from environment variables."""
        env_vars = {
            "SRUNX_DEFAULT_NODES": "2",
            "SRUNX_DEFAULT_GPUS_PER_NODE": "1",
            "SRUNX_DEFAULT_MEMORY_PER_NODE": "32GB",
            "SRUNX_DEFAULT_PARTITION": "gpu",
            "SRUNX_DEFAULT_CONDA": "ml_env",
            "SRUNX_DEFAULT_LOG_DIR": "custom_logs",
        }

        with patch.dict(os.environ, env_vars):
            result = load_config_from_env()

            expected = {
                "resources": {
                    "nodes": 2,
                    "gpus_per_node": 1,
                    "memory_per_node": "32GB",
                    "partition": "gpu",
                },
                "environment": {"conda": "ml_env"},
                "log_dir": "custom_logs",
            }
            assert result == expected

    def test_load_config_from_env_invalid_values(self):
        """Test loading config from environment with invalid values."""
        env_vars = {
            "SRUNX_DEFAULT_NODES": "invalid",
            "SRUNX_DEFAULT_GPUS_PER_NODE": "not_a_number",
        }

        with patch.dict(os.environ, env_vars):
            result = load_config_from_env()
            assert result == {}


class TestConfigPaths:
    """Test configuration path functions."""

    def test_get_config_paths(self):
        """Test getting configuration paths."""
        paths = get_config_paths()
        assert (
            len(paths) == 4
        )  # system, user, project (.srunx.json), project (srunx.json)
        assert all(isinstance(path, Path) for path in paths)

    @patch("os.name", "posix")
    def test_get_config_paths_posix(self):
        """Test getting configuration paths on POSIX systems."""
        paths = get_config_paths()
        assert str(paths[0]).startswith("/etc/srunx/")
        assert ".config/srunx/" in str(paths[1])

    # Skipping Windows test as it's complex to mock across platforms


class TestConfigSaving:
    """Test configuration saving functions."""

    def test_save_user_config(self):
        """Test saving user configuration."""
        config = SrunxConfig(
            resources=ResourceDefaults(nodes=2, partition="gpu"), log_dir="custom_logs"
        )

        with tempfile.TemporaryDirectory() as tmp_dir:
            user_config_path = Path(tmp_dir) / "config.json"

            with patch("srunx.config.get_config_paths") as mock_paths:
                mock_paths.return_value = [
                    Path("/etc/srunx/config.json"),  # system
                    user_config_path,  # user
                    Path(".srunx.json"),  # project
                    Path("srunx.json"),  # project
                ]

                save_user_config(config)

                assert user_config_path.exists()
                with open(user_config_path) as f:
                    saved_config = json.load(f)

                assert saved_config["resources"]["nodes"] == 2
                assert saved_config["resources"]["partition"] == "gpu"
                assert saved_config["log_dir"] == "custom_logs"


class TestExampleConfig:
    """Test example configuration creation."""

    def test_create_example_config(self):
        """Test creating example configuration."""
        example = create_example_config()
        assert isinstance(example, str)

        # Parse as JSON to ensure it's valid
        config_data = json.loads(example)
        assert "resources" in config_data
        assert "environment" in config_data
        assert "log_dir" in config_data


class TestFullConfigLoading:
    """Test full configuration loading integration."""

    def test_load_config_integration(self):
        """Test loading configuration with multiple sources."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            # Create a test config file
            config_path = Path(tmp_dir) / "config.json"
            test_config = {
                "resources": {"nodes": 2, "partition": "gpu"},
                "log_dir": "file_logs",
            }
            with open(config_path, "w") as f:
                json.dump(test_config, f)

            # Mock the config paths to use our test file
            with patch("srunx.config.get_config_paths") as mock_paths:
                mock_paths.return_value = [config_path]

                # Mock environment variables
                env_vars = {
                    "SRUNX_DEFAULT_NODES": "4",  # Should override file
                    "SRUNX_DEFAULT_CONDA": "env_ml",  # Should be additional
                }

                with patch.dict(os.environ, env_vars):
                    config = load_config()

                    # Environment should override file values
                    assert config.resources.nodes == 4
                    # File values should be preserved when not overridden
                    assert config.resources.partition == "gpu"
                    # Environment should add new values
                    assert config.environment.conda == "env_ml"
                    # File values should be preserved
                    assert config.log_dir == "file_logs"
