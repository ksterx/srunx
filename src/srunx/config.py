"""Configuration management for srunx."""

import json
import os
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field

from srunx.logging import get_logger
from srunx.models import ContainerResource

logger = get_logger(__name__)


class ResourceDefaults(BaseModel):
    """Default resource configuration."""

    nodes: int = Field(default=1, ge=1, description="Default number of compute nodes")
    gpus_per_node: int = Field(
        default=0, ge=0, description="Default number of GPUs per node"
    )
    ntasks_per_node: int = Field(
        default=1, ge=1, description="Default number of tasks per node"
    )
    cpus_per_task: int = Field(
        default=1, ge=1, description="Default number of CPUs per task"
    )
    memory_per_node: str | None = Field(
        default=None, description="Default memory per node"
    )
    time_limit: str | None = Field(default=None, description="Default time limit")
    nodelist: str | None = Field(default=None, description="Default nodelist")
    partition: str | None = Field(default=None, description="Default partition")


class EnvironmentDefaults(BaseModel):
    """Default environment configuration."""

    conda: str | None = Field(default=None, description="Default conda environment")
    venv: str | None = Field(
        default=None, description="Default virtual environment path"
    )
    container: ContainerResource | None = Field(
        default=None, description="Default container resource"
    )
    env_vars: dict[str, str] = Field(
        default_factory=dict, description="Default environment variables"
    )


class SrunxConfig(BaseModel):
    """Main srunx configuration."""

    resources: ResourceDefaults = Field(default_factory=ResourceDefaults)
    environment: EnvironmentDefaults = Field(default_factory=EnvironmentDefaults)
    log_dir: str = Field(default="logs", description="Default log directory")
    work_dir: str | None = Field(default=None, description="Default working directory")


def get_config_paths() -> list[Path]:
    """Get configuration file paths in order of precedence (lowest to highest)."""
    paths = []

    # System-wide config (for pip installations)
    # On Unix: /etc/srunx/config.json
    # On Windows: C:\ProgramData\srunx\config.json
    if os.name == "posix":
        paths.append(Path("/etc/srunx/config.json"))
    else:
        paths.append(Path("C:/ProgramData/srunx/config.json"))

    # User-wide config
    # On Unix: ~/.config/srunx/config.json
    # On Windows: ~/AppData/Roaming/srunx/config.json
    if os.name == "posix":
        user_config_dir = Path.home() / ".config" / "srunx"
    else:
        user_config_dir = Path.home() / "AppData" / "Roaming" / "srunx"
    paths.append(user_config_dir / "config.json")

    # Project-wide config (current working directory)
    paths.append(Path.cwd() / ".srunx.json")
    paths.append(Path.cwd() / "srunx.json")

    return paths


def load_config_from_file(config_path: Path) -> dict[str, Any]:
    """Load configuration from a JSON file."""
    try:
        if config_path.exists():
            logger.debug(f"Loading config from {config_path}")
            with open(config_path, encoding="utf-8") as f:
                return json.load(f)
    except (OSError, json.JSONDecodeError) as e:
        logger.warning(f"Failed to load config from {config_path}: {e}")
    return {}


def merge_config(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    """Recursively merge configuration dictionaries."""
    result = base.copy()

    for key, value in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = merge_config(result[key], value)
        else:
            result[key] = value

    return result


def load_config_from_env() -> dict[str, Any]:
    """Load configuration from environment variables."""
    config: dict[str, Any] = {}

    # Resource defaults from environment
    resources: dict[str, Any] = {}
    if nodes := os.getenv("SRUNX_DEFAULT_NODES"):
        try:
            resources["nodes"] = int(nodes)
        except ValueError:
            logger.warning(f"Invalid SRUNX_DEFAULT_NODES value: {nodes}")

    if gpus := os.getenv("SRUNX_DEFAULT_GPUS_PER_NODE"):
        try:
            resources["gpus_per_node"] = int(gpus)
        except ValueError:
            logger.warning(f"Invalid SRUNX_DEFAULT_GPUS_PER_NODE value: {gpus}")

    if ntasks := os.getenv("SRUNX_DEFAULT_NTASKS_PER_NODE"):
        try:
            resources["ntasks_per_node"] = int(ntasks)
        except ValueError:
            logger.warning(f"Invalid SRUNX_DEFAULT_NTASKS_PER_NODE value: {ntasks}")

    if cpus := os.getenv("SRUNX_DEFAULT_CPUS_PER_TASK"):
        try:
            resources["cpus_per_task"] = int(cpus)
        except ValueError:
            logger.warning(f"Invalid SRUNX_DEFAULT_CPUS_PER_TASK value: {cpus}")

    if memory := os.getenv("SRUNX_DEFAULT_MEMORY_PER_NODE"):
        resources["memory_per_node"] = memory

    if time_limit := os.getenv("SRUNX_DEFAULT_TIME_LIMIT"):
        resources["time_limit"] = time_limit

    if nodelist := os.getenv("SRUNX_DEFAULT_NODELIST"):
        resources["nodelist"] = nodelist

    if partition := os.getenv("SRUNX_DEFAULT_PARTITION"):
        resources["partition"] = partition

    if resources:
        config["resources"] = resources

    # Environment defaults from environment
    environment: dict[str, Any] = {}
    if conda := os.getenv("SRUNX_DEFAULT_CONDA"):
        environment["conda"] = conda

    if venv := os.getenv("SRUNX_DEFAULT_VENV"):
        environment["venv"] = venv

    if container := os.getenv("SRUNX_DEFAULT_CONTAINER"):
        environment["container"]["image"] = container

    if environment:
        config["environment"] = environment

    # General defaults from environment
    if log_dir := os.getenv("SRUNX_DEFAULT_LOG_DIR"):
        config["log_dir"] = log_dir

    if work_dir := os.getenv("SRUNX_DEFAULT_WORK_DIR"):
        config["work_dir"] = work_dir

    return config


def load_config() -> SrunxConfig:
    """Load configuration from all sources in order of precedence."""
    # Start with empty config
    config_data: dict[str, Any] = {}

    # Load from config files (lowest to highest precedence)
    for config_path in get_config_paths():
        file_config = load_config_from_file(config_path)
        if file_config:
            config_data = merge_config(config_data, file_config)

    # Override with environment variables (highest precedence)
    env_config = load_config_from_env()
    if env_config:
        config_data = merge_config(config_data, env_config)

    # Create and validate config
    try:
        return SrunxConfig.model_validate(config_data)
    except Exception as e:
        logger.warning(f"Failed to validate config: {e}. Using defaults.")
        return SrunxConfig()


def save_user_config(config: SrunxConfig) -> None:
    """Save configuration to user config file."""
    config_paths = get_config_paths()
    # Use the user-wide config path (second in the list)
    user_config_path = config_paths[1]

    # Create directory if it doesn't exist
    user_config_path.parent.mkdir(parents=True, exist_ok=True)

    # Save config
    try:
        with open(user_config_path, "w", encoding="utf-8") as f:
            json.dump(config.model_dump(exclude_unset=True), f, indent=2)
        logger.info(f"Configuration saved to {user_config_path}")
    except OSError as e:
        logger.error(f"Failed to save config to {user_config_path}: {e}")


def create_example_config() -> str:
    """Create an example configuration file content."""
    example_config = {
        "resources": {
            "nodes": 1,
            "gpus_per_node": 1,
            "ntasks_per_node": 1,
            "cpus_per_task": 8,
            "memory_per_node": "32GB",
            "time_limit": "2:00:00",
            "partition": "gpu",
        },
        "environment": {
            "conda": "ml_env",
            "env_vars": {"CUDA_VISIBLE_DEVICES": "0", "OMP_NUM_THREADS": "8"},
        },
        "log_dir": "slurm_logs",
        "work_dir": "/scratch/username",
    }
    return json.dumps(example_config, indent=2)


# Global config instance
_config: SrunxConfig | None = None


def get_config(reload: bool = False) -> SrunxConfig:
    """Get the global configuration instance."""
    global _config
    if _config is None or reload:
        _config = load_config()
    return _config
