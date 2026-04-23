"""Configuration management for srunx."""

import json
import os
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field

from srunx.domain.jobs import ContainerResource
from srunx.logging import get_logger

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


class NotificationConfig(BaseModel):
    """Notification configuration.

    ``slack_webhook_url`` is DEPRECATED starting with the notification-and-state
    -persistence rewrite. On first startup after upgrade, its value is
    bootstrapped into the ``endpoints`` table (kind='slack_webhook',
    name='default') and thereafter ignored. New integrations should
    manage endpoints via the ``/api/endpoints`` API or the Settings UI.
    """

    slack_webhook_url: str | None = Field(
        default=None,
        description=(
            "DEPRECATED: Slack webhook URL. Kept only for one-time migration "
            "into the endpoints table; prefer Settings → Notifications."
        ),
    )
    default_endpoint_name: str | None = Field(
        default=None,
        description=(
            "Default endpoint name pre-selected in the submit dialog. Must "
            "match an existing endpoint row's ``name`` column. Null = no "
            "preselection."
        ),
    )
    default_preset: str = Field(
        default="terminal",
        description=(
            "Default subscription preset for new submissions. One of "
            "'terminal', 'running_and_terminal', 'all', 'digest'."
        ),
    )


class CliTransportConfig(BaseModel):
    """CLI transport resolution behaviour (REQ-1, Phase 2)."""

    use_current_profile: bool = Field(
        default=True,
        description=(
            "When True (default), top-level CLI commands (submit / cancel / "
            "status / list / logs / flow run / monitor jobs) fall back to the "
            "active SSH profile set via 'srunx ssh profile set <name>' when "
            "no explicit --profile / --local / $SRUNX_SSH_PROFILE is given. "
            "Set to False to force pre-Phase-2 behaviour where the CLI only "
            "routes through SSH on an explicit flag or env var."
        ),
    )


class SyncDefaults(BaseModel):
    """Workspace sync behaviour for job submission.

    srunx can rsync the user's local mount to the remote cluster before
    invoking sbatch, so that submitted jobs see the freshest source
    files. This section controls the defaults. Per-invocation overrides
    (``srunx sbatch --sync`` / ``--no-sync``) always win.
    """

    auto: bool = Field(
        default=True,
        description=(
            "Run rsync on the script's enclosing mount before sbatch. "
            "Disable (false) when you manage sync yourself, or when the "
            "mount sits on shared storage already."
        ),
    )
    lock_timeout_seconds: int = Field(
        default=120,
        ge=1,
        description=(
            "Maximum time (seconds) to wait for the per-(profile,mount) "
            "sync lock before aborting. Protects against two concurrent "
            "rsyncs fighting over the same tree. Increase for huge trees "
            "or slow networks."
        ),
    )
    warn_dirty: bool = Field(
        default=True,
        description=(
            "Log a concise warning before sync if the mount has "
            "uncommitted git changes. Informational only — does not "
            "block submission."
        ),
    )
    require_clean: bool = Field(
        default=False,
        description=(
            "Refuse to sync when the mount has uncommitted git changes. "
            "Recommended for CI / shared-workstation scenarios."
        ),
    )
    owner_check: bool = Field(
        default=True,
        description=(
            "Refuse to sync when the remote ``.srunx-owner.json`` marker "
            "shows a different machine last touched the mount. Catches "
            "the cross-workstation overwrite footgun (#137 part 4). "
            "Override per-invocation with ``--force-sync`` or disable "
            "globally for solo-machine setups."
        ),
    )
    verify_remote_hash: bool = Field(
        default=False,
        description=(
            "After auto-sync, SHA-256 the script we're about to "
            "``sbatch`` on both ends and abort on mismatch (#137 part 5). "
            "Catches silent rsync failures — a stray exclude rule, a "
            "path-translation bug, an incremental-algorithm hiccup — "
            "where rsync exits 0 but the file we cared about never "
            "actually reached the cluster. Off by default because it "
            "adds an ssh round-trip per submit; enable for CI / "
            "shared-cluster setups where silently submitting stale "
            "bytes is unacceptable."
        ),
    )


class SrunxConfig(BaseModel):
    """Main srunx configuration."""

    resources: ResourceDefaults = Field(default_factory=ResourceDefaults)
    environment: EnvironmentDefaults = Field(default_factory=EnvironmentDefaults)
    notifications: NotificationConfig = Field(default_factory=NotificationConfig)
    cli: CliTransportConfig = Field(default_factory=CliTransportConfig)
    sync: SyncDefaults = Field(default_factory=SyncDefaults)
    log_dir: str = Field(default="logs", description="Default log directory")
    work_dir: str | None = Field(default=None, description="Default working directory")


def _user_config_dir() -> Path:
    """Return the XDG-compliant per-user srunx config directory.

    Resolution order:

    1. ``$XDG_CONFIG_HOME/srunx`` when the env var is set (POSIX spec).
    2. ``~/.config/srunx`` on POSIX fallback.
    3. ``~/AppData/Roaming/srunx`` on Windows.

    Matches :func:`srunx.db.connection.get_config_dir` so that the
    state DB and the JSON config land under the same root — flipping
    ``XDG_CONFIG_HOME`` isolates both in one go (tests rely on this).
    """
    xdg = os.environ.get("XDG_CONFIG_HOME")
    if xdg:
        return Path(xdg).expanduser() / "srunx"
    if os.name == "posix":
        return Path.home() / ".config" / "srunx"
    return Path.home() / "AppData" / "Roaming" / "srunx"


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

    # User-wide config (honours XDG_CONFIG_HOME on POSIX)
    paths.append(_user_config_dir() / "config.json")

    # Project-wide config (current working directory)
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
        environment["container"] = {"image": container}

    if container_runtime := os.getenv("SRUNX_DEFAULT_CONTAINER_RUNTIME"):
        # Only override runtime on an existing container config —
        # runtime alone (without image) is not a valid container.
        if "container" in environment:
            environment["container"]["runtime"] = container_runtime

    if environment:
        config["environment"] = environment

    # General defaults from environment
    if log_dir := os.getenv("SRUNX_DEFAULT_LOG_DIR"):
        config["log_dir"] = log_dir

    if work_dir := os.getenv("SRUNX_DEFAULT_WORK_DIR"):
        config["work_dir"] = work_dir

    # Sync defaults from environment
    sync: dict[str, Any] = {}
    if (auto := os.getenv("SRUNX_SYNC_AUTO")) is not None:
        sync["auto"] = auto.lower() in ("1", "true", "yes", "on")
    if (timeout := os.getenv("SRUNX_SYNC_LOCK_TIMEOUT")) is not None:
        try:
            sync["lock_timeout_seconds"] = int(timeout)
        except ValueError:
            logger.warning(f"Invalid SRUNX_SYNC_LOCK_TIMEOUT value: {timeout}")
    if (warn := os.getenv("SRUNX_SYNC_WARN_DIRTY")) is not None:
        sync["warn_dirty"] = warn.lower() in ("1", "true", "yes", "on")
    if (clean := os.getenv("SRUNX_SYNC_REQUIRE_CLEAN")) is not None:
        sync["require_clean"] = clean.lower() in ("1", "true", "yes", "on")
    if (owner := os.getenv("SRUNX_SYNC_OWNER_CHECK")) is not None:
        sync["owner_check"] = owner.lower() in ("1", "true", "yes", "on")
    if (verify := os.getenv("SRUNX_SYNC_VERIFY_REMOTE_HASH")) is not None:
        sync["verify_remote_hash"] = verify.lower() in ("1", "true", "yes", "on")
    if sync:
        config["sync"] = sync

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
    """Save configuration to user config file.

    Merges SrunxConfig fields into the existing file so that
    SSH profile data (managed by ConfigManager) is preserved.
    """
    config_paths = get_config_paths()
    # Use the user-wide config path (second in the list)
    user_config_path = config_paths[1]

    # Create directory if it doesn't exist
    user_config_path.parent.mkdir(parents=True, exist_ok=True)

    # Load existing data to preserve non-SrunxConfig keys (e.g. SSH profiles)
    existing: dict[str, Any] = {}
    if user_config_path.exists():
        try:
            with open(user_config_path, encoding="utf-8") as f:
                content = f.read().strip()
                if content:
                    existing = json.loads(content)
        except (OSError, json.JSONDecodeError):
            pass

    # Merge: SrunxConfig fields overwrite, other keys preserved
    existing.update(config.model_dump(exclude_unset=True))

    # Save config
    try:
        with open(user_config_path, "w", encoding="utf-8") as f:
            json.dump(existing, f, indent=2)
        logger.info(f"Configuration saved to {user_config_path}")
    except OSError as e:
        logger.error(f"Failed to save config to {user_config_path}: {e}")
        raise


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
            "container": {
                "image": "nvcr.io/nvidia/pytorch:24.01-py3",
                "runtime": "pyxis",
            },
            "env_vars": {"CUDA_VISIBLE_DEVICES": "0", "OMP_NUM_THREADS": "8"},
        },
        "log_dir": "slurm_logs",
        "work_dir": "/scratch/username",
    }
    return json.dumps(example_config, indent=2)


# Global config instance
_config: SrunxConfig | None = None
_config_lock = __import__("threading").Lock()


def get_config(reload: bool = False) -> SrunxConfig:
    """Get the global configuration instance."""
    global _config
    if _config is None or reload:
        with _config_lock:
            if _config is None or reload:
                _config = load_config()
    return _config
