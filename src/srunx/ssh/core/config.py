import json
import os
import re
from pathlib import Path
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

# Shared profile-name regex. ``scheduler_key`` uses a colon-delimited
# grammar (``ssh:<profile>``) that has to round-trip through SQLite
# target_refs (``job:ssh:<profile>:<id>``), so profile names must not
# contain ``:``. Length is capped at 64 so oversized names cannot
# wedge UI tables or log lines.
_PROFILE_NAME_RE = re.compile(r"^[A-Za-z0-9_.\-]{1,64}$")


def validate_profile_name(name: str) -> None:
    """Validate *name* against the shared profile-name regex.

    Raises:
        ValueError: When *name* is empty, too long, or contains any
            character outside the allowed alphabet. ``':'`` is
            explicitly called out because it's the scheduler_key
            delimiter and the most likely accidental violation.
    """
    if not isinstance(name, str) or not _PROFILE_NAME_RE.match(name):
        raise ValueError(
            f"Invalid profile name {name!r}: must match "
            f"{_PROFILE_NAME_RE.pattern}. Profile names cannot contain "
            "':' (reserved for scheduler_key grammar) or exceed 64 characters."
        )


class MountConfig(BaseModel):
    """Local-to-remote path mapping for a project directory.

    Deeply immutable: ``frozen=True`` locks fields and ``exclude_patterns``
    is a tuple so nested state cannot be mutated either. This lets the
    model be safely embedded in ``frozen`` dataclasses (e.g. the SSH
    adapter pool spec) without a ``list[str]`` escape hatch.
    """

    model_config = ConfigDict(frozen=True)

    name: str
    local: str  # local path, e.g. "~/projects/ml-project" (resolved absolute after validation)
    remote: str  # remote path, must be absolute
    exclude_patterns: tuple[str, ...] = Field(
        default_factory=tuple,
        description="Additional rsync exclude patterns for this mount",
    )

    @field_validator("local", mode="before")
    @classmethod
    def _expand_local(cls, v: Any) -> Any:
        """Expand ``~`` and resolve to an absolute path."""
        if not isinstance(v, str):
            return v
        return str(Path(v).expanduser().resolve())

    @field_validator("remote")
    @classmethod
    def _validate_remote(cls, v: str) -> str:
        if not v.startswith("/"):
            raise ValueError(f"Mount remote path must be absolute: {v}")
        return v

    @field_validator("exclude_patterns", mode="before")
    @classmethod
    def _coerce_patterns_to_tuple(cls, v: Any) -> Any:
        # JSON / YAML round-trip produces list[str]; normalize so the stored
        # field is always a tuple and the model stays hashable-compatible.
        if isinstance(v, list):
            return tuple(v)
        return v


class ServerProfile(BaseModel):
    hostname: str = Field(..., description="The hostname of the server")
    username: str = Field(..., description="The username of the server")
    key_filename: str = Field(..., description="The key filename of the server")
    port: int = Field(22, description="The port of the server")
    description: str | None = Field(None, description="The description of the server")
    ssh_host: str | None = Field(
        None, description="The SSH config host name if using SSH config"
    )
    proxy_jump: str | None = Field(
        None, description="The ProxyJump host name if using ProxyJump"
    )
    env_vars: dict[str, str] | None = Field(
        None, description="The environment variables for this profile"
    )
    mounts: list[MountConfig] = Field(
        default=[], description="Local-to-remote path mappings for project directories"
    )

    @model_validator(mode="after")
    def expand_key_filename(self) -> "ServerProfile":
        """Expand ~ in key_filename so paramiko receives a concrete path."""
        if self.key_filename:
            self.key_filename = str(Path(self.key_filename).expanduser())
        return self


class ConfigManager:
    def __init__(self, config_path: str | None = None):
        self.config_path = (
            Path(config_path) if config_path else self._get_default_config_path()
        )
        self.config_data: dict[str, Any] = {}
        self._loaded_mtime: float | None = None
        self.load_config()

    def _get_default_config_path(self) -> Path:
        config_dir = Path.home() / ".config" / "srunx"
        config_dir.mkdir(parents=True, exist_ok=True)
        return config_dir / "config.json"

    def load_config(self) -> None:
        if self.config_path.exists():
            try:
                with open(self.config_path) as f:
                    content = f.read().strip()
                    if not content:
                        # Empty file, use defaults
                        self.config_data = {"current_profile": None, "profiles": {}}
                        self.save_config()
                    else:
                        self.config_data = json.loads(content)
                self._loaded_mtime = self.config_path.stat().st_mtime
            except (OSError, json.JSONDecodeError) as e:
                raise RuntimeError(
                    f"Failed to load config from {self.config_path}: {e}"
                ) from e
        else:
            self.config_data = {"current_profile": None, "profiles": {}}
            self.save_config()

    def _reload_if_stale(self) -> None:
        """Re-read the config file when its mtime has advanced.

        Long-lived :class:`ConfigManager` instances (e.g. held by the
        web app ``TransportRegistry``) must observe external edits
        (``srunx ssh profile add`` / ``remove``) so cached adapters do
        not keep routing to stale profiles. The stat call is ~µs and
        runs before each read-path lookup.
        """
        if self._loaded_mtime is None or not self.config_path.exists():
            return
        try:
            current_mtime = self.config_path.stat().st_mtime
        except OSError:
            return
        if current_mtime != self._loaded_mtime:
            self.load_config()

    def save_config(self) -> None:
        """Save SSH profile data, preserving non-SSH keys (e.g. SrunxConfig)."""
        try:
            # Load existing data to preserve SrunxConfig keys
            existing: dict[str, Any] = {}
            if self.config_path.exists():
                try:
                    with open(self.config_path) as f:
                        content = f.read().strip()
                        if content:
                            existing = json.loads(content)
                except (OSError, json.JSONDecodeError):
                    pass

            # Merge: SSH keys overwrite, other keys preserved
            existing.update(self.config_data)

            with open(self.config_path, "w") as f:
                json.dump(existing, f, indent=2)
        except OSError as e:
            raise RuntimeError(
                f"Failed to save config to {self.config_path}: {e}"
            ) from e

    def add_profile(self, name: str, profile: ServerProfile) -> None:
        validate_profile_name(name)
        if "profiles" not in self.config_data:
            self.config_data["profiles"] = {}

        self.config_data["profiles"][name] = profile.model_dump()
        self.save_config()

    def remove_profile(self, name: str) -> bool:
        if name in self.config_data.get("profiles", {}):
            del self.config_data["profiles"][name]

            if self.config_data.get("current_profile") == name:
                self.config_data["current_profile"] = None

            self.save_config()
            return True
        return False

    def get_profile(self, name: str) -> ServerProfile | None:
        self._reload_if_stale()
        profiles = self.config_data.get("profiles", {})
        if name in profiles:
            return ServerProfile.model_validate(profiles[name])
        return None

    def list_profiles(self) -> dict[str, ServerProfile]:
        self._reload_if_stale()
        profiles = {}
        for name, data in self.config_data.get("profiles", {}).items():
            profiles[name] = ServerProfile.model_validate(data)
        return profiles

    def set_current_profile(self, name: str) -> bool:
        if name in self.config_data.get("profiles", {}):
            self.config_data["current_profile"] = name
            self.save_config()
            return True
        return False

    def get_current_profile(self) -> ServerProfile | None:
        current_name = self.config_data.get("current_profile")
        if current_name:
            return self.get_profile(current_name)
        return None

    def get_current_profile_name(self) -> str | None:
        return self.config_data.get("current_profile")

    def update_profile(self, name: str, **kwargs) -> bool:
        if name in self.config_data.get("profiles", {}):
            profile_data = self.config_data["profiles"][name]

            for key, value in kwargs.items():
                if value is not None:  # Only update non-None values
                    profile_data[key] = value

            self.save_config()
            return True
        return False

    def expand_path(self, path: str) -> str:
        return os.path.expanduser(path)

    def set_profile_env_var(self, profile_name: str, key: str, value: str) -> bool:
        """Set an environment variable for a profile."""
        if profile_name in self.config_data.get("profiles", {}):
            profile_data = self.config_data["profiles"][profile_name]
            if "env_vars" not in profile_data:
                profile_data["env_vars"] = {}
            profile_data["env_vars"][key] = value
            self.save_config()
            return True
        return False

    def unset_profile_env_var(self, profile_name: str, key: str) -> bool:
        """Unset an environment variable for a profile."""
        if profile_name in self.config_data.get("profiles", {}):
            profile_data = self.config_data["profiles"][profile_name]
            env_vars = profile_data.get("env_vars", {})
            if key in env_vars:
                del env_vars[key]
                self.save_config()
                return True
        return False

    def add_profile_mount(self, profile_name: str, mount: MountConfig) -> bool:
        """Add a mount to a profile."""
        if profile_name in self.config_data.get("profiles", {}):
            profile_data = self.config_data["profiles"][profile_name]
            if "mounts" not in profile_data:
                profile_data["mounts"] = []
            profile_data["mounts"].append(mount.model_dump())
            self.save_config()
            return True
        return False

    def remove_profile_mount(self, profile_name: str, mount_name: str) -> bool:
        """Remove a mount from a profile by name."""
        if profile_name in self.config_data.get("profiles", {}):
            profile_data = self.config_data["profiles"][profile_name]
            mounts = profile_data.get("mounts", [])
            for i, m in enumerate(mounts):
                if m.get("name") == mount_name:
                    mounts.pop(i)
                    self.save_config()
                    return True
        return False
