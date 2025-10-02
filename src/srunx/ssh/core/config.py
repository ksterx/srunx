import json
import os
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field


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


class ConfigManager:
    def __init__(self, config_path: str | None = None):
        self.config_path = (
            Path(config_path) if config_path else self._get_default_config_path()
        )
        self.config_data: dict[str, Any] = {}
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
            except (OSError, json.JSONDecodeError) as e:
                raise RuntimeError(
                    f"Failed to load config from {self.config_path}: {e}"
                ) from e
        else:
            self.config_data = {"current_profile": None, "profiles": {}}
            self.save_config()

    def save_config(self) -> None:
        try:
            with open(self.config_path, "w") as f:
                json.dump(self.config_data, f, indent=2)
        except OSError as e:
            raise RuntimeError(
                f"Failed to save config to {self.config_path}: {e}"
            ) from e

    def add_profile(self, name: str, profile: ServerProfile) -> None:
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
        profiles = self.config_data.get("profiles", {})
        if name in profiles:
            return ServerProfile.model_validate(profiles[name])
        return None

    def list_profiles(self) -> dict[str, ServerProfile]:
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
