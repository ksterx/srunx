from __future__ import annotations

import os

from pydantic import BaseModel, Field


class WebConfig(BaseModel):
    """Web server configuration."""

    host: str = Field(default="127.0.0.1")
    port: int = Field(default=8000)
    cors_origins: list[str] = Field(default_factory=lambda: ["http://localhost:3000"])

    # SSH connection — either profile_name or (hostname + username)
    ssh_profile: str | None = Field(
        default_factory=lambda: os.getenv("SRUNX_SSH_PROFILE")
    )
    ssh_hostname: str | None = Field(
        default_factory=lambda: os.getenv("SRUNX_SSH_HOSTNAME")
    )
    ssh_username: str | None = Field(
        default_factory=lambda: os.getenv("SRUNX_SSH_USERNAME")
    )
    ssh_key_filename: str | None = Field(
        default_factory=lambda: os.getenv("SRUNX_SSH_KEY")
    )
    ssh_port: int = Field(
        default_factory=lambda: int(os.getenv("SRUNX_SSH_PORT", "22"))
    )

    # UI verbosity — when False, lifespan suppresses info logs in favour of a banner.
    verbose: bool = Field(default=False)


_config: WebConfig | None = None
_config_lock = __import__("threading").Lock()


def get_web_config() -> WebConfig:
    global _config
    if _config is None:
        with _config_lock:
            if _config is None:
                _config = WebConfig()
    return _config
