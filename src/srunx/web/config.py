from __future__ import annotations

import os
from pathlib import Path

from pydantic import BaseModel, Field, model_validator


class WebConfig(BaseModel):
    """Web server configuration."""

    host: str = Field(default="127.0.0.1")
    port: int = Field(default=8000)
    cors_origins: list[str] = Field(default_factory=lambda: ["http://localhost:3000"])

    # Optional bearer token required on all /api/* routes. Unset by default so
    # the local (127.0.0.1) experience is unchanged; required when exposing the
    # server on a non-loopback host. Set via SRUNX_WEB_TOKEN.
    auth_token: str | None = Field(
        default_factory=lambda: os.getenv("SRUNX_WEB_TOKEN") or None
    )
    # Extra Host header values accepted by the anti-DNS-rebinding check
    # (loopback names are always allowed). Set via SRUNX_WEB_ALLOWED_HOSTS
    # (comma-separated).
    allowed_hosts: list[str] = Field(
        default_factory=lambda: [
            h.strip()
            for h in os.getenv("SRUNX_WEB_ALLOWED_HOSTS", "").split(",")
            if h.strip()
        ]
    )

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

    @model_validator(mode="after")
    def expand_ssh_key_filename(self) -> WebConfig:
        """Expand ~ so paramiko receives a concrete path (covers both
        explicit args and the SRUNX_SSH_KEY default_factory value)."""
        if self.ssh_key_filename:
            self.ssh_key_filename = str(Path(self.ssh_key_filename).expanduser())
        return self


_config: WebConfig | None = None
_config_lock = __import__("threading").Lock()


def get_web_config() -> WebConfig:
    global _config
    if _config is None:
        with _config_lock:
            if _config is None:
                _config = WebConfig()
    return _config
