from __future__ import annotations

import os

from pydantic import BaseModel, Field


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

    # Project roots the UI may act on (``srunx ui --project``, or
    # ``SRUNX_WEB_PROJECTS`` separated by ``os.pathsep``). The project
    # discovered from the server's cwd is added at startup.
    projects: list[str] = Field(
        default_factory=lambda: [
            p for p in os.getenv("SRUNX_WEB_PROJECTS", "").split(os.pathsep) if p
        ]
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
