"""Request-time security controls for the srunx Web API.

The API performs high-impact, state-changing operations (submitting cluster
jobs, reading files, managing SSH profiles). Two lightweight controls guard it
without changing the default local experience:

1. **Anti-DNS-rebinding** — when bound to loopback (the default), only requests
   whose ``Host`` header is a loopback name are accepted on ``/api/*``. A
   malicious web page that rebinds its domain to 127.0.0.1 sends ``Host:
   attacker.com`` and is rejected. Cross-origin reads were already impossible
   via CORS; this closes the state-changing drive-by.
2. **Bearer token** — when ``auth_token`` is set (``SRUNX_WEB_TOKEN``), every
   ``/api/*`` request must carry ``Authorization: Bearer <token>``. A token is
   required to expose the server on a non-loopback host (enforced at startup),
   so the local no-token flow is preserved while network exposure is protected.
"""

from __future__ import annotations

import hmac

from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.types import ASGIApp

_LOOPBACK_HOSTS = frozenset({"localhost", "127.0.0.1", "::1", "[::1]"})


def is_loopback_bind(host: str) -> bool:
    """True if binding ``host`` only exposes the server to the local machine."""
    return host in {"127.0.0.1", "::1", "localhost"}


class InsecureBindError(RuntimeError):
    """Raised when exposing the API on a non-loopback host without auth."""


def assert_safe_bind(host: str, auth_token: str | None) -> None:
    """Refuse to expose an unauthenticated API on a non-loopback interface.

    Binding to e.g. ``0.0.0.0`` puts every state-changing endpoint (cluster
    job submission, file read, SSH-profile control) on the network. Require a
    bearer token in that case.
    """
    if not is_loopback_bind(host) and not auth_token:
        raise InsecureBindError(
            f"Refusing to bind the srunx Web API to '{host}' without "
            "authentication. Set SRUNX_WEB_TOKEN to a secret value to require "
            "a bearer token on all /api/* requests, or bind to 127.0.0.1 for "
            "local-only use."
        )


def _host_without_port(host_header: str) -> str:
    """Strip a trailing ``:port`` from a Host header, keeping ``[::1]`` intact."""
    if not host_header:
        return ""
    if host_header.startswith("["):  # IPv6 literal, e.g. [::1]:8000
        return host_header.split("]", 1)[0] + "]"
    return host_header.rsplit(":", 1)[0] if ":" in host_header else host_header


class WebSecurityMiddleware(BaseHTTPMiddleware):
    """Enforce Host-header and bearer-token checks on ``/api/*`` routes."""

    def __init__(
        self,
        app: ASGIApp,
        *,
        auth_token: str | None,
        bind_host: str,
        allowed_hosts: list[str],
    ) -> None:
        super().__init__(app)
        self.auth_token = auth_token
        self.enforce_host = is_loopback_bind(bind_host)
        self.allowed_hosts = _LOOPBACK_HOSTS | {h.lower() for h in allowed_hosts}

    async def dispatch(
        self, request: Request, call_next: RequestResponseEndpoint
    ) -> Response:
        path = request.url.path
        if path.startswith("/api/") and request.method != "OPTIONS":
            if self.enforce_host:
                hostname = _host_without_port(request.headers.get("host", "")).lower()
                if hostname not in self.allowed_hosts:
                    return JSONResponse(
                        status_code=403,
                        content={"detail": "Host header not allowed"},
                    )
            if self.auth_token:
                header = request.headers.get("authorization", "")
                token = header[7:] if header[:7].lower() == "bearer " else ""
                if not hmac.compare_digest(token, self.auth_token):
                    return JSONResponse(
                        status_code=401,
                        content={"detail": "Missing or invalid bearer token"},
                    )
        return await call_next(request)
