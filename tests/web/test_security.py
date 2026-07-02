"""Tests for the Web API security middleware and bind guard (#216)."""

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from srunx.web.security import (
    InsecureBindError,
    WebSecurityMiddleware,
    assert_safe_bind,
    is_loopback_bind,
)


def _app(*, auth_token=None, bind_host="127.0.0.1", allowed_hosts=None) -> FastAPI:
    app = FastAPI()

    @app.get("/api/ping")
    def ping() -> dict:
        return {"ok": True}

    @app.get("/healthz")
    def health() -> dict:
        return {"ok": True}

    app.add_middleware(
        WebSecurityMiddleware,
        auth_token=auth_token,
        bind_host=bind_host,
        allowed_hosts=allowed_hosts or [],
    )
    return app


class TestHostHeaderCheck:
    def test_loopback_host_allowed(self):
        c = TestClient(_app(), base_url="http://127.0.0.1")
        assert c.get("/api/ping").status_code == 200

    def test_rebinding_host_rejected(self):
        c = TestClient(_app(), base_url="http://attacker.example.com")
        assert c.get("/api/ping").status_code == 403

    def test_allowed_hosts_extra(self):
        c = TestClient(
            _app(allowed_hosts=["myproxy.internal"]),
            base_url="http://myproxy.internal",
        )
        assert c.get("/api/ping").status_code == 200

    def test_non_api_path_not_host_checked(self):
        c = TestClient(_app(), base_url="http://attacker.example.com")
        assert c.get("/healthz").status_code == 200

    def test_host_check_skipped_when_bound_non_loopback(self):
        # Exposed bind: host allowlisting can't enumerate the public name, so
        # the token (not the Host header) is the control.
        c = TestClient(
            _app(bind_host="0.0.0.0", auth_token="s3cret"),
            base_url="http://anything",
        )
        assert (
            c.get("/api/ping", headers={"Authorization": "Bearer s3cret"}).status_code
            == 200
        )


class TestTokenCheck:
    def test_missing_token_rejected(self):
        c = TestClient(_app(auth_token="s3cret"), base_url="http://127.0.0.1")
        assert c.get("/api/ping").status_code == 401

    def test_wrong_token_rejected(self):
        c = TestClient(_app(auth_token="s3cret"), base_url="http://127.0.0.1")
        r = c.get("/api/ping", headers={"Authorization": "Bearer nope"})
        assert r.status_code == 401

    def test_correct_token_accepted(self):
        c = TestClient(_app(auth_token="s3cret"), base_url="http://127.0.0.1")
        r = c.get("/api/ping", headers={"Authorization": "Bearer s3cret"})
        assert r.status_code == 200

    def test_no_token_configured_allows_loopback(self):
        c = TestClient(_app(auth_token=None), base_url="http://127.0.0.1")
        assert c.get("/api/ping").status_code == 200


class TestBindGuard:
    def test_is_loopback_bind(self):
        assert is_loopback_bind("127.0.0.1")
        assert is_loopback_bind("localhost")
        assert not is_loopback_bind("0.0.0.0")

    def test_non_loopback_without_token_refused(self):
        with pytest.raises(InsecureBindError):
            assert_safe_bind("0.0.0.0", None)

    def test_non_loopback_with_token_ok(self):
        assert_safe_bind("0.0.0.0", "s3cret")  # no raise

    def test_loopback_without_token_ok(self):
        assert_safe_bind("127.0.0.1", None)  # no raise
