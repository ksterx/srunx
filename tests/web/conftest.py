"""Shared setup for web API tests.

Starlette's ``TestClient`` sends ``Host: testserver``. The API's
anti-DNS-rebinding middleware (``WebSecurityMiddleware``) only accepts loopback
Host headers by default, so allow ``testserver`` for the test environment —
the same mechanism a real deployment uses to whitelist its reverse-proxy
hostname via ``SRUNX_WEB_ALLOWED_HOSTS``. Set at import time so it is present
before any per-test fixture rebuilds ``WebConfig``.
"""

import os

os.environ.setdefault("SRUNX_WEB_ALLOWED_HOSTS", "testserver")
