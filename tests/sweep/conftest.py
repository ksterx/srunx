"""Shared fixtures for sweep orchestrator / reconciler tests."""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import pytest


@pytest.fixture
def isolated_db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[Path]:
    """Redirect the srunx config directory to a per-test tmp path.

    Orchestrator + reconciler code opens fresh connections via
    :func:`srunx.observability.storage.connection.open_connection` (which honours
    ``XDG_CONFIG_HOME``). Pointing the env var at a tmp dir keeps every
    test file-backed and fully isolated from the real user DB.
    """
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path))

    from srunx.observability.storage.connection import init_db

    db_path = init_db(delete_legacy=False)
    yield db_path
