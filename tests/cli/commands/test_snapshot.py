"""Tests for ``srunx snapshot create``."""

from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest
from typer.testing import CliRunner

from srunx.cli.main import app
from srunx.snapshot import PreparedSnapshot, SnapshotPlan, SnapshotRemoteError
from srunx.snapshot.errors import SnapshotValidationError

SHA = "c" * 40
PATH = f"/r/repo-123456789abc/trees/{SHA}"


def _plan() -> SnapshotPlan:
    return SnapshotPlan(
        repo_top=Path("/repo"),
        repo_id="repo-123456789abc",
        sha=SHA,
        script_rel=None,
        script_text=None,
        cwd_rel=None,
        warnings=("1 untracked file(s) are not part of the snapshot",),
    )


class _Service:
    created: list[Any] = []
    error: Exception | None = None
    leased_called = False

    def __init__(self, **kw: Any) -> None:
        self.kw = kw

    def create(self, plan: SnapshotPlan) -> PreparedSnapshot:
        if _Service.error is not None:
            raise _Service.error
        _Service.created.append(plan)
        return PreparedSnapshot(
            plan=plan,
            root="/r",
            repo_dir="/r/repo-123456789abc",
            path=PATH,
            status="created",
        )

    def leased_snapshot(self, plan: SnapshotPlan) -> None:  # pragma: no cover
        _Service.leased_called = True
        raise AssertionError("snapshot create must not take a lease")


@pytest.fixture
def env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / "xdg"))
    monkeypatch.delenv("SRUNX_SSH_PROFILE", raising=False)
    _Service.created = []
    _Service.error = None
    _Service.leased_called = False
    state: dict[str, Any] = {"plan_kw": None, "plan_error": None, "transport": "ssh"}

    def fake_plan(**kw: Any) -> SnapshotPlan:
        state["plan_kw"] = kw
        if state["plan_error"] is not None:
            raise state["plan_error"]
        return _plan()

    @contextmanager
    def fake_rt(**_kw: Any):
        rt = MagicMock()
        rt.transport_type = state["transport"]
        rt.profile_name = "p" if state["transport"] == "ssh" else None
        yield rt

    monkeypatch.setattr("srunx.cli.commands.snapshot.plan_snapshot", fake_plan)
    monkeypatch.setattr("srunx.cli.commands.snapshot.resolve_transport", fake_rt)
    monkeypatch.setattr("srunx.cli.commands.snapshot.SnapshotService", _Service)
    cfg = MagicMock()
    cfg.return_value.get_profile.return_value = MagicMock()
    monkeypatch.setattr("srunx.ssh.core.config.ConfigManager", cfg)
    return state


def test_snapshot_create_stdout_is_exactly_the_path(env: dict[str, Any]) -> None:
    result = CliRunner().invoke(app, ["snapshot", "create", "v1", "--profile", "p"])
    assert result.exit_code == 0, result.output
    assert result.stdout == PATH + "\n"
    assert result.stderr.count("Snapshot ") == 1  # status line once, not also logged
    assert "untracked" in result.stderr
    assert env["plan_kw"]["rev"] == "v1"
    assert env["plan_kw"]["script"] is None
    assert len(_Service.created) == 1


def test_snapshot_create_writes_no_lease(env: dict[str, Any]) -> None:
    result = CliRunner().invoke(app, ["snapshot", "create", "--profile", "p"])
    assert result.exit_code == 0
    assert _Service.leased_called is False
    assert env["plan_kw"]["rev"] is None


def test_snapshot_create_rejects_local_transport(env: dict[str, Any]) -> None:
    env["transport"] = "local"
    result = CliRunner().invoke(app, ["snapshot", "create", "--local"])
    assert result.exit_code == 2
    assert result.stdout == ""
    assert _Service.created == []


def test_snapshot_create_validation_error_exits_2(env: dict[str, Any]) -> None:
    env["plan_error"] = SnapshotValidationError("tracked changes")
    result = CliRunner().invoke(app, ["snapshot", "create", "--profile", "p"])
    assert result.exit_code == 2
    assert result.stdout == ""
    assert _Service.created == []


def test_snapshot_create_service_validation_error_exits_2(
    env: dict[str, Any],
) -> None:
    _Service.error = SnapshotValidationError("root inside mount")
    result = CliRunner().invoke(app, ["snapshot", "create", "--profile", "p"])
    assert result.exit_code == 2
    assert result.stdout == ""


def test_snapshot_create_remote_error_exits_1(env: dict[str, Any]) -> None:
    _Service.error = SnapshotRemoteError("push failed")
    result = CliRunner().invoke(app, ["snapshot", "create", "--profile", "p"])
    assert result.exit_code == 1
    assert result.stdout == ""
    assert "push failed" in result.stderr
