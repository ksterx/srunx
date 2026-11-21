"""Tests for first-sync target approval (:mod:`srunx.sync.approval`)."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest

from srunx.common.config import SyncDefaults
from srunx.ssh.core.config import MountConfig, ServerProfile
from srunx.sync import approval
from srunx.sync.service import SyncAbortedError, ensure_mount_synced


def _profile(tmp_path: Path) -> ServerProfile:
    local = tmp_path / "ml"
    local.mkdir()
    key = tmp_path / "id_rsa"
    key.write_text("dummy")
    return ServerProfile(
        hostname="h",
        username="u",
        key_filename=str(key),
        mounts=(MountConfig(name="ml", local=str(local), remote="/r/ml"),),
    )


def test_unapproved_non_interactive_raises() -> None:
    with patch("sys.stdin.isatty", return_value=False):
        with pytest.raises(approval.SyncNotApprovedError):
            approval.ensure_approved("c", "/l", "/r")
    assert not approval.is_approved("c", "/r")


def test_assume_yes_records_approval() -> None:
    approval.ensure_approved("c", "/l", "/r", assume_yes=True)
    assert approval.is_approved("c", "/r")
    assert not approval.is_approved("other", "/r")
    # Subsequent calls pass without assume_yes.
    with patch("sys.stdin.isatty", return_value=False):
        approval.ensure_approved("c", "/l", "/r")


def test_first_sync_requires_approval_then_remembers(tmp_path: Path) -> None:
    profile = _profile(tmp_path)
    config = SyncDefaults(owner_check=False)
    with (
        patch("srunx.sync.service.sync_mount_by_name") as fake_rsync,
        patch("srunx.sync.service.is_dirty_git_worktree", return_value=(False, "")),
        patch("sys.stdin.isatty", return_value=False),
    ):
        with pytest.raises(SyncAbortedError, match="needs approval"):
            ensure_mount_synced(
                profile_name="alice",
                profile=profile,
                mount=profile.mounts[0],
                config=config,
            )
        fake_rsync.assert_not_called()

        ensure_mount_synced(
            profile_name="alice",
            profile=profile,
            mount=profile.mounts[0],
            config=config,
            assume_yes=True,
        )
        ensure_mount_synced(
            profile_name="alice",
            profile=profile,
            mount=profile.mounts[0],
            config=config,
        )
    assert fake_rsync.call_count == 2


def test_sync_false_mount_is_never_rsynced(tmp_path: Path) -> None:
    profile = _profile(tmp_path)
    mount = profile.mounts[0].model_copy(update={"sync": False})
    with (
        patch("srunx.sync.service.sync_mount_by_name") as fake_rsync,
        patch("srunx.sync.service.is_dirty_git_worktree", return_value=(False, "")),
    ):
        outcome = ensure_mount_synced(
            profile_name="alice",
            profile=profile,
            mount=mount,
            config=SyncDefaults(owner_check=False),
            assume_yes=True,
        )
    fake_rsync.assert_not_called()
    assert outcome.performed is False
