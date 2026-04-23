"""Tests for :mod:`srunx.sync.service`.

Covers the service layer that ties dirty-tree detection, locking, and
the underlying rsync helper into a single ``ensure_mount_synced`` call
that the CLI consumes. We mock both ``sync_mount_by_name`` (rsync) and
``is_dirty_git_worktree`` (git) so the tests stay deterministic and
don't fight the repo-wide autouse ``subprocess.run`` patch in
``tests/conftest.py``. The git inspector itself is exercised by
``ensure_mount_synced``'s integration tests via the ``warn_dirty`` /
``require_clean`` paths.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest

from srunx.config import SyncDefaults
from srunx.ssh.core.config import MountConfig, ServerProfile
from srunx.sync.service import SyncAbortedError, ensure_mount_synced


@pytest.fixture(autouse=True)
def isolated_config_home(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / "xdg"))


def _profile(tmp_path: Path, mount_local: Path) -> ServerProfile:
    key = tmp_path / "id_rsa"
    key.write_text("dummy")
    return ServerProfile(
        hostname="h",
        username="u",
        key_filename=str(key),
        mounts=(MountConfig(name="ml", local=str(mount_local), remote="/r/ml"),),
    )


class TestEnsureMountSynced:
    """Pre-#137-part-4 behaviours: dirty-tree handling, lock + rsync wiring.

    These tests deliberately set ``owner_check=False`` because the
    owner-marker subsystem has its own test class
    (:class:`TestOwnerMarkerIntegration`) below — keeping the two
    concerns separate makes the dirty-tree / rsync assertions easier
    to read.
    """

    def test_happy_path_invokes_rsync(self, tmp_path: Path) -> None:
        mount_local = tmp_path / "ml"
        mount_local.mkdir()
        profile = _profile(tmp_path, mount_local)
        config = SyncDefaults(owner_check=False)

        with (
            patch("srunx.sync.service.sync_mount_by_name") as fake_rsync,
            patch(
                "srunx.sync.service.is_dirty_git_worktree",
                return_value=(False, ""),
            ),
        ):
            outcome = ensure_mount_synced(
                profile_name="alice",
                profile=profile,
                mount=profile.mounts[0],
                config=config,
            )

        # Auto-sync calls rsync with delete=False (Codex blocker #4):
        # mount-resident outputs/checkpoints must survive a sync.
        # ``verbose=False`` is the default surfaced by the CLI when the
        # user did not pass ``--verbose`` (#137 part 3).
        fake_rsync.assert_called_once_with(profile, "ml", delete=False, verbose=False)
        assert outcome.performed is True
        assert outcome.warnings == ()

    def test_dirty_warn_does_not_block(self, tmp_path: Path) -> None:
        mount_local = tmp_path / "ml"
        mount_local.mkdir()
        profile = _profile(tmp_path, mount_local)
        config = SyncDefaults(warn_dirty=True, require_clean=False, owner_check=False)

        with (
            patch("srunx.sync.service.sync_mount_by_name"),
            patch(
                "srunx.sync.service.is_dirty_git_worktree",
                return_value=(True, "?? untracked.txt"),
            ),
        ):
            outcome = ensure_mount_synced(
                profile_name="alice",
                profile=profile,
                mount=profile.mounts[0],
                config=config,
            )

        # Sync should still happen, but the warning surfaces back to
        # the caller so the CLI can echo it.
        assert outcome.performed is True
        assert any("uncommitted changes" in w for w in outcome.warnings)

    def test_dirty_with_require_clean_aborts(self, tmp_path: Path) -> None:
        mount_local = tmp_path / "ml"
        mount_local.mkdir()
        profile = _profile(tmp_path, mount_local)
        config = SyncDefaults(warn_dirty=False, require_clean=True, owner_check=False)

        with (
            patch("srunx.sync.service.sync_mount_by_name") as fake_rsync,
            patch(
                "srunx.sync.service.is_dirty_git_worktree",
                return_value=(True, "?? untracked.txt"),
            ),
        ):
            with pytest.raises(SyncAbortedError, match="uncommitted changes"):
                ensure_mount_synced(
                    profile_name="alice",
                    profile=profile,
                    mount=profile.mounts[0],
                    config=config,
                )

        # rsync must NOT have been invoked when require_clean fires.
        fake_rsync.assert_not_called()

    def test_clean_repo_no_warning(self, tmp_path: Path) -> None:
        """A clean tree produces no warning even when ``warn_dirty=True``."""
        mount_local = tmp_path / "ml"
        mount_local.mkdir()
        profile = _profile(tmp_path, mount_local)
        config = SyncDefaults(warn_dirty=True, owner_check=False)

        with (
            patch("srunx.sync.service.sync_mount_by_name"),
            patch(
                "srunx.sync.service.is_dirty_git_worktree",
                return_value=(False, ""),
            ),
        ):
            outcome = ensure_mount_synced(
                profile_name="alice",
                profile=profile,
                mount=profile.mounts[0],
                config=config,
            )

        assert outcome.warnings == ()

    def test_rsync_failure_propagates(self, tmp_path: Path) -> None:
        mount_local = tmp_path / "ml"
        mount_local.mkdir()
        profile = _profile(tmp_path, mount_local)

        def _boom(
            _profile: ServerProfile,
            _name: str,
            *,
            delete: bool = False,
            verbose: bool = False,
        ) -> str:
            raise RuntimeError("rsync exited 23: permission denied")

        with (
            patch("srunx.sync.service.sync_mount_by_name", side_effect=_boom),
            patch(
                "srunx.sync.service.is_dirty_git_worktree",
                return_value=(False, ""),
            ),
        ):
            with pytest.raises(RuntimeError, match="permission denied"):
                ensure_mount_synced(
                    profile_name="alice",
                    profile=profile,
                    mount=profile.mounts[0],
                    config=SyncDefaults(owner_check=False),
                )
