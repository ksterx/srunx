"""Tests for the per-script hash verification (#137 part 5).

Two layers exercised here:

* :mod:`srunx.sync.hash_verify` — the policy / helpers themselves,
  mocked at the :class:`RsyncClient` boundary so we never touch real
  ssh.
* :func:`srunx.sync.service.mount_sync_session` integration —
  asserts that the verify call only fires when the opt-in config flag
  is on AND ``verify_paths`` was supplied, and that it slots between
  rsync and the marker write (a hash mismatch must NOT result in a
  stale ownership marker).
"""

from __future__ import annotations

import hashlib
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from srunx.config import SyncDefaults
from srunx.ssh.core.config import MountConfig, ServerProfile
from srunx.sync.hash_verify import (
    HashMismatch,
    local_sha256,
    verify_paths_match,
)
from srunx.sync.service import SyncAbortedError, mount_sync_session


@pytest.fixture(autouse=True)
def isolated_config_home(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / "xdg"))


def _profile(tmp_path: Path) -> tuple[ServerProfile, Path]:
    """Return a profile with a single 'ml' mount + the local mount root."""
    key = tmp_path / "id_rsa"
    key.write_text("dummy")
    mount_local = tmp_path / "ml"
    mount_local.mkdir()
    profile = ServerProfile(
        hostname="h",
        username="u",
        key_filename=str(key),
        mounts=(MountConfig(name="ml", local=str(mount_local), remote="/r/ml"),),
    )
    return profile, mount_local


# ── HashMismatch ──────────────────────────────────────────────────


class TestHashMismatch:
    def test_subclasses_sync_aborted_error(self) -> None:
        """Existing CLI / Web error handling catches one base class.

        ``SyncAbortedError`` is the single ""we refused to sync""
        channel; a hash mismatch must ride that same channel rather
        than introducing a parallel exception type the callers would
        need to know about.
        """
        assert issubclass(HashMismatch, SyncAbortedError)

    def test_carries_fields_on_mismatch(self, tmp_path: Path) -> None:
        local = tmp_path / "train.sbatch"
        local.write_text("hi")
        exc = HashMismatch(
            local_path=local,
            remote_path="/r/ml/train.sbatch",
            local_hash="a" * 64,
            remote_hash="b" * 64,
        )
        assert exc.local_path == local
        assert exc.remote_path == "/r/ml/train.sbatch"
        assert exc.local_hash == "a" * 64
        assert exc.remote_hash == "b" * 64
        # Both hashes must appear in the message so a debugging user
        # can grep them out of stderr without scraping fields.
        msg = str(exc)
        assert "a" * 64 in msg
        assert "b" * 64 in msg
        # And a hint about excludes so the most common cause is
        # signposted rather than left as an exercise.
        assert "exclude" in msg.lower()

    def test_message_for_missing_remote(self, tmp_path: Path) -> None:
        """``remote_hash=None`` produces a distinct ""file not found"" message.

        The user needs to know whether the remote bytes are wrong
        (mismatch) or simply not there (missing) — different debug
        paths.
        """
        local = tmp_path / "train.sbatch"
        local.write_text("hi")
        exc = HashMismatch(
            local_path=local,
            remote_path="/r/ml/train.sbatch",
            local_hash="a" * 64,
            remote_hash=None,
        )
        msg = str(exc)
        assert "not found" in msg.lower()
        assert "/r/ml/train.sbatch" in msg


# ── local_sha256 ──────────────────────────────────────────────────


class TestLocalSha256:
    def test_matches_hashlib(self, tmp_path: Path) -> None:
        path = tmp_path / "script.sbatch"
        payload = b"#!/bin/bash\necho hi\n"
        path.write_bytes(payload)
        expected = hashlib.sha256(payload).hexdigest()
        assert local_sha256(path) == expected

    def test_empty_file(self, tmp_path: Path) -> None:
        """sha256 of empty bytes is e3b0c442… — sanity check."""
        path = tmp_path / "empty"
        path.write_bytes(b"")
        assert local_sha256(path) == hashlib.sha256(b"").hexdigest()


# ── verify_paths_match ────────────────────────────────────────────


class TestVerifyPathsMatch:
    """The single decision point for hash verification.

    Mocking :func:`build_rsync_client` lets us drive the four cases
    (match / mismatch / remote missing / no tool) deterministically
    without standing up an SSH server.
    """

    def test_empty_paths_is_noop(self, tmp_path: Path) -> None:
        """No paths → no rsync client built (no ssh round-trip)."""
        profile, _ = _profile(tmp_path)
        with patch("srunx.sync.hash_verify.build_rsync_client") as fake_build:
            verify_paths_match(profile, profile.mounts[0], [])
        fake_build.assert_not_called()

    def test_matching_hash_returns_silently(self, tmp_path: Path) -> None:
        """Equal local + remote hash → no exception."""
        profile, mount_local = _profile(tmp_path)
        script = mount_local / "train.sbatch"
        payload = b"#!/bin/bash\necho hi\n"
        script.write_bytes(payload)
        digest = hashlib.sha256(payload).hexdigest()

        client = MagicMock()
        client.remote_sha256.return_value = digest

        with patch("srunx.sync.hash_verify.build_rsync_client", return_value=client):
            verify_paths_match(profile, profile.mounts[0], [script])

        client.remote_sha256.assert_called_once_with("/r/ml/train.sbatch")

    def test_mismatched_hash_raises(self, tmp_path: Path) -> None:
        """Distinct local + remote hash → ``HashMismatch`` with both digests."""
        profile, mount_local = _profile(tmp_path)
        script = mount_local / "train.sbatch"
        script.write_bytes(b"local-bytes")
        local_hash = hashlib.sha256(b"local-bytes").hexdigest()
        remote_hash = "f" * 64

        client = MagicMock()
        client.remote_sha256.return_value = remote_hash

        with patch("srunx.sync.hash_verify.build_rsync_client", return_value=client):
            with pytest.raises(HashMismatch) as exc_info:
                verify_paths_match(profile, profile.mounts[0], [script])

        assert exc_info.value.local_hash == local_hash
        assert exc_info.value.remote_hash == remote_hash
        assert exc_info.value.local_path == script
        assert exc_info.value.remote_path == "/r/ml/train.sbatch"

    def test_remote_missing_raises(self, tmp_path: Path) -> None:
        """remote_sha256 → None and read_remote_file → None: file is gone.

        This is the headline failure mode the verifier exists for —
        rsync exited 0 but the file we cared about never landed.
        """
        profile, mount_local = _profile(tmp_path)
        script = mount_local / "train.sbatch"
        script.write_bytes(b"hi")

        client = MagicMock()
        client.remote_sha256.return_value = None
        client.read_remote_file.return_value = None  # file genuinely gone

        with patch("srunx.sync.hash_verify.build_rsync_client", return_value=client):
            with pytest.raises(HashMismatch) as exc_info:
                verify_paths_match(profile, profile.mounts[0], [script])

        assert exc_info.value.remote_hash is None
        assert "not found" in str(exc_info.value).lower()

    def test_remote_no_tool_skips_silently(self, tmp_path: Path) -> None:
        """remote_sha256 → None but file exists: no tool on remote PATH.

        Forcing every cluster admin to install ``sha256sum`` to use
        srunx would be a regression — the check ships opt-in
        precisely so users who care can flip it on, while users on
        toolless clusters can leave it off without penalty.
        """
        profile, mount_local = _profile(tmp_path)
        script = mount_local / "train.sbatch"
        script.write_bytes(b"hi")

        client = MagicMock()
        client.remote_sha256.return_value = None
        # File exists, so the disambiguation probe sees content back.
        client.read_remote_file.return_value = "remote bytes (not None)"

        with patch("srunx.sync.hash_verify.build_rsync_client", return_value=client):
            verify_paths_match(profile, profile.mounts[0], [script])

        # No raise. read_remote_file was used to disambiguate.
        client.read_remote_file.assert_called_once_with("/r/ml/train.sbatch")

    def test_ssh_failure_propagates(self, tmp_path: Path) -> None:
        """RuntimeError from remote_sha256 propagates verbatim.

        Letting it bubble means the user sees ""ssh: connection
        refused"" instead of a misleading ""hash mismatch"" — the
        same defensive shape ``check_owner`` uses for marker reads.
        """
        profile, mount_local = _profile(tmp_path)
        script = mount_local / "train.sbatch"
        script.write_bytes(b"hi")

        client = MagicMock()
        client.remote_sha256.side_effect = RuntimeError(
            "ssh: connect to host h: connection refused"
        )

        with patch("srunx.sync.hash_verify.build_rsync_client", return_value=client):
            with pytest.raises(RuntimeError, match="connection refused"):
                verify_paths_match(profile, profile.mounts[0], [script])

    def test_remote_path_translation(self, tmp_path: Path) -> None:
        """The remote path passed to ``remote_sha256`` is the mount-translated form."""
        profile, mount_local = _profile(tmp_path)
        sub = mount_local / "experiments" / "v1"
        sub.mkdir(parents=True)
        script = sub / "train.sbatch"
        script.write_bytes(b"hi")

        client = MagicMock()
        client.remote_sha256.return_value = hashlib.sha256(b"hi").hexdigest()

        with patch("srunx.sync.hash_verify.build_rsync_client", return_value=client):
            verify_paths_match(profile, profile.mounts[0], [script])

        client.remote_sha256.assert_called_once_with(
            "/r/ml/experiments/v1/train.sbatch"
        )


# ── mount_sync_session integration ────────────────────────────────


class TestMountSyncSessionVerifyIntegration:
    """The service layer wires verify_paths_match in at the right point.

    Acceptance criteria:
    * Default config (``verify_remote_hash=False``) → verify NOT called.
    * Opt-in + paths supplied → verify called between rsync and
      marker write.
    * Opt-in but no paths → verify NOT called.
    * Hash mismatch → marker NOT written (we don't claim ownership of
      corrupt remote state).
    """

    def test_verify_off_by_default_does_not_call_helper(self, tmp_path: Path) -> None:
        profile, mount_local = _profile(tmp_path)
        script = mount_local / "train.sbatch"
        script.write_bytes(b"hi")

        with (
            patch("srunx.sync.hash_verify.verify_paths_match") as fake_verify,
            patch("srunx.sync.service.sync_mount_by_name", return_value=""),
            patch("srunx.sync.service.check_owner"),
            patch("srunx.sync.service.write_owner_marker"),
            patch(
                "srunx.sync.service.is_dirty_git_worktree",
                return_value=(False, ""),
            ),
        ):
            with mount_sync_session(
                profile_name="alice",
                profile=profile,
                mount=profile.mounts[0],
                config=SyncDefaults(),  # verify_remote_hash defaults to False
                sync_required=True,
                verify_paths=[str(script)],
            ):
                pass

        fake_verify.assert_not_called()

    def test_verify_on_with_paths_calls_helper_between_rsync_and_marker(
        self, tmp_path: Path
    ) -> None:
        profile, mount_local = _profile(tmp_path)
        script = mount_local / "train.sbatch"
        script.write_bytes(b"hi")

        order: list[str] = []

        def _record_rsync(*a: object, **k: object) -> str:
            order.append("rsync")
            return ""

        def _record_verify(*a: object, **k: object) -> None:
            order.append("verify")

        def _record_write(*a: object, **k: object) -> object:
            order.append("write")
            return None

        with (
            patch("srunx.sync.service.sync_mount_by_name", _record_rsync),
            patch("srunx.sync.hash_verify.verify_paths_match", _record_verify),
            patch("srunx.sync.service.check_owner"),
            patch("srunx.sync.service.write_owner_marker", _record_write),
            patch(
                "srunx.sync.service.is_dirty_git_worktree",
                return_value=(False, ""),
            ),
        ):
            with mount_sync_session(
                profile_name="alice",
                profile=profile,
                mount=profile.mounts[0],
                config=SyncDefaults(verify_remote_hash=True),
                sync_required=True,
                verify_paths=[str(script)],
            ):
                pass

        assert order == ["rsync", "verify", "write"], (
            "verify must run AFTER rsync (so we hash the post-rsync "
            "remote state) and BEFORE the marker write (so a hash "
            "mismatch refuses to claim ownership of corrupt state)."
        )

    def test_verify_on_no_paths_skips_helper(self, tmp_path: Path) -> None:
        """Opt-in but no specific paths → still skip.

        ``srunx ssh sync`` and similar callers don't have a single
        script to verify; the verifier should no-op rather than
        guess at what to hash.
        """
        profile, _ = _profile(tmp_path)

        with (
            patch("srunx.sync.hash_verify.verify_paths_match") as fake_verify,
            patch("srunx.sync.service.sync_mount_by_name", return_value=""),
            patch("srunx.sync.service.check_owner"),
            patch("srunx.sync.service.write_owner_marker"),
            patch(
                "srunx.sync.service.is_dirty_git_worktree",
                return_value=(False, ""),
            ),
        ):
            with mount_sync_session(
                profile_name="alice",
                profile=profile,
                mount=profile.mounts[0],
                config=SyncDefaults(verify_remote_hash=True),
                sync_required=True,
                # Both None and empty list must skip — the absence of
                # a specific file to verify is the off-switch.
                verify_paths=None,
            ):
                pass

        fake_verify.assert_not_called()

    def test_hash_mismatch_does_not_write_marker(self, tmp_path: Path) -> None:
        """A failed verify must NOT result in a stale ownership marker.

        If we wrote the marker anyway, the next user to sync would
        see ""this machine just claimed it"" and skip THEIR sync —
        compounding the silent-rsync bug instead of letting it
        surface.
        """
        profile, mount_local = _profile(tmp_path)
        script = mount_local / "train.sbatch"
        script.write_bytes(b"hi")

        def _verify_boom(*a: object, **k: object) -> None:
            raise HashMismatch(
                local_path=script,
                remote_path="/r/ml/train.sbatch",
                local_hash="a" * 64,
                remote_hash="b" * 64,
            )

        with (
            patch("srunx.sync.service.sync_mount_by_name", return_value=""),
            patch("srunx.sync.hash_verify.verify_paths_match", _verify_boom),
            patch("srunx.sync.service.check_owner"),
            patch("srunx.sync.service.write_owner_marker") as fake_write,
            patch(
                "srunx.sync.service.is_dirty_git_worktree",
                return_value=(False, ""),
            ),
        ):
            with pytest.raises(SyncAbortedError):
                with mount_sync_session(
                    profile_name="alice",
                    profile=profile,
                    mount=profile.mounts[0],
                    config=SyncDefaults(verify_remote_hash=True),
                    sync_required=True,
                    verify_paths=[str(script)],
                ):
                    pass

        fake_write.assert_not_called()
