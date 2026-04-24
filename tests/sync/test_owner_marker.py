"""Tests for the per-machine ownership marker (#137 part 4).

Two layers exercised here:

* :mod:`srunx.sync.owner_marker` — pure parse/serialise + the
  :func:`check_owner` policy gate, mocked at the
  :class:`RsyncClient` boundary so we never touch a real ssh.
* :func:`srunx.sync.service.mount_sync_session` integration —
  asserts the marker check fires before rsync, that rsync's
  successful return triggers a marker write, and that
  ``--force-sync`` (``force_sync=True``) bypasses the check
  without skipping the write.
"""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import pytest

from srunx.common.config import SyncDefaults
from srunx.ssh.core.config import MountConfig, ServerProfile
from srunx.sync.owner_marker import (
    OwnerMarker,
    OwnerMismatch,
    check_owner,
    current_machine_id,
    read_owner_marker,
    write_owner_marker,
)
from srunx.sync.service import SyncAbortedError, mount_sync_session


@pytest.fixture(autouse=True)
def isolated_config_home(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / "xdg"))


def _profile(tmp_path: Path) -> ServerProfile:
    key = tmp_path / "id_rsa"
    key.write_text("dummy")
    mount_local = tmp_path / "ml"
    mount_local.mkdir()
    return ServerProfile(
        hostname="h",
        username="u",
        key_filename=str(key),
        mounts=(MountConfig(name="ml", local=str(mount_local), remote="/r/ml"),),
    )


# ── OwnerMarker JSON parse/serialise ──────────────────────────────


class TestOwnerMarkerJSON:
    def test_round_trip(self) -> None:
        m = OwnerMarker(
            hostname="alice", mount_name="ml", last_sync_at="2026-04-23T00:00:00+00:00"
        )
        round_tripped = OwnerMarker.from_json(m.to_json())
        assert round_tripped == m

    def test_unparseable_returns_none(self) -> None:
        """Defensive: garbage input must not raise.

        A corrupted marker should be treated as absent (next sync
        overwrites it) — not as a hard error that locks the user
        out.
        """
        assert OwnerMarker.from_json("not json") is None
        assert OwnerMarker.from_json("[1,2,3]") is None
        assert OwnerMarker.from_json("{}") is None  # missing fields

    def test_wrong_field_types_returns_none(self) -> None:
        bad = json.dumps({"hostname": 42, "mount_name": "ml", "last_sync_at": "now"})
        assert OwnerMarker.from_json(bad) is None

    def test_serialised_format_is_stable(self) -> None:
        """The on-disk format is a contract — sort_keys + indent.

        Operators may grep / tail these markers manually; surprising
        them with reordered keys after a refactor would be unkind.
        """
        m = OwnerMarker(hostname="h", mount_name="m", last_sync_at="t")
        serialised = m.to_json()
        # Sort order = hostname < last_sync_at < mount_name
        assert (
            serialised.index('"hostname"')
            < serialised.index('"last_sync_at"')
            < serialised.index('"mount_name"')
        )


# ── check_owner policy gate ───────────────────────────────────────


class TestCheckOwnerPolicy:
    """``check_owner`` should be the single decision point.

    Every code path (CLI auto-sync, Web one-shot sync, future MCP
    sync) routes the same inputs through this function; the tests
    cover the full truth table of the four flags
    (enabled, force, marker present, hostname match).
    """

    def test_disabled_skips_check(self, tmp_path: Path) -> None:
        """``enabled=False`` is the global config opt-out.

        Should NOT call ``read_owner_marker`` at all — that would
        force a needless ssh round-trip per sync for users who
        opted out.
        """
        profile = _profile(tmp_path)
        with patch("srunx.sync.owner_marker.read_owner_marker") as fake_read:
            check_owner(
                profile,
                profile.mounts[0],
                enabled=False,
                force=False,
            )
        fake_read.assert_not_called()

    def test_force_skips_check(self, tmp_path: Path) -> None:
        """``--force-sync`` bypasses even when ``enabled=True``."""
        profile = _profile(tmp_path)
        with patch("srunx.sync.owner_marker.read_owner_marker") as fake_read:
            check_owner(
                profile,
                profile.mounts[0],
                enabled=True,
                force=True,
            )
        fake_read.assert_not_called()

    def test_no_marker_passes(self, tmp_path: Path) -> None:
        """First sync (no marker exists) must succeed."""
        profile = _profile(tmp_path)
        with patch("srunx.sync.owner_marker.read_owner_marker", return_value=None):
            check_owner(
                profile,
                profile.mounts[0],
                enabled=True,
                force=False,
            )

    def test_matching_hostname_passes(self, tmp_path: Path) -> None:
        profile = _profile(tmp_path)
        marker = OwnerMarker(hostname="me", mount_name="ml", last_sync_at="t")
        with patch("srunx.sync.owner_marker.read_owner_marker", return_value=marker):
            check_owner(
                profile,
                profile.mounts[0],
                enabled=True,
                force=False,
                hostname="me",
            )

    def test_different_hostname_raises(self, tmp_path: Path) -> None:
        profile = _profile(tmp_path)
        marker = OwnerMarker(
            hostname="alice-laptop",
            mount_name="ml",
            last_sync_at="2026-04-23T00:00:00+00:00",
        )
        with (
            patch("srunx.sync.owner_marker.read_owner_marker", return_value=marker),
            pytest.raises(OwnerMismatch) as exc_info,
        ):
            check_owner(
                profile,
                profile.mounts[0],
                enabled=True,
                force=False,
                hostname="bob-desktop",
            )

        # Error carries the recorded + local hostnames so the user
        # knows WHO last touched it. Plain pytest.raises message
        # match would be fragile; reach for the structured fields.
        assert exc_info.value.local_machine == "bob-desktop"
        assert exc_info.value.recorded_machine == "alice-laptop"
        assert "alice-laptop" in str(exc_info.value)
        assert "--force-sync" in str(exc_info.value)

    def test_ssh_failure_does_not_block(
        self, tmp_path: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        """A transient ssh failure during the marker read should NOT abort.

        The user is already in trouble; silently aborting would
        only hide the problem. The rsync that follows will surface
        the same connection failure with its own error message.
        """
        profile = _profile(tmp_path)

        def _boom(*a: object, **k: object) -> None:
            raise RuntimeError("ssh: connect to host h: connection refused")

        with patch("srunx.sync.owner_marker.read_owner_marker", side_effect=_boom):
            check_owner(
                profile,
                profile.mounts[0],
                enabled=True,
                force=False,
            )


# ── current_machine_id ────────────────────────────────────────────


class TestCurrentMachineId:
    def test_returns_socket_hostname(self, monkeypatch: pytest.MonkeyPatch) -> None:
        import socket

        monkeypatch.setattr(socket, "gethostname", lambda: "test-host")
        assert current_machine_id() == "test-host"


# ── Read / write marker via mocked RsyncClient ────────────────────


class TestReadWriteMarker:
    def test_read_returns_none_when_file_missing(self, tmp_path: Path) -> None:
        """``read_remote_file`` returning ``None`` propagates as ``None``."""
        profile = _profile(tmp_path)
        with patch("srunx.sync.owner_marker.build_rsync_client") as fake_build:
            fake_build.return_value.read_remote_file.return_value = None
            assert read_owner_marker(profile, profile.mounts[0]) is None

    def test_read_parses_valid_json(self, tmp_path: Path) -> None:
        profile = _profile(tmp_path)
        marker = OwnerMarker(
            hostname="alice", mount_name="ml", last_sync_at="2026-04-23T00:00:00+00:00"
        )
        with patch("srunx.sync.owner_marker.build_rsync_client") as fake_build:
            fake_build.return_value.read_remote_file.return_value = marker.to_json()
            assert read_owner_marker(profile, profile.mounts[0]) == marker

    def test_read_returns_none_for_malformed_json(self, tmp_path: Path) -> None:
        """Corrupted marker → treat as absent, don't raise."""
        profile = _profile(tmp_path)
        with patch("srunx.sync.owner_marker.build_rsync_client") as fake_build:
            fake_build.return_value.read_remote_file.return_value = "{broken"
            assert read_owner_marker(profile, profile.mounts[0]) is None

    def test_write_calls_remote_with_marker_json(self, tmp_path: Path) -> None:
        profile = _profile(tmp_path)
        with patch("srunx.sync.owner_marker.build_rsync_client") as fake_build:
            written = write_owner_marker(profile, profile.mounts[0], hostname="alice")

        # The returned marker is what was written — caller can use it
        # for logging / state without re-reading.
        assert written.hostname == "alice"
        assert written.mount_name == "ml"

        # And the underlying client was given the JSON form at the
        # canonical remote path.
        fake_build.return_value.write_remote_file.assert_called_once()
        args, _ = fake_build.return_value.write_remote_file.call_args
        assert args[0] == "/r/ml/.srunx-owner.json"
        assert json.loads(args[1])["hostname"] == "alice"


# ── mount_sync_session integration ────────────────────────────────


class TestMountSyncSessionWithOwnerMarker:
    """End-to-end: the service layer calls check + write at the right times.

    These assert the *integration* contract; the policy / parse
    behaviour is exercised in the focused classes above.
    """

    def test_owner_check_runs_before_rsync(self, tmp_path: Path) -> None:
        profile = _profile(tmp_path)
        order: list[str] = []

        def _record_check(*a: object, **k: object) -> None:
            order.append("check")

        def _record_rsync(*a: object, **k: object) -> str:
            order.append("rsync")
            return ""

        def _record_write(*a: object, **k: object) -> OwnerMarker:
            order.append("write")
            return OwnerMarker(hostname="h", mount_name="m", last_sync_at="t")

        with (
            patch("srunx.sync.service.check_owner", _record_check),
            patch("srunx.sync.service.write_owner_marker", _record_write),
            patch("srunx.sync.service.sync_mount_by_name", _record_rsync),
            patch(
                "srunx.sync.service.is_dirty_git_worktree",
                return_value=(False, ""),
            ),
        ):
            with mount_sync_session(
                profile_name="alice",
                profile=profile,
                mount=profile.mounts[0],
                config=SyncDefaults(),  # owner_check defaults to True
                sync_required=True,
            ):
                pass

        assert order == ["check", "rsync", "write"], (
            "Owner check must run BEFORE rsync (so a mismatch aborts "
            "without touching the remote), and the marker write must "
            "run AFTER rsync (so a failed sync doesn't claim ownership)."
        )

    def test_owner_mismatch_re_raises_as_sync_aborted(self, tmp_path: Path) -> None:
        """``OwnerMismatch`` is wrapped so callers catch one exception type.

        CLI / Web both already handle ``SyncAbortedError`` as the
        single ""we refused to sync"" signal — owner_marker errors
        ride the same channel.
        """
        profile = _profile(tmp_path)

        def _mismatch(*a: object, **k: object) -> None:
            raise OwnerMismatch(
                mount_name="ml",
                local_machine="bob",
                recorded_machine="alice",
                recorded_at="2026-04-23T00:00:00+00:00",
            )

        with (
            patch("srunx.sync.service.check_owner", _mismatch),
            patch("srunx.sync.service.sync_mount_by_name") as fake_rsync,
            patch(
                "srunx.sync.service.is_dirty_git_worktree",
                return_value=(False, ""),
            ),
        ):
            with pytest.raises(SyncAbortedError, match="alice"):
                with mount_sync_session(
                    profile_name="alice",
                    profile=profile,
                    mount=profile.mounts[0],
                    config=SyncDefaults(),
                    sync_required=True,
                ):
                    pass

        # rsync must NOT have run when the marker rejected the sync.
        fake_rsync.assert_not_called()

    def test_owner_check_off_skips_check_and_write(self, tmp_path: Path) -> None:
        """``owner_check=False`` skips BOTH the check AND the write.

        A solo-machine setup that opted out shouldn't accumulate
        markers nobody reads — keep the remote tree clean.
        """
        profile = _profile(tmp_path)

        with (
            patch("srunx.sync.service.check_owner") as fake_check,
            patch("srunx.sync.service.write_owner_marker") as fake_write,
            patch("srunx.sync.service.sync_mount_by_name", return_value=""),
            patch(
                "srunx.sync.service.is_dirty_git_worktree",
                return_value=(False, ""),
            ),
        ):
            with mount_sync_session(
                profile_name="alice",
                profile=profile,
                mount=profile.mounts[0],
                config=SyncDefaults(owner_check=False),
                sync_required=True,
            ):
                pass

        # check_owner is still called (it's the policy gate that
        # itself short-circuits on enabled=False) — but write must
        # not fire.
        fake_check.assert_called_once()
        assert fake_check.call_args.kwargs["enabled"] is False
        fake_write.assert_not_called()

    def test_force_sync_bypasses_check_but_still_writes(self, tmp_path: Path) -> None:
        """``force_sync=True`` (CLI ``--force-sync``) takes ownership.

        The user is intentionally taking the mount over from
        another machine — they SHOULD write a fresh marker so they
        become the recorded owner from now on.
        """
        profile = _profile(tmp_path)

        with (
            patch("srunx.sync.service.check_owner") as fake_check,
            patch("srunx.sync.service.write_owner_marker") as fake_write,
            patch("srunx.sync.service.sync_mount_by_name", return_value=""),
            patch(
                "srunx.sync.service.is_dirty_git_worktree",
                return_value=(False, ""),
            ),
        ):
            with mount_sync_session(
                profile_name="alice",
                profile=profile,
                mount=profile.mounts[0],
                config=SyncDefaults(),
                sync_required=True,
                force_sync=True,
            ):
                pass

        # check_owner is still called with force=True (it's the
        # policy gate; the no-op decision happens inside).
        assert fake_check.call_args.kwargs["force"] is True
        # And the write fires so the takeover is recorded.
        fake_write.assert_called_once()

    def test_marker_write_failure_warns_but_does_not_abort(
        self, tmp_path: Path
    ) -> None:
        """A failed marker write must NOT roll back a successful rsync.

        Network blips at the exact write moment shouldn't fail an
        otherwise-good submission — the user's bytes already shipped.
        Surface the warning via the SyncOutcome instead.
        """
        profile = _profile(tmp_path)

        with (
            patch("srunx.sync.service.check_owner"),
            patch(
                "srunx.sync.service.write_owner_marker",
                side_effect=RuntimeError("ssh write timeout"),
            ),
            patch("srunx.sync.service.sync_mount_by_name", return_value=""),
            patch(
                "srunx.sync.service.is_dirty_git_worktree",
                return_value=(False, ""),
            ),
        ):
            with mount_sync_session(
                profile_name="alice",
                profile=profile,
                mount=profile.mounts[0],
                config=SyncDefaults(),
                sync_required=True,
            ) as outcome:
                pass

        assert outcome.performed is True
        assert any("owner marker" in w for w in outcome.warnings)
