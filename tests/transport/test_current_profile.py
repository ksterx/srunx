"""Phase 2: active SSH profile fallback.

``resolve_transport`` / ``peek_scheduler_key`` / ``resolve_transport_source``
all consult ``srunx ssh profile set`` as the 4th-priority fallback (before
``local``). This module pins the precedence ladder and the config opt-out.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import srunx.transport.registry as _reg
from srunx.transport import (
    peek_scheduler_key,
    resolve_transport,
    resolve_transport_source,
)


class TestCurrentProfileFallback:
    def test_current_profile_picked_when_no_flag_or_env(self, capsys, monkeypatch):
        """No --profile / --local / env → current profile drives resolution."""
        monkeypatch.delenv("SRUNX_SSH_PROFILE", raising=False)
        # Override the autouse fixture in conftest that forces None.
        monkeypatch.setattr(_reg, "_current_profile_name", lambda: "dgx")

        with patch("srunx.transport.registry._build_ssh_handle") as mock_build:
            handle = MagicMock()
            handle.scheduler_key = "ssh:dgx"
            handle.profile_name = "dgx"
            handle.transport_type = "ssh"
            mock_build.return_value = (handle, MagicMock())
            with resolve_transport() as rt:
                assert rt.source == "current-profile"
                assert rt.scheduler_key == "ssh:dgx"
        assert "via current profile" in capsys.readouterr().err

    def test_env_beats_current_profile(self, monkeypatch):
        """$SRUNX_SSH_PROFILE (priority 3) overrides current profile (4)."""
        monkeypatch.setenv("SRUNX_SSH_PROFILE", "envprof")
        monkeypatch.setattr(_reg, "_current_profile_name", lambda: "dgx")

        with patch("srunx.transport.registry._build_ssh_handle") as mock_build:
            handle = MagicMock()
            handle.scheduler_key = "ssh:envprof"
            handle.profile_name = "envprof"
            handle.transport_type = "ssh"
            mock_build.return_value = (handle, None)
            with resolve_transport() as rt:
                assert rt.source == "env"
                assert rt.scheduler_key == "ssh:envprof"

    def test_local_flag_beats_current_profile(self, monkeypatch):
        """--local overrides the current-profile fallback."""
        monkeypatch.delenv("SRUNX_SSH_PROFILE", raising=False)
        monkeypatch.setattr(_reg, "_current_profile_name", lambda: "dgx")
        with resolve_transport(local=True) as rt:
            assert rt.source == "--local"
            assert rt.scheduler_key == "local"

    def test_explicit_profile_beats_current_profile(self, monkeypatch):
        """--profile overrides the current-profile fallback."""
        monkeypatch.delenv("SRUNX_SSH_PROFILE", raising=False)
        monkeypatch.setattr(_reg, "_current_profile_name", lambda: "dgx")
        with patch("srunx.transport.registry._build_ssh_handle") as mock_build:
            handle = MagicMock()
            handle.scheduler_key = "ssh:explicit"
            handle.profile_name = "explicit"
            handle.transport_type = "ssh"
            mock_build.return_value = (handle, None)
            with resolve_transport(profile="explicit") as rt:
                assert rt.source == "--profile"
                assert rt.scheduler_key == "ssh:explicit"

    def test_no_current_profile_falls_through_to_local(self, monkeypatch):
        """When config has no active profile, resolution stays on local."""
        monkeypatch.delenv("SRUNX_SSH_PROFILE", raising=False)
        monkeypatch.setattr(_reg, "_current_profile_name", lambda: None)
        with resolve_transport() as rt:
            assert rt.source == "default"
            assert rt.scheduler_key == "local"


class TestPeekAndSourceParity:
    def test_peek_scheduler_key_honours_current_profile(self, monkeypatch):
        monkeypatch.delenv("SRUNX_SSH_PROFILE", raising=False)
        monkeypatch.setattr(_reg, "_current_profile_name", lambda: "pyxis")
        assert peek_scheduler_key() == "ssh:pyxis"

    def test_resolve_transport_source_honours_current_profile(self, monkeypatch):
        monkeypatch.delenv("SRUNX_SSH_PROFILE", raising=False)
        monkeypatch.setattr(_reg, "_current_profile_name", lambda: "pyxis")
        assert resolve_transport_source() == "current-profile"


class TestOptOut:
    def test_use_current_profile_false_disables_fallback(self, monkeypatch):
        """When ``cli.use_current_profile=False``, active profile is ignored.

        We mock :func:`_current_profile_name` directly because it's the
        single point where the config flag is read.
        """
        monkeypatch.delenv("SRUNX_SSH_PROFILE", raising=False)
        # _current_profile_name returns None when the config flag is off;
        # simulate that path.
        monkeypatch.setattr(_reg, "_current_profile_name", lambda: None)
        with resolve_transport() as rt:
            assert rt.source == "default"
            assert rt.scheduler_key == "local"
