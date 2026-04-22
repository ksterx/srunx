"""Phase 4: resolve_transport and TransportRegistry tests.

Covers the Phase-4 AC:

* AC-1.2: ``--profile`` and ``--local`` together raise ``BadParameter``.
* AC-1.4: ``--local`` overrides ``$SRUNX_SSH_PROFILE``.
* AC-8.5: unknown SSH profile returns ``None`` (no crash).
* Plus banner emission semantics (REQ-7 / AC-10.2).
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
import typer

from srunx.transport import (
    ResolvedTransport,
    TransportHandle,
    TransportRegistry,
    resolve_transport,
)


class TestResolveTransport:
    def test_default_is_local_and_silent(self, capsys, monkeypatch):
        """AC-10.2: fallback path is silent on stderr."""
        monkeypatch.delenv("SRUNX_SSH_PROFILE", raising=False)
        with resolve_transport(profile=None, local=False) as rt:
            assert rt.scheduler_key == "local"
            assert rt.source == "default"
            assert rt.transport_type == "local"
            assert rt.profile_name is None
            assert rt.executor_factory is not None
            assert rt.submission_context is None
        captured = capsys.readouterr()
        assert captured.err == ""

    def test_local_flag_emits_banner(self, capsys, monkeypatch):
        """AC-7 / REQ-7: explicit --local produces a stderr banner."""
        monkeypatch.delenv("SRUNX_SSH_PROFILE", raising=False)
        with resolve_transport(profile=None, local=True) as rt:
            assert rt.scheduler_key == "local"
            assert rt.source == "--local"
        captured = capsys.readouterr()
        assert "transport: local" in captured.err
        assert "from --local" in captured.err

    def test_profile_and_local_conflict(self, monkeypatch):
        """AC-1.2: --profile + --local is rejected at startup."""
        monkeypatch.delenv("SRUNX_SSH_PROFILE", raising=False)
        with pytest.raises(typer.BadParameter):
            with resolve_transport(profile="foo", local=True):
                pass

    def test_env_source_when_only_env_set(self, monkeypatch, capsys):
        """REQ-1: $SRUNX_SSH_PROFILE picks SSH transport when no flag set."""
        monkeypatch.setenv("SRUNX_SSH_PROFILE", "envprof")
        fake_handle = TransportHandle(
            scheduler_key="ssh:envprof",
            profile_name="envprof",
            transport_type="ssh",
            job_ops=MagicMock(),
            queue_client=MagicMock(),
            executor_factory=MagicMock(),
            submission_context=None,
        )
        with patch(
            "srunx.transport.registry._build_ssh_handle",
            return_value=(fake_handle, None),
        ):
            with resolve_transport() as rt:
                assert rt.source == "env"
                assert rt.scheduler_key == "ssh:envprof"
        captured = capsys.readouterr()
        assert "from env" in captured.err

    def test_local_flag_overrides_env(self, monkeypatch, capsys):
        """AC-1.4: --local beats $SRUNX_SSH_PROFILE."""
        monkeypatch.setenv("SRUNX_SSH_PROFILE", "envprof")
        with resolve_transport(local=True) as rt:
            assert rt.scheduler_key == "local"
            assert rt.source == "--local"
        captured = capsys.readouterr()
        assert "from --local" in captured.err

    def test_profile_flag_beats_env(self, monkeypatch, capsys):
        """REQ-1: --profile beats $SRUNX_SSH_PROFILE."""
        monkeypatch.setenv("SRUNX_SSH_PROFILE", "envprof")
        fake_handle = TransportHandle(
            scheduler_key="ssh:cli",
            profile_name="cli",
            transport_type="ssh",
            job_ops=MagicMock(),
            queue_client=MagicMock(),
            executor_factory=MagicMock(),
            submission_context=None,
        )
        with patch(
            "srunx.transport.registry._build_ssh_handle",
            return_value=(fake_handle, None),
        ) as build:
            with resolve_transport(profile="cli") as rt:
                assert rt.source == "--profile"
                assert rt.scheduler_key == "ssh:cli"
            # Fix #5: callbacks + submission_source are now forwarded
            # from resolve_transport into _build_ssh_handle so pooled
            # clones inherit the caller's callback list. F9/F2: the
            # forwarded kwarg list also carries ``mount_name`` and
            # ``pool_size`` so we assert on argument presence rather
            # than an exhaustive signature.
            build.assert_called_once()
            args, kwargs = build.call_args
            assert args == ("cli",)
            assert kwargs.get("callbacks") is None
            assert kwargs.get("submission_source") == "cli"
            assert kwargs.get("mount_name") is None
            assert kwargs.get("pool_size") == 2

    def test_quiet_suppresses_banner(self, capsys, monkeypatch):
        """--quiet silences the banner even when source is explicit."""
        monkeypatch.delenv("SRUNX_SSH_PROFILE", raising=False)
        with resolve_transport(local=True, quiet=True) as rt:
            assert rt.scheduler_key == "local"
        captured = capsys.readouterr()
        assert captured.err == ""

    def test_banner_kwarg_false_suppresses_even_without_quiet(
        self, capsys, monkeypatch
    ):
        """Library callers can opt out of banners entirely via banner=False."""
        monkeypatch.delenv("SRUNX_SSH_PROFILE", raising=False)
        with resolve_transport(local=True, banner=False) as rt:
            assert rt.scheduler_key == "local"
        captured = capsys.readouterr()
        assert captured.err == ""

    def test_ssh_pool_closed_on_exit(self, monkeypatch):
        """SSH pools must be closed when the context manager exits."""
        monkeypatch.delenv("SRUNX_SSH_PROFILE", raising=False)
        fake_handle = TransportHandle(
            scheduler_key="ssh:foo",
            profile_name="foo",
            transport_type="ssh",
            job_ops=MagicMock(),
            queue_client=MagicMock(),
            executor_factory=MagicMock(),
            submission_context=None,
        )
        fake_pool = MagicMock()
        with patch(
            "srunx.transport.registry._build_ssh_handle",
            return_value=(fake_handle, fake_pool),
        ):
            with resolve_transport(profile="foo", banner=False):
                pass
        fake_pool.close.assert_called_once()

    def test_ssh_pool_close_failure_is_swallowed(self, monkeypatch):
        """A noisy pool.close() must not propagate out of the CM."""
        monkeypatch.delenv("SRUNX_SSH_PROFILE", raising=False)
        fake_handle = TransportHandle(
            scheduler_key="ssh:foo",
            profile_name="foo",
            transport_type="ssh",
            job_ops=MagicMock(),
            queue_client=MagicMock(),
            executor_factory=MagicMock(),
            submission_context=None,
        )
        fake_pool = MagicMock()
        fake_pool.close.side_effect = RuntimeError("boom")
        with patch(
            "srunx.transport.registry._build_ssh_handle",
            return_value=(fake_handle, fake_pool),
        ):
            with resolve_transport(profile="foo", banner=False):
                pass
        fake_pool.close.assert_called_once()

    def test_resolved_transport_shortcut_properties(self, monkeypatch):
        """ResolvedTransport shortcut properties must mirror the handle."""
        monkeypatch.delenv("SRUNX_SSH_PROFILE", raising=False)
        with resolve_transport(local=True, banner=False) as rt:
            assert isinstance(rt, ResolvedTransport)
            assert rt.scheduler_key == rt.handle.scheduler_key
            assert rt.profile_name is rt.handle.profile_name
            assert rt.transport_type == rt.handle.transport_type
            assert rt.job_ops is rt.handle.job_ops
            assert rt.queue_client is rt.handle.queue_client
            assert rt.executor_factory is rt.handle.executor_factory
            assert rt.submission_context is rt.handle.submission_context


class TestTransportRegistry:
    def test_local_always_resolves(self):
        reg = TransportRegistry(profile_loader=lambda name: None)
        handle = reg.resolve("local")
        assert handle is not None
        assert handle.scheduler_key == "local"
        assert handle.transport_type == "local"
        assert handle.profile_name is None
        reg.close()

    def test_local_handle_is_cached(self):
        reg = TransportRegistry(profile_loader=lambda name: None)
        first = reg.resolve("local")
        second = reg.resolve("local")
        assert first is second
        reg.close()

    def test_unknown_ssh_profile_returns_none(self):
        """AC-8.5: unresolvable SSH profile returns None."""
        reg = TransportRegistry(profile_loader=lambda name: None)
        assert reg.resolve("ssh:nonexistent") is None
        reg.close()

    def test_malformed_scheduler_key_returns_none(self):
        reg = TransportRegistry(profile_loader=lambda name: None)
        assert reg.resolve("malformed") is None
        assert reg.resolve("") is None
        assert reg.resolve("ssh:") is None
        reg.close()

    def test_ssh_build_failure_returns_none_and_logs(self):
        """A TransportError while building an SSH handle yields None, not a crash."""
        from srunx.exceptions import TransportError

        # Profile loader says the profile exists...
        profile_loader = MagicMock(return_value=MagicMock(mounts=[]))
        reg = TransportRegistry(profile_loader=profile_loader)
        with patch(
            "srunx.transport.registry._build_ssh_handle",
            side_effect=TransportError("forged failure"),
        ):
            assert reg.resolve("ssh:broken") is None
        reg.close()

    def test_close_disposes_pools(self):
        """close() drains every pool accumulated via SSH resolves."""
        fake_handle = TransportHandle(
            scheduler_key="ssh:foo",
            profile_name="foo",
            transport_type="ssh",
            job_ops=MagicMock(),
            queue_client=MagicMock(),
            executor_factory=MagicMock(),
            submission_context=None,
        )
        fake_pool = MagicMock()
        profile_loader = MagicMock(return_value=MagicMock(mounts=[]))
        reg = TransportRegistry(profile_loader=profile_loader)
        with patch(
            "srunx.transport.registry._build_ssh_handle",
            return_value=(fake_handle, fake_pool),
        ):
            handle = reg.resolve("ssh:foo")
            assert handle is fake_handle

        reg.close()
        fake_pool.close.assert_called_once()

    def test_known_scheduler_keys_reads_jobs_table(self):
        """DISTINCT scheduler_key reader pulls from the jobs table only."""
        conn = MagicMock()
        conn.execute.return_value.fetchall.return_value = [
            ("local",),
            ("ssh:dgx",),
            (None,),  # NULL row — should be filtered out
        ]
        reg = TransportRegistry(profile_loader=lambda name: None)
        keys = reg.known_scheduler_keys(conn)
        assert keys == {"local", "ssh:dgx"}
        # Exact SQL check so we catch accidental JOINs / filter additions.
        conn.execute.assert_called_once_with("SELECT DISTINCT scheduler_key FROM jobs")
        reg.close()
