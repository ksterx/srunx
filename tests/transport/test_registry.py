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

from srunx.common.exceptions import TransportSelectionError
from srunx.transport import (
    ResolvedTransport,
    TransportHandle,
    TransportPolicy,
    TransportRegistry,
    peek_scheduler_key,
    resolve_transport,
    resolve_transport_source,
)
from srunx.transport import registry as _registry


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
        assert "local" in captured.err
        assert "via --local" in captured.err

    def test_profile_and_local_conflict(self, monkeypatch):
        """AC-1.2: --profile + --local is rejected at startup.

        The pure resolver raises the framework-neutral
        :class:`TransportSelectionError`; the CLI wrapper
        (:mod:`srunx.cli._helpers.transport`) is what maps it to
        ``typer.BadParameter``.
        """
        monkeypatch.delenv("SRUNX_SSH_PROFILE", raising=False)
        with pytest.raises(TransportSelectionError):
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
        assert "via $SRUNX_SSH_PROFILE" in captured.err

    def test_local_flag_overrides_env(self, monkeypatch, capsys):
        """AC-1.4: --local beats $SRUNX_SSH_PROFILE."""
        monkeypatch.setenv("SRUNX_SSH_PROFILE", "envprof")
        with resolve_transport(local=True) as rt:
            assert rt.scheduler_key == "local"
            assert rt.source == "--local"
        captured = capsys.readouterr()
        assert "via --local" in captured.err

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
        from srunx.common.exceptions import TransportError

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


class TestTransportPolicy:
    """The implicit ladder rungs (env, current-profile) are policy-gated.

    Explicit ``--profile`` / ``--local`` are never gated — only the ambient
    sources are. MCP passes a policy with both off so an API surface never
    resolves a remote from ambient machine state.
    """

    def test_default_policy_honours_env(self, monkeypatch):
        monkeypatch.setenv("SRUNX_SSH_PROFILE", "dgx")
        assert resolve_transport_source() == "env"
        assert peek_scheduler_key() == "ssh:dgx"

    def test_allow_env_false_skips_env(self, monkeypatch):
        monkeypatch.setenv("SRUNX_SSH_PROFILE", "dgx")
        policy = TransportPolicy(allow_env=False)
        assert resolve_transport_source(policy=policy) == "default"
        assert peek_scheduler_key(policy=policy) == "local"

    def test_default_policy_honours_current_profile(self, monkeypatch):
        monkeypatch.delenv("SRUNX_SSH_PROFILE", raising=False)
        monkeypatch.setattr(_registry, "_current_profile_name", lambda: "cur")
        assert resolve_transport_source() == "current-profile"
        assert peek_scheduler_key() == "ssh:cur"

    def test_allow_current_profile_false_skips_current(self, monkeypatch):
        monkeypatch.delenv("SRUNX_SSH_PROFILE", raising=False)
        monkeypatch.setattr(_registry, "_current_profile_name", lambda: "cur")
        policy = TransportPolicy(allow_current_profile=False)
        assert resolve_transport_source(policy=policy) == "default"
        assert peek_scheduler_key(policy=policy) == "local"

    def test_both_off_is_explicit_only(self, monkeypatch):
        """MCP-style policy: env + current ignored, but explicit profile wins."""
        monkeypatch.setenv("SRUNX_SSH_PROFILE", "dgx")
        monkeypatch.setattr(_registry, "_current_profile_name", lambda: "cur")
        policy = TransportPolicy(allow_env=False, allow_current_profile=False)
        # No explicit selection -> local despite env + current set.
        assert peek_scheduler_key(policy=policy) == "local"
        # Explicit profile is never gated.
        assert peek_scheduler_key(profile="prod", policy=policy) == "ssh:prod"


class TestSSHBannerBody:
    """``_format_ssh_banner_body`` must surface a useful ``user@host``.

    Regression guard: profiles that delegate to a ``Host`` block in
    ``~/.ssh/config`` (``ssh_host`` alias with blank ``username`` /
    ``hostname``) used to render as a literal ``@`` because the banner
    never consulted ssh_config the way the SSH connection layer does.
    """

    def _stub_profile(
        self,
        *,
        username: str = "",
        hostname: str = "",
        port: int = 22,
        ssh_host: str | None = None,
        proxy_jump: str | None = None,
    ):
        """Build a minimal ServerProfile stand-in for the banner formatter.

        The formatter only reads a fixed set of fields; using a
        ``MagicMock`` keeps the test independent of pydantic validation
        rules on ``ServerProfile`` (which require key_filename etc.).
        """
        profile = MagicMock()
        profile.username = username
        profile.hostname = hostname
        profile.port = port
        profile.ssh_host = ssh_host
        profile.proxy_jump = proxy_jump
        return profile

    def test_ssh_host_alias_only_resolves_via_ssh_config(self, monkeypatch):
        profile = self._stub_profile(ssh_host="gmo")
        monkeypatch.setattr(_registry, "_lookup_profile_silently", lambda name: profile)
        resolved = MagicMock(
            user="user_00028_557dc2",
            hostname="connect.gpucloud.gmo",
            port=8822,
        )
        with patch(
            "srunx.ssh.core.ssh_config.get_ssh_config_host",
            return_value=resolved,
        ) as lookup:
            body = _registry._format_ssh_banner_body(
                profile_name="gmo",
                source_display="via current profile",
            )
        lookup.assert_called_once_with("gmo")
        assert "user_00028_557dc2@connect.gpucloud.gmo:8822" in body
        assert "(profile: gmo · via current profile)" in body
        assert "@[/cyan]" not in body  # no empty-target leak

    def test_explicit_hostname_username_skips_ssh_config(self, monkeypatch):
        profile = self._stub_profile(
            username="alice",
            hostname="dgx.example.com",
            ssh_host="dgx",  # ssh_host set, but real fields take precedence
        )
        monkeypatch.setattr(_registry, "_lookup_profile_silently", lambda name: profile)
        with patch(
            "srunx.ssh.core.ssh_config.get_ssh_config_host",
        ) as lookup:
            body = _registry._format_ssh_banner_body(
                profile_name="dgx",
                source_display="via --profile",
            )
        lookup.assert_not_called()
        assert "alice@dgx.example.com" in body

    def test_ssh_host_alias_fallback_when_ssh_config_lookup_returns_none(
        self, monkeypatch
    ):
        """No matching ``Host`` in ``~/.ssh/config`` → show alias name."""
        profile = self._stub_profile(ssh_host="orphan")
        monkeypatch.setattr(_registry, "_lookup_profile_silently", lambda name: profile)
        with patch(
            "srunx.ssh.core.ssh_config.get_ssh_config_host",
            return_value=None,
        ):
            body = _registry._format_ssh_banner_body(
                profile_name="orphan",
                source_display="via --profile",
            )
        assert "Connected to" in body
        # The alias itself becomes the target so the banner still has
        # *something* identifiable to show.
        assert "orphan" in body

    def test_ssh_config_lookup_exception_does_not_raise(self, monkeypatch):
        """The banner must never crash from a degraded ssh_config state."""
        profile = self._stub_profile(ssh_host="gmo")
        monkeypatch.setattr(_registry, "_lookup_profile_silently", lambda name: profile)
        with patch(
            "srunx.ssh.core.ssh_config.get_ssh_config_host",
            side_effect=RuntimeError("ssh_config exploded"),
        ):
            body = _registry._format_ssh_banner_body(
                profile_name="gmo",
                source_display="via --profile",
            )
        # Falls back to the alias-name path.
        assert "gmo" in body

    def test_default_port_omitted_for_user_at_host(self, monkeypatch):
        profile = self._stub_profile(username="bob", hostname="example.com", port=22)
        monkeypatch.setattr(_registry, "_lookup_profile_silently", lambda name: profile)
        body = _registry._format_ssh_banner_body(
            profile_name="example",
            source_display="via --profile",
        )
        assert "bob@example.com" in body
        assert ":22" not in body

    def test_no_profile_returns_fallback(self, monkeypatch):
        monkeypatch.setattr(_registry, "_lookup_profile_silently", lambda name: None)
        body = _registry._format_ssh_banner_body(
            profile_name="ghost",
            source_display="via --profile",
        )
        assert "SSH profile: ghost" in body
