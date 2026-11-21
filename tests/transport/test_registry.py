"""Phase 4: resolve_transport and TransportRegistry tests.

Covers the Phase-4 AC:

* cluster selection order (design §5) and errors for missing / unknown clusters
* AC-8.5: unknown SSH cluster returns ``None`` from the registry (no crash).
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


def _write_user(text: str) -> None:
    from srunx.common import paths
    from srunx.common.config import clear_settings_cache

    paths.user_config_path().write_text(text)
    clear_settings_cache()


CLUSTERS = """
default_cluster = "local"

[clusters.local]
transport = "local"

[clusters.foo]
transport = "ssh"
host = "foo"

[clusters.envc]
transport = "ssh"
host = "envc"
"""


def _fake_ssh_handle(name: str) -> TransportHandle:
    return TransportHandle(
        scheduler_key=f"ssh:{name}",
        profile_name=name,
        transport_type="ssh",
        job_ops=MagicMock(),
        queue_client=MagicMock(),
        executor_factory=MagicMock(),
        submission_context=None,
        cluster_name=name,
    )


class TestResolveTransport:
    def test_default_cluster_is_used_and_announced(self, capsys):
        with resolve_transport() as rt:
            assert rt.scheduler_key == "local"
            assert rt.source == "config"
            assert rt.transport_type == "local"
            assert rt.cluster_name == "local"
            assert rt.profile_name is None
            assert rt.executor_factory is not None
            assert rt.submission_context is None
        assert "Local SLURM" in capsys.readouterr().err

    def test_no_cluster_is_an_error(self):
        _write_user('[clusters.local]\ntransport = "local"\n')
        with pytest.raises(TransportSelectionError, match="No cluster selected"):
            with resolve_transport():
                pass

    def test_unknown_cluster_is_an_error(self):
        with pytest.raises(TransportSelectionError, match="Unknown cluster"):
            with resolve_transport(cluster="nope"):
                pass

    def test_empty_cluster_is_an_error(self):
        with pytest.raises(TransportSelectionError):
            with resolve_transport(cluster="  "):
                pass

    def test_env_selects_cluster(self, monkeypatch, capsys):
        _write_user(CLUSTERS)
        monkeypatch.setenv("SRUNX_CLUSTER", "envc")
        with patch(
            "srunx.transport.registry._build_ssh_handle",
            return_value=(_fake_ssh_handle("envc"), None),
        ):
            with resolve_transport() as rt:
                assert rt.source == "env"
                assert rt.scheduler_key == "ssh:envc"
        assert "via $SRUNX_CLUSTER" in capsys.readouterr().err

    def test_explicit_cluster_beats_env(self, monkeypatch):
        _write_user(CLUSTERS)
        monkeypatch.setenv("SRUNX_CLUSTER", "envc")
        with patch(
            "srunx.transport.registry._build_ssh_handle",
            return_value=(_fake_ssh_handle("foo"), None),
        ) as build:
            with resolve_transport(cluster="foo") as rt:
                assert rt.source == "--cluster"
                assert rt.scheduler_key == "ssh:foo"
            args, kwargs = build.call_args
            assert args == ("foo",)
            assert kwargs.get("callbacks") is None
            assert kwargs.get("submission_source") == "cli"
            assert kwargs.get("mount_name") is None
            assert kwargs.get("pool_size") == 2

    def test_explicit_local_cluster_beats_env(self, monkeypatch):
        _write_user(CLUSTERS)
        monkeypatch.setenv("SRUNX_CLUSTER", "envc")
        with resolve_transport(cluster="local", banner=False) as rt:
            assert rt.scheduler_key == "local"

    def test_project_cluster(self, tmp_path):
        from srunx.common.config import use_project

        _write_user(CLUSTERS)
        root = tmp_path / "p"
        (root / ".srunx").mkdir(parents=True)
        (root / ".srunx" / "config.toml").write_text('name = "p"\ncluster = "foo"\n')
        with use_project(root):
            assert resolve_transport_source() == "project"
            assert peek_scheduler_key() == "ssh:foo"

    def test_active_config_follows_cluster(self):
        from srunx.common.config import active_config

        _write_user(
            CLUSTERS + '\n[clusters.local.defaults.resources]\npartition = "cpu"\n'
        )
        with resolve_transport(cluster="local", banner=False):
            cfg = active_config()
            assert cfg is not None
            assert cfg.cluster == "local"
            assert cfg.resources.partition == "cpu"
        assert active_config() is None

    def test_quiet_suppresses_banner(self, capsys):
        with resolve_transport(cluster="local", quiet=True) as rt:
            assert rt.scheduler_key == "local"
        assert capsys.readouterr().err == ""

    def test_banner_kwarg_false_suppresses(self, capsys):
        with resolve_transport(cluster="local", banner=False):
            pass
        assert capsys.readouterr().err == ""

    def test_ssh_pool_closed_on_exit(self):
        _write_user(CLUSTERS)
        fake_pool = MagicMock()
        with patch(
            "srunx.transport.registry._build_ssh_handle",
            return_value=(_fake_ssh_handle("foo"), fake_pool),
        ):
            with resolve_transport(cluster="foo", banner=False):
                pass
        fake_pool.close.assert_called_once()

    def test_ssh_pool_close_failure_is_swallowed(self):
        _write_user(CLUSTERS)
        fake_pool = MagicMock()
        fake_pool.close.side_effect = RuntimeError("boom")
        with patch(
            "srunx.transport.registry._build_ssh_handle",
            return_value=(_fake_ssh_handle("foo"), fake_pool),
        ):
            with resolve_transport(cluster="foo", banner=False):
                pass
        fake_pool.close.assert_called_once()

    def test_resolved_transport_shortcut_properties(self):
        with resolve_transport(cluster="local", banner=False) as rt:
            assert isinstance(rt, ResolvedTransport)
            assert rt.scheduler_key == rt.handle.scheduler_key
            assert rt.profile_name is rt.handle.profile_name
            assert rt.cluster_name == rt.handle.cluster_name
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
    """Implicit selection (env / project / default_cluster) is policy-gated."""

    def test_default_policy_honours_env(self, monkeypatch):
        _write_user(CLUSTERS)
        monkeypatch.setenv("SRUNX_CLUSTER", "foo")
        assert resolve_transport_source() == "env"
        assert peek_scheduler_key() == "ssh:foo"

    def test_explicit_only_policy_rejects_implicit(self, monkeypatch):
        _write_user(CLUSTERS)
        monkeypatch.setenv("SRUNX_CLUSTER", "foo")
        policy = TransportPolicy(allow_implicit=False)
        with pytest.raises(TransportSelectionError, match="explicitly"):
            peek_scheduler_key(policy=policy)
        assert peek_scheduler_key(cluster="foo", policy=policy) == "ssh:foo"


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
        profile = self._stub_profile(ssh_host="gpu")
        monkeypatch.setattr(_registry, "_lookup_profile_silently", lambda name: profile)
        resolved = MagicMock(
            user="alice",
            hostname="login.example.org",
            port=8822,
        )
        with patch(
            "srunx.ssh.core.ssh_config.get_ssh_config_host",
            return_value=resolved,
        ) as lookup:
            body = _registry._format_ssh_banner_body(
                profile_name="gpu",
                source_display="via current profile",
            )
        lookup.assert_called_once_with("gpu")
        assert "alice@login.example.org:8822" in body
        assert "(cluster: gpu · via current profile)" in body
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
        profile = self._stub_profile(ssh_host="gpu")
        monkeypatch.setattr(_registry, "_lookup_profile_silently", lambda name: profile)
        with patch(
            "srunx.ssh.core.ssh_config.get_ssh_config_host",
            side_effect=RuntimeError("ssh_config exploded"),
        ):
            body = _registry._format_ssh_banner_body(
                profile_name="gpu",
                source_display="via --profile",
            )
        # Falls back to the alias-name path.
        assert "gpu" in body

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
        assert "Cluster: ghost" in body


class TestSubmissionContextMountSelection:
    """Mount auto-selection for :func:`_resolve_submission_context`."""

    class _FakeMount:
        def __init__(self, name: str, local: str) -> None:
            self.name = name
            self.local = local
            self.remote = f"/remote/{name}"

    def _resolve(self, mounts, mount_name=None, allow_cwd_mount=True):
        return _registry._resolve_submission_context(
            profile_name="gpu",
            profile_mounts=mounts,
            mount_name=mount_name,
            allow_cwd_mount=allow_cwd_mount,
        )

    def test_no_mounts_returns_none(self):
        assert self._resolve([]) is None

    def test_single_mount_auto_selected_regardless_of_cwd(self, tmp_path, monkeypatch):
        outside = tmp_path / "outside"
        outside.mkdir()
        monkeypatch.chdir(outside)
        mount = self._FakeMount("only", str(tmp_path / "proj"))
        ctx = self._resolve([mount])
        assert ctx is not None
        assert ctx.mount_name == "only"

    def test_explicit_mount_name_wins_over_cwd(self, tmp_path, monkeypatch):
        a = tmp_path / "a"
        b = tmp_path / "b"
        a.mkdir()
        b.mkdir()
        monkeypatch.chdir(a)
        ctx = self._resolve(
            [self._FakeMount("a", str(a)), self._FakeMount("b", str(b))],
            mount_name="b",
        )
        assert ctx is not None
        assert ctx.mount_name == "b"

    def test_cwd_selects_the_owning_mount(self, tmp_path, monkeypatch):
        a = tmp_path / "a"
        b = tmp_path / "b"
        (a / "sub" / "deeper").mkdir(parents=True)
        b.mkdir()
        monkeypatch.chdir(a / "sub" / "deeper")
        ctx = self._resolve(
            [self._FakeMount("a", str(a)), self._FakeMount("b", str(b))]
        )
        assert ctx is not None
        assert ctx.mount_name == "a"

    def test_nested_mounts_resolve_to_the_deepest(self, tmp_path, monkeypatch):
        outer = tmp_path / "outer"
        inner = outer / "inner"
        inner.mkdir(parents=True)
        monkeypatch.chdir(inner)
        ctx = self._resolve(
            [self._FakeMount("outer", str(outer)), self._FakeMount("inner", str(inner))]
        )
        assert ctx is not None
        assert ctx.mount_name == "inner"

    def test_cwd_outside_every_mount_disables_translation_quietly(
        self, tmp_path, monkeypatch, caplog
    ):
        a = tmp_path / "a"
        b = tmp_path / "b"
        outside = tmp_path / "outside"
        for d in (a, b, outside):
            d.mkdir()
        monkeypatch.chdir(outside)
        with caplog.at_level("WARNING"):
            ctx = self._resolve(
                [self._FakeMount("a", str(a)), self._FakeMount("b", str(b))]
            )
        assert ctx is not None
        assert ctx.mount_name is None
        # Read-only commands (squeue / sinfo / tail) build a handle too;
        # the no-translation fallback must not shout at them.
        assert "path translation" not in caplog.text

    def test_cwd_fallback_is_off_by_default(self, tmp_path, monkeypatch):
        """Long-lived callers (Web app, MCP) must not inherit the cwd.

        Their working directory is an accident of how the process was
        launched, so honouring it would make identical requests resolve
        to different remote paths on two machines.
        """
        a = tmp_path / "a"
        b = tmp_path / "b"
        a.mkdir()
        b.mkdir()
        monkeypatch.chdir(a)
        ctx = _registry._resolve_submission_context(
            profile_name="gpu",
            profile_mounts=[self._FakeMount("a", str(a)), self._FakeMount("b", str(b))],
            mount_name=None,
        )
        assert ctx is not None
        assert ctx.mount_name is None
