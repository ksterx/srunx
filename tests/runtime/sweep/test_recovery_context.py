"""Sweep crash recovery resumes on the recorded cluster, never a default."""

from __future__ import annotations

import pytest

from srunx.common.config import activate_config, load_settings
from srunx.runtime.sweep.context import load_sweep_context, record_sweep_context
from srunx.runtime.sweep.reconciler import SweepReconciler, SweepRecoveryRefused


def test_record_uses_active_cluster(add_clusters):
    add_clusters("gpu")
    assert load_sweep_context(1) is None
    record_sweep_context(1)  # nothing active: nothing recorded
    assert load_sweep_context(1) is None
    with activate_config(load_settings(reload=True).effective("gpu")):
        record_sweep_context(1)
    ctx = load_sweep_context(1)
    assert ctx is not None and ctx.cluster == "gpu"


def _sweep(sweep_id: int, source: str = "web"):
    from datetime import UTC, datetime

    from srunx.observability.storage.models import SweepRun

    return SweepRun(
        id=sweep_id,
        name="s",
        status="running",
        matrix={},
        fail_fast=False,
        max_parallel=1,
        cell_count=1,
        submission_source=source,
        started_at=datetime.now(UTC),
    )


def test_provider_refuses_unknown_context():
    from srunx.web.app import _build_web_executor_factory_provider

    provider = _build_web_executor_factory_provider()
    with pytest.raises(SweepRecoveryRefused):
        provider(_sweep(42))
    assert provider(_sweep(42, "cli")) is None


def test_provider_refuses_local_cluster():
    from srunx.web.app import _build_web_executor_factory_provider

    with activate_config(load_settings(reload=True).effective("local")):
        record_sweep_context(7)
    with pytest.raises(SweepRecoveryRefused):
        _build_web_executor_factory_provider()(_sweep(7))


def test_reconciler_does_not_fall_back_when_refused():
    def provider(_sweep):
        raise SweepRecoveryRefused("no")

    with pytest.raises(SweepRecoveryRefused):
        SweepReconciler._resolve_bundle(_sweep(1), provider)

    def broken(_sweep):
        raise RuntimeError("other")

    assert SweepReconciler._resolve_bundle(_sweep(1), broken) is None


def test_provider_restores_config_and_project(tmp_path):
    from srunx.web.app import _build_web_executor_factory_provider
    from tests.conftest import append_user_config

    append_user_config(
        '[clusters.gpu]\ntransport = "ssh"\nhostname = "gpu.example"\n'
        'username = "u"\n[clusters.gpu.defaults.resources]\npartition = "a100"'
    )
    root = tmp_path / "p"
    (root / ".srunx").mkdir(parents=True)
    (root / ".srunx" / "config.toml").write_text(
        'name = "p"\n[clusters.gpu]\nremote_dir = "/r/p"\n'
    )
    from srunx.common.config import use_project

    with (
        use_project(root),
        activate_config(load_settings(reload=True).effective("gpu")),
    ):
        record_sweep_context(9)
    bundle = _build_web_executor_factory_provider()(_sweep(9))
    try:
        assert bundle.config.resources.partition == "a100"
        assert bundle.project_root == str(root.resolve())
        assert [m.remote for m in bundle.submission_context.mounts] == ["/r/p"]

        from srunx.common.config import active_config
        from srunx.runtime.sweep.reconciler import _bundle_scope

        with _bundle_scope(bundle):
            assert active_config() is bundle.config
            assert load_settings().project_root == root.resolve()
        assert active_config() is None
    finally:
        bundle.cleanup()
