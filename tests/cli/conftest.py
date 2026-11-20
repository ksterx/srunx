"""CLI test fixtures."""

import pytest


@pytest.fixture(autouse=True)
def _cli_clusters(add_clusters):
    """SSH cluster names the CLI tests refer to with ``--cluster``."""
    add_clusters(
        "dgx",
        "foo",
        "ml-cluster",
        "p",
        "test-profile",
        "pyxis",
        "prod",
        "envprof",
        "cli",
    )


@pytest.fixture(autouse=True)
def _sync_targets_preapproved(monkeypatch, request):
    """Treat every sync target as approved unless a test opts out.

    First-sync approval is covered by ``tests/cli/test_sync_approval.py``
    (marked ``real_approval``).
    """
    if request.node.get_closest_marker("real_approval"):
        return
    monkeypatch.setattr("srunx.sync.approval.is_approved", lambda *_a: True)
