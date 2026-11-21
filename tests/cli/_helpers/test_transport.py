"""Tests for the CLI-boundary transport resolver wrappers.

The wrappers add exactly one thing on top of the registry resolvers: a
``TransportSelectionError`` -> ``typer.BadParameter`` translation so Typer
renders a bad ``--cluster`` selection as a normal flag error.
"""

from __future__ import annotations

import pytest
import typer

from srunx.cli._helpers.transport import (
    cluster_for_job,
    peek_scheduler_key,
    resolve_cluster_name,
    resolve_transport,
    resolve_transport_source,
)


class TestBadParameterTranslation:
    def test_resolve_transport_unknown_cluster(self):
        with pytest.raises(typer.BadParameter) as excinfo:
            with resolve_transport(cluster="nope"):
                pass
        assert excinfo.value.param_hint == "--cluster"

    def test_peek_scheduler_key_unknown_cluster(self):
        with pytest.raises(typer.BadParameter):
            peek_scheduler_key(cluster="nope")

    def test_resolve_transport_source_unknown_cluster(self):
        with pytest.raises(typer.BadParameter):
            resolve_transport_source(cluster="nope")

    def test_empty_cluster_becomes_bad_parameter(self):
        with pytest.raises(typer.BadParameter) as excinfo:
            peek_scheduler_key(cluster="   ")
        assert excinfo.value.param_hint == "--cluster"


class TestHappyPathPassThrough:
    def test_default_resolves_local(self):
        with resolve_transport(banner=False) as rt:
            assert rt.scheduler_key == "local"

    def test_peek_local(self):
        assert peek_scheduler_key() == "local"

    def test_resolve_cluster_name(self):
        assert resolve_cluster_name() == ("local", "local")
        assert resolve_cluster_name(cluster="dgx") == ("dgx", "ssh")


class TestClusterForJob:
    """Job-specific commands find the cluster from history (design §8)."""

    def _record(self, job_id: int, scheduler_key: str) -> None:
        from srunx.observability.storage.connection import initialized_connection

        transport = "local" if scheduler_key == "local" else "ssh"
        profile = None if transport == "local" else scheduler_key[4:]
        with initialized_connection() as conn:
            conn.execute(
                "INSERT INTO jobs (job_id, name, status, submitted_at, "
                "transport_type, profile_name, scheduler_key, submission_source) "
                "VALUES (?, 'j', 'PENDING', '2026-01-01T00:00:00Z', ?, ?, ?, 'cli')",
                (job_id, transport, profile, scheduler_key),
            )
            conn.commit()

    def test_explicit_cluster_wins(self):
        assert cluster_for_job(1, "dgx") == "dgx"

    def test_single_history_match(self):
        self._record(10, "ssh:dgx")
        assert cluster_for_job(10, None) == "dgx"

    def test_local_history_maps_to_local_cluster(self):
        self._record(11, "local")
        assert cluster_for_job(11, None) == "local"

    def test_unknown_job_needs_cluster(self):
        with pytest.raises(typer.BadParameter, match="not in srunx history"):
            cluster_for_job(12, None)

    def test_ambiguous_job_needs_cluster(self):
        self._record(13, "ssh:dgx")
        self._record(13, "ssh:foo")
        with pytest.raises(typer.BadParameter, match="several clusters"):
            cluster_for_job(13, None)


def test_job_target_reports_history_source():
    from srunx.cli._helpers.transport import job_target

    assert job_target(5, "dgx") == ("dgx", None)
    TestClusterForJob()._record(21, "ssh:dgx")
    assert job_target(21, None) == ("dgx", "history")


def test_tail_banner_says_job_history(capsys):
    from unittest.mock import MagicMock, patch

    from typer.testing import CliRunner

    from srunx.cli.main import app

    TestClusterForJob()._record(22, "local")
    with patch("srunx.slurm.local.Slurm") as slurm_cls:
        slurm_cls.return_value = MagicMock()
        result = CliRunner().invoke(app, ["tail", "22"])
    assert result.exit_code == 0, result.output
    assert "via job history" in result.stderr
