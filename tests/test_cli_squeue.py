"""Tests for ``srunx squeue`` (active-jobs listing).

The command now mirrors native ``squeue`` semantics: all users' jobs by
default, User/Partition/CPUs/GPUs/NodeList columns, ``-u/--user`` to
filter. The previous ``--show-gpus`` flag is gone — GPUs are always
shown because the user explicitly asked for a combined CPU + GPU +
NODELIST view.
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest
from typer.testing import CliRunner

from srunx.cli.main import app
from srunx.domain import BaseJob, JobStatus


@pytest.fixture
def runner() -> CliRunner:
    return CliRunner()


def _make_job(
    *,
    job_id: int,
    name: str,
    user: str,
    partition: str,
    status: JobStatus,
    nodes: int,
    cpus: int,
    gpus: int,
    nodelist: str,
    elapsed: str = "0:05",
    time_limit: str = "UNLIMITED",
) -> BaseJob:
    job = BaseJob(
        name=name,
        job_id=job_id,
        user=user,
        partition=partition,
        nodes=nodes,
        cpus=cpus,
        gpus=gpus,
        nodelist=nodelist,
        elapsed_time=elapsed,
        time_limit=time_limit,
    )
    job.status = status
    return job


@pytest.fixture
def mock_jobs() -> list[BaseJob]:
    return [
        _make_job(
            job_id=12345,
            name="gpu_job",
            user="alice",
            partition="gpu",
            status=JobStatus.RUNNING,
            nodes=2,
            cpus=32,
            gpus=8,
            nodelist="dgx-node[1-2]",
        ),
        _make_job(
            job_id=12346,
            name="cpu_job",
            user="bob",
            partition="defq",
            status=JobStatus.PENDING,
            nodes=1,
            cpus=4,
            gpus=0,
            nodelist="(Priority)",
        ),
        _make_job(
            job_id=12347,
            name="multi_gpu",
            user="ksterx",
            partition="gpu",
            status=JobStatus.RUNNING,
            nodes=4,
            cpus=128,
            gpus=16,
            nodelist="dgx-node[3-6]",
        ),
    ]


class TestSqueueTable:
    def test_default_columns(self, runner: CliRunner, mock_jobs) -> None:
        """Default view: Job ID / User / Name / Status / GPUs / Elapsed /
        NodeList. Partition / CPUs / Limit / Nodes are opt-in."""
        with patch("srunx.slurm.local.Slurm") as mock_slurm:
            client = MagicMock()
            client.queue.return_value = mock_jobs
            mock_slurm.return_value = client
            # Rich auto-shrinks column headers when the (captured) terminal
            # is narrower than the table needs — set COLUMNS so header text
            # doesn't truncate to single characters.
            result = runner.invoke(app, ["squeue", "--local"], env={"COLUMNS": "200"})

        assert result.exit_code == 0
        for shown in (
            "Job ID",
            "User",
            "Name",
            "Status",
            "GPUs",
            "Elapsed",
            "NodeList",
        ):
            assert shown in result.stdout, f"default column missing: {shown}"
        # Opt-in columns must not appear by default.
        for hidden in ("Partition", "CPUs", "Limit", "Nodes"):
            assert hidden not in result.stdout, f"opt-in column leaked: {hidden}"
        # Default-set values still surface.
        assert "alice" in result.stdout
        assert "dgx-node[1-2]" in result.stdout
        assert "RUNNING" in result.stdout

    def test_show_all_flag_reveals_every_column(
        self, runner: CliRunner, mock_jobs
    ) -> None:
        with patch("srunx.slurm.local.Slurm") as mock_slurm:
            client = MagicMock()
            client.queue.return_value = mock_jobs
            mock_slurm.return_value = client
            result = runner.invoke(
                app, ["squeue", "--local", "-a"], env={"COLUMNS": "200"}
            )

        assert result.exit_code == 0
        for header in (
            "Job ID",
            "User",
            "Name",
            "Partition",
            "Status",
            "Nodes",
            "CPUs",
            "GPUs",
            "Elapsed",
            "Limit",
            "NodeList",
        ):
            assert header in result.stdout, f"column missing under -a: {header}"

    def test_individual_show_flags(self, runner: CliRunner, mock_jobs) -> None:
        """Each --show-X flag adds exactly one of the opt-in columns."""
        with patch("srunx.slurm.local.Slurm") as mock_slurm:
            client = MagicMock()
            client.queue.return_value = mock_jobs
            mock_slurm.return_value = client
            result = runner.invoke(
                app,
                ["squeue", "--local", "--show-partition", "--show-cpus"],
                env={"COLUMNS": "200"},
            )

        assert result.exit_code == 0
        assert "Partition" in result.stdout
        assert "CPUs" in result.stdout
        # The two NOT requested remain hidden.
        assert "Limit" not in result.stdout
        assert "Nodes" not in result.stdout

    def test_empty_queue(self, runner: CliRunner) -> None:
        with patch("srunx.slurm.local.Slurm") as mock_slurm:
            client = MagicMock()
            client.queue.return_value = []
            mock_slurm.return_value = client
            result = runner.invoke(app, ["squeue", "--local"])
        assert result.exit_code == 0
        assert "No jobs in queue" in result.stdout

    def test_error_exit_code(self, runner: CliRunner) -> None:
        with patch("srunx.slurm.local.Slurm") as mock_slurm:
            client = MagicMock()
            client.queue.side_effect = RuntimeError("SLURM connection error")
            mock_slurm.return_value = client
            result = runner.invoke(app, ["squeue", "--local"])
        assert result.exit_code == 1


class TestSqueueJson:
    def test_json_schema_includes_new_fields(
        self, runner: CliRunner, mock_jobs
    ) -> None:
        with patch("srunx.slurm.local.Slurm") as mock_slurm:
            client = MagicMock()
            client.queue.return_value = mock_jobs
            mock_slurm.return_value = client
            result = runner.invoke(app, ["squeue", "--local", "--format", "json"])

        assert result.exit_code == 0
        data = json.loads(result.stdout)
        assert len(data) == 3
        first = data[0]
        # Every field a srunx user should see for a job — surfaced in
        # the JSON too so scripts don't have to round-trip through the
        # table renderer.
        assert first["job_id"] == 12345
        assert first["user"] == "alice"
        assert first["name"] == "gpu_job"
        assert first["partition"] == "gpu"
        assert first["status"] == "RUNNING"
        assert first["nodes"] == 2
        assert first["cpus"] == 32
        assert first["gpus"] == 8
        assert first["nodelist"] == "dgx-node[1-2]"

    def test_empty_queue_emits_empty_list(self, runner: CliRunner) -> None:
        """Pre-existing regression — ``--format json`` on empty queue
        must emit ``[]`` (valid JSON), not the human string."""
        with patch("srunx.slurm.local.Slurm") as mock_slurm:
            client = MagicMock()
            client.queue.return_value = []
            mock_slurm.return_value = client
            result = runner.invoke(app, ["squeue", "--local", "--format", "json"])
        assert result.exit_code == 0
        assert json.loads(result.stdout) == []


class TestSqueueUserFilter:
    def test_user_flag_forwarded_to_client(self, runner: CliRunner, mock_jobs) -> None:
        """``--user`` must reach the ``queue()`` call so SLURM filters
        server-side — client-side filtering would miss jobs that
        squeue's own ``--user`` flag would include in edge cases."""
        with patch("srunx.slurm.local.Slurm") as mock_slurm:
            client = MagicMock()
            client.queue.return_value = mock_jobs
            mock_slurm.return_value = client
            result = runner.invoke(
                app, ["squeue", "--local", "--user", "alice", "--format", "json"]
            )

        assert result.exit_code == 0
        client.queue.assert_called_once_with(user="alice")

    def test_default_shows_all_users(self, runner: CliRunner, mock_jobs) -> None:
        """Without ``--user``, pass ``user=None`` — matches native
        ``squeue`` (all users) and local ``Slurm.queue`` semantics."""
        with patch("srunx.slurm.local.Slurm") as mock_slurm:
            client = MagicMock()
            client.queue.return_value = mock_jobs
            mock_slurm.return_value = client
            result = runner.invoke(app, ["squeue", "--local", "--format", "json"])

        assert result.exit_code == 0
        client.queue.assert_called_once_with(user=None)


class TestSqueueJobIdFilter:
    def test_job_id_filter_narrows_result(self, runner: CliRunner, mock_jobs) -> None:
        with patch("srunx.slurm.local.Slurm") as mock_slurm:
            client = MagicMock()
            client.queue.return_value = mock_jobs
            mock_slurm.return_value = client
            result = runner.invoke(
                app, ["squeue", "--local", "-j", "12346", "--format", "json"]
            )
        assert result.exit_code == 0
        data = json.loads(result.stdout)
        assert len(data) == 1
        assert data[0]["job_id"] == 12346
