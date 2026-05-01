"""Tests for ``srunx sinfo`` (partition / state / nodelist listing).

The GPU-aggregate snapshot previously rendered by ``srunx sinfo``
moved to ``srunx gpus`` — see ``test_cli_gpus.py``. This file
exercises the new ``sinfo`` behaviour, which mirrors native SLURM
``sinfo`` output columns (PARTITION / AVAIL / TIMELIMIT / NODES /
STATE / NODELIST).
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest
from typer.testing import CliRunner

from srunx.cli.main import app
from srunx.slurm.partitions import (
    PartitionRow,
    parse_sinfo_partition_rows,
)

_SAMPLE_STDOUT = (
    "defq*|up|infinite|2|mixed|indus,sagittarius\n"
    "defq*|up|infinite|2|allocated|lynx,orion\n"
    "gpu|up|1-00:00:00|1|idle|node01\n"
)


@pytest.fixture
def runner() -> CliRunner:
    return CliRunner()


@pytest.fixture
def sample_rows() -> list[PartitionRow]:
    return parse_sinfo_partition_rows(_SAMPLE_STDOUT)


class TestSinfoParsing:
    def test_parses_default_marker_and_fields(self) -> None:
        rows = parse_sinfo_partition_rows(_SAMPLE_STDOUT)
        assert len(rows) == 3
        r0, r1, r2 = rows
        assert r0.partition == "defq"
        assert r0.is_default is True
        assert r0.avail == "up"
        assert r0.timelimit == "infinite"
        assert r0.nodes == 2
        assert r0.state == "mixed"
        assert r0.nodelist == "indus,sagittarius"

        assert r1.partition == "defq"
        assert r1.state == "allocated"
        assert r1.nodelist == "lynx,orion"

        assert r2.partition == "gpu"
        assert r2.is_default is False
        assert r2.timelimit == "1-00:00:00"
        assert r2.nodelist == "node01"

    def test_skips_malformed_lines(self) -> None:
        stdout = (
            "defq*|up|infinite|2|mixed|indus\n"
            "bogus line without delimiters\n"
            "gpu|up|1:00:00|notanumber|idle|node01\n"
            "\n"
            "ok|up|infinite|1|idle|n1\n"
        )
        rows = parse_sinfo_partition_rows(stdout)
        assert [r.partition for r in rows] == ["defq", "ok"]


class TestSinfoLocalPath:
    def test_renders_partition_rows(
        self, runner: CliRunner, sample_rows: list[PartitionRow]
    ) -> None:
        with patch(
            "srunx.slurm.partitions.fetch_sinfo_rows_local",
            return_value=sample_rows,
        ):
            result = runner.invoke(app, ["sinfo", "--local"])

        assert result.exit_code == 0, result.stdout + (result.stderr or "")
        # Native-sinfo columns must all be present in the rendered table.
        for column in (
            "PARTITION",
            "AVAIL",
            "TIMELIMIT",
            "NODES",
            "STATE",
            "NODELIST",
        ):
            assert column in result.stdout
        # Row values: default marker, nodelist, state all surface.
        assert "defq*" in result.stdout
        assert "infinite" in result.stdout
        assert "mixed" in result.stdout
        assert "allocated" in result.stdout
        assert "lynx,orion" in result.stdout
        assert "gpu" in result.stdout
        assert "node01" in result.stdout

    def test_json_format_emits_row_list(
        self, runner: CliRunner, sample_rows: list[PartitionRow]
    ) -> None:
        with patch(
            "srunx.slurm.partitions.fetch_sinfo_rows_local",
            return_value=sample_rows,
        ):
            result = runner.invoke(app, ["sinfo", "--local", "--format", "json"])

        assert result.exit_code == 0
        data = json.loads(result.stdout)
        assert isinstance(data, list)
        assert len(data) == 3
        first = data[0]
        # Same keys as PartitionRow.to_dict
        assert first["partition"] == "defq"
        assert first["is_default"] is True
        assert first["avail"] == "up"
        assert first["state"] == "mixed"
        assert first["nodelist"] == "indus,sagittarius"
        # Nodes must be an int, not a string
        assert isinstance(first["nodes"], int)

    def test_partition_filter_forwarded_to_fetcher(
        self, runner: CliRunner, sample_rows: list[PartitionRow]
    ) -> None:
        with patch(
            "srunx.slurm.partitions.fetch_sinfo_rows_local",
            return_value=sample_rows,
        ) as fetch:
            result = runner.invoke(app, ["sinfo", "--local", "--partition", "gpu"])

        assert result.exit_code == 0
        fetch.assert_called_once_with("gpu")

    def test_error_exits_nonzero(self, runner: CliRunner) -> None:
        with patch(
            "srunx.slurm.partitions.fetch_sinfo_rows_local",
            side_effect=RuntimeError("sinfo not on PATH"),
        ):
            result = runner.invoke(app, ["sinfo", "--local"])
        assert result.exit_code == 1
        assert "Error" in result.stdout


class TestSinfoSshPath:
    """``--profile`` must route through the SSH adapter, not local sinfo."""

    def test_profile_calls_ssh_fetcher(
        self, runner: CliRunner, sample_rows: list[PartitionRow]
    ) -> None:
        from srunx.transport.registry import TransportHandle

        fake_adapter = MagicMock()
        fake_handle = TransportHandle(
            scheduler_key="ssh:dgx",
            profile_name="dgx",
            transport_type="ssh",
            job_ops=fake_adapter,
            queue_client=fake_adapter,
            executor_factory=MagicMock(),
            submission_context=None,
        )

        with (
            patch(
                "srunx.transport.registry._build_ssh_handle",
                return_value=(fake_handle, None),
            ),
            patch(
                "srunx.slurm.partitions.fetch_sinfo_rows_ssh",
                return_value=sample_rows,
            ) as ssh_fetch,
            patch("srunx.slurm.partitions.fetch_sinfo_rows_local") as local_fetch,
        ):
            result = runner.invoke(
                app,
                ["sinfo", "--profile", "dgx", "--format", "json"],
            )

        assert result.exit_code == 0, result.stdout + (result.stderr or "")
        # SSH branch must run; local subprocess must not.
        ssh_fetch.assert_called_once()
        called_adapter, called_partition = ssh_fetch.call_args.args
        assert called_adapter is fake_adapter
        assert called_partition is None
        local_fetch.assert_not_called()

        data = json.loads(result.stdout)
        assert len(data) == 3
        assert {r["partition"] for r in data} == {"defq", "gpu"}

    def test_profile_with_partition_flag_forwards_filter(
        self, runner: CliRunner, sample_rows: list[PartitionRow]
    ) -> None:
        from srunx.transport.registry import TransportHandle

        fake_adapter = MagicMock()
        fake_handle = TransportHandle(
            scheduler_key="ssh:dgx",
            profile_name="dgx",
            transport_type="ssh",
            job_ops=fake_adapter,
            queue_client=fake_adapter,
            executor_factory=MagicMock(),
            submission_context=None,
        )

        with (
            patch(
                "srunx.transport.registry._build_ssh_handle",
                return_value=(fake_handle, None),
            ),
            patch(
                "srunx.slurm.partitions.fetch_sinfo_rows_ssh",
                return_value=[sample_rows[2]],
            ) as ssh_fetch,
        ):
            result = runner.invoke(
                app,
                [
                    "sinfo",
                    "--profile",
                    "dgx",
                    "--partition",
                    "gpu",
                    "--format",
                    "json",
                ],
            )

        assert result.exit_code == 0
        ssh_fetch.assert_called_once_with(fake_adapter, "gpu")
