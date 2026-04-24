"""Tests for ``srunx sacct`` (native SLURM sacct wrapper).

Distinct from ``srunx history`` (which reads srunx's own SQLite):
``sacct`` shells out to the cluster accounting DB via the real
``sacct`` binary. Tests here exercise the parser, the step-filtering
default, and the local / SSH transport dispatch — never the real
subprocess.
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest
from typer.testing import CliRunner

from srunx.cli.main import app
from srunx.slurm.accounting import (
    SacctRow,
    filter_out_steps,
    parse_sacct_rows,
)

# 13-column sample: JobID|JobName|User|Partition|Account|State|ExitCode|
# Elapsed|Submit|Start|End|AllocCPUS|AllocTRES
_SAMPLE_STDOUT = (
    "2780|train|alice|defq|sci|RUNNING|0:0|00:05:00|"
    "2026-04-24T12:00:00|2026-04-24T12:00:05|Unknown|2|cpu=2,gres/gpu=1\n"
    "2780.batch|batch||||RUNNING|0:0|00:05:00||||2|cpu=2\n"
    "19849|g4-cpt-v17|bob|defq||FAILED by 1000|1:0|01:00:00|"
    "2026-04-23T10:00:00|2026-04-23T10:00:05|2026-04-23T11:00:05|16|"
    "cpu=16,gres/gpu=8\n"
    "19849.batch|batch||||FAILED|1:0|01:00:00||||16|cpu=16\n"
)


@pytest.fixture
def runner() -> CliRunner:
    return CliRunner()


@pytest.fixture
def sample_rows() -> list[SacctRow]:
    return parse_sacct_rows(_SAMPLE_STDOUT)


class TestSacctParsing:
    def test_field_extraction(self) -> None:
        rows = parse_sacct_rows(_SAMPLE_STDOUT)
        assert len(rows) == 4

        parent, step, failed, failed_step = rows
        assert parent.job_id == "2780"
        assert parent.is_step is False
        assert parent.user == "alice"
        assert parent.partition == "defq"
        assert parent.account == "sci"
        assert parent.state == "RUNNING"
        assert parent.alloc_cpus == 2
        assert parent.alloc_tres == "cpu=2,gres/gpu=1"

        assert step.job_id == "2780.batch"
        assert step.is_step is True
        assert step.user is None  # sub-steps have empty user
        assert step.alloc_cpus == 2

        assert failed.job_id == "19849"
        # "FAILED by 1000" must collapse to the canonical state name.
        assert failed.state == "FAILED"
        assert failed.user == "bob"
        assert failed.exit_code == "1:0"

        assert failed_step.is_step is True

    def test_filter_out_steps_returns_parents_only(
        self, sample_rows: list[SacctRow]
    ) -> None:
        parents = filter_out_steps(sample_rows)
        assert [r.job_id for r in parents] == ["2780", "19849"]

    def test_malformed_rows_skipped(self) -> None:
        # Too few fields → dropped; correctly shaped survives.
        stdout = (
            "not|enough|fields\n"
            "2780|train|alice|defq|sci|RUNNING|0:0|00:05:00||||2|cpu=2\n"
        )
        rows = parse_sacct_rows(stdout)
        assert [r.job_id for r in rows] == ["2780"]


class TestSacctLocalCLI:
    def test_default_hides_steps(
        self, runner: CliRunner, sample_rows: list[SacctRow]
    ) -> None:
        """Parent rows only by default — .batch steps should be hidden."""
        with patch(
            "srunx.slurm.accounting.fetch_sacct_rows_local",
            return_value=sample_rows,
        ):
            result = runner.invoke(app, ["sacct", "--local"], env={"COLUMNS": "200"})

        assert result.exit_code == 0, result.stdout + (result.stderr or "")
        assert "2780" in result.stdout
        assert "19849" in result.stdout
        # Sub-step rows must not leak through the default view.
        assert "2780.batch" not in result.stdout
        assert "19849.batch" not in result.stdout

    def test_show_steps_includes_sub_steps(
        self, runner: CliRunner, sample_rows: list[SacctRow]
    ) -> None:
        with patch(
            "srunx.slurm.accounting.fetch_sacct_rows_local",
            return_value=sample_rows,
        ):
            result = runner.invoke(
                app,
                ["sacct", "--local", "--show-steps"],
                env={"COLUMNS": "200"},
            )

        assert result.exit_code == 0
        assert "2780.batch" in result.stdout
        assert "19849.batch" in result.stdout

    def test_default_columns(
        self, runner: CliRunner, sample_rows: list[SacctRow]
    ) -> None:
        with patch(
            "srunx.slurm.accounting.fetch_sacct_rows_local",
            return_value=sample_rows,
        ):
            result = runner.invoke(app, ["sacct", "--local"], env={"COLUMNS": "200"})

        assert result.exit_code == 0
        for shown in (
            "Job ID",
            "User",
            "Name",
            "Partition",
            "State",
            "ExitCode",
            "Elapsed",
        ):
            assert shown in result.stdout, f"default column missing: {shown}"
        # Opt-in columns stay hidden.
        assert "Account" not in result.stdout
        assert "Submit" not in result.stdout
        assert "Start" not in result.stdout
        assert "End" not in result.stdout

    def test_show_account_and_times(
        self, runner: CliRunner, sample_rows: list[SacctRow]
    ) -> None:
        with patch(
            "srunx.slurm.accounting.fetch_sacct_rows_local",
            return_value=sample_rows,
        ):
            result = runner.invoke(
                app,
                [
                    "sacct",
                    "--local",
                    "--show-account",
                    "--show-times",
                ],
                env={"COLUMNS": "240"},
            )

        assert result.exit_code == 0
        assert "Account" in result.stdout
        assert "Submit" in result.stdout
        assert "Start" in result.stdout
        assert "End" in result.stdout

    def test_json_schema(self, runner: CliRunner, sample_rows: list[SacctRow]) -> None:
        with patch(
            "srunx.slurm.accounting.fetch_sacct_rows_local",
            return_value=sample_rows,
        ):
            result = runner.invoke(app, ["sacct", "--local", "--format", "json"])

        assert result.exit_code == 0
        data = json.loads(result.stdout)
        # Default (no --show-steps): parent rows only.
        assert len(data) == 2
        first = data[0]
        for key in (
            "job_id",
            "job_name",
            "user",
            "partition",
            "account",
            "state",
            "exit_code",
            "elapsed",
            "submit",
            "start",
            "end",
            "alloc_cpus",
            "alloc_tres",
            "is_step",
        ):
            assert key in first, f"JSON field missing: {key}"

    def test_empty_shows_sentinel(self, runner: CliRunner) -> None:
        with patch(
            "srunx.slurm.accounting.fetch_sacct_rows_local",
            return_value=[],
        ):
            result = runner.invoke(app, ["sacct", "--local"])
        assert result.exit_code == 0
        assert "No accounting records" in result.stdout

    def test_filters_forwarded_to_fetcher(
        self, runner: CliRunner, sample_rows: list[SacctRow]
    ) -> None:
        """Every CLI filter option must reach the subprocess fetcher.

        Otherwise SLURM would happily run an unfiltered query and the
        CLI would post-process client-side — defeating the point of
        letting slurmdbd narrow results.
        """
        with patch(
            "srunx.slurm.accounting.fetch_sacct_rows_local",
            return_value=sample_rows,
        ) as fetch:
            result = runner.invoke(
                app,
                [
                    "sacct",
                    "--local",
                    "-j",
                    "2780",
                    "-j",
                    "19849",
                    "-u",
                    "alice",
                    "-S",
                    "now-1day",
                    "-E",
                    "now",
                    "-s",
                    "FAILED,TIMEOUT",
                    "-p",
                    "defq",
                ],
            )

        assert result.exit_code == 0
        fetch.assert_called_once_with(
            job_ids=[2780, 19849],
            user="alice",
            all_users=False,
            start_time="now-1day",
            end_time="now",
            state="FAILED,TIMEOUT",
            partition="defq",
        )

    def test_allusers_and_user_both_forwarded(
        self, runner: CliRunner, sample_rows: list[SacctRow]
    ) -> None:
        """Both flags flow through to the fetcher so sacct can combine
        them (``-a`` scans everyone, ``-u`` narrows to one user)."""
        with patch(
            "srunx.slurm.accounting.fetch_sacct_rows_local",
            return_value=sample_rows,
        ) as fetch:
            result = runner.invoke(app, ["sacct", "--local", "-a", "-u", "alice"])

        assert result.exit_code == 0
        kwargs = fetch.call_args.kwargs
        assert kwargs["all_users"] is True
        assert kwargs["user"] == "alice"


class TestSacctSshRouting:
    """``--profile`` must route through the SSH adapter, not local subprocess."""

    def test_profile_calls_ssh_fetcher(
        self, runner: CliRunner, sample_rows: list[SacctRow]
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
                "srunx.slurm.accounting.fetch_sacct_rows_ssh",
                return_value=sample_rows,
            ) as ssh_fetch,
            patch("srunx.slurm.accounting.fetch_sacct_rows_local") as local_fetch,
        ):
            result = runner.invoke(
                app,
                ["sacct", "--profile", "dgx", "--format", "json"],
            )

        assert result.exit_code == 0, result.stdout + (result.stderr or "")
        ssh_fetch.assert_called_once()
        # First positional arg is the adapter — guard so a refactor
        # can't silently pass something else.
        assert ssh_fetch.call_args.args[0] is fake_adapter
        local_fetch.assert_not_called()


class TestBuildFilterArgs:
    """Direct unit coverage of the shared filter-arg builder.

    Guarantees the two fetchers can't diverge on which CLI flag maps
    to which ``sacct`` argument.
    """

    def test_allusers_and_user_coexist(self) -> None:
        """Native sacct accepts ``-a -u <name>`` (scan-all-then-filter).

        Earlier srunx dropped ``-u`` when ``-a`` was set; this guards
        the regression-fix that now forwards both flags to match real
        sacct semantics.
        """
        from srunx.slurm.accounting import build_sacct_filter_args

        args = build_sacct_filter_args(user="alice", all_users=True)
        assert "--allusers" in args
        assert "--user" in args
        assert "alice" in args

    def test_user_forwarded_when_no_allusers(self) -> None:
        from srunx.slurm.accounting import build_sacct_filter_args

        args = build_sacct_filter_args(user="alice")
        assert "--user" in args
        assert "alice" in args

    def test_job_ids_comma_joined(self) -> None:
        from srunx.slurm.accounting import build_sacct_filter_args

        args = build_sacct_filter_args(job_ids=[1, 2, 3])
        assert "--jobs" in args
        assert "1,2,3" in args
