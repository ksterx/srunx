"""Tests for ``srunx.slurm.protocols`` and ``queue_by_ids`` implementations."""

from __future__ import annotations

import subprocess
from datetime import datetime
from unittest.mock import MagicMock, patch

from srunx.slurm.local import Slurm
from srunx.slurm.protocols import (
    Client,
    JobSnapshot,
    parse_slurm_datetime,
    parse_slurm_duration,
)

# ---- parse helpers ----


def test_parse_slurm_datetime_valid() -> None:
    got = parse_slurm_datetime("2026-04-18T10:00:00")
    assert got == datetime(2026, 4, 18, 10, 0, 0)


def test_parse_slurm_datetime_unknown_values() -> None:
    for v in ("", "   ", "N/A", "Unknown", "None", None):
        assert parse_slurm_datetime(v) is None


def test_parse_slurm_datetime_invalid() -> None:
    assert parse_slurm_datetime("not-a-date") is None


def test_parse_slurm_duration_hhmmss() -> None:
    assert parse_slurm_duration("01:02:03") == 3723


def test_parse_slurm_duration_mmss() -> None:
    assert parse_slurm_duration("02:03") == 123


def test_parse_slurm_duration_with_days() -> None:
    assert parse_slurm_duration("2-01:00:00") == 2 * 86400 + 3600


def test_parse_slurm_duration_invalid_values() -> None:
    for v in (None, "", "N/A", "Unknown", "abc", "1:2:3:4"):
        assert parse_slurm_duration(v) is None


# ---- JobSnapshot ----


def test_job_status_info_defaults_optional() -> None:
    info = JobSnapshot(status="RUNNING")
    assert info.status == "RUNNING"
    assert info.started_at is None
    assert info.completed_at is None
    assert info.duration_secs is None
    assert info.nodelist is None


# ---- Slurm.queue_by_ids ----


def _run_result(stdout: str = "", returncode: int = 0) -> MagicMock:
    m = MagicMock(spec=subprocess.CompletedProcess)
    m.stdout = stdout
    m.stderr = ""
    m.returncode = returncode
    return m


def test_queue_by_ids_empty_returns_empty_dict() -> None:
    client = Slurm()
    with patch("subprocess.run") as run_mock:
        got = client.queue_by_ids([])
    assert got == {}
    run_mock.assert_not_called()


def test_queue_by_ids_parses_squeue_output() -> None:
    client = Slurm()
    squeue_out = (
        "12345|RUNNING|2026-04-18T10:00:00|00:05:23|node01\n"
        "12346|PENDING|N/A|00:00:00|(Resources)\n"
    )
    with patch("subprocess.run") as run_mock:
        run_mock.return_value = _run_result(squeue_out)
        got = client.queue_by_ids([12345, 12346])

    # Only squeue was called (all ids found there).
    assert run_mock.call_count == 1
    cmd = run_mock.call_args.args[0]
    assert cmd[0] == "squeue"
    assert "12345,12346" in cmd

    assert got[12345].status == "RUNNING"
    assert got[12345].started_at == datetime(2026, 4, 18, 10, 0, 0)
    assert got[12345].duration_secs == 5 * 60 + 23
    assert got[12345].nodelist == "node01"
    assert got[12345].completed_at is None

    assert got[12346].status == "PENDING"
    assert got[12346].started_at is None
    assert got[12346].duration_secs == 0


def test_queue_by_ids_falls_back_to_sacct_for_terminal_jobs() -> None:
    client = Slurm()
    squeue_out = ""  # job no longer in queue
    sacct_out = (
        "12345|COMPLETED|2026-04-18T10:00:00|2026-04-18T11:00:00|01:00:00|node01\n"
        "12345.batch|COMPLETED|2026-04-18T10:00:00|2026-04-18T11:00:00|01:00:00|node01\n"
    )

    def fake_run(cmd: list[str], *_, **__):
        if cmd[0] == "squeue":
            return _run_result(squeue_out)
        if cmd[0] == "sacct":
            return _run_result(sacct_out)
        return _run_result("", returncode=1)

    with patch("subprocess.run", side_effect=fake_run):
        got = client.queue_by_ids([12345])

    assert got[12345].status == "COMPLETED"
    assert got[12345].started_at == datetime(2026, 4, 18, 10, 0, 0)
    assert got[12345].completed_at == datetime(2026, 4, 18, 11, 0, 0)
    assert got[12345].duration_secs == 3600
    assert got[12345].nodelist == "node01"


def test_queue_by_ids_missing_jobs_are_omitted() -> None:
    client = Slurm()

    def fake_run(cmd: list[str], *_, **__):
        # Neither squeue nor sacct know about the job.
        return _run_result("")

    with patch("subprocess.run", side_effect=fake_run):
        got = client.queue_by_ids([99999])

    assert got == {}


def test_queue_by_ids_cancelled_state_word_is_extracted() -> None:
    client = Slurm()
    squeue_out = ""
    sacct_out = "12345|CANCELLED by 1000|2026-04-18T10:00:00|2026-04-18T10:05:00|00:05:00|node01\n"

    def fake_run(cmd: list[str], *_, **__):
        if cmd[0] == "squeue":
            return _run_result(squeue_out)
        return _run_result(sacct_out)

    with patch("subprocess.run", side_effect=fake_run):
        got = client.queue_by_ids([12345])

    assert got[12345].status == "CANCELLED"


# ---- Protocol ----


def test_slurm_satisfies_protocol() -> None:
    client = Slurm()
    # runtime_checkable — covers structural subtyping.
    assert isinstance(client, Client)
