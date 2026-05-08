"""Phase 3 A-1: scontrol fallback for SLURM status on pyxis clusters.

Exercises the parser and the three-tier sacct → squeue → scontrol probe
chain in :meth:`SSHSlurmClient.get_job_status`, which is the path the
SSH sweep/web surface uses when slurmdbd is unreachable and jobs drop
off squeue.
"""

from __future__ import annotations

from unittest.mock import MagicMock

from srunx.ssh.core.slurm import SlurmRemoteClient
from srunx.ssh.core.utils import parse_scontrol_job_state

# ── Pure parser ──────────────────────────────────────────────────────


class TestParseScontrolStatus:
    def test_parses_running(self) -> None:
        out = "JobId=123 JobName=foo JobState=RUNNING Reason=None ExitCode=0:0"
        assert parse_scontrol_job_state(out) == "RUNNING"

    def test_parses_pending(self) -> None:
        out = "JobId=1 JobState=PENDING Reason=Resources ExitCode=0:0"
        assert parse_scontrol_job_state(out) == "PENDING"

    def test_completed_with_clean_exit_is_completed(self) -> None:
        out = "JobId=1 JobState=COMPLETED Reason=None ExitCode=0:0"
        assert parse_scontrol_job_state(out) == "COMPLETED"

    def test_completed_with_nonzero_exit_downgrades_to_failed(self) -> None:
        out = "JobId=1 JobState=COMPLETED Reason=None ExitCode=1:0"
        assert parse_scontrol_job_state(out) == "FAILED"

    def test_completed_with_signal_downgrades_to_failed(self) -> None:
        out = "JobId=1 JobState=COMPLETED Reason=None ExitCode=0:9"
        assert parse_scontrol_job_state(out) == "FAILED"

    def test_failed_state_passes_through(self) -> None:
        out = "JobId=1 JobState=FAILED Reason=NonZeroExit ExitCode=1:0"
        assert parse_scontrol_job_state(out) == "FAILED"

    def test_cancelled_state_passes_through(self) -> None:
        out = "JobId=1 JobState=CANCELLED Reason=None ExitCode=0:15"
        assert parse_scontrol_job_state(out) == "CANCELLED"

    def test_empty_input_returns_none(self) -> None:
        assert parse_scontrol_job_state("") is None

    def test_whitespace_only_returns_none(self) -> None:
        assert parse_scontrol_job_state("   \n  \t  \n") is None

    def test_missing_jobstate_returns_none(self) -> None:
        out = "JobId=1 JobName=foo Reason=None"
        assert parse_scontrol_job_state(out) is None

    def test_multiline_output(self) -> None:
        out = (
            "JobId=42 JobName=train\n"
            "   UserId=u(1000) GroupId=g(1000)\n"
            "   Priority=1 JobState=COMPLETED Reason=None ExitCode=0:0\n"
            "   RunTime=00:05:12 TimeLimit=01:00:00\n"
        )
        assert parse_scontrol_job_state(out) == "COMPLETED"


# ── get_job_status three-tier fallback ───────────────────────────────


def _bare_ssh_client() -> SlurmRemoteClient:
    """Minimal SlurmRemoteClient bypassing paramiko / SLURM-path discovery.

    ``get_job_status`` lives on the :class:`SlurmRemoteClient` component
    after the facade-deduplication refactor, so the three-tier fallback
    tests target it directly. ``_conn`` / ``_files`` are MagicMocks the
    tests don't read; the suite stubs ``execute_slurm_command`` directly
    since that's the only path ``get_job_status`` traverses.
    """
    slurm = SlurmRemoteClient(MagicMock(), MagicMock())
    slurm.logger = MagicMock()
    return slurm


def _stub_exec(responses: list[tuple[str, str, int]]):
    """Return an `execute_slurm_command` stub that pops responses in order."""
    idx = {"i": 0}

    def _exec(cmd: str) -> tuple[str, str, int]:
        i = idx["i"]
        idx["i"] += 1
        assert i < len(responses), f"unexpected call #{i}: {cmd!r}"
        return responses[i]

    _exec.calls_made = idx  # type: ignore[attr-defined]
    return _exec


class TestGetJobStatusFallback:
    def test_sacct_hit_short_circuits(self) -> None:
        client = _bare_ssh_client()
        client.execute_slurm_command = _stub_exec(  # type: ignore[method-assign]
            [("12345  COMPLETED\n", "", 0)]
        )
        assert client.get_job_status("12345") == "COMPLETED"

    def test_squeue_hit_when_sacct_empty(self) -> None:
        client = _bare_ssh_client()
        client.execute_slurm_command = _stub_exec(  # type: ignore[method-assign]
            [("", "", 0), ("RUNNING\n", "", 0)]
        )
        assert client.get_job_status("123") == "RUNNING"

    def test_uses_scontrol_when_sacct_and_squeue_empty(self) -> None:
        """Core A-1 regression: pyxis, sacct empty, job dropped from squeue."""
        client = _bare_ssh_client()
        scontrol_out = (
            "JobId=777 JobName=ok JobState=COMPLETED Reason=None ExitCode=0:0"
        )
        client.execute_slurm_command = _stub_exec(  # type: ignore[method-assign]
            [
                ("", "", 0),  # sacct
                ("", "", 0),  # squeue
                (scontrol_out, "", 0),  # scontrol
            ]
        )
        assert client.get_job_status("777") == "COMPLETED"

    def test_scontrol_nonzero_exit_yields_failed(self) -> None:
        client = _bare_ssh_client()
        scontrol_out = "JobId=777 JobState=COMPLETED Reason=NonZeroExit ExitCode=1:0"
        client.execute_slurm_command = _stub_exec(  # type: ignore[method-assign]
            [("", "", 0), ("", "", 0), (scontrol_out, "", 0)]
        )
        assert client.get_job_status("777") == "FAILED"

    def test_all_three_sources_empty_returns_not_found(self) -> None:
        client = _bare_ssh_client()
        client.execute_slurm_command = _stub_exec(  # type: ignore[method-assign]
            [("", "", 0), ("", "", 0), ("", "", 0)]
        )
        assert client.get_job_status("999") == "NOT_FOUND"

    def test_scontrol_command_failure_returns_not_found(self) -> None:
        """scontrol exiting non-zero (e.g. permission denied) → NOT_FOUND."""
        client = _bare_ssh_client()
        client.execute_slurm_command = _stub_exec(  # type: ignore[method-assign]
            [("", "", 0), ("", "", 0), ("", "no job", 1)]
        )
        assert client.get_job_status("999") == "NOT_FOUND"

    def test_invalid_job_id_short_circuits_without_probing(self) -> None:
        client = _bare_ssh_client()
        # No stubs registered — if get_job_status reaches execute_slurm_command
        # it would AttributeError.
        assert client.get_job_status("not; a-number") == "ERROR"
