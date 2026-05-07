"""Phase 2: LSP alignment of Slurm and SlurmSSHClient with JobOperations.

These tests verify the new Protocol-conformant entry points added in
``src/srunx/client.py`` and ``src/srunx/web/ssh_adapter.py`` without
exercising the full submit path (which would need a live SLURM or SSH
endpoint). The goal is surface conformance, not behavioural regression
— existing tests cover the latter.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest

from srunx.slurm.local import Slurm
from srunx.slurm.protocols import JobOperations, LogChunk


class TestSlurmProtocolCompliance:
    def test_slurm_satisfies_protocol(self) -> None:
        """``Slurm()`` must be a runtime-checkable JobOperations."""
        assert isinstance(Slurm(), JobOperations)

    def test_status_delegates_to_retrieve(self) -> None:
        """``status`` is a thin alias for ``retrieve`` on the happy path."""
        s = Slurm()
        with patch.object(s, "retrieve", return_value="mock_result") as m:
            assert s.status(12345) == "mock_result"
            m.assert_called_once_with(12345)

    def test_status_wraps_unknown_id_into_job_not_found(self) -> None:
        """``retrieve`` raises ValueError on missing jobs; status converts it."""
        from srunx.common.exceptions import JobNotFoundError

        s = Slurm()
        with patch.object(s, "retrieve", side_effect=ValueError("no such job")):
            with pytest.raises(JobNotFoundError):
                s.status(99999)


class TestSlurmTailLogIncremental:
    def test_missing_file_returns_empty_chunk_at_same_offset(self) -> None:
        """If the log file doesn't exist yet, return empty LogChunk at same offset."""
        s = Slurm()
        with patch.object(Slurm, "_find_log_paths", return_value=(None, None)):
            chunk = s.tail_log_incremental(job_id=1, stdout_offset=0, stderr_offset=0)
            assert isinstance(chunk, LogChunk)
            assert chunk.stdout == ""
            assert chunk.stderr == ""
            assert chunk.stdout_offset == 0
            assert chunk.stderr_offset == 0

    def test_reads_from_offset_and_advances(self, tmp_path) -> None:
        """Reading from a real file advances the offset by bytes consumed."""
        log = tmp_path / "job.log"
        log.write_text("line1\nline2\n")
        s = Slurm()
        with patch.object(Slurm, "_find_log_paths", return_value=(str(log), str(log))):
            # Fresh read: offset 0 → all bytes.
            chunk = s.tail_log_incremental(job_id=1, stdout_offset=0, stderr_offset=0)
            assert chunk.stdout == "line1\nline2\n"
            assert chunk.stdout_offset == len(b"line1\nline2\n")
            # When stderr is the same path as stdout, we don't double-count.
            assert chunk.stderr == ""
            assert chunk.stderr_offset == 0

    def test_returns_log_chunk_type(self) -> None:
        """Return value is a Pydantic LogChunk, not a tuple or dict."""
        s = Slurm()
        with patch.object(Slurm, "_find_log_paths", return_value=(None, None)):
            chunk = s.tail_log_incremental(job_id=1)
        assert isinstance(chunk, LogChunk)


class TestSlurmSSHClientProtocolCompliance:
    """SSHAdapter needs a live SSH connection to instantiate — use
    class-level structural checks instead of ``isinstance(instance, ...)``.
    """

    def test_ssh_adapter_has_protocol_methods(self) -> None:
        """Structural check: all 5 Protocol methods are defined."""
        from srunx.slurm.clients.ssh import SlurmSSHClient

        for method in ("submit", "cancel", "status", "queue", "tail_log_incremental"):
            assert hasattr(SlurmSSHClient, method), f"missing method: {method}"
            assert callable(getattr(SlurmSSHClient, method))

    def test_ssh_adapter_backcompat_aliases_preserved(self) -> None:
        """Existing method names must still be callable (non-breaking change)."""
        from srunx.slurm.clients.ssh import SlurmSSHClient

        for legacy_method in (
            "submit_job",
            "cancel_job",
            "get_job_status",
            "list_jobs",
            "get_job_output",
        ):
            assert hasattr(SlurmSSHClient, legacy_method), (
                f"legacy method removed: {legacy_method}"
            )
            assert callable(getattr(SlurmSSHClient, legacy_method))

    def test_ssh_adapter_status_returns_base_job_shape(self) -> None:
        """``status`` returns a BaseJob whose ``status`` attribute is a JobStatus."""
        from srunx.domain import BaseJob, JobStatus
        from srunx.slurm.clients.ssh import SlurmSSHClient
        from srunx.slurm.protocols import JobSnapshot

        # Patch out ``queue_by_ids`` to avoid touching any SSH client.
        with patch.object(
            SlurmSSHClient,
            "queue_by_ids",
            return_value={99: JobSnapshot(status="RUNNING")},
        ):
            # Bypass __init__ entirely — we only test the status() path shape.
            adapter = SlurmSSHClient.__new__(SlurmSSHClient)
            result = adapter.status(99)
        assert isinstance(result, BaseJob)
        assert result.job_id == 99
        assert result._status == JobStatus.RUNNING

    def test_ssh_adapter_status_raises_job_not_found(self) -> None:
        """Missing job → JobNotFoundError, matching the Protocol contract."""
        from srunx.common.exceptions import JobNotFoundError
        from srunx.slurm.clients.ssh import SlurmSSHClient

        with patch.object(SlurmSSHClient, "queue_by_ids", return_value={}):
            adapter = SlurmSSHClient.__new__(SlurmSSHClient)
            with pytest.raises(JobNotFoundError):
                adapter.status(99999)

    def test_ssh_adapter_queue_returns_list_base_job(self) -> None:
        """``queue`` adapts active-squeue dicts into Pydantic BaseJob instances.

        Post-S1: ``queue`` routes through :meth:`_list_active_jobs`
        (squeue only) rather than :meth:`list_jobs` (squeue + sacct
        merge), so ``srunx squeue`` matches native SLURM ``squeue``
        semantics. This test patches the active-only helper to prove
        the sacct merge never runs on the CLI path.
        """
        from srunx.domain import BaseJob, JobStatus
        from srunx.slurm.clients.ssh import SlurmSSHClient

        fake_rows = [
            {
                "name": "jobA",
                "job_id": 101,
                "status": "RUNNING",
                "partition": "gpu",
                "nodes": 1,
                "gpus": 2,
                "elapsed_time": "0:10",
            }
        ]
        with (
            patch.object(
                SlurmSSHClient,
                "_list_active_jobs",
                return_value=(fake_rows, {101}),
            ),
            patch.object(SlurmSSHClient, "list_jobs") as list_jobs_mock,
        ):
            adapter = SlurmSSHClient.__new__(SlurmSSHClient)
            adapter._username = "alice"  # queue() reads profile's username on None
            out = adapter.queue(user="alice")
        assert isinstance(out, list)
        assert len(out) == 1
        assert isinstance(out[0], BaseJob)
        assert out[0].job_id == 101
        assert out[0]._status == JobStatus.RUNNING
        # Regression guard — queue must NOT pull the sacct merge path.
        list_jobs_mock.assert_not_called()

    def test_ssh_adapter_tail_log_incremental_returns_log_chunk(self) -> None:
        """``tail_log_incremental`` returns a Pydantic LogChunk."""
        from srunx.slurm.clients.ssh import SlurmSSHClient
        from srunx.slurm.protocols import LogChunk

        with patch.object(
            SlurmSSHClient,
            "get_job_output",
            return_value=("stdout bytes", "stderr bytes", 12, 12),
        ):
            adapter = SlurmSSHClient.__new__(SlurmSSHClient)
            chunk = adapter.tail_log_incremental(
                job_id=1, stdout_offset=0, stderr_offset=0
            )
        assert isinstance(chunk, LogChunk)
        assert chunk.stdout == "stdout bytes"
        assert chunk.stderr == "stderr bytes"
        assert chunk.stdout_offset == 12
        assert chunk.stderr_offset == 12
