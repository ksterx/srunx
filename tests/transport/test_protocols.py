"""Unit tests for Phase 1 of CLI transport unification."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from srunx.common.exceptions import (
    JobNotFoundError,
    RemoteCommandError,
    SubmissionError,
    TransportAuthError,
    TransportConnectionError,
    TransportError,
    TransportTimeoutError,
)
from srunx.slurm.protocols import JobOperations, LogChunk


class TestLogChunk:
    def test_construct_zero_offsets(self) -> None:
        chunk = LogChunk(stdout="", stderr="", stdout_offset=0, stderr_offset=0)
        assert chunk.stdout == ""
        assert chunk.stdout_offset == 0

    def test_construct_positive_offsets(self) -> None:
        chunk = LogChunk(stdout="a", stderr="b", stdout_offset=10, stderr_offset=5)
        assert chunk.stdout_offset == 10

    def test_negative_stdout_offset_rejected(self) -> None:
        with pytest.raises(ValidationError):
            LogChunk(stdout="", stderr="", stdout_offset=-1, stderr_offset=0)

    def test_negative_stderr_offset_rejected(self) -> None:
        with pytest.raises(ValidationError):
            LogChunk(stdout="", stderr="", stdout_offset=0, stderr_offset=-1)


class TestJobOperations:
    def test_is_runtime_checkable(self) -> None:
        """Phase 1 only defines the protocol; no implementation yet."""
        # A bare object doesn't satisfy the protocol
        assert not isinstance(object(), JobOperations)


class TestExceptionHierarchy:
    def test_transport_subclasses(self) -> None:
        assert issubclass(TransportConnectionError, TransportError)
        assert issubclass(TransportAuthError, TransportError)
        assert issubclass(TransportTimeoutError, TransportError)
        assert issubclass(RemoteCommandError, TransportError)

    def test_job_not_found_independent(self) -> None:
        assert not issubclass(JobNotFoundError, TransportError)

    def test_submission_error_independent(self) -> None:
        assert not issubclass(SubmissionError, TransportError)
