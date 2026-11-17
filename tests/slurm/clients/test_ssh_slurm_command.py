"""Tests for :meth:`SlurmSSHClient.run_slurm_command`."""

from __future__ import annotations

import threading
from unittest.mock import MagicMock

import pytest

from srunx.slurm.clients.ssh import SlurmSSHClient


def _client() -> tuple[SlurmSSHClient, MagicMock]:
    client = object.__new__(SlurmSSHClient)
    client._io_lock = threading.RLock()
    inner = MagicMock()
    client._client = inner
    return client, inner.slurm.execute_slurm_command


def test_returns_stdout_stderr_and_exit_code() -> None:
    client, execute_mock = _client()
    execute_mock.return_value = ("out\n", "warn\n", 0)
    client._ensure_connected = MagicMock()  # type: ignore[method-assign]
    assert client.run_slurm_command("squeue -h") == ("out\n", "warn\n", 0)
    execute_mock.assert_called_once_with("squeue -h")


def test_non_zero_exit_does_not_raise() -> None:
    client, execute_mock = _client()
    execute_mock.return_value = ("", "boom", 3)
    client._ensure_connected = MagicMock()  # type: ignore[method-assign]
    assert client.run_slurm_command("false") == ("", "boom", 3)


def test_connects_first_and_holds_the_io_lock() -> None:
    client, execute_mock = _client()
    seen: list[str] = []

    def held() -> bool:
        # An RLock owned by this thread can be re-acquired; one owned by
        # another thread cannot, so probe from a second thread.
        result: list[bool] = []
        t = threading.Thread(
            target=lambda: result.append(client._io_lock.acquire(blocking=False))
        )
        t.start()
        t.join()
        if result[0]:
            client._io_lock.release()
        return not result[0]

    def ensure() -> None:
        seen.append(f"ensure:{held()}")

    def execute(cmd: str) -> tuple[str, str, int]:
        seen.append(f"execute:{held()}")
        return "", "", 0

    client._ensure_connected = ensure  # type: ignore[method-assign]
    execute_mock.side_effect = execute
    client.run_slurm_command("x")
    assert seen == ["ensure:True", "execute:True"]


def test_connection_error_propagates() -> None:
    client, execute_mock = _client()
    client._ensure_connected = MagicMock(side_effect=RuntimeError("down"))  # type: ignore[method-assign]
    with pytest.raises(RuntimeError, match="down"):
        client.run_slurm_command("x")
