"""Tests for :meth:`SlurmSSHClient.run_slurm_command`."""

from __future__ import annotations

import shutil
import threading
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from srunx.slurm.clients.ssh import SlurmSSHClient
from tests.integration._local_shell import REAL_RUN


def _client(slurm_path: str | None = None) -> tuple[SlurmSSHClient, MagicMock]:
    client = object.__new__(SlurmSSHClient)
    client._io_lock = threading.RLock()
    inner = MagicMock()
    inner.slurm.slurm_path = slurm_path
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


def _bash_runner(client_exec: MagicMock, env_path: str):
    """Make the fake paramiko layer run commands through a real bash."""

    def run(cmd: str) -> tuple[str, str, int]:
        proc = REAL_RUN(
            ["/bin/bash", "-c", cmd],
            capture_output=True,
            text=True,
            env={"PATH": env_path},
            check=False,
        )
        return proc.stdout, proc.stderr, proc.returncode

    client_exec.side_effect = run


def test_known_slurm_dir_is_prepended_to_path_for_chained_commands() -> None:
    client, execute_mock = _client("/opt/slurm bin")
    client._ensure_connected = MagicMock()  # type: ignore[method-assign]
    execute_mock.return_value = ("", "", 0)
    client.run_slurm_command("echo a; squeue -h")
    (sent,) = execute_mock.call_args.args
    assert sent == "export PATH='/opt/slurm bin':\"$PATH\"; echo a; squeue -h"


def test_no_path_prefix_when_no_slurm_dir_is_known() -> None:
    client, execute_mock = _client(None)
    client._ensure_connected = MagicMock()  # type: ignore[method-assign]
    execute_mock.return_value = ("", "", 0)
    client.run_slurm_command("echo a; squeue -h")
    assert execute_mock.call_args.args == ("echo a; squeue -h",)


def test_squeue_reachable_only_through_the_discovered_dir_is_found(
    tmp_path: Path,
) -> None:
    slurm_bin = tmp_path / "slurm bin"
    slurm_bin.mkdir()
    shim = slurm_bin / "squeue"
    shim.write_text("#!/bin/sh\necho found-squeue\n")
    shim.chmod(0o755)
    base = tmp_path / "base"
    base.mkdir()
    (base / "echo").symlink_to(shutil.which("echo") or "/bin/echo")

    def run_with(slurm_path: str | None) -> tuple[str, str, int]:
        client, execute_mock = _client(slurm_path)
        client._ensure_connected = MagicMock()  # type: ignore[method-assign]
        _bash_runner(execute_mock, str(base))
        return client.run_slurm_command("echo start; squeue -h")

    out, _, rc = run_with(str(slurm_bin))
    assert rc == 0 and "found-squeue" in out
    _, _, rc = run_with(None)
    assert rc != 0  # without the prefix squeue is not on PATH
