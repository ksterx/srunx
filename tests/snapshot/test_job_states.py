"""Tests for srunx.snapshot.job_states."""

from __future__ import annotations

import re

import pytest

from srunx.snapshot.job_states import (
    build_cluster_command,
    query_job_cluster,
)


def _nonce(cmd: str) -> str:
    m = re.search(r"__SRUNX_([0-9a-f]+)_CLUSTER__", cmd)
    assert m
    return m.group(1)


class FakeExecute:
    """Answers the cluster command with *rows* and exit code *rc*."""

    def __init__(
        self, rows: str = "", rc: int = 0, *, code: int = 0, noise: str = ""
    ) -> None:
        self.rows, self.rc, self.code, self.noise = rows, rc, code, noise
        self.commands: list[str] = []

    def __call__(self, cmd: str) -> tuple[str, str, int]:
        self.commands.append(cmd)
        n = _nonce(cmd)
        out = f"{self.noise}__SRUNX_{n}_CLUSTER__\n{self.rows}\nRC {self.rc}\n"
        return out, "", self.code


def test_cluster_name_is_returned() -> None:
    ex = FakeExecute("gpucloud2\n")
    assert query_job_cluster(5, ex) == "gpucloud2"
    assert len(ex.commands) == 1


def test_cluster_login_noise_is_ignored() -> None:
    assert query_job_cluster(5, FakeExecute("c1\n", noise="Welcome!\n")) == "c1"


def test_cluster_empty_output_is_none() -> None:
    assert query_job_cluster(5, FakeExecute("")) is None


def test_cluster_mixed_names_is_none() -> None:
    assert query_job_cluster(5, FakeExecute("a\nb\n")) is None


def test_cluster_repeated_same_name_is_accepted() -> None:
    assert query_job_cluster(5, FakeExecute("a\na\n")) == "a"


def test_cluster_invalid_name_is_none() -> None:
    assert query_job_cluster(5, FakeExecute("bad name;rm\n")) is None


def test_cluster_timeout_rc_124_is_none() -> None:
    assert query_job_cluster(5, FakeExecute("gpucloud2\n", rc=124)) is None


def test_cluster_nonzero_exit_is_none() -> None:
    assert query_job_cluster(5, FakeExecute("gpucloud2\n", code=1)) is None


def test_cluster_missing_marker_is_none() -> None:
    assert query_job_cluster(5, lambda cmd: ("gpucloud2\nRC 0\n", "", 0)) is None


def test_cluster_missing_rc_line_is_none() -> None:
    def ex(cmd: str) -> tuple[str, str, int]:
        return f"__SRUNX_{_nonce(cmd)}_CLUSTER__\ngpucloud2\n", "", 0

    assert query_job_cluster(5, ex) is None


def test_cluster_execute_raising_is_none() -> None:
    def ex(cmd: str) -> tuple[str, str, int]:
        raise RuntimeError("ssh down")

    assert query_job_cluster(5, ex) is None


def test_cluster_without_runner_is_none() -> None:
    assert query_job_cluster(5, None) is None


def test_cluster_command_is_one_bounded_attempt() -> None:
    cmd = build_cluster_command(42, "ab" * 8)
    assert "command -v timeout" in cmd
    assert "timeout 10 sacct" in cmd
    assert cmd.count("sacct") == 2  # one per branch of the if
    assert "-j 42" in cmd and "-o Cluster" in cmd
    assert "sleep" not in cmd
    assert "while" not in cmd and "until" not in cmd


def test_cluster_command_rejects_bad_nonce() -> None:
    with pytest.raises(ValueError):
        build_cluster_command(1, "x; rm -rf /")
