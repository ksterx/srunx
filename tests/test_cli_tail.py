"""Tests for ``srunx tail`` — both one-shot and ``--follow`` over SSH.

Local ``--follow`` still delegates to the existing ``Slurm.tail_log``
subprocess path (covered by ``tests/test_logs.py``); this file focuses
on the SSH polling loop introduced alongside ``squeue -i``.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from typer.testing import CliRunner

from srunx.cli.main import app
from srunx.slurm.protocols import LogChunk


@pytest.fixture
def runner() -> CliRunner:
    return CliRunner()


def _chunk(stdout: str, stderr: str, *, out_off: int, err_off: int) -> LogChunk:
    return LogChunk(
        stdout=stdout,
        stderr=stderr,
        stdout_offset=out_off,
        stderr_offset=err_off,
    )


class TestTailFollowValidation:
    def test_rejects_non_positive_interval(self, runner: CliRunner) -> None:
        result = runner.invoke(app, ["tail", "12345", "--follow", "--interval", "0"])
        assert result.exit_code != 0
        combined = result.stdout + (result.stderr or "")
        assert "positive" in combined.lower()


class TestTailFollowSsh:
    """``--follow`` over SSH must poll ``tail_log_incremental`` with the
    offset returned by the previous chunk and keep streaming until
    Ctrl+C — same structural pattern as ``squeue -i``.
    """

    def _fake_handle(self, job_ops: MagicMock):
        from srunx.transport.registry import TransportHandle

        return TransportHandle(
            scheduler_key="ssh:dgx",
            profile_name="dgx",
            transport_type="ssh",
            job_ops=job_ops,
            queue_client=job_ops,
            executor_factory=MagicMock(),
            submission_context=None,
        )

    def test_poll_loop_advances_offsets_and_prints_new_chunks(
        self, runner: CliRunner
    ) -> None:
        """Each tick's offset must flow into the next ``tail_log_incremental``
        call so we never re-read already-printed bytes.
        """
        chunks = [
            _chunk("first\n", "", out_off=6, err_off=0),
            _chunk("second\n", "warn\n", out_off=13, err_off=5),
            _chunk("", "", out_off=13, err_off=5),
        ]
        call_args: list[tuple[int, int]] = []

        def fake_tail(job_id: int, out_off: int, err_off: int) -> LogChunk:
            call_args.append((out_off, err_off))
            return (
                chunks.pop(0)
                if chunks
                else _chunk("", "", out_off=out_off, err_off=err_off)
            )

        job_ops = MagicMock()
        job_ops.tail_log_incremental.side_effect = fake_tail

        sleep_calls = {"n": 0}

        def fake_sleep(_seconds: float) -> None:
            sleep_calls["n"] += 1
            # Let the loop run through all three canned chunks then abort.
            if sleep_calls["n"] >= 3:
                raise KeyboardInterrupt

        with (
            patch(
                "srunx.transport.registry._build_ssh_handle",
                return_value=(self._fake_handle(job_ops), None),
            ),
            patch("time.sleep", fake_sleep),
        ):
            result = runner.invoke(
                app,
                ["tail", "12345", "--follow", "--interval", "0.01", "--profile", "dgx"],
            )

        assert result.exit_code == 0, result.stdout + (result.stderr or "")
        # First call at offset (0, 0); then (6, 0) from chunk 1; then (13, 5) from chunk 2.
        assert call_args[:3] == [(0, 0), (6, 0), (13, 5)]
        assert "first" in result.stdout
        assert "second" in result.stdout

    def test_transient_poll_failure_does_not_kill_loop(self, runner: CliRunner) -> None:
        """An exception from ``tail_log_incremental`` mid-loop must
        surface as a dim notice and the next tick must still run.
        Matches the error-tolerance contract of ``_run_squeue_live``.
        """
        first = _chunk("hello\n", "", out_off=6, err_off=0)
        recovered = _chunk("world\n", "", out_off=12, err_off=0)
        side_effects: list = [first, RuntimeError("ssh hiccup"), recovered]

        def fake_tail(*_a, **_kw) -> LogChunk:
            item = side_effects.pop(0)
            if isinstance(item, Exception):
                raise item
            return item

        job_ops = MagicMock()
        job_ops.tail_log_incremental.side_effect = fake_tail

        tick = {"n": 0}

        def fake_sleep(_seconds: float) -> None:
            tick["n"] += 1
            # Stop after we've seen the post-error recovery chunk.
            if tick["n"] >= 3:
                raise KeyboardInterrupt

        with (
            patch(
                "srunx.transport.registry._build_ssh_handle",
                return_value=(self._fake_handle(job_ops), None),
            ),
            patch("time.sleep", fake_sleep),
        ):
            result = runner.invoke(
                app,
                ["tail", "12345", "--follow", "--interval", "0.01", "--profile", "dgx"],
            )

        assert result.exit_code == 0
        # Both pre-error and post-error chunks reach stdout.
        assert "hello" in result.stdout
        assert "world" in result.stdout
