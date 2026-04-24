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

        def fake_tail(
            job_id: int,
            out_off: int,
            err_off: int,
            *,
            last_n: int | None = None,
        ) -> LogChunk:
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

    def test_last_flag_forwarded_as_last_n_on_initial_call(
        self, runner: CliRunner
    ) -> None:
        """``--last N`` must reach the adapter as ``last_n=N`` on the
        *initial* poll so the remote runs ``tail -n N`` and only the
        tail ships over SSH. Subsequent ticks must NOT pass ``last_n``
        (the follow loop is already incrementing offsets).
        """
        seen_last_n: list[int | None] = []

        def fake_tail(
            job_id: int,
            out_off: int,
            err_off: int,
            *,
            last_n: int | None = None,
        ) -> LogChunk:
            seen_last_n.append(last_n)
            return _chunk("tail\n", "", out_off=5, err_off=0)

        job_ops = MagicMock()
        job_ops.tail_log_incremental.side_effect = fake_tail

        tick = {"n": 0}

        def fake_sleep(_seconds: float) -> None:
            tick["n"] += 1
            if tick["n"] >= 2:
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
                [
                    "tail",
                    "12345",
                    "--follow",
                    "--interval",
                    "0.01",
                    "--profile",
                    "dgx",
                    "--last",
                    "50",
                ],
            )

        assert result.exit_code == 0
        # Initial call gets last_n=50; every subsequent call gets None
        # so the adapter falls back to pure-offset delta reads.
        assert seen_last_n[0] == 50
        assert all(x is None for x in seen_last_n[1:])

    def test_non_follow_one_shot_forwards_last_n(self, runner: CliRunner) -> None:
        """``srunx tail 123 --last 50 --profile X`` (no --follow) must
        push ``last_n=50`` into the single SSH call so the remote
        ships only the tail."""
        job_ops = MagicMock()
        job_ops.tail_log_incremental.return_value = _chunk(
            "only tail lines\n", "", out_off=100, err_off=0
        )
        with patch(
            "srunx.transport.registry._build_ssh_handle",
            return_value=(self._fake_handle(job_ops), None),
        ):
            result = runner.invoke(
                app,
                ["tail", "12345", "--profile", "dgx", "--last", "50"],
            )
        assert result.exit_code == 0
        job_ops.tail_log_incremental.assert_called_once_with(12345, 0, 0, last_n=50)

    def test_default_last_is_ten_not_full_log(self, runner: CliRunner) -> None:
        """``srunx tail <id> --profile X`` with no ``--last`` and no
        ``--all`` must default to 10 lines — matches native ``tail``
        and prevents an accidental multi-GB SSH download.
        """
        job_ops = MagicMock()
        job_ops.tail_log_incremental.return_value = _chunk("", "", out_off=0, err_off=0)
        with patch(
            "srunx.transport.registry._build_ssh_handle",
            return_value=(self._fake_handle(job_ops), None),
        ):
            result = runner.invoke(app, ["tail", "12345", "--profile", "dgx"])
        assert result.exit_code == 0
        job_ops.tail_log_incremental.assert_called_once_with(12345, 0, 0, last_n=10)

    def test_all_flag_dumps_full_log(self, runner: CliRunner) -> None:
        """``--all`` overrides ``--last`` and passes ``last_n=None`` to
        the adapter, which falls through to the legacy ``cat`` path."""
        job_ops = MagicMock()
        job_ops.tail_log_incremental.return_value = _chunk(
            "everything\n", "", out_off=11, err_off=0
        )
        with patch(
            "srunx.transport.registry._build_ssh_handle",
            return_value=(self._fake_handle(job_ops), None),
        ):
            result = runner.invoke(app, ["tail", "12345", "--profile", "dgx", "--all"])
        assert result.exit_code == 0
        job_ops.tail_log_incremental.assert_called_once_with(12345, 0, 0, last_n=None)

    def test_zero_last_rejected_unless_all(self, runner: CliRunner) -> None:
        result = runner.invoke(app, ["tail", "12345", "--last", "0"])
        assert result.exit_code != 0
        combined = result.stdout + (result.stderr or "")
        assert "positive" in combined.lower() or "--all" in combined

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
