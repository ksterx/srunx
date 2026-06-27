"""Tests for srunx.ssh.core.log_reader.RemoteLogReader."""

from unittest.mock import Mock

import pytest

from srunx.ssh.core.client import SSHSlurmClient


@pytest.fixture
def client():
    c = SSHSlurmClient(
        hostname="test.example.com",
        username="testuser",
        key_filename="/test/key",
    )
    c.connection.ssh_client = Mock()
    c.connection.sftp_client = Mock()
    c.connection.proxy_client = None
    return c


class TestGetJobOutput:
    def test_via_scontrol(self, client):
        """scontrol returns StdOut/StdErr → read those files directly."""
        scontrol_output = (
            "JobId=12345 JobName=train\n"
            "   StdOut=/home/user/logs/train_12345.log "
            "StdErr=/home/user/logs/train_12345.err\n"
        )

        def _exec(cmd):
            if "scontrol" in cmd:
                return (scontrol_output, "", 0)
            if "cat" in cmd and "train_12345.log" in cmd:
                return ("epoch 1 done\nepoch 2 done\n", "", 0)
            if "wc -c" in cmd and "train_12345.log" in cmd:
                return ("28\n", "", 0)
            if "cat" in cmd and "train_12345.err" in cmd:
                return ("WARNING: low memory\n", "", 0)
            if "wc -c" in cmd and "train_12345.err" in cmd:
                return ("20\n", "", 0)
            return ("", "", 1)

        client.connection.execute_command = Mock(side_effect=_exec)

        stdout, stderr, out_off, err_off = client.logs.get_job_output("12345")

        assert "epoch 1 done" in stdout
        assert "WARNING: low memory" in stderr
        assert out_off == 28
        assert err_off == 20

    def test_scontrol_same_path(self, client):
        """When StdOut == StdErr (combined log), stderr should be empty."""
        scontrol_output = "StdOut=/logs/job_12345.log StdErr=/logs/job_12345.log\n"

        def _exec(cmd):
            if "scontrol" in cmd:
                return (scontrol_output, "", 0)
            if "cat" in cmd and "job_12345.log" in cmd:
                return ("all output here\n", "", 0)
            if "wc -c" in cmd and "job_12345.log" in cmd:
                return ("16\n", "", 0)
            return ("", "", 1)

        client.connection.execute_command = Mock(side_effect=_exec)

        stdout, stderr, out_off, _ = client.logs.get_job_output("12345")

        assert "all output here" in stdout
        assert stderr == ""
        assert out_off == 16

    def test_with_offset(self, client):
        """Non-zero offset uses tail -c for incremental reads."""
        scontrol_output = "StdOut=/logs/out.log StdErr=/logs/err.log\n"

        def _exec(cmd):
            if "scontrol" in cmd:
                return (scontrol_output, "", 0)
            if "tail -c +101" in cmd and "out.log" in cmd:
                return ("new stdout\n", "", 0)
            if "wc -c" in cmd and "out.log" in cmd:
                return ("111\n", "", 0)
            if "tail -c +51" in cmd and "err.log" in cmd:
                return ("new stderr\n", "", 0)
            if "wc -c" in cmd and "err.log" in cmd:
                return ("61\n", "", 0)
            return ("", "", 1)

        client.connection.execute_command = Mock(side_effect=_exec)

        stdout, stderr, out_off, err_off = client.logs.get_job_output(
            "12345", stdout_offset=100, stderr_offset=50
        )

        assert stdout == "new stdout\n"
        assert stderr == "new stderr\n"
        assert out_off == 111
        assert err_off == 61

    def test_scontrol_fails_falls_back_to_pattern(self, client):
        """scontrol fails → fall back to pattern-based file search."""

        def _exec(cmd):
            if cmd.startswith("scontrol"):
                return ("", "error", 1)
            if "find" in cmd and "slurm-12345.out" in cmd:
                return ("/tmp/slurm-12345.out\n", "", 0)
            if "cat" in cmd and "/tmp/slurm-12345.out" in cmd:
                return ("fallback output\n", "", 0)
            return ("", "", 1)

        client.connection.execute_command = Mock(side_effect=_exec)

        stdout, stderr, out_off, _ = client.logs.get_job_output("12345")

        assert "fallback output" in stdout
        assert out_off == len(b"fallback output\n")
        # scontrol was attempted first.
        first_call = client.connection.execute_command.call_args_list[0]
        assert "scontrol" in first_call[0][0]

    def test_exception_returns_empty(self, client):
        """Any unexpected exception returns empty strings and preserves offsets."""
        client.connection.execute_command = Mock(
            side_effect=RuntimeError("connection lost")
        )

        stdout, stderr, out_off, err_off = client.logs.get_job_output(
            "12345", stdout_offset=50, stderr_offset=30
        )

        assert stdout == ""
        assert stderr == ""
        assert out_off == 50
        assert err_off == 30


class TestGetLogPathsFromScontrol:
    def test_parses_stdout_stderr(self, client):
        scontrol_output = (
            "JobId=99 JobName=test UserId=user(1000)\n"
            "   StdOut=/data/logs/test_99.out StdErr=/data/logs/test_99.err\n"
            "   WorkDir=/home/user\n"
        )
        client.connection.execute_command = Mock(return_value=(scontrol_output, "", 0))

        stdout_path, stderr_path = client.logs._get_log_paths_from_scontrol("99")

        assert stdout_path == "/data/logs/test_99.out"
        assert stderr_path == "/data/logs/test_99.err"

    def test_not_found_returns_none(self, client):
        client.connection.execute_command = Mock(return_value=("", "", 1))

        stdout_path, stderr_path = client.logs._get_log_paths_from_scontrol("99999")

        assert stdout_path is None
        assert stderr_path is None


class TestScontrolFoundButFileEmpty:
    """scontrol gave us a real StdOut path — trust it even if the file is empty.

    Regression guard: the previous flow fell through to pattern-based
    file search whenever the scontrol-reported StdOut/StdErr files
    happened to be empty (very common in the first seconds of a job),
    which logged a misleading ``No log files found ... using common
    patterns`` warning even though SLURM itself reported the exact log
    path.
    """

    def test_empty_stdout_no_pattern_fallback_no_warning(self, client, caplog):
        scontrol_output = "StdOut=/logs/job.log StdErr=/logs/job.err\n"
        calls: list[str] = []

        def _exec(cmd: str):
            calls.append(cmd)
            if "scontrol" in cmd:
                return (scontrol_output, "", 0)
            # tail / cat on the StdOut/StdErr path → empty file
            if "job.log" in cmd or "job.err" in cmd:
                if "wc -c" in cmd:
                    return ("0\n", "", 0)
                return ("", "", 0)
            return ("", "", 1)

        client.connection.execute_command = Mock(side_effect=_exec)

        with caplog.at_level("WARNING"):
            stdout, stderr, out_off, err_off = client.logs.get_job_output("12345")

        assert stdout == ""
        assert stderr == ""
        assert out_off == 0
        assert err_off == 0
        # No fallback pattern search: zero ``find`` shellouts.
        assert not any("find " in c for c in calls)
        # And no misleading "No log files found" warning.
        assert not any("No log files found" in rec.message for rec in caplog.records)

    def test_only_stdout_path_stderr_remains_empty(self, client):
        """scontrol returned StdOut only (no StdErr line) → stderr=""."""
        scontrol_output = "StdOut=/logs/out.log\n"

        def _exec(cmd: str):
            if "scontrol" in cmd:
                return (scontrol_output, "", 0)
            if "out.log" in cmd:
                if "wc -c" in cmd:
                    return ("12\n", "", 0)
                return ("hello world\n", "", 0)
            return ("", "", 1)

        client.connection.execute_command = Mock(side_effect=_exec)

        stdout, stderr, out_off, err_off = client.logs.get_job_output("12345")

        assert "hello world" in stdout
        assert stderr == ""
        assert out_off == 12
        # stderr_offset stays at the input (0) when no path is known.
        assert err_off == 0


class TestSqueueFallback:
    """scontrol unavailable on the cluster → fall back to ``squeue -O``.

    Some sites (e.g. multi-tenant managed SLURM) lock down scontrol but
    leave squeue open. The fallback chain must surface log paths via
    squeue before resorting to pattern-based file search and emitting
    the misleading "No log files found" warning.
    """

    def test_scontrol_fails_squeue_returns_path(self, client, caplog):
        """scontrol fails → squeue path used → no pattern fallback, no warning."""
        calls: list[str] = []

        def _exec(cmd: str):
            calls.append(cmd)
            if "scontrol" in cmd:
                return ("", "permission denied", 1)
            if "squeue" in cmd and "StdOut" in cmd:
                # squeue -O pads to the requested width; mimic real output.
                return ("/data/logs/run.out" + " " * 2030 + "\n", "", 0)
            if "squeue" in cmd and "StdErr" in cmd:
                return ("/data/logs/run.err" + " " * 2030 + "\n", "", 0)
            if "/data/logs/run.out" in cmd:
                if "wc -c" in cmd:
                    return ("20\n", "", 0)
                return ("epoch 1 done\n", "", 0)
            if "/data/logs/run.err" in cmd:
                if "wc -c" in cmd:
                    return ("5\n", "", 0)
                return ("WARN\n", "", 0)
            return ("", "", 1)

        client.connection.execute_command = Mock(side_effect=_exec)

        with caplog.at_level("WARNING"):
            stdout, stderr, out_off, err_off = client.logs.get_job_output("99")

        assert "epoch 1 done" in stdout
        assert "WARN" in stderr
        assert out_off == 20
        assert err_off == 5
        # No fallback pattern search means no `find` shellouts.
        assert not any("find " in c for c in calls)
        assert not any("No log files found" in rec.message for rec in caplog.records)

    def test_stderr_only_path_is_read_without_pattern_fallback(self, client, caplog):
        """``StdOut=(null)`` but ``StdErr=<path>`` → read stderr, no warning.

        Regression guard: earlier the read block was gated on
        ``stdout_path`` alone, so a configured stderr path with no
        stdout (job sends useful output only to stderr, or
        ``StdOut=/dev/null``) was silently dropped on the floor and
        ``srunx tail`` fell through to pattern search.
        """
        calls: list[str] = []

        def _exec(cmd: str):
            calls.append(cmd)
            if "scontrol" in cmd:
                return ("", "denied", 1)
            if "squeue" in cmd and "StdOut" in cmd:
                return ("(null)" + " " * 2042 + "\n", "", 0)
            if "squeue" in cmd and "StdErr" in cmd:
                return ("/data/logs/run.err" + " " * 2030 + "\n", "", 0)
            if "/data/logs/run.err" in cmd:
                if "wc -c" in cmd:
                    return ("9\n", "", 0)
                return ("stderr ok\n", "", 0)
            return ("", "", 1)

        client.connection.execute_command = Mock(side_effect=_exec)

        with caplog.at_level("WARNING"):
            stdout, stderr, out_off, err_off = client.logs.get_job_output("77")

        assert stdout == ""
        assert "stderr ok" in stderr
        assert err_off == 9
        # SLURM-reported path wins → no pattern fallback, no warning.
        assert not any("find " in c for c in calls)
        assert not any("No log files found" in rec.message for rec in caplog.records)

    def test_squeue_null_values_trigger_pattern_fallback(self, client):
        """Both ``StdOut`` and ``StdErr`` unset → fall through to pattern search."""

        def _exec(cmd: str):
            if "scontrol" in cmd:
                return ("", "denied", 1)
            if "squeue" in cmd and "StdOut" in cmd:
                return ("(null)" + " " * 2042 + "\n", "", 0)
            if "squeue" in cmd and "StdErr" in cmd:
                return ("N/A" + " " * 2045 + "\n", "", 0)
            if "find" in cmd and "slurm-99.out" in cmd:
                return ("/tmp/slurm-99.out\n", "", 0)
            if "cat" in cmd and "/tmp/slurm-99.out" in cmd:
                return ("pattern hit\n", "", 0)
            return ("", "", 1)

        client.connection.execute_command = Mock(side_effect=_exec)

        stdout, _, _, _ = client.logs.get_job_output("99")

        assert "pattern hit" in stdout

    def test_squeue_path_stripped_of_padding(self, client):
        """The padded squeue output must be trimmed before use."""
        captured: list[str] = []

        def _exec(cmd: str):
            captured.append(cmd)
            if "scontrol" in cmd:
                return ("", "denied", 1)
            if "squeue" in cmd and "StdOut" in cmd:
                # Real-world width padding: trailing spaces only.
                return ("/x/y/z.log" + " " * 2038 + "\n", "", 0)
            if "squeue" in cmd and "StdErr" in cmd:
                return ("(null)" + " " * 2042 + "\n", "", 0)
            if cmd.startswith("tail -n ") and "/x/y/z.log" in cmd:
                return ("ok\n", "", 0)
            if "wc -c" in cmd and "/x/y/z.log" in cmd:
                return ("3\n", "", 0)
            return ("", "", 1)

        client.connection.execute_command = Mock(side_effect=_exec)
        stdout, _, out_off, _ = client.logs.get_job_output("42", last_n=10)
        assert stdout == "ok\n"
        assert out_off == 3
        # The path used in the tail command must be the stripped form,
        # never the padded one (would shell out to a non-existent file).
        tail_calls = [c for c in captured if c.startswith("tail -n ")]
        assert tail_calls, "expected a tail -n call"
        # shlex.quote keeps safe paths unquoted, so a stripped path
        # appears verbatim followed by ` 2>/dev/null`.
        assert any("tail -n 10 /x/y/z.log 2>/dev/null" == c for c in tail_calls)
        # And no tail call carries the padding bytes (would look like
        # ``tail -n 10 /x/y/z.log<lots of spaces>`` if strip was missed).
        for c in tail_calls:
            assert "/x/y/z.log  " not in c
