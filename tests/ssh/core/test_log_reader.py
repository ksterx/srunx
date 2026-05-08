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
