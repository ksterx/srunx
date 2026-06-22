"""Tests for srunx.mcp.tools.jobs.

Every cluster-acting tool routes through ``srunx.mcp.transport.mcp_transport``
(see :mod:`srunx.mcp.transport`). Rather than reproducing the
``resolve_transport`` ladder, these tests patch ``mcp_transport`` at the tool
module's lookup site with a contextmanager that yields a controllable ``rt``
(a stand-in :class:`~srunx.transport.ResolvedTransport`). Local vs SSH is just
``rt.transport_type``; both share the single ``rt.job_ops`` path.
"""

from contextlib import contextmanager
from unittest.mock import MagicMock, patch

from srunx.mcp.tools.jobs import (
    cancel_job,
    get_job_logs,
    get_job_status,
    list_jobs,
    submit_job,
)


def _fake_transport(rt):
    """Build a patch target for ``mcp_transport`` yielding *rt*."""

    @contextmanager
    def _cm(transport, *, mount_name=None):
        yield rt

    return _cm


def _make_rt(transport_type="local"):
    """A minimal ResolvedTransport double.

    ``job_ops`` is a MagicMock exposing submit / queue / status / cancel /
    get_job_output_detailed; ``submission_context`` defaults to None (local).
    """
    rt = MagicMock()
    rt.transport_type = transport_type
    rt.submission_context = None
    return rt


class TestSubmitJob:
    """Test submit_job tool."""

    def test_submit_local_success(self):
        rt = _make_rt("local")
        result_job = MagicMock()
        result_job.job_id = "12345"
        result_job.name = "test_job"
        result_job._status.value = "PENDING"
        del result_job.script_path
        result_job.command = "python train.py"
        for attr in (
            "partition",
            "user",
            "elapsed_time",
            "nodes",
            "nodelist",
            "cpus",
            "gpus",
        ):
            setattr(result_job, attr, None)
        rt.job_ops.submit.return_value = result_job

        with patch(
            "srunx.mcp.tools.jobs.mcp_transport", _fake_transport(rt)
        ):
            result = submit_job(command="python train.py", name="test_job")

        assert result["success"] is True
        assert result["job_id"] == "12345"
        assert result["name"] == "test_job"
        assert result["status"] == "PENDING"
        # submit_job forwards the resolved submission context.
        rt.job_ops.submit.assert_called_once()
        assert (
            rt.job_ops.submit.call_args.kwargs["submission_context"]
            is rt.submission_context
        )

    def test_submit_ssh_requires_work_dir(self):
        rt = _make_rt("ssh")
        with patch(
            "srunx.mcp.tools.jobs.mcp_transport", _fake_transport(rt)
        ):
            result = submit_job(command="python train.py", transport="prod")
        assert result["success"] is False
        assert "work_dir is required" in result["error"]
        rt.job_ops.submit.assert_not_called()

    def test_submit_ssh_success(self):
        rt = _make_rt("ssh")
        result_job = MagicMock()
        result_job.job_id = "99999"
        result_job.name = "ssh_job"
        result_job._status.value = "PENDING"
        del result_job.script_path
        result_job.command = "python train.py"
        for attr in (
            "partition",
            "user",
            "elapsed_time",
            "nodes",
            "nodelist",
            "cpus",
            "gpus",
        ):
            setattr(result_job, attr, None)
        rt.job_ops.submit.return_value = result_job

        with patch(
            "srunx.mcp.tools.jobs.mcp_transport", _fake_transport(rt)
        ):
            result = submit_job(
                command="python train.py",
                name="ssh_job",
                transport="prod",
                work_dir="/remote/workdir",
            )
        assert result["success"] is True
        assert result["job_id"] == "99999"

    def test_submit_catches_exception(self):
        rt = _make_rt("local")
        rt.job_ops.submit.side_effect = RuntimeError("slurm not available")
        with patch(
            "srunx.mcp.tools.jobs.mcp_transport", _fake_transport(rt)
        ):
            result = submit_job(command="echo hi")
        assert result["success"] is False
        assert "slurm not available" in result["error"]


class TestListJobs:
    """Test list_jobs tool."""

    def test_list_local(self):
        rt = _make_rt("local")
        job1 = MagicMock()
        job1.name = "job1"
        job1.job_id = "1"
        job1._status.value = "RUNNING"
        job1.command = "echo"
        job1.partition = None
        job1.user = None
        job1.elapsed_time = None
        job1.nodes = None
        job1.nodelist = None
        job1.cpus = None
        job1.gpus = None
        del job1.script_path
        rt.job_ops.queue.return_value = [job1]

        with patch(
            "srunx.mcp.tools.jobs.mcp_transport", _fake_transport(rt)
        ):
            result = list_jobs()
        assert result["success"] is True
        assert result["count"] == 1
        assert result["jobs"][0]["name"] == "job1"

    def test_list_ssh(self):
        rt = _make_rt("ssh")
        job1 = MagicMock()
        job1.name = "train"
        job1.job_id = "12345"
        job1._status.value = "RUNNING"
        job1.command = "echo"
        job1.partition = "gpu"
        job1.user = "user1"
        job1.elapsed_time = "0:05:00"
        job1.nodes = 1
        job1.nodelist = "node001"
        job1.cpus = None
        job1.gpus = 1
        del job1.script_path
        rt.job_ops.queue.return_value = [job1]

        with patch(
            "srunx.mcp.tools.jobs.mcp_transport", _fake_transport(rt)
        ):
            result = list_jobs(transport="prod")
        assert result["success"] is True
        assert result["count"] == 1
        assert result["jobs"][0]["job_id"] == "12345"
        assert result["jobs"][0]["status"] == "RUNNING"

    def test_list_catches_exception(self):
        rt = _make_rt("local")
        rt.job_ops.queue.side_effect = RuntimeError("no slurm")
        with patch(
            "srunx.mcp.tools.jobs.mcp_transport", _fake_transport(rt)
        ):
            result = list_jobs()
        assert result["success"] is False


class TestGetJobStatus:
    """Test get_job_status tool."""

    def test_invalid_job_id(self):
        result = get_job_status(job_id="abc")
        assert result["success"] is False
        assert "Invalid job ID" in result["error"]

    def test_local_calls_status(self):
        """Local get_job_status calls job_ops.status with int job_id."""
        rt = _make_rt("local")
        rt.job_ops.status.side_effect = ValueError("test")
        with patch(
            "srunx.mcp.tools.jobs.mcp_transport", _fake_transport(rt)
        ):
            result = get_job_status(job_id="12345")
        rt.job_ops.status.assert_called_once_with(12345)
        assert result["success"] is False

    def test_ssh_success(self):
        rt = _make_rt("ssh")
        job = MagicMock()
        job.name = "j"
        job.job_id = "12345"
        job._status.value = "COMPLETED"
        del job.script_path
        del job.command
        for attr in (
            "partition",
            "user",
            "elapsed_time",
            "nodes",
            "nodelist",
            "cpus",
            "gpus",
        ):
            setattr(job, attr, None)
        rt.job_ops.status.return_value = job

        with patch(
            "srunx.mcp.tools.jobs.mcp_transport", _fake_transport(rt)
        ):
            result = get_job_status(job_id="12345", transport="prod")
        assert result["success"] is True
        assert result["status"] == "COMPLETED"
        rt.job_ops.status.assert_called_once_with(12345)

    def test_injection_attempt(self):
        result = get_job_status(job_id="123; rm -rf /")
        assert result["success"] is False
        assert "Invalid job ID" in result["error"]


class TestCancelJob:
    """Test cancel_job tool."""

    def test_invalid_job_id(self):
        result = cancel_job(job_id="not_a_number")
        assert result["success"] is False
        assert "Invalid job ID" in result["error"]

    def test_local_cancel(self):
        rt = _make_rt("local")
        with patch(
            "srunx.mcp.tools.jobs.mcp_transport", _fake_transport(rt)
        ):
            result = cancel_job(job_id="12345")
        assert result["success"] is True
        assert result["message"] == "Job cancelled"
        rt.job_ops.cancel.assert_called_once_with(12345)

    def test_ssh_cancel(self):
        rt = _make_rt("ssh")
        with patch(
            "srunx.mcp.tools.jobs.mcp_transport", _fake_transport(rt)
        ):
            result = cancel_job(job_id="12345", transport="prod")
        assert result["success"] is True
        assert result["message"] == "Job cancelled"
        rt.job_ops.cancel.assert_called_once_with(12345)

    def test_cancel_fails(self):
        rt = _make_rt("ssh")
        rt.job_ops.cancel.side_effect = RuntimeError("permission denied")
        with patch(
            "srunx.mcp.tools.jobs.mcp_transport", _fake_transport(rt)
        ):
            result = cancel_job(job_id="12345", transport="prod")
        assert result["success"] is False
        assert "permission denied" in result["error"]


class TestGetJobLogs:
    """Test get_job_logs tool."""

    def test_invalid_job_id(self):
        result = get_job_logs(job_id="bad_id")
        assert result["success"] is False
        assert "Invalid job ID" in result["error"]

    def test_local_logs(self):
        rt = _make_rt("local")
        rt.job_ops.get_job_output_detailed.return_value = {
            "output": "training started\n",
            "error": "",
            "found_files": ["logs/job-12345.out"],
        }
        with patch(
            "srunx.mcp.tools.jobs.mcp_transport", _fake_transport(rt)
        ):
            result = get_job_logs(job_id="12345")
        assert result["success"] is True
        assert result["stdout"] == "training started\n"
        assert result["log_files"] == ["logs/job-12345.out"]

    def test_ssh_logs(self):
        rt = _make_rt("ssh")
        rt.job_ops.get_job_output_detailed.return_value = {
            "output": "stdout content",
            "error": "stderr content",
            "found_files": ["a.out", "a.err"],
        }
        with patch(
            "srunx.mcp.tools.jobs.mcp_transport", _fake_transport(rt)
        ):
            result = get_job_logs(job_id="12345", transport="prod")
        assert result["success"] is True
        assert result["stdout"] == "stdout content"
        assert result["stderr"] == "stderr content"
        assert result["log_files"] == ["a.out", "a.err"]

    def test_logs_empty(self):
        rt = _make_rt("local")
        rt.job_ops.get_job_output_detailed.return_value = {
            "output": "",
            "error": "",
            "found_files": [],
        }
        with patch(
            "srunx.mcp.tools.jobs.mcp_transport", _fake_transport(rt)
        ):
            result = get_job_logs(job_id="12345")
        assert result["success"] is True
        assert result["stdout"] == ""
        assert result["log_files"] == []
