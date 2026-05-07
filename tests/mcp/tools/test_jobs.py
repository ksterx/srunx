"""Tests for srunx.mcp.tools.jobs."""

from unittest.mock import MagicMock, patch

from srunx.mcp.tools.jobs import (
    cancel_job,
    get_job_logs,
    get_job_status,
    list_jobs,
    submit_job,
)


class TestSubmitJob:
    """Test submit_job tool."""

    @patch("srunx.slurm.local.Slurm")
    def test_submit_local_success(self, mock_slurm_cls):
        mock_slurm = MagicMock()
        mock_job = MagicMock()
        mock_job.job_id = "12345"
        mock_job.name = "test_job"
        mock_job._status.value = "PENDING"
        mock_slurm.submit.return_value = mock_job
        mock_slurm_cls.return_value = mock_slurm

        result = submit_job(command="python train.py", name="test_job")
        assert result["success"] is True
        assert result["job_id"] == "12345"
        assert result["name"] == "test_job"

    def test_submit_ssh_requires_work_dir(self):
        result = submit_job(command="python train.py", use_ssh=True)
        assert result["success"] is False
        assert "work_dir is required" in result["error"]

    @patch("srunx.mcp.tools.jobs.get_ssh_client")
    @patch(
        "jinja2.Template",
        return_value=MagicMock(render=MagicMock(return_value="#!/bin/bash\necho hi")),
    )
    def test_submit_ssh_success(self, _mock_tpl, mock_get_client):
        mock_client = MagicMock()
        mock_returned_job = MagicMock()
        mock_returned_job.job_id = "99999"
        mock_returned_job.name = "ssh_job"
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client.slurm.submit_sbatch_job.return_value = mock_returned_job
        mock_get_client.return_value = mock_client

        result = submit_job(
            command="python train.py",
            name="ssh_job",
            use_ssh=True,
            work_dir="/remote/workdir",
        )
        assert result["success"] is True
        assert result["job_id"] == "99999"

    @patch("srunx.mcp.tools.jobs.get_ssh_client")
    @patch(
        "jinja2.Template",
        return_value=MagicMock(render=MagicMock(return_value="#!/bin/bash")),
    )
    def test_submit_ssh_returns_none(self, _mock_tpl, mock_get_client):
        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client.slurm.submit_sbatch_job.return_value = None
        mock_get_client.return_value = mock_client

        result = submit_job(
            command="python train.py",
            use_ssh=True,
            work_dir="/remote/workdir",
        )
        assert result["success"] is False
        assert "SSH job submission failed" in result["error"]

    def test_submit_catches_exception(self):
        with patch(
            "srunx.slurm.local.Slurm", side_effect=RuntimeError("slurm not available")
        ):
            result = submit_job(command="echo hi")
            assert result["success"] is False
            assert "slurm not available" in result["error"]

    @patch("srunx.mcp.tools.jobs.get_ssh_client")
    def test_submit_ssh_real_template_render(self, mock_get_client):
        """Regression test for #117: real template render must succeed."""
        mock_client = MagicMock()
        mock_returned_job = MagicMock()
        mock_returned_job.job_id = "42"
        mock_returned_job.name = "ssh_job"
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client.slurm.submit_sbatch_job.return_value = mock_returned_job
        mock_get_client.return_value = mock_client

        result = submit_job(
            command="echo hi",
            name="ssh_job",
            use_ssh=True,
            work_dir="/remote/workdir",
        )
        assert result["success"] is True, result
        script_content = mock_client.slurm.submit_sbatch_job.call_args[0][0]
        assert "#SBATCH --job-name=ssh_job" in script_content
        assert "SRUNX_OUTPUTS_DIR" not in script_content


class TestListJobs:
    """Test list_jobs tool."""

    @patch("srunx.slurm.local.Slurm")
    def test_list_local(self, mock_slurm_cls):
        mock_slurm = MagicMock()
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

        mock_slurm.queue.return_value = [job1]
        mock_slurm_cls.return_value = mock_slurm

        result = list_jobs(use_ssh=False)
        assert result["success"] is True
        assert result["count"] == 1
        assert result["jobs"][0]["name"] == "job1"

    @patch("srunx.mcp.tools.jobs.get_ssh_client")
    def test_list_ssh(self, mock_get_client):
        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client.slurm.execute_slurm_command.return_value = (
            "12345     gpu       train         user1  RUNNING   0:05:00  1:00:00      1 node001 gpu:1\n",
            "",
            0,
        )
        mock_get_client.return_value = mock_client

        result = list_jobs(use_ssh=True)
        assert result["success"] is True
        assert result["count"] == 1
        assert result["jobs"][0]["job_id"] == "12345"
        assert result["jobs"][0]["status"] == "RUNNING"

    @patch("srunx.mcp.tools.jobs.get_ssh_client")
    def test_list_ssh_squeue_fails(self, mock_get_client):
        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client.slurm.execute_slurm_command.return_value = ("", "error msg", 1)
        mock_get_client.return_value = mock_client

        result = list_jobs(use_ssh=True)
        assert result["success"] is False
        assert "squeue failed" in result["error"]

    def test_list_catches_exception(self):
        with patch("srunx.slurm.local.Slurm", side_effect=RuntimeError("no slurm")):
            result = list_jobs()
            assert result["success"] is False


class TestGetJobStatus:
    """Test get_job_status tool."""

    def test_invalid_job_id(self):
        result = get_job_status(job_id="abc")
        assert result["success"] is False
        assert "Invalid job ID" in result["error"]

    @patch("srunx.slurm.local.Slurm")
    def test_local_calls_retrieve(self, mock_slurm_cls):
        """Local get_job_status calls Slurm.retrieve with int job_id."""
        mock_slurm_cls.retrieve.side_effect = ValueError("test")
        result = get_job_status(job_id="12345")
        mock_slurm_cls.retrieve.assert_called_once_with(12345)
        assert result["success"] is False

    @patch("srunx.mcp.tools.jobs.get_ssh_client")
    def test_ssh_success(self, mock_get_client):
        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client.slurm.get_job_status.return_value = "COMPLETED"
        mock_get_client.return_value = mock_client

        result = get_job_status(job_id="12345", use_ssh=True)
        assert result["success"] is True
        assert result["status"] == "COMPLETED"

    @patch("srunx.mcp.tools.jobs.get_ssh_client")
    def test_ssh_not_found(self, mock_get_client):
        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client.slurm.get_job_status.return_value = "NOT_FOUND"
        mock_get_client.return_value = mock_client

        result = get_job_status(job_id="99999", use_ssh=True)
        assert result["success"] is False
        assert "NOT_FOUND" in result["error"]

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

    @patch("srunx.slurm.local.Slurm")
    def test_local_cancel(self, mock_slurm_cls):
        mock_slurm = MagicMock()
        mock_slurm_cls.return_value = mock_slurm

        result = cancel_job(job_id="12345")
        assert result["success"] is True
        assert result["message"] == "Job cancelled"
        mock_slurm.cancel.assert_called_once_with(12345)

    @patch("srunx.mcp.tools.jobs.get_ssh_client")
    def test_ssh_cancel(self, mock_get_client):
        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client.slurm.execute_slurm_command.return_value = ("", "", 0)
        mock_get_client.return_value = mock_client

        result = cancel_job(job_id="12345", use_ssh=True)
        assert result["success"] is True
        assert result["message"] == "Job cancelled"

    @patch("srunx.mcp.tools.jobs.get_ssh_client")
    def test_ssh_cancel_fails(self, mock_get_client):
        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client.slurm.execute_slurm_command.return_value = (
            "",
            "permission denied",
            1,
        )
        mock_get_client.return_value = mock_client

        result = cancel_job(job_id="12345", use_ssh=True)
        assert result["success"] is False
        assert "scancel failed" in result["error"]


class TestGetJobLogs:
    """Test get_job_logs tool."""

    def test_invalid_job_id(self):
        result = get_job_logs(job_id="bad_id")
        assert result["success"] is False
        assert "Invalid job ID" in result["error"]

    @patch("srunx.slurm.local.Slurm")
    def test_local_logs(self, mock_slurm_cls):
        mock_slurm = MagicMock()
        mock_slurm.get_job_output_detailed.return_value = {
            "output": "training started\n",
            "error": "",
            "found_files": ["logs/job-12345.out"],
        }
        mock_slurm_cls.return_value = mock_slurm

        result = get_job_logs(job_id="12345")
        assert result["success"] is True
        assert result["stdout"] == "training started\n"
        assert result["log_files"] == ["logs/job-12345.out"]

    @patch("srunx.mcp.tools.jobs.get_ssh_client")
    def test_ssh_logs(self, mock_get_client):
        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client.logs.get_job_output.return_value = (
            "stdout content",
            "stderr content",
            100,
            50,
        )
        mock_get_client.return_value = mock_client

        result = get_job_logs(job_id="12345", use_ssh=True)
        assert result["success"] is True
        assert result["stdout"] == "stdout content"
        assert result["stderr"] == "stderr content"

    @patch("srunx.mcp.tools.jobs.get_ssh_client")
    def test_ssh_logs_no_output(self, mock_get_client):
        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client.logs.get_job_output.return_value = ("", "", 0, 0)
        mock_get_client.return_value = mock_client

        result = get_job_logs(job_id="12345", use_ssh=True)
        assert result["success"] is False
        assert "No logs found" in result["error"]
