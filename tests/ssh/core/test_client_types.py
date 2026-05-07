"""Tests for srunx.ssh.core.client_types.SlurmJob."""

from srunx.ssh.core.client_types import SlurmJob


class TestSlurmJob:
    def test_create_basic_job(self):
        job = SlurmJob(job_id="12345", name="test_job")

        assert job.job_id == "12345"
        assert job.name == "test_job"
        assert job.status == "UNKNOWN"
        assert job.output_file is None
        assert job.error_file is None
        assert job.script_path is None
        assert job.is_local_script is False
        assert job._cleanup is False

    def test_create_full_job(self):
        job = SlurmJob(
            job_id="67890",
            name="ml_training",
            status="RUNNING",
            output_file="/path/to/output.out",
            error_file="/path/to/error.err",
            script_path="/path/to/script.sh",
            is_local_script=True,
            _cleanup=True,
        )

        assert job.job_id == "67890"
        assert job.name == "ml_training"
        assert job.status == "RUNNING"
        assert job.output_file == "/path/to/output.out"
        assert job.error_file == "/path/to/error.err"
        assert job.script_path == "/path/to/script.sh"
        assert job.is_local_script is True
        assert job._cleanup is True
