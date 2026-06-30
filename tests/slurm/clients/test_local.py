"""Tests for srunx.slurm.clients.local.LocalClient.

Focus: the submission-environment route (REQ-5). Local submit must invoke
sbatch with ``--export=ALL`` and a process environment composed as
``{**os.environ, **job.environment.env_vars}`` for both the Job and the
ShellJob branch. ``subprocess.run`` is mocked at the call site so we can
inspect the argv and the ``env=`` mapping.
"""

from __future__ import annotations

import os
from unittest.mock import MagicMock, patch

import pytest

from srunx.domain import Job, JobEnvironment, ShellJob
from srunx.slurm.clients.local import LocalClient


def _fake_run_result(job_id: str = "12345") -> MagicMock:
    result = MagicMock()
    result.stdout = job_id
    result.stderr = ""
    result.returncode = 0
    return result


@pytest.fixture
def client() -> LocalClient:
    return LocalClient()


class TestLocalSubmitExportAll:
    def test_job_branch_export_all_and_merged_env(self, client):
        job = Job(
            name="job_export_test",
            command=["echo", "hi"],
            environment=JobEnvironment(env_vars={"FOO": "bar"}),
            log_dir="",
            work_dir="",
        )
        with patch(
            "srunx.slurm.clients.local.subprocess.run",
            return_value=_fake_run_result(),
        ) as mock_run:
            client.submit(job)

        args, kwargs = mock_run.call_args
        argv = args[0]
        assert "--export=ALL" in argv
        passed_env = kwargs["env"]
        # Merged over os.environ, with the job env_var present.
        assert passed_env["FOO"] == "bar"
        for key in os.environ:
            assert key in passed_env

    def test_shell_job_branch_export_all_and_merged_env(self, client, tmp_path):
        user_script = tmp_path / "run.sh"
        user_script.write_text("#!/bin/bash\necho hi\n")
        job = ShellJob(
            name="shell_export_test",
            script_path=str(user_script),
            environment=JobEnvironment(env_vars={"FOO": "bar"}),
        )
        with patch(
            "srunx.slurm.clients.local.subprocess.run",
            return_value=_fake_run_result(),
        ) as mock_run:
            client.submit(job)

        args, kwargs = mock_run.call_args
        argv = args[0]
        assert "--export=ALL" in argv
        passed_env = kwargs["env"]
        assert passed_env["FOO"] == "bar"
        for key in os.environ:
            assert key in passed_env


class TestLocalSubmitNoEnvPreservesExportPolicy:
    """No --env → leave the script's / site's export policy untouched.

    A command-line ``--export`` overrides a script's own ``#SBATCH
    --export=NONE``, so we must NOT force ``--export=ALL`` (nor override the
    child env) when no job env vars were requested.
    """

    def test_job_branch_no_env(self, client):
        job = Job(
            name="plain_job",
            command=["echo", "hi"],
            log_dir="",
            work_dir="",
        )
        with patch(
            "srunx.slurm.clients.local.subprocess.run",
            return_value=_fake_run_result(),
        ) as mock_run:
            client.submit(job)

        args, kwargs = mock_run.call_args
        assert "--export=ALL" not in args[0]
        assert kwargs["env"] is None

    def test_shell_job_branch_no_env(self, client, tmp_path):
        user_script = tmp_path / "run.sh"
        user_script.write_text("#!/bin/bash\necho hi\n")
        job = ShellJob(name="plain_shell", script_path=str(user_script))
        with patch(
            "srunx.slurm.clients.local.subprocess.run",
            return_value=_fake_run_result(),
        ) as mock_run:
            client.submit(job)

        args, kwargs = mock_run.call_args
        assert "--export=ALL" not in args[0]
        assert kwargs["env"] is None

    def test_job_path_override_does_not_break_sbatch_resolution(self, client):
        """A job-level PATH override must not hide sbatch from the launcher.

        ``subprocess.run`` resolves the executable via ``env["PATH"]``; if we
        passed the job's PATH there, a PATH without the SLURM bin dir would
        fail submission before queueing. The launcher must resolve sbatch
        against its own PATH (abs path) while the job still receives PATH via
        --export=ALL.
        """
        job = Job(
            name="path_job",
            command=["echo", "hi"],
            environment=JobEnvironment(env_vars={"PATH": "/custom/bin"}),
            log_dir="",
            work_dir="",
        )
        with (
            patch(
                "srunx.slurm.clients.local.shutil.which",
                return_value="/opt/slurm/bin/sbatch",
            ),
            patch(
                "srunx.slurm.clients.local.subprocess.run",
                return_value=_fake_run_result(),
            ) as mock_run,
        ):
            client.submit(job)

        args, kwargs = mock_run.call_args
        # Launcher invokes the abs-resolved sbatch (not bare "sbatch").
        assert args[0][0] == "/opt/slurm/bin/sbatch"
        # The job still receives the overridden PATH via --export=ALL.
        assert kwargs["env"]["PATH"] == "/custom/bin"
        assert "--export=ALL" in args[0]
