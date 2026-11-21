"""Pytest configuration and fixtures."""

import os
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest

from srunx.domain import Job, JobEnvironment, JobResource


@pytest.fixture(autouse=True)
def _isolate_srunx_dirs(tmp_path_factory, monkeypatch):
    """Point every srunx file location at a per-test tmp tree.

    * ``XDG_CONFIG_HOME`` / ``XDG_STATE_HOME`` → fresh dirs, so the
      developer's real config, DB, locks and caches never leak in or get
      written to.
    * The user config gets a single ``local`` cluster as
      ``default_cluster`` so commands that need a cluster behave like a
      plain head-node setup unless a test configures otherwise.
    * ``$SRUNX_CLUSTER`` is cleared and settings caches are reset.
    """
    import srunx.common.config as _cfg

    fake_xdg = tmp_path_factory.mktemp("xdg")
    fake_state = tmp_path_factory.mktemp("xdg_state")
    monkeypatch.setenv("XDG_CONFIG_HOME", str(fake_xdg))
    monkeypatch.setenv("XDG_STATE_HOME", str(fake_state))
    monkeypatch.delenv("SRUNX_CLUSTER", raising=False)
    (fake_xdg / "srunx").mkdir()
    (fake_xdg / "srunx" / "config.toml").write_text(
        'default_cluster = "local"\n\n[clusters.local]\ntransport = "local"\n'
    )
    _cfg.clear_settings_cache()
    _cfg.set_active_config(None)
    yield
    _cfg.clear_settings_cache()
    _cfg.set_active_config(None)


@pytest.fixture(autouse=True)
def _stable_terminal_output(monkeypatch):
    """Stabilise Typer/Rich CLI output so substring assertions in
    help-text tests are deterministic regardless of host environment.

    Two coordinated env vars:

    - ``COLUMNS=200`` — wide enough that Rich does not wrap any
      single-line option description across multiple ``│``-separated
      table rows. Without this, CI runners (default ~75 cols) split
      ``"Start execution from this job"`` across lines and break a
      contiguous substring match.
    - ``NO_COLOR=1`` — disables Rich's syntax highlighting on option
      flags. Without this, Rich emits ``\\x1b[1;36m-\\x1b[0m\\x1b[1;36m-from\\x1b[0m``
      for ``--from``, splitting the token across reset+cyan ANSI
      sequences so even ``"--from" in result.stdout`` fails. The
      ``NO_COLOR`` convention (https://no-color.org) is honoured by
      Rich and Click.
    """
    monkeypatch.setenv("COLUMNS", "200")
    monkeypatch.setenv("NO_COLOR", "1")


@pytest.fixture
def temp_dir():
    """Create a temporary directory for tests."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        yield Path(tmp_dir)


@pytest.fixture
def tmp_srunx_db(tmp_path, monkeypatch):
    """Yield an isolated, file-backed srunx SQLite DB.

    Monkeypatches ``XDG_STATE_HOME`` so ``get_db_path()`` resolves under
    the per-test tmp dir, bootstraps the schema via ``init_db``, and
    yields an opened connection. **File-backed (NOT ``:memory:``)** so
    that multi-connection + WAL semantics work correctly for the outbox
    concurrency tests.
    """
    from srunx.observability.storage.connection import init_db, open_connection

    monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path))
    db_path = init_db()
    conn = open_connection(db_path)
    try:
        yield conn, db_path
    finally:
        conn.close()


@pytest.fixture
def sample_job():
    """Create a sample job for testing."""
    return Job(
        name="test_job",
        command=["python", "test.py"],
        resources=JobResource(
            nodes=1,
            gpus_per_node=0,
            ntasks_per_node=1,
            cpus_per_task=1,
        ),
        environment=JobEnvironment(conda="test_env"),
        log_dir="logs",
        work_dir="/tmp",
    )


@pytest.fixture
def sample_job_resource():
    """Create a sample job resource for testing."""
    return JobResource(
        nodes=2,
        gpus_per_node=1,
        ntasks_per_node=4,
        cpus_per_task=2,
        memory_per_node="32GB",
        time_limit="2:00:00",
    )


@pytest.fixture
def sample_job_environment():
    """Create a sample job environment for testing."""
    return JobEnvironment(
        conda="ml_env",
        env_vars={"CUDA_VISIBLE_DEVICES": "0,1", "OMP_NUM_THREADS": "4"},
    )


@pytest.fixture
def mock_subprocess_run(monkeypatch):
    """Mock subprocess.run for testing."""
    import subprocess
    from unittest.mock import Mock

    mock_result = Mock()
    mock_result.stdout = "12345"
    mock_result.returncode = 0

    def mock_run(*args, **kwargs):
        return mock_result

    monkeypatch.setattr(subprocess, "run", mock_run)
    return mock_result


@pytest.fixture(autouse=True)
def reset_config():
    """Reset global configuration and ensure clean test environment."""
    import srunx.domain

    if hasattr(srunx.domain, "_cached_config"):
        srunx.domain._cached_config = None

    # Clear environment variables that might affect jobs
    env_vars = ["SLURM_LOG_DIR"]

    original_values = {}
    for var in env_vars:
        original_values[var] = os.environ.get(var)
        if var in os.environ:
            del os.environ[var]

    # Mock subprocess to prevent actual system calls during tests
    with patch("subprocess.run") as mock_run:

        def mock_subprocess(*args, **kwargs):
            # Mock sacct to return PENDING status
            if args and len(args[0]) > 0 and "sacct" in str(args[0]):
                from unittest.mock import Mock

                result = Mock()
                result.stdout = "12345|PENDING\n"
                result.returncode = 0
                return result
            else:
                # For other subprocess calls, simulate command not found
                from subprocess import CalledProcessError

                raise CalledProcessError(1, args[0] if args else "unknown")

        mock_run.side_effect = mock_subprocess

        # Mock BaseJob.refresh to prevent real sacct calls and keep
        # _status unchanged (the mocked subprocess.run above would reset
        # every job to PENDING, breaking tests that set custom statuses).
        # Returns self to match the real refresh() signature.
        with patch.object(
            srunx.domain.BaseJob, "refresh", side_effect=lambda self=None: self
        ):
            yield

    # Restore original environment values
    for var, value in original_values.items():
        if value is not None:
            os.environ[var] = value
        elif var in os.environ:
            del os.environ[var]

    if hasattr(srunx.domain, "_cached_config"):
        srunx.domain._cached_config = None


@pytest.fixture
def add_clusters():
    """Return ``add(*names, **kw)`` that registers SSH clusters in the user config.

    Each name becomes ``[clusters.<name>] transport = "ssh"`` with ``host``
    set to the name (an ``~/.ssh/config`` alias). Pass ``default="name"`` to
    change ``default_cluster``.
    """
    import srunx.common.config as _cfg
    from srunx.common import paths

    def add(*names: str, default: str | None = None) -> None:
        path = paths.user_config_path()
        text = path.read_text()
        if default is not None:
            text = text.replace(
                'default_cluster = "local"', f'default_cluster = "{default}"'
            )
        for name in names:
            text += f'\n[clusters.{name}]\ntransport = "ssh"\nhost = "{name}"\n'
        path.write_text(text)
        _cfg.clear_settings_cache()

    return add


def append_user_config(text: str) -> None:
    """Append TOML *text* to the test's user config and drop cached settings."""
    import srunx.common.config as _cfg
    from srunx.common import paths

    path = paths.user_config_path()
    path.write_text(path.read_text() + "\n" + text + "\n")
    _cfg.clear_settings_cache()
