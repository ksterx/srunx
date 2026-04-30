"""Pytest configuration and fixtures."""

import os
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest

from srunx.domain import Job, JobEnvironment, JobResource


@pytest.fixture(autouse=True)
def _disable_current_profile_fallback(monkeypatch):
    """Neutralise the Phase 2 current-profile fallback for every test.

    The real ``~/.config/srunx/config.json`` on a developer machine may
    have ``current_profile`` set via ``srunx ssh profile set``. Without
    this fixture, tests that expect the "default → local" transport
    path (AC-10.2 backward compat suite) would fail on that developer's
    machine because ``resolve_transport`` now falls through to the
    active SSH profile. Individual tests that *want* to exercise the
    current-profile fallback should monkeypatch it back in explicitly.
    """
    import srunx.transport.registry as _reg

    monkeypatch.setattr(_reg, "_current_profile_name", lambda: None)


@pytest.fixture(autouse=True)
def _isolate_xdg_config_home(tmp_path_factory, monkeypatch):
    """Block the developer's ``~/.config/srunx/config.json`` from leaking
    into tests.

    ``srunx.common.config`` resolves the user-wide config file under
    ``$XDG_CONFIG_HOME/srunx/config.json``, falling back to
    ``~/.config/srunx/config.json``. A developer who has set custom
    defaults there (e.g. ``nodes=2`` / ``log_dir='custom_logs'`` /
    ``partition='gpu'``) would see those values bleed into tests that
    exercise :class:`SrunxConfig` or :func:`get_config`. Redirecting
    ``XDG_CONFIG_HOME`` to a session-tmp dir guarantees a pristine
    config tree for every test, regardless of host machine.
    """
    fake_xdg = tmp_path_factory.mktemp("xdg")
    monkeypatch.setenv("XDG_CONFIG_HOME", str(fake_xdg))


@pytest.fixture(autouse=True)
def _stable_terminal_width(monkeypatch):
    """Pin ``COLUMNS`` to a wide value so Typer/Rich help output doesn't
    wrap mid-phrase under different host terminal sizes.

    CLI help-text tests rely on substring assertions like
    ``assert "Start execution from this job" in result.stdout``. When
    ``COLUMNS`` is small (CI runners default to ~75), Rich wraps the
    help description across multiple table lines with ``│`` separators,
    so the contiguous substring no longer appears in the rendered
    output. Pinning to 200 cols leaves room for any single-line
    description to render verbatim, regardless of host terminal.
    """
    monkeypatch.setenv("COLUMNS", "200")


@pytest.fixture(autouse=True)
def _isolate_legacy_history_db(tmp_path_factory, monkeypatch):
    """Make sure no test can delete the user's real ``~/.srunx/history.db``.

    After the Phase-2 history cutover, production code may call
    ``init_db(delete_legacy=True)`` (the new default) which runs
    :func:`srunx.observability.storage.connection._delete_legacy_history_db`. That
    helper targets ``Path.home() / '.srunx' / 'history.db'`` — outside
    any ``XDG_CONFIG_HOME`` isolation a test sets. Redirect the path
    to a session-scoped tmp dir so the delete is always safe.
    """
    import srunx.observability.storage.connection as _conn

    safe_legacy = tmp_path_factory.mktemp("legacy_history_safe") / "history.db"
    monkeypatch.setattr(_conn, "LEGACY_HISTORY_DB_PATH", safe_legacy)


@pytest.fixture
def temp_dir():
    """Create a temporary directory for tests."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        yield Path(tmp_dir)


@pytest.fixture
def tmp_srunx_db(tmp_path, monkeypatch):
    """Yield an isolated, file-backed srunx SQLite DB.

    Monkeypatches ``XDG_CONFIG_HOME`` so ``get_db_path()`` resolves under
    the per-test tmp dir, bootstraps the schema via ``init_db``, and
    yields an opened connection. **File-backed (NOT ``:memory:``)** so
    that multi-connection + WAL semantics work correctly for the outbox
    concurrency tests.
    """
    from srunx.observability.storage.connection import init_db, open_connection

    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path))
    db_path = init_db(delete_legacy=False)
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
    # Clear any cached config
    import srunx.common.config

    srunx.common.config._config = None

    # Also clear any module-level defaults cache
    import srunx.domain

    if hasattr(srunx.domain, "_cached_config"):
        srunx.domain._cached_config = None

    # Clear environment variables that might affect config
    env_vars = [
        "SLURM_LOG_DIR",
        "SRUNX_DEFAULT_NODES",
        "SRUNX_DEFAULT_GPUS_PER_NODE",
        "SRUNX_DEFAULT_MEMORY_PER_NODE",
        "SRUNX_DEFAULT_PARTITION",
        "SRUNX_DEFAULT_CONDA",
        "SRUNX_DEFAULT_VENV",
        "SRUNX_DEFAULT_CONTAINER",
        "SRUNX_DEFAULT_CONTAINER_RUNTIME",
        "SRUNX_DEFAULT_NTASKS_PER_NODE",
        "SRUNX_DEFAULT_CPUS_PER_TASK",
        "SRUNX_DEFAULT_TIME_LIMIT",
        "SRUNX_DEFAULT_NODELIST",
        "SRUNX_DEFAULT_LOG_DIR",
        "SRUNX_DEFAULT_WORK_DIR",
    ]

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

    # Clear config cache again
    srunx.common.config._config = None
    if hasattr(srunx.domain, "_cached_config"):
        srunx.domain._cached_config = None
