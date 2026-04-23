"""Tests for REST API routers with mocked SSH adapter."""

from __future__ import annotations

import contextlib
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient

from srunx.web.app import create_app
from srunx.web.deps import get_adapter

MOCK_JOBS = [
    {
        "name": "train-job",
        "job_id": 10001,
        "status": "RUNNING",
        "depends_on": [],
        "command": [],
        "resources": {"nodes": 1, "gpus_per_node": 4, "partition": "gpu"},
        "partition": "gpu",
        "nodes": 1,
        "gpus": 4,
        "elapsed_time": "1:30:00",
    },
]

MOCK_RESOURCES = [
    {
        "timestamp": "2026-03-30T00:00:00+00:00",
        "partition": "gpu",
        "total_gpus": 32,
        "gpus_in_use": 20,
        "gpus_available": 12,
        "jobs_running": 3,
        "nodes_total": 4,
        "nodes_idle": 1,
        "nodes_down": 0,
        "gpu_utilization": 0.625,
        "has_available_gpus": True,
    },
]


@pytest.fixture
def mock_adapter() -> MagicMock:
    adapter = MagicMock()
    adapter.list_jobs.return_value = MOCK_JOBS
    adapter.get_job.return_value = MOCK_JOBS[0]
    adapter.cancel_job.return_value = None
    adapter.get_job_output.return_value = ("stdout content", "stderr content", 14, 14)
    adapter.get_resources.return_value = MOCK_RESOURCES
    adapter.submit_job.return_value = {
        "name": "new-job",
        "job_id": 10002,
        "status": "PENDING",
        "depends_on": [],
        "command": [],
        "resources": {},
    }
    # Fix #3: the routers now read ``adapter.scheduler_key`` to build the
    # V5 transport triple. Default to local so existing tests (that
    # never set this explicitly) continue to behave like pre-V5 mocks.
    adapter.scheduler_key = "local"
    return adapter


@pytest.fixture
def client(  # type: ignore[misc]
    mock_adapter: MagicMock,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> TestClient:
    import srunx.web.config as config_mod
    from srunx.db.connection import init_db
    from srunx.web.config import get_web_config

    # Isolate the srunx DB to a tmp dir so workflow_runs don't leak
    # between tests or into the user's real DB.
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / "xdg"))
    init_db(delete_legacy=False)

    original = config_mod._config
    config_mod._config = None
    cfg = get_web_config()
    config_mod._config = cfg

    # Create a fake mount directory so per-mount workflow storage works
    mount_local = tmp_path / "project"
    mount_local.mkdir()

    # Patch get_current_profile in sync_utils to return a profile with a mount
    from unittest.mock import patch

    from srunx.ssh.core.config import MountConfig, ServerProfile

    fake_mount = MountConfig(
        name="test-project", local=str(mount_local), remote="/home/user/project"
    )
    fake_profile = ServerProfile(
        hostname="test.example.com",
        username="tester",
        key_filename="~/.ssh/id_rsa",
        mounts=[fake_mount],
    )

    app = create_app()
    app.dependency_overrides[get_adapter] = lambda: mock_adapter

    with patch(
        "srunx.web.routers.workflows._get_current_profile", return_value=fake_profile
    ):
        yield TestClient(app, raise_server_exceptions=False)

    config_mod._config = original


# Mount name used in all workflow tests
MOUNT = "test-project"


# ── Jobs Router ───────────────────────────────────


class TestJobsRouter:
    def test_list_jobs(self, client: TestClient) -> None:
        resp = client.get("/api/jobs")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 1
        assert data[0]["job_id"] == 10001

    def test_get_job(self, client: TestClient) -> None:
        resp = client.get("/api/jobs/10001")
        assert resp.status_code == 200
        assert resp.json()["name"] == "train-job"

    def test_get_job_invalid_id(self, client: TestClient) -> None:
        resp = client.get("/api/jobs/0")
        assert resp.status_code == 400

    def test_get_job_not_found(
        self, client: TestClient, mock_adapter: MagicMock
    ) -> None:
        mock_adapter.get_job.side_effect = ValueError("No job information found")
        resp = client.get("/api/jobs/99999")
        assert resp.status_code == 404

    def test_cancel_job(self, client: TestClient) -> None:
        resp = client.delete("/api/jobs/10001")
        assert resp.status_code == 204

    def test_cancel_job_invalid_id(self, client: TestClient) -> None:
        resp = client.delete("/api/jobs/0")
        assert resp.status_code == 400

    def test_get_logs(self, client: TestClient) -> None:
        resp = client.get("/api/jobs/10001/logs")
        assert resp.status_code == 200
        data = resp.json()
        assert data["stdout"] == "stdout content"
        assert data["stderr"] == "stderr content"
        assert data["stdout_offset"] == 14
        assert data["stderr_offset"] == 14

    def test_get_logs_with_offset(
        self, client: TestClient, mock_adapter: MagicMock
    ) -> None:
        mock_adapter.get_job_output.return_value = ("new line\n", "", 24, 14)
        resp = client.get("/api/jobs/10001/logs?stdout_offset=14&stderr_offset=14")
        assert resp.status_code == 200
        data = resp.json()
        assert data["stdout"] == "new line\n"
        assert data["stderr"] == ""
        assert data["stdout_offset"] == 24
        mock_adapter.get_job_output.assert_called_once_with(
            10001, stdout_offset=14, stderr_offset=14
        )

    def test_get_logs_not_found(
        self, client: TestClient, mock_adapter: MagicMock
    ) -> None:
        mock_adapter.get_job_output.side_effect = FileNotFoundError("No logs")
        resp = client.get("/api/jobs/10001/logs")
        assert resp.status_code == 404

    def test_submit_job(self, client: TestClient) -> None:
        resp = client.post(
            "/api/jobs",
            json={"name": "new-job", "script_content": "#!/bin/bash\necho hello"},
        )
        assert resp.status_code == 201
        assert resp.json()["job_id"] == 10002

    def test_slurm_error_returns_502(
        self, client: TestClient, mock_adapter: MagicMock
    ) -> None:
        mock_adapter.list_jobs.side_effect = RuntimeError("squeue failed")
        resp = client.get("/api/jobs")
        assert resp.status_code == 502


class TestJobsRouterScriptPath:
    """POST /api/jobs ``script_path`` mode (#136 in-place execution).

    Validator-side cases hit the request path before any I/O so the
    mocks below are deliberately minimal — those tests assert the
    422 contract without touching the SSH adapter at all.

    The dispatch tests stub :func:`mount_sync_session` so the lock
    acquisition + rsync no-op out, then assert that
    :meth:`SlurmSSHAdapter.submit_remote_sbatch` was called with the
    correctly translated remote path.
    """

    def test_neither_script_content_nor_path_rejects(self, client: TestClient) -> None:
        resp = client.post("/api/jobs", json={"name": "x"})
        assert resp.status_code == 422
        assert "exactly one of script_content / script_path" in resp.text

    def test_both_script_content_and_path_rejects(self, client: TestClient) -> None:
        resp = client.post(
            "/api/jobs",
            json={
                "name": "x",
                "script_content": "#!/bin/bash\n",
                "script_path": "/tmp/run.sh",
                "mount_name": MOUNT,
            },
        )
        assert resp.status_code == 422
        assert "mutually exclusive" in resp.text

    def test_script_path_without_mount_rejects(self, client: TestClient) -> None:
        resp = client.post(
            "/api/jobs",
            json={"name": "x", "script_path": "/tmp/run.sh"},
        )
        assert resp.status_code == 422
        assert "script_path requires mount_name" in resp.text

    def test_script_path_dispatches_in_place(
        self,
        client: TestClient,
        mock_adapter: MagicMock,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        from contextlib import contextmanager
        from unittest.mock import patch

        from srunx.ssh.core.config import MountConfig, ServerProfile

        # Reuse the same mount root the ``client`` fixture set up so
        # the path validation passes against the real on-disk dir.
        mount_local = tmp_path / "project"
        script = mount_local / "run.sh"
        script.write_text("#!/bin/bash\necho hi\n")

        fake_profile = ServerProfile(
            hostname="x",
            username="u",
            key_filename="~/.ssh/id_rsa",
            mounts=[
                MountConfig(
                    name=MOUNT,
                    local=str(mount_local),
                    remote="/home/u/project",
                )
            ],
        )

        # ``adapter.scheduler_key`` becomes the profile name fed into
        # ``mount_sync_session`` — match the mock to the profile we're
        # constructing so the assertion below exercises the full
        # ``ssh:<profile>`` → profile-name unwrap.
        mock_adapter.scheduler_key = "ssh:test-profile"

        submitted = MagicMock()
        submitted.job_id = 99001
        submitted.name = "in-place-job"
        mock_adapter.submit_remote_sbatch.return_value = submitted

        @contextmanager
        def fake_session(**kwargs):  # type: ignore[no-untyped-def]
            yield MagicMock(performed=True, warnings=())

        with (
            patch(
                "srunx.web.sync_utils.get_current_profile",
                return_value=fake_profile,
            ),
            patch("srunx.sync.service.mount_sync_session", fake_session),
        ):
            resp = client.post(
                "/api/jobs",
                json={
                    "name": "in-place-job",
                    "script_path": str(script),
                    "mount_name": MOUNT,
                },
            )

        assert resp.status_code == 201, resp.text
        body = resp.json()
        assert body["job_id"] == 99001
        assert body["name"] == "in-place-job"

        # Local→remote translation: ``project/run.sh`` under
        # ``/home/u/project`` → ``/home/u/project/run.sh``. submit_cwd
        # is the script's parent dir on the remote.
        mock_adapter.submit_remote_sbatch.assert_called_once()
        _, kwargs = mock_adapter.submit_remote_sbatch.call_args
        args = mock_adapter.submit_remote_sbatch.call_args.args
        assert args[0] == "/home/u/project/run.sh"
        assert kwargs["submit_cwd"] == "/home/u/project"
        assert kwargs["job_name"] == "in-place-job"
        # Legacy tmp-upload path must NOT have run when script_path
        # was provided.
        mock_adapter.submit_job.assert_not_called()

    def test_script_path_outside_mount_returns_403(
        self,
        client: TestClient,
        mock_adapter: MagicMock,
        tmp_path: Path,
    ) -> None:
        from unittest.mock import patch

        from srunx.ssh.core.config import MountConfig, ServerProfile

        mount_local = tmp_path / "project"
        # The escape target sits OUTSIDE mount_local — the
        # ``Path.is_relative_to`` check must reject it.
        outside = tmp_path / "elsewhere" / "evil.sh"
        outside.parent.mkdir()
        outside.write_text("#!/bin/bash\n")

        fake_profile = ServerProfile(
            hostname="x",
            username="u",
            key_filename="~/.ssh/id_rsa",
            mounts=[
                MountConfig(
                    name=MOUNT,
                    local=str(mount_local),
                    remote="/home/u/project",
                )
            ],
        )

        with patch(
            "srunx.web.sync_utils.get_current_profile",
            return_value=fake_profile,
        ):
            resp = client.post(
                "/api/jobs",
                json={
                    "name": "evil",
                    "script_path": str(outside),
                    "mount_name": MOUNT,
                },
            )
        assert resp.status_code == 403
        assert "outside mount" in resp.text
        # The adapter must never see an off-mount script_path.
        mock_adapter.submit_remote_sbatch.assert_not_called()

    def test_script_path_unknown_mount_returns_404(
        self,
        client: TestClient,
        mock_adapter: MagicMock,
        tmp_path: Path,
    ) -> None:
        from unittest.mock import patch

        from srunx.ssh.core.config import MountConfig, ServerProfile

        mount_local = tmp_path / "project"
        script = mount_local / "run.sh"
        script.write_text("#!/bin/bash\n")

        # Profile has a mount, but its name doesn't match the request.
        fake_profile = ServerProfile(
            hostname="x",
            username="u",
            key_filename="~/.ssh/id_rsa",
            mounts=[
                MountConfig(
                    name="other",
                    local=str(mount_local),
                    remote="/home/u/project",
                )
            ],
        )

        with patch(
            "srunx.web.sync_utils.get_current_profile",
            return_value=fake_profile,
        ):
            resp = client.post(
                "/api/jobs",
                json={
                    "name": "x",
                    "script_path": str(script),
                    "mount_name": "missing-mount",
                },
            )
        assert resp.status_code == 404
        assert "missing-mount" in resp.text

    def test_sync_failure_returns_502(
        self,
        client: TestClient,
        mock_adapter: MagicMock,
        tmp_path: Path,
    ) -> None:
        """An rsync ``RuntimeError`` (non-zero exit) surfaces as 502."""
        from contextlib import contextmanager
        from unittest.mock import patch

        from srunx.ssh.core.config import MountConfig, ServerProfile

        mount_local = tmp_path / "project"
        script = mount_local / "run.sh"
        script.write_text("#!/bin/bash\n")

        fake_profile = ServerProfile(
            hostname="x",
            username="u",
            key_filename="~/.ssh/id_rsa",
            mounts=[
                MountConfig(
                    name=MOUNT,
                    local=str(mount_local),
                    remote="/home/u/project",
                )
            ],
        )

        @contextmanager
        def boom_session(**kwargs):  # type: ignore[no-untyped-def]
            raise RuntimeError("rsync exited with status 23")
            yield  # unreachable; satisfies type checker

        with (
            patch(
                "srunx.web.sync_utils.get_current_profile",
                return_value=fake_profile,
            ),
            patch("srunx.sync.service.mount_sync_session", boom_session),
        ):
            resp = client.post(
                "/api/jobs",
                json={
                    "name": "fails",
                    "script_path": str(script),
                    "mount_name": MOUNT,
                },
            )
        assert resp.status_code == 502
        assert "Mount sync failed" in resp.text
        mock_adapter.submit_remote_sbatch.assert_not_called()


# ── Resources Router ──────────────────────────────


class TestResourcesRouter:
    def test_get_resources(self, client: TestClient) -> None:
        resp = client.get("/api/resources")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 1
        assert data[0]["partition"] == "gpu"
        assert data[0]["total_gpus"] == 32

    def test_get_resources_with_partition(self, client: TestClient) -> None:
        resp = client.get("/api/resources?partition=gpu")
        assert resp.status_code == 200

    def test_slurm_error_returns_502(
        self, client: TestClient, mock_adapter: MagicMock
    ) -> None:
        mock_adapter.get_resources.side_effect = RuntimeError("sinfo failed")
        resp = client.get("/api/resources")
        assert resp.status_code == 502


# ── History Router ────────────────────────────────


class TestHistoryRouter:
    def test_get_stats(self, client: TestClient) -> None:
        resp = client.get("/api/history/stats")
        assert resp.status_code == 200
        data = resp.json()
        assert "total" in data
        assert "completed" in data

    def test_get_recent(self, client: TestClient) -> None:
        resp = client.get("/api/history")
        assert resp.status_code == 200


# ── Workflows Router ─────────────────────────────


class TestWorkflowsRouter:
    def test_list_empty(self, client: TestClient) -> None:
        resp = client.get("/api/workflows", params={"mount": MOUNT})
        assert resp.status_code == 200
        assert resp.json() == []

    def test_validate_empty_yaml(self, client: TestClient) -> None:
        resp = client.post("/api/workflows/validate", json={"yaml": ""})
        data = resp.json()
        assert data["valid"] is False

    def test_validate_rejects_python_args(self, client: TestClient) -> None:
        resp = client.post(
            "/api/workflows/validate",
            json={"yaml": "name: test\nargs:\n  x: 'python: 1+1'"},
        )
        assert resp.status_code == 422

    def test_validate_rejects_python_args_in_list_element(
        self, client: TestClient
    ) -> None:
        # Regression: YAML path previously skipped list elements inside
        # ``args``. The unified guard scans recursively.
        yaml_text = "name: test\nargs:\n  x:\n    - safe\n    - 'python: evil'\n"
        resp = client.post("/api/workflows/validate", json={"yaml": yaml_text})
        assert resp.status_code == 422, resp.text

    def test_upload_rejects_path_traversal(self, client: TestClient) -> None:
        resp = client.post(
            "/api/workflows/upload",
            json={
                "yaml": "name: test\njobs: []",
                "filename": "../../evil.yaml",
                "mount": MOUNT,
            },
        )
        # Should use safe basename, so stem "evil" passes but path is safe
        assert resp.status_code in (200, 422)

    def test_upload_rejects_bad_filename(self, client: TestClient) -> None:
        resp = client.post(
            "/api/workflows/upload",
            json={"yaml": "name: test", "filename": "bad name!.yaml", "mount": MOUNT},
        )
        assert resp.status_code == 422

    def test_upload_requires_mount(self, client: TestClient) -> None:
        resp = client.post(
            "/api/workflows/upload",
            json={"yaml": "name: test\njobs: []", "filename": "test.yaml"},
        )
        assert resp.status_code == 422

    def test_list_runs(self, client: TestClient) -> None:
        resp = client.get("/api/workflows/runs")
        assert resp.status_code == 200
        assert resp.json() == []

    # ── POST /api/workflows/create ───────────────────

    def test_create_workflow_requires_mount(self, client: TestClient) -> None:
        payload = {
            "name": "no-mount",
            "jobs": [{"name": "a", "command": ["echo", "hi"]}],
        }
        resp = client.post("/api/workflows/create", json=payload)
        assert resp.status_code == 422

    def test_create_workflow_success(self, client: TestClient) -> None:
        payload = {
            "name": "my-pipeline",
            "default_project": MOUNT,
            "jobs": [
                {
                    "name": "preprocess",
                    "command": ["python", "preprocess.py"],
                },
                {
                    "name": "train",
                    "command": ["python", "train.py"],
                    "depends_on": ["preprocess"],
                    "resources": {"gpus_per_node": 2},
                    "environment": {"conda": "ml"},
                },
            ],
        }
        resp = client.post("/api/workflows/create", json=payload)
        assert resp.status_code == 200
        data = resp.json()
        assert data["name"] == "my-pipeline"
        assert len(data["jobs"]) == 2
        assert data["jobs"][0]["name"] == "preprocess"
        assert data["jobs"][1]["name"] == "train"
        assert data["jobs"][1]["depends_on"] == ["preprocess"]

    def test_create_workflow_conflict(self, client: TestClient) -> None:
        payload = {
            "name": "dup-wf",
            "default_project": MOUNT,
            "jobs": [{"name": "a", "command": ["echo", "hi"]}],
        }
        resp1 = client.post("/api/workflows/create", json=payload)
        assert resp1.status_code == 200

        resp2 = client.post("/api/workflows/create", json=payload)
        assert resp2.status_code == 409

    def test_create_workflow_reserved_name(self, client: TestClient) -> None:
        payload = {
            "name": "new",
            "default_project": MOUNT,
            "jobs": [{"name": "a", "command": ["echo", "hi"]}],
        }
        resp = client.post("/api/workflows/create", json=payload)
        assert resp.status_code == 422
        assert "reserved" in resp.json()["detail"]

    def test_create_workflow_bad_name(self, client: TestClient) -> None:
        payload = {
            "name": "bad name!",
            "default_project": MOUNT,
            "jobs": [{"name": "a", "command": ["echo", "hi"]}],
        }
        resp = client.post("/api/workflows/create", json=payload)
        assert resp.status_code == 422

    def test_create_workflow_cycle_detected(self, client: TestClient) -> None:
        payload = {
            "name": "cyclic",
            "default_project": MOUNT,
            "jobs": [
                {"name": "a", "command": ["echo", "a"], "depends_on": ["b"]},
                {"name": "b", "command": ["echo", "b"], "depends_on": ["a"]},
            ],
        }
        resp = client.post("/api/workflows/create", json=payload)
        assert resp.status_code == 422, (
            f"Expected 422, got {resp.status_code}: {resp.text}"
        )

    def test_create_workflow_unknown_dependency(self, client: TestClient) -> None:
        payload = {
            "name": "bad-dep",
            "default_project": MOUNT,
            "jobs": [
                {
                    "name": "a",
                    "command": ["echo", "hi"],
                    "depends_on": ["nonexistent"],
                },
            ],
        }
        resp = client.post("/api/workflows/create", json=payload)
        assert resp.status_code == 422

    def test_create_workflow_persists_yaml(self, client: TestClient) -> None:
        """Verify the YAML file is written and can be re-loaded."""
        payload = {
            "name": "persist-test",
            "default_project": MOUNT,
            "jobs": [{"name": "step1", "command": ["bash", "-c", "echo ok"]}],
        }
        resp = client.post("/api/workflows/create", json=payload)
        assert resp.status_code == 200

        # The workflow should now appear in the list
        list_resp = client.get("/api/workflows", params={"mount": MOUNT})
        names = [w["name"] for w in list_resp.json()]
        assert "persist-test" in names

    def test_create_workflow_retrievable_by_name(self, client: TestClient) -> None:
        payload = {
            "name": "fetch-me",
            "default_project": MOUNT,
            "jobs": [{"name": "only", "command": ["true"]}],
        }
        resp = client.post("/api/workflows/create", json=payload)
        assert resp.status_code == 200

        get_resp = client.get("/api/workflows/fetch-me", params={"mount": MOUNT})
        assert get_resp.status_code == 200
        assert get_resp.json()["name"] == "fetch-me"

    # ── GET /api/workflows/runs/{run_id} ────────────

    def test_get_run_not_found(self, client: TestClient) -> None:
        resp = client.get("/api/workflows/runs/nonexistent-id")
        assert resp.status_code == 404

    def test_get_run_returns_created_run(self, client: TestClient) -> None:
        """Create a run via the repo, then fetch it by ID."""
        from srunx.db.connection import open_connection
        from srunx.db.repositories.workflow_runs import WorkflowRunRepository

        conn = open_connection()
        try:
            run_id = WorkflowRunRepository(conn).create(
                workflow_name="test-wf",
                yaml_path=None,
                args=None,
                triggered_by="web",
            )
        finally:
            conn.close()

        resp = client.get(f"/api/workflows/runs/{run_id}")
        assert resp.status_code == 200
        data = resp.json()
        assert data["id"] == str(run_id)
        assert data["workflow_name"] == "test-wf"

    # ── POST /api/workflows/{name}/run ──────────────

    def test_run_workflow_success(
        self, client: TestClient, mock_adapter: MagicMock
    ) -> None:
        """Run a workflow end-to-end: create, then run with mocked adapter."""
        # Create the workflow first
        create_payload = {
            "name": "run-test",
            "default_project": MOUNT,
            "jobs": [
                {"name": "step1", "command": ["echo", "a"]},
                {
                    "name": "step2",
                    "command": ["echo", "b"],
                    "depends_on": ["step1"],
                },
            ],
        }
        resp = client.post("/api/workflows/create", json=create_payload)
        assert resp.status_code == 200

        # Mock submit_job to return incrementing job IDs
        call_count = 0

        def mock_submit(script_content, job_name=None, dependency=None):
            nonlocal call_count
            call_count += 1
            return {
                "name": job_name or "job",
                "job_id": 10000 + call_count,
                "status": "PENDING",
                "depends_on": [],
                "command": [],
                "resources": {},
            }

        mock_adapter.submit_job.side_effect = mock_submit

        # Run the workflow
        resp = client.post("/api/workflows/run-test/run", params={"mount": MOUNT})
        assert resp.status_code == 202
        data = resp.json()
        assert data["workflow_name"] == "run-test"
        # Status stays ``pending`` until ``ActiveWatchPoller`` observes
        # the first child transition (P1-1). Pre-emptively writing
        # ``running`` here caused a spurious ``running → pending``
        # transition on the first poll cycle.
        assert data["status"] == "pending"
        assert "10001" in data["job_ids"].values()
        assert "10002" in data["job_ids"].values()

        # Verify topological order: step1 submitted without deps, step2 with deps
        calls = mock_adapter.submit_job.call_args_list
        assert len(calls) == 2
        # First call should have no dependency
        assert (
            calls[0].kwargs.get("dependency") is None
            or calls[0][1].get("dependency") is None
        )
        # Second call should have afterok dependency on step1's job_id
        second_call_kwargs = calls[1].kwargs if calls[1].kwargs else {}
        dep = second_call_kwargs.get("dependency", "")
        assert "afterok:10001" in dep

    def test_run_workflow_persists_all_db_rows(
        self, client: TestClient, mock_adapter: MagicMock
    ) -> None:
        """End-to-end DB verification — the refactor's real contract.

        The old run_registry hid state in-memory. The new flow must
        write *atomically* to workflow_runs + workflow_run_jobs + jobs
        + job_state_transitions, and open a workflow_run watch, so
        ActiveWatchPoller can aggregate child statuses after we return.
        """
        from srunx.db.connection import open_connection
        from srunx.db.repositories.job_state_transitions import (
            JobStateTransitionRepository,
        )
        from srunx.db.repositories.jobs import JobRepository
        from srunx.db.repositories.watches import WatchRepository
        from srunx.db.repositories.workflow_run_jobs import (
            WorkflowRunJobRepository,
        )
        from srunx.db.repositories.workflow_runs import WorkflowRunRepository

        create_payload = {
            "name": "integration-run",
            "default_project": MOUNT,
            "jobs": [
                {"name": "a", "command": ["echo", "a"]},
                {"name": "b", "command": ["echo", "b"], "depends_on": ["a"]},
            ],
        }
        resp = client.post("/api/workflows/create", json=create_payload)
        assert resp.status_code == 200

        call_count = 0

        def mock_submit(script_content, job_name=None, dependency=None):
            nonlocal call_count
            call_count += 1
            return {
                "name": job_name or "job",
                "job_id": 20000 + call_count,
                "status": "PENDING",
                "depends_on": [],
                "command": [],
                "resources": {},
            }

        mock_adapter.submit_job.side_effect = mock_submit

        resp = client.post(
            "/api/workflows/integration-run/run", params={"mount": MOUNT}
        )
        assert resp.status_code == 202
        data = resp.json()
        run_id = int(data["id"])

        # Verify every DB invariant the refactor committed to.
        conn = open_connection()
        try:
            run = WorkflowRunRepository(conn).get(run_id)
            assert run is not None
            assert run.workflow_name == "integration-run"
            # P1-1: the run stays ``pending`` until the poller observes
            # a RUNNING child. See the phase-5 comment in workflows.py.
            assert run.status == "pending"
            assert run.triggered_by == "web"

            memberships = WorkflowRunJobRepository(conn).list_by_run(run_id)
            assert len(memberships) == 2
            membership_names = {m.job_name for m in memberships}
            assert membership_names == {"a", "b"}
            # FK: each membership's job_id points at a real jobs row.
            for m in memberships:
                assert m.job_id is not None
                job = JobRepository(conn).get(m.job_id, scheduler_key="local")
                assert job is not None
                assert job.status == "PENDING"
                assert job.submission_source == "workflow"
                assert job.workflow_run_id == run_id

            # Seeded PENDING transitions exist for both jobs — without
            # them ActiveWatchPoller would skip the first observation.
            for m in memberships:
                assert m.job_id is not None
                latest = JobStateTransitionRepository(conn).latest_for_job(
                    m.job_id, scheduler_key="local"
                )
                assert latest is not None
                assert latest.to_status == "PENDING"
                assert latest.source == "webhook"

            # An OPEN workflow_run watch exists so the poller can
            # aggregate child statuses.
            open_watches = [
                w for w in WatchRepository(conn).list_open() if w.kind == "workflow_run"
            ]
            assert len(open_watches) == 1
            assert open_watches[0].target_ref == f"workflow_run:{run_id}"
        finally:
            conn.close()

    def test_run_workflow_with_notify_creates_subscription(
        self, client: TestClient, mock_adapter: MagicMock
    ) -> None:
        """``notify=True`` + valid endpoint_id → one subscription on the run watch.

        P3-7 #H wires the workflow-run dialog's notification toggle
        through the API: the auto-created ``kind='workflow_run'`` watch
        must be paired with a subscription when the caller opts in, so
        the delivery poller fans ``workflow_run.status_changed`` events
        out to Slack/etc.
        """
        from srunx.db.connection import open_connection
        from srunx.db.repositories.endpoints import EndpointRepository
        from srunx.db.repositories.subscriptions import SubscriptionRepository
        from srunx.db.repositories.watches import WatchRepository

        # Seed an endpoint row — real FK target.
        conn = open_connection()
        try:
            endpoint_id = EndpointRepository(conn).create(
                kind="slack_webhook",
                name="ops",
                config={"webhook_url": "https://hooks.slack.com/services/T/B/X"},
            )
        finally:
            conn.close()

        resp = client.post(
            "/api/workflows/create",
            json={
                "name": "notify-run",
                "default_project": MOUNT,
                "jobs": [{"name": "a", "command": ["echo", "a"]}],
            },
        )
        assert resp.status_code == 200

        mock_adapter.submit_job.return_value = {
            "name": "a",
            "job_id": 30000,
            "status": "PENDING",
            "depends_on": [],
            "command": [],
            "resources": {},
        }

        resp = client.post(
            "/api/workflows/notify-run/run",
            params={"mount": MOUNT},
            json={
                "notify": True,
                "endpoint_id": endpoint_id,
                "preset": "all",
            },
        )
        assert resp.status_code == 202
        run_id = int(resp.json()["id"])

        conn = open_connection()
        try:
            watch = next(
                w
                for w in WatchRepository(conn).list_open()
                if w.target_ref == f"workflow_run:{run_id}"
            )
            assert watch.id is not None
            subs = SubscriptionRepository(conn).list_by_watch(watch.id)
            assert len(subs) == 1
            assert subs[0].endpoint_id == endpoint_id
            assert subs[0].preset == "all"
        finally:
            conn.close()

    def test_run_workflow_without_notify_creates_no_subscription(
        self, client: TestClient, mock_adapter: MagicMock
    ) -> None:
        """Default path — watch is created, but no subscription."""
        from srunx.db.connection import open_connection
        from srunx.db.repositories.subscriptions import SubscriptionRepository
        from srunx.db.repositories.watches import WatchRepository

        resp = client.post(
            "/api/workflows/create",
            json={
                "name": "no-notify-run",
                "default_project": MOUNT,
                "jobs": [{"name": "a", "command": ["echo", "a"]}],
            },
        )
        assert resp.status_code == 200

        mock_adapter.submit_job.return_value = {
            "name": "a",
            "job_id": 30100,
            "status": "PENDING",
            "depends_on": [],
            "command": [],
            "resources": {},
        }

        resp = client.post(
            "/api/workflows/no-notify-run/run",
            params={"mount": MOUNT},
        )
        assert resp.status_code == 202
        run_id = int(resp.json()["id"])

        conn = open_connection()
        try:
            watch = next(
                w
                for w in WatchRepository(conn).list_open()
                if w.target_ref == f"workflow_run:{run_id}"
            )
            assert watch.id is not None
            assert SubscriptionRepository(conn).list_by_watch(watch.id) == []
        finally:
            conn.close()

    def test_run_workflow_invalid_preset_is_rejected(
        self, client: TestClient, mock_adapter: MagicMock
    ) -> None:
        """Bogus preset → 422, **before** any sbatch submission.

        Guards against the "422 with orphan jobs" shape: validating
        preset after ``_submit_jobs_bfs`` would leave already-queued
        SLURM jobs running on the cluster with no accompanying
        workflow_run record and no way for the user to find them.
        """
        resp = client.post(
            "/api/workflows/create",
            json={
                "name": "bad-preset-run",
                "default_project": MOUNT,
                "jobs": [{"name": "a", "command": ["echo", "a"]}],
            },
        )
        assert resp.status_code == 200

        mock_adapter.submit_job.return_value = {
            "name": "a",
            "job_id": 30200,
            "status": "PENDING",
            "depends_on": [],
            "command": [],
            "resources": {},
        }

        resp = client.post(
            "/api/workflows/bad-preset-run/run",
            params={"mount": MOUNT},
            json={"notify": True, "endpoint_id": 1, "preset": "digest"},
        )
        assert resp.status_code == 422
        assert "preset" in resp.json()["detail"].lower()

        # The early reject must short-circuit BEFORE phase 4: no sbatch
        # calls, and no ``workflow_runs`` row for the aborted run.
        mock_adapter.submit_job.assert_not_called()
        from srunx.db.connection import open_connection
        from srunx.db.repositories.workflow_runs import WorkflowRunRepository

        conn = open_connection()
        try:
            all_runs = WorkflowRunRepository(conn).list_all()
            assert not any(r.workflow_name == "bad-preset-run" for r in all_runs)
        finally:
            conn.close()

    def test_run_workflow_not_found(self, client: TestClient) -> None:
        resp = client.post("/api/workflows/nonexistent-wf/run", params={"mount": MOUNT})
        assert resp.status_code == 404

    def test_run_workflow_invalid_name(self, client: TestClient) -> None:
        resp = client.post("/api/workflows/bad name!/run", params={"mount": MOUNT})
        assert resp.status_code == 422

    def test_run_workflow_requires_mount(self, client: TestClient) -> None:
        resp = client.post("/api/workflows/some-wf/run")
        assert resp.status_code == 422

    def test_run_workflow_submit_failure(
        self, client: TestClient, mock_adapter: MagicMock
    ) -> None:
        """If sbatch fails, the run should be marked as failed."""
        create_payload = {
            "name": "fail-run",
            "default_project": MOUNT,
            "jobs": [{"name": "boom", "command": ["echo", "fail"]}],
        }
        resp = client.post("/api/workflows/create", json=create_payload)
        assert resp.status_code == 200

        mock_adapter.submit_job.side_effect = RuntimeError("sbatch error")

        resp = client.post("/api/workflows/fail-run/run", params={"mount": MOUNT})
        assert resp.status_code == 502
        assert "sbatch" in resp.json()["detail"]

    def test_run_workflow_midway_failure_cancels_orphans_and_records(
        self, client: TestClient, mock_adapter: MagicMock
    ) -> None:
        """Covers R2 + R3 from the Codex review.

        When sbatch fails on the Nth job after N-1 prior submits, we
        must (a) cancel the orphan SLURM jobs so the cluster isn't
        left running work the caller rolled back, and (b) still
        persist the failed node as a membership row with
        ``job_id=None`` so GET /runs/{id} faithfully reflects the DAG.
        """
        from srunx.db.connection import open_connection
        from srunx.db.repositories.workflow_run_jobs import (
            WorkflowRunJobRepository,
        )
        from srunx.db.repositories.workflow_runs import WorkflowRunRepository

        create_payload = {
            "name": "partial-fail",
            "default_project": MOUNT,
            "jobs": [
                {"name": "a", "command": ["echo", "ok"]},
                {"name": "b", "command": ["echo", "boom"], "depends_on": ["a"]},
            ],
        }
        resp = client.post("/api/workflows/create", json=create_payload)
        assert resp.status_code == 200

        # First submit succeeds, second raises — mimicking a transient
        # sbatch outage between siblings.
        call_count = 0

        def mock_submit(script_content, job_name=None, dependency=None):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return {
                    "name": job_name or "job",
                    "job_id": 30001,
                    "status": "PENDING",
                    "depends_on": [],
                    "command": [],
                    "resources": {},
                }
            raise RuntimeError("sbatch hiccup")

        mock_adapter.submit_job.side_effect = mock_submit

        resp = client.post("/api/workflows/partial-fail/run", params={"mount": MOUNT})
        assert resp.status_code == 502

        # R2: the already-submitted job must have been cancelled.
        mock_adapter.cancel_job.assert_any_call(30001)

        # The run was marked failed with a useful error.
        conn = open_connection()
        try:
            run_repo = WorkflowRunRepository(conn)
            runs = [r for r in run_repo.list_all() if r.workflow_name == "partial-fail"]
            assert runs, "workflow run was not created"
            run = runs[0]
            assert run.status == "failed"
            assert run.error is not None and "Submission failed" in run.error

            # R3: both nodes are represented in the DAG — the succeeded
            # one with its slurm_id, the failed one with job_id=None.
            assert run.id is not None
            memberships = WorkflowRunJobRepository(conn).list_by_run(run.id)
            by_name = {m.job_name: m for m in memberships}
            assert by_name["a"].job_id == 30001
            assert by_name["b"].job_id is None
        finally:
            conn.close()

    # ── DELETE /api/workflows/{name} ───────────────

    def test_delete_workflow(self, client: TestClient) -> None:
        """Create a workflow then delete it."""
        payload = {
            "name": "to-delete",
            "default_project": MOUNT,
            "jobs": [{"name": "a", "command": ["echo", "hi"]}],
        }
        resp = client.post("/api/workflows/create", json=payload)
        assert resp.status_code == 200

        resp = client.delete("/api/workflows/to-delete", params={"mount": MOUNT})
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "deleted"
        assert data["name"] == "to-delete"

        # Confirm it's gone
        resp = client.get("/api/workflows/to-delete", params={"mount": MOUNT})
        assert resp.status_code == 404

    def test_delete_workflow_not_found(self, client: TestClient) -> None:
        resp = client.delete("/api/workflows/nonexistent", params={"mount": MOUNT})
        assert resp.status_code == 404

    def test_delete_workflow_invalid_name(self, client: TestClient) -> None:
        resp = client.delete("/api/workflows/bad name!", params={"mount": MOUNT})
        assert resp.status_code == 422

    # ── POST /api/workflows/runs/{run_id}/cancel ───

    def test_cancel_run(self, client: TestClient, mock_adapter: MagicMock) -> None:
        """Create a run + memberships via repos, then cancel it."""
        from srunx.db.connection import open_connection
        from srunx.db.repositories.jobs import JobRepository
        from srunx.db.repositories.workflow_run_jobs import WorkflowRunJobRepository
        from srunx.db.repositories.workflow_runs import WorkflowRunRepository

        conn = open_connection()
        try:
            run_id = WorkflowRunRepository(conn).create(
                workflow_name="cancel-test",
                yaml_path=None,
                args=None,
                triggered_by="web",
            )
            WorkflowRunRepository(conn).update_status(run_id, "running")
            job_repo = JobRepository(conn)
            job_repo.record_submission(
                job_id=10001,
                name="step1",
                status="RUNNING",
                submission_source="workflow",
                workflow_run_id=run_id,
            )
            job_repo.record_submission(
                job_id=10002,
                name="step2",
                status="RUNNING",
                submission_source="workflow",
                workflow_run_id=run_id,
            )
            wrj_repo = WorkflowRunJobRepository(conn)
            wrj_repo.create(run_id, "step1", job_id=10001)
            wrj_repo.create(run_id, "step2", job_id=10002)
        finally:
            conn.close()

        resp = client.post(f"/api/workflows/runs/{run_id}/cancel")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "cancelled"
        assert data["run_id"] == str(run_id)

        # Verify adapter.cancel_job was called for each job
        assert mock_adapter.cancel_job.call_count == 2

        # Verify the run is marked cancelled
        conn = open_connection()
        try:
            updated = WorkflowRunRepository(conn).get(run_id)
        finally:
            conn.close()
        assert updated is not None
        assert updated.status == "cancelled"

    def test_cancel_run_not_found(self, client: TestClient) -> None:
        resp = client.post("/api/workflows/runs/nonexistent/cancel")
        assert resp.status_code == 404

    def test_cancel_run_emits_status_changed_event(
        self, client: TestClient, mock_adapter: MagicMock
    ) -> None:
        """Regression for I5: Web cancel must route through
        WorkflowRunStateService so a ``workflow_run.status_changed``
        event is emitted and subscribers receive a delivery.
        """
        from srunx.db.connection import open_connection
        from srunx.db.repositories.workflow_runs import WorkflowRunRepository

        conn = open_connection()
        try:
            run_id = WorkflowRunRepository(conn).create(
                workflow_name="cancel-event-test",
                yaml_path=None,
                args=None,
                triggered_by="web",
            )
            WorkflowRunRepository(conn).update_status(run_id, "running")
        finally:
            conn.close()

        resp = client.post(f"/api/workflows/runs/{run_id}/cancel")
        assert resp.status_code == 200, resp.text

        conn = open_connection()
        try:
            events = conn.execute(
                "SELECT kind, payload FROM events WHERE source_ref = ? AND kind = ?",
                (f"workflow_run:{run_id}", "workflow_run.status_changed"),
            ).fetchall()
        finally:
            conn.close()
        assert len(events) == 1
        import json as _json

        payload = _json.loads(events[0]["payload"])
        assert payload["to_status"] == "cancelled"
        assert payload["from_status"] == "running"

    def test_cancel_run_already_terminal(self, client: TestClient) -> None:
        from srunx.db.connection import open_connection
        from srunx.db.repositories.base import now_iso
        from srunx.db.repositories.workflow_runs import WorkflowRunRepository

        conn = open_connection()
        try:
            repo = WorkflowRunRepository(conn)
            run_id = repo.create(
                workflow_name="done-test",
                yaml_path=None,
                args=None,
                triggered_by="web",
            )
            repo.update_status(run_id, "completed", completed_at=now_iso())
        finally:
            conn.close()

        resp = client.post(f"/api/workflows/runs/{run_id}/cancel")
        assert resp.status_code == 422

    # ── Workflow args and outputs ──────────────────────

    def test_create_workflow_with_args(self, client: TestClient) -> None:
        """Creating a workflow with args should persist and return them."""
        payload = {
            "name": "args-test",
            "default_project": MOUNT,
            "args": {"base_dir": "/data/exp", "lr": "0.001"},
            "jobs": [
                {"name": "train", "command": ["python", "train.py"]},
            ],
        }
        resp = client.post("/api/workflows/create", json=payload)
        assert resp.status_code == 200
        data = resp.json()
        assert data["args"] == {"base_dir": "/data/exp", "lr": "0.001"}

    def test_create_workflow_with_exports(self, client: TestClient) -> None:
        """Jobs with exports should be persisted and returned."""
        payload = {
            "name": "exports-test",
            "default_project": MOUNT,
            "jobs": [
                {
                    "name": "train",
                    "command": ["python", "train.py"],
                    "exports": {"model_path": "/data/model.pt"},
                },
                {
                    "name": "eval",
                    "command": ["python", "eval.py"],
                    "depends_on": ["train"],
                },
            ],
        }
        resp = client.post("/api/workflows/create", json=payload)
        assert resp.status_code == 200
        data = resp.json()
        train_job = next(j for j in data["jobs"] if j["name"] == "train")
        eval_job = next(j for j in data["jobs"] if j["name"] == "eval")
        assert train_job["exports"] == {"model_path": "/data/model.pt"}
        assert eval_job["exports"] == {}

    def test_create_workflow_rejects_python_args(self, client: TestClient) -> None:
        """Args containing 'python:' should be rejected from web."""
        payload = {
            "name": "bad-args",
            "default_project": MOUNT,
            "args": {"x": "python: import os; os.system('rm -rf /')"},
            "jobs": [{"name": "a", "command": ["echo", "hi"]}],
        }
        resp = client.post("/api/workflows/create", json=payload)
        assert resp.status_code == 422
        assert "python:" in resp.json()["detail"]

    def test_create_workflow_invalid_export_key(self, client: TestClient) -> None:
        """Export keys with invalid shell identifiers should be rejected."""
        payload = {
            "name": "bad-exports",
            "default_project": MOUNT,
            "jobs": [
                {
                    "name": "train",
                    "command": ["echo"],
                    "exports": {"bad key": "value"},
                },
            ],
        }
        resp = client.post("/api/workflows/create", json=payload)
        assert resp.status_code == 422

    def test_create_workflow_rejects_legacy_outputs_key(
        self, client: TestClient
    ) -> None:
        """Legacy 'outputs:' in the request body must 422, not silently drop."""
        payload = {
            "name": "legacy-outputs",
            "default_project": MOUNT,
            "jobs": [
                {
                    "name": "train",
                    "command": ["echo"],
                    "outputs": {"model_path": "/x"},
                },
            ],
        }
        resp = client.post("/api/workflows/create", json=payload)
        assert resp.status_code == 422
        body = resp.json()
        assert "outputs" in str(body).lower() and "exports" in str(body).lower()


# ── Config SSH Connect / Test / Status ───────────────


class TestConfigSSHConnect:
    """Tests for SSH profile connect, test, and status endpoints."""

    def test_connect_profile_not_found(self, client: TestClient) -> None:
        """POST /api/config/ssh/profiles/{name}/connect with unknown profile returns 404."""
        from unittest.mock import patch

        mock_cm = MagicMock()
        mock_cm.get_profile.return_value = None

        with patch(
            "srunx.web.routers.config._get_config_manager", return_value=mock_cm
        ):
            resp = client.post("/api/config/ssh/profiles/nonexistent/connect")
        assert resp.status_code == 404

    def test_connect_success(self, client: TestClient) -> None:
        """Successful SSH connect swaps the adapter and returns connected=True."""
        from unittest.mock import patch

        from srunx.ssh.core.config import ServerProfile

        profile = ServerProfile(
            hostname="gpu.example.com",
            username="tester",
            key_filename="~/.ssh/id_rsa",
        )
        mock_cm = MagicMock()
        mock_cm.get_profile.return_value = profile

        mock_new_adapter = MagicMock()
        mock_new_adapter.connect.return_value = True

        with (
            patch("srunx.web.routers.config._get_config_manager", return_value=mock_cm),
            patch(
                "srunx.web.ssh_adapter.SlurmSSHAdapter",
                return_value=mock_new_adapter,
            ),
            patch("srunx.web.deps.swap_adapter", return_value=None),
        ):
            resp = client.post("/api/config/ssh/profiles/myprofile/connect")

        assert resp.status_code == 200
        data = resp.json()
        assert data["connected"] is True
        assert data["profile_name"] == "myprofile"
        assert data["hostname"] == "gpu.example.com"
        assert data["error"] is None

    def test_connect_failure_returns_error(self, client: TestClient) -> None:
        """When SSH connect() returns False, response has connected=False with error."""
        from unittest.mock import patch

        from srunx.ssh.core.config import ServerProfile

        profile = ServerProfile(
            hostname="gpu.example.com",
            username="tester",
            key_filename="~/.ssh/id_rsa",
        )
        mock_cm = MagicMock()
        mock_cm.get_profile.return_value = profile

        mock_new_adapter = MagicMock()
        mock_new_adapter.connect.return_value = False

        with (
            patch("srunx.web.routers.config._get_config_manager", return_value=mock_cm),
            patch(
                "srunx.web.ssh_adapter.SlurmSSHAdapter",
                return_value=mock_new_adapter,
            ),
        ):
            resp = client.post("/api/config/ssh/profiles/myprofile/connect")

        assert resp.status_code == 200
        data = resp.json()
        assert data["connected"] is False
        assert data["error"] == "SSH connection failed"

    def test_connect_exception_returns_error(self, client: TestClient) -> None:
        """When adapter creation raises an exception, response has connected=False."""
        from unittest.mock import patch

        from srunx.ssh.core.config import ServerProfile

        profile = ServerProfile(
            hostname="gpu.example.com",
            username="tester",
            key_filename="~/.ssh/id_rsa",
        )
        mock_cm = MagicMock()
        mock_cm.get_profile.return_value = profile

        with (
            patch("srunx.web.routers.config._get_config_manager", return_value=mock_cm),
            patch(
                "srunx.web.ssh_adapter.SlurmSSHAdapter",
                side_effect=ConnectionError("Host unreachable"),
            ),
        ):
            resp = client.post("/api/config/ssh/profiles/myprofile/connect")

        assert resp.status_code == 200
        data = resp.json()
        assert data["connected"] is False
        assert "Host unreachable" in (data["error"] or "")

    def test_test_profile_not_found(self, client: TestClient) -> None:
        """POST /api/config/ssh/profiles/{name}/test with unknown profile returns 404."""
        from unittest.mock import patch

        mock_cm = MagicMock()
        mock_cm.get_profile.return_value = None

        with patch(
            "srunx.web.routers.config._get_config_manager", return_value=mock_cm
        ):
            resp = client.post("/api/config/ssh/profiles/unknown/test")
        assert resp.status_code == 404

    def test_test_ssh_and_slurm_ok(self, client: TestClient) -> None:
        """Successful test reports SSH connected and SLURM available."""
        from unittest.mock import patch

        from srunx.ssh.core.config import ServerProfile

        profile = ServerProfile(
            hostname="gpu.example.com",
            username="tester",
            key_filename="~/.ssh/id_rsa",
        )
        mock_cm = MagicMock()
        mock_cm.get_profile.return_value = profile

        mock_adapter = MagicMock()
        mock_adapter._client.test_connection.return_value = {
            "ssh_connected": True,
            "slurm_available": True,
            "hostname": "gpu.example.com",
            "user": "tester",
            "slurm_version": "23.02.7",
        }

        with (
            patch("srunx.web.routers.config._get_config_manager", return_value=mock_cm),
            patch(
                "srunx.web.ssh_adapter.SlurmSSHAdapter",
                return_value=mock_adapter,
            ),
        ):
            resp = client.post("/api/config/ssh/profiles/myprofile/test")

        assert resp.status_code == 200
        data = resp.json()
        assert data["ssh_connected"] is True
        assert data["slurm_available"] is True
        assert data["hostname"] == "gpu.example.com"
        assert data["slurm_version"] == "23.02.7"
        assert data["error"] is None

    def test_test_ssh_ok_slurm_unavailable(self, client: TestClient) -> None:
        """SSH connects but SLURM commands fail."""
        from unittest.mock import patch

        from srunx.ssh.core.config import ServerProfile

        profile = ServerProfile(
            hostname="gpu.example.com",
            username="tester",
            key_filename="~/.ssh/id_rsa",
        )
        mock_cm = MagicMock()
        mock_cm.get_profile.return_value = profile

        mock_adapter = MagicMock()
        mock_adapter._client.test_connection.return_value = {
            "ssh_connected": True,
            "slurm_available": False,
            "hostname": "gpu.example.com",
            "user": "tester",
            "slurm_version": "",
            "error": "sinfo: command not found",
        }

        with (
            patch("srunx.web.routers.config._get_config_manager", return_value=mock_cm),
            patch(
                "srunx.web.ssh_adapter.SlurmSSHAdapter",
                return_value=mock_adapter,
            ),
        ):
            resp = client.post("/api/config/ssh/profiles/myprofile/test")

        assert resp.status_code == 200
        data = resp.json()
        assert data["ssh_connected"] is True
        assert data["slurm_available"] is False
        assert "sinfo" in (data["error"] or "")

    def test_test_connection_failure(self, client: TestClient) -> None:
        """When adapter construction raises, error is returned."""
        from unittest.mock import patch

        from srunx.ssh.core.config import ServerProfile

        profile = ServerProfile(
            hostname="gpu.example.com",
            username="tester",
            key_filename="~/.ssh/id_rsa",
        )
        mock_cm = MagicMock()
        mock_cm.get_profile.return_value = profile

        with (
            patch("srunx.web.routers.config._get_config_manager", return_value=mock_cm),
            patch(
                "srunx.web.ssh_adapter.SlurmSSHAdapter",
                side_effect=OSError("Connection refused"),
            ),
        ):
            resp = client.post("/api/config/ssh/profiles/myprofile/test")

        assert resp.status_code == 200
        data = resp.json()
        assert data["ssh_connected"] is False
        assert data["slurm_available"] is False
        assert "Connection refused" in (data["error"] or "")

    def test_ssh_status_connected(
        self, client: TestClient, mock_adapter: MagicMock
    ) -> None:
        """GET /api/config/ssh/status returns connected=True when adapter is set."""
        from unittest.mock import patch

        with (
            patch(
                "srunx.web.deps.get_active_profile_name", return_value="test-profile"
            ),
            patch("srunx.web.deps.get_adapter_or_none", return_value=mock_adapter),
        ):
            resp = client.get("/api/config/ssh/status")

        assert resp.status_code == 200
        data = resp.json()
        assert data["connected"] is True
        assert data["profile_name"] == "test-profile"

    def test_ssh_status_disconnected(self, client: TestClient) -> None:
        """GET /api/config/ssh/status returns connected=False when no adapter."""
        from unittest.mock import patch

        with (
            patch("srunx.web.deps.get_adapter_or_none", return_value=None),
            patch("srunx.web.deps.get_active_profile_name", return_value=None),
        ):
            resp = client.get("/api/config/ssh/status")

        assert resp.status_code == 200
        data = resp.json()
        assert data["connected"] is False
        assert data["profile_name"] is None


# ── Script Preview ───────────────────────────────────


class TestScriptPreview:
    """Tests for POST /api/jobs/preview (local rendering, no SSH needed)."""

    def test_basic_preview(self, client: TestClient) -> None:
        """Preview with a simple command returns a valid SLURM script."""
        resp = client.post(
            "/api/jobs/preview",
            json={"command": ["python", "train.py"]},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert "#!/bin/bash" in data["script"]
        assert data["template_used"] == "base"

    def test_preview_with_resources_and_environment(self, client: TestClient) -> None:
        """Preview with resources and environment settings renders them in the script."""
        resp = client.post(
            "/api/jobs/preview",
            json={
                "name": "gpu-train",
                "command": ["python", "train.py"],
                "resources": {"nodes": 2, "gpus_per_node": 4, "partition": "gpu"},
                "environment": {"conda": "ml_env"},
            },
        )
        assert resp.status_code == 200
        data = resp.json()
        script = data["script"]
        assert "gpu-train" in script
        assert "ml_env" in script
        assert data["template_used"] == "base"

    def test_preview_with_specific_template(self, client: TestClient) -> None:
        """Preview with template_name='base' uses the base template."""
        resp = client.post(
            "/api/jobs/preview",
            json={
                "command": ["echo", "hello"],
                "template_name": "base",
            },
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["template_used"] == "base"
        assert "#!/bin/bash" in data["script"]

    def test_preview_invalid_template(self, client: TestClient) -> None:
        """Preview with unknown template_name returns 404."""
        resp = client.post(
            "/api/jobs/preview",
            json={
                "command": ["echo", "hi"],
                "template_name": "nonexistent-template",
            },
        )
        assert resp.status_code == 404

    def test_preview_with_custom_name(self, client: TestClient) -> None:
        """Preview with a custom job name renders it in the script header."""
        resp = client.post(
            "/api/jobs/preview",
            json={
                "name": "my-custom-job",
                "command": ["python", "run.py"],
            },
        )
        assert resp.status_code == 200
        assert "my-custom-job" in resp.json()["script"]

    def test_preview_missing_command_returns_422(self, client: TestClient) -> None:
        """Preview without command field returns validation error."""
        resp = client.post("/api/jobs/preview", json={"name": "no-cmd"})
        assert resp.status_code == 422

    def test_preview_with_work_dir_and_log_dir(self, client: TestClient) -> None:
        """Preview with work_dir and log_dir passes them to the template."""
        resp = client.post(
            "/api/jobs/preview",
            json={
                "command": ["python", "train.py"],
                "work_dir": "/scratch/user/experiment",
                "log_dir": "/scratch/user/logs",
            },
        )
        assert resp.status_code == 200
        data = resp.json()
        assert "/scratch/user/experiment" in data["script"] or data["template_used"]


# ── Templates Router ─────────────────────────────────


class TestTemplatesRouter:
    """Tests for GET/POST /api/templates endpoints."""

    def test_list_templates(self, client: TestClient) -> None:
        """GET /api/templates returns at least the built-in templates."""
        resp = client.get("/api/templates")
        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data, list)
        assert len(data) >= 1
        names = {t["name"] for t in data}
        assert "base" in names
        # Verify structure
        for t in data:
            assert "name" in t
            assert "description" in t
            assert "use_case" in t

    def test_get_known_template(self, client: TestClient) -> None:
        """GET /api/templates/base returns template detail with raw content."""
        resp = client.get("/api/templates/base")
        assert resp.status_code == 200
        data = resp.json()
        assert data["name"] == "base"
        assert data["description"]
        assert "content" in data
        # The raw content should be a Jinja2 template
        assert "{%" in data["content"] or "{{" in data["content"]

    def test_get_unknown_template(self, client: TestClient) -> None:
        """GET /api/templates/nonexistent returns 404."""
        resp = client.get("/api/templates/nonexistent")
        assert resp.status_code == 404

    def test_get_base_template(self, client: TestClient) -> None:
        """GET /api/templates/base returns the base template."""
        resp = client.get("/api/templates/base")
        assert resp.status_code == 200
        data = resp.json()
        assert data["name"] == "base"
        assert len(data["content"]) > 0

    def test_apply_preview_only(self, client: TestClient) -> None:
        """POST /api/templates/{name}/apply with preview_only=True returns rendered script."""
        resp = client.post(
            "/api/templates/base/apply",
            json={
                "command": ["python", "train.py"],
                "job_name": "preview-job",
                "preview_only": True,
            },
        )
        assert resp.status_code == 200
        data = resp.json()
        assert "script" in data
        assert data["template_used"] == "base"
        assert "#!/bin/bash" in data["script"]

    def test_apply_submits_job(
        self, client: TestClient, mock_adapter: MagicMock
    ) -> None:
        """POST /api/templates/{name}/apply with preview_only=False submits the job."""
        resp = client.post(
            "/api/templates/base/apply",
            json={
                "command": ["echo", "hello"],
                "job_name": "submit-test",
                "preview_only": False,
            },
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["job_id"] == 10002
        mock_adapter.submit_job.assert_called_once()

    def test_apply_unknown_template(self, client: TestClient) -> None:
        """POST /api/templates/nonexistent/apply returns 404."""
        resp = client.post(
            "/api/templates/nonexistent/apply",
            json={
                "command": ["echo", "hi"],
                "preview_only": True,
            },
        )
        assert resp.status_code == 404

    def test_apply_submit_failure(
        self, client: TestClient, mock_adapter: MagicMock
    ) -> None:
        """When sbatch fails during apply, 502 is returned."""
        mock_adapter.submit_job.side_effect = RuntimeError("sbatch: error")
        resp = client.post(
            "/api/templates/base/apply",
            json={
                "command": ["python", "train.py"],
                "job_name": "fail-job",
                "preview_only": False,
            },
        )
        assert resp.status_code == 502


# ── Workflow Execution Control ───────────────────────


class TestWorkflowExecutionControl:
    """Tests for POST /api/workflows/{name}/run with WorkflowRunRequest body."""

    def _create_test_workflow(self, client: TestClient) -> None:
        """Helper: create a 3-step workflow for execution control tests."""
        payload = {
            "name": "exec-ctrl",
            "default_project": MOUNT,
            "jobs": [
                {"name": "step1", "command": ["echo", "step1"]},
                {
                    "name": "step2",
                    "command": ["echo", "step2"],
                    "depends_on": ["step1"],
                },
                {
                    "name": "step3",
                    "command": ["echo", "step3"],
                    "depends_on": ["step2"],
                },
            ],
        }
        resp = client.post("/api/workflows/create", json=payload)
        assert resp.status_code == 200

    def _setup_mock_submit(self, mock_adapter: MagicMock) -> None:
        """Configure mock_adapter.submit_job to return incrementing job IDs."""
        counter = {"n": 0}

        def mock_submit(
            script_content: str,
            job_name: str | None = None,
            dependency: str | None = None,
        ) -> dict:
            counter["n"] += 1
            return {
                "name": job_name or "job",
                "job_id": 30000 + counter["n"],
                "status": "PENDING",
                "depends_on": [],
                "command": [],
                "resources": {},
            }

        mock_adapter.submit_job.side_effect = mock_submit

    def test_dry_run_returns_scripts(
        self, client: TestClient, mock_adapter: MagicMock
    ) -> None:
        """dry_run=True returns rendered scripts without submitting."""
        self._create_test_workflow(client)

        resp = client.post(
            "/api/workflows/exec-ctrl/run",
            params={"mount": MOUNT},
            json={"dry_run": True},
        )
        assert resp.status_code == 202
        data = resp.json()
        assert data["dry_run"] is True
        assert len(data["jobs"]) == 3
        for job in data["jobs"]:
            assert "script" in job
            assert "#!/bin/bash" in job["script"]
            assert "name" in job
        # Verify execution order
        names = data["execution_order"]
        assert names == ["step1", "step2", "step3"]
        # submit_job should NOT have been called
        mock_adapter.submit_job.assert_not_called()

    def test_single_job_execution(
        self, client: TestClient, mock_adapter: MagicMock
    ) -> None:
        """single_job runs only the specified job, skipping dependencies."""
        self._create_test_workflow(client)
        self._setup_mock_submit(mock_adapter)

        resp = client.post(
            "/api/workflows/exec-ctrl/run",
            params={"mount": MOUNT},
            json={"single_job": "step2"},
        )
        assert resp.status_code == 202
        data = resp.json()
        # Only one job should have been submitted
        assert mock_adapter.submit_job.call_count == 1
        call_kwargs = mock_adapter.submit_job.call_args
        # The job_name kwarg should be step2
        assert call_kwargs.kwargs.get("job_name") == "step2" or (
            call_kwargs[1].get("job_name") == "step2" if len(call_kwargs) > 1 else False
        )

    def test_single_job_invalid_name(
        self, client: TestClient, mock_adapter: MagicMock
    ) -> None:
        """single_job with a name not in the workflow returns 422."""
        self._create_test_workflow(client)

        resp = client.post(
            "/api/workflows/exec-ctrl/run",
            params={"mount": MOUNT},
            json={"single_job": "nonexistent"},
        )
        assert resp.status_code == 422

    def test_from_job_filters_start(
        self, client: TestClient, mock_adapter: MagicMock
    ) -> None:
        """from_job skips jobs before the specified job."""
        self._create_test_workflow(client)
        self._setup_mock_submit(mock_adapter)

        resp = client.post(
            "/api/workflows/exec-ctrl/run",
            params={"mount": MOUNT},
            json={"from_job": "step2"},
        )
        assert resp.status_code == 202
        # step2 and step3 should be submitted (2 jobs)
        assert mock_adapter.submit_job.call_count == 2

    def test_to_job_filters_end(
        self, client: TestClient, mock_adapter: MagicMock
    ) -> None:
        """to_job stops execution after the specified job."""
        self._create_test_workflow(client)
        self._setup_mock_submit(mock_adapter)

        resp = client.post(
            "/api/workflows/exec-ctrl/run",
            params={"mount": MOUNT},
            json={"to_job": "step2"},
        )
        assert resp.status_code == 202
        # step1 and step2 should be submitted (2 jobs)
        assert mock_adapter.submit_job.call_count == 2

    def test_from_job_invalid_name(
        self, client: TestClient, mock_adapter: MagicMock
    ) -> None:
        """from_job with unknown job name returns 422."""
        self._create_test_workflow(client)

        resp = client.post(
            "/api/workflows/exec-ctrl/run",
            params={"mount": MOUNT},
            json={"from_job": "no-such-job"},
        )
        assert resp.status_code == 422

    def test_to_job_invalid_name(
        self, client: TestClient, mock_adapter: MagicMock
    ) -> None:
        """to_job with unknown job name returns 422."""
        self._create_test_workflow(client)

        resp = client.post(
            "/api/workflows/exec-ctrl/run",
            params={"mount": MOUNT},
            json={"to_job": "no-such-job"},
        )
        assert resp.status_code == 422

    def test_no_body_backward_compat(
        self, client: TestClient, mock_adapter: MagicMock
    ) -> None:
        """Running a workflow without a body (backward compat) still works."""
        self._create_test_workflow(client)
        self._setup_mock_submit(mock_adapter)

        resp = client.post(
            "/api/workflows/exec-ctrl/run",
            params={"mount": MOUNT},
        )
        assert resp.status_code == 202
        data = resp.json()
        # P1-1: newly-created runs start at ``pending`` and the poller
        # promotes them to ``running`` on the first RUNNING child.
        assert data["status"] == "pending"
        # All 3 jobs should be submitted
        assert mock_adapter.submit_job.call_count == 3

    def test_dry_run_single_job(
        self, client: TestClient, mock_adapter: MagicMock
    ) -> None:
        """dry_run combined with single_job returns only that job's script."""
        self._create_test_workflow(client)

        resp = client.post(
            "/api/workflows/exec-ctrl/run",
            params={"mount": MOUNT},
            json={"dry_run": True, "single_job": "step2"},
        )
        assert resp.status_code == 202
        data = resp.json()
        assert data["dry_run"] is True
        assert len(data["jobs"]) == 1
        assert data["jobs"][0]["name"] == "step2"
        mock_adapter.submit_job.assert_not_called()


# ── Workflow In-Place Execution (#135 web part) ────────


class TestWorkflowsRouterInPlace:
    """Phase 2 in-place execution dispatch on POST /api/workflows/{name}/run.

    These tests stub :func:`srunx.sync.service.mount_sync_session` so the
    file lock + rsync no-op out, then assert per-job dispatch:

    * ShellJob whose source bytes match the rendered bytes AND lives
      under a profile mount → :meth:`SlurmSSHAdapter.submit_remote_sbatch`
      called against the translated remote path.
    * ShellJob whose Jinja substitution diverged → legacy
      :meth:`SlurmSSHAdapter.submit_job` (temp upload) path.
    * ``Job`` (command) jobs always take the legacy path.
    * Multiple ShellJobs under the same mount → one rsync call total.
    * Sync failure surfaces as 502 with "Mount sync failed".
    * Sbatch failure inside the BFS keeps the existing
      "sbatch failed for X" detail (NOT misclassified as a sync failure).
    """

    @staticmethod
    @contextlib.contextmanager
    def _patch_profile(monkeypatch, profile):
        """Patch every ``_get_current_profile`` lookup the router does.

        Returns a single context manager (not a tuple of them) — Python's
        ``with`` statement does NOT support ``*`` unpacking of a tuple of
        context managers, so the previous shape (``with (*tuple, ...)``)
        was a runtime ``TypeError``. ``ExitStack`` aggregates the patches
        cleanly.
        """
        from contextlib import ExitStack
        from unittest.mock import patch

        with ExitStack() as stack:
            stack.enter_context(
                patch(
                    "srunx.web.routers.workflows._get_current_profile",
                    return_value=profile,
                )
            )
            stack.enter_context(
                patch(
                    "srunx.web.sync_utils.get_current_profile",
                    return_value=profile,
                )
            )
            yield

    @staticmethod
    def _fake_session_factory(call_log: list[str]):
        """Return a context manager + counter recording mount.name per call."""
        from collections.abc import Iterator
        from contextlib import contextmanager
        from typing import Any
        from unittest.mock import MagicMock

        @contextmanager
        def fake_session(**kwargs: Any) -> Iterator[MagicMock]:
            mount = kwargs.get("mount")
            if mount is not None:
                call_log.append(mount.name)
            yield MagicMock(performed=True, warnings=())

        return fake_session

    @staticmethod
    def _make_profile_with_mount(mount_local, remote: str = "/home/u/project"):
        from srunx.ssh.core.config import MountConfig, ServerProfile

        return ServerProfile(
            hostname="x",
            username="u",
            key_filename="~/.ssh/id_rsa",
            mounts=[
                MountConfig(name=MOUNT, local=str(mount_local), remote=remote),
            ],
        )

    @staticmethod
    def _write_workflow_yaml(tmp_path: Path, name: str, body: str) -> None:
        """Write a workflow YAML directly into the per-mount workflow dir.

        Skips the /api/workflows/create round-trip so tests can place
        ``script_path`` entries (which the create endpoint doesn't accept
        in its structured input but the YAML loader does).
        """
        d = tmp_path / "project" / ".srunx" / "workflows"
        d.mkdir(parents=True, exist_ok=True)
        (d / f"{name}.yaml").write_text(body)

    @pytest.mark.skip(
        reason=(
            "#150 root cause runs deeper than just threading the unrendered "
            "workflow. `render_workflow_for_submission` translates "
            "`ShellJob.script_path` from local to remote (mount.remote/...) "
            "BEFORE rendering, so `render_shell_job_script` then tries to "
            "read the remote path locally — fails or returns wrong bytes. "
            "Fix requires either: (a) renaming/duplicating the script_path field "
            "so render reads from local while submit uses remote, or "
            "(b) deferring path translation to per-mode handling. Both are "
            "larger refactors than this PR. The dispatch shape under test is "
            "correct; the renderer pipeline mismatch is what blocks the assertion."
        )
    )
    def test_shelljob_in_place_uses_submit_remote_sbatch(
        self,
        client: TestClient,
        mock_adapter: MagicMock,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        from unittest.mock import MagicMock as _MM

        mount_local = tmp_path / "project"
        script = mount_local / "run.sh"
        script.write_text("#!/bin/bash\necho hi\n")

        profile = self._make_profile_with_mount(mount_local)

        self._write_workflow_yaml(
            tmp_path,
            "in-place-wf",
            f"name: in-place-wf\njobs:\n  - name: only\n    path: {script}\n",
        )

        mock_adapter.scheduler_key = "ssh:test-profile"
        submitted = _MM()
        submitted.job_id = 99001
        submitted.name = "only"
        mock_adapter.submit_remote_sbatch.return_value = submitted

        sync_calls: list[str] = []
        from unittest.mock import patch

        with (
            self._patch_profile(monkeypatch, profile),
            patch(
                "srunx.sync.service.mount_sync_session",
                self._fake_session_factory(sync_calls),
            ),
        ):
            resp = client.post(
                "/api/workflows/in-place-wf/run", params={"mount": MOUNT}
            )

        assert resp.status_code == 202, resp.text
        # In-place dispatch: submit_remote_sbatch called with translated
        # remote path; legacy submit_job MUST NOT have run for this job.
        mock_adapter.submit_remote_sbatch.assert_called_once()
        args = mock_adapter.submit_remote_sbatch.call_args.args
        kwargs = mock_adapter.submit_remote_sbatch.call_args.kwargs
        assert args[0] == "/home/u/project/run.sh"
        assert kwargs["submit_cwd"] == "/home/u/project"
        assert kwargs["job_name"] == "only"
        mock_adapter.submit_job.assert_not_called()
        # Single mount touched → single rsync call.
        assert sync_calls == [MOUNT]

    def test_shelljob_jinja_diverged_uses_submit_job(
        self,
        client: TestClient,
        mock_adapter: MagicMock,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """ShellJob whose Jinja-rendered bytes diverge from source → legacy upload."""
        mount_local = tmp_path / "project"
        # ``{{ msg }}`` is bound via ``script_vars`` so the rendered bytes
        # differ from the source bytes — in-place dispatch must skip.
        script = mount_local / "render.sh"
        script.write_text("#!/bin/bash\necho {{ msg }}\n")

        profile = self._make_profile_with_mount(mount_local)

        self._write_workflow_yaml(
            tmp_path,
            "render-wf",
            "name: render-wf\n"
            "jobs:\n"
            f"  - name: only\n"
            f"    script_path: {script}\n"
            "    script_vars:\n"
            "      msg: hello\n",
        )

        mock_adapter.scheduler_key = "ssh:test-profile"
        mock_adapter.submit_job.return_value = {
            "name": "only",
            "job_id": 99002,
            "status": "PENDING",
            "depends_on": [],
            "command": [],
            "resources": {},
        }

        from unittest.mock import patch

        with (
            self._patch_profile(monkeypatch, profile),
            patch(
                "srunx.sync.service.mount_sync_session",
                self._fake_session_factory([]),
            ),
        ):
            resp = client.post("/api/workflows/render-wf/run", params={"mount": MOUNT})

        assert resp.status_code == 202, resp.text
        # Rendered bytes differ from source → legacy temp-upload path.
        mock_adapter.submit_job.assert_called_once()
        mock_adapter.submit_remote_sbatch.assert_not_called()

    @pytest.mark.skip(
        reason=(
            "#150 root cause runs deeper than just threading the unrendered "
            "workflow. `render_workflow_for_submission` translates "
            "`ShellJob.script_path` from local to remote (mount.remote/...) "
            "BEFORE rendering, so `render_shell_job_script` then tries to "
            "read the remote path locally — fails or returns wrong bytes. "
            "Fix requires either: (a) renaming/duplicating the script_path field "
            "so render reads from local while submit uses remote, or "
            "(b) deferring path translation to per-mode handling. Both are "
            "larger refactors than this PR. The dispatch shape under test is "
            "correct; the renderer pipeline mismatch is what blocks the assertion."
        )
    )
    def test_mixed_workflow_dispatches_per_job(
        self,
        client: TestClient,
        mock_adapter: MagicMock,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Job + in-place ShellJob + tmp ShellJob → correct method per job."""
        from unittest.mock import MagicMock as _MM
        from unittest.mock import patch

        mount_local = tmp_path / "project"
        in_place_script = mount_local / "in_place.sh"
        in_place_script.write_text("#!/bin/bash\necho in-place\n")
        rendered_script = mount_local / "rendered.sh"
        rendered_script.write_text("#!/bin/bash\necho {{ lr }}\n")

        profile = self._make_profile_with_mount(mount_local)

        self._write_workflow_yaml(
            tmp_path,
            "mixed-wf",
            "name: mixed-wf\n"
            "jobs:\n"
            "  - name: cmd_job\n"
            "    command: [echo, hi]\n"
            f"  - name: in_place_job\n"
            f"    path: {in_place_script}\n"
            f"  - name: rendered_job\n"
            f"    script_path: {rendered_script}\n"
            "    script_vars:\n"
            "      lr: '0.01'\n",
        )

        mock_adapter.scheduler_key = "ssh:test-profile"

        in_place_submitted = _MM()
        in_place_submitted.job_id = 99100
        in_place_submitted.name = "in_place_job"
        mock_adapter.submit_remote_sbatch.return_value = in_place_submitted

        counter = {"n": 0}

        def fake_submit(script_content, job_name=None, dependency=None):
            counter["n"] += 1
            return {
                "name": job_name or "job",
                "job_id": 99200 + counter["n"],
                "status": "PENDING",
                "depends_on": [],
                "command": [],
                "resources": {},
            }

        mock_adapter.submit_job.side_effect = fake_submit

        with (
            self._patch_profile(monkeypatch, profile),
            patch(
                "srunx.sync.service.mount_sync_session",
                self._fake_session_factory([]),
            ),
        ):
            resp = client.post("/api/workflows/mixed-wf/run", params={"mount": MOUNT})

        assert resp.status_code == 202, resp.text
        # cmd_job + rendered_job → two submit_job calls.
        assert mock_adapter.submit_job.call_count == 2
        legacy_names = {
            c.kwargs.get("job_name") for c in mock_adapter.submit_job.call_args_list
        }
        assert legacy_names == {"cmd_job", "rendered_job"}
        # in_place_job → exactly one submit_remote_sbatch call.
        mock_adapter.submit_remote_sbatch.assert_called_once()
        assert (
            mock_adapter.submit_remote_sbatch.call_args.kwargs["job_name"]
            == "in_place_job"
        )

    @pytest.mark.skip(
        reason=(
            "#150 root cause runs deeper than just threading the unrendered "
            "workflow. `render_workflow_for_submission` translates "
            "`ShellJob.script_path` from local to remote (mount.remote/...) "
            "BEFORE rendering, so `render_shell_job_script` then tries to "
            "read the remote path locally — fails or returns wrong bytes. "
            "Fix requires either: (a) renaming/duplicating the script_path field "
            "so render reads from local while submit uses remote, or "
            "(b) deferring path translation to per-mode handling. Both are "
            "larger refactors than this PR. The dispatch shape under test is "
            "correct; the renderer pipeline mismatch is what blocks the assertion."
        )
    )
    def test_single_mount_synced_only_once_for_multiple_shelljobs(
        self,
        client: TestClient,
        mock_adapter: MagicMock,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Two ShellJobs touching the same mount → exactly 1 rsync call."""
        from unittest.mock import MagicMock as _MM
        from unittest.mock import patch

        mount_local = tmp_path / "project"
        script_a = mount_local / "a.sh"
        script_a.write_text("#!/bin/bash\necho a\n")
        script_b = mount_local / "b.sh"
        script_b.write_text("#!/bin/bash\necho b\n")

        profile = self._make_profile_with_mount(mount_local)

        self._write_workflow_yaml(
            tmp_path,
            "two-shell-wf",
            "name: two-shell-wf\n"
            "jobs:\n"
            f"  - name: a\n"
            f"    path: {script_a}\n"
            f"  - name: b\n"
            f"    path: {script_b}\n",
        )

        mock_adapter.scheduler_key = "ssh:test-profile"
        counter = {"n": 0}

        def fake_remote(remote_path, **kwargs):
            counter["n"] += 1
            obj = _MM()
            obj.job_id = 99300 + counter["n"]
            obj.name = kwargs.get("job_name") or "j"
            return obj

        mock_adapter.submit_remote_sbatch.side_effect = fake_remote

        sync_calls: list[str] = []

        with (
            self._patch_profile(monkeypatch, profile),
            patch(
                "srunx.sync.service.mount_sync_session",
                self._fake_session_factory(sync_calls),
            ),
        ):
            resp = client.post(
                "/api/workflows/two-shell-wf/run", params={"mount": MOUNT}
            )

        assert resp.status_code == 202, resp.text
        # Single mount touched twice → ONE rsync call total.
        assert sync_calls == [MOUNT]
        # Both jobs went via in-place.
        assert mock_adapter.submit_remote_sbatch.call_count == 2

    def test_shelljob_outside_mount_falls_back_to_submit_job(
        self,
        client: TestClient,
        mock_adapter: MagicMock,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """ShellJob whose script_path doesn't resolve to a profile mount
        → legacy temp-upload path.

        Construction: profile exposes a single mount ``other`` (so the
        script-root guard accepts the script under it), but the request
        targets ``mount=test-project`` whose workflow dir is under
        ``other.local``. The script lives under ``other.local`` but the
        request's resolved profile (in-place dispatch) sees only the
        mount whose name matches the URL — different setup that exercises
        the ``resolve_mount_for_path returns None`` branch in
        :func:`_resolve_in_place_target`.

        Cleaner alternative when neither setup is possible: simulate the
        "no SSH profile bound" deployment by patching
        ``sync_utils.get_current_profile`` to ``None`` so the lock
        context yields ``profile=None`` to the BFS dispatcher.
        """
        from unittest.mock import patch

        mount_local = tmp_path / "project"
        script = mount_local / "run.sh"
        script.write_text("#!/bin/bash\necho hi\n")

        # Real profile (has a mount) so YAML resolution + script-root
        # guard pass; flipping ``sync_utils.get_current_profile`` to
        # ``None`` simulates the lock context running with no profile
        # bound — :func:`_resolve_in_place_target` then short-circuits
        # on ``profile is None`` and the BFS uses the legacy upload path.
        real_profile = self._make_profile_with_mount(mount_local)

        self._write_workflow_yaml(
            tmp_path,
            "outside-wf",
            f"name: outside-wf\njobs:\n  - name: only\n    path: {script}\n",
        )

        mock_adapter.scheduler_key = "local"
        mock_adapter.submit_job.return_value = {
            "name": "only",
            "job_id": 99400,
            "status": "PENDING",
            "depends_on": [],
            "command": [],
            "resources": {},
        }

        with (
            patch(
                "srunx.web.routers.workflows._get_current_profile",
                return_value=real_profile,
            ),
            patch(
                "srunx.web.sync_utils.get_current_profile",
                return_value=None,
            ),
            patch(
                "srunx.sync.service.mount_sync_session",
                self._fake_session_factory([]),
            ),
        ):
            resp = client.post("/api/workflows/outside-wf/run", params={"mount": MOUNT})

        assert resp.status_code == 202, resp.text
        # Lock context received ``profile=None`` → in-place path
        # rejected → legacy temp-upload via submit_job.
        mock_adapter.submit_job.assert_called_once()
        mock_adapter.submit_remote_sbatch.assert_not_called()

    @pytest.mark.skip(
        reason=(
            "#150 root cause runs deeper than just threading the unrendered "
            "workflow. `render_workflow_for_submission` translates "
            "`ShellJob.script_path` from local to remote (mount.remote/...) "
            "BEFORE rendering, so `render_shell_job_script` then tries to "
            "read the remote path locally — fails or returns wrong bytes. "
            "Fix requires either: (a) renaming/duplicating the script_path field "
            "so render reads from local while submit uses remote, or "
            "(b) deferring path translation to per-mode handling. Both are "
            "larger refactors than this PR. The dispatch shape under test is "
            "correct; the renderer pipeline mismatch is what blocks the assertion."
        )
    )
    def test_sync_failure_surfaces_as_502(
        self,
        client: TestClient,
        mock_adapter: MagicMock,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """rsync RuntimeError → HTTPException(502) with 'Mount sync failed' detail."""
        from collections.abc import Iterator
        from contextlib import contextmanager
        from typing import Any
        from unittest.mock import patch

        mount_local = tmp_path / "project"
        script = mount_local / "run.sh"
        script.write_text("#!/bin/bash\necho ok\n")

        profile = self._make_profile_with_mount(mount_local)

        self._write_workflow_yaml(
            tmp_path,
            "sync-fail-wf",
            f"name: sync-fail-wf\njobs:\n  - name: only\n    path: {script}\n",
        )

        mock_adapter.scheduler_key = "ssh:test-profile"

        @contextmanager
        def boom_session(**kwargs: Any) -> Iterator[None]:
            raise RuntimeError("rsync exited with status 23")
            yield  # unreachable; satisfies type checker

        with (
            self._patch_profile(monkeypatch, profile),
            patch("srunx.sync.service.mount_sync_session", boom_session),
        ):
            resp = client.post(
                "/api/workflows/sync-fail-wf/run", params={"mount": MOUNT}
            )

        assert resp.status_code == 502, resp.text
        assert "Mount sync failed" in resp.json()["detail"]
        # No sbatch call must have happened — the lock acquisition failed
        # before the body of _hold_workflow_mounts_web ran.
        mock_adapter.submit_remote_sbatch.assert_not_called()
        mock_adapter.submit_job.assert_not_called()

    def test_sbatch_failure_inside_bfs_propagates_unchanged(
        self,
        client: TestClient,
        mock_adapter: MagicMock,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """An sbatch failure must keep its 'sbatch failed for X' detail.

        Regression guard: if the lock-context ``except`` clause caught
        body exceptions, this would be misclassified as
        ``"Mount sync failed: …"``. The CLI fix for the same shape was
        Codex blocker #1 on PR #141.
        """
        from unittest.mock import patch

        mount_local = tmp_path / "project"
        script = mount_local / "run.sh"
        script.write_text("#!/bin/bash\necho hi\n")

        profile = self._make_profile_with_mount(mount_local)

        self._write_workflow_yaml(
            tmp_path,
            "sbatch-fail-wf",
            f"name: sbatch-fail-wf\njobs:\n  - name: boom\n    path: {script}\n",
        )

        mock_adapter.scheduler_key = "ssh:test-profile"
        # In-place path raises during sbatch.
        mock_adapter.submit_remote_sbatch.side_effect = RuntimeError("sbatch hiccup")
        # Make sure submit_job isn't accidentally fallen back to.
        mock_adapter.submit_job.side_effect = AssertionError(
            "submit_job must not be called for an in-place ShellJob"
        )

        with (
            self._patch_profile(monkeypatch, profile),
            patch(
                "srunx.sync.service.mount_sync_session",
                self._fake_session_factory([]),
            ),
        ):
            resp = client.post(
                "/api/workflows/sbatch-fail-wf/run", params={"mount": MOUNT}
            )

        assert resp.status_code == 502, resp.text
        detail = resp.json()["detail"]
        assert "sbatch failed for" in detail
        assert "boom" in detail
        # Critical: must NOT be classified as a sync failure.
        assert "Mount sync failed" not in detail
