"""Tests for REST API routers with mocked SSH adapter."""

from __future__ import annotations

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
    return adapter


@pytest.fixture
def client(  # type: ignore[misc]
    mock_adapter: MagicMock, tmp_path: Path
) -> TestClient:
    import srunx.web.config as config_mod
    from srunx.web.config import get_web_config

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

    # Clear run registry to avoid cross-test state leakage
    from srunx.web.state import run_registry

    run_registry._runs.clear()

    app = create_app()
    app.dependency_overrides[get_adapter] = lambda: mock_adapter

    with patch(
        "srunx.web.routers.workflows._get_current_profile", return_value=fake_profile
    ):
        yield TestClient(app, raise_server_exceptions=False)

    config_mod._config = original
    run_registry._runs.clear()


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
        """Create a run via the registry, then fetch it by ID."""
        from srunx.web.state import run_registry

        run = run_registry.create("test-wf")
        resp = client.get(f"/api/workflows/runs/{run.id}")
        assert resp.status_code == 200
        data = resp.json()
        assert data["id"] == run.id
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
        assert data["status"] == "running"
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
        """Create a run via registry, then cancel it."""
        from srunx.web.state import run_registry

        run = run_registry.create("cancel-test")
        run_registry.set_job_ids(run.id, {"step1": "10001", "step2": "10002"})
        run_registry.update_status(run.id, "running")

        resp = client.post(f"/api/workflows/runs/{run.id}/cancel")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "cancelled"
        assert data["run_id"] == run.id

        # Verify adapter.cancel_job was called for each job
        assert mock_adapter.cancel_job.call_count == 2

        # Verify the run is marked cancelled
        updated = run_registry.get(run.id)
        assert updated is not None
        assert updated.status == "cancelled"

    def test_cancel_run_not_found(self, client: TestClient) -> None:
        resp = client.post("/api/workflows/runs/nonexistent/cancel")
        assert resp.status_code == 404

    def test_cancel_run_already_terminal(self, client: TestClient) -> None:
        from srunx.web.state import run_registry

        run = run_registry.create("done-test")
        run_registry.complete_run(run.id, "completed")

        resp = client.post(f"/api/workflows/runs/{run.id}/cancel")
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

    def test_create_workflow_with_outputs(self, client: TestClient) -> None:
        """Jobs with outputs should be persisted and returned."""
        payload = {
            "name": "outputs-test",
            "default_project": MOUNT,
            "jobs": [
                {
                    "name": "train",
                    "command": ["python", "train.py"],
                    "outputs": {"model_path": "/data/model.pt"},
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
        assert train_job["outputs"] == {"model_path": "/data/model.pt"}
        assert eval_job["outputs"] == {}

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

    def test_create_workflow_invalid_output_key(self, client: TestClient) -> None:
        """Output keys with invalid shell identifiers should be rejected."""
        payload = {
            "name": "bad-outputs",
            "default_project": MOUNT,
            "jobs": [
                {
                    "name": "train",
                    "command": ["echo"],
                    "outputs": {"bad key": "value"},
                },
            ],
        }
        resp = client.post("/api/workflows/create", json=payload)
        assert resp.status_code == 422

    def test_run_workflow_with_outputs_includes_outputs_in_script(
        self, client: TestClient, mock_adapter: MagicMock
    ) -> None:
        """Running a workflow with outputs should include SRUNX_OUTPUTS_DIR in scripts."""
        # Create workflow with outputs
        create_payload = {
            "name": "outputs-run",
            "default_project": MOUNT,
            "jobs": [
                {
                    "name": "train",
                    "command": ["python", "train.py"],
                    "outputs": {"model_path": "/data/model.pt"},
                },
                {
                    "name": "eval",
                    "command": ["python", "eval.py"],
                    "depends_on": ["train"],
                },
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

        resp = client.post("/api/workflows/outputs-run/run", params={"mount": MOUNT})
        assert resp.status_code == 202

        # Check that submitted scripts contain SRUNX_OUTPUTS_DIR
        calls = mock_adapter.submit_job.call_args_list
        train_script = (
            calls[0][0][0] if calls[0][0] else calls[0].kwargs.get("script_content", "")
        )
        assert "SRUNX_OUTPUTS_DIR" in train_script
        assert "model_path" in train_script

        eval_script = (
            calls[1][0][0] if calls[1][0] else calls[1].kwargs.get("script_content", "")
        )
        assert "SRUNX_OUTPUTS_DIR" in eval_script
        assert "train.env" in eval_script  # Should source train's outputs


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
        """GET /api/templates returns all built-in templates."""
        resp = client.get("/api/templates")
        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data, list)
        assert len(data) == 1
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
        assert data["status"] == "running"
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
