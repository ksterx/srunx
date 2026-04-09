"""Tests for templates router: /api/templates/*"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from srunx.web.app import create_app
from srunx.web.deps import get_adapter

FAKE_TEMPLATES = [
    {
        "name": "base",
        "description": "Base SLURM template",
        "use_case": "All job types",
    },
    {
        "name": "custom-gpu",
        "description": "Custom GPU template",
        "use_case": "GPU training",
        "user_defined": "true",
    },
]

FAKE_TEMPLATE_INFO = {
    "name": "base",
    "description": "Base SLURM template",
    "use_case": "All job types",
}


@pytest.fixture
def mock_adapter() -> MagicMock:
    adapter = MagicMock()
    adapter.submit_job.return_value = {
        "name": "test-job",
        "job_id": 10001,
        "status": "PENDING",
    }
    return adapter


@pytest.fixture
def client(mock_adapter: MagicMock, tmp_path):  # type: ignore[misc]
    import srunx.web.config as config_mod

    original = config_mod._config
    config_mod._config = None
    config_mod.get_web_config()

    app = create_app()
    app.dependency_overrides[get_adapter] = lambda: mock_adapter

    yield TestClient(app, raise_server_exceptions=False)

    app.dependency_overrides.clear()
    config_mod._config = original


class TestListTemplates:
    @patch("srunx.web.routers.templates.list_templates", return_value=FAKE_TEMPLATES)
    def test_list_templates(self, mock_list, client: TestClient) -> None:
        resp = client.get("/api/templates")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 2
        assert data[0]["name"] == "base"
        assert data[0]["user_defined"] is False
        assert data[1]["name"] == "custom-gpu"
        assert data[1]["user_defined"] is True


class TestGetTemplate:
    @patch(
        "srunx.web.routers.templates.get_template_info", return_value=FAKE_TEMPLATE_INFO
    )
    @patch("srunx.web.routers.templates.get_template_path")
    def test_get_template(
        self, mock_path, mock_info, client: TestClient, tmp_path
    ) -> None:
        template_file = tmp_path / "base.slurm.jinja"
        template_file.write_text("#!/bin/bash\n#SBATCH --job-name={{ name }}")
        mock_path.return_value = str(template_file)

        resp = client.get("/api/templates/base")
        assert resp.status_code == 200
        data = resp.json()
        assert data["name"] == "base"
        assert "#SBATCH" in data["content"]

    @patch(
        "srunx.web.routers.templates.get_template_info",
        side_effect=ValueError("Template 'nope' not found"),
    )
    def test_get_template_not_found(self, mock_info, client: TestClient) -> None:
        resp = client.get("/api/templates/nope")
        assert resp.status_code == 404
        assert "not found" in resp.json()["detail"]


class TestApplyTemplate:
    @patch("srunx.web.routers.templates.get_template_path")
    @patch("srunx.models.render_job_script")
    def test_apply_template_preview_only(
        self, mock_render, mock_path, client: TestClient, tmp_path
    ) -> None:
        # render_job_script is imported inside the endpoint body from srunx.models
        script_file = tmp_path / "rendered.sh"
        script_file.write_text("#!/bin/bash\necho hello")
        mock_path.return_value = str(tmp_path / "base.slurm.jinja")
        mock_render.return_value = str(script_file)

        resp = client.post(
            "/api/templates/base/apply",
            json={
                "command": ["python", "train.py"],
                "job_name": "test-job",
                "preview_only": True,
            },
        )
        assert resp.status_code == 200
        data = resp.json()
        assert "script" in data
        assert data["template_used"] == "base"

    @patch(
        "srunx.web.routers.templates.get_template_path",
        side_effect=ValueError("Template 'nope' not found"),
    )
    def test_apply_template_not_found(self, mock_path, client: TestClient) -> None:
        resp = client.post(
            "/api/templates/nope/apply",
            json={"command": ["echo", "hi"], "preview_only": True},
        )
        assert resp.status_code == 404


class TestCreateTemplate:
    @patch("srunx.template.create_user_template")
    def test_create_template(self, mock_create, client: TestClient) -> None:
        mock_create.return_value = {
            "name": "my-template",
            "description": "My custom template",
            "use_case": "Testing",
        }
        resp = client.post(
            "/api/templates",
            json={
                "name": "my-template",
                "description": "My custom template",
                "use_case": "Testing",
                "content": "#!/bin/bash\necho test",
            },
        )
        assert resp.status_code == 201
        data = resp.json()
        assert data["name"] == "my-template"

    @patch(
        "srunx.template.create_user_template",
        side_effect=ValueError("Template 'dup' already exists."),
    )
    def test_create_template_conflict(self, mock_create, client: TestClient) -> None:
        resp = client.post(
            "/api/templates",
            json={
                "name": "dup",
                "description": "Duplicate",
                "use_case": "Testing",
                "content": "#!/bin/bash",
            },
        )
        assert resp.status_code == 409

    def test_create_template_missing_fields(self, client: TestClient) -> None:
        resp = client.post("/api/templates", json={"name": "incomplete"})
        assert resp.status_code == 422


class TestDeleteTemplate:
    @patch("srunx.template.delete_user_template")
    def test_delete_template(self, mock_delete, client: TestClient) -> None:
        resp = client.delete("/api/templates/my-template")
        assert resp.status_code == 204
        mock_delete.assert_called_once_with("my-template")

    @patch(
        "srunx.template.delete_user_template",
        side_effect=ValueError("Cannot delete built-in template 'base'."),
    )
    def test_delete_builtin_template_fails(
        self, mock_delete, client: TestClient
    ) -> None:
        resp = client.delete("/api/templates/base")
        assert resp.status_code == 400
        assert "built-in" in resp.json()["detail"]
