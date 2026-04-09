"""Tests for files router: /api/files/*"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from srunx.ssh.core.config import MountConfig, ServerProfile
from srunx.web.app import create_app
from srunx.web.deps import get_adapter


def _make_profile(tmp_path) -> ServerProfile:
    """Create a fake ServerProfile with a mount pointing to tmp_path."""
    mount_local = tmp_path / "project"
    mount_local.mkdir(exist_ok=True)
    mount = MountConfig(
        name="test-project", local=str(mount_local), remote="/home/user/project"
    )
    return ServerProfile(
        hostname="test.example.com",
        username="tester",
        key_filename="~/.ssh/id_rsa",
        mounts=[mount],
    )


@pytest.fixture
def mock_adapter() -> MagicMock:
    return MagicMock()


@pytest.fixture
def fake_profile(tmp_path) -> ServerProfile:
    return _make_profile(tmp_path)


@pytest.fixture
def client(mock_adapter: MagicMock, fake_profile: ServerProfile):  # type: ignore[misc]
    import srunx.web.config as config_mod

    original = config_mod._config
    config_mod._config = None
    config_mod.get_web_config()

    app = create_app()
    app.dependency_overrides[get_adapter] = lambda: mock_adapter

    profile = fake_profile

    with patch("srunx.web.routers.files._get_current_profile", return_value=profile):
        yield TestClient(app, raise_server_exceptions=False)

    app.dependency_overrides.clear()
    config_mod._config = original


class TestListMounts:
    def test_list_mounts(self, client: TestClient) -> None:
        resp = client.get("/api/files/mounts")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 1
        assert data[0]["name"] == "test-project"
        assert data[0]["remote"] == "/home/user/project"
        # Local paths should NOT be exposed
        assert "local" not in data[0]

    def test_list_mounts_no_profile(self, mock_adapter: MagicMock) -> None:
        import srunx.web.config as config_mod

        original = config_mod._config
        config_mod._config = None
        config_mod.get_web_config()

        app = create_app()
        app.dependency_overrides[get_adapter] = lambda: mock_adapter

        with patch("srunx.web.routers.files._get_current_profile", return_value=None):
            c = TestClient(app, raise_server_exceptions=False)
            resp = c.get("/api/files/mounts")
            assert resp.status_code == 200
            assert resp.json() == []

        app.dependency_overrides.clear()
        config_mod._config = original


class TestBrowseFiles:
    def test_browse_root(self, client: TestClient, fake_profile: ServerProfile) -> None:
        from pathlib import Path

        mount_root = Path(fake_profile.mounts[0].local)
        mount_root.mkdir(parents=True, exist_ok=True)
        (mount_root / "src").mkdir()
        (mount_root / "README.md").write_text("hello")

        resp = client.get("/api/files/browse", params={"mount": "test-project"})
        assert resp.status_code == 200
        data = resp.json()
        assert data["mount_name"] == "test-project"
        assert data["remote_prefix"] == "/home/user/project"
        names = [e["name"] for e in data["entries"]]
        assert "src" in names
        assert "README.md" in names

    def test_browse_subdirectory(
        self, client: TestClient, fake_profile: ServerProfile
    ) -> None:
        from pathlib import Path

        mount_root = Path(fake_profile.mounts[0].local)
        subdir = mount_root / "src"
        subdir.mkdir(exist_ok=True)
        (subdir / "main.py").write_text("print('hello')")

        resp = client.get(
            "/api/files/browse", params={"mount": "test-project", "path": "src"}
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["remote_prefix"] == "/home/user/project/src"
        names = [e["name"] for e in data["entries"]]
        assert "main.py" in names

    def test_browse_hides_hidden_files(
        self, client: TestClient, fake_profile: ServerProfile
    ) -> None:
        from pathlib import Path

        mount_root = Path(fake_profile.mounts[0].local)
        (mount_root / ".hidden").write_text("secret")
        (mount_root / "visible.txt").write_text("public")

        resp = client.get("/api/files/browse", params={"mount": "test-project"})
        assert resp.status_code == 200
        names = [e["name"] for e in resp.json()["entries"]]
        assert ".hidden" not in names
        assert "visible.txt" in names

    def test_browse_path_traversal_blocked(self, client: TestClient) -> None:
        resp = client.get(
            "/api/files/browse",
            params={"mount": "test-project", "path": "../../etc"},
        )
        assert resp.status_code == 403
        assert "outside mount boundary" in resp.json()["detail"]

    def test_browse_nonexistent_directory(self, client: TestClient) -> None:
        resp = client.get(
            "/api/files/browse",
            params={"mount": "test-project", "path": "no-such-dir"},
        )
        assert resp.status_code == 404

    def test_browse_file_not_directory(
        self, client: TestClient, fake_profile: ServerProfile
    ) -> None:
        from pathlib import Path

        mount_root = Path(fake_profile.mounts[0].local)
        (mount_root / "file.txt").write_text("content")

        resp = client.get(
            "/api/files/browse",
            params={"mount": "test-project", "path": "file.txt"},
        )
        assert resp.status_code == 400
        assert "not a directory" in resp.json()["detail"]

    def test_browse_mount_not_found(self, client: TestClient) -> None:
        resp = client.get("/api/files/browse", params={"mount": "nonexistent-mount"})
        assert resp.status_code == 404

    def test_browse_symlink_within_boundary(
        self, client: TestClient, fake_profile: ServerProfile
    ) -> None:
        from pathlib import Path

        mount_root = Path(fake_profile.mounts[0].local)
        target = mount_root / "real_dir"
        target.mkdir()
        (target / "data.txt").write_text("data")
        link = mount_root / "link_dir"
        link.symlink_to(target)

        resp = client.get("/api/files/browse", params={"mount": "test-project"})
        assert resp.status_code == 200
        entries = resp.json()["entries"]
        link_entry = next((e for e in entries if e["name"] == "link_dir"), None)
        assert link_entry is not None
        assert link_entry["type"] == "symlink"
        assert link_entry["accessible"] is True
        assert link_entry["target_kind"] == "directory"

    def test_browse_symlink_outside_boundary(
        self, client: TestClient, fake_profile: ServerProfile, tmp_path
    ) -> None:
        from pathlib import Path

        mount_root = Path(fake_profile.mounts[0].local)
        # Create a target outside the mount boundary
        outside = tmp_path / "outside"
        outside.mkdir()
        link = mount_root / "escape_link"
        link.symlink_to(outside)

        resp = client.get("/api/files/browse", params={"mount": "test-project"})
        assert resp.status_code == 200
        entries = resp.json()["entries"]
        link_entry = next((e for e in entries if e["name"] == "escape_link"), None)
        assert link_entry is not None
        assert link_entry["type"] == "symlink"
        assert link_entry["accessible"] is False


class TestReadFile:
    def test_read_file(self, client: TestClient, fake_profile: ServerProfile) -> None:
        from pathlib import Path

        mount_root = Path(fake_profile.mounts[0].local)
        (mount_root / "hello.txt").write_text("Hello, world!")

        resp = client.get(
            "/api/files/read",
            params={"mount": "test-project", "path": "hello.txt"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["content"] == "Hello, world!"
        assert data["path"] == "hello.txt"
        assert data["mount"] == "test-project"

    def test_read_file_path_traversal_blocked(self, client: TestClient) -> None:
        resp = client.get(
            "/api/files/read",
            params={"mount": "test-project", "path": "../../etc/passwd"},
        )
        assert resp.status_code == 403
        assert "outside mount boundary" in resp.json()["detail"]

    def test_read_file_not_found(self, client: TestClient) -> None:
        resp = client.get(
            "/api/files/read",
            params={"mount": "test-project", "path": "nonexistent.txt"},
        )
        assert resp.status_code == 404

    def test_read_file_empty_path(self, client: TestClient) -> None:
        resp = client.get(
            "/api/files/read", params={"mount": "test-project", "path": ""}
        )
        assert resp.status_code == 400
        assert "path is required" in resp.json()["detail"]

    def test_read_directory_returns_400(
        self, client: TestClient, fake_profile: ServerProfile
    ) -> None:
        from pathlib import Path

        mount_root = Path(fake_profile.mounts[0].local)
        (mount_root / "subdir").mkdir()

        resp = client.get(
            "/api/files/read",
            params={"mount": "test-project", "path": "subdir"},
        )
        assert resp.status_code == 400
        assert "not a file" in resp.json()["detail"]

    def test_read_file_too_large(
        self, client: TestClient, fake_profile: ServerProfile
    ) -> None:
        from pathlib import Path

        mount_root = Path(fake_profile.mounts[0].local)
        large_file = mount_root / "big.bin"
        # Create a file larger than 1 MB
        large_file.write_bytes(b"x" * (1024 * 1024 + 1))

        resp = client.get(
            "/api/files/read",
            params={"mount": "test-project", "path": "big.bin"},
        )
        assert resp.status_code == 413
        assert "too large" in resp.json()["detail"]

    def test_read_mount_not_found(self, client: TestClient) -> None:
        resp = client.get(
            "/api/files/read",
            params={"mount": "nonexistent", "path": "file.txt"},
        )
        assert resp.status_code == 404
