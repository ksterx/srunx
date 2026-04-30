"""Tests for srunx.runtime.templates module."""

import json
from pathlib import Path

import pytest

from srunx.runtime.templates import (
    create_user_template,
    delete_user_template,
    get_template_info,
    get_template_path,
    list_templates,
    update_user_template,
)


@pytest.fixture()
def user_templates_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Override the user templates directory to a temp directory."""
    templates_dir = tmp_path / "templates"
    templates_dir.mkdir()
    monkeypatch.setattr(
        "srunx.runtime.templates._user_templates_dir", lambda: templates_dir
    )
    monkeypatch.setattr(
        "srunx.runtime.templates._user_meta_path",
        lambda: templates_dir / "meta.json",
    )
    return templates_dir


class TestListTemplates:
    """Tests for list_templates()."""

    def test_returns_list_with_base_template(self) -> None:
        """list_templates should return a list containing at least the 'base' template."""
        templates = list_templates()
        names = [t["name"] for t in templates]
        assert "base" in names

    def test_base_template_has_expected_keys(self) -> None:
        """The base template entry should have name, description, path, and use_case."""
        templates = list_templates()
        base = next(t for t in templates if t["name"] == "base")
        assert "name" in base
        assert "description" in base
        assert "path" in base
        assert "use_case" in base

    def test_includes_user_templates(self, user_templates_dir: Path) -> None:
        """list_templates should include user-defined templates."""
        create_user_template("custom", "A custom template", "testing", "#!/bin/bash")
        templates = list_templates()
        names = [t["name"] for t in templates]
        assert "custom" in names
        custom = next(t for t in templates if t["name"] == "custom")
        assert custom["user_defined"] == "true"


class TestGetTemplatePath:
    """Tests for get_template_path()."""

    def test_returns_valid_path_for_base(self) -> None:
        """get_template_path('base') should return a path string ending with the template filename."""
        path = get_template_path("base")
        assert isinstance(path, str)
        assert path.endswith("base.slurm.jinja")

    def test_raises_for_nonexistent_template(self) -> None:
        """get_template_path should raise ValueError for a template that does not exist."""
        with pytest.raises(ValueError, match="not found"):
            get_template_path("nonexistent_template_xyz")

    def test_returns_user_template_path(self, user_templates_dir: Path) -> None:
        """get_template_path should return the user template file path."""
        create_user_template("mytemplate", "desc", "use", "content")
        path = get_template_path("mytemplate")
        assert path == str(user_templates_dir / "mytemplate.slurm.jinja")

    def test_error_message_lists_available(self) -> None:
        """The error message should list available template names."""
        with pytest.raises(ValueError, match="base"):
            get_template_path("does_not_exist")


class TestGetTemplateInfo:
    """Tests for get_template_info()."""

    def test_returns_dict_with_expected_keys_for_base(self) -> None:
        """get_template_info('base') should return a dict with name, description, path, use_case."""
        info = get_template_info("base")
        assert isinstance(info, dict)
        assert info["name"] == "base"
        assert "description" in info
        assert "path" in info
        assert "use_case" in info

    def test_raises_for_nonexistent_template(self) -> None:
        """get_template_info should raise ValueError for unknown templates."""
        with pytest.raises(ValueError, match="not found"):
            get_template_info("nonexistent_template_xyz")

    def test_returns_user_template_info(self, user_templates_dir: Path) -> None:
        """get_template_info should return info for a user-defined template."""
        create_user_template("myinfo", "My description", "My use case", "content")
        info = get_template_info("myinfo")
        assert info["name"] == "myinfo"
        assert info["description"] == "My description"
        assert info["use_case"] == "My use case"


class TestCreateUserTemplate:
    """Tests for create_user_template()."""

    def test_creates_template_file(self, user_templates_dir: Path) -> None:
        """create_user_template should write the template file to disk."""
        content = "#!/bin/bash\necho hello"
        result = create_user_template("newjob", "A new job", "general", content)
        template_file = user_templates_dir / "newjob.slurm.jinja"
        assert template_file.exists()
        assert template_file.read_text(encoding="utf-8") == content
        assert result["name"] == "newjob"
        assert result["description"] == "A new job"
        assert result["use_case"] == "general"

    def test_saves_metadata(self, user_templates_dir: Path) -> None:
        """create_user_template should persist metadata to meta.json."""
        create_user_template("metajob", "desc", "use", "content")
        meta_path = user_templates_dir / "meta.json"
        assert meta_path.exists()
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
        assert "metajob" in meta
        assert meta["metajob"]["description"] == "desc"
        assert meta["metajob"]["use_case"] == "use"

    def test_raises_on_duplicate(self, user_templates_dir: Path) -> None:
        """create_user_template should raise ValueError if the template already exists."""
        create_user_template("dup", "first", "use", "content")
        with pytest.raises(ValueError, match="already exists"):
            create_user_template("dup", "second", "use", "content2")

    def test_raises_on_builtin_name(self, user_templates_dir: Path) -> None:
        """create_user_template should refuse to overwrite a built-in template."""
        with pytest.raises(ValueError, match="built-in"):
            create_user_template("base", "override", "use", "content")

    def test_validates_name_alphanumeric(self, user_templates_dir: Path) -> None:
        """create_user_template should reject names with invalid characters."""
        with pytest.raises(ValueError, match="Invalid template name"):
            create_user_template("bad name!", "desc", "use", "content")

    def test_validates_name_with_spaces(self, user_templates_dir: Path) -> None:
        """Names with spaces are invalid."""
        with pytest.raises(ValueError, match="Invalid template name"):
            create_user_template("has space", "desc", "use", "content")

    def test_validates_empty_name(self, user_templates_dir: Path) -> None:
        """Empty name should be rejected."""
        with pytest.raises(ValueError, match="Invalid template name"):
            create_user_template("", "desc", "use", "content")

    def test_allows_hyphens_and_underscores(self, user_templates_dir: Path) -> None:
        """Names with hyphens and underscores are valid."""
        result = create_user_template("my-template_v2", "desc", "use", "content")
        assert result["name"] == "my-template_v2"


class TestUpdateUserTemplate:
    """Tests for update_user_template()."""

    def test_updates_description(self, user_templates_dir: Path) -> None:
        """update_user_template should update the description."""
        create_user_template("upd", "old desc", "use", "content")
        result = update_user_template("upd", description="new desc")
        assert result["description"] == "new desc"
        # Verify persisted
        info = get_template_info("upd")
        assert info["description"] == "new desc"

    def test_updates_use_case(self, user_templates_dir: Path) -> None:
        """update_user_template should update the use_case."""
        create_user_template("upd2", "desc", "old use", "content")
        result = update_user_template("upd2", use_case="new use")
        assert result["use_case"] == "new use"

    def test_updates_content(self, user_templates_dir: Path) -> None:
        """update_user_template should update the template file content."""
        create_user_template("upd3", "desc", "use", "old content")
        update_user_template("upd3", content="new content")
        template_file = user_templates_dir / "upd3.slurm.jinja"
        assert template_file.read_text(encoding="utf-8") == "new content"

    def test_raises_on_nonexistent(self, user_templates_dir: Path) -> None:
        """update_user_template should raise ValueError for a template that does not exist."""
        with pytest.raises(ValueError, match="not found"):
            update_user_template("ghost", description="nope")

    def test_raises_on_builtin(self, user_templates_dir: Path) -> None:
        """update_user_template should refuse to modify a built-in template."""
        with pytest.raises(ValueError, match="built-in"):
            update_user_template("base", description="hacked")

    def test_partial_update_preserves_other_fields(
        self, user_templates_dir: Path
    ) -> None:
        """Updating only description should preserve use_case and vice versa."""
        create_user_template("partial", "original desc", "original use", "content")
        update_user_template("partial", description="updated desc")
        info = get_template_info("partial")
        assert info["description"] == "updated desc"
        assert info["use_case"] == "original use"


class TestDeleteUserTemplate:
    """Tests for delete_user_template()."""

    def test_deletes_template_file_and_metadata(self, user_templates_dir: Path) -> None:
        """delete_user_template should remove both the file and metadata entry."""
        create_user_template("todelete", "desc", "use", "content")
        template_file = user_templates_dir / "todelete.slurm.jinja"
        assert template_file.exists()
        delete_user_template("todelete")
        assert not template_file.exists()
        # Metadata should also be gone
        meta = json.loads(
            (user_templates_dir / "meta.json").read_text(encoding="utf-8")
        )
        assert "todelete" not in meta

    def test_raises_on_builtin(self, user_templates_dir: Path) -> None:
        """delete_user_template should refuse to delete a built-in template."""
        with pytest.raises(ValueError, match="built-in"):
            delete_user_template("base")

    def test_raises_on_nonexistent(self, user_templates_dir: Path) -> None:
        """delete_user_template should raise ValueError for unknown templates."""
        with pytest.raises(ValueError, match="not found"):
            delete_user_template("ghost")

    def test_template_no_longer_listed_after_delete(
        self, user_templates_dir: Path
    ) -> None:
        """After deletion, the template should not appear in list_templates."""
        create_user_template("ephemeral", "desc", "use", "content")
        names_before = [t["name"] for t in list_templates()]
        assert "ephemeral" in names_before
        delete_user_template("ephemeral")
        names_after = [t["name"] for t in list_templates()]
        assert "ephemeral" not in names_after

    def test_delete_when_file_already_missing(self, user_templates_dir: Path) -> None:
        """delete_user_template should succeed even if the file was already removed."""
        create_user_template("fileless", "desc", "use", "content")
        # Manually remove the file but leave metadata
        (user_templates_dir / "fileless.slurm.jinja").unlink()
        # Should not raise
        delete_user_template("fileless")
        meta = json.loads(
            (user_templates_dir / "meta.json").read_text(encoding="utf-8")
        )
        assert "fileless" not in meta
