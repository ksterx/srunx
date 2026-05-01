"""Job template management for common use cases."""

import json
import re
from importlib.resources import files
from pathlib import Path

BUILTIN_TEMPLATES = {
    "base": {
        "name": "base",
        "description": "SLURM job template with full resource control",
        "path": "base.slurm.jinja",
        "use_case": "All job types including distributed training",
    },
}

# Keep backward-compatible alias
TEMPLATES = BUILTIN_TEMPLATES

_VALID_NAME = re.compile(r"^[a-zA-Z0-9_-]+$")


def _user_templates_dir() -> Path:
    """Return the user templates directory under the srunx config dir.

    Delegates to :func:`srunx.common.config._user_config_dir` so that the
    JSON config file, the state DB, and user templates all share the same
    XDG_CONFIG_HOME-honouring root — flipping ``XDG_CONFIG_HOME`` isolates
    every user-state surface in one go (tests rely on this).
    """
    from srunx.common.config import _user_config_dir

    return _user_config_dir() / "templates"


def _user_meta_path() -> Path:
    """Return the path to user templates metadata file."""
    return _user_templates_dir() / "meta.json"


def _load_user_meta() -> dict[str, dict[str, str]]:
    """Load user template metadata from disk."""
    meta_path = _user_meta_path()
    if not meta_path.exists():
        return {}
    try:
        with open(meta_path, encoding="utf-8") as f:
            return json.load(f)
    except (OSError, json.JSONDecodeError):
        return {}


def _save_user_meta(meta: dict[str, dict[str, str]]) -> None:
    """Save user template metadata to disk."""
    meta_path = _user_meta_path()
    meta_path.parent.mkdir(parents=True, exist_ok=True)
    with open(meta_path, "w", encoding="utf-8") as f:
        json.dump(meta, f, indent=2)


def _user_template_file(name: str) -> Path:
    """Return the path to a user template file."""
    return _user_templates_dir() / f"{name}.slurm.jinja"


def list_templates() -> list[dict[str, str]]:
    """List all available templates (built-in + user)."""
    result = list(BUILTIN_TEMPLATES.values())
    for name, info in _load_user_meta().items():
        result.append({"name": name, **info, "user_defined": "true"})
    return result


def get_template_path(template_name: str) -> str:
    """Get the path to a template file."""
    if template_name in BUILTIN_TEMPLATES:
        template_file = BUILTIN_TEMPLATES[template_name]["path"]
        return str(files("srunx.runtime").joinpath("_jinja", template_file))

    user_file = _user_template_file(template_name)
    if user_file.exists():
        return str(user_file)

    all_names = list(BUILTIN_TEMPLATES.keys()) + list(_load_user_meta().keys())
    available = ", ".join(all_names)
    raise ValueError(
        f"Template '{template_name}' not found. Available templates: {available}"
    )


def get_template_info(template_name: str) -> dict[str, str]:
    """Get information about a specific template."""
    if template_name in BUILTIN_TEMPLATES:
        return BUILTIN_TEMPLATES[template_name]

    user_meta = _load_user_meta()
    if template_name in user_meta:
        return {"name": template_name, **user_meta[template_name]}

    all_names = list(BUILTIN_TEMPLATES.keys()) + list(user_meta.keys())
    available = ", ".join(all_names)
    raise ValueError(
        f"Template '{template_name}' not found. Available templates: {available}"
    )


def create_user_template(
    name: str, description: str, use_case: str, content: str
) -> dict[str, str]:
    """Create a new user-defined template."""
    if not _VALID_NAME.match(name):
        raise ValueError(
            f"Invalid template name '{name}'. Use only alphanumeric, hyphens, underscores."
        )
    if name in BUILTIN_TEMPLATES:
        raise ValueError(f"Cannot overwrite built-in template '{name}'.")

    user_meta = _load_user_meta()
    if name in user_meta:
        raise ValueError(f"Template '{name}' already exists.")

    template_file = _user_template_file(name)
    template_file.parent.mkdir(parents=True, exist_ok=True)
    template_file.write_text(content, encoding="utf-8")

    user_meta[name] = {"description": description, "use_case": use_case}
    _save_user_meta(user_meta)

    return {"name": name, "description": description, "use_case": use_case}


def update_user_template(
    name: str,
    description: str | None = None,
    use_case: str | None = None,
    content: str | None = None,
) -> dict[str, str]:
    """Update an existing user-defined template."""
    if name in BUILTIN_TEMPLATES:
        raise ValueError(f"Cannot modify built-in template '{name}'.")

    user_meta = _load_user_meta()
    if name not in user_meta:
        raise ValueError(f"User template '{name}' not found.")

    if description is not None:
        user_meta[name]["description"] = description
    if use_case is not None:
        user_meta[name]["use_case"] = use_case
    if content is not None:
        _user_template_file(name).write_text(content, encoding="utf-8")

    _save_user_meta(user_meta)
    return {"name": name, **user_meta[name]}


def delete_user_template(name: str) -> None:
    """Delete a user-defined template."""
    if name in BUILTIN_TEMPLATES:
        raise ValueError(f"Cannot delete built-in template '{name}'.")

    user_meta = _load_user_meta()
    if name not in user_meta:
        raise ValueError(f"User template '{name}' not found.")

    template_file = _user_template_file(name)
    if template_file.exists():
        template_file.unlink()

    del user_meta[name]
    _save_user_meta(user_meta)
