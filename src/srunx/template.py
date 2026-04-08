"""Job template management for common use cases."""

from importlib.resources import files

TEMPLATES = {
    "base": {
        "name": "base",
        "description": "SLURM job template with full resource control and inter-job outputs",
        "path": "base.slurm.jinja",
        "use_case": "All job types including distributed training",
    },
}


def list_templates() -> list[dict[str, str]]:
    """List all available templates.

    Returns:
        List of template information dictionaries.
    """
    return list(TEMPLATES.values())


def get_template_path(template_name: str) -> str:
    """Get the path to a template file.

    Args:
        template_name: Name of the template (e.g., 'base')

    Returns:
        Path to the template file.

    Raises:
        ValueError: If template name is not found.
    """
    if template_name not in TEMPLATES:
        available = ", ".join(TEMPLATES.keys())
        raise ValueError(
            f"Template '{template_name}' not found. Available templates: {available}"
        )

    template_file = TEMPLATES[template_name]["path"]
    return str(files("srunx.templates").joinpath(template_file))


def get_template_info(template_name: str) -> dict[str, str]:
    """Get information about a specific template.

    Args:
        template_name: Name of the template

    Returns:
        Template information dictionary.

    Raises:
        ValueError: If template name is not found.
    """
    if template_name not in TEMPLATES:
        available = ", ".join(TEMPLATES.keys())
        raise ValueError(
            f"Template '{template_name}' not found. Available templates: {available}"
        )

    return TEMPLATES[template_name]
