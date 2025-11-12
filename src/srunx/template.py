"""Job template management for common use cases."""

from importlib.resources import files

TEMPLATES = {
    "base": {
        "name": "base",
        "description": "Basic SLURM job template",
        "path": "base.slurm.jinja",
        "use_case": "Simple single-node jobs",
    },
    "advanced": {
        "name": "advanced",
        "description": "Advanced SLURM job template with all features",
        "path": "advanced.slurm.jinja",
        "use_case": "Complex jobs with custom resource requirements",
    },
    "pytorch-ddp": {
        "name": "pytorch-ddp",
        "description": "PyTorch Distributed Data Parallel (DDP)",
        "path": "pytorch_ddp.slurm.jinja",
        "use_case": "Multi-node/multi-GPU PyTorch distributed training",
    },
    "tensorflow-multiworker": {
        "name": "tensorflow-multiworker",
        "description": "TensorFlow MultiWorkerMirroredStrategy",
        "path": "tensorflow_multiworker.slurm.jinja",
        "use_case": "Multi-node/multi-GPU TensorFlow distributed training",
    },
    "horovod": {
        "name": "horovod",
        "description": "Horovod distributed training framework",
        "path": "horovod.slurm.jinja",
        "use_case": "Multi-framework distributed training (PyTorch, TensorFlow, etc.)",
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
        template_name: Name of the template (e.g., 'pytorch-ddp')

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
