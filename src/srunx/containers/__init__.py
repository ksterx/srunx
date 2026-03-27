"""Container runtime abstraction for srunx."""

from srunx.containers.apptainer import ApptainerRuntime
from srunx.containers.base import ContainerRuntime, LaunchSpec
from srunx.containers.pyxis import PyxisRuntime

__all__ = [
    "ContainerRuntime",
    "LaunchSpec",
    "PyxisRuntime",
    "ApptainerRuntime",
    "get_runtime",
]


def get_runtime(name: str) -> ContainerRuntime:
    """Return a container runtime backend by name.

    Args:
        name: Runtime identifier -- "pyxis", "apptainer", or "singularity".

    Returns:
        A ContainerRuntime implementation.

    Raises:
        ValueError: If the runtime name is not recognized.
    """
    match name:
        case "pyxis":
            return PyxisRuntime()
        case "apptainer":
            return ApptainerRuntime(binary="apptainer")
        case "singularity":
            return ApptainerRuntime(binary="singularity")
        case _:
            raise ValueError(
                f"Unknown container runtime '{name}'. "
                f"Valid runtimes: pyxis, apptainer, singularity"
            )
