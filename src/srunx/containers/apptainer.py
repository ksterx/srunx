"""Apptainer / Singularity container runtime backend.

Generates `apptainer exec` (or `singularity exec`) as a launch_prefix
that wraps the user command.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from srunx.containers.base import LaunchSpec

if TYPE_CHECKING:
    from srunx.domain import ContainerResource


class ApptainerRuntime:
    """Apptainer/Singularity runtime backend -- generates launch_prefix."""

    def __init__(self, binary: str = "apptainer") -> None:
        self.binary = binary

    def build_launch_spec(self, config: ContainerResource) -> LaunchSpec:
        parts: list[str] = [self.binary, "exec"]

        # GPU passthrough
        if config.nv:
            parts.append("--nv")
        if config.rocm:
            parts.append("--rocm")

        # Environment flags
        if config.cleanenv:
            parts.append("--cleanenv")
        if config.fakeroot:
            parts.append("--fakeroot")
        if config.writable_tmpfs:
            parts.append("--writable-tmpfs")

        # Overlay
        if config.overlay:
            parts.append(f"--overlay {config.overlay}")

        # Bind mounts
        for mount in config.mounts:
            parts.append(f"--bind {mount}")

        # Environment variables
        for key, value in config.env.items():
            parts.append(f"--env {key}={value}")

        # Working directory
        if config.workdir:
            parts.append(f"--pwd {config.workdir}")

        # Image (must be last before the command)
        if config.image:
            parts.append(config.image)

        return LaunchSpec(
            prelude="",
            srun_args="",
            launch_prefix=" ".join(parts),
        )
