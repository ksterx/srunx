"""Pyxis container runtime backend.

Generates NVIDIA Pyxis --container-* flags as srun arguments,
reproducing the existing behavior that was previously hardcoded
in _build_environment_setup().
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from srunx.containers.base import LaunchSpec

if TYPE_CHECKING:
    from srunx.domain import ContainerResource


class PyxisRuntime:
    """Pyxis runtime backend -- generates --container-* srun flags."""

    def build_launch_spec(self, config: ContainerResource) -> LaunchSpec:
        container_args: list[str] = []
        if config.image:
            container_args.append(f"--container-image {config.image}")
        if config.mounts:
            container_args.append(f"--container-mounts {','.join(config.mounts)}")
        if config.workdir:
            container_args.append(f"--container-workdir {config.workdir}")

        prelude_lines = [
            "declare -a CONTAINER_ARGS=(",
            *container_args,
            ")",
        ]

        return LaunchSpec(
            prelude="\n".join(prelude_lines),
            srun_args='"${CONTAINER_ARGS[@]}"',
            launch_prefix="",
        )
