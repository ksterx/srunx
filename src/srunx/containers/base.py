"""Base abstractions for container runtime backends."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from srunx.domain import ContainerResource


@dataclass(frozen=True)
class LaunchSpec:
    """Runtime-agnostic container launch specification.

    Three distinct outputs model different injection points in generated scripts:
    - prelude: Shell setup lines executed before the command (e.g., declare arrays)
    - srun_args: Flags passed to srun itself (Pyxis uses this)
    - launch_prefix: Command wrapper prepended to the user command (Apptainer uses this)
    """

    prelude: str = ""
    srun_args: str = ""
    launch_prefix: str = ""


class ContainerRuntime(Protocol):
    """Protocol for container runtime backends."""

    def build_launch_spec(self, config: ContainerResource) -> LaunchSpec: ...
