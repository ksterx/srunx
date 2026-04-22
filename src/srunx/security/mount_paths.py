"""Mount-root guard for ShellJob script paths.

Both the Web API and the MCP tool surface the same attack shape: a
workflow YAML can declare ``template: shell`` with an arbitrary
``script_path`` that ``render_shell_job_script`` then reads verbatim.
Without a guard, a caller could exfiltrate or inject arbitrary host
files via e.g. ``script_path: ../../../etc/passwd``.

The helper here returns a structured ``ShellJobScriptViolation`` so
each transport caller can raise the right exception type (Web →
``HTTPException(403)``, MCP → ``ValueError``) while the actual
directory-check logic lives in one place.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from srunx.models import Workflow


@dataclass(frozen=True)
class ShellJobScriptViolation:
    """First ShellJob whose script path escapes the allowed mount roots."""

    job_name: str
    script_path: str


def find_shell_script_violation(
    workflow: Workflow,
    mount_roots: Iterable[Path],
) -> ShellJobScriptViolation | None:
    """Return the first ShellJob pointing outside every ``mount_roots`` entry.

    ``mount_roots`` must already be resolved absolute paths (see
    :meth:`pathlib.Path.resolve`). Non-ShellJob entries are ignored.
    Returns ``None`` when every ShellJob's ``script_path`` is contained
    in at least one root.
    """
    from srunx.models import ShellJob  # local import to avoid cycles

    roots = list(mount_roots)
    for job in workflow.jobs:
        if isinstance(job, ShellJob):
            resolved = Path(job.script_path).resolve()
            if not any(resolved.is_relative_to(root) for root in roots):
                return ShellJobScriptViolation(
                    job_name=job.name,
                    script_path=job.script_path,
                )
    return None
