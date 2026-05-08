"""Pre-submission guards for CLI workflow runs."""

from __future__ import annotations

from pathlib import Path

import typer

from srunx.common.logging import get_logger
from srunx.domain import Workflow
from srunx.runtime.security import find_shell_script_violation
from srunx.transport import ResolvedTransport

logger = get_logger(__name__)


def _enforce_shell_script_roots_cli(workflow: Workflow, rt: ResolvedTransport) -> None:
    """Reject ShellJob script paths that escape the SSH profile's mounts.

    Mirrors the Web router and MCP tool guards (see
    :func:`srunx.web.routers.workflows._enforce_shell_script_roots` and
    :func:`srunx.mcp.tools.workflows._enforce_shell_script_roots`): when the
    workflow will be dispatched over SSH, every :class:`ShellJob`
    ``script_path`` must sit under one of the profile's mount ``local``
    roots so the remote executor can map it to a legitimate remote path.

    Local transport imposes no check: Phase 5b keeps local ShellJob
    behaviour unchanged.
    """
    if rt.transport_type != "ssh":
        return
    ctx = rt.submission_context
    if ctx is None or not ctx.mounts:
        # Profile has no mounts configured: fall through and warn instead
        # of raising — see _warn_missing_mounts.
        return

    allowed_roots = [Path(m.local).resolve() for m in ctx.mounts]
    violation = find_shell_script_violation(workflow, allowed_roots)
    if violation is not None:
        raise typer.BadParameter(
            f"ShellJob '{violation.job_name}' script_path "
            f"'{violation.script_path}' is not under any mount's local root "
            f"for profile '{rt.profile_name}'. "
            f"Allowed roots: {[str(r) for r in allowed_roots]}"
        )


def _warn_missing_mounts(rt: ResolvedTransport) -> None:
    """Warn when an SSH-bound flow has no profile mounts configured.

    Without mount translation, workflow ``work_dir`` / ``log_dir`` /
    ``ShellJob.script_path`` fields render verbatim on the remote side;
    they must already exist there or the submission will fail at the
    remote executor. The warning surfaces this expectation instead of
    letting the failure happen silently in a sbatch script that SLURM
    refuses.
    """
    if rt.transport_type != "ssh":
        return
    ctx = rt.submission_context
    if ctx is None or not ctx.mounts:
        logger.warning(
            "Profile '%s' has no mounts configured; workflow paths "
            "(work_dir / log_dir / script_path) will be rendered as-is "
            "on the remote cluster.",
            rt.profile_name,
        )
