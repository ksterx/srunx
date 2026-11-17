"""``sbatch --snapshot``: option validation, planning, and submission.

Submits an SSH job from a read-only per-commit snapshot of the local git
repository instead of the (mutable) rsync'd mount. Snapshot mode never enters
mount sync or the per-mount lock.
"""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path
from typing import Any

import typer
from rich.console import Console

from srunx.cli._helpers.sbatch_helpers import (
    last_option_value,
    script_sbatch_tokens,
)
from srunx.common.exceptions import SbatchSubmitError, TransportError
from srunx.common.logging import get_logger
from srunx.snapshot import (
    SnapshotError,
    SnapshotPlan,
    SnapshotService,
    SnapshotValidationError,
    plan_snapshot,
)
from srunx.snapshot.remote import DEFAULT_SNAPSHOT_ROOT

logger = get_logger(__name__)


def validate_snapshot_options(
    *,
    snapshot: bool,
    snapshot_rev: str | None,
    script: Path | None,
    wrap: str | None,
    template: str | None,
    local: bool,
    sync: bool | None,
    force_sync: bool,
    profile: str | None,
) -> bool:
    """Return True when snapshot mode is requested; reject conflicting options.

    ``--snapshot-rev`` implies ``--snapshot``. Raises ``typer.BadParameter``
    (exit 2) naming the conflicting option. ``--no-sync`` is accepted (snapshot
    mode never syncs); only an explicit ``--sync`` conflicts. A transport that
    *resolves* to local is rejected by the caller via :func:`require_ssh_transport`.
    """
    if not snapshot and snapshot_rev is None:
        return False
    conflicts = (
        (wrap is not None, "--wrap"),
        (template is not None, "--template"),
        (local, "--local"),
        (sync is True, "--sync"),
        (force_sync, "--force-sync"),
    )
    for present, flag in conflicts:
        if present:
            raise typer.BadParameter(
                f"--snapshot cannot be combined with {flag}.",
                param_hint="--snapshot",
            )
    if script is None:
        raise typer.BadParameter(
            "--snapshot requires a positional script path.",
            param_hint="--snapshot / <script>",
        )
    if snapshot_rev is not None and not snapshot_rev.strip():
        raise typer.BadParameter(
            "--snapshot-rev must not be empty.", param_hint="--snapshot-rev"
        )
    return True


def require_ssh_transport(*, profile: str | None, local: bool) -> str:
    """Return the SSH profile name; reject a transport that resolves to local (exit 2)."""
    from srunx.cli._helpers.transport import peek_scheduler_key

    key = peek_scheduler_key(profile=profile, local=local)
    if not key.startswith("ssh:"):
        raise typer.BadParameter(
            "--snapshot requires an SSH profile (use --profile <name>).",
            param_hint="--snapshot / --profile",
        )
    return key[len("ssh:") :]


def build_snapshot_plan(
    *, script: Path | None, rev: str | None, cwd: Path
) -> SnapshotPlan:
    """Run local validation; ``SnapshotValidationError`` becomes ``BadParameter``."""
    try:
        return plan_snapshot(start=cwd, rev=rev, script=script, cwd=cwd)
    except SnapshotValidationError as exc:
        raise typer.BadParameter(str(exc)) from exc


def print_snapshot_dry_run(
    console: Console, plan: SnapshotPlan, *, profile_name: str | None
) -> None:
    from srunx.ssh.core.config import ConfigManager

    profile = ConfigManager().get_profile(profile_name) if profile_name else None
    snapshot_root = profile.snapshot_root if profile else None
    root = snapshot_root or f"{DEFAULT_SNAPSHOT_ROOT} (default)"
    console.print("  Snapshot mode (no remote call in dry run):")
    console.print(f"    Commit: {plan.sha}")
    console.print(f"    Repo id: {plan.repo_id}")
    console.print(f"    Snapshot root: {root}")
    console.print(f"    Script (in commit): {plan.script_rel}")
    console.print(f"    Job cwd (in snapshot): {plan.cwd_rel or '<snapshot top>'}")
    for w in plan.warnings:
        console.print(f"    [yellow]warning: {w}[/yellow]")


def _effective(
    extra: Sequence[str], directive_tokens: Sequence[str], short: str, long: str
) -> str | None:
    value = last_option_value(extra, short, long)
    if value is None:
        value = last_option_value(directive_tokens, short, long)
    return value


def _is_absolute(value: str) -> bool:
    return value.startswith("/")


def output_warning(script_text: str, extra_sbatch_args: Sequence[str]) -> str | None:
    """Warn when the job would write into the read-only snapshot (REQ-16).

    Effective values are the CLI/passthrough value if given, else the script's
    ``#SBATCH`` directive. Warns when ``--chdir`` is missing or relative and
    either ``--output`` is missing or relative, or ``--error`` is relative.
    """
    directive_tokens = [t for line in script_sbatch_tokens(script_text) for t in line]
    chdir = _effective(extra_sbatch_args, directive_tokens, "-D", "--chdir")
    output = _effective(extra_sbatch_args, directive_tokens, "-o", "--output")
    error = _effective(extra_sbatch_args, directive_tokens, "-e", "--error")

    if chdir is not None and _is_absolute(chdir):
        return None
    output_bad = output is None or not _is_absolute(output)
    error_bad = error is not None and not _is_absolute(error)
    if not (output_bad or error_bad):
        return None
    return (
        "The job's working directory is the read-only snapshot and its "
        "--output/--error path is missing or relative, so SLURM may fail to "
        "write the job log there. Pass an absolute --output/--error (or "
        "--log-dir) or an absolute --chdir."
    )


def submit_via_snapshot(
    *,
    rt: Any,
    job: Any,
    plan: SnapshotPlan,
    profile_name: str,
    extra_sbatch_args: list[str] | None,
    inject_job_name: bool,
) -> Any:
    """Prepare the snapshot, submit from it under a lease, and bind the lease."""
    from srunx.ssh.core.config import ConfigManager

    profile = ConfigManager().get_profile(profile_name)
    if profile is None:
        raise typer.BadParameter(
            f"SSH profile {profile_name!r} not found.", param_hint="--profile"
        )

    for w in plan.warnings:
        logger.warning("{}", w)
    if plan.script_text is not None:
        warning = output_warning(plan.script_text, extra_sbatch_args or [])
        if warning:
            logger.warning("{}", warning)

    service = SnapshotService(profile_name=profile_name, profile=profile)
    try:
        with service.leased_snapshot(plan) as handle:
            prepared = handle.prepared
            script_path = prepared.script_path
            if script_path is None:  # plan always carries a script in sbatch mode
                raise SnapshotValidationError("snapshot plan has no script")
            # Snapshot keys win over a same-named ``--env``.
            job.environment.env_vars.update(prepared.env_vars())
            submitted = rt.job_ops.submit_remote_sbatch(
                script_path,
                submit_cwd=prepared.submit_cwd,
                job_name=job.name if inject_job_name else None,
                extra_sbatch_args=extra_sbatch_args or None,
                callbacks_job=job,
                metadata=handle.metadata(),
            )
            # submit_remote_sbatch always returns the job with its id set.
            assert submitted.job_id is not None
            handle.bind(int(submitted.job_id))
    except SnapshotValidationError as exc:
        raise typer.BadParameter(str(exc)) from exc
    except (SnapshotError, SbatchSubmitError, TransportError) as exc:
        typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(code=1) from exc

    if hasattr(job, "script_path"):
        job.script_path = script_path
    return submitted
