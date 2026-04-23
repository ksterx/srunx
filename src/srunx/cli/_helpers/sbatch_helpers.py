"""Private helpers backing ``srunx sbatch`` (transport dispatch, flag forwarding,
dry-run sync preview, and the small KEY=VALUE / container-string parsers).

These were siblings of ``sbatch`` in the old monolithic ``main.py``; they live
here so ``commands/jobs.py`` only holds Typer command functions.
"""

import sys
from pathlib import Path
from typing import Any

import typer
from rich.console import Console

from srunx.callbacks import Callback
from srunx.logging import get_logger
from srunx.models import ContainerResource

logger = get_logger(__name__)


def _submit_via_transport(
    *,
    rt: Any,
    job: Any,
    script_path: Path | None,
    profile_name: str | None,
    sync_flag: bool | None,
    template: str | None,
    verbose: bool,
    callbacks: list[Callback],
    config: Any,
    extra_sbatch_args: list[str] | None = None,
    force_sync: bool = False,
) -> Any:
    """Dispatch a submit to the right adapter method + optional mount sync.

    Local transport keeps the rich ``Slurm.submit`` signature
    (callbacks + template_path + verbose). The SSH transport goes
    through :func:`srunx.runtime.submission_plan.plan_sbatch_submission`
    to decide between:

    * IN_PLACE: rsync the owning mount (unless ``--no-sync``),
      translate to the remote path, and invoke
      ``rt.job_ops.submit_remote_sbatch`` — the script stays where
      the user edits it, preserving their own ``#SBATCH`` directives.
      The per-mount sync lock is held across both rsync and sbatch
      so a concurrent CLI invocation can't rsync stale bytes between
      our sync and our submission (Codex blocker #3).
    * TEMP_UPLOAD: fall through to ``rt.job_ops.submit`` which
      uploads a rendered script into ``$SRUNX_TEMP_DIR`` (legacy).

    ``is_rendered_artifact`` is True when the caller forced a template
    render (``--template <name>``): even if the positional script
    happens to sit under a mount, the submitted bytes came from the
    template engine, not the on-disk source, so running "in place"
    would execute the wrong thing.

    ``extra_sbatch_args`` are CLI-side resource flags (``-N`` /
    ``--gres=gpu:N`` / etc.) that need to reach the cluster's
    ``sbatch`` command line in IN_PLACE mode. SLURM treats them as
    overrides of the script's ``#SBATCH`` directives, matching real
    sbatch's precedence. Closes Codex blocker #1: previously these
    flags silently no-op'd in ShellJob (positional-script) mode.
    """
    from srunx.exceptions import TransportError
    from srunx.models import ShellJob as _ShellJob
    from srunx.runtime.submission_plan import (
        SubmissionMode,
        plan_sbatch_submission,
    )
    from srunx.sync.lock import SyncLockTimeoutError
    from srunx.sync.service import SyncAbortedError, mount_sync_session

    if rt.transport_type == "local":
        # Resolve ``Slurm`` via the ``srunx.cli.main`` module so
        # ``patch("srunx.cli.main.Slurm")`` in tests continues to
        # intercept the instantiation from this helper.
        _main_module = sys.modules["srunx.cli.main"]
        client = _main_module.Slurm(callbacks=callbacks)
        return client.submit(job, template_path=template, verbose=verbose)

    # --- SSH transport ---
    sub_ctx = rt.submission_context
    effective_sync = config.sync.auto if sync_flag is None else sync_flag
    is_rendered_artifact = template is not None

    from srunx.ssh.core.config import ConfigManager

    profile = ConfigManager().get_profile(profile_name) if profile_name else None
    plan = plan_sbatch_submission(
        script_path=script_path,
        profile=profile,
        cwd=Path.cwd(),
        sync_enabled=effective_sync,
        is_rendered_artifact=is_rendered_artifact,
    )

    for w in plan.warnings:
        logger.warning(w)

    if plan.mode == SubmissionMode.TEMP_UPLOAD:
        return rt.job_ops.submit(job, submission_context=sub_ctx)

    # IN_PLACE branch: hold the per-(profile,mount) lock across the
    # entire sync + sbatch handoff so a concurrent invocation can't
    # rsync different bytes in between.
    assert plan.mount is not None
    assert plan.remote_script_path is not None
    assert profile_name is not None and profile is not None

    if not hasattr(rt.job_ops, "submit_remote_sbatch"):
        raise TransportError(
            "Current transport does not support in-place submission; "
            "re-run with --no-sync to force the legacy tmp-upload path."
        )

    # We split the try/except across the sync phase and the submit
    # phase so a sbatch failure can never wear an "rsync failed"
    # error message. Codex follow-up on PR #134.
    try:
        sync_ctx = mount_sync_session(
            profile_name=profile_name,
            profile=profile,
            mount=plan.mount,
            config=config.sync,
            sync_required=plan.sync_required,
            force_sync=force_sync,
            verbose=verbose,
            # Per-script hash verification (#137 part 5): the local
            # source of truth for the file we're about to ``sbatch``.
            # Gated upstream by ``config.sync.verify_remote_hash``;
            # passing the path unconditionally keeps the CLI ignorant
            # of that flag.
            verify_paths=[str(script_path)] if script_path is not None else None,
        )
        sync_ctx_entered = sync_ctx.__enter__()
    except SyncAbortedError as exc:
        raise typer.BadParameter(str(exc)) from exc
    except SyncLockTimeoutError as exc:
        raise typer.BadParameter(str(exc)) from exc
    except RuntimeError as exc:
        raise typer.BadParameter(f"rsync failed: {exc}") from exc

    try:
        if sync_ctx_entered.performed:
            Console().print(f"⇅  Synced mount [cyan]{plan.mount.name}[/cyan]")
        try:
            submitted = rt.job_ops.submit_remote_sbatch(
                plan.remote_script_path,
                submit_cwd=plan.submit_cwd,
                job_name=job.name,
                extra_sbatch_args=extra_sbatch_args or None,
                callbacks_job=job,
            )
        except RuntimeError as exc:
            # In-place sbatch failure: surface the underlying message
            # verbatim. Distinct from the "rsync failed" wrapper above
            # so users can tell which phase failed.
            raise typer.BadParameter(f"sbatch failed: {exc}") from exc
    finally:
        sync_ctx.__exit__(None, None, None)

    # Re-mutate the original ShellJob so the wait/notification watch
    # path (which reads job_id off the original instance the caller
    # constructed) sees the post-submit state.
    if isinstance(job, _ShellJob):
        job.script_path = plan.remote_script_path
    return submitted


_SBATCH_FLAG_BY_PARAM: dict[str, str] = {
    "nodes": "--nodes",
    "gpus_per_node": "--gpus-per-node",
    "ntasks_per_node": "--ntasks-per-node",
    "cpus_per_task": "--cpus-per-task",
    "memory": "--mem",
    "time": "--time",
    "nodelist": "--nodelist",
    "partition": "--partition",
    "work_dir": "--chdir",
}


def _build_extra_sbatch_args(
    ctx: typer.Context,
    *,
    values: dict[str, object],
    log_dir_user: str | None,
) -> list[str]:
    """Forward CLI-typed flags to ``sbatch`` for ShellJob mode.

    "CLI-typed" means the user wrote the flag on the command line —
    determined via Click's :meth:`Context.get_parameter_source`. We
    deliberately do NOT compare against defaults because that
    confuses three different cases:

    * ``srunx sbatch script.sh`` — no flag typed, planner default 1.
    * ``srunx sbatch script.sh --nodes 1`` — explicit 1, must
      override any ``#SBATCH --nodes=8`` in the script.
    * ``srunx sbatch script.sh`` with config providing ``work_dir``
      — config injected, user did NOT type ``-D``, so the script's
      ``#SBATCH --chdir=`` (if any) wins.

    The default-comparison heuristic the previous version used got
    all three confused — Codex follow-up on PR #134.

    ``log_dir_user`` is passed in separately because the sbatch flag
    expansion (``--output=`` + ``--error=``) builds two args from one
    typed value, and the conversion lives at the call site (caller
    knows the configured default to suppress).
    """
    from click.core import ParameterSource

    args: list[str] = []
    for param_name, sbatch_flag in _SBATCH_FLAG_BY_PARAM.items():
        try:
            source = ctx.get_parameter_source(param_name)
        except (LookupError, AttributeError):
            # No such parameter; defensive against signature drift.
            source = None
        if source != ParameterSource.COMMANDLINE:
            continue
        value = values.get(param_name)
        if value is None or value == "":
            continue
        args.append(f"{sbatch_flag}={value}")

    if log_dir_user:
        # ``--log-dir`` was explicitly typed; expand into the
        # ``--output`` + ``--error`` pair sbatch expects.
        args.append(f"--output={log_dir_user}/%x_%j.log")
        args.append(f"--error={log_dir_user}/%x_%j.log")

    return args


def _parse_gres_gpu(gres: str | None) -> int | None:
    """Parse a sbatch-style ``--gres=gpu:N`` value into an integer GPU count.

    Returns ``None`` for falsy input; raises :class:`typer.BadParameter`
    when the resource type is not ``gpu`` or the count is not a positive
    integer. The intent is to accept the most common SLURM convention
    (``--gres=gpu:N``) so ``srunx sbatch`` reads identically to
    ``sbatch``; richer gres forms (``gpu:tesla:2`` etc.) are out of
    scope for this minimal compatibility layer.
    """
    if not gres:
        return None
    parts = gres.split(":")
    if len(parts) != 2 or parts[0] != "gpu":
        raise typer.BadParameter(
            f"--gres only supports 'gpu:N' form (got {gres!r}).",
            param_hint="--gres",
        )
    try:
        count = int(parts[1])
    except ValueError as exc:
        raise typer.BadParameter(
            f"--gres gpu count must be an integer (got {parts[1]!r}).",
            param_hint="--gres",
        ) from exc
    if count < 0:
        raise typer.BadParameter(
            "--gres gpu count must be non-negative.", param_hint="--gres"
        )
    return count


def _print_in_place_sync_preview(
    *,
    console: Console,
    script: Path | None,
    profile_name: str | None,
    local: bool,
    sync_flag: bool | None,
    config: Any,
) -> None:
    """Show the rsync ``-n -i`` preview for an SSH in-place dry-run.

    Quietly no-ops in every "this isn't an in-place candidate" case
    (local transport, no positional script, no resolvable profile, no
    profile mounts, script not under any mount). Failures from the
    rsync subprocess itself are caught and surfaced as a single
    coloured line — the preview is best-effort and must never abort
    the larger ``--dry-run`` flow.
    """
    if local or script is None:
        return

    from srunx.transport import peek_scheduler_key

    try:
        sched_key = peek_scheduler_key(profile=profile_name, local=local)
    except typer.BadParameter:
        # ``--profile foo --local`` conflict — already surfaced by the
        # main resolution path; nothing more to add here.
        return

    if not sched_key.startswith("ssh:"):
        return

    resolved_profile_name = sched_key[len("ssh:") :]

    from srunx.runtime.submission_plan import resolve_mount_for_path
    from srunx.ssh.core.config import ConfigManager

    profile = ConfigManager().get_profile(resolved_profile_name)
    if profile is None or not profile.mounts:
        return

    mount = resolve_mount_for_path(script, profile)
    if mount is None:
        return

    sync_enabled = config.sync.auto if sync_flag is None else sync_flag
    if not sync_enabled:
        console.print(f"  Sync: skipped (--no-sync) for mount '{mount.name}'")
        return

    console.print(f"  Sync preview for mount [cyan]{mount.name}[/cyan]:")
    try:
        from srunx.sync.mount_helpers import sync_mount_by_name

        output = sync_mount_by_name(profile, mount.name, dry_run=True)
    except RuntimeError as exc:
        console.print(f"    [red]rsync preview failed: {exc}[/red]")
        return

    if not output.strip():
        console.print("    (no changes — remote already up to date)")
        return
    for line in output.splitlines():
        console.print(f"    {line}")


def _parse_env_vars(env_var_list: list[str] | None) -> dict[str, str]:
    """Parse environment variables from list of KEY=VALUE strings."""
    if not env_var_list:
        return {}

    env_vars = {}
    for env_str in env_var_list:
        if "=" not in env_str:
            raise ValueError(f"Invalid environment variable format: {env_str}")
        key, value = env_str.split("=", 1)
        env_vars[key] = value
    return env_vars


def _parse_bool(value: str) -> bool:
    """Parse a boolean string value."""
    return value.lower() in ("true", "1", "yes")


def _parse_container_args(container_arg: str | None) -> ContainerResource | None:
    """Parse container argument into ContainerResource.

    Supports simple image path or key=value pairs separated by commas:
      image=<path>, mounts=<m1>;<m2>, bind=<m1>;<m2> (alias for mounts),
      workdir=<path>, runtime=<name>, nv=true, rocm=true, cleanenv=true,
      fakeroot=true, writable_tmpfs=true, overlay=<path>,
      env=KEY1=VAL1;KEY2=VAL2
    """
    if not container_arg:
        return None

    # Simple case: just image path (no commas, no braces, no key=value)
    if not container_arg.startswith("{") and "," not in container_arg:
        # Check if it looks like a bare key=value (e.g. "runtime=apptainer")
        if "=" in container_arg:
            first_key = container_arg.split("=", 1)[0]
            known_keys = {
                "image",
                "mounts",
                "bind",
                "workdir",
                "runtime",
                "nv",
                "rocm",
                "cleanenv",
                "fakeroot",
                "writable_tmpfs",
                "overlay",
                "env",
            }
            if first_key not in known_keys:
                return ContainerResource(image=container_arg)
        else:
            return ContainerResource(image=container_arg)

    # Complex case: parse key=value pairs
    kwargs: dict[str, Any] = {}
    raw = container_arg
    if raw.startswith("{") and raw.endswith("}"):
        raw = raw[1:-1]

    for pair in raw.split(","):
        if "=" not in pair:
            continue
        key, value = pair.strip().split("=", 1)

        match key:
            case "image":
                kwargs["image"] = value
            case "mounts" | "bind":
                kwargs["mounts"] = value.split(";")
            case "workdir":
                kwargs["workdir"] = value
            case "runtime":
                kwargs["runtime"] = value
            case "nv":
                kwargs["nv"] = _parse_bool(value)
            case "rocm":
                kwargs["rocm"] = _parse_bool(value)
            case "cleanenv":
                kwargs["cleanenv"] = _parse_bool(value)
            case "fakeroot":
                kwargs["fakeroot"] = _parse_bool(value)
            case "writable_tmpfs":
                kwargs["writable_tmpfs"] = _parse_bool(value)
            case "overlay":
                kwargs["overlay"] = value
            case "env":
                env_dict: dict[str, str] = {}
                for env_pair in value.split(";"):
                    if "=" in env_pair:
                        ek, ev = env_pair.split("=", 1)
                        env_dict[ek] = ev
                kwargs["env"] = env_dict

    if kwargs:
        return ContainerResource(**kwargs)
    else:
        return ContainerResource(image=container_arg)
