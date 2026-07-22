"""Private helpers backing ``srunx sbatch`` (transport dispatch, flag forwarding,
dry-run sync preview, and the small KEY=VALUE / container-string parsers).

These were siblings of ``sbatch`` in the old monolithic ``main.py``; they live
here so ``commands/jobs.py`` only holds Typer command functions.
"""

import shlex
from pathlib import Path
from typing import Any

import typer
from rich.console import Console

import srunx.slurm.local as _slurm_local  # module-level so ``patch("srunx.slurm.local.Slurm")`` intercepts
from srunx.callbacks import Callback
from srunx.common.logging import get_logger
from srunx.domain import ContainerResource

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
    inject_job_name: bool = True,
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

    ``inject_job_name`` controls whether ``--job-name`` reaches the
    remote sbatch command line. It is ``False`` when the user did not
    type ``-J`` on a positional script, so the script's own ``#SBATCH
    --job-name`` (or SLURM's default) wins instead of being clobbered
    by srunx's logical name. ``job.name`` still carries the resolved
    logical name for display / history regardless — the two concerns
    are deliberately separate. Non-CLI callers keep the default
    ``True`` so workflow / Web / MCP submissions still name their jobs.
    """
    from srunx.common.exceptions import TransportError
    from srunx.domain import ShellJob as _ShellJob
    from srunx.runtime.submission_plan import (
        SubmissionMode,
        plan_sbatch_submission,
    )
    from srunx.sync.lock import SyncLockTimeoutError
    from srunx.sync.service import SyncAbortedError, mount_sync_session

    if rt.transport_type == "local":
        client = _slurm_local.Slurm(callbacks=callbacks)
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
        return rt.job_ops.submit(
            job, submission_context=sub_ctx, inject_job_name=inject_job_name
        )

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
                job_name=job.name if inject_job_name else None,
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


def _job_name_from_tokens(tokens: list[str]) -> str | None:
    """Extract ``--job-name`` / ``-J`` from one ``#SBATCH`` line's tokens.

    Handles the four sbatch spellings — ``--job-name=X`` / ``--job-name X``
    / ``-J X`` / ``-JX`` — and returns the LAST one on the line (matching
    SLURM's last-wins semantics within a directive line). ``None`` when the
    line carries no job-name option.
    """
    result: str | None = None
    i = 0
    while i < len(tokens):
        tok = tokens[i]
        if tok.startswith("--job-name="):
            result = tok.split("=", 1)[1]
        elif tok in ("--job-name", "-J") and i + 1 < len(tokens):
            result = tokens[i + 1]
            i += 1
        elif tok.startswith("-J") and len(tok) > 2:
            result = tok[2:]
        i += 1
    return result or None


def _script_job_name(script_path: Path | str) -> str | None:
    """Return the job name declared by a script's ``#SBATCH`` directives.

    Mirrors ``sbatch``'s own directive scan: read ``#SBATCH`` lines from the
    top of the file, stop at the first non-blank, non-comment line (the first
    executable command), and honour the LAST ``--job-name`` / ``-J`` seen.
    Returns ``None`` when no such directive exists or the file can't be read
    — callers fall back to SLURM's default naming.

    Only the job-name option is parsed (not a general ``#SBATCH`` grammar);
    a ``%x`` / env-expansion inside a name is returned verbatim (best-effort
    — the value is used only for srunx's own display, never re-injected).
    """
    try:
        # ``errors="replace"`` so a non-UTF-8 byte anywhere in the script
        # (e.g. a locale-encoded comment) never raises — such scripts are
        # valid shell and previously submitted fine (rsync is byte-exact,
        # the remote sbatch reads the bytes); this display-only scan must
        # not become a new failure point. ``UnicodeDecodeError`` subclasses
        # ``ValueError``, NOT ``OSError``, so it would otherwise escape.
        text = Path(script_path).read_text(encoding="utf-8", errors="replace")
    except OSError:
        return None

    found: str | None = None
    for raw in text.splitlines():
        stripped = raw.strip()
        if not stripped:
            continue
        if stripped.startswith("#SBATCH"):
            try:
                tokens = shlex.split(stripped[len("#SBATCH") :])
            except ValueError:
                continue
            name = _job_name_from_tokens(tokens)
            if name is not None:
                found = name  # last directive wins, matching SLURM
            continue
        if stripped.startswith("#"):
            continue  # shebang / comment line — keep scanning
        break  # first executable line ends #SBATCH processing
    return found


def _resolve_job_name(
    script: Path | str, cli_name: str, *, cli_name_explicit: bool
) -> tuple[str, bool]:
    """Resolve ``(logical job name, inject-as---job-name?)`` for a positional script.

    Name precedence mirrors ``sbatch``: an explicit CLI ``-J`` wins, else
    the script's own ``#SBATCH --job-name`` / ``-J`` directive, else the
    script's file name (sbatch(1): "The default job name is the name of the
    batch script"). Resolved offline so srunx's CLI display / history match
    the scheduler without a post-submit ``squeue`` / ``sacct`` query.

    The second element says whether to place ``--job-name`` on the sbatch
    command line. Injection is suppressed **only** when the script declares
    its own directive and the user didn't type ``-J`` — then the directive
    must win. With no directive we inject the resolved name so SLURM agrees
    with srunx; this is essential on the temp-upload path, where the script
    is uploaded to a random ``job_<uuid>.sh`` and SLURM's *default* name
    would be that opaque temp filename rather than the source basename.
    """
    if cli_name_explicit:
        return cli_name, True
    directive = _script_job_name(script)
    if directive is not None:
        return directive, False
    return Path(script).name, True


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

    from srunx.cli._helpers.transport import peek_scheduler_key

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
