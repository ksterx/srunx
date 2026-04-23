"""CLI interface for workflow management."""

import contextlib
import os
import signal
import sys
from collections.abc import Iterator
from pathlib import Path
from typing import TYPE_CHECKING, Annotated, Any

import typer
import yaml  # type: ignore
from rich.console import Console

if TYPE_CHECKING:
    from srunx.ssh.core.config import MountConfig

from srunx.callbacks import Callback, NotificationWatchCallback, SlackCallback
from srunx.cli._helpers.transport_options import LocalOpt, ProfileOpt, QuietOpt
from srunx.config import get_config
from srunx.exceptions import WorkflowValidationError
from srunx.logging import configure_workflow_logging, get_logger
from srunx.models import Job, ShellJob, Workflow
from srunx.runner import WorkflowRunner
from srunx.security import find_shell_script_violation
from srunx.sweep import SweepSpec
from srunx.sweep.expand import (
    merge_sweep_specs,
    parse_arg_flags,
    parse_sweep_flags,
)
from srunx.sweep.orchestrator import SweepOrchestrator
from srunx.transport import (
    ResolvedTransport,
    peek_scheduler_key,
    resolve_transport,
)

logger = get_logger(__name__)


def _resolve_sync_flag(sync: bool | None) -> bool:
    """Resolve the effective ``--sync`` value (CLI > config default).

    The Workflow Phase 2 CLI surface mirrors ``srunx sbatch`` here:
    ``None`` means "fall back to ``[sync] auto``" (default true).
    Explicit ``--sync`` / ``--no-sync`` always wins. Kept as a tiny
    free function so the sweep + non-sweep call sites stay readable.
    """
    if sync is not None:
        return sync
    return get_config().sync.auto


def _in_place_context(
    rt: "ResolvedTransport",
    *,
    locked_mount_names: tuple[str, ...] = (),
) -> "Any":
    """Return ``rt.submission_context`` with ``allow_in_place=True``.

    The CLI workflow runner holds the per-(profile, mount) sync lock
    for the entire run via :func:`_hold_workflow_mounts`, so it is
    safe for the SSH adapter to take the IN_PLACE submission path
    inside this run. The flag lives on
    :class:`~srunx.rendering.SubmissionRenderContext` because it
    rides the existing context that the sweep orchestrator and the
    non-sweep runner already pass through to the adapter — adding
    it here avoids touching every executor / Protocol signature.
    Closes Codex blocker #3 on PR #141.

    ``locked_mount_names`` (#143) is a defence-in-depth list of the
    mounts the caller is currently holding the lock for. The SSH
    adapter rejects IN_PLACE for any mount outside this set,
    surfacing aggregation bugs as a clear error rather than a
    silent rsync race. Empty tuple disables enforcement (preserves
    pre-#143 single-workflow callers verbatim).

    Returns ``None`` when there's no context to clone (e.g. local
    transport); callers fall back to ``rt.submission_context``.
    """
    import dataclasses

    if rt.submission_context is None:
        return None
    return dataclasses.replace(
        rt.submission_context,
        allow_in_place=True,
        locked_mount_names=locked_mount_names,
    )


def _expand_sweep_cell_args(
    workflow_data: dict[str, Any],
    args_override: dict[str, Any],
    sweep_spec: SweepSpec,
) -> list[dict[str, Any]]:
    """Expand the matrix into per-cell effective arg dicts.

    Mirrors :meth:`SweepOrchestrator._expand_cells` so the CLI's lock
    aggregation step (#143) sees the same per-cell args the
    orchestrator will later hand to :meth:`WorkflowRunner.from_yaml`.
    Returning an empty list means "no cells to expand"; callers
    should treat that as "fall back to the single-workflow lock-set
    rather than the per-cell union".
    """
    from srunx.sweep.expand import expand_matrix

    base_args: dict[str, Any] = {
        **(workflow_data.get("args") or {}),
        **args_override,
    }
    return expand_matrix(sweep_spec.matrix, base_args)


def _resolve_sweep_locked_mounts(
    rt: "ResolvedTransport",
    cell_args_overrides: list[dict[str, Any]],
    yaml_file: Path,
) -> "list[MountConfig]":
    """Return the union of MountConfigs every sweep cell can touch.

    Drives both the lock-acquisition step (``_hold_workflow_mounts``)
    and the SSH adapter's defence-in-depth check
    (``locked_mount_names`` on :class:`SubmissionRenderContext`) off
    the same rendered set, so the two never disagree about which
    mounts are actually locked. Returns an empty list when no
    profile is bound or the profile doesn't exist — the SSH adapter
    treats that as "no enforcement" which matches the legacy
    pre-#143 behaviour.
    """
    from srunx.runtime.submission_plan import collect_touched_mounts_across_cells
    from srunx.ssh.core.config import ConfigManager

    if rt.profile_name is None:
        return []
    profile = ConfigManager().get_profile(rt.profile_name)
    if profile is None:
        return []
    return collect_touched_mounts_across_cells(
        yaml_file,
        None,
        cell_args_overrides,
        profile,
    )


@contextlib.contextmanager
def _hold_workflow_mounts(
    *,
    rt: "ResolvedTransport",
    workflow_for_mounts: "Workflow | None",
    sync_required: bool,
    explicit_mounts: "list[MountConfig] | None" = None,
) -> Iterator[None]:
    """Acquire per-mount sync locks for every mount the workflow touches.

    Phase 2 (#135): scans the workflow's :class:`ShellJob` ``script_path``
    values for mounts under the resolved SSH profile, then opens a
    :func:`mount_sync_session` for each unique mount via
    :class:`contextlib.ExitStack`. Locks are held across every job
    submission inside the workflow run, closing the same race window
    ``mount_sync_session`` closes for single-job ``sbatch``.

    No-ops when:

    * Transport is local — there's no remote workspace to sync.
    * No profile is bound (legacy direct-hostname path).
    * Workflow has no ShellJobs touching any mount.

    Each mount is rsynced **at most once** even when the workflow
    fans out into many cells (sweep) or many ShellJobs targeting the
    same mount, so a 100-cell sweep doesn't trigger 100 rsyncs.

    ``sync_required=False`` (``--no-sync``) still acquires the locks
    but skips the rsync invocation; this preserves the lock-held
    invariant while letting the user opt out of the transfer.

    Sweep callers (#143) can pass ``explicit_mounts`` to override the
    single-workflow scan with a pre-computed union (typically the
    per-cell mount aggregation from :func:`collect_touched_mounts_across_cells`).
    This avoids re-rendering every cell here when the caller already
    rendered them to compute ``locked_mount_names`` for the SSH
    adapter — both the lock and the safety net see the same mount
    list. When omitted the helper falls back to the single-workflow
    scan and existing non-sweep behaviour is preserved bit-for-bit.
    """
    if (
        rt.transport_type != "ssh"
        or rt.profile_name is None
        or workflow_for_mounts is None
    ):
        yield
        return

    from srunx.runtime.submission_plan import collect_touched_mounts
    from srunx.ssh.core.config import ConfigManager
    from srunx.sync.lock import SyncLockTimeoutError
    from srunx.sync.service import SyncAbortedError, mount_sync_session

    profile = ConfigManager().get_profile(rt.profile_name)
    if profile is None:
        yield
        return

    if explicit_mounts is not None:
        mounts = list(explicit_mounts)
    else:
        mounts = collect_touched_mounts(workflow_for_mounts, profile)
    if not mounts:
        yield
        return

    # Sort by profile.mounts order so concurrent ``srunx flow run``
    # invocations across overlapping mount sets always acquire locks
    # in the same global order, eliminating lock-inversion deadlocks.
    # Codex follow-up #2 on PR #141.
    mount_order = {m.name: i for i, m in enumerate(profile.mounts)}
    mounts.sort(key=lambda m: mount_order.get(m.name, len(mount_order)))

    config = get_config()
    with contextlib.ExitStack() as stack:
        # Acquisition + sync errors must surface as BadParameter so
        # the CLI exits with a clear message. Errors raised from
        # **inside** the workflow body (the ``yield`` block — job
        # failures, sweep cancellations, adapter exceptions) MUST
        # propagate unchanged — wrapping them as "rsync failed"
        # would mask the real failure. Codex blocker #1 on PR #141.
        try:
            for mount in mounts:
                outcome = stack.enter_context(
                    mount_sync_session(
                        profile_name=rt.profile_name,
                        profile=profile,
                        mount=mount,
                        config=config.sync,
                        sync_required=sync_required,
                    )
                )
                if outcome.performed:
                    Console().print(f"⇅  Synced mount [cyan]{mount.name}[/cyan]")
        except SyncAbortedError as exc:
            raise typer.BadParameter(str(exc)) from exc
        except SyncLockTimeoutError as exc:
            raise typer.BadParameter(str(exc)) from exc
        except RuntimeError as exc:
            raise typer.BadParameter(f"rsync failed: {exc}") from exc

        # Body executes here. Any exception escapes the ExitStack
        # which still releases the locks via __exit__, then bubbles
        # up to the workflow CLI's top-level handler unchanged.
        yield


def _build_workflow_callbacks(
    *,
    endpoint: str | None,
    effective_preset: str,
    is_sweep: bool,
    slack: bool,
    debug: bool,
    scheduler_key: str,
) -> list[Callback]:
    """Assemble the callback list for a CLI workflow invocation.

    ``NotificationWatchCallback`` is omitted for sweep runs because the
    orchestrator manages a sweep-level watch + subscription; attaching a
    per-job callback there would spam one notification per cell. The
    ``scheduler_key`` is threaded in so the watch the callback creates
    targets the transport the workflow will actually submit against.

    ``SlackCallback`` is legacy in-process delivery and is still attached
    in both modes for backward compatibility.
    """
    callbacks: list[Callback] = []
    if endpoint and not is_sweep:
        callbacks.append(
            NotificationWatchCallback(
                endpoint_name=endpoint,
                preset=effective_preset,
                scheduler_key=scheduler_key,
            )
        )
    if slack:
        logger.warning(
            "`--slack` is deprecated; configure an endpoint via "
            "Settings → Notifications and pass `--endpoint <name>`."
        )
        webhook_url = os.getenv("SLACK_WEBHOOK_URL")
        if not webhook_url:
            raise ValueError("SLACK_WEBHOOK_URL environment variable is not set")
        callbacks.append(SlackCallback(webhook_url=webhook_url))

    if debug:
        # Imported lazily to avoid pulling main.py's typer surface at
        # module import time.
        from srunx.cli.main import DebugCallback

        callbacks.append(DebugCallback())

    return callbacks


# Create Typer app for workflow management
app = typer.Typer(
    help="Execute YAML-defined workflows using SLURM",
    epilog="""
Example YAML workflow:

  name: ml_pipeline
  jobs:
    - name: preprocess
      command: ["python", "preprocess.py"]
      resources:
        nodes: 1
        gpus_per_node: 2

    - name: train
      path: /path/to/train.sh
      depends_on:
        - preprocess

    - name: evaluate
      command: ["python", "evaluate.py"]
      depends_on:
        - train
      environment:
        conda: ml_env

    - name: upload
      command: ["python", "upload_model.py"]
      depends_on:
        - train
      environment:
        venv: /path/to/venv

    - name: notify
      command: ["python", "notify.py"]
      depends_on:
        - evaluate
        - upload
      environment:
        venv: /path/to/venv

Parameter sweeps:

  # Single-arg override (no sweep)
  srunx flow run --arg dataset=imagenet train.yaml

  # Ad-hoc 3x3 sweep (9 cells, 4 at a time)
  srunx flow run --sweep lr=0.001,0.01,0.1 --sweep seed=1,2,3 \\
      --max-parallel 4 train.yaml

  # Preview cell args without submitting
  srunx flow run --sweep lr=0.001,0.01 --max-parallel 2 --dry-run train.yaml
""",
)


def _load_yaml_sweep(yaml_file: Path) -> tuple[dict[str, Any], SweepSpec | None]:
    """Return ``(raw_yaml_dict, yaml_sweep_spec)``.

    ``yaml_sweep_spec`` is ``None`` when the YAML has no ``sweep:`` block.
    Invalid sweep blocks surface via ``WorkflowValidationError`` so the
    CLI path reports them consistently with the orchestrator.
    """
    with open(yaml_file, encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    if not isinstance(data, dict):
        return {}, None
    raw_sweep = data.get("sweep")
    if raw_sweep is None:
        return data, None
    if not isinstance(raw_sweep, dict):
        raise WorkflowValidationError(
            "YAML sweep: must be a mapping with matrix / fail_fast / max_parallel"
        )
    try:
        spec = SweepSpec.model_validate(raw_sweep)
    except Exception as exc:  # noqa: BLE001 — surface as validation error
        raise WorkflowValidationError(f"invalid YAML sweep block: {exc}") from exc
    return data, spec


def _resolve_endpoint_id(endpoint: str | None) -> int | None:
    """Look up a notification endpoint by name, returning its id or ``None``.

    Matches the attach logic used by :class:`NotificationWatchCallback`
    (lookup by name, skip when missing or disabled) so sweep runs honour
    ``--endpoint`` the same way non-sweep runs do.
    """
    if not endpoint:
        return None
    try:
        from srunx.db.connection import open_connection
        from srunx.db.repositories.endpoints import EndpointRepository
    except ImportError:  # pragma: no cover — DB module unavailable
        return None
    conn = open_connection()
    try:
        row = EndpointRepository(conn).get_by_name("slack_webhook", endpoint)
    finally:
        conn.close()
    if row is None or row.disabled_at is not None or row.id is None:
        return None
    return row.id


def _preview_sweep_dry_run(
    sweep_spec: SweepSpec,
    workflow_data: dict[str, Any],
    args_override: dict[str, Any],
) -> None:
    """Expand the matrix and print cell count / per-cell args without executing."""
    from srunx.sweep.expand import expand_matrix

    base_args: dict[str, Any] = {
        **(workflow_data.get("args") or {}),
        **args_override,
    }
    cells = expand_matrix(sweep_spec.matrix, base_args)
    console = Console()
    console.print("🔍 Sweep dry run:")
    console.print(f"  Cell count: {len(cells)}")
    console.print(f"  Matrix: {sweep_spec.matrix}")
    console.print(f"  Max parallel: {sweep_spec.max_parallel}")
    console.print(f"  Fail fast: {sweep_spec.fail_fast}")
    for i, cell in enumerate(cells):
        console.print(f"  Cell {i}: {cell}")


def _validate_all_sweep_cells(
    *,
    yaml_file: Path,
    workflow_data: dict[str, Any],
    args_override: dict[str, Any],
    sweep_spec: SweepSpec,
    callbacks: list[Callback],
) -> None:
    """Load + validate the workflow once per expanded matrix cell.

    Reuses :func:`srunx.sweep.expand.expand_matrix` to produce the same
    per-cell arg overlay the orchestrator will feed into
    :meth:`WorkflowRunner.from_yaml` at execution time. The first cell
    that fails Jinja rendering or workflow validation is reported with
    its index and effective args, matching the error shape used by the
    non-sweep validator.
    """
    from srunx.sweep.expand import expand_matrix

    base_args: dict[str, Any] = {
        **(workflow_data.get("args") or {}),
        **args_override,
    }
    cells = expand_matrix(sweep_spec.matrix, base_args)

    for idx, cell_args in enumerate(cells):
        try:
            runner = WorkflowRunner.from_yaml(
                yaml_file, callbacks=callbacks, args_override=cell_args
            )
            runner.workflow.validate()
        except WorkflowValidationError as exc:
            raise WorkflowValidationError(
                f"cell {idx} args={cell_args}: {exc}"
            ) from exc


def _enforce_shell_script_roots_cli(workflow: Workflow, rt: ResolvedTransport) -> None:
    """Reject ShellJob script paths that escape the SSH profile's mounts.

    Mirrors the Web router and MCP tool guards (see
    :func:`srunx.web.routers.workflows._enforce_shell_script_roots` and
    :func:`srunx.mcp.server._enforce_shell_script_roots_mcp`): when the
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


def _run_sweep(
    yaml_file: Path,
    workflow_data: dict[str, Any],
    args_override: dict[str, Any],
    sweep_spec: SweepSpec,
    callbacks: list[Callback],
    endpoint_id: int | None,
    preset: str,
    rt: ResolvedTransport,
    submission_context: Any | None = None,
) -> None:
    """Drive :class:`SweepOrchestrator` under a SIGINT → request_cancel guard.

    First Ctrl+C triggers :meth:`SweepOrchestrator.request_cancel` (drain
    pending cells). A second Ctrl+C re-raises ``KeyboardInterrupt`` so
    the user can escape a stuck orchestrator.

    The resolved transport's ``executor_factory`` and ``submission_context``
    are forwarded to the orchestrator so SSH-backed sweeps route each
    cell through the pool + mount translation path already used by the
    Web / MCP sweep surfaces.
    """
    # Caller may supply an override (CLI workflow Phase 2 passes one
    # with ``allow_in_place=True`` after the workflow lock is held).
    # When omitted, fall back to the transport's default context.
    effective_context = (
        submission_context if submission_context is not None else rt.submission_context
    )
    orchestrator = SweepOrchestrator(
        workflow_yaml_path=yaml_file,
        workflow_data=workflow_data,
        args_override=args_override or None,
        sweep_spec=sweep_spec,
        submission_source="cli",
        callbacks=callbacks,
        endpoint_id=endpoint_id,
        preset=preset,
        executor_factory=rt.executor_factory,
        submission_context=effective_context,
    )

    original_handler = signal.getsignal(signal.SIGINT)
    cancel_requested = {"n": 0}

    def _on_sigint(signum: int, frame: Any) -> None:
        cancel_requested["n"] += 1
        if cancel_requested["n"] == 1:
            logger.warning(
                "Received Ctrl+C — requesting sweep cancel. Press Ctrl+C again to abort."
            )
            try:
                orchestrator.request_cancel()
            except Exception:  # noqa: BLE001
                logger.warning("request_cancel raised", exc_info=True)
            # Restore default so the second Ctrl+C raises KeyboardInterrupt.
            signal.signal(signal.SIGINT, signal.default_int_handler)

    signal.signal(signal.SIGINT, _on_sigint)
    try:
        sweep_run = orchestrator.run()
    finally:
        signal.signal(signal.SIGINT, original_handler)

    logger.info(
        f"Sweep {sweep_run.id} finished: status={sweep_run.status} "
        f"cells={sweep_run.cell_count} completed={sweep_run.cells_completed} "
        f"failed={sweep_run.cells_failed} cancelled={sweep_run.cells_cancelled}"
    )


def _execute_workflow(
    yaml_file: Path,
    validate: bool = False,
    dry_run: bool = False,
    log_level: str = "INFO",
    slack: bool = False,
    endpoint: str | None = None,
    preset: str | None = None,
    from_job: str | None = None,
    to_job: str | None = None,
    job: str | None = None,
    arg: list[str] | None = None,
    sweep: list[str] | None = None,
    fail_fast: bool = False,
    max_parallel: int | None = None,
    debug: bool = False,
    profile: str | None = None,
    local: bool = False,
    quiet: bool = False,
    sync: bool | None = None,
) -> None:
    """Common workflow execution logic."""
    # Configure logging for workflow execution
    configure_workflow_logging(level=log_level)

    if job and (from_job or to_job):
        logger.error("❌ Cannot use --job with --from or --to options")
        sys.exit(1)

    try:
        if not yaml_file.exists():
            logger.error(f"Workflow file not found: {yaml_file}")
            sys.exit(1)

        # Parse --arg / --sweep; errors here are WorkflowValidationError.
        args_override = parse_arg_flags(arg or [])
        cli_sweep_axes_raw = parse_sweep_flags(sweep or [])
        # Narrow to ScalarValue lists (CLI values are str).
        cli_sweep_axes: dict[str, list[Any]] = {
            k: list(v) for k, v in cli_sweep_axes_raw.items()
        }

        workflow_data, yaml_sweep_spec = _load_yaml_sweep(yaml_file)

        # Only pass a bool when the user set --fail-fast; otherwise let
        # YAML decide (merge_sweep_specs treats ``None`` as "unset").
        cli_fail_fast: bool | None = True if fail_fast else None

        sweep_spec = merge_sweep_specs(
            yaml_sweep_spec,
            cli_sweep_axes,
            args_override,
            cli_fail_fast,
            max_parallel,
        )

        # Build callbacks BEFORE resolve_transport so the SSH adapter +
        # pool both see the full callback list at construction time (the
        # pool copies ``callbacks`` into ``_callbacks`` in ``__init__``,
        # so a post-hoc mutation wouldn't reach pooled clones). The
        # scheduler_key the transport will pick is deterministic from
        # the CLI flags + env, so we can pre-compute it via
        # :func:`peek_scheduler_key` and bind it to
        # ``NotificationWatchCallback`` up front.
        pre_scheduler_key = peek_scheduler_key(profile=profile, local=local)
        effective_preset = preset or get_config().notifications.default_preset
        is_sweep = sweep_spec is not None
        callbacks = _build_workflow_callbacks(
            endpoint=endpoint,
            effective_preset=effective_preset,
            is_sweep=is_sweep,
            slack=slack,
            debug=debug,
            scheduler_key=pre_scheduler_key,
        )

        # Resolve transport once for the whole invocation. The context
        # manager closes any SSH pool on exit. Validation / dry-run /
        # sweep / single-run all branch inside this scope so every path
        # sees the same executor_factory + submission_context.
        with resolve_transport(
            profile=profile,
            local=local,
            quiet=quiet,
            callbacks=callbacks,
            submission_source="cli",
        ) as rt:
            _warn_missing_mounts(rt)

            if sweep_spec is not None:
                if validate:
                    # Validate every matrix cell: a Jinja undefined or type
                    # error in a non-zero cell (e.g. only cell 3 references
                    # an undeclared arg) must still fail the validator.
                    _validate_all_sweep_cells(
                        yaml_file=yaml_file,
                        workflow_data=workflow_data,
                        args_override=args_override,
                        sweep_spec=sweep_spec,
                        callbacks=callbacks,
                    )
                    logger.info("Workflow validation successful")
                    return

                if dry_run:
                    _preview_sweep_dry_run(sweep_spec, workflow_data, args_override)
                    return

                # Enforce ShellJob script_path guard against the profile's
                # mounts before any cell is submitted. Load the workflow
                # once with empty cell args for the check; the orchestrator
                # will reload per cell with its own args.
                if rt.transport_type == "ssh":
                    _check_runner = WorkflowRunner.from_yaml(
                        yaml_file,
                        callbacks=callbacks,
                        args_override=args_override or None,
                    )
                    _enforce_shell_script_roots_cli(_check_runner.workflow, rt)

                endpoint_id = _resolve_endpoint_id(endpoint)
                # Phase 2 (#135): hold the per-(profile, mount) sync
                # lock across every cell in the sweep so a concurrent
                # invocation can't rsync mid-sweep, AND so the rsync
                # itself runs once per mount instead of once per cell.
                #
                # Issue #143: when the sweep matrix can influence
                # ``ShellJob.script_path`` (e.g. ``path: "{{ scratch
                # }}/{{ seed }}/run.sh"``), the base render alone
                # misses mounts only a non-base cell can resolve to.
                # Expand the matrix here, render each cell, and union
                # the touched mounts so the lock-set covers every
                # cell. Then it is safe to flip ``allow_in_place=True``
                # for the sweep path.
                cell_args_overrides = _expand_sweep_cell_args(
                    workflow_data, args_override, sweep_spec
                )
                # Render every cell once, take the union of touched
                # mounts, and feed the SAME list to both the lock-
                # acquisition step and the SSH adapter's defence-in-
                # depth check. Sharing the resolved list avoids a
                # second N-cell render pass and guarantees the two
                # views can never disagree.
                sweep_locked_mounts = (
                    _resolve_sweep_locked_mounts(rt, cell_args_overrides, yaml_file)
                    if rt.transport_type == "ssh"
                    else []
                )
                sweep_locked_names = tuple(m.name for m in sweep_locked_mounts)
                with _hold_workflow_mounts(
                    rt=rt,
                    workflow_for_mounts=_check_runner.workflow
                    if rt.transport_type == "ssh"
                    else None,
                    sync_required=_resolve_sync_flag(sync),
                    explicit_mounts=sweep_locked_mounts
                    if rt.transport_type == "ssh"
                    else None,
                ):
                    # Lock-set is now sound for every cell — flip
                    # ``allow_in_place`` for the sweep so each cell
                    # whose rendered ShellJob bytes match its source
                    # can take the IN_PLACE shortcut on the remote.
                    # The locked-mount tuple rides the context as a
                    # defence-in-depth rejection signal: a buggy cell
                    # that escapes the aggregation hits a clear
                    # "mount X not locked" error in the SSH adapter
                    # instead of silently racing rsync.
                    #
                    # When ``sweep_locked_mounts`` is empty AND we're
                    # on SSH, we deliberately keep ``allow_in_place``
                    # at its default ``False``: an empty union means
                    # either no cell touches a mount (nothing to flip
                    # for) or the profile lookup failed (in which case
                    # we never held the lock — flipping the flag here
                    # would let the adapter race rsync against an
                    # unsynchronised remote). Both outcomes match the
                    # safer pre-#143 sweep behaviour.
                    if rt.transport_type == "ssh" and sweep_locked_mounts:
                        sweep_submission_context = _in_place_context(
                            rt, locked_mount_names=sweep_locked_names
                        )
                    else:
                        sweep_submission_context = rt.submission_context
                    _run_sweep(
                        yaml_file=yaml_file,
                        workflow_data=workflow_data,
                        args_override=args_override,
                        sweep_spec=sweep_spec,
                        callbacks=callbacks,
                        endpoint_id=endpoint_id,
                        preset=effective_preset,
                        rt=rt,
                        submission_context=sweep_submission_context,
                    )
                return

            # Non-sweep path.
            runner = WorkflowRunner.from_yaml(
                yaml_file,
                callbacks=callbacks,
                single_job=job,
                args_override=args_override or None,
                executor_factory=rt.executor_factory,
                submission_context=rt.submission_context,
            )

            runner.workflow.validate()
            _enforce_shell_script_roots_cli(runner.workflow, rt)

            if validate:
                logger.info("Workflow validation successful")
                return

            if dry_run:
                console = Console()
                console.print("🔍 Dry run mode - showing workflow structure:")
                console.print(f"Workflow: {runner.workflow.name}")

                jobs_to_execute = runner._get_jobs_to_execute(from_job, to_job, job)

                if job:
                    console.print(f"Executing single job: {job}")
                elif from_job or to_job:
                    range_info = []
                    if from_job:
                        range_info.append(f"from {from_job}")
                    if to_job:
                        range_info.append(f"to {to_job}")
                    console.print(
                        f"Executing jobs {' '.join(range_info)}: "
                        f"{len(jobs_to_execute)} jobs"
                    )
                else:
                    console.print(f"Executing all jobs: {len(jobs_to_execute)} jobs")

                for job_obj in jobs_to_execute:
                    if isinstance(job_obj, Job) and job_obj.command:
                        command_str = (
                            job_obj.command
                            if isinstance(job_obj.command, str)
                            else " ".join(job_obj.command or [])
                        )
                    elif isinstance(job_obj, ShellJob):
                        command_str = f"Shell script: {job_obj.script_path}"
                    else:
                        command_str = "N/A"
                    console.print(f"  - {job_obj.name}: {command_str}")
                return

            # Execute workflow.
            #
            # Phase 2 (#135): wrap the call in ``_hold_workflow_mounts``
            # so each mount that any ShellJob's ``script_path`` lives
            # under is rsynced **once** at the start, and the per-mount
            # lock is held across every job in the workflow. The lock
            # serialisation closes the same race
            # ``mount_sync_session`` closes for single-job ``sbatch``.
            with _hold_workflow_mounts(
                rt=rt,
                workflow_for_mounts=runner.workflow,
                sync_required=_resolve_sync_flag(sync),
            ):
                # Lock is held → flip ``allow_in_place`` so the SSH
                # adapter's ShellJob branch is permitted to take the
                # IN_PLACE path. Mutate the runner's stored context
                # rather than re-building the runner so all the
                # per-job state (parsed dependencies, etc.) survives.
                if rt.transport_type == "ssh":
                    in_place_ctx = _in_place_context(rt)
                    if in_place_ctx is not None:
                        runner._submission_context = in_place_ctx  # noqa: SLF001
                results = runner.run(from_job=from_job, to_job=to_job, single_job=job)

            logger.info("Job Results:")
            for task_name, job_result in results.items():
                if hasattr(job_result, "job_id") and job_result.job_id:
                    logger.info(f"  {task_name}: Job ID {job_result.job_id}")
                else:
                    logger.info(f"  {task_name}: {job_result}")

    except WorkflowValidationError as e:
        logger.error(f"❌ Workflow validation error: {e}")
        sys.exit(1)
    except FileNotFoundError as e:
        logger.error(f"❌ Workflow file not found: {e}")
        sys.exit(1)
    except PermissionError as e:
        logger.error(f"❌ Permission denied: {e}")
        logger.error("💡 Check if you have write permissions to the target directories")
        sys.exit(1)
    except OSError as e:
        if e.errno == 30:  # Read-only file system
            logger.error(f"❌ Cannot write to read-only file system: {e}")
            logger.error(
                "💡 The target directory appears to be read-only. Check mount permissions."
            )
        else:
            logger.error(f"❌ System error: {e}")
        sys.exit(1)
    except ImportError as e:
        logger.error(f"❌ Missing dependency: {e}")
        logger.error(
            "💡 Make sure all required packages are installed in your environment"
        )
        sys.exit(1)
    except Exception as e:
        logger.error(f"❌ Workflow execution failed: {e}")
        logger.error(f"💡 Error type: {type(e).__name__}")
        import traceback

        logger.error("📍 Error location:")
        logger.error(traceback.format_exc())
        sys.exit(1)


@app.command(name="run")
def run_command(
    yaml_file: Annotated[
        Path, typer.Argument(help="Path to YAML workflow definition file")
    ],
    validate: Annotated[
        bool,
        typer.Option(
            "--validate", help="Only validate the workflow file without executing"
        ),
    ] = False,
    dry_run: Annotated[
        bool,
        typer.Option(
            "--dry-run", help="Show what would be executed without running jobs"
        ),
    ] = False,
    log_level: Annotated[
        str, typer.Option("--log-level", help="Set logging level")
    ] = "INFO",
    slack: Annotated[
        bool, typer.Option("--slack", help="Send notifications to Slack")
    ] = False,
    endpoint: Annotated[
        str | None,
        typer.Option(
            "--endpoint",
            help=(
                "Name of a configured notification endpoint (see "
                "`/api/endpoints` / Settings UI). Attaches a watch per "
                "submitted job via the poller pipeline."
            ),
        ),
    ] = None,
    preset: Annotated[
        str | None,
        typer.Option(
            "--preset",
            help=(
                "Subscription preset for --endpoint: terminal (default), "
                "running_and_terminal, or all."
            ),
        ),
    ] = None,
    from_job: Annotated[
        str | None,
        typer.Option(
            "--from",
            help="Start execution from this job (ignoring dependencies before this job)",
        ),
    ] = None,
    to_job: Annotated[
        str | None, typer.Option("--to", help="Stop execution at this job (inclusive)")
    ] = None,
    job: Annotated[
        str | None,
        typer.Option(
            "--job", help="Execute only this specific job (ignoring all dependencies)"
        ),
    ] = None,
    arg: Annotated[
        list[str] | None,
        typer.Option(
            "--arg",
            help=(
                "Override workflow args (KEY=VALUE). Repeat to set multiple; "
                "e.g. --arg lr=0.01 --arg dataset=imagenet."
            ),
        ),
    ] = None,
    sweep: Annotated[
        list[str] | None,
        typer.Option(
            "--sweep",
            help=(
                "Add a sweep matrix axis (KEY=v1,v2,v3). Repeat to cross "
                "multiple axes; e.g. --sweep lr=0.001,0.01,0.1 "
                "--sweep seed=1,2,3. Requires --max-parallel."
            ),
        ),
    ] = None,
    fail_fast: Annotated[
        bool,
        typer.Option(
            "--fail-fast",
            help="Cancel remaining sweep cells after the first failure",
        ),
    ] = False,
    max_parallel: Annotated[
        int | None,
        typer.Option(
            "--max-parallel",
            help="Maximum concurrent sweep cells (overrides YAML sweep.max_parallel)",
        ),
    ] = None,
    sync: Annotated[
        bool | None,
        typer.Option(
            "--sync/--no-sync",
            help=(
                "Rsync each touched mount once at the start of the run "
                "(default ``[sync] auto``, true unless disabled). "
                "``--no-sync`` skips the rsync but still acquires the "
                "per-mount lock for race-free submission."
            ),
        ),
    ] = None,
    profile: ProfileOpt = None,
    local: LocalOpt = False,
    quiet: QuietOpt = False,
) -> None:
    """Execute workflow from YAML file."""
    _execute_workflow(
        yaml_file=yaml_file,
        validate=validate,
        dry_run=dry_run,
        log_level=log_level,
        slack=slack,
        endpoint=endpoint,
        preset=preset,
        from_job=from_job,
        to_job=to_job,
        job=job,
        arg=arg,
        sweep=sweep,
        fail_fast=fail_fast,
        max_parallel=max_parallel,
        profile=profile,
        local=local,
        quiet=quiet,
        sync=sync,
    )


def main() -> None:
    """Main entry point for workflow CLI."""
    app()


if __name__ == "__main__":
    main()
