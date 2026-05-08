"""Sweep-related helpers for CLI workflow runs.

Includes matrix expansion, per-cell mount aggregation, dry-run preview,
per-cell validation, and the SIGINT-aware sweep runner that drives
:class:`SweepOrchestrator`.
"""

from __future__ import annotations

import signal
from pathlib import Path
from typing import TYPE_CHECKING, Any

from rich.console import Console

from srunx.callbacks import Callback
from srunx.common.exceptions import WorkflowValidationError
from srunx.common.logging import get_logger
from srunx.runtime.sweep import SweepSpec
from srunx.runtime.sweep.orchestrator import SweepOrchestrator
from srunx.runtime.workflow.runner import WorkflowRunner
from srunx.transport import ResolvedTransport

if TYPE_CHECKING:
    from srunx.ssh.core.config import MountConfig

logger = get_logger(__name__)


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
    from srunx.runtime.sweep.expand import expand_matrix

    base_args: dict[str, Any] = {
        **(workflow_data.get("args") or {}),
        **args_override,
    }
    return expand_matrix(sweep_spec.matrix, base_args)


def _resolve_sweep_locked_mounts(
    rt: ResolvedTransport,
    cell_args_overrides: list[dict[str, Any]],
    yaml_file: Path,
) -> list[MountConfig]:
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


def _preview_sweep_dry_run(
    sweep_spec: SweepSpec,
    workflow_data: dict[str, Any],
    args_override: dict[str, Any],
) -> None:
    """Expand the matrix and print cell count / per-cell args without executing."""
    from srunx.runtime.sweep.expand import expand_matrix

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

    Reuses :func:`srunx.runtime.sweep.expand.expand_matrix` to produce the same
    per-cell arg overlay the orchestrator will feed into
    :meth:`WorkflowRunner.from_yaml` at execution time. The first cell
    that fails Jinja rendering or workflow validation is reported with
    its index and effective args, matching the error shape used by the
    non-sweep validator.
    """
    from srunx.runtime.sweep.expand import expand_matrix

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
                # Defensive catch — signal handlers must not crash the
                # process. Logging + falling through to the
                # default-int-handler swap is the safest outcome.
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
