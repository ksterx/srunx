"""Per-mount sync lock + in-place context helpers for CLI workflow runs."""

from __future__ import annotations

import contextlib
import dataclasses
from collections.abc import Iterator
from typing import TYPE_CHECKING, Any

import typer
from rich.console import Console

from srunx.common.config import get_config
from srunx.transport import ResolvedTransport

if TYPE_CHECKING:
    from srunx.domain import Workflow
    from srunx.ssh.core.config import MountConfig


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
    rt: ResolvedTransport,
    *,
    locked_mount_names: tuple[str, ...] = (),
) -> Any:
    """Return ``rt.submission_context`` with ``allow_in_place=True``.

    The CLI workflow runner holds the per-(profile, mount) sync lock
    for the entire run via :func:`_hold_workflow_mounts`, so it is
    safe for the SSH adapter to take the IN_PLACE submission path
    inside this run. The flag lives on
    :class:`~srunx.runtime.rendering.SubmissionRenderContext` because it
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
    if rt.submission_context is None:
        return None
    return dataclasses.replace(
        rt.submission_context,
        allow_in_place=True,
        locked_mount_names=locked_mount_names,
    )


@contextlib.contextmanager
def _hold_workflow_mounts(
    *,
    rt: ResolvedTransport,
    workflow_for_mounts: Workflow | None,
    sync_required: bool,
    explicit_mounts: list[MountConfig] | None = None,
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
