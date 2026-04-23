"""Shared helpers across ``WorkflowStorageService``, ``WorkflowSubmissionService``,
and ``SweepSubmissionService``.

These were previously private top-level helpers in
``srunx.web.routers.workflows``. They live here so both the service modules
and the router can import them without creating a service↔router cycle.
The router keeps module-level re-exports (``_get_current_profile`` etc.)
so existing test patches on ``srunx.web.routers.workflows.<helper>``
continue to work.
"""

from __future__ import annotations

import contextlib
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

import anyio
import yaml
from fastapi import HTTPException

from srunx.common.logging import get_logger
from srunx.domain import Workflow
from srunx.runtime.rendering import SubmissionRenderContext
from srunx.runtime.security import find_python_prefix
from srunx.runtime.workflow.runner import WorkflowRunner

logger = get_logger(__name__)


class MountSyncFailedError(Exception):
    """Raised by ``_hold_workflow_mounts_web`` when rsync/lock acquisition
    fails before any sbatch runs.

    Callers (``run_workflow`` / ``_dispatch_sweep``) convert this into
    ``HTTPException(502)``. It's a distinct class from ``HTTPException``
    so the non-sweep handler can tell "lock acquisition failed"
    (no run row yet) apart from "sbatch body failed" (run row needs
    to be marked failed).
    """


# ── Path resolution ────────────────────────────────────────────────


def get_current_profile() -> Any:
    """Wrapper around :func:`srunx.web.sync_utils.get_current_profile`.

    Kept as a distinct helper so the router's ``_get_current_profile``
    re-export stays a one-line delegate that ``unittest.mock.patch``
    targets cleanly.
    """
    from ..sync_utils import get_current_profile as _resolve

    return _resolve()


def find_mount(profile: Any, mount_name: str) -> Any:
    """Find a mount by name. Raises HTTPException 404 if not found."""
    from ..sync_utils import find_mount as _find

    try:
        return _find(profile, mount_name)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e


def workflow_dir(mount_name: str, profile_resolver: Any) -> Path:
    """Resolve workflow directory for a given mount.

    Returns ``<mount.local>/.srunx/workflows/``. ``profile_resolver`` is
    a zero-arg callable returning the active ``ServerProfile`` (or
    ``None``); the router passes its own ``_get_current_profile`` so test
    patches keep working.
    """
    profile = profile_resolver()
    if profile is None:
        raise HTTPException(status_code=503, detail="No SSH profile configured")
    mount = find_mount(profile, mount_name)
    return Path(mount.local) / ".srunx" / "workflows"


def ensure_workflow_dir(mount_name: str, profile_resolver: Any) -> Path:
    """Like :func:`workflow_dir` but creates the directory (and
    ``.srunx/.gitignore``) if needed."""
    d = workflow_dir(mount_name, profile_resolver)
    d.mkdir(parents=True, exist_ok=True)
    gitignore = d.parent / ".gitignore"
    if not gitignore.exists():
        gitignore.write_text("*\n!workflows/\n!workflows/**\n!.gitignore\n")
    return d


def find_yaml(name: str, mount_name: str, profile_resolver: Any) -> Path:
    d = workflow_dir(mount_name, profile_resolver)
    for ext in (".yaml", ".yml"):
        p = d / f"{name}{ext}"
        if p.exists():
            return p
    raise HTTPException(status_code=404, detail=f"Workflow '{name}' not found")


# ── python: prefix security guards ─────────────────────────────────


def reject_python_prefix_web(payload: Any, *, source: str) -> None:
    """Reject ``python:``-prefixed strings in Web API payloads.

    Centralizes the guard applied to both YAML args (pre-parsed by the
    caller) and JSON ``args_override`` / ``sweep.matrix`` payloads.
    Raises ``HTTPException(422)`` on the first violation.
    """
    violation = find_python_prefix(payload, source=source)
    if violation is not None:
        raise HTTPException(
            status_code=422,
            detail=(
                f"{violation.source} at '{violation.path}' contains 'python:' "
                "prefix which is not allowed via web for security reasons"
            ),
        )


def reject_python_prefix_in_yaml_args(yaml_content: str) -> None:
    """Parse YAML text and apply the ``python:`` guard to its ``args`` section.

    Uses ``yaml.safe_load`` so legitimate uses of ``python:`` in commands
    or comments are not blocked. Malformed YAML is left to downstream
    validation to report.
    """
    try:
        data = yaml.safe_load(yaml_content)
    except Exception:
        return

    if not isinstance(data, dict):
        return

    args = data.get("args")
    if not isinstance(args, dict):
        return

    reject_python_prefix_web(args, source="args")


# ── Mount + submission context ─────────────────────────────────────


def build_submission_context(
    mount_name: str | None,
    profile: Any,
) -> SubmissionRenderContext:
    """Construct a :class:`SubmissionRenderContext` from the Web profile + mount.

    - ``mount_name`` is the selected ``?mount=<name>`` query parameter.
      When ``None``, no mount translation is performed.
    - ``profile`` is the configured :class:`ServerProfile` (or ``None``
      when no SSH profile is set up). Its ``mounts`` list is frozen into
      a tuple so the context stays hashable / immutable.
    - ``default_work_dir`` is the selected mount's remote path (so jobs
      whose ``work_dir`` is empty inherit the mount root).
    """
    mounts: tuple[Any, ...] = tuple(profile.mounts) if profile is not None else ()
    default_work_dir: str | None = None
    if mount_name is not None and profile is not None:
        for m in profile.mounts:
            if m.name == mount_name:
                default_work_dir = m.remote
                break
    return SubmissionRenderContext(
        mount_name=mount_name,
        mounts=mounts,
        default_work_dir=default_work_dir,
    )


def enforce_shell_script_roots(
    workflow: Workflow,
    mount: str,
    profile: Any,
    *,
    profile_resolver: Any,
) -> None:
    """Guard that every :class:`ShellJob`'s script_path stays under allowed roots.

    The canonical render helper reads :class:`ShellJob` scripts
    verbatim (``render_shell_job_script`` uses ``script_path`` as the
    template); we still need the directory-traversal check that the
    old ``_render_scripts`` performed before the file was opened. Called
    before render so bogus paths surface as 403 with no partial render
    side effects.
    """
    from srunx.runtime.security import find_shell_script_violation

    allowed_roots = [workflow_dir(mount, profile_resolver).resolve()]
    if profile:
        allowed_roots.extend(Path(m.local).resolve() for m in profile.mounts)
    violation = find_shell_script_violation(workflow, allowed_roots)
    if violation is not None:
        raise HTTPException(
            403,
            f"Script path '{violation.script_path}' is outside allowed directories",
        )


# ── Mount session (lock + rsync) ───────────────────────────────────


@contextlib.asynccontextmanager
async def hold_workflow_mounts_web(
    workflow: Workflow,
    runner: WorkflowRunner,
    *,
    sync_required: bool = True,
) -> AsyncIterator[Any]:
    """Hold the per-mount sync lock across the whole workflow submission.

    Workflow Phase 2 (#135) — web parity with the CLI's
    :func:`srunx.cli.workflow._hold_workflow_mounts`. Each unique
    mount touched by the workflow's :class:`ShellJob` ``script_path``
    values is rsynced **once** under
    :func:`~srunx.sync.service.mount_sync_session`, and the
    per-(profile, mount) lock is held for the entire ``async with``
    block so a concurrent ``srunx flow run`` / ``/api/workflows/run``
    can't rsync different bytes between our sync and our submission.

    Sort order matches the profile's ``mounts`` list so two web
    requests touching overlapping mount sets always acquire locks
    in the same global order — eliminates lock-inversion deadlock,
    same fix as Codex follow-up #2 on PR #141.

    Yields the resolved :class:`ServerProfile` so the caller can use
    it for path translation / submission-context construction. Yields
    ``None`` when no SSH profile is configured (legacy local path).

    Lock acquisition + rsync errors surface as
    :class:`MountSyncFailedError`. Exceptions raised from the body
    (sbatch failures, render errors) propagate **unchanged** so the
    caller can route them through the existing per-phase ``_fail``
    bookkeeping. Mirrors the CLI exception-scoping rationale (Codex
    blocker #1 on PR #141).

    ``sync_required=False`` skips the rsync but still acquires the
    lock — preserves the race-free submission invariant for callers
    that opted out of the transfer.
    """
    from srunx.common.config import get_config
    from srunx.runtime.submission_plan import collect_touched_mounts
    from srunx.ssh.core.config import ConfigManager
    from srunx.sync.lock import SyncLockTimeoutError
    from srunx.sync.service import SyncAbortedError, mount_sync_session

    from ..config import get_web_config
    from ..sync_utils import get_current_profile

    def _resolve_profile_with_name() -> tuple[Any, str | None]:
        # Mirrors ``sync_utils.get_current_profile`` but returns the
        # name too — :func:`acquire_sync_lock` keys the lock file on
        # ``(profile_name, mount_name)`` so we need both halves.
        web_cfg = get_web_config()
        cm = ConfigManager()
        name = web_cfg.ssh_profile or cm.get_current_profile_name()
        if not name:
            return None, None
        return cm.get_profile(name), name

    profile, profile_name = await anyio.to_thread.run_sync(_resolve_profile_with_name)
    if profile is None:
        # Fall back to the patched-in ``get_current_profile`` so tests
        # that mock the helper directly (without seeding the
        # ``ConfigManager`` registry) still see a profile here.
        profile = await anyio.to_thread.run_sync(get_current_profile)
        if profile is None:
            yield None
            return
        profile_name = profile_name or runner.default_project or ""

    mounts = collect_touched_mounts(workflow, profile)
    if not mounts:
        yield profile
        return

    mount_order = {m.name: i for i, m in enumerate(profile.mounts)}
    mounts.sort(key=lambda m: mount_order.get(m.name, len(mount_order)))

    config = get_config()
    # Lock-file key — falls back to the workflow's default_project so
    # we never hand an empty string to acquire_sync_lock.
    effective_profile_name = profile_name or runner.default_project or "default"

    def _enter_all_mounts() -> contextlib.ExitStack:
        # ExitStack lifetime spans the ``async with`` body; constructed
        # off-thread because mount_sync_session is a synchronous CM
        # (file lock + subprocess rsync). The stack is closed back in
        # ``_close_stack`` after the body exits.
        stack = contextlib.ExitStack()
        try:
            for mount in mounts:
                outcome = stack.enter_context(
                    mount_sync_session(
                        profile_name=effective_profile_name,
                        profile=profile,
                        mount=mount,
                        config=config.sync,
                        sync_required=sync_required,
                    )
                )
                if outcome.performed:
                    logger.info("Synced mount '%s'", mount.name)
        except BaseException:
            stack.close()
            raise
        return stack

    try:
        stack = await anyio.to_thread.run_sync(_enter_all_mounts)
    except SyncAbortedError as exc:
        raise MountSyncFailedError(str(exc)) from exc
    except SyncLockTimeoutError as exc:
        raise MountSyncFailedError(str(exc)) from exc
    except RuntimeError as exc:
        raise MountSyncFailedError(str(exc)) from exc

    try:
        # Body exceptions MUST propagate as-is (sbatch failures vs sync
        # failures must stay distinguishable to the caller's per-phase
        # _fail bookkeeping). Codex blocker #1 on PR #141.
        yield profile
    finally:
        await anyio.to_thread.run_sync(stack.close)
