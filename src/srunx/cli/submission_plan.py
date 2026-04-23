"""Compute how an ``srunx sbatch`` invocation should reach the cluster.

Given a script path + SSH profile, we need to answer three questions
before any sbatch call happens:

1. **Is this script under a configured mount?**  If yes, we can
   translate its path to the remote filesystem and execute it
   *in place* (no tmp copy).
2. **Should we sync the mount first?**  Governed by the effective
   ``--sync / --no-sync / config.sync.auto`` resolution.
3. **What remote working directory should sbatch run from?**  Matters
   because SSH sessions start in ``$HOME``; relative paths inside the
   script (e.g. ``#SBATCH --output=./logs/%j.out``) only make sense
   when we ``cd`` somewhere meaningful first.

Encapsulating this as a planner keeps the CLI command handler
readable and leaves a clean seam for Phase 2 (workflow) and Phase 3
(web) to reuse the same logic. The planner itself does no IO — it
inspects paths and returns a dataclass — so it's trivial to unit
test.
"""

from __future__ import annotations

import enum
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from srunx.ssh.core.config import MountConfig, ServerProfile


class SubmissionMode(enum.StrEnum):
    """How the rendered sbatch script reaches the cluster."""

    IN_PLACE = "in_place"
    """Execute the script at its already-on-remote path (post-rsync).

    Used when the script source sits under a mount's ``local`` root,
    no Jinja rendering is required (source bytes == rendered bytes),
    and the user allowed the file to be synced. The cluster runs
    exactly the file the user edits — no tmp copy, no surprise
    rewrites.
    """

    TEMP_UPLOAD = "temp_upload"
    """Upload a generated script into ``$SRUNX_TEMP_DIR`` on the remote.

    Used for ``--wrap`` (no source file), ``--template`` (rendered
    artifact), workflow jobs whose Jinja rendering differs from the
    source, or any script that isn't under a configured mount.
    """


@dataclass(frozen=True)
class SubmissionPlan:
    """Decision record for a single sbatch submission.

    Attributes:
        mode: Which execution path to take.
        mount: The mount that owns the script (``None`` when
            ``mode == TEMP_UPLOAD``).
        remote_script_path: Absolute remote path to ``sbatch`` against.
            ``None`` when the script hasn't been rendered-and-uploaded
            yet — that's ``TEMP_UPLOAD``'s job at submit time.
        submit_cwd: Remote directory to ``cd`` into before sbatch.
            ``None`` means "let SSH default" (== remote ``$HOME``).
        sync_required: Whether the caller must invoke rsync before
            submitting. True only for IN_PLACE with sync enabled.
        warnings: Human-readable notes the CLI should surface to the
            user (e.g. "running without syncing local edits").
    """

    mode: SubmissionMode
    mount: MountConfig | None = None
    remote_script_path: str | None = None
    submit_cwd: str | None = None
    sync_required: bool = False
    warnings: tuple[str, ...] = ()


def resolve_mount_for_path(path: Path, profile: ServerProfile) -> MountConfig | None:
    """Return the profile mount whose ``local`` root contains *path*.

    Uses longest-prefix match so nested mounts (unusual but legal)
    resolve to the deepest one. Resolves symlinks on *path* first to
    avoid leaking outside the mount via symlink traversal — the same
    convention the web router's shell-script guard uses.
    """
    if not profile.mounts:
        return None

    resolved = path.resolve()
    best: MountConfig | None = None
    best_len = -1
    for mount in profile.mounts:
        mount_root = Path(mount.local).expanduser().resolve()
        try:
            resolved.relative_to(mount_root)
        except ValueError:
            continue
        root_len = len(str(mount_root))
        if root_len > best_len:
            best = mount
            best_len = root_len
    return best


def translate_local_to_remote(path: Path, mount: MountConfig) -> str:
    """Translate a local path under *mount* into its remote equivalent.

    Preconditions:
        *path* must already be under ``mount.local`` (caller should
        have verified via :func:`resolve_mount_for_path`).

    The remote side is POSIX-only, so we join with ``/`` explicitly
    rather than relying on ``Path`` (which would pick up the host's
    separator on Windows workstations).
    """
    local_root = Path(mount.local).expanduser().resolve()
    rel = path.resolve().relative_to(local_root)
    remote_root = mount.remote.rstrip("/")
    rel_posix = str(rel).replace("\\", "/")
    if rel_posix in ("", "."):
        return remote_root
    return f"{remote_root}/{rel_posix}"


def plan_sbatch_submission(
    *,
    script_path: Path | None,
    profile: ServerProfile | None,
    cwd: Path | None,
    sync_enabled: bool,
    is_rendered_artifact: bool,
) -> SubmissionPlan:
    """Decide how to submit a single sbatch script.

    Args:
        script_path: The user's positional script path, or ``None``
            when ``--wrap`` / ``--template`` were used (no source
            file).
        profile: The resolved SSH profile, or ``None`` for a local
            SLURM submission (no mount concept there).
        cwd: The workstation's current working directory, used to
            derive ``submit_cwd`` when ``cwd`` also lives under a
            mount. ``None`` skips the cwd translation.
        sync_enabled: Effective value of the ``--sync / --no-sync /
            config.sync.auto`` resolution.
        is_rendered_artifact: Caller already rendered the script
            (e.g. ``--template`` expanded a Jinja file); the result
            is a generated artifact and must not be treated as a
            source file even if ``script_path`` sits under a mount.
    """
    if script_path is None or profile is None or is_rendered_artifact:
        return SubmissionPlan(
            mode=SubmissionMode.TEMP_UPLOAD,
            submit_cwd=_translate_cwd(cwd, profile),
        )

    mount = resolve_mount_for_path(script_path, profile)
    if mount is None:
        return SubmissionPlan(
            mode=SubmissionMode.TEMP_UPLOAD,
            submit_cwd=_translate_cwd(cwd, profile),
        )

    remote_path = translate_local_to_remote(script_path, mount)
    warnings: list[str] = []
    if not sync_enabled:
        warnings.append(
            f"Running without syncing '{mount.name}'. The remote may "
            f"be out of date; re-run with --sync or rerun "
            f"'srunx ssh sync' to refresh."
        )

    return SubmissionPlan(
        mode=SubmissionMode.IN_PLACE,
        mount=mount,
        remote_script_path=remote_path,
        submit_cwd=_preferred_submit_cwd(script_path, cwd, mount),
        sync_required=sync_enabled,
        warnings=tuple(warnings),
    )


def _preferred_submit_cwd(
    script_path: Path, cwd: Path | None, mount: MountConfig
) -> str:
    """Pick a remote cwd for sbatch when the script is in a mount.

    Order of preference:

    1. If the caller's ``cwd`` sits under the same mount, translate it.
       This preserves relative paths a user might have typed in
       interactively (``#SBATCH -o ./logs/%j.out``).
    2. Otherwise fall back to the script's enclosing directory on the
       remote. That's still strictly better than the SSH default
       ``$HOME``.
    """
    if cwd is not None:
        mount_root = Path(mount.local).expanduser().resolve()
        resolved_cwd = cwd.resolve()
        try:
            resolved_cwd.relative_to(mount_root)
        except ValueError:
            pass
        else:
            return translate_local_to_remote(resolved_cwd, mount)

    # Script's parent directory on the remote.
    remote_script = translate_local_to_remote(script_path, mount)
    parent, _, _ = remote_script.rpartition("/")
    return parent or remote_script


def collect_touched_mounts(workflow: Any, profile: ServerProfile) -> list[MountConfig]:
    """Return mounts that the workflow's ShellJob ``script_path`` values touch.

    Workflow Phase 2 (#135): the runner needs to rsync each touched
    mount **once** at the start of the run, then hold the lock
    across every job submission. This helper does the resolution part
    (``script_path`` → owning ``MountConfig``) so the runner can
    feed the result into a single ``mount_sync_session`` per mount.

    Jobs without a local ``script_path`` (``Job`` with command, or
    ShellJobs whose path lives outside any mount) contribute nothing;
    they fall back to the temp-upload path at submission time.

    The return order is stable (insertion order of profile.mounts) so
    the lock-acquisition order is deterministic, sidestepping any
    mount-vs-mount deadlock when two ``srunx flow run`` invocations
    target overlapping mount sets.
    """
    seen: dict[str, MountConfig] = {}
    for job in getattr(workflow, "jobs", ()):
        script_attr = getattr(job, "script_path", None)
        if not script_attr:
            continue
        try:
            mount = resolve_mount_for_path(Path(script_attr), profile)
        except (OSError, ValueError):
            continue
        if mount is not None and mount.name not in seen:
            seen[mount.name] = mount
    return list(seen.values())


def render_matches_source(rendered_path: Path, source_path: Path) -> bool:
    """Return ``True`` when the rendered ShellJob bytes equal the source.

    Used by the workflow's per-ShellJob plan step to decide whether
    Jinja substitution actually changed anything. When the bytes are
    identical the script can run *in place* on the cluster (the
    user's source file is what executes); when they differ the
    rendered output is a generated artifact and must take the
    temp-upload path.

    Trailing newlines are normalised before comparison so a Jinja
    pass that round-trips a no-substitution script doesn't get
    flagged as "different" because of a stripped final ``\\n``.
    Codex blocker #2 on PR #141 caught this — we also fixed the
    Jinja env in :func:`_render_base_script` to ``keep_trailing_newline``,
    but rstripping defends against any future template/Jinja behaviour
    drift.

    Both paths must exist; missing files return ``False`` so the
    caller treats the mismatch as "render produced something
    different" and stays on the safe (temp-upload) path.
    """
    try:
        rendered = rendered_path.read_bytes()
        source = source_path.read_bytes()
    except OSError:
        return False
    return rendered.rstrip(b"\n") == source.rstrip(b"\n")


def _translate_cwd(cwd: Path | None, profile: ServerProfile | None) -> str | None:
    """Translate *cwd* to its remote equivalent if it lives under any mount.

    Used for ``TEMP_UPLOAD`` submissions (``--wrap``, mount-outside
    scripts) so that relative paths the wrapped command expects still
    resolve. Returns ``None`` when no mount matches — the caller
    should then leave sbatch to default to remote ``$HOME``.
    """
    if cwd is None or profile is None:
        return None
    mount = resolve_mount_for_path(cwd, profile)
    if mount is None:
        return None
    return translate_local_to_remote(cwd, mount)
